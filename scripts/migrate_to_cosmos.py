from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from mymail.cosmos import containers as cosmos_containers
from mymail.cosmos import ensure_resources as cosmos_ensure_resources


def _require_table_conn_str() -> str:
    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py/.env (origen Table Storage).")
    return conn


def _require_cosmos() -> tuple[str, str, str]:
    endpoint = (getattr(config, "COSMOS_ENDPOINT", "") or "").strip()
    key = (getattr(config, "COSMOS_KEY", "") or "").strip()
    db = (getattr(config, "COSMOS_DATABASE", "") or "").strip() or "mymailreview"
    if not endpoint or not key:
        raise RuntimeError("Falta COSMOS_ENDPOINT/COSMOS_KEY en config.py/.env (destino CosmosDB).")
    return endpoint, key, db


def _table_service():
    try:
        from azure.data.tables import TableServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-data-tables (pip install -r requirements.txt)") from exc
    return TableServiceClient.from_connection_string(_require_table_conn_str())


def _cosmos_client():
    try:
        from azure.cosmos import CosmosClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-cosmos (pip install -r requirements.txt)") from exc
    endpoint, key, _ = _require_cosmos()
    return CosmosClient(endpoint, credential=key)


def _cosmos_container(name: str):
    _, _, db = _require_cosmos()
    cli = _cosmos_client()
    return cli.get_database_client(db).get_container_client(name)


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return value.hex()
    if isinstance(value, (datetime, date)):
        try:
            if isinstance(value, datetime) and value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return str(value)


def _iter_table_entities(table_name: str, *, filter_: str | None = None) -> Iterable[Dict[str, Any]]:
    svc = _table_service()
    client = svc.get_table_client(table_name)
    if filter_:
        for ent in client.query_entities(query_filter=filter_):
            yield dict(ent)
        return
    for ent in client.list_entities():
        yield dict(ent)


def _day_keys(days: int) -> list[str]:
    days = max(1, int(days))
    now = datetime.now(timezone.utc)
    return [(now - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]


def migrate_users(*, dry_run: bool, limit: int | None = None) -> int:
    c = _cosmos_container(cosmos_containers().users)
    moved = 0
    for ent in _iter_table_entities(getattr(config, "TABLE_USERS", "users"), filter_="PartitionKey eq 'users'"):
        username = str(ent.get("RowKey", "") or "").strip()
        if not username:
            continue
        doc = {
            "id": username,
            "pk": "users",
            "password_hash": str(ent.get("password_hash", "") or ""),
            "role": str(ent.get("role", "") or ""),
            "active": bool(ent.get("active", True)),
            "created_at": str(ent.get("created_at", "") or ""),
        }
        doc = _jsonable(doc)
        if not dry_run:
            c.upsert_item(doc)
        moved += 1
        if limit is not None and moved >= limit:
            break
    return moved


def migrate_logs(*, dry_run: bool, limit: int | None = None, days: int | None = None) -> int:
    c = _cosmos_container(cosmos_containers().logs)
    moved = 0
    src_table = getattr(config, "TABLE_LOGS", "logs")
    if days is not None:
        for d in _day_keys(days):
            for ent in _iter_table_entities(src_table, filter_=f"PartitionKey eq '{d}'"):
                doc = dict(ent)
                doc = {
                    "id": str(doc.get("RowKey", "") or ""),
                    "pk": str(doc.get("PartitionKey", "") or ""),
                    **{k: _jsonable(v) for k, v in doc.items() if k not in {"RowKey", "PartitionKey"}},
                }
                if not doc["id"] or not doc["pk"]:
                    continue
                if not dry_run:
                    c.upsert_item(doc)
                moved += 1
                if limit is not None and moved >= limit:
                    return moved
        return moved

    for ent in _iter_table_entities(src_table):
        doc = dict(ent)
        doc = {
            "id": str(doc.get("RowKey", "") or ""),
            "pk": str(doc.get("PartitionKey", "") or ""),
            **{k: _jsonable(v) for k, v in doc.items() if k not in {"RowKey", "PartitionKey"}},
        }
        if not doc["id"] or not doc["pk"]:
            continue
        if not dry_run:
            c.upsert_item(doc)
        moved += 1
        if limit is not None and moved >= limit:
            break
    return moved


def migrate_day_partitioned(
    *,
    src_table: str,
    dst_container: str,
    dry_run: bool,
    limit: int | None = None,
    days: int | None = None,
) -> int:
    c = _cosmos_container(dst_container)
    moved = 0
    if days is not None:
        for d in _day_keys(days):
            for ent in _iter_table_entities(src_table, filter_=f"PartitionKey eq '{d}'"):
                doc = dict(ent)
                doc = {
                    "id": str(doc.get("RowKey", "") or ""),
                    "pk": str(doc.get("PartitionKey", "") or ""),
                    **{k: _jsonable(v) for k, v in doc.items() if k not in {"RowKey", "PartitionKey"}},
                }
                if not doc["id"] or not doc["pk"]:
                    continue
                if not dry_run:
                    c.upsert_item(doc)
                moved += 1
                if limit is not None and moved >= limit:
                    return moved
        return moved

    for ent in _iter_table_entities(src_table):
        doc = dict(ent)
        doc = {
            "id": str(doc.get("RowKey", "") or ""),
            "pk": str(doc.get("PartitionKey", "") or ""),
            **{k: _jsonable(v) for k, v in doc.items() if k not in {"RowKey", "PartitionKey"}},
        }
        if not doc["id"] or not doc["pk"]:
            continue
        if not dry_run:
            c.upsert_item(doc)
        moved += 1
        if limit is not None and moved >= limit:
            break
    return moved


def migrate_entrada(*, dry_run: bool, limit: int | None = None, partition: str = "active") -> int:
    c = _cosmos_container(cosmos_containers().entrada)
    moved = 0
    src_table = getattr(config, "TABLE_ENTRADA", "entrada")
    for ent in _iter_table_entities(src_table, filter_=f"PartitionKey eq '{partition}'"):
        doc = dict(ent)
        doc = {
            "id": str(doc.get("RowKey", "") or ""),
            "pk": str(doc.get("PartitionKey", "") or ""),
            **{k: _jsonable(v) for k, v in doc.items() if k not in {"RowKey", "PartitionKey"}},
        }
        if not doc["id"] or not doc["pk"]:
            continue
        doc.setdefault("lock_owner", "")
        doc.setdefault("lock_token", "")
        doc.setdefault("lock_until", "")
        doc.setdefault("lock_acquired_at", "")
        if not dry_run:
            c.upsert_item(doc)
        moved += 1
        if limit is not None and moved >= limit:
            break
    return moved


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Migración de Azure Table Storage → Azure Cosmos DB (SQL API)")
    parser.add_argument("--dry-run", action="store_true", help="No escribe en Cosmos, solo cuenta/valida.")
    parser.add_argument("--ensure", action="store_true", help="Crea DB/containers de Cosmos si no existen.")
    parser.add_argument("--limit", type=int, default=0, help="Máximo de items por bloque (0 = sin límite).")
    parser.add_argument("--days", type=int, default=0, help="Para tablas por día (logs/resultados/descartes): últimos N días (0 = todo).")
    parser.add_argument("--entrada-partition", default="active", help="PartitionKey de entrada a migrar (default: active).")
    parser.add_argument(
        "--only",
        default="all",
        help="Qué migrar: all | users | logs | resultados | descartes | entrada (coma-separado).",
    )
    args = parser.parse_args(argv)

    _require_table_conn_str()
    _require_cosmos()

    if args.ensure:
        cosmos_ensure_resources()

    limit = int(args.limit) if int(args.limit) > 0 else None
    days = int(args.days) if int(args.days) > 0 else None

    only = {s.strip().lower() for s in str(args.only or "all").split(",") if s.strip()}
    if "all" in only:
        only = {"users", "logs", "resultados", "descartes", "entrada"}

    total = 0

    if "users" in only:
        n = migrate_users(dry_run=bool(args.dry_run), limit=limit)
        print(f"OK users: {n}")
        total += n

    if "logs" in only:
        n = migrate_logs(dry_run=bool(args.dry_run), limit=limit, days=days)
        print(f"OK logs: {n}")
        total += n

    if "resultados" in only:
        n = migrate_day_partitioned(
            src_table=getattr(config, "TABLE_RESULTADOS", "resultados"),
            dst_container=cosmos_containers().resultados,
            dry_run=bool(args.dry_run),
            limit=limit,
            days=days,
        )
        print(f"OK resultados: {n}")
        total += n

    if "descartes" in only:
        n = migrate_day_partitioned(
            src_table=getattr(config, "TABLE_DESCARTES", "descartes"),
            dst_container=cosmos_containers().descartes,
            dry_run=bool(args.dry_run),
            limit=limit,
            days=days,
        )
        print(f"OK descartes: {n}")
        total += n

    if "entrada" in only:
        n = migrate_entrada(dry_run=bool(args.dry_run), limit=limit, partition=str(args.entrada_partition))
        print(f"OK entrada[{args.entrada_partition}]: {n}")
        total += n

    print(f"TOTAL: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

