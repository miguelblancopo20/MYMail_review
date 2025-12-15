from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from werkzeug.security import check_password_hash, generate_password_hash

import config


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _day(value: datetime) -> str:
    return value.strftime("%Y%m%d")


def _weekday(value: datetime) -> str:
    return value.strftime("%A")


def _require_conn_str() -> str:
    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py")
    return conn


def _service():
    try:
        from azure.data.tables import TableServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-data-tables (pip install -r requirements.txt)") from exc
    return TableServiceClient.from_connection_string(_require_conn_str())


def _table(name: str):
    service = _service()
    client = service.get_table_client(name)
    try:
        client.create_table()
    except Exception:
        pass
    return client


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    reason: str = ""


def create_user(username: str, password: str) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vacío")
    if not password:
        raise ValueError("password vacío")

    now = _utcnow()
    client = _table(config.TABLE_USERS)
    entity = {
        "PartitionKey": "users",
        "RowKey": username,
        "password_hash": generate_password_hash(password),
        "active": True,
        "created_at": now.isoformat(),
    }
    client.upsert_entity(mode="merge", entity=entity)


def verify_user(username: str, password: str) -> AuthResult:
    username = (username or "").strip()
    if not username or not password:
        return AuthResult(False, "missing")

    client = _table(config.TABLE_USERS)
    try:
        entity = client.get_entity(partition_key="users", row_key=username)
    except Exception:
        return AuthResult(False, "not_found")

    if not bool(entity.get("active", True)):
        return AuthResult(False, "inactive")

    pwd_hash = entity.get("password_hash") or ""
    if not pwd_hash or not check_password_hash(pwd_hash, password):
        return AuthResult(False, "invalid")

    return AuthResult(True, "")


def log_click(
    *,
    action: str,
    username: str,
    record_id: str = "",
    result: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    now = _utcnow()
    client = _table(config.TABLE_LOGS)
    entity: Dict[str, Any] = {
        "PartitionKey": _day(now),
        "RowKey": uuid.uuid4().hex,
        "timestamp": now.isoformat(),
        "day": _day(now),
        "weekday": _weekday(now),
        "user": username or "",
        "action": action,
        "record_id": record_id or "",
        "result": result or "",
    }
    if extra:
        entity["extra_json"] = json.dumps(extra, ensure_ascii=False)
    client.create_entity(entity=entity)


def write_resultado(
    *,
    username: str,
    record: Dict[str, str],
    status: str,
    reviewer_note: str,
    internal_note: str,
) -> None:
    now = _utcnow()
    client = _table(config.TABLE_RESULTADOS)
    entity: Dict[str, Any] = {
        "PartitionKey": _day(now),
        "RowKey": uuid.uuid4().hex,
        "timestamp": now.isoformat(),
        "day": _day(now),
        "weekday": _weekday(now),
        "user": username or "",
        "record_id": record.get("IdCorreo", "") or "",
        "automatismo": record.get("Automatismo", "") or "",
        "status": status,
        "reviewer_note": reviewer_note or "",
        "internal_note": internal_note or "",
        "record_json": json.dumps(record, ensure_ascii=False),
    }
    client.create_entity(entity=entity)


def write_descarte(*, username: str, record: Dict[str, str]) -> None:
    now = _utcnow()
    client = _table(config.TABLE_DESCARTES)
    entity: Dict[str, Any] = {
        "PartitionKey": _day(now),
        "RowKey": uuid.uuid4().hex,
        "timestamp": now.isoformat(),
        "day": _day(now),
        "weekday": _weekday(now),
        "user": username or "",
        "record_id": record.get("IdCorreo", "") or "",
        "automatismo": record.get("Automatismo", "") or "",
        "record_json": json.dumps(record, ensure_ascii=False),
    }
    client.create_entity(entity=entity)


def _list_by_days(table_name: str, days: list[str]):
    client = _table(table_name)
    out = []
    for d in days:
        filt = f"PartitionKey eq '{d}'"
        out.extend(list(client.query_entities(query_filter=filt)))
    return out
