from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from werkzeug.security import check_password_hash, generate_password_hash

import config
from mymail.cosmos import container as cosmos_container
from mymail.cosmos import containers as cosmos_containers
from mymail.cosmos import cosmos_enabled
from mymail.revisiones_blob import upload_revision


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


def _use_cosmos() -> bool:
    return cosmos_enabled()


def _cosmos(name: str):
    return cosmos_container(name)


def _containers():
    return cosmos_containers()


def _cosmos_container_name_for(table_name: str) -> str:
    name = str(table_name or "").strip()
    if name == str(getattr(config, "TABLE_USERS", "") or ""):
        return _containers().users
    if name == str(getattr(config, "TABLE_LOGS", "") or ""):
        return _containers().logs
    if name == str(getattr(config, "TABLE_RESULTADOS", "") or ""):
        return _containers().resultados
    if name == str(getattr(config, "TABLE_DESCARTES", "") or ""):
        return _containers().descartes
    return name


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    reason: str = ""
    role: str = ""


ROLE_REVISOR = "Revisor"
ROLE_ADMIN = "Administrador"


def normalize_role(role: str) -> str:
    role = (role or "").strip()
    if not role:
        return ROLE_REVISOR
    if role.lower() in {"admin", "administrador", "administrator"}:
        return ROLE_ADMIN
    if role.lower() in {"revisor", "reviewer"}:
        return ROLE_REVISOR
    return ROLE_REVISOR


def create_user(username: str, password: str, *, role: str = ROLE_REVISOR) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vacío")
    if not password:
        raise ValueError("password vacío")

    now = _utcnow()
    role = normalize_role(role)
    if username.lower() == "admin":
        role = ROLE_ADMIN
    if _use_cosmos():
        c = _cosmos(_containers().users)
        c.upsert_item(
            {
                "id": username,
                "pk": "users",
                "password_hash": generate_password_hash(password),
                "role": role,
                "active": True,
                "created_at": now.isoformat(),
            }
        )
        return

    client = _table(config.TABLE_USERS)
    client.upsert_entity(
        mode="merge",
        entity={
            "PartitionKey": "users",
            "RowKey": username,
            "password_hash": generate_password_hash(password),
            "role": role,
            "active": True,
            "created_at": now.isoformat(),
        },
    )


def set_user_role(username: str, role: str) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vac︽")
    role = normalize_role(role)
    if username.lower() == "admin":
        role = ROLE_ADMIN
    if _use_cosmos():
        c = _cosmos(_containers().users)
        c.upsert_item({"id": username, "pk": "users", "role": role})
        return

    client = _table(config.TABLE_USERS)
    client.upsert_entity(mode="merge", entity={"PartitionKey": "users", "RowKey": username, "role": role})


def set_user_password(username: str, password: str) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vacío")
    if not password:
        raise ValueError("password vacío")

    if _use_cosmos():
        c = _cosmos(_containers().users)
        c.upsert_item({"id": username, "pk": "users", "password_hash": generate_password_hash(password)})
        return

    client = _table(config.TABLE_USERS)
    client.upsert_entity(
        mode="merge",
        entity={"PartitionKey": "users", "RowKey": username, "password_hash": generate_password_hash(password)},
    )


def list_users() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if _use_cosmos():
        try:
            c = _cosmos(_containers().users)
            rows = c.query_items(
                query="SELECT c.id, c.role, c.active FROM c WHERE c.pk=@pk",
                parameters=[{"name": "@pk", "value": "users"}],
                enable_cross_partition_query=True,
            )
            for ent in rows:
                out.append(
                    {
                        "username": str(ent.get("id", "") or ""),
                        "role": normalize_role(str(ent.get("role", "") or ROLE_REVISOR)),
                        "active": "1" if bool(ent.get("active", True)) else "0",
                    }
                )
        except Exception:
            return []
    else:
        client = _table(config.TABLE_USERS)
        try:
            for ent in client.query_entities(query_filter="PartitionKey eq 'users'"):
                out.append(
                    {
                        "username": str(ent.get("RowKey", "") or ""),
                        "role": normalize_role(str(ent.get("role", "") or ROLE_REVISOR)),
                        "active": "1" if bool(ent.get("active", True)) else "0",
                    }
                )
        except Exception:
            return []
    out.sort(key=lambda u: u.get("username", ""))
    return out


def verify_user(username: str, password: str) -> AuthResult:
    username = (username or "").strip()
    if not username or not password:
        return AuthResult(False, "missing")

    if _use_cosmos():
        try:
            from azure.cosmos.exceptions import CosmosResourceNotFoundError
        except Exception:  # pragma: no cover
            return AuthResult(False, "not_found")
        c = _cosmos(_containers().users)
        try:
            entity = c.read_item(item=username, partition_key="users")
        except CosmosResourceNotFoundError:
            return AuthResult(False, "not_found")
        except Exception:
            return AuthResult(False, "not_found")
    else:
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

    role = normalize_role(str(entity.get("role", "") or ROLE_REVISOR))
    if username.lower() == "admin":
        role = ROLE_ADMIN
    return AuthResult(True, "", role=role)


def log_click(
    *,
    action: str,
    username: str,
    record_id: str = "",
    result: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    now = _utcnow()
    if _use_cosmos():
        c = _cosmos(_containers().logs)
        ent: Dict[str, Any] = {
            "id": uuid.uuid4().hex,
            "pk": _day(now),
            "timestamp": now.isoformat(),
            "day": _day(now),
            "weekday": _weekday(now),
            "user": username or "",
            "action": action,
            "record_id": record_id or "",
            "result": result or "",
        }
        if extra:
            ent["extra_json"] = json.dumps(extra, ensure_ascii=False)
        c.create_item(ent)
        return

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
    ko_mym_reason: str = "",
    multitematica: bool = False,
) -> None:
    now = _utcnow()
    history = [
        {
            "timestamp": now.isoformat(),
            "user": username or "",
            "action": "created",
            "changes": {
                "status": {"from": "", "to": status},
                "ko_mym_reason": {"from": "", "to": ko_mym_reason or ""},
                "multitematica": {"from": False, "to": bool(multitematica)},
                "reviewer_note": {"from": "", "to": reviewer_note or ""},
                "internal_note": {"from": "", "to": internal_note or ""},
            },
        }
    ]
    if _use_cosmos():
        c = _cosmos(_containers().resultados)
        c.create_item(
            {
                "id": uuid.uuid4().hex,
                "pk": _day(now),
                "timestamp": now.isoformat(),
                "day": _day(now),
                "weekday": _weekday(now),
                "user": username or "",
                "record_id": record.get("IdCorreo", "") or "",
                "automatismo": record.get("Automatismo", "") or "",
                "status": status,
                "ko_mym_reason": ko_mym_reason or "",
                "multitematica": bool(multitematica),
                "reviewer_note": reviewer_note or "",
                "internal_note": internal_note or "",
                "record_json": json.dumps(record, ensure_ascii=False),
            }
        )
    else:
        client = _table(config.TABLE_RESULTADOS)
        client.create_entity(
            entity={
                "PartitionKey": _day(now),
                "RowKey": uuid.uuid4().hex,
                "timestamp": now.isoformat(),
                "day": _day(now),
                "weekday": _weekday(now),
                "user": username or "",
                "record_id": record.get("IdCorreo", "") or "",
                "automatismo": record.get("Automatismo", "") or "",
                "status": status,
                "ko_mym_reason": ko_mym_reason or "",
                "multitematica": bool(multitematica),
                "reviewer_note": reviewer_note or "",
                "internal_note": internal_note or "",
                "record_json": json.dumps(record, ensure_ascii=False),
            }
        )

    try:
        upload_revision(
            {
                "timestamp": now.isoformat(),
                "user": username or "",
                "role": "",
                "record_id": record.get("IdCorreo", "") or "",
                "automatismo": record.get("Automatismo", "") or "",
                "status": status,
                "ko_mym_reason": ko_mym_reason or "",
                "multitematica": bool(multitematica),
                "reviewer_note": reviewer_note or "",
                "internal_note": internal_note or "",
                "history": history,
                "record": record,
            },
            username=username or "",
            reviewed_at=now,
        )
    except Exception:
        pass


def write_descarte(*, username: str, record: Dict[str, str]) -> None:
    now = _utcnow()
    if _use_cosmos():
        c = _cosmos(_containers().descartes)
        c.create_item(
            {
                "id": uuid.uuid4().hex,
                "pk": _day(now),
                "timestamp": now.isoformat(),
                "day": _day(now),
                "weekday": _weekday(now),
                "user": username or "",
                "record_id": record.get("IdCorreo", "") or "",
                "automatismo": record.get("Automatismo", "") or "",
                "record_json": json.dumps(record, ensure_ascii=False),
            }
        )
        return

    client = _table(config.TABLE_DESCARTES)
    client.create_entity(
        entity={
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
    )


def _list_by_days(table_name: str, days: list[str]):
    out = []
    if _use_cosmos():
        # Para Cosmos ignoramos table_name y consultamos el contenedor correspondiente por nombre.
        c = _cosmos(_cosmos_container_name_for(str(table_name)))
        for d in days:
            out.extend(
                list(
                    c.query_items(
                        query="SELECT * FROM c WHERE c.pk=@pk",
                        parameters=[{"name": "@pk", "value": str(d)}],
                        enable_cross_partition_query=True,
                    )
                )
            )
        return out

    client = _table(table_name)
    for d in days:
        filt = f"PartitionKey eq '{d}'"
        out.extend(list(client.query_entities(query_filter=filt)))
    return out


def _list_by_day_range(table_name: str, *, start_day: str, end_day: str):
    start_day = str(start_day or "").strip()
    end_day = str(end_day or "").strip()
    if not start_day or not end_day:
        return []
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    if _use_cosmos():
        c = _cosmos(_cosmos_container_name_for(str(table_name)))
        return list(
            c.query_items(
                query="SELECT * FROM c WHERE c.pk >= @s AND c.pk <= @e",
                parameters=[{"name": "@s", "value": start_day}, {"name": "@e", "value": end_day}],
                enable_cross_partition_query=True,
            )
        )

    client = _table(table_name)
    filt = f"PartitionKey ge '{start_day}' and PartitionKey le '{end_day}'"
    return list(client.query_entities(query_filter=filt))
