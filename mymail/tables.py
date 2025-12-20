from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from werkzeug.security import check_password_hash, generate_password_hash

from mymail.cosmos import container as cosmos_container
from mymail.cosmos import cosmos_enabled
from mymail.cosmos import containers as cosmos_containers


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _day(value: datetime) -> str:
    return value.strftime("%Y%m%d")


def _weekday(value: datetime) -> str:
    return value.strftime("%A")


def _cosmos(name: str):
    return cosmos_container(name)


def _containers():
    return cosmos_containers()


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


def set_user_role(username: str, role: str) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vac︽")
    role = normalize_role(role)
    if username.lower() == "admin":
        role = ROLE_ADMIN
    c = _cosmos(_containers().users)
    c.upsert_item({"id": username, "pk": "users", "role": role})


def set_user_password(username: str, password: str) -> None:
    username = (username or "").strip()
    if not username:
        raise ValueError("username vacío")
    if not password:
        raise ValueError("password vacío")

    c = _cosmos(_containers().users)
    c.upsert_item({"id": username, "pk": "users", "password_hash": generate_password_hash(password)})


def list_users() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
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
    out.sort(key=lambda u: u.get("username", ""))
    return out


def verify_user(username: str, password: str) -> AuthResult:
    username = (username or "").strip()
    if not username or not password:
        return AuthResult(False, "missing")

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
    if not cosmos_enabled():
        return
    now = _utcnow()
    try:
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
    except Exception:
        return


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

    # Cosmos-only: el "Listado" se alimenta del contenedor `resultados`, no se generan snapshots aparte.


def write_descarte(*, username: str, record: Dict[str, str]) -> None:
    now = _utcnow()
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


def _list_by_days(container_name: str, days: list[str]):
    out = []
    c = _cosmos(str(container_name))
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


def _list_by_day_range(container_name: str, *, start_day: str, end_day: str):
    start_day = str(start_day or "").strip()
    end_day = str(end_day or "").strip()
    if not start_day or not end_day:
        return []
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    c = _cosmos(str(container_name))
    return list(
        c.query_items(
            query="SELECT * FROM c WHERE c.pk >= @s AND c.pk <= @e",
            parameters=[{"name": "@s", "value": start_day}, {"name": "@e", "value": end_day}],
            enable_cross_partition_query=True,
        )
    )
