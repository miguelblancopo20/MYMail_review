from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from mymail.cosmos import container as cosmos_container
from mymail.cosmos import containers as cosmos_containers


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_EXEC = ThreadPoolExecutor(max_workers=8)


def _with_timeout(fn, *, timeout_s: float = 20.0):
    fut = _EXEC.submit(fn)
    try:
        return fut.result(timeout=timeout_s)
    except FuturesTimeoutError as exc:
        raise TimeoutError(f"Timeout ({timeout_s}s) conectando con CosmosDB.") from exc


def _container():
    return cosmos_container(cosmos_containers().entrada)


DEFAULT_PARTITION = "active"
LOCK_TTL_SECONDS = 600


@dataclass(frozen=True)
class EntradaKey:
    partition_key: str
    row_key: str


def _parse_dt(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _lock_until(now: datetime, ttl_seconds: int) -> datetime:
    ttl = max(1, int(ttl_seconds))
    return now + timedelta(seconds=ttl)


def list_keys(partition_key: str = DEFAULT_PARTITION) -> List[EntradaKey]:
    c = _container()
    out: List[EntradaKey] = []
    rows = _with_timeout(
        lambda: list(
            c.query_items(
                query="SELECT c.pk, c.id FROM c WHERE c.pk=@pk",
                parameters=[{"name": "@pk", "value": str(partition_key)}],
                enable_cross_partition_query=True,
            )
        ),
        timeout_s=20.0,
    )
    for ent in rows:
        out.append(EntradaKey(partition_key=str(ent.get("pk", "") or ""), row_key=str(ent.get("id", "") or "")))
    return out


def list_pending_meta(partition_key: str = DEFAULT_PARTITION, *, limit: int | None = None) -> List[Dict[str, str]]:
    c = _container()
    out: List[Dict[str, str]] = []
    rows = _with_timeout(
        lambda: list(
            c.query_items(
                query="SELECT c.pk, c.id, c.record_id, c.timestamp, c.automatismo, c.lock_owner, c.lock_until FROM c WHERE c.pk=@pk",
                parameters=[{"name": "@pk", "value": str(partition_key)}],
                enable_cross_partition_query=True,
            )
        ),
        timeout_s=25.0,
    )
    for ent in rows:
        out.append(
            {
                "pk": str(ent.get("pk", "") or ""),
                "rk": str(ent.get("id", "") or ""),
                "record_id": str(ent.get("record_id", "") or ""),
                "timestamp": str(ent.get("timestamp", "") or ""),
                "automatismo": str(ent.get("automatismo", "") or ""),
                "lock_owner": str(ent.get("lock_owner", "") or ""),
                "lock_until": str(ent.get("lock_until", "") or ""),
            }
        )
        if limit is not None and len(out) >= int(limit):
            break
    return out


def list_pending_payloads_for_stats(
    partition_key: str = DEFAULT_PARTITION, *, limit: int | None = None
) -> List[Dict[str, str]]:
    c = _container()
    out: List[Dict[str, str]] = []
    rows = _with_timeout(
        lambda: list(
            c.query_items(
                query="SELECT c.pk, c.id, c.timestamp, c.record_json FROM c WHERE c.pk=@pk",
                parameters=[{"name": "@pk", "value": str(partition_key)}],
                enable_cross_partition_query=True,
            )
        ),
        timeout_s=30.0,
    )
    for ent in rows:
        out.append(
            {
                "pk": str(ent.get("pk", "") or ""),
                "rk": str(ent.get("id", "") or ""),
                "timestamp": str(ent.get("timestamp", "") or ""),
                "record_json": str(ent.get("record_json", "") or ""),
                "record_blob": "",  # compat
            }
        )
        if limit is not None and len(out) >= int(limit):
            break
    return out


def record_from_payload(*, record_json: str = "", record_blob: str = "") -> Dict[str, str]:
    payload = (record_json or "").strip()
    if not payload:
        return {}
    try:
        record = json.loads(payload)
    except Exception:
        record = {}
    if not isinstance(record, dict):
        record = {}
    return {k: ("" if v is None else str(v)) for k, v in record.items()}


def get_record(key: EntradaKey) -> Dict[str, str]:
    c = _container()
    ent = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
    payload = str(ent.get("record_json", "") or "").strip() or "{}"
    try:
        record = json.loads(payload)
    except Exception:
        record = {}
    if not isinstance(record, dict):
        record = {}
    return {k: ("" if v is None else str(v)) for k, v in record.items()}


def delete_record(key: EntradaKey) -> None:
    c = _container()
    _with_timeout(lambda: c.delete_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)


def clear_partition(partition_key: str = DEFAULT_PARTITION) -> int:
    c = _container()
    deleted = 0
    for key in list_keys(partition_key=partition_key):
        try:
            c.delete_item(item=key.row_key, partition_key=key.partition_key)
            deleted += 1
        except Exception:
            continue
    return deleted


def try_acquire_lock(key: EntradaKey, *, owner: str, ttl_seconds: int = LOCK_TTL_SECONDS) -> Optional[tuple[str, datetime]]:
    owner = (owner or "").strip()
    if not owner:
        return None

    c = _container()
    now = _utcnow()
    try:
        ent = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return None

    current_owner = str(ent.get("lock_owner", "") or "")
    until = _parse_dt(str(ent.get("lock_until", "") or ""))
    is_free = (not current_owner) or (until is None) or (until <= now)
    if not is_free:
        return None

    token = uuid.uuid4().hex
    ent["lock_owner"] = owner
    ent["lock_token"] = token
    ent["lock_acquired_at"] = now.isoformat()
    until_dt = _lock_until(now, ttl_seconds)
    ent["lock_until"] = until_dt.isoformat()

    try:
        from azure.core import MatchConditions

        etag = str(ent.get("_etag", "") or "").strip()
        if etag:
            try:
                _with_timeout(
                    lambda: c.replace_item(
                        item=key.row_key,
                        body=ent,
                        partition_key=key.partition_key,
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    ),
                    timeout_s=20.0,
                )
            except Exception:
                # Fallback: re-lee y reintenta sin condición si sigue libre (evita "0 pendientes" por etag/condición).
                ent2 = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
                current_owner2 = str(ent2.get("lock_owner", "") or "")
                until2 = _parse_dt(str(ent2.get("lock_until", "") or ""))
                is_free2 = (not current_owner2) or (until2 is None) or (until2 <= now)
                if not is_free2:
                    return None
                ent2["lock_owner"] = owner
                ent2["lock_token"] = token
                ent2["lock_acquired_at"] = now.isoformat()
                ent2["lock_until"] = until_dt.isoformat()
                _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent2, partition_key=key.partition_key), timeout_s=20.0)
        else:
            _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return None

    return token, until_dt


def validate_lock(key: EntradaKey, *, owner: str, token: str) -> bool:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return False

    c = _container()
    now = _utcnow()
    try:
        ent = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return False

    if str(ent.get("lock_owner", "") or "") != owner:
        return False
    if str(ent.get("lock_token", "") or "") != token:
        return False
    until = _parse_dt(str(ent.get("lock_until", "") or ""))
    return bool(until and until > now)


def refresh_lock(key: EntradaKey, *, owner: str, token: str, ttl_seconds: int = LOCK_TTL_SECONDS) -> Optional[datetime]:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return None

    c = _container()
    now = _utcnow()
    try:
        ent = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return None

    if str(ent.get("lock_owner", "") or "") != owner:
        return None
    if str(ent.get("lock_token", "") or "") != token:
        return None
    until = _parse_dt(str(ent.get("lock_until", "") or ""))
    if until is None or until <= now:
        return None

    new_until = _lock_until(now, ttl_seconds)
    ent["lock_until"] = new_until.isoformat()

    try:
        from azure.core import MatchConditions

        etag = str(ent.get("_etag", "") or "").strip()
        if etag:
            try:
                _with_timeout(
                    lambda: c.replace_item(
                        item=key.row_key,
                        body=ent,
                        partition_key=key.partition_key,
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    ),
                    timeout_s=20.0,
                )
            except Exception:
                ent2 = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
                if str(ent2.get("lock_owner", "") or "") != owner:
                    return None
                if str(ent2.get("lock_token", "") or "") != token:
                    return None
                until2 = _parse_dt(str(ent2.get("lock_until", "") or ""))
                if until2 is None or until2 <= now:
                    return None
                ent2["lock_until"] = new_until.isoformat()
                _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent2, partition_key=key.partition_key), timeout_s=20.0)
        else:
            _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return None

    return new_until


def release_lock(key: EntradaKey, *, owner: str, token: str) -> bool:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return False

    c = _container()
    try:
        ent = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return False

    if str(ent.get("lock_owner", "") or "") != owner:
        return False
    if str(ent.get("lock_token", "") or "") != token:
        return False

    ent["lock_owner"] = ""
    ent["lock_token"] = ""
    ent["lock_until"] = ""
    ent["lock_acquired_at"] = ""

    try:
        from azure.core import MatchConditions

        etag = str(ent.get("_etag", "") or "").strip()
        if etag:
            try:
                _with_timeout(
                    lambda: c.replace_item(
                        item=key.row_key,
                        body=ent,
                        partition_key=key.partition_key,
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    ),
                    timeout_s=20.0,
                )
            except Exception:
                ent2 = _with_timeout(lambda: c.read_item(item=key.row_key, partition_key=key.partition_key), timeout_s=20.0)
                if str(ent2.get("lock_owner", "") or "") != owner:
                    return False
                if str(ent2.get("lock_token", "") or "") != token:
                    return False
                ent2["lock_owner"] = ""
                ent2["lock_token"] = ""
                ent2["lock_until"] = ""
                ent2["lock_acquired_at"] = ""
                _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent2, partition_key=key.partition_key), timeout_s=20.0)
        else:
            _with_timeout(lambda: c.replace_item(item=key.row_key, body=ent, partition_key=key.partition_key), timeout_s=20.0)
    except Exception:
        return False
    return True


def clear_expired_locks(partition_key: str = DEFAULT_PARTITION) -> int:
    c = _container()
    now = _utcnow()
    cleared = 0
    try:
        entities = _with_timeout(
            lambda: list(
                c.query_items(
                    query="SELECT * FROM c WHERE c.pk=@pk AND c.lock_owner != ''",
                    parameters=[{"name": "@pk", "value": str(partition_key)}],
                    enable_cross_partition_query=True,
                )
            ),
            timeout_s=30.0,
        )
    except Exception:
        return 0

    for ent in entities:
        until = _parse_dt(str(ent.get("lock_until", "") or ""))
        if until is not None and until > now:
            continue
        ent["lock_owner"] = ""
        ent["lock_token"] = ""
        ent["lock_until"] = ""
        ent["lock_acquired_at"] = ""
        try:
            from azure.core import MatchConditions

            item_id = str(ent.get("id", "") or "")
            pk = str(ent.get("pk", "") or str(partition_key))
            etag = str(ent.get("_etag", "") or "").strip()
            if etag:
                try:
                    _with_timeout(
                        lambda: c.replace_item(
                            item=item_id,
                            body=ent,
                            partition_key=pk,
                            etag=etag,
                            match_condition=MatchConditions.IfNotModified,
                        ),
                        timeout_s=20.0,
                    )
                except Exception:
                    ent2 = _with_timeout(lambda: c.read_item(item=item_id, partition_key=pk), timeout_s=20.0)
                    until2 = _parse_dt(str(ent2.get("lock_until", "") or ""))
                    if until2 is not None and until2 > now:
                        continue
                    ent2["lock_owner"] = ""
                    ent2["lock_token"] = ""
                    ent2["lock_until"] = ""
                    ent2["lock_acquired_at"] = ""
                    _with_timeout(lambda: c.replace_item(item=item_id, body=ent2, partition_key=pk), timeout_s=20.0)
            else:
                _with_timeout(lambda: c.replace_item(item=item_id, body=ent, partition_key=pk), timeout_s=20.0)
            cleared += 1
        except Exception:
            continue
    return cleared


def ingest_records(
    records: Iterable[Dict[str, Any]],
    *,
    partition_key: str = DEFAULT_PARTITION,
    source_blob: str = "",
    source_sheet: str = "",
) -> int:
    c = _container()
    now = _utcnow().isoformat()
    created = 0
    for rec in records:
        row_key = uuid.uuid4().hex
        record_id = str(rec.get("IdCorreo", "") or "")
        record_json = json.dumps(rec, ensure_ascii=False)
        entity: Dict[str, Any] = {
            "id": row_key,
            "pk": partition_key,
            "created_at": now,
            "record_id": record_id,
            "timestamp": str(rec.get("@timestamp", "") or ""),
            "automatismo": str(rec.get("Automatismo", "") or ""),
            "source_blob": source_blob,
            "source_sheet": source_sheet,
            "record_json": record_json,
            "lock_owner": "",
            "lock_token": "",
            "lock_until": "",
            "lock_acquired_at": "",
        }
        c.create_item(entity)
        created += 1
    return created
