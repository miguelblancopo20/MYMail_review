from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import config
from mymail.cosmos import container as cosmos_container
from mymail.cosmos import containers as cosmos_containers
from mymail.cosmos import cosmos_enabled


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_conn_str() -> str:
    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py")
    return conn


def _table():
    if cosmos_enabled():
        return cosmos_container(cosmos_containers().entrada)

    try:
        from azure.data.tables import TableServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-data-tables (pip install -r requirements.txt)") from exc

    service = TableServiceClient.from_connection_string(_require_conn_str())
    client = service.get_table_client(getattr(config, "TABLE_ENTRADA", "entrada"))
    try:
        client.create_table()
    except Exception:
        pass
    return client


DEFAULT_PARTITION = "active"
_MAX_TABLE_STRING_CHARS = 30000  # safe margin vs 32K UTF-16 limit
LOCK_TTL_SECONDS = 600


@dataclass(frozen=True)
class EntradaKey:
    partition_key: str
    row_key: str


def list_keys(partition_key: str = DEFAULT_PARTITION) -> List[EntradaKey]:
    client = _table()
    keys: List[EntradaKey] = []
    if cosmos_enabled():
        rows = client.query_items(
            query="SELECT c.pk, c.id FROM c WHERE c.pk=@pk",
            parameters=[{"name": "@pk", "value": partition_key}],
            enable_cross_partition_query=True,
        )
        for ent in rows:
            keys.append(EntradaKey(partition_key=str(ent.get("pk", "") or ""), row_key=str(ent.get("id", "") or "")))
        return keys

    filt = f"PartitionKey eq '{partition_key}'"
    for ent in client.query_entities(query_filter=filt, select=["PartitionKey", "RowKey"]):
        keys.append(EntradaKey(partition_key=str(ent["PartitionKey"]), row_key=str(ent["RowKey"])))
    return keys


def list_pending_meta(partition_key: str = DEFAULT_PARTITION, *, limit: int | None = None) -> List[Dict[str, str]]:
    client = _table()
    if cosmos_enabled():
        out: List[Dict[str, str]] = []
        rows = client.query_items(
            query="SELECT c.pk, c.id, c.record_id, c.timestamp, c.automatismo, c.lock_owner, c.lock_until FROM c WHERE c.pk=@pk",
            parameters=[{"name": "@pk", "value": partition_key}],
            enable_cross_partition_query=True,
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

    filt = f"PartitionKey eq '{partition_key}'"
    select = ["PartitionKey", "RowKey", "record_id", "timestamp", "automatismo", "lock_owner", "lock_until"]
    out: List[Dict[str, str]] = []
    for ent in client.query_entities(query_filter=filt, select=select):
        out.append(
            {
                "pk": str(ent.get("PartitionKey", "") or ""),
                "rk": str(ent.get("RowKey", "") or ""),
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
    """
    Lista entradas pendientes devolviendo payload (record_json/record_blob) sin hacer un get por registro.

    Se usa para estadísticas (evita N round-trips con get_record por cada key).
    """
    client = _table()
    out: List[Dict[str, str]] = []

    if cosmos_enabled():
        rows = client.query_items(
            query="SELECT c.pk, c.id, c.timestamp, c.record_json, c.record_blob FROM c WHERE c.pk=@pk",
            parameters=[{"name": "@pk", "value": partition_key}],
            enable_cross_partition_query=True,
        )
        for ent in rows:
            out.append(
                {
                    "pk": str(ent.get("pk", "") or ""),
                    "rk": str(ent.get("id", "") or ""),
                    "timestamp": str(ent.get("timestamp", "") or ""),
                    "record_json": str(ent.get("record_json", "") or ""),
                    "record_blob": str(ent.get("record_blob", "") or ""),
                }
            )
            if limit is not None and len(out) >= int(limit):
                break
        return out

    filt = f"PartitionKey eq '{partition_key}'"
    select = ["PartitionKey", "RowKey", "timestamp", "record_json", "record_blob"]
    for ent in client.query_entities(query_filter=filt, select=select):
        out.append(
            {
                "pk": str(ent.get("PartitionKey", "") or ""),
                "rk": str(ent.get("RowKey", "") or ""),
                "timestamp": str(ent.get("timestamp", "") or ""),
                "record_json": str(ent.get("record_json", "") or ""),
                "record_blob": str(ent.get("record_blob", "") or ""),
            }
        )
        if limit is not None and len(out) >= int(limit):
            break
    return out


def record_from_payload(*, record_json: str = "", record_blob: str = "") -> Dict[str, str]:
    payload = (record_json or "").strip()
    if not payload and (record_blob or "").strip():
        payload = _download_blob_text(str(record_blob))
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
    client = _table()
    if cosmos_enabled():
        ent = client.read_item(item=key.row_key, partition_key=key.partition_key)
        payload = ent.get("record_json") or ""
    else:
        ent = client.get_entity(partition_key=key.partition_key, row_key=key.row_key)
        payload = ent.get("record_json") or ""
    if not payload:
        blob_name = ent.get("record_blob") or ""
        if blob_name:
            payload = _download_blob_text(blob_name)
    if not payload:
        payload = "{}"
    try:
        record = json.loads(payload)
    except Exception:
        record = {}
    if not isinstance(record, dict):
        record = {}
    return {k: ("" if v is None else str(v)) for k, v in record.items()}


def delete_record(key: EntradaKey) -> None:
    client = _table()
    if cosmos_enabled():
        client.delete_item(item=key.row_key, partition_key=key.partition_key)
        return
    client.delete_entity(partition_key=key.partition_key, row_key=key.row_key)


def clear_partition(partition_key: str = DEFAULT_PARTITION) -> int:
    client = _table()
    deleted = 0
    for key in list_keys(partition_key=partition_key):
        try:
            if cosmos_enabled():
                client.delete_item(item=key.row_key, partition_key=key.partition_key)
            else:
                client.delete_entity(partition_key=key.partition_key, row_key=key.row_key)
            deleted += 1
        except Exception:
            continue
    return deleted


def _blob_container() -> str:
    return getattr(config, "AZURE_BLOB_CONTAINER_ENTRADA", "entrada")


def _blob_service():
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-storage-blob (pip install -r requirements.txt)") from exc
    return BlobServiceClient.from_connection_string(_require_conn_str())


def _ensure_container() -> None:
    service = _blob_service()
    container_client = service.get_container_client(_blob_container())
    try:
        container_client.create_container()
    except Exception:
        pass


def _upload_blob_text(blob_name: str, text: str) -> None:
    _ensure_container()
    service = _blob_service()
    blob_client = service.get_blob_client(container=_blob_container(), blob=blob_name)
    blob_client.upload_blob(text.encode("utf-8"), overwrite=True)


def _download_blob_text(blob_name: str) -> str:
    service = _blob_service()
    blob_client = service.get_blob_client(container=_blob_container(), blob=blob_name)
    data = blob_client.download_blob().readall()
    return data.decode("utf-8", errors="replace")


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


def try_acquire_lock(key: EntradaKey, *, owner: str, ttl_seconds: int = LOCK_TTL_SECONDS) -> Optional[tuple[str, datetime]]:
    owner = (owner or "").strip()
    if not owner:
        return None

    client = _table()
    now = _utcnow()

    try:
        if cosmos_enabled():
            ent = client.read_item(item=key.row_key, partition_key=key.partition_key)
        else:
            ent = client.get_entity(partition_key=key.partition_key, row_key=key.row_key)
    except Exception:
        return None

    current_owner = str(ent.get("lock_owner", "") or "")
    current_token = str(ent.get("lock_token", "") or "")
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

        if cosmos_enabled():
            etag = ent.get("_etag", "")
            client.replace_item(
                item=key.row_key,
                body=ent,
                partition_key=key.partition_key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        else:
            etag = ent.get("etag") or ent.get("odata.etag") or "*"
            client.update_entity(entity=ent, mode="merge", etag=etag, match_condition=MatchConditions.IfNotModified)
    except Exception as exc:
        if exc.__class__.__name__ in {"ResourceModifiedError", "HttpResponseError", "CosmosHttpResponseError"}:
            return None
        return None

    return token, until_dt


def validate_lock(key: EntradaKey, *, owner: str, token: str) -> bool:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return False

    client = _table()
    now = _utcnow()
    try:
        if cosmos_enabled():
            ent = client.read_item(item=key.row_key, partition_key=key.partition_key)
        else:
            ent = client.get_entity(partition_key=key.partition_key, row_key=key.row_key)
    except Exception:
        return False

    if str(ent.get("lock_owner", "") or "") != owner:
        return False
    if str(ent.get("lock_token", "") or "") != token:
        return False

    until = _parse_dt(str(ent.get("lock_until", "") or ""))
    if until is None or until <= now:
        return False
    return True


def refresh_lock(key: EntradaKey, *, owner: str, token: str, ttl_seconds: int = LOCK_TTL_SECONDS) -> Optional[datetime]:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return None

    client = _table()
    now = _utcnow()
    try:
        if cosmos_enabled():
            ent = client.read_item(item=key.row_key, partition_key=key.partition_key)
        else:
            ent = client.get_entity(partition_key=key.partition_key, row_key=key.row_key)
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
        if cosmos_enabled():
            etag = ent.get("_etag", "")
            client.replace_item(
                item=key.row_key,
                body=ent,
                partition_key=key.partition_key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        else:
            etag = ent.get("etag") or ent.get("odata.etag") or "*"
            client.update_entity(entity=ent, mode="merge", etag=etag, match_condition=MatchConditions.IfNotModified)
    except Exception:
        return None

    return new_until


def release_lock(key: EntradaKey, *, owner: str, token: str) -> bool:
    owner = (owner or "").strip()
    token = (token or "").strip()
    if not owner or not token:
        return False

    client = _table()
    try:
        if cosmos_enabled():
            ent = client.read_item(item=key.row_key, partition_key=key.partition_key)
        else:
            ent = client.get_entity(partition_key=key.partition_key, row_key=key.row_key)
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
        if cosmos_enabled():
            etag = ent.get("_etag", "")
            client.replace_item(
                item=key.row_key,
                body=ent,
                partition_key=key.partition_key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        else:
            etag = ent.get("etag") or ent.get("odata.etag") or "*"
            client.update_entity(entity=ent, mode="merge", etag=etag, match_condition=MatchConditions.IfNotModified)
    except Exception:
        return False

    return True


def clear_expired_locks(partition_key: str = DEFAULT_PARTITION) -> int:
    client = _table()
    now = _utcnow()
    cleared = 0

    if cosmos_enabled():
        try:
            entities = list(
                client.query_items(
                    query="SELECT * FROM c WHERE c.pk=@pk AND c.lock_owner != ''",
                    parameters=[{"name": "@pk", "value": partition_key}],
                    enable_cross_partition_query=True,
                )
            )
        except Exception:
            return 0
    else:
        filt = f"PartitionKey eq '{partition_key}' and lock_owner ne ''"
        select = ["PartitionKey", "RowKey", "lock_owner", "lock_token", "lock_until"]
        try:
            entities = list(client.query_entities(query_filter=filt, select=select))
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
            if cosmos_enabled():
                etag = ent.get("_etag", "")
                client.replace_item(
                    item=str(ent.get("id", "") or ""),
                    body=ent,
                    partition_key=str(ent.get("pk", "") or partition_key),
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            else:
                etag = ent.get("etag") or ent.get("odata.etag") or "*"
                client.update_entity(entity=ent, mode="merge", etag=etag, match_condition=MatchConditions.IfNotModified)
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
    client = _table()
    now = _utcnow().isoformat()
    created = 0

    for rec in records:
        row_key = uuid.uuid4().hex
        record_id = str(rec.get("IdCorreo", "") or "")
        record_json = json.dumps(rec, ensure_ascii=False)
        record_blob = ""
        if len(record_json) > _MAX_TABLE_STRING_CHARS:
            record_blob = f"records/{row_key}.json"
            _upload_blob_text(record_blob, record_json)
            record_json = ""
        if cosmos_enabled():
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
                "record_blob": record_blob,
                "lock_owner": "",
                "lock_token": "",
                "lock_until": "",
                "lock_acquired_at": "",
            }
            client.create_item(entity)
        else:
            entity = {
                "PartitionKey": partition_key,
                "RowKey": row_key,
                "created_at": now,
                "record_id": record_id,
                "timestamp": str(rec.get("@timestamp", "") or ""),
                "automatismo": str(rec.get("Automatismo", "") or ""),
                "source_blob": source_blob,
                "source_sheet": source_sheet,
                "record_json": record_json,
                "record_blob": record_blob,
            }
            client.create_entity(entity=entity)
        created += 1

    return created
