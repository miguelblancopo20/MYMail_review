from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import config


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_conn_str() -> str:
    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py")
    return conn


def _table():
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


@dataclass(frozen=True)
class EntradaKey:
    partition_key: str
    row_key: str


def list_keys(partition_key: str = DEFAULT_PARTITION) -> List[EntradaKey]:
    client = _table()
    keys: List[EntradaKey] = []
    filt = f"PartitionKey eq '{partition_key}'"
    for ent in client.query_entities(query_filter=filt, select=["PartitionKey", "RowKey"]):
        keys.append(EntradaKey(partition_key=str(ent["PartitionKey"]), row_key=str(ent["RowKey"])))
    return keys


def get_record(key: EntradaKey) -> Dict[str, str]:
    client = _table()
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
    client.delete_entity(partition_key=key.partition_key, row_key=key.row_key)


def clear_partition(partition_key: str = DEFAULT_PARTITION) -> int:
    client = _table()
    deleted = 0
    for key in list_keys(partition_key=partition_key):
        try:
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
        entity: Dict[str, Any] = {
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
