from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import config


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_conn_str() -> str:
    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py")
    return conn


def _container_name() -> str:
    return getattr(config, "AZURE_BLOB_CONTAINER_REVISIONES", "revisiones")


def _service():
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-storage-blob (pip install -r requirements.txt)") from exc
    return BlobServiceClient.from_connection_string(_require_conn_str())


def _ensure_container() -> None:
    service = _service()
    client = service.get_container_client(_container_name())
    try:
        client.create_container()
    except Exception:
        pass


def upload_revision(payload: Dict[str, Any], *, username: str, reviewed_at: Optional[datetime] = None) -> str:
    _ensure_container()
    service = _service()
    container = service.get_container_client(_container_name())

    reviewed_at = reviewed_at or _utcnow()
    ts = reviewed_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_user = (username or "unknown").strip() or "unknown"
    blob_name = f"revisiones/{safe_user}/{ts}_{uuid.uuid4().hex}.json"
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    container.upload_blob(name=blob_name, data=data, overwrite=True)
    return blob_name


def list_revisions(*, username: str = "", limit: int = 500) -> list[dict[str, Any]]:
    service = _service()
    container = service.get_container_client(_container_name())
    prefix = "revisiones/"
    if username:
        prefix = f"revisiones/{username.strip()}/"

    blobs = list(container.list_blobs(name_starts_with=prefix))
    blobs.sort(key=lambda b: str(getattr(b, "name", "") or ""), reverse=True)
    blobs = blobs[: max(1, int(limit))]

    out: list[dict[str, Any]] = []
    for b in blobs:
        name = str(getattr(b, "name", "") or "")
        try:
            data = container.download_blob(name).readall().decode("utf-8", errors="replace")
            obj = json.loads(data)
            if not isinstance(obj, dict):
                continue
            obj["_blob_name"] = name
            out.append(obj)
        except Exception:
            continue
    return out


def get_revision(blob_name: str) -> dict[str, Any]:
    blob_name = str(blob_name or "").strip()
    if not blob_name:
        raise ValueError("Falta blob_name")
    service = _service()
    container = service.get_container_client(_container_name())
    data = container.download_blob(blob_name).readall().decode("utf-8", errors="replace")
    obj = json.loads(data)
    if not isinstance(obj, dict):
        raise ValueError("Revision inválida")
    obj["_blob_name"] = blob_name
    return obj
