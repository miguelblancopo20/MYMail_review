from __future__ import annotations

from typing import Iterable

import config


def _require_connection_string() -> str:
    cs = (getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or "").strip()
    if not cs:
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py/.env")
    return cs


def _container_name_loads() -> str:
    return str(getattr(config, "BLOB_CONTAINER_LOADS", "loads") or "loads")


def _service():
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-storage-blob (pip install -r requirements.txt)") from exc
    return BlobServiceClient.from_connection_string(_require_connection_string())


def _container_client(container_name: str):
    cli = _service()
    c = cli.get_container_client(container_name)
    try:
        c.create_container()
    except Exception:
        pass
    return c


def upload_load_blob(*, week_key: str, logical_name: str, file_path: str, content_type: str = "text/csv") -> str:
    week_key = (week_key or "").strip()
    logical_name = (logical_name or "").strip()
    if not week_key:
        raise ValueError("week_key vacío")
    if not logical_name:
        raise ValueError("logical_name vacío")

    blob_name = f"{week_key}/{logical_name}.csv"
    c = _container_client(_container_name_loads())
    with open(file_path, "rb") as f:
        c.upload_blob(name=blob_name, data=f, overwrite=False, content_type=content_type or "text/csv")
    return blob_name


def delete_load_blobs_by_prefix(*, week_key: str) -> int:
    week_key = (week_key or "").strip()
    if not week_key:
        raise ValueError("week_key vacío")
    prefix = f"{week_key}/"
    c = _container_client(_container_name_loads())
    deleted = 0
    for b in c.list_blobs(name_starts_with=prefix):
        try:
            c.delete_blob(b.name)
            deleted += 1
        except Exception:
            continue
    return deleted


def delete_load_blobs(*, blob_names: Iterable[str]) -> int:
    c = _container_client(_container_name_loads())
    deleted = 0
    for name in blob_names:
        n = str(name or "").strip()
        if not n:
            continue
        try:
            c.delete_blob(n)
            deleted += 1
        except Exception:
            continue
    return deleted

