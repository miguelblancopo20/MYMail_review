from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from mymail.cosmos import container as cosmos_container
from mymail.cosmos import cosmos_enabled
from mymail.cosmos import containers as cosmos_containers


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def save_load_batch(*, username: str, week_key: str, week_start: str, files: Iterable[Dict[str, Any]]) -> str:
    """
    Guarda un "batch" de ficheros (metadata) en el contenedor `loads`.
    Cada elemento de `files` debe incluir: key, filename, mimetype, size, sha256, blob_name.
    """
    if not cosmos_enabled():
        raise RuntimeError("CosmosDB no disponible.")

    week_key = (week_key or "").strip()
    week_start = (week_start or "").strip()
    if not week_key:
        raise ValueError("week_key vacío")
    if not week_start:
        raise ValueError("week_start vacío")

    now = _utcnow()
    batch_id = uuid.uuid4().hex
    out_files: Dict[str, Any] = {}

    for f in files:
        key = str(f.get("key", "") or "").strip()
        filename = str(f.get("filename", "") or "").strip()
        mimetype = str(f.get("mimetype", "") or "").strip()
        blob_name = str(f.get("blob_name", "") or "").strip()
        try:
            size = int(f.get("size", 0) or 0)
        except Exception:
            size = 0
        sha256 = str(f.get("sha256", "") or "").strip()

        if not key:
            raise ValueError("file key vacío")
        if not filename:
            raise ValueError(f"filename vacío para '{key}'")
        if not blob_name:
            raise ValueError(f"blob_name vacío para '{key}'")

        out_files[key] = {
            "filename": filename,
            "mimetype": mimetype,
            "size": size,
            "sha256": sha256,
            "blob_name": blob_name,
        }

    c = cosmos_container(cosmos_containers().loads)
    c.create_item(
        {
            "id": batch_id,
            "pk": "loads",
            "timestamp": now.isoformat(),
            "user": username or "",
            "week_key": week_key,
            "week_start": week_start,
            "files": out_files,
        }
    )
    return batch_id


def get_load_batch_by_week(week_key: str) -> Optional[Dict[str, Any]]:
    week_key = (week_key or "").strip()
    if not week_key:
        return None
    if not cosmos_enabled():
        return None
    c = cosmos_container(cosmos_containers().loads)
    rows = list(
        c.query_items(
            query="SELECT TOP 1 * FROM c WHERE c.pk=@pk AND c.week_key=@wk ORDER BY c.timestamp DESC",
            parameters=[{"name": "@pk", "value": "loads"}, {"name": "@wk", "value": week_key}],
            enable_cross_partition_query=False,
        )
    )
    if not rows:
        return None
    ent = rows[0]
    return ent if isinstance(ent, dict) else None


def get_latest_load_batch() -> Optional[Dict[str, Any]]:
    if not cosmos_enabled():
        return None
    c = cosmos_container(cosmos_containers().loads)
    rows = list(
        c.query_items(
            query="SELECT TOP 1 * FROM c WHERE c.pk=@pk ORDER BY c.timestamp DESC",
            parameters=[{"name": "@pk", "value": "loads"}],
            enable_cross_partition_query=False,
        )
    )
    if not rows:
        return None
    ent = rows[0]
    return ent if isinstance(ent, dict) else None

