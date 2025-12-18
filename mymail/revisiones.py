from __future__ import annotations

import uuid
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from mymail.cosmos import container as cosmos_container
from mymail.cosmos import containers as cosmos_containers


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _results_container():
    return cosmos_container(cosmos_containers().resultados)


def _split_key(blob_name: str) -> tuple[str, str]:
    blob_name = str(blob_name or "").strip()
    if not blob_name:
        raise ValueError("Falta id de revisión")
    if "|" in blob_name:
        pk, id_ = blob_name.split("|", 1)
        pk = pk.strip()
        id_ = id_.strip()
        if pk and id_:
            return pk, id_
    return "", blob_name


def _record_from_json(record_json: str) -> dict[str, Any]:
    raw = str(record_json or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def save_revision(blob_name: str, payload: Dict[str, Any]) -> None:
    pk, id_ = _split_key(blob_name)
    if not pk:
        raise ValueError("Falta partition key en el id (esperado: '<pk>|<id>').")

    c = _results_container()
    current = c.read_item(item=id_, partition_key=pk)
    etag = str((current or {}).get("_etag", "") or "")

    doc = dict(payload or {})
    doc.pop("_blob_name", None)

    # Normaliza record -> record_json (resultados almacena string JSON).
    if isinstance(doc.get("record"), dict):
        doc["record_json"] = json.dumps(doc["record"], ensure_ascii=False)
        doc.pop("record", None)

    # Asegura claves esenciales
    doc["id"] = id_
    doc["pk"] = pk

    from azure.core import MatchConditions

    c.replace_item(
        item=id_,
        body=doc,
        partition_key=pk,
        etag=etag,
        match_condition=MatchConditions.IfNotModified,
    )


def list_revisions(*, username: str = "", limit: int = 500) -> list[dict[str, Any]]:
    c = _results_container()
    limit_i = max(1, int(limit))

    username = (username or "").strip()
    if username:
        rows = c.query_items(
            query=f"SELECT * FROM c WHERE c.user=@u ORDER BY c.timestamp DESC OFFSET 0 LIMIT {limit_i}",
            parameters=[{"name": "@u", "value": username}],
            enable_cross_partition_query=True,
        )
    else:
        rows = c.query_items(
            query=f"SELECT * FROM c ORDER BY c.timestamp DESC OFFSET 0 LIMIT {limit_i}",
            parameters=[],
            enable_cross_partition_query=True,
        )

    out: list[dict[str, Any]] = []
    for ent in rows:
        if not isinstance(ent, dict):
            continue
        ent = dict(ent)
        pk = str(ent.get("pk", "") or "").strip()
        id_ = str(ent.get("id", "") or "").strip()
        ent["record"] = _record_from_json(str(ent.get("record_json", "") or ""))
        ent["_blob_name"] = f"{pk}|{id_}" if pk and id_ else id_
        out.append(ent)
    return out


def get_revision(blob_name: str) -> dict[str, Any]:
    pk, id_ = _split_key(blob_name)
    c = _results_container()
    if pk:
        ent = c.read_item(item=id_, partition_key=pk)
    else:
        # Fallback: buscar por id en cross-partition
        rows = list(
            c.query_items(
                query="SELECT * FROM c WHERE c.id=@id",
                parameters=[{"name": "@id", "value": id_}],
                enable_cross_partition_query=True,
            )
        )
        if not rows:
            raise ValueError("Revisión no encontrada.")
        ent = rows[0]
    if not isinstance(ent, dict):
        raise ValueError("Revisión inválida")
    ent = dict(ent)
    pk2 = str(ent.get("pk", "") or "").strip()
    id2 = str(ent.get("id", "") or "").strip()
    ent["record"] = _record_from_json(str(ent.get("record_json", "") or ""))
    ent["_blob_name"] = f"{pk2}|{id2}" if pk2 and id2 else (blob_name or id2)
    return ent
