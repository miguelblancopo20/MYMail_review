from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import config


@dataclass(frozen=True)
class CosmosContainers:
    users: str
    logs: str
    resultados: str
    descartes: str
    entrada: str


def cosmos_enabled() -> bool:
    endpoint = (getattr(config, "COSMOS_ENDPOINT", "") or "").strip()
    key = (getattr(config, "COSMOS_KEY", "") or "").strip()
    return bool(endpoint and key)


def containers() -> CosmosContainers:
    return CosmosContainers(
        users=str(getattr(config, "COSMOS_CONTAINER_USERS", "users") or "users"),
        logs=str(getattr(config, "COSMOS_CONTAINER_LOGS", "logs") or "logs"),
        resultados=str(getattr(config, "COSMOS_CONTAINER_RESULTADOS", "resultados") or "resultados"),
        descartes=str(getattr(config, "COSMOS_CONTAINER_DESCARTES", "descartes") or "descartes"),
        entrada=str(getattr(config, "COSMOS_CONTAINER_ENTRADA", "entrada") or "entrada"),
    )


def _require_endpoint() -> str:
    endpoint = (getattr(config, "COSMOS_ENDPOINT", "") or "").strip()
    if not endpoint:
        raise RuntimeError("Falta COSMOS_ENDPOINT en config.py/.env")
    return endpoint


def _require_key() -> str:
    key = (getattr(config, "COSMOS_KEY", "") or "").strip()
    if not key:
        raise RuntimeError("Falta COSMOS_KEY en config.py/.env")
    return key


def _require_db() -> str:
    db = (getattr(config, "COSMOS_DATABASE", "") or "").strip()
    if not db:
        raise RuntimeError("Falta COSMOS_DATABASE en config.py/.env")
    return db


_CLIENT = None
_DB = None
_CONTAINERS: Dict[str, Any] = {}


def client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    try:
        from azure.cosmos import CosmosClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-cosmos (pip install -r requirements.txt)") from exc
    # Evita "cargas infinitas" si hay problemas de red o Cosmos está degradado.
    # Estos timeouts fuerzan a que falle rápido y podamos mostrar un error en la UI.
    _CLIENT = CosmosClient(
        _require_endpoint(),
        credential=_require_key(),
        connection_timeout=5,
        request_timeout=20,
    )
    return _CLIENT


def database():
    global _DB
    if _DB is not None:
        return _DB
    cli = client()
    _DB = cli.get_database_client(_require_db())
    return _DB


def container(name: str):
    name = str(name or "").strip()
    if not name:
        raise ValueError("container name vacío")
    if name in _CONTAINERS:
        return _CONTAINERS[name]
    c = database().get_container_client(name)
    _CONTAINERS[name] = c
    return c


def ensure_resources() -> None:
    """
    Crea DB y contenedores si no existen.
    PartitionKey path esperado:
    - users/logs/resultados/descartes/entrada: /pk
    """
    from azure.cosmos import PartitionKey
    from azure.cosmos.exceptions import CosmosResourceExistsError

    cli = client()
    db_name = _require_db()
    try:
        cli.create_database(db_name)
    except CosmosResourceExistsError:
        pass

    db = cli.get_database_client(db_name)
    for name in containers().__dict__.values():
        try:
            db.create_container(id=name, partition_key=PartitionKey(path="/pk"))
        except CosmosResourceExistsError:
            continue
