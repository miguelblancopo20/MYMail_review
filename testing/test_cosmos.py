from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from mymail.cosmos import containers as cosmos_containers
from mymail.cosmos import ensure_resources as cosmos_ensure_resources


def _require(name: str) -> str:
    value = (getattr(config, name, "") or "").strip()
    if not value:
        raise RuntimeError(f"Falta {name} en config.py/.env")
    return value


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Test de conexión a Azure Cosmos DB (SQL API)")
    parser.add_argument("--ensure", action="store_true", help="Crea DB/containers si no existen (usa /pk).")
    parser.add_argument("--database", default="", help="Sobrescribe COSMOS_DATABASE")
    args = parser.parse_args(argv)

    endpoint = _require("COSMOS_ENDPOINT")
    key = _require("COSMOS_KEY")
    db_name = (args.database or (getattr(config, "COSMOS_DATABASE", "") or "")).strip() or "mymailreview"

    try:
        from azure.cosmos import CosmosClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-cosmos (pip install -r requirements.txt)") from exc

    if args.ensure:
        cosmos_ensure_resources()

    client = CosmosClient(endpoint, credential=key)
    db = client.get_database_client(db_name)
    try:
        _ = db.read()
    except Exception as exc:
        print(f"ERROR: no se pudo leer la DB {db_name!r}: {exc}")
        return 2

    names = cosmos_containers()
    container_names = [names.users, names.logs, names.resultados, names.descartes, names.entrada]
    checked = 0
    for cname in container_names:
        c = db.get_container_client(cname)
        try:
            _ = c.read()
            _ = list(
                c.query_items(
                    query="SELECT TOP 1 c.id FROM c",
                    enable_cross_partition_query=True,
                )
            )
            checked += 1
        except Exception as exc:
            print(f"ERROR: no se pudo acceder al container {cname!r}: {exc}")
            return 3

    print(f"OK (db={db_name}, containers={checked})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
