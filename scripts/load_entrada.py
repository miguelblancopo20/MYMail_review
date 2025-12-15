from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config
from mymail.entrada import DEFAULT_PARTITION, clear_partition, ingest_records


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def upload_blob(local_path: Path, *, blob_name: str) -> str:
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Falta instalar azure-storage-blob (pip install -r requirements.txt)") from exc

    conn = getattr(config, "AZURE_STORAGE_CONNECTION_STRING", "") or ""
    if not conn.strip():
        raise RuntimeError("Falta AZURE_STORAGE_CONNECTION_STRING en config.py")

    container = getattr(config, "AZURE_BLOB_CONTAINER_ENTRADA", "entrada")
    service = BlobServiceClient.from_connection_string(conn)
    container_client = service.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    with local_path.open("rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    return f"{container}/{blob_name}"


def load_excel_records(path: Path, *, sheet_name: str):
    import pandas as pd

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str).fillna("")
    for _, row in df.iterrows():
        yield row.to_dict()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Carga Excel a Azure (blob + tabla de entrada)")
    parser.add_argument("--excel", default="Validados_V3.xlsx", help="Ruta al Excel de entrada")
    parser.add_argument("--sheet", default="1 dic - 8 dic", help="Nombre de hoja")
    parser.add_argument("--partition", default=DEFAULT_PARTITION, help="PartitionKey de la tabla de entrada")
    parser.add_argument("--replace", action="store_true", help="Borra la partición antes de cargar")
    args = parser.parse_args(argv)

    excel_path = Path(args.excel)
    if not excel_path.exists():
        print(f"ERROR: No existe {excel_path}", file=sys.stderr)
        return 2

    blob_name = f"{excel_path.stem}_{_utc_stamp()}{excel_path.suffix}"
    blob_ref = upload_blob(excel_path, blob_name=blob_name)
    print(f"OK: subido a blob {blob_ref}")

    if args.replace:
        deleted = clear_partition(partition_key=args.partition)
        print(f"OK: limpiados {deleted} registros de la partición '{args.partition}'")

    created = ingest_records(
        load_excel_records(excel_path, sheet_name=args.sheet),
        partition_key=args.partition,
        source_blob=blob_ref,
        source_sheet=args.sheet,
    )
    print(f"OK: insertados {created} registros en tabla '{getattr(config,'TABLE_ENTRADA','entrada')}' (PartitionKey={args.partition})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

