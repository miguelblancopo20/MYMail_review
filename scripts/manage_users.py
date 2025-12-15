from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.tables import create_user


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Gestión de usuarios (Azure Table Storage)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="Crear/actualizar usuario")
    add.add_argument("--username", required=True)
    add.add_argument("--password", required=True)

    args = parser.parse_args(argv)

    if args.cmd == "add":
        create_user(username=args.username, password=args.password)
        print(f"OK: usuario '{args.username}' creado/actualizado")
        return 0

    parser.error("Comando no soportado")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
