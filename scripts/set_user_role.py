from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.tables import ROLE_ADMIN, ROLE_REVISOR, set_user_role


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Cambiar rol a un usuario")
    parser.add_argument("--username", required=True)
    parser.add_argument("--role", required=True, choices=[ROLE_REVISOR, ROLE_ADMIN])
    args = parser.parse_args(argv)

    set_user_role(username=args.username, role=args.role)
    role = ROLE_ADMIN if args.username.lower() == "admin" else args.role
    print(f"OK: rol de '{args.username}' actualizado a {role}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

