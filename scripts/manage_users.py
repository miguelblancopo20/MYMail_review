from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.tables import ROLE_ADMIN, ROLE_REVISOR, ROLE_SUPERADMIN, create_user, list_users, set_user_password, set_user_role


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Gestión de usuarios (Azure Table Storage)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="Crear/actualizar usuario")
    add.add_argument("--username", required=True)
    add.add_argument("--password", required=True)
    add.add_argument("--role", default=ROLE_REVISOR, choices=[ROLE_REVISOR, ROLE_ADMIN, ROLE_SUPERADMIN])

    setrole = sub.add_parser("set-role", help="Cambiar rol a un usuario")
    setrole.add_argument("--username", required=True)
    setrole.add_argument("--role", required=True, choices=[ROLE_REVISOR, ROLE_ADMIN, ROLE_SUPERADMIN])

    setpwd = sub.add_parser("set-password", help="Actualizar contraseña de un usuario")
    setpwd.add_argument("--username", required=True)
    setpwd.add_argument("--password", required=True)

    sub.add_parser("list", help="Listar usuarios")

    args = parser.parse_args(argv)

    if args.cmd == "add":
        create_user(username=args.username, password=args.password, role=args.role)
        role = ROLE_ADMIN if args.username.lower() == "admin" else args.role
        print(f"OK: usuario '{args.username}' creado/actualizado (rol={role})")
        return 0

    if args.cmd == "set-role":
        set_user_role(username=args.username, role=args.role)
        role = ROLE_ADMIN if args.username.lower() == "admin" else args.role
        print(f"OK: rol de '{args.username}' actualizado a {role}")
        return 0

    if args.cmd == "set-password":
        set_user_password(username=args.username, password=args.password)
        print(f"OK: contraseña actualizada para '{args.username}'")
        return 0

    if args.cmd == "list":
        users = list_users()
        if not users:
            print("No hay usuarios o no se pudo leer la tabla.")
            return 0
        for u in users:
            print(f"{u.get('username','')}\t{u.get('role','')}\tactive={u.get('active','')}")
        return 0

    parser.error("Comando no soportado")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
