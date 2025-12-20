from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class AdminMenuTests(unittest.TestCase):
    def test_admin_menu_renders_for_admin(self):
        try:
            import flask_app
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"Flask no disponible para test: {exc}")

        app = flask_app.create_app()
        app.testing = True

        with patch.object(flask_app, "list_users", return_value=[{"username": "u1", "role": "Administrador", "active": "1"}]):
            client = app.test_client()
            with client.session_transaction() as sess:
                sess["authenticated"] = True
                sess["user"] = "admin"
                sess["role"] = "Administrador"

            resp = client.get("/admin")
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"Administrador", resp.data)
            self.assertIn(b"Usuarios", resp.data)

    def test_admin_menu_redirects_for_non_admin(self):
        try:
            import flask_app
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"Flask no disponible para test: {exc}")

        app = flask_app.create_app()
        app.testing = True
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["authenticated"] = True
            sess["user"] = "u1"
            sess["role"] = "Revisor"

        resp = client.get("/admin", follow_redirects=False)
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/menu", resp.headers.get("Location", ""))


if __name__ == "__main__":
    unittest.main()

