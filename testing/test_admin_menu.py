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

        with patch.object(
            flask_app,
            "list_users",
            return_value=[{"username": "u1", "role": "Administrador", "active": "1", "created_at": "2025-12-20T10:00:00Z"}],
        ):
            client = app.test_client()
            with client.session_transaction() as sess:
                sess["authenticated"] = True
                sess["user"] = "admin"
                sess["role"] = "Administrador"

            resp = client.get("/admin")
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"Administrador", resp.data)
            self.assertIn(b"Usuarios", resp.data)
            self.assertIn(b"Alta", resp.data)

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

    def test_admin_cannot_change_other_user_password_via_admin_post(self):
        try:
            import flask_app
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"Flask no disponible para test: {exc}")

        app = flask_app.create_app()
        app.testing = True
        client = app.test_client()
        with client.session_transaction() as sess:
            sess["authenticated"] = True
            sess["user"] = "admin"
            sess["role"] = "Administrador"

        with patch.object(flask_app, "set_user_password") as sp, patch.object(flask_app, "set_user_role") as sr:
            resp = client.post(
                "/admin/users",
                data={"action": "set_password", "username": "u2", "password": "newpass123"},
                follow_redirects=False,
            )
            self.assertEqual(resp.status_code, 302)
            sp.assert_not_called()
            sr.assert_not_called()


class AccountPasswordTests(unittest.TestCase):
    def test_account_password_changes_only_self(self):
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

        class Ok:
            ok = True
            reason = ""

        with patch.object(flask_app, "verify_user", return_value=Ok()), patch.object(flask_app, "set_user_password") as sp:
            resp = client.post(
                "/account/password",
                data={"current_password": "old", "new_password": "newpass123", "confirm_password": "newpass123"},
                follow_redirects=False,
            )
            self.assertEqual(resp.status_code, 302)
            sp.assert_called_once()
            self.assertEqual(sp.call_args.kwargs.get("username"), "u1")


if __name__ == "__main__":
    unittest.main()
