from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.entrada import EntradaKey


class FakeState:
    def __init__(self):
        self.current_key = EntradaKey(partition_key="active", row_key="rk1")
        self.lock_token = "tok"

    def pending_count(self) -> int:
        return 1

    def current_record(self, *, owner: str):
        return {
            "IdCorreo": "0003CaMK1G9B8KUW",
            "@timestamp": "2025-12-04T12:47:12.344Z",
            "Automatismo": "Tramitar servicios",
            "Question": "Hola",
            "MailToAgent": "{}",
        }


class AcceptanceReviewPageTests(unittest.TestCase):
    def test_review_renders_record_and_sets_session_lock(self):
        try:
            import flask_app
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"Flask no disponible para test de aceptación: {exc}")

        app = flask_app.create_app()
        app.testing = True

        fixed_until = datetime(2025, 12, 18, 12, 10, 0, tzinfo=timezone.utc)

        with patch.object(flask_app, "get_state", return_value=FakeState()), patch.object(
            flask_app, "refresh_lock", return_value=fixed_until
        ):
            client = app.test_client()
            with client.session_transaction() as sess:
                sess["authenticated"] = True
                sess["user"] = "u1"
                sess["role"] = "Administrador"

            resp = client.get("/review")

            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"0003CaMK1G9B8KUW", resp.data)

            with client.session_transaction() as sess:
                self.assertIn("_lock", sess)
                self.assertEqual(sess["_lock"]["token"], "tok")
                self.assertIn("_lock_until_ms", sess)


if __name__ == "__main__":
    unittest.main()
