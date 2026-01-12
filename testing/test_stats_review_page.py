from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class StatsReviewPageTests(unittest.TestCase):
    def test_stats_renders_for_admin(self):
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
            sess["role"] = "Administrador"

        with patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}):
            resp = client.get("/stats")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"Estad", resp.data)
        self.assertIn(b"MY Review", resp.data)
        self.assertIn(b'name=\"week\"', resp.data)


if __name__ == "__main__":
    unittest.main()

