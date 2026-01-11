from __future__ import annotations

import os
import sys
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class LoadPageTests(unittest.TestCase):
    def test_load_get_renders_for_admin(self):
        try:
            import flask_app
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"Flask no disponible para test: {exc}")

        app = flask_app.create_app()
        app.testing = True

        latest = {
            "id": "b1",
            "pk": "loads",
            "timestamp": "2025-12-21T10:00:00Z",
            "user": "admin",
            "files": {"ia": {"filename": "ia.csv", "size": 1, "sha256": "x"}},
        }
        with (
            patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}),
            patch.object(flask_app, "get_latest_load_batch", return_value=latest),
            patch.object(flask_app, "get_load_batch_by_week", return_value=None),
        ):
            client = app.test_client()
            with client.session_transaction() as sess:
                sess["authenticated"] = True
                sess["user"] = "u1"
                sess["role"] = "Administrador"

            resp = client.get("/load")
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"Subir ficheros", resp.data)
            self.assertIn(b"carga", resp.data.lower())

    def test_load_get_redirects_for_non_admin(self):
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

        resp = client.get("/load", follow_redirects=False)
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/menu", resp.headers.get("Location", ""))

    def test_load_post_requires_all_files(self):
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
            sess["_csrf_token"] = "tok"

        with (
            patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}),
            patch.object(flask_app, "get_load_batch_by_week", return_value=None),
            patch.object(flask_app, "save_load_batch") as save,
        ):
            resp = client.post("/load", data={"csrf_token": "tok", "week_key": "2026-W01"}, follow_redirects=False)
            self.assertEqual(resp.status_code, 302)
            save.assert_not_called()

    def test_load_post_validates_filename(self):
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
            sess["_csrf_token"] = "tok"

        def make_data(*, ia_filename: str) -> dict:
            return {
                "csrf_token": "tok",
                "week_key": "2026-W01",
                "fichas_levantadas": (BytesIO(b"x"), "fichas_levantadas.csv"),
                "ia": (BytesIO(b"x"), ia_filename),
                "ia-transacciones": (BytesIO(b"x"), "ia-transacciones.csv"),
                "orquestador_contexto": (BytesIO(b"x"), "orquestador_contexto.csv"),
                "rpa": (BytesIO(b"x"), "rpa.csv"),
                "validaciones": (BytesIO(b"x"), "VALIDACIONES.csv"),
            }

        with (
            patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}),
            patch.object(flask_app, "get_load_batch_by_week", return_value=None),
            patch.object(flask_app, "upload_load_blob", side_effect=lambda **kwargs: f"{kwargs['week_key']}/{kwargs['logical_name']}.csv") as upl,
            patch.object(flask_app, "save_load_batch", return_value="b1") as save,
        ):
            resp = client.post("/load", data=make_data(ia_filename="ia.csv"), follow_redirects=False, content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 302)
            save.assert_called_once()
            self.assertTrue(upl.called)

        with (
            patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}),
            patch.object(flask_app, "get_load_batch_by_week", return_value=None),
            patch.object(flask_app, "upload_load_blob") as upl2,
            patch.object(flask_app, "save_load_batch") as save2,
        ):
            resp2 = client.post(
                "/load",
                data=make_data(ia_filename="ia_mal.csv"),
                follow_redirects=False,
                content_type="multipart/form-data",
            )
            self.assertEqual(resp2.status_code, 302)
            save2.assert_not_called()
            upl2.assert_not_called()

    def test_load_post_blocks_when_week_exists(self):
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
            sess["_csrf_token"] = "tok"

        data = {
            "csrf_token": "tok",
            "week_key": "2026-W01",
            "fichas_levantadas": (BytesIO(b"x"), "fichas_levantadas.csv"),
            "ia": (BytesIO(b"x"), "ia.csv"),
            "ia-transacciones": (BytesIO(b"x"), "ia-transacciones.csv"),
            "orquestador_contexto": (BytesIO(b"x"), "orquestador_contexto.csv"),
            "rpa": (BytesIO(b"x"), "rpa.csv"),
            "validaciones": (BytesIO(b"x"), "validaciones.csv"),
        }

        with (
            patch.dict(os.environ, {"MYMAIL_LOAD_TODAY": "2026-01-10"}),
            patch.object(flask_app, "get_load_batch_by_week", return_value={"id": "b1"}),
            patch.object(flask_app, "upload_load_blob") as upl,
            patch.object(flask_app, "save_load_batch") as save,
        ):
            resp = client.post("/load", data=data, follow_redirects=False, content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 302)
            save.assert_not_called()
            upl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
