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
from mymail.state import ReviewState


class FakeCosmosError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class FakeContainerMulti:
    def __init__(self, items: list[dict]):
        self._items = {(it["pk"], it["id"]): dict(it) for it in items}

    def read_item(self, *, item: str, partition_key: str):
        key = (partition_key, item)
        if key not in self._items:
            raise FakeCosmosError("not found", status_code=404)
        return dict(self._items[key])

    def replace_item(self, *, item: str, body: dict, etag=None, match_condition=None):
        key = (str(body.get("pk", "")), item)
        if key not in self._items:
            raise FakeCosmosError("not found", status_code=404)
        if etag and str(self._items[key].get("_etag", "")) != str(etag):
            raise FakeCosmosError("etag mismatch", status_code=412)
        self._items[key] = dict(body)
        return dict(self._items[key])


class IntegrationStateWithFakeCosmosTests(unittest.TestCase):
    def test_state_current_record_acquires_lock_and_reads_payload(self):
        now = datetime(2025, 12, 18, 12, 0, 0, tzinfo=timezone.utc)
        container = FakeContainerMulti(
            [
                {
                    "id": "rk1",
                    "pk": "active",
                    "_etag": "etag1",
                    "record_json": "{\"IdCorreo\":\"0003CaMK1G9B8KUW\",\"Question\":\"Hola\",\"MailToAgent\":\"{}\"}",
                    "lock_owner": "",
                    "lock_token": "",
                    "lock_until": "",
                    "lock_acquired_at": "",
                }
            ]
        )

        state = ReviewState(queue=[EntradaKey(partition_key="active", row_key="rk1")])

        with patch("mymail.entrada._container", return_value=container), patch("mymail.entrada._utcnow", return_value=now), patch(
            "mymail.entrada._with_timeout", side_effect=lambda fn, timeout_s=20.0: fn()
        ):
            rec = state.current_record(owner="u1")

        self.assertEqual(rec.get("IdCorreo"), "0003CaMK1G9B8KUW")
        self.assertEqual(state.current_key, EntradaKey(partition_key="active", row_key="rk1"))
        self.assertTrue(state.lock_token)


if __name__ == "__main__":
    unittest.main()
