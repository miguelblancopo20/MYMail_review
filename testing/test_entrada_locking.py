from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.entrada import EntradaKey, try_acquire_lock


class FakeCosmosError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class FakeContainer:
    def __init__(self, item: dict):
        self._item = dict(item)
        self.replaces: list[tuple[str | None, object | None]] = []

    def read_item(self, *, item: str, partition_key: str):
        if item != self._item.get("id") or partition_key != self._item.get("pk"):
            raise FakeCosmosError("not found", status_code=404)
        return dict(self._item)

    def replace_item(self, *, item: str, body: dict, etag=None, match_condition=None):
        if item != self._item.get("id"):
            raise FakeCosmosError("not found", status_code=404)
        if etag and str(self._item.get("_etag", "")) != str(etag):
            raise FakeCosmosError("etag mismatch", status_code=412)
        self._item = dict(body)
        if "_etag" in body:
            self._item["_etag"] = body["_etag"]
        self.replaces.append((etag, match_condition))
        return dict(self._item)


class EntradaLockingTests(unittest.TestCase):
    def test_try_acquire_lock_acquires_when_free_without_azure_core(self):
        now = datetime(2025, 12, 18, 12, 0, 0, tzinfo=timezone.utc)
        item = {
            "id": "rk1",
            "pk": "active",
            "_etag": "etag1",
            "lock_owner": "",
            "lock_token": "",
            "lock_until": "",
            "lock_acquired_at": "",
            "record_json": "{}",
        }
        fake = FakeContainer(item)

        with patch("mymail.entrada._container", return_value=fake), patch("mymail.entrada._utcnow", return_value=now), patch(
            "mymail.entrada._with_timeout", side_effect=lambda fn, timeout_s=20.0: fn()
        ):
            token_until = try_acquire_lock(EntradaKey(partition_key="active", row_key="rk1"), owner="u1", ttl_seconds=600)

        self.assertIsNotNone(token_until)
        token, until = token_until  # type: ignore[misc]
        self.assertTrue(token)
        self.assertGreater(until, now)
        self.assertEqual(fake._item.get("lock_owner"), "u1")
        self.assertEqual(fake._item.get("lock_token"), token)
        self.assertTrue(str(fake._item.get("lock_until", "")).startswith("2025-12-18T12:"))

    def test_try_acquire_lock_returns_none_when_already_locked(self):
        now = datetime(2025, 12, 18, 12, 0, 0, tzinfo=timezone.utc)
        item = {
            "id": "rk1",
            "pk": "active",
            "_etag": "etag1",
            "lock_owner": "other",
            "lock_token": "t",
            "lock_until": (now + timedelta(minutes=5)).isoformat(),
            "lock_acquired_at": now.isoformat(),
            "record_json": "{}",
        }
        fake = FakeContainer(item)

        with patch("mymail.entrada._container", return_value=fake), patch("mymail.entrada._utcnow", return_value=now), patch(
            "mymail.entrada._with_timeout", side_effect=lambda fn, timeout_s=20.0: fn()
        ):
            out = try_acquire_lock(EntradaKey(partition_key="active", row_key="rk1"), owner="u1", ttl_seconds=600)

        self.assertIsNone(out)
        self.assertEqual(fake.replaces, [])


if __name__ == "__main__":
    unittest.main()
