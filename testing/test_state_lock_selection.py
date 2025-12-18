from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mymail.entrada import EntradaKey
from mymail.state import ReviewState


class ReviewStateLockSelectionTests(unittest.TestCase):
    def test_current_record_returns_record_when_lock_acquired(self):
        now = datetime(2025, 12, 18, 12, 0, 0, tzinfo=timezone.utc)
        state = ReviewState(queue=[EntradaKey(partition_key="active", row_key="rk1")])

        with patch("mymail.state.try_acquire_lock", return_value=("tok", now + timedelta(minutes=10))), patch(
            "mymail.state.get_record", return_value={"IdCorreo": "X", "Question": "Q", "MailToAgent": "{}"}
        ):
            rec = state.current_record(owner="u1")

        self.assertEqual(rec.get("IdCorreo"), "X")
        self.assertEqual(state.lock_token, "tok")
        self.assertIsNotNone(state.current_key)

    def test_current_record_times_out_after_25s_of_no_lock(self):
        state = ReviewState(queue=[])
        state._next_key = lambda: EntradaKey(partition_key="active", row_key="rk1")  # type: ignore[method-assign]

        # Simula el paso del tiempo sin esperar realmente.
        t = {"v": 0.0}

        def mono():
            t["v"] += 1.0
            return t["v"]

        with patch("mymail.state.try_acquire_lock", return_value=None), patch("mymail.state.time.sleep", return_value=None), patch(
            "mymail.state.time.monotonic", side_effect=mono
        ):
            with self.assertRaises(TimeoutError) as ctx:
                _ = state.current_record(owner="u1")

        self.assertIn("No hay correos disponibles", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
