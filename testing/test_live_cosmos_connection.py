from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class LiveCosmosConnectionTests(unittest.TestCase):
    def test_can_read_cosmos_database(self):
        if (os.environ.get("RUN_LIVE_COSMOS_TEST") or "").strip() not in {"1", "true", "True"}:
            self.skipTest("Set RUN_LIVE_COSMOS_TEST=1 to run live Cosmos connectivity test.")

        try:
            from azure.cosmos import CosmosClient  # type: ignore
        except Exception as exc:
            self.skipTest(f"azure-cosmos not installed: {exc}")

        import config

        endpoint = (getattr(config, "COSMOS_ENDPOINT", "") or "").strip()
        key = (getattr(config, "COSMOS_KEY", "") or "").strip()
        db_name = (getattr(config, "COSMOS_DATABASE", "") or "").strip()
        if not endpoint or not key or not db_name:
            self.skipTest("Missing COSMOS_ENDPOINT/COSMOS_KEY/COSMOS_DATABASE.")

        cli = CosmosClient(endpoint, credential=key, connection_timeout=5, request_timeout=20)
        db = cli.get_database_client(db_name)
        props = db.read()
        self.assertTrue(bool(props))


if __name__ == "__main__":
    unittest.main()

