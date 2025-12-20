from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _ensure_azure_exception_types():
    try:
        from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError  # type: ignore

        return CosmosHttpResponseError, CosmosResourceNotFoundError
    except Exception:
        azure = sys.modules.get("azure") or types.ModuleType("azure")
        cosmos = sys.modules.get("azure.cosmos") or types.ModuleType("azure.cosmos")
        exceptions = sys.modules.get("azure.cosmos.exceptions") or types.ModuleType("azure.cosmos.exceptions")

        class CosmosResourceNotFoundError(Exception):
            pass

        class CosmosHttpResponseError(Exception):
            pass

        exceptions.CosmosResourceNotFoundError = CosmosResourceNotFoundError
        exceptions.CosmosHttpResponseError = CosmosHttpResponseError
        cosmos.exceptions = exceptions
        azure.cosmos = cosmos

        sys.modules["azure"] = azure
        sys.modules["azure.cosmos"] = cosmos
        sys.modules["azure.cosmos.exceptions"] = exceptions
        return CosmosHttpResponseError, CosmosResourceNotFoundError


class VerifyUserCosmosErrorsTests(unittest.TestCase):
    def test_verify_user_returns_cosmos_disabled_when_no_config(self):
        from mymail import tables

        with patch.object(tables, "cosmos_enabled", return_value=False):
            res = tables.verify_user("u1", "p1")
            self.assertFalse(res.ok)
            self.assertEqual(res.reason, "cosmos_disabled")

    def test_verify_user_returns_not_found_when_user_missing(self):
        from mymail import tables

        CosmosHttpResponseError, CosmosResourceNotFoundError = _ensure_azure_exception_types()

        class FakeContainer:
            def read_item(self, *, item, partition_key):
                raise CosmosResourceNotFoundError("missing")

        with patch.object(tables, "cosmos_enabled", return_value=True), patch.object(tables, "_cosmos", return_value=FakeContainer()):
            res = tables.verify_user("u1", "p1")
            self.assertFalse(res.ok)
            self.assertEqual(res.reason, "not_found")

    def test_verify_user_returns_cosmos_error_on_http_error(self):
        from mymail import tables

        CosmosHttpResponseError, CosmosResourceNotFoundError = _ensure_azure_exception_types()

        class FakeContainer:
            def read_item(self, *, item, partition_key):
                raise CosmosHttpResponseError("boom")

        with patch.object(tables, "cosmos_enabled", return_value=True), patch.object(tables, "_cosmos", return_value=FakeContainer()):
            res = tables.verify_user("u1", "p1")
            self.assertFalse(res.ok)
            self.assertEqual(res.reason, "cosmos_error")

    def test_verify_user_returns_cosmos_error_on_unexpected_error(self):
        from mymail import tables

        _ensure_azure_exception_types()

        class FakeContainer:
            def read_item(self, *, item, partition_key):
                raise RuntimeError("network down")

        with patch.object(tables, "cosmos_enabled", return_value=True), patch.object(tables, "_cosmos", return_value=FakeContainer()):
            res = tables.verify_user("u1", "p1")
            self.assertFalse(res.ok)
            self.assertEqual(res.reason, "cosmos_error")


if __name__ == "__main__":
    unittest.main()

