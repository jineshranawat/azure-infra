#!/usr/bin/env python3
"""Unit tests for orchestrate fault-tolerance (auth + platform deploy fallbacks)."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from orchestrate import (  # noqa: E402
    Config,
    DATABRICKS_LOCATION,
    ServicePrincipalAuth,
    _core_platforms_ready,
    _deployment_text,
    _ensure_az_login,
    _parse_deployment_outputs,
    _platform_bicep_params,
    _purview_tenant_exists_error,
    _run_platform_bicep,
    _trigger_all_databricks_cleanup_async,
)


class PurviewErrorTests(unittest.TestCase):
    def test_detects_tenant_catalog_error_code(self) -> None:
        text = 'Inner Errors:\n{"code": "35001", "message": "Validation failed"}'
        self.assertTrue(_purview_tenant_exists_error(text))

    def test_detects_tenant_catalog_message(self) -> None:
        text = "An Enterprise Tenant-level Purview Account already exists for tenant"
        self.assertTrue(_purview_tenant_exists_error(text))

    def test_ignores_unrelated_errors(self) -> None:
        self.assertFalse(_purview_tenant_exists_error("Authorization failed"))


class PlatformParamTests(unittest.TestCase):
    def _cfg(self) -> Config:
        return Config(
            subscription_id="sub",
            learner="demo",
            owner_email="trainer@example.com",
            location="uksouth",
            principal_type="ServicePrincipal",
            service_principal=None,
        )

    def test_default_platform_params_exclude_synapse_and_fabric(self) -> None:
        params = _platform_bicep_params(self._cfg(), "stgdemo", deploy_purview=True)
        joined = " ".join(params)
        self.assertIn("deploySynapse=false", joined)
        self.assertIn("deployFabric=false", joined)
        self.assertIn("deployPurview=true", joined)
        self.assertIn(f"databricksLocation={DATABRICKS_LOCATION}", joined)
        self.assertIn("deployDatabricks=true", joined)

    def test_purview_off_params(self) -> None:
        params = _platform_bicep_params(self._cfg(), "stgdemo", deploy_purview=False)
        self.assertIn("deployPurview=false", " ".join(params))


class PlatformDeployFallbackTests(unittest.TestCase):
    def _cfg(self) -> Config:
        return Config(
            subscription_id="sub",
            learner="sunil",
            owner_email="trainer@example.com",
            location="uksouth",
            principal_type="ServicePrincipal",
            service_principal=None,
        )

    @patch("orchestrate._run")
    def test_deploy_platforms_retries_without_purview(self, mock_run: MagicMock) -> None:
        from orchestrate import _deploy_platforms

        purview_error = subprocess.CompletedProcess(
            args=[],
            returncode=1,
            stdout="",
            stderr='{"code": "35001", "message": "Tenant-level Purview Account already exists"}',
        )
        success = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {
                    "properties": {
                        "outputs": {
                            "dataFactoryName": {"value": "adf-sunil-abc123"},
                            "databricksWorkspaceName": {"value": "dbw-sunil-abc123"},
                            "purviewAccountName": {"value": "skipped"},
                        }
                    }
                }
            ),
            stderr="",
        )

        mock_run.side_effect = [purview_error, success]

        with patch("orchestrate._register_platform_providers"), patch(
            "orchestrate._trigger_all_databricks_cleanup_async", return_value=False
        ), patch("orchestrate._databricks_in_eastus2", return_value=True):
            outputs = _deploy_platforms(
                "az",
                self._cfg(),
                {"storageAccountName": "stgsunilabc"},
                Path("."),
            )

        self.assertEqual(outputs["dataFactoryName"], "adf-sunil-abc123")
        self.assertEqual(outputs["purviewAccountName"], "skipped-tenant-catalog-exists")

    @patch("orchestrate._discover_outputs")
    @patch("orchestrate._run")
    def test_deploy_platforms_continues_when_partial_estate_has_core_services(
        self, mock_run: MagicMock, mock_discover: MagicMock
    ) -> None:
        from orchestrate import _deploy_platforms

        fail = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="unknown error")
        mock_run.side_effect = [fail, fail]
        mock_discover.return_value = {
            "dataFactoryName": "adf-sunil-abc123",
            "databricksWorkspaceName": "dbw-sunil-abc123",
        }

        with patch("orchestrate._register_platform_providers"), patch(
            "orchestrate._trigger_all_databricks_cleanup_async", return_value=False
        ), patch("orchestrate._databricks_in_eastus2", return_value=True):
            outputs = _deploy_platforms(
                "az",
                self._cfg(),
                {"storageAccountName": "stgsunilabc"},
                Path("."),
            )

        self.assertEqual(outputs["dataFactoryName"], "adf-sunil-abc123")


class ServicePrincipalLoginTests(unittest.TestCase):
    def _sp(self) -> ServicePrincipalAuth:
        return ServicePrincipalAuth(
            tenant_id="tenant-id",
            client_id="app-id",
            client_secret="secret",
        )

    @patch("orchestrate._run")
    def test_stale_user_session_triggers_logout_and_sp_login(self, mock_run: MagicMock) -> None:
        probe_user = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {"user": {"name": "learner@example.com", "type": "user"}}
            ),
            stderr="",
        )
        logout = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        login = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

        mock_run.side_effect = [probe_user, logout, login]

        _ensure_az_login("az", use_device_code=False, service_principal=self._sp())

        calls = [call.args[0] for call in mock_run.call_args_list]
        self.assertEqual(calls[0][:3], ["az", "account", "show"])
        self.assertEqual(calls[1][:2], ["az", "logout"])
        self.assertEqual(calls[2][:4], ["az", "login", "--service-principal", "--username"])

    @patch("orchestrate._run")
    def test_matching_sp_session_skips_relogin(self, mock_run: MagicMock) -> None:
        probe_sp = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps({"user": {"name": "app-id", "type": "servicePrincipal"}}),
            stderr="",
        )
        mock_run.return_value = probe_sp

        _ensure_az_login("az", use_device_code=False, service_principal=self._sp())

        self.assertEqual(mock_run.call_count, 1)


class HelperTests(unittest.TestCase):
    @patch("orchestrate._delete_resource_group_async")
    @patch("orchestrate._request_databricks_delete")
    @patch("orchestrate._run")
    def test_trigger_cleanup_async_for_wrong_region(
        self, mock_run: MagicMock, mock_delete: MagicMock, mock_rg_delete: MagicMock
    ) -> None:
        cfg = Config(
            subscription_id="sub",
            learner="demo",
            owner_email="trainer@example.com",
            location="uksouth",
            principal_type="User",
            service_principal=None,
        )
        list_ok = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps([{"name": "dbw-demo-abc123", "location": "uksouth"}]),
            stderr="",
        )
        show_managed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {
                    "properties": {
                        "managedResourceGroupId": "/subscriptions/sub/resourceGroups/rg-demo-dbw-abc123"
                    }
                }
            ),
            stderr="",
        )
        mock_run.side_effect = [list_ok, show_managed]

        result = _trigger_all_databricks_cleanup_async("az", cfg)

        self.assertTrue(result)
        mock_delete.assert_called_once_with("az", cfg, "dbw-demo-abc123")
        mock_rg_delete.assert_called_once_with("az", "rg-demo-dbw-abc123")

    @patch("orchestrate._run")
    def test_trigger_cleanup_skips_when_already_eastus2(self, mock_run: MagicMock) -> None:
        cfg = Config(
            subscription_id="sub",
            learner="demo",
            owner_email="trainer@example.com",
            location="uksouth",
            principal_type="User",
            service_principal=None,
        )
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                [{"name": "dbw-demo-abc123", "location": DATABRICKS_LOCATION}]
            ),
            stderr="",
        )

        result = _trigger_all_databricks_cleanup_async("az", cfg)

        self.assertFalse(result)
        self.assertEqual(mock_run.call_count, 1)

    def test_core_platforms_ready(self) -> None:
        ready, missing = _core_platforms_ready(
            {"dataFactoryName": "adf", "databricksWorkspaceName": "dbw"}
        )
        self.assertTrue(ready)
        self.assertEqual(missing, [])

        ready, missing = _core_platforms_ready({"dataFactoryName": "adf"})
        self.assertFalse(ready)
        self.assertEqual(missing, ["Databricks"])

    def test_parse_deployment_outputs(self) -> None:
        proc = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {"properties": {"outputs": {"dataFactoryName": {"value": "adf-demo"}}}}
            ),
            stderr="",
        )
        self.assertEqual(_parse_deployment_outputs(proc)["dataFactoryName"], "adf-demo")
        self.assertEqual(_parse_deployment_outputs(subprocess.CompletedProcess([], 1, "", "")), {})

    def test_deployment_text_merges_streams(self) -> None:
        proc = subprocess.CompletedProcess(args=[], returncode=1, stdout="out", stderr="err")
        self.assertEqual(_deployment_text(proc), "errout")


if __name__ == "__main__":
    unittest.main()
