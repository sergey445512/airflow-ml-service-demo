"""Regression checks for MLflow's missing-alias REST response."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from mlflow.exceptions import RestException

from service.model_registry import register_candidate, production_version


class RegistryTest(unittest.TestCase):
    def test_first_candidate_gets_production_alias(self):
        client = MagicMock()
        client.search_model_versions.return_value = []
        client.get_model_version_by_alias.side_effect = RestException({
            "error_code": "INVALID_PARAMETER_VALUE",
            "message": "Registered model alias production not found.",
        })
        with patch("service.model_registry.MlflowClient", return_value=client), \
             patch("service.model_registry.mlflow.register_model", return_value=SimpleNamespace(version="1")):
            version = register_candidate({"mlflow_run_id": "run", "model_uri": "models:/model-id"})
            self.assertIsNone(production_version())
        self.assertEqual(version.version, "1")
        client.set_registered_model_alias.assert_called_once_with("churn-prediction", "production", "1")

    def test_later_candidate_does_not_change_production(self):
        candidate = SimpleNamespace(run_id="run", source="models:/model-id", version="2")
        client = MagicMock()
        client.search_model_versions.return_value = [candidate]
        client.get_model_version_by_alias.return_value = SimpleNamespace(version="1")
        with patch("service.model_registry.MlflowClient", return_value=client):
            self.assertIs(register_candidate({"mlflow_run_id": "run", "model_uri": "models:/model-id"}), candidate)
        client.set_registered_model_alias.assert_not_called()


if __name__ == "__main__":
    unittest.main()
