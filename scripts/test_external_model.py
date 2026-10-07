"""Check metadata and contract handling for non-Airflow model versions."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from service.app import active_model


class ExternalModelTest(unittest.TestCase):
    def test_external_run_needs_no_airflow_or_quality_metadata(self):
        version = SimpleNamespace(run_id="external-run", source="models:/model-id", version="7", tags={})
        run = SimpleNamespace(data=SimpleNamespace(tags={"training_source": "notebook"}, metrics={}))
        with patch("service.app.MlflowClient") as client:
            client.return_value.get_run.return_value = run
            info = active_model(version)
        self.assertEqual(info["training_source"], "notebook")
        self.assertIsNone(info["airflow_run_id"])
        self.assertIsNone(info["git_commit"])
        self.assertIsNone(info["roc_auc"])
        self.assertEqual(info["model_version"], "7")

    def test_incompatible_contract_is_rejected(self):
        version = SimpleNamespace(run_id="external-run", source="models:/model-id", version="7",
                                  tags={"model_contract": "churn-v2"})
        run = SimpleNamespace(data=SimpleNamespace(tags={}, metrics={}))
        with patch("service.app.MlflowClient") as client:
            client.return_value.get_run.return_value = run
            with self.assertRaises(HTTPException) as raised:
                active_model(version)
        self.assertEqual(raised.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
