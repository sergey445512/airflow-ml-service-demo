"""Check exception and metadata outage behavior without changing running services."""
import unittest
from unittest.mock import patch

import psycopg2
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from service.app import app
from service.metrics import ModelCollector


class MetricsTest(unittest.TestCase):
    def test_failed_prediction_is_counted(self):
        before = REGISTRY.get_sample_value("demo_predict_requests_total", {"status": "500"})
        client = TestClient(app, raise_server_exceptions=False)
        with patch("service.app.active_model", side_effect=RuntimeError("test failure")):
            response = client.post("/predict", json={"tenure_months": 6, "monthly_spend": 120, "support_tickets": 5})
        self.assertEqual(response.status_code, 500)
        self.assertEqual(REGISTRY.get_sample_value("demo_predict_requests_total", {"status": "500"}), before + 1)

    def test_metadata_outage_does_not_export_stale_quality(self):
        with patch.dict("os.environ", {"DEMO_DATABASE_URL": "unused"}), patch("service.metrics.psycopg2.connect", side_effect=psycopg2.OperationalError("unavailable")):
            samples = [s for f in ModelCollector().collect() for s in f.samples]
        self.assertEqual([(s.name, s.value) for s in samples], [("demo_model_metadata_up", 0)])


if __name__ == "__main__":
    unittest.main()
