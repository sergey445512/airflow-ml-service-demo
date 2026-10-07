import unittest
from unittest.mock import patch

from architecture_map import app as observer

counter_rate = observer.counter_rate


class CounterRateTest(unittest.TestCase):
    def test_normal_increment(self):
        self.assertEqual(counter_rate(104, 100, 4), 1.0)

    def test_unavailable_during_redeploy(self):
        self.assertIsNone(counter_rate(None, 100, 4))
        self.assertIsNone(counter_rate(2, None, 4))

    def test_counter_resets_with_new_container(self):
        self.assertEqual(counter_rate(2, 100, 4), 0.5)

    def test_state_recovers_after_temporary_metrics_outage(self):
        observer._previous = {"requests": None, "sample_at": 0}
        metrics = {"total": 2, "neighbors": 0, "qdrant": 0}
        with patch.object(observer, "probe", side_effect=lambda item: (item[0], {"status": "up", "url": item[1][1]})), \
             patch.object(observer, "dag_state", return_value={"status": "no_runs", "tasks": []}), \
             patch.object(observer, "model_state", return_value={"status": "unavailable"}), \
             patch.object(observer, "request_stats", return_value=metrics):
            result = observer.state()
        self.assertEqual(result["requests"]["total"], 2)
        self.assertEqual(result["services"]["inference"]["status"], "up")
