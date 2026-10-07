import unittest
from unittest.mock import MagicMock, patch

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
             patch.object(observer, "request_stats", return_value=metrics), \
             patch.object(observer, "latest_prediction", return_value={"status": "empty"}), \
             patch.object(observer, "prometheus_state", return_value={"status": "unavailable"}):
            result = observer.state()
        self.assertEqual(result["requests"]["total"], 2)
        self.assertEqual(result["services"]["inference"]["status"], "up")


class DagSelectionTest(unittest.TestCase):
    def test_latest_or_selected_demo_dag(self):
        def fake_api(_session, _method, path, **_kwargs):
            if path == "/dags":
                return {"dags": [{"dag_id": "demo_01_ml_pipeline"}, {"dag_id": "demo_04_train_and_publish"}]}
            if path.endswith("/dagRuns"):
                dag_id = path.split("/")[2]
                started = "2026-10-07T10:00:00Z" if dag_id == "demo_01_ml_pipeline" else "2026-10-07T09:00:00Z"
                return {"dag_runs": [{"dag_run_id": "run-" + dag_id, "state": "success", "start_date": started}]}
            return {"task_instances": [{"task_id": "example", "state": "success"}]}

        with patch.object(observer, "airflow_session", return_value=MagicMock()), \
             patch.object(observer, "airflow_api", side_effect=fake_api):
            latest = observer.dag_state()
            selected = observer.dag_state("demo_04_train_and_publish")
        self.assertEqual(latest["dag_id"], "demo_01_ml_pipeline")
        self.assertEqual(selected["dag_id"], "demo_04_train_and_publish")
        self.assertEqual(len(latest["dag_runs"]), 2)

    def test_active_run_takes_priority_over_newer_completed_run(self):
        runs = [
            {"dag_id": "demo_01_ml_pipeline", "status": "success", "start_date": "2026-10-07T10:00:00Z"},
            {"dag_id": "demo_04_train_and_publish", "status": "running", "start_date": "2026-10-07T09:00:00Z"},
        ]
        self.assertEqual(observer.choose_dag(runs)["dag_id"], "demo_04_train_and_publish")
