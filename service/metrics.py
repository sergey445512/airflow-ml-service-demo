"""Low-cardinality metrics for the single-process demo inference service."""
import os

import psycopg2
from prometheus_client import Counter, Histogram, REGISTRY
from prometheus_client.core import GaugeMetricFamily

REQUESTS = Counter("demo_predict_requests", "Prediction HTTP responses", ["status"])
LATENCY = Histogram("demo_predict_duration_seconds", "End-to-end prediction latency",
                    buckets=(.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30))
PROBABILITY = Histogram("demo_churn_probability", "Successful prediction probabilities",
                        buckets=(.1, .2, .3, .4, .5, .6, .7, .8, .9, 1))
PREDICTIONS = Counter("demo_predictions", "Successful predictions by class", ["prediction"])
for label in ("0", "1"):
    PREDICTIONS.labels(label)
for status in ("200", "422", "500", "503"):
    REQUESTS.labels(status)
FEATURES = {
    "tenure_months": Histogram("demo_feature_tenure_months", "Tenure in successful requests", buckets=(6, 12, 24, 36, 48, 60, 72)),
    "monthly_spend": Histogram("demo_feature_monthly_spend", "Spend in successful requests", buckets=(25, 50, 75, 100, 125, 150, 175, 200)),
    "support_tickets": Histogram("demo_feature_support_tickets", "Tickets in successful requests", buckets=tuple(range(9))),
}
LOADS = Counter("demo_model_loads", "Model load attempts", ["result"])
LOAD_TIME = Histogram("demo_model_load_duration_seconds", "Model load latency", buckets=(.1, .5, 1, 2, 5, 10, 30, 60))


class ModelCollector:
    """Read the publication pointer at scrape time, even before the first prediction.

    On DB failure expose metadata_up=0 and omit stale model information.
    Never label request metrics with customer IDs, features or model revisions.
    """

    def describe(self):
        return []

    def collect(self):
        up = GaugeMetricFamily("demo_model_metadata_up", "Model metadata query succeeded")
        try:
            with psycopg2.connect(os.environ["DEMO_DATABASE_URL"], connect_timeout=3,
                                  options="-c statement_timeout=2000") as conn, conn.cursor() as cur:
                cur.execute("""SELECT r.mlflow_run_id,r.git_commit,r.roc_auc,
                    EXTRACT(EPOCH FROM a.activated_at)
                    FROM app.active_model a JOIN app.model_runs r USING(mlflow_run_id)
                    WHERE a.singleton""")
                row = cur.fetchone()
                cur.execute("SELECT count(*) FROM app.model_runs")
                count = cur.fetchone()[0]
        except psycopg2.Error:
            up.add_metric([], 0)
            yield up
            return
        up.add_metric([], 1)
        yield up
        available = GaugeMetricFamily("demo_model_available", "A model has been published")
        available.add_metric([], int(row is not None))
        yield available
        runs = GaugeMetricFamily("demo_training_runs", "Training runs recorded in PostgreSQL")
        runs.add_metric([], count)
        yield runs
        if row:
            info = GaugeMetricFamily("demo_active_model_info", "Published model identity", labels=["run_id", "git_commit"])
            info.add_metric([row[0], row[1]], 1)
            yield info
            auc = GaugeMetricFamily("demo_model_validation_roc_auc", "Offline validation ROC AUC; NOT live quality")
            auc.add_metric([], float(row[2]))
            yield auc
            activated = GaugeMetricFamily("demo_model_activated_timestamp_seconds", "Publication time")
            activated.add_metric([], float(row[3]))
            yield activated


REGISTRY.register(ModelCollector())
