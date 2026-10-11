"""Weekly and manually triggered scoring of customers with the production model."""
import logging
import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(dag_id="demo_05_weekly_batch_scoring", schedule="0 3 * * 1",
     start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["ml-ui-demo", "batch-scoring"],
     description="Weekly customer scoring with the current MLflow production model",
     doc_md="""## Weekly churn scoring

Every Monday at 03:00 UTC (or on a manual trigger), read customers from PostgreSQL,
resolve the MLflow `production` alias once, score them with that exact version, and
atomically save the daily snapshot in `app.churn_batch_scores`. This DAG does **not**
train or promote a model and does not call the online API for every customer.

The short task pauses make the stages visible on the architecture map.
""")
def weekly_batch_scoring():
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def select_production_model():
        from src.batch_scoring import pin_production_model
        result = pin_production_model()
        logging.info("Pinned production model v%s", result["model_version"])
        time.sleep(4)
        return result

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def check_customers(model_info):
        from src.batch_scoring import customer_count
        count = customer_count()
        # Fixed for this DAG run, including task retries and manual runs.
        scored_on = get_current_context()["dag_run"].run_after.date().isoformat()
        logging.info("Customer snapshot: %s rows; score date: %s", count, scored_on)
        time.sleep(4)
        return {**model_info, "scored_on": scored_on, "customer_count": count}

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def score_and_save(snapshot):
        from src.batch_scoring import score_customers
        run_id = get_current_context()["run_id"]
        result = score_customers(snapshot, snapshot["scored_on"], run_id,
                                 snapshot["customer_count"])
        time.sleep(4)
        return result

    @task
    def verify_batch(result):
        from src.batch_scoring import verify_scores
        summary = verify_scores(result)
        logging.info("Batch result: %s", summary)
        return summary

    verify_batch(score_and_save(check_customers(select_production_model())))


weekly_batch_scoring()
