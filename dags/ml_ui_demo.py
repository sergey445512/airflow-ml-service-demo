"""Small, deliberately slow tasks for an Airflow UI demonstration."""
import logging
import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context


def stage(name, seconds=12):
    for tick in range(3):
        logging.info("%s: step %s/3 (demo, no real training)", name, tick + 1)
        time.sleep(seconds / 3)


DEFAULTS = dict(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ml-ui-demo"],
)


@dag(dag_id="demo_01_ml_pipeline", **DEFAULTS,
     description="Success: dependencies, parallel training, logs and XCom",
     doc_md="""## Successful ML pipeline
Trigger this DAG, open Graph, and watch two training tasks run in parallel.
Open a task's logs and XCom. Metrics are simulated; no dataset is needed.
""")
def pipeline():
    @task
    def load_data():
        stage("load_data")
        return {"rows": 10000, "source": "synthetic demo"}

    @task
    def validate_data(data):
        stage("validate_data")
        logging.info("Received data summary: %s", data)

    @task
    def build_features():
        stage("build_features")

    @task
    def train(model):
        stage("train_" + model, seconds=30)
        return {"model": model, "auc": 0.84 if model == "boosting" else 0.79}

    @task
    def compare_models(first, second):
        stage("compare_models")
        winner = max([first, second], key=lambda item: item["auc"])
        logging.info("Demo winner: %s", winner)
        return winner

    @task
    def save_result(winner):
        stage("save_result")
        logging.info("Demo result (no external writes): %s", winner)

    data = load_data()
    valid = validate_data(data)
    features = build_features()
    first = train.override(task_id="train_linear")("linear")
    second = train.override(task_id="train_boosting")("boosting")
    valid >> features >> [first, second]
    save_result(compare_models(first, second))


@dag(dag_id="demo_02_auto_retry", **DEFAULTS,
     description="A temporary failure, retry delay, then success")
def auto_retry():
    @task(retries=1, retry_delay=timedelta(seconds=25))
    def fetch_data():
        stage("fetch_data")
        attempt = get_current_context()["ti"].try_number
        logging.info("Attempt number: %s", attempt)
        if attempt == 1:
            raise RuntimeError("DEMO: temporary data source failure; retry will succeed")
        logging.info("Data source recovered")

    @task
    def train_model():
        stage("train_model", seconds=20)

    fetch_data() >> train_model()


@dag(dag_id="demo_03_manual_recovery", **DEFAULTS,
     description="Failure, upstream_failed, then Clear and recovery",
     doc_md="""## Manual recovery
The quality gate deliberately fails on its first attempt in each run.
Wait until the run fails. Clear `quality_gate` and its downstream tasks,
then watch the same run recover. Do not reset try numbers.
This simulates a repaired external problem; Clear alone does not fix real bugs.
""")
def manual_recovery():
    @task
    def prepare_data():
        stage("prepare_data")

    @task(retries=0)
    def quality_gate():
        stage("quality_gate")
        attempt = get_current_context()["ti"].try_number
        logging.info("Quality gate attempt: %s", attempt)
        if attempt == 1:
            raise ValueError("DEMO: quality check failed. Inspect logs, then Clear with downstream.")
        logging.info("Simulated repair applied: quality check passed")

    @task
    def train_model():
        stage("train_model", seconds=20)

    @task
    def save_result():
        stage("save_result")

    prepare_data() >> quality_gate() >> train_model() >> save_result()


pipeline()
auto_retry()
manual_recovery()
