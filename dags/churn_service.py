import logging
import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context


@dag(dag_id="demo_04_train_and_publish", schedule=None,
     start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["ml-ui-demo", "online-service"],
     description="Git → PostgreSQL → Qdrant → training → MLflow → online prediction",
     doc_md="""## Connected ML service
1. Seed synthetic customers in PostgreSQL.
2. Index historical customers in Qdrant for similarity search.
3. Train and log a real small model in MLflow, including Git commit.
4. Register a candidate version in MLflow. The first version gets alias `production`.
5. Check the candidate and call online inference with the currently selected version.

To promote a later candidate, open MLflow Models → churn-prediction and move the
`production` alias to its version. The service will load it on the next request.

Open http://localhost:8000 to try a prediction and http://localhost:5000 for MLflow.
""")
def service_pipeline():
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def prepare_data():
        from src.pipeline import seed_data
        time.sleep(8)
        return seed_data()

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def index_customers(dataset):
        from src.neighbors import sync_customers

        result = sync_customers()
        logging.info("Indexed %s customers in Qdrant collection %s",
                     result["indexed_customers"], result["collection"])
        return dataset | {"qdrant": result}

    @task
    def train_and_log(dataset):
        from src.pipeline import train
        logging.info("Training from %s", dataset)
        time.sleep(12)
        return train(get_current_context()["run_id"])

    @task
    def publish_model(result):
        from src.pipeline import activate
        time.sleep(8)
        return activate(result)

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def verify_online_prediction(result):
        import requests
        from mlflow import MlflowClient

        candidate = MlflowClient().get_model_version("churn-prediction", result["model_version"])
        if candidate.run_id != result["mlflow_run_id"]:
            raise ValueError("Candidate version does not match the training run")
        response = requests.post("http://inference:8000/predict", json={"tenure_months": 6, "monthly_spend": 120, "support_tickets": 5}, timeout=120)
        response.raise_for_status()
        prediction = response.json()
        production = MlflowClient().get_model_version_by_alias("churn-prediction", "production")
        if prediction["mlflow_run_id"] != production.run_id:
            raise ValueError("Service is not serving the MLflow production alias")
        logging.info("Candidate version %s; currently served: %s", result["model_version"], prediction)
        return prediction

    verify_online_prediction(publish_model(train_and_log(index_customers(prepare_data()))))


service_pipeline()
