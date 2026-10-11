"""Score the current customer snapshot with one pinned MLflow Registry version."""
import logging
from datetime import date

import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from psycopg2.extras import execute_values

from service.model_registry import MODEL_NAME, PRODUCTION_ALIAS
from src.pipeline import FEATURES, connect, initialize

MODEL_CONTRACT = "churn-v1"


def pin_production_model():
    """Resolve the mutable alias once; downstream tasks use this exact version."""
    client = MlflowClient()
    version = client.get_model_version_by_alias(MODEL_NAME, PRODUCTION_ALIAS)
    if not version.run_id:
        raise ValueError("The production model version has no MLflow run")
    run = client.get_run(version.run_id)
    contract = version.tags.get("model_contract") or run.data.tags.get("model_contract")
    if contract and contract != MODEL_CONTRACT:
        raise ValueError(f"Unsupported model contract: {contract}")
    return {"model_version": int(version.version), "mlflow_run_id": version.run_id}


def customer_count():
    initialize()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM app.customers")
        count = cur.fetchone()[0]
    if not count:
        raise ValueError("No customers to score; run the training DAG first")
    return count


def score_customers(model_info, scored_on, airflow_run_id, expected_count):
    """Replace one day's snapshot atomically, including on a task retry."""
    scored_on = date.fromisoformat(scored_on)
    client = MlflowClient()
    version = client.get_model_version(MODEL_NAME, str(model_info["model_version"]))
    if version.run_id != model_info["mlflow_run_id"]:
        raise ValueError("Pinned MLflow version no longer matches the original run")
    model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT customer_id, tenure_months, monthly_spend, support_tickets
            FROM app.customers ORDER BY customer_id""")
        customers = cur.fetchall()
    if not customers:
        raise ValueError("No customers to score")
    if len(customers) != expected_count:
        raise ValueError("Customer snapshot changed during scoring")
    features = pd.DataFrame([row[1:] for row in customers], columns=FEATURES)
    probabilities = np.asarray(model.predict_proba(features))
    if (list(model.classes_) != [0, 1] or probabilities.shape != (len(customers), 2)
            or not np.isfinite(probabilities).all()
            or not np.allclose(probabilities.sum(axis=1), 1)
            or np.any(probabilities < 0) or np.any(probabilities > 1)):
        raise ValueError("Model must return probabilities for classes 0 and 1")
    rows = [(scored_on, int(customer[0]), model_info["mlflow_run_id"],
             int(model_info["model_version"]), float(probability), int(probability >= 0.5),
             airflow_run_id)
            for customer, probability in zip(customers, probabilities[:, 1])]
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM app.churn_batch_scores WHERE scored_on = %s", (scored_on,))
        execute_values(cur, """INSERT INTO app.churn_batch_scores
            (scored_on, customer_id, mlflow_run_id, model_version, probability, prediction, airflow_run_id)
            VALUES %s""", rows)
    logging.info("Saved %s customer scores for %s with model v%s", len(rows), scored_on,
                 model_info["model_version"])
    return {**model_info, "scored_on": scored_on.isoformat(), "count": len(rows),
            "airflow_run_id": airflow_run_id}


def verify_scores(result):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT count(*), count(*) FILTER (WHERE prediction = 1),
            min(probability), max(probability), count(DISTINCT mlflow_run_id),
            count(DISTINCT model_version)
            FROM app.churn_batch_scores WHERE scored_on = %s AND airflow_run_id = %s""",
            (result["scored_on"], result["airflow_run_id"]))
        count, high_risk, minimum, maximum, runs, versions = cur.fetchone()
    if (count != result["count"] or runs != 1 or versions != 1
            or minimum < 0 or maximum > 1):
        raise ValueError("Saved batch does not match the pinned model and customer count")
    return {**result, "high_risk": high_risk}
