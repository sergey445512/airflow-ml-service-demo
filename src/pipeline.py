"""Tiny real training pipeline; PostgreSQL is the shared data boundary."""
import json
import logging
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
FEATURES = ["tenure_months", "monthly_spend", "support_tickets"]


def connect():
    return psycopg2.connect(os.environ["DEMO_DATABASE_URL"])


def initialize():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS app;
            CREATE TABLE IF NOT EXISTS app.customers (
                customer_id INTEGER PRIMARY KEY,
                tenure_months INTEGER NOT NULL,
                monthly_spend DOUBLE PRECISION NOT NULL,
                support_tickets INTEGER NOT NULL,
                churn INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS app.model_runs (
                mlflow_run_id TEXT PRIMARY KEY,
                airflow_run_id TEXT NOT NULL,
                git_commit TEXT NOT NULL,
                roc_auc DOUBLE PRECISION NOT NULL,
                model_uri TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS app.active_model (
                singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
                mlflow_run_id TEXT NOT NULL REFERENCES app.model_runs(mlflow_run_id),
                activated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS app.predictions (
                id BIGSERIAL PRIMARY KEY,
                mlflow_run_id TEXT NOT NULL REFERENCES app.model_runs(mlflow_run_id),
                features JSONB NOT NULL,
                probability DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)


def seed_data():
    initialize()
    rng = np.random.default_rng(42)
    rows = []
    for i in range(1200):
        tenure = int(rng.integers(1, 73))
        spend = round(float(rng.uniform(10, 200)), 2)
        tickets = int(rng.integers(0, 9))
        logit = -1.8 - 0.035 * tenure + 0.012 * spend + 0.42 * tickets
        churn = int(rng.random() < 1 / (1 + np.exp(-logit)))
        rows.append((i + 1, tenure, spend, tickets, churn))
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, """INSERT INTO app.customers VALUES %s
            ON CONFLICT (customer_id) DO NOTHING""", rows)
    logging.info("Synthetic customers available in app.customers: 1200")
    return {"table": "app.customers", "dataset_version": "synthetic-v1"}


def train(airflow_run_id):
    import mlflow
    import mlflow.sklearn
    from mlflow.models import infer_signature
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score, accuracy_score
    from sklearn.model_selection import train_test_split

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT tenure_months, monthly_spend, support_tickets, churn FROM app.customers ORDER BY customer_id")
        frame = pd.DataFrame(cur.fetchall(), columns=FEATURES + ["churn"])
    if len(frame) < 100 or frame.churn.nunique() != 2:
        raise ValueError("Training requires >=100 rows and both target classes")
    config = json.loads((ROOT / "model_config.json").read_text())
    commit = subprocess.check_output(["git", "-c", f"safe.directory={ROOT}", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "-c", f"safe.directory={ROOT}", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    x_train, x_test, y_train, y_test = train_test_split(frame[FEATURES], frame.churn, test_size=0.25, random_state=42, stratify=frame.churn)
    model = RandomForestClassifier(**config, n_jobs=1)
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "churn-online-demo"))
    with mlflow.start_run(run_name=airflow_run_id) as run:
        mlflow.set_tags({"git_commit": commit, "git_dirty": str(dirty).lower(), "airflow_run_id": airflow_run_id, "dataset_version": "synthetic-v1"})
        mlflow.log_params(config | {"train_rows": len(x_train), "test_rows": len(x_test)})
        mlflow.log_dict(config, "model_config.json")
        mlflow.log_artifact(str(ROOT / "src" / "pipeline.py"), artifact_path="source")
        model.fit(x_train, y_train)
        probabilities = model.predict_proba(x_test)[:, 1]
        auc = float(roc_auc_score(y_test, probabilities))
        mlflow.log_metrics({"roc_auc": auc, "accuracy": float(accuracy_score(y_test, probabilities >= 0.5))})
        logged = mlflow.sklearn.log_model(model, name="model", input_example=x_test.head(2), signature=infer_signature(x_test, model.predict(x_test)))
        result = {"mlflow_run_id": run.info.run_id, "airflow_run_id": airflow_run_id, "git_commit": commit, "roc_auc": auc, "model_uri": logged.model_uri}
        logging.info("MLflow result: %s", result)
        return result


def activate(result):
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO app.model_runs (mlflow_run_id,airflow_run_id,git_commit,roc_auc,model_uri)
            VALUES (%(mlflow_run_id)s,%(airflow_run_id)s,%(git_commit)s,%(roc_auc)s,%(model_uri)s)
            ON CONFLICT (mlflow_run_id) DO NOTHING""", result)
        cur.execute("""INSERT INTO app.active_model(singleton,mlflow_run_id) VALUES(true,%s)
            ON CONFLICT(singleton) DO UPDATE SET mlflow_run_id=excluded.mlflow_run_id,activated_at=now()""", (result["mlflow_run_id"],))
    return result
