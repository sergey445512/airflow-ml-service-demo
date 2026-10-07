"""Exercise promotion and rollback of a model trained outside Airflow."""

import time

import requests
from mlflow import MlflowClient

from src.pipeline import connect

client = MlflowClient()
original = client.get_model_version_by_alias("churn-prediction", "production")
external = next(
    (version for version in client.search_model_versions("name = 'churn-prediction'")
     if client.get_run(version.run_id).data.tags.get("training_source") == "external-script"),
    None,
)
assert external is not None, "No external candidate was registered"
assert external.version != original.version, "External candidate was promoted automatically"

try:
    client.set_registered_model_alias("churn-prediction", "production", external.version)
    info = requests.get("http://inference:8000/model", timeout=120)
    info.raise_for_status()
    info = info.json()
    assert info["model_version"] == external.version, info
    assert info["mlflow_run_id"] == external.run_id, info
    assert info["training_source"] == "external-script", info
    assert info["model_contract"] == "churn-v1", info
    assert info["airflow_run_id"] is None and info["git_commit"] is None, info

    response = requests.post("http://inference:8000/predict", json={
        "tenure_months": 6, "monthly_spend": 120, "support_tickets": 5,
    }, timeout=120)
    response.raise_for_status()
    assert response.json()["model_version"] == external.version, response.text
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT p.model_version,r.training_source,r.airflow_run_id
            FROM app.predictions p JOIN app.model_runs r USING(mlflow_run_id)
            WHERE p.mlflow_run_id=%s ORDER BY p.id DESC LIMIT 1""", (external.run_id,))
        row = cur.fetchone()
    assert row == (int(external.version), "external-script", None), row
    print(f"External model v{external.version} served and recorded in PostgreSQL", flush=True)
finally:
    client.set_registered_model_alias("churn-prediction", "production", original.version)
    deadline = time.monotonic() + 60
    while True:
        response = requests.get("http://inference:8000/model", timeout=15)
        if response.ok and response.json()["model_version"] == original.version:
            break
        assert time.monotonic() < deadline, response.text
        time.sleep(2)
    print(f"Restored production v{original.version}", flush=True)
