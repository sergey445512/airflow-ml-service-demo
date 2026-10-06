"""One-time migration: make historical demo runs selectable in MLflow Models."""

from src.pipeline import connect
from service.model_registry import register_candidate

with connect() as conn, conn.cursor() as cur:
    cur.execute("SELECT mlflow_run_id, model_uri FROM app.model_runs ORDER BY created_at")
    rows = cur.fetchall()

for run_id, model_uri in rows:
    version = register_candidate({"mlflow_run_id": run_id, "model_uri": model_uri})
    print(f"{run_id} -> churn-prediction version {version.version}", flush=True)
