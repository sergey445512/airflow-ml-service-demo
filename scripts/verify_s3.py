"""Read-only check: docker compose exec mlflow python scripts/verify_s3.py."""
import hashlib
import json
import os
import tempfile
from pathlib import Path

import boto3
import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient

s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])
bucket = os.environ["S3_BUCKET"]
client = MlflowClient()
experiment = client.get_experiment_by_name("churn-online-demo")
models = mlflow.search_logged_models(experiment_ids=[experiment.experiment_id], output_format="list")
assert len(models) >= 1
results = []
for model in models:
    key = f"{experiment.experiment_id}/models/{model.model_id}/artifacts/model.pkl"
    obj = s3.get_object(Bucket=bucket, Key=key)
    with obj["Body"] as stream:
        sha = hashlib.sha256(stream.read()).hexdigest()
    with tempfile.TemporaryDirectory() as folder:
        local = mlflow.artifacts.download_artifacts(artifact_uri=f"models:/{model.model_id}", dst_path=folder)
        with (Path(local) / "model.pkl").open("rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == sha
        loaded = mlflow.sklearn.load_model(f"models:/{model.model_id}")
        probability = float(loaded.predict_proba(pd.DataFrame([{
            "tenure_months": 6, "monthly_spend": 120.0, "support_tickets": 5,
        }]))[0, 1])
        assert 0 <= probability <= 1
    results.append({"model_id": model.model_id, "s3_key": key, "bytes": obj["ContentLength"], "sha256_matches_mlflow": True})
print(json.dumps({"result": "PASS", "bucket": bucket, "models": results}, indent=2))
