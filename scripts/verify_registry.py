"""Verify the MLflow production alias matches the model used by online inference."""

import time

import requests
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from service.model_registry import MODEL_NAME, PRODUCTION_ALIAS

client = MlflowClient()
deadline = time.monotonic() + 60
while True:
    response = requests.get("http://inference:8000/model", timeout=10)
    response.raise_for_status()
    active = response.json()
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, PRODUCTION_ALIAS)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        version = None
    if version and version.run_id == active["mlflow_run_id"] and version.source == active["model_uri"]:
        print(f"MLflow {MODEL_NAME}@{PRODUCTION_ALIAS} -> version {version.version}, run {version.run_id}")
        break
    if time.monotonic() >= deadline:
        raise AssertionError(f"MLflow production alias does not match active model: {active}, {version}")
    time.sleep(2)
