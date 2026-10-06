"""Keep the MLflow production alias aligned with the model served from PostgreSQL."""

import logging

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

MODEL_NAME = "churn-prediction"
PRODUCTION_ALIAS = "production"


def sync_production_alias(model_info):
    """Register the active logged model once and point production at its version."""
    client = MlflowClient()
    try:
        aliased = client.get_model_version_by_alias(MODEL_NAME, PRODUCTION_ALIAS)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        aliased = None
    if aliased and aliased.run_id == model_info["mlflow_run_id"] and aliased.source == model_info["model_uri"]:
        return aliased

    version = next(
        (item for item in client.search_model_versions(f"name = '{MODEL_NAME}'")
         if item.run_id == model_info["mlflow_run_id"] and item.source == model_info["model_uri"]),
        None,
    )
    if version is None:
        version = mlflow.register_model(model_info["model_uri"], MODEL_NAME)
    client.set_registered_model_alias(MODEL_NAME, PRODUCTION_ALIAS, version.version)
    logging.info("MLflow %s@%s -> version %s (run %s)", MODEL_NAME, PRODUCTION_ALIAS,
                 version.version, model_info["mlflow_run_id"])
    return version
