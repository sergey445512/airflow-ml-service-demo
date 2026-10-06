"""MLflow Registry is the source of truth for the serving model."""

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

MODEL_NAME = "churn-prediction"
PRODUCTION_ALIAS = "production"


def register_candidate(model_info):
    """Add a trained model as a version; only bootstrap an empty registry."""
    client = MlflowClient()
    version = next(
        (item for item in client.search_model_versions(f"name = '{MODEL_NAME}'")
         if item.run_id == model_info["mlflow_run_id"] and item.source == model_info["model_uri"]),
        None,
    )
    if version is None:
        version = mlflow.register_model(model_info["model_uri"], MODEL_NAME)
    try:
        client.get_model_version_by_alias(MODEL_NAME, PRODUCTION_ALIAS)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        client.set_registered_model_alias(MODEL_NAME, PRODUCTION_ALIAS, version.version)
    return version


def production_version():
    """Return the exact version selected in MLflow's Models UI."""
    try:
        return MlflowClient().get_model_version_by_alias(MODEL_NAME, PRODUCTION_ALIAS)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        return None
