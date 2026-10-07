"""Train and register a churn candidate without Airflow or PostgreSQL.

Run this in any Python environment that can reach the same MLflow tracking server.
It deliberately does not assign the production alias.
"""

import json
import os

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

if not os.environ.get("MLFLOW_TRACKING_URI"):
    raise SystemExit("Set MLFLOW_TRACKING_URI to the demo MLflow server")

rng = np.random.default_rng(117)
size = 1400
x = pd.DataFrame({
    "tenure_months": rng.integers(1, 73, size=size),
    "monthly_spend": rng.uniform(10, 200, size=size).round(2),
    "support_tickets": rng.integers(0, 9, size=size),
})
logit = -1.8 - 0.035 * x.tenure_months + 0.012 * x.monthly_spend + 0.42 * x.support_tickets
y = (rng.random(size) < 1 / (1 + np.exp(-logit))).astype(int)
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.25, random_state=117, stratify=y)

model = LogisticRegression(max_iter=500)
model.fit(x_train, y_train)
auc = float(roc_auc_score(y_test, model.predict_proba(x_test)[:, 1]))
if auc < .65:
    raise SystemExit(f"Candidate failed the demonstration quality gate: ROC AUC {auc:.3f}")

mlflow.set_experiment("external-churn-demo")
with mlflow.start_run(run_name="external-logistic-regression") as run:
    mlflow.set_tags({"training_source": "external-script", "model_contract": "churn-v1",
                     "dataset_version": "independent-synthetic-v1"})
    mlflow.log_params({"algorithm": "LogisticRegression", "train_rows": len(x_train)})
    mlflow.log_metric("roc_auc", auc)
    logged = mlflow.sklearn.log_model(
        model, name="external_churn_model", input_example=x_test.head(2),
        signature=infer_signature(x_test, model.predict(x_test)),
    )
    version = mlflow.register_model(logged.model_uri, "churn-prediction")
    client = mlflow.MlflowClient()
    client.set_model_version_tag("churn-prediction", version.version, "validation_status", "passed")
    client.set_model_version_tag("churn-prediction", version.version, "model_contract", "churn-v1")
    print(json.dumps({"run_id": run.info.run_id, "model_uri": logged.model_uri,
                      "registered_model": "churn-prediction", "model_version": version.version,
                      "roc_auc": auc, "production_changed": False}), flush=True)
