"""Check the deployed code revision and a real prediction without retraining."""
import argparse
import time

import requests

parser = argparse.ArgumentParser()
parser.add_argument("--revision", required=True)
args = parser.parse_args()
base = "http://localhost:8000"
for _ in range(60):
    try:
        response = requests.get(base + "/health", timeout=5)
        response.raise_for_status()
        break
    except requests.RequestException:
        time.sleep(2)
else:
    raise TimeoutError("Inference did not become healthy")
version = requests.get(base + "/version", timeout=10)
version.raise_for_status()
assert version.json()["git_commit"] == args.revision, version.text
assert version.json()["git_dirty"] is False, version.text
prediction = requests.post(base + "/predict", json={"tenure_months": 6, "monthly_spend": 120, "support_tickets": 5}, timeout=120)
prediction.raise_for_status()
result = prediction.json()
assert 0 <= result["churn_probability"] <= 1, result
assert result["model_uri"].startswith("models:/"), result
print({"status": "PASS", "deployed_commit": args.revision, "model_run": result["mlflow_run_id"]})
