"""Run inside airflow: docker compose exec airflow python scripts/verify_demo.py.

Creates real demo runs, clears the intentional failure, and checks inference.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "http://localhost:8080"
for attempt in range(90):
    try:
        if requests.get(BASE + "/api/v2/version", timeout=3).status_code < 500:
            break
    except requests.RequestException:
        pass
    time.sleep(2)
else:
    raise TimeoutError("Airflow did not start within 180 seconds")
passwords = json.loads((Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "simple_auth_manager_passwords.json.generated").read_text())
session = requests.Session()
token = session.post(BASE + "/auth/token", json={"username": "admin", "password": passwords["admin"]}, timeout=20)
token.raise_for_status()
session.headers["Authorization"] = "Bearer " + token.json()["access_token"]


def api(method, path, **kwargs):
    response = session.request(method, BASE + "/api/v2" + path, timeout=30, **kwargs)
    if not response.ok:
        raise RuntimeError(f"{method} {path}: {response.status_code} {response.text[:1500]}")
    return response.json()


errors = api("GET", "/importErrors")
assert errors["total_entries"] == 0, errors
ids = ["demo_01_ml_pipeline", "demo_02_auto_retry", "demo_03_manual_recovery", "demo_04_train_and_publish"]
for _ in range(90):
    available = {dag["dag_id"] for dag in api("GET", "/dags", params={"limit": 100})["dags"]}
    if set(ids) <= available:
        break
    time.sleep(2)
else:
    raise TimeoutError("Demo DAGs were not parsed")
run_id = "verification_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
for dag in ids:
    api("PATCH", f"/dags/{dag}", json={"is_paused": False})
    api("POST", f"/dags/{dag}/dagRuns", json={"dag_run_id": run_id, "logical_date": None, "conf": {}})
    print("Triggered", dag, run_id, flush=True)

observed = {}
recovered = False
deadline = time.monotonic() + 600
while time.monotonic() < deadline:
    states = {dag: api("GET", f"/dags/{dag}/dagRuns/{run_id}")["state"] for dag in ids}
    if states != observed:
        print(json.dumps(states), flush=True)
        observed = states
    for dag, state in states.items():
        if state == "failed" and dag != "demo_03_manual_recovery":
            details = api("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances")
            raise AssertionError(details)
    if states["demo_03_manual_recovery"] == "failed" and not recovered:
        failed = api("GET", f"/dags/demo_03_manual_recovery/dagRuns/{run_id}/taskInstances")
        print("Before Clear:", [(t["task_id"], t["state"]) for t in failed["task_instances"]], flush=True)
        api("POST", "/dags/demo_03_manual_recovery/clearTaskInstances", json={
            "dag_run_id": run_id, "task_ids": ["quality_gate"], "include_downstream": True,
            "dry_run": False, "reset_dag_runs": True,
        })
        recovered = True
        print("Cleared quality gate and downstream tasks", flush=True)
    if all(state == "success" for state in states.values()):
        break
    time.sleep(5)
else:
    raise TimeoutError(observed)

assert recovered
retry = api("GET", f"/dags/demo_02_auto_retry/dagRuns/{run_id}/taskInstances/fetch_data")
assert retry["try_number"] == 2, retry
response = requests.post("http://inference:8000/predict", json={"tenure_months": 6, "monthly_spend": 120, "support_tickets": 5}, timeout=120)
response.raise_for_status()
prediction = response.json()
assert prediction["airflow_run_id"] == run_id, prediction
assert 0 <= prediction["churn_probability"] <= 1, prediction
invalid = requests.post("http://inference:8000/predict", json={"tenure_months": -1, "monthly_spend": 120, "support_tickets": 5}, timeout=20)
assert invalid.status_code == 422, invalid.text
from src.pipeline import connect
with connect() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM app.customers")
    assert cur.fetchone()[0] == 1200
    cur.execute("SELECT count(*) FROM app.predictions WHERE mlflow_run_id=%s", (prediction["mlflow_run_id"],))
    count = cur.fetchone()[0]
    assert count >= 2
print(json.dumps({"result": "PASS", "dag_states": states, "retry_attempts": retry["try_number"], "manual_recovery": recovered, "prediction": prediction, "saved_predictions": count}, indent=2), flush=True)
