"""Verify real request metrics, Prometheus ingestion and provisioned Grafana queries."""
import base64
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from prometheus_client.parser import text_string_to_metric_families


def get(url, auth=False):
    headers = {}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(b"admin:demo_grafana").decode()
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as r:
        return r.read().decode()


def metrics():
    return {(s.name, tuple(sorted(s.labels.items()))): s.value
            for f in text_string_to_metric_families(get("http://inference:8000/metrics")) for s in f.samples}


def sample(values, name, **labels):
    return values[(name, tuple(sorted(labels.items())))]


before = metrics()
for tenure, expected in [(6, 200), (-1, 422)]:
    req = urllib.request.Request("http://inference:8000/predict",
        data=json.dumps(dict(tenure_months=tenure, monthly_spend=120, support_tickets=5)).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            assert r.status == expected
    except urllib.error.HTTPError as e:
        assert e.code == expected
        e.close()
after = metrics()
for status in ("200", "422"):
    assert sample(after, "demo_predict_requests_total", status=status) >= sample(before, "demo_predict_requests_total", status=status) + 1
assert sample(after, "demo_churn_probability_count") >= sample(before, "demo_churn_probability_count") + 1
assert sample(after, "demo_predict_duration_seconds_count") >= sample(before, "demo_predict_duration_seconds_count") + 2
info = json.loads(get("http://inference:8000/model"))
assert sample(after, "demo_active_model_info", run_id=info["mlflow_run_id"], git_commit=info["git_commit"]) == 1
assert math.isclose(sample(after, "demo_model_validation_roc_auc"), info["roc_auc"])

deadline = time.monotonic() + 60
while True:
    result = json.loads(get("http://prometheus:9090/api/v1/query?" + urllib.parse.urlencode({"query": 'up{job="inference"}'})))
    if result["data"]["result"] and result["data"]["result"][0]["value"][1] == "1":
        break
    assert time.monotonic() < deadline, result
    time.sleep(2)
dashboard = json.loads(get("http://grafana:3000/api/dashboards/uid/ml-model-overview", auth=True))["dashboard"]
expected_dashboard = json.loads(Path("monitoring/grafana/dashboards/model.json").read_text())
assert len(dashboard["panels"]) == len(expected_dashboard["panels"])
health = json.loads(get("http://grafana:3000/api/datasources/uid/prometheus/health", auth=True))
assert health["status"] == "OK", health
for panel in dashboard["panels"]:
    for target in panel.get("targets", []):
        expression = target["expr"].replace("$__rate_interval", "1m")
        result = json.loads(get("http://grafana:3000/api/datasources/proxy/uid/prometheus/api/v1/query?" + urllib.parse.urlencode({"query": expression}), auth=True))
        assert result["status"] == "success", (expression, result)
print("Monitoring OK: 200/422 counters, latency, predictions, model identity/AUC, scrape, Grafana datasource and all panel queries")
