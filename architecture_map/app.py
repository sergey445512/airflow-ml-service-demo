"""Read-only observer for the local ML platform, with an explicit demo DAG trigger."""
import json
import os
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import requests
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI(title="ML platform map")
ROOT = Path(__file__).parent
AIRFLOW = "http://airflow:8080"
DAG_ID = "demo_04_train_and_publish"
DEMO_DAGS = (
    "demo_01_ml_pipeline", "demo_02_auto_retry", "demo_03_manual_recovery", DAG_ID,
)
SERVICES = {
    "airflow": ("http://airflow:8080/api/v2/version", "http://localhost:8080"),
    "mlflow": ("http://mlflow:5000/health", "http://localhost:5000"),
    "inference": ("http://inference:8000/health", "http://localhost:8000"),
    "postgres": ("tcp://postgres:5432", "http://localhost:8081"),
    "s3": ("http://s3:9000/health", "http://localhost:9001"),
    "redis": ("tcp://redis:6379", "http://localhost:5540"),
    "qdrant": ("http://qdrant:6333/healthz", "http://localhost:6333/dashboard"),
    "prometheus": ("http://prometheus:9090/-/ready", "http://localhost:9090"),
    "grafana": ("http://grafana:3000/api/health", "http://localhost:3000"),
    "database-ui": ("http://database-ui:8080", "http://localhost:8081"),
    "redisinsight": ("http://redisinsight:5540", "http://localhost:5540"),
    "architecture-map": ("http://localhost:8090/health", "http://localhost:8090"),
    "traffic": (None, "http://localhost:8000"),
    "github": (None, "https://github.com/sergey445512/airflow-ml-service-demo/actions"),
}
_lock = threading.Lock()
_events = []
_ci = None
_previous = {}
_last_request_seen = 0.0
_last_model_change = 0.0
_last_prediction_seen = 0.0
_last_prometheus_seen = 0.0


def stamp():
    return datetime.now(timezone.utc).isoformat()


def record(kind, title, detail=""):
    with _lock:
        if _events and _events[0]["title"] == title and _events[0]["detail"] == detail:
            return
        _events.insert(0, {"time": stamp(), "kind": kind, "title": title, "detail": detail})
        del _events[40:]


def probe(item):
    name, (url, link) = item
    if not url:
        return name, {"status": "unknown", "url": link}
    try:
        if url.startswith("tcp://"):
            host, port = url[6:].split(":")
            with socket.create_connection((host, int(port)), timeout=1.2):
                pass
        else:
            response = requests.get(url, timeout=1.5)
            response.raise_for_status()
        status = "up"
    except (OSError, requests.RequestException):
        status = "down"
    return name, {"status": status, "url": link}


def airflow_session():
    password_file = Path("/airflow-auth/simple_auth_manager_passwords.json.generated")
    password = json.loads(password_file.read_text(encoding="utf-8"))["admin"]
    token = requests.post(AIRFLOW + "/auth/token", json={"username": "admin", "password": password}, timeout=3)
    token.raise_for_status()
    session = requests.Session()
    session.headers["Authorization"] = "Bearer " + token.json()["access_token"]
    return session


def airflow_api(session, method, path, **kwargs):
    response = session.request(method, AIRFLOW + "/api/v2" + path, timeout=4, **kwargs)
    response.raise_for_status()
    return response.json()


def airflow_request(method, path, **kwargs):
    with airflow_session() as session:
        return airflow_api(session, method, path, **kwargs)


def choose_dag(runs, selected_dag=None):
    """Follow an active DAG by default, then the most recently started run."""
    if selected_dag:
        return next((run for run in runs if run["dag_id"] == selected_dag), None)
    active = [run for run in runs if run["status"] in {"running", "queued"}]
    candidates = active or runs
    return max(candidates, key=lambda run: run.get("start_date") or "") if candidates else None


def dag_state(selected_dag=None):
    try:
        with airflow_session() as session:
            available = {dag["dag_id"] for dag in airflow_api(session, "GET", "/dags", params={"limit": 100})["dags"]}
            runs = []
            for dag_id in DEMO_DAGS:
                if dag_id not in available:
                    continue
                response = airflow_api(session, "GET", f"/dags/{dag_id}/dagRuns",
                                       params={"limit": 1, "order_by": "-start_date"})
                for run in response.get("dag_runs", []):
                    runs.append({"dag_id": dag_id, "run_id": run["dag_run_id"],
                                 "status": run.get("state") or "unknown",
                                 "start_date": run.get("start_date") or run.get("logical_date")})
            runs.sort(key=lambda run: run.get("start_date") or "", reverse=True)
            chosen = choose_dag(runs, selected_dag)
            result = {"dag_runs": runs, "running_count": sum(
                run["status"] in {"running", "queued"} for run in runs)}
            if not chosen:
                return {**result, "dag_id": selected_dag, "status": "no_runs", "tasks": []}
            tasks = airflow_api(session, "GET",
                                f"/dags/{chosen['dag_id']}/dagRuns/{chosen['run_id']}/taskInstances",
                                params={"limit": 100})
            return {**result, "dag_id": chosen["dag_id"], "status": chosen["status"],
                    "run_id": chosen["run_id"], "start_date": chosen["start_date"],
                    "tasks": [{"id": task["task_id"], "status": task.get("state") or "pending"}
                              for task in tasks.get("task_instances", [])]}
    except (OSError, ValueError, KeyError, requests.RequestException) as exc:
        return {"status": "unavailable", "reason": type(exc).__name__, "tasks": [], "dag_runs": []}


def model_state():
    try:
        model = requests.get("http://inference:8000/model", timeout=4)
        model.raise_for_status()
        version = requests.get("http://inference:8000/version", timeout=2)
        version.raise_for_status()
        return {"status": "ready", **model.json(), "service_revision": version.json()}
    except (ValueError, requests.RequestException) as exc:
        return {"status": "unavailable", "reason": type(exc).__name__}


def request_stats():
    try:
        metrics = requests.get("http://inference:8000/metrics", timeout=2)
        metrics.raise_for_status()
        total = sum(float(value) for value in re.findall(
            r'^demo_predict_requests_total\{[^\n]*\}\s+([0-9.eE+-]+)', metrics.text, re.M))
        def counter(name):
            return sum(float(value) for value in re.findall(
                rf'^{name}\{{[^\n]*\}}\s+([0-9.eE+-]+)', metrics.text, re.M))
        return {"total": int(total), "neighbors": int(counter("demo_neighbor_cache_lookups_total")),
                "qdrant": int(counter("demo_qdrant_queries_total"))}
    except requests.RequestException:
        return {"total": None, "neighbors": None, "qdrant": None}


def latest_prediction():
    """Observe a committed prediction without coupling inference to the map."""
    try:
        with closing(psycopg2.connect(os.environ["MAP_DATABASE_URL"], connect_timeout=2,
                                      options="-c default_transaction_read_only=on")) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""SELECT id, features, probability, model_version, created_at
                    FROM app.predictions ORDER BY id DESC LIMIT 1""")
                row = cursor.fetchone()
        if row is None:
            return {"status": "empty"}
        row_id, features, probability, version, created_at = row
        return {"status": "ready", "id": row_id, "features": features,
                "probability": round(float(probability), 4), "prediction": int(probability >= .5),
                "model_version": version, "created_at": created_at.isoformat()}
    except (KeyError, psycopg2.Error) as exc:
        return {"status": "unavailable", "reason": type(exc).__name__}


def prometheus_state():
    """Read Prometheus' own scrape status and aggregate, not inference's local copy."""
    def value(session, expression):
        response = session.get("http://prometheus:9090/api/v1/query",
                               params={"query": expression}, timeout=2)
        response.raise_for_status()
        result = response.json()["data"]["result"]
        return float(result[0]["value"][1]) if result else None

    try:
        with requests.Session() as session:
            response = session.get("http://prometheus:9090/api/v1/targets",
                                   params={"state": "active"}, timeout=2)
            response.raise_for_status()
            targets = response.json()["data"]["activeTargets"]
            target = next(item for item in targets if item.get("labels", {}).get("job") == "inference")
            total = value(session, "sum(demo_predict_requests_total)")
            rate = value(session, "sum(rate(demo_predict_requests_total[1m]))")
        return {"status": "ready", "scrape_health": target.get("health"),
                "last_scrape": target.get("lastScrape"),
                "requests_total": int(total) if total is not None else None,
                "rate_1m": round(rate, 2) if rate is not None else None}
    except (StopIteration, KeyError, ValueError, requests.RequestException) as exc:
        return {"status": "unavailable", "reason": type(exc).__name__}


def counter_rate(current, previous, elapsed):
    """A new container resets process-local Prometheus counters to zero."""
    if current is None or previous is None:
        return None
    delta = current - previous if current >= previous else current
    return round(delta / max(elapsed, 0.01), 2)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/state")
def state(dag_id: str | None = None):
    global _previous, _last_request_seen, _last_model_change, _last_prediction_seen, _last_prometheus_seen
    if dag_id and dag_id not in DEMO_DAGS:
        raise HTTPException(400, "Unknown demo DAG")
    with ThreadPoolExecutor(max_workers=14) as pool:
        service_future = pool.submit(lambda: dict(pool.map(probe, SERVICES.items())))
        dag_future = pool.submit(dag_state, dag_id)
        model_future = pool.submit(model_state)
        metrics_future = pool.submit(request_stats)
        prediction_future = pool.submit(latest_prediction)
        prometheus_future = pool.submit(prometheus_state)
        services = service_future.result()
        dag = dag_future.result()
        model = model_future.result()
        metrics = metrics_future.result()
        prediction = prediction_future.result()
        prometheus = prometheus_future.result()
    with _lock:
        ci = dict(_ci) if _ci else None
        previous = dict(_previous)
    ci_fresh = bool(ci and time.time() - ci["received_unix"] < 45)
    services["github"]["status"] = "up" if ci_fresh else "unknown"
    if ci_fresh:
        ci.pop("received_unix", None)
    else:
        ci = {"status": "unavailable", "reason": "Локальный наблюдатель GitHub Actions не запущен"}
    previous_total = previous.get("requests")
    if metrics["total"] is not None and metrics["total"] > 0 and metrics["total"] != previous_total:
        _last_request_seen = time.time()
    services["traffic"]["status"] = "up" if time.time() - _last_request_seen < 15 else "unknown"
    previous_runs = previous.get("dag_runs")
    if previous_runs is not None:
        for run in dag.get("dag_runs", []):
            old = previous_runs.get(run["dag_id"])
            if not old or old["run_id"] != run["run_id"]:
                record("dag", "Новый запуск: " + run["dag_id"], run["run_id"])
            elif old["status"] != run["status"]:
                record("dag", run["dag_id"] + ": " + run["status"], run["run_id"])
    if model.get("model_version") and model.get("model_version") != previous.get("model_version"):
        record("model", "Активная модель: версия " + str(model["model_version"]), model.get("mlflow_run_id", ""))
        if previous.get("model_version"):
            _last_model_change = time.time()
    old_tasks = previous.get("tasks", {})
    if (dag.get("dag_id"), dag.get("run_id")) == (previous.get("dag_id"), previous.get("dag_run")):
        for task in dag.get("tasks", []):
            if old_tasks.get(task["id"]) != task["status"] and task["status"] in {"running", "success", "failed"}:
                record("task", task["id"] + ": " + task["status"], dag["run_id"])
    elapsed = time.time() - previous.get("sample_at", time.time())
    metrics["rate"] = counter_rate(metrics["total"], previous_total, elapsed)
    metrics["neighbors_active"] = (metrics["neighbors"] is not None and previous.get("neighbors") is not None
                                   and metrics["neighbors"] > previous["neighbors"])
    metrics["qdrant_active"] = (metrics["qdrant"] is not None and previous.get("qdrant") is not None
                                 and metrics["qdrant"] > previous["qdrant"])
    if (prediction.get("id") and previous.get("prediction_id") is not None
            and prediction["id"] != previous["prediction_id"]):
        _last_prediction_seen = time.time()
    if (prometheus.get("last_scrape") and previous.get("prometheus_scrape") is not None
            and prometheus["last_scrape"] != previous["prometheus_scrape"]):
        _last_prometheus_seen = time.time()
    with _lock:
        _previous = {"dag_id": dag.get("dag_id"), "dag_run": dag.get("run_id"),
                     "dag_runs": {run["dag_id"]: run for run in dag.get("dag_runs", [])},
                     "tasks": {task["id"]: task["status"] for task in dag.get("tasks", [])},
                     "model_version": model.get("model_version"), "requests": metrics["total"],
                     "neighbors": metrics["neighbors"], "qdrant": metrics["qdrant"],
                     "prediction_id": prediction.get("id"),
                     "prometheus_scrape": prometheus.get("last_scrape"), "sample_at": time.time()}
        events = list(_events[:14])
    return {"time": stamp(), "services": services, "dag": dag, "model": model,
            "requests": metrics, "model_change_recent": time.time() - _last_model_change < 20,
            "prediction": prediction, "prediction_recent": time.time() - _last_prediction_seen < 6,
            "prometheus": prometheus, "prometheus_recent": time.time() - _last_prometheus_seen < 7,
            "ci": ci, "events": events}


@app.post("/api/demo/trigger")
def trigger():
    try:
        airflow_request("PATCH", f"/dags/{DAG_ID}", json={"is_paused": False})
        run_id = "map_demo_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        airflow_request("POST", f"/dags/{DAG_ID}/dagRuns",
                        json={"dag_run_id": run_id, "logical_date": None, "conf": {}})
        record("dag", "Запуск DAG с карты", run_id)
        return {"dag_id": DAG_ID, "run_id": run_id}
    except (OSError, ValueError, KeyError, requests.RequestException) as exc:
        raise HTTPException(status_code=502, detail=f"Airflow недоступен: {type(exc).__name__}") from exc


@app.post("/api/observer")
def observer(payload: dict):
    """Local host bridge submits sanitized GitHub state, never credentials."""
    global _ci
    run = payload.get("run")
    if not isinstance(run, dict) or not isinstance(run.get("status"), str):
        raise HTTPException(400, "invalid run")
    snapshot = {"status": run["status"][:40], "conclusion": str(run.get("conclusion") or "")[:40],
                "url": str(run.get("url") or "")[:300], "head_sha": str(run.get("head_sha") or "")[:40],
                "jobs": [{"name": str(job.get("name") or "")[:100],
                          "status": str(job.get("status") or "")[:40],
                          "conclusion": str(job.get("conclusion") or "")[:40]}
                         for job in run.get("jobs", [])[:8]], "received_unix": time.time()}
    with _lock:
        old = _ci
        _ci = snapshot
    if old is None or (old.get("status"), old.get("conclusion")) != (snapshot["status"], snapshot["conclusion"]):
        record("ci", "GitHub Actions: " + (snapshot["conclusion"] or snapshot["status"]), snapshot["head_sha"][:8])
    return {"status": "ok"}


@app.post("/api/deploy-event")
def deploy_event(payload: dict):
    stage = str(payload.get("stage") or "")[:80]
    if stage not in {"pull", "update", "verify", "success", "rollback"}:
        raise HTTPException(400, "invalid stage")
    record("deploy", {"pull": "Загрузка образа", "update": "Замена inference",
                      "verify": "Проверка релиза", "success": "Релиз завершён",
                      "rollback": "Откат релиза"}[stage], str(payload.get("revision") or "")[:8])
    return {"status": "ok"}


@app.get("/")
def index():
    return FileResponse(ROOT / "index.html")
