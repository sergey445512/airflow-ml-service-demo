import json
import logging
import threading
import time
from pathlib import Path

import mlflow.sklearn
import pandas as pd
import redis
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from src.pipeline import FEATURES, connect, initialize
from src.revision import code_revision
from src.neighbors import CACHE_TTL_SECONDS, COLLECTION, cache_client, cache_key, find_neighbors
from service import metrics
from service.model_registry import sync_production_alias

app = FastAPI(title="Churn demo — online inference", version="1.0")
_lock = threading.Lock()
_cache = {}


@app.middleware("http")
async def measure_prediction(request, call_next):
    if request.url.path != "/predict" or request.method != "POST":
        return await call_next(request)
    started = time.perf_counter()
    status = "500"
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    finally:
        metrics.REQUESTS.labels(status).inc()
        metrics.LATENCY.observe(time.perf_counter() - started)


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


class Customer(BaseModel):
    tenure_months: int = Field(ge=1, le=72, examples=[6])
    monthly_spend: float = Field(ge=10, le=200, examples=[120])
    support_tickets: int = Field(ge=0, le=8, examples=[5])


class SimilarCustomer(Customer):
    limit: int = Field(default=5, ge=1, le=10)


@app.on_event("startup")
def startup():
    initialize()
    threading.Thread(target=sync_registry_loop, daemon=True, name="mlflow-production-alias").start()


def sync_registry_loop():
    while True:
        try:
            sync_production_alias(active_model())
        except HTTPException as exc:
            if exc.status_code != 503:
                logging.exception("Could not sync the MLflow production alias")
        except Exception:
            logging.exception("Could not sync the MLflow production alias")
        time.sleep(15)


def active_model():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT r.mlflow_run_id,r.git_commit,r.model_uri,r.roc_auc,r.airflow_run_id
            FROM app.active_model a JOIN app.model_runs r USING(mlflow_run_id) WHERE a.singleton""")
        row = cur.fetchone()
    if row is None:
        raise HTTPException(503, "Модель ещё не обучена. Запустите demo_04_train_and_publish в Airflow.")
    return dict(zip(["mlflow_run_id", "git_commit", "model_uri", "roc_auc", "airflow_run_id"], row))


@app.get("/health")
def health():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/model")
def model_info():
    return active_model()


@app.get("/version")
def version():
    return code_revision()


@app.post("/predict")
def predict(customer: Customer):
    info = active_model()
    with _lock:
        if _cache.get("run_id") != info["mlflow_run_id"]:
            try:
                with metrics.LOAD_TIME.time():
                    loaded = mlflow.sklearn.load_model(info["model_uri"])
            except Exception:
                metrics.LOADS.labels("error").inc()
                raise
            metrics.LOADS.labels("success").inc()
            _cache.update(run_id=info["mlflow_run_id"], model=loaded)
        model = _cache["model"]
    features = customer.model_dump()
    probability = float(model.predict_proba(pd.DataFrame([features], columns=FEATURES))[0, 1])
    with connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO app.predictions(mlflow_run_id,features,probability) VALUES(%s,%s::jsonb,%s)", (info["mlflow_run_id"], json.dumps(features), probability))
    metrics.PROBABILITY.observe(probability)
    metrics.PREDICTIONS.labels(str(int(probability >= 0.5))).inc()
    for feature, histogram in metrics.FEATURES.items():
        histogram.observe(features[feature])
    return {"churn_probability": round(probability, 4), "prediction": int(probability >= 0.5), **info}


@app.post("/neighbors")
def neighbors(customer: SimilarCustomer):
    """Show similar historical customers; repeated requests are cached in Redis."""
    features = {feature: getattr(customer, feature) for feature in FEATURES}
    key = cache_key(features, customer.limit)
    client = None
    try:
        client = cache_client()
        cached = client.get(key)
    except redis.RedisError:
        cached = None
        client = None
    if cached is not None:
        metrics.NEIGHBOR_CACHE.labels("hit").inc()
        return {"neighbors": json.loads(cached), "cache_hit": True,
                "cache_status": "hit", "collection": COLLECTION}

    status = "miss" if client is not None else "unavailable"
    try:
        with metrics.QDRANT_LATENCY.time():
            found = find_neighbors(features, customer.limit)
    except requests.RequestException as exc:
        metrics.QDRANT_QUERIES.labels("error").inc()
        raise HTTPException(503, "Поиск похожих клиентов пока недоступен") from exc
    metrics.QDRANT_QUERIES.labels("success").inc()
    if client is not None:
        try:
            client.setex(key, CACHE_TTL_SECONDS, json.dumps(found))
        except redis.RedisError:
            status = "unavailable"
    metrics.NEIGHBOR_CACHE.labels(status).inc()
    return {"neighbors": found, "cache_hit": False,
            "cache_status": status, "collection": COLLECTION}


@app.get("/", response_class=HTMLResponse)
def home():
    return Path(__file__).with_name("index.html").read_text(encoding="utf-8")
