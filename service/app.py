import json
import threading
import time
from pathlib import Path

import mlflow.sklearn
import numpy as np
import pandas as pd
import redis
import requests
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from src.pipeline import FEATURES, connect, initialize
from src.revision import code_revision
from src.neighbors import CACHE_TTL_SECONDS, COLLECTION, cache_client, cache_key, find_neighbors
from service import metrics
from service.model_registry import MODEL_NAME, PRODUCTION_ALIAS, production_version

app = FastAPI(title="Churn demo — online inference", version="1.0")
_lock = threading.Lock()
_cache = {}
MODEL_CONTRACT = "churn-v1"


@app.middleware("http")
async def measure_prediction(request, call_next):
    if request.url.path not in {"/churn/predict", "/predict"} or request.method != "POST":
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

def active_model(version):
    """Read lineage from MLflow; training can happen in any process or repository."""
    if not version.run_id:
        raise HTTPException(503, "Версия в MLflow не связана с запуском обучения")
    try:
        run = MlflowClient().get_run(version.run_id)
    except MlflowException as exc:
        raise HTTPException(503, "Не удалось прочитать запуск модели в MLflow") from exc
    tags = run.data.tags
    contract = version.tags.get("model_contract") or tags.get("model_contract")
    if contract and contract != MODEL_CONTRACT:
        raise HTTPException(503, f"Модель требует контракт {contract}; сервис поддерживает {MODEL_CONTRACT}")
    return {
        "mlflow_run_id": version.run_id,
        "git_commit": tags.get("git_commit"),
        "model_uri": version.source,
        "roc_auc": run.data.metrics.get("roc_auc"),
        "airflow_run_id": tags.get("airflow_run_id"),
        "training_source": tags.get("training_source") or ("airflow" if tags.get("airflow_run_id") else "external"),
        "model_contract": contract or "legacy-churn-v1",
        "registered_model": MODEL_NAME,
        "model_version": str(version.version),
        "model_alias": PRODUCTION_ALIAS,
    }


def serving_model():
    """Load and validate the selected version before recording it as served."""
    version = production_version()
    if version is None:
        raise HTTPException(503, "В MLflow пока нет production-модели. Зарегистрируйте версию и назначьте alias production.")
    with _lock:
        if _cache.get("model_version") != str(version.version):
            info = active_model(version)
            try:
                with metrics.LOAD_TIME.time():
                    loaded = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{version.version}")
                    sample = pd.DataFrame([{"tenure_months": 6, "monthly_spend": 120.0,
                                            "support_tickets": 5}], columns=FEATURES)
                    probability = np.asarray(loaded.predict_proba(sample))
                    if (list(loaded.classes_) != [0, 1] or probability.shape != (1, 2)
                            or not np.isfinite(probability).all()
                            or not np.allclose(probability.sum(axis=1), 1)
                            or np.any(probability < 0) or np.any(probability > 1)):
                        raise ValueError("Model must return probabilities for classes 0 and 1")
            except Exception as exc:
                metrics.LOADS.labels("error").inc()
                raise HTTPException(503, "Не удалось загрузить или проверить выбранную модель") from exc
            with connect() as conn, conn.cursor() as cur:
                cur.execute("""INSERT INTO app.model_runs
                    (mlflow_run_id,airflow_run_id,git_commit,roc_auc,model_uri,training_source)
                    VALUES (%(mlflow_run_id)s,%(airflow_run_id)s,%(git_commit)s,%(roc_auc)s,%(model_uri)s,%(training_source)s)
                    ON CONFLICT (mlflow_run_id) DO NOTHING""", info)
                cur.execute("""INSERT INTO app.active_model
                    (singleton,mlflow_run_id,registered_model,model_version) VALUES(true,%s,%s,%s)
                    ON CONFLICT(singleton) DO UPDATE SET
                        mlflow_run_id=excluded.mlflow_run_id,
                        registered_model=excluded.registered_model,
                        model_version=excluded.model_version,
                        activated_at=now()
                    WHERE (app.active_model.mlflow_run_id,app.active_model.registered_model,app.active_model.model_version)
                        IS DISTINCT FROM (excluded.mlflow_run_id,excluded.registered_model,excluded.model_version)""",
                    (info["mlflow_run_id"], MODEL_NAME, int(version.version)))
            _cache.update(model_version=info["model_version"], model=loaded, info=info)
            metrics.LOADS.labels("success").inc()
        return _cache["info"], _cache["model"]


@app.get("/health")
def health():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/model")
def model_info():
    return serving_model()[0]


@app.get("/version")
def version():
    return code_revision()


@app.post("/predict", include_in_schema=False)
@app.post("/churn/predict")
def predict(customer: Customer):
    """Predict churn probability for a single customer; record the prediction in Postgres."""
    info, model = serving_model()
    features = customer.model_dump()
    probability = float(model.predict_proba(pd.DataFrame([features], columns=FEATURES))[0, 1])
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO app.predictions
            (mlflow_run_id,registered_model,model_version,features,probability)
            VALUES(%s,%s,%s,%s::jsonb,%s)""",
            (info["mlflow_run_id"], MODEL_NAME, int(info["model_version"]), json.dumps(features), probability))
    metrics.PROBABILITY.observe(probability)
    metrics.PREDICTIONS.labels(str(int(probability >= 0.5))).inc()
    for feature, histogram in metrics.FEATURES.items():
        histogram.observe(features[feature])
    return {"churn_probability": round(probability, 4), "prediction": int(probability >= 0.5), **info}


@app.post("/neighbors")
def neighbors(customer: SimilarCustomer):
    """Show similar historical customers; repeated requests are cached in Redis"""
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
