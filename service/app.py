import json
import threading
import time
from pathlib import Path

import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from src.pipeline import FEATURES, connect, initialize
from src.revision import code_revision
from service import metrics

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


@app.on_event("startup")
def startup():
    initialize()


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


@app.get("/", response_class=HTMLResponse)
def home():
    return Path(__file__).with_name("index.html").read_text(encoding="utf-8")
