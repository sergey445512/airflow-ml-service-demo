"""A small, inspectable PostgreSQL -> Qdrant index and Redis lookup cache."""
import hashlib
import json
import os

import redis
import requests

from src.pipeline import FEATURES, connect, seed_data

COLLECTION = "customers_synthetic_v1"
CACHE_TTL_SECONDS = 300


def qdrant_url():
    return os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")


def cache_client():
    return redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"),
                                socket_connect_timeout=2, socket_timeout=2)


def vector(features):
    """Put the three differently scaled raw features on comparable 0..1 axes."""
    return [(features["tenure_months"] - 1) / 71,
            (features["monthly_spend"] - 10) / 190,
            features["support_tickets"] / 8]


def cache_key(features, limit):
    document = json.dumps({"features": features, "limit": limit}, sort_keys=True,
                          separators=(",", ":"))
    return "neighbors:v1:" + hashlib.sha256(document.encode()).hexdigest()


def sync_customers():
    """Idempotently index the 1200 fixed demo customers in Qdrant."""
    seed_data()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""SELECT customer_id,tenure_months,monthly_spend,support_tickets,churn
            FROM app.customers ORDER BY customer_id""")
        rows = cur.fetchall()
    base = qdrant_url() + "/collections/" + COLLECTION
    existing = requests.get(base, timeout=10)
    if existing.status_code == 404:
        created = requests.put(base, json={"vectors": {"size": 3, "distance": "Euclid"}}, timeout=15)
        created.raise_for_status()
    else:
        existing.raise_for_status()
    for start in range(0, len(rows), 200):
        points = []
        for customer_id, tenure, spend, tickets, churn in rows[start:start + 200]:
            features = dict(zip(FEATURES, (tenure, spend, tickets)))
            points.append({"id": customer_id, "vector": vector(features),
                           "payload": {"customer_id": customer_id, **features,
                                       "historical_churn": churn}})
        response = requests.put(base + "/points?wait=true", json={"points": points}, timeout=30)
        response.raise_for_status()
    count = requests.post(base + "/points/count", json={"exact": True}, timeout=10)
    count.raise_for_status()
    indexed = count.json()["result"]["count"]
    if indexed != len(rows):
        raise RuntimeError(f"Qdrant has {indexed} points but PostgreSQL has {len(rows)} rows")
    return {"collection": COLLECTION, "indexed_customers": indexed,
            "dataset_version": "synthetic-v1"}


def find_neighbors(features, limit):
    response = requests.post(
        qdrant_url() + "/collections/" + COLLECTION + "/points/query",
        json={"query": vector(features), "limit": limit, "with_payload": True},
        timeout=10,
    )
    response.raise_for_status()
    return [{"customer_id": point["id"], "score": point["score"],
             "features": {feature: point["payload"][feature] for feature in FEATURES},
             "historical_churn": point["payload"]["historical_churn"]}
            for point in response.json()["result"]["points"]]
