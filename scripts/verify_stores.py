"""Check indexing, nearest-neighbor search, a real Redis hit and both web UIs."""
import time

import requests

from src.neighbors import CACHE_TTL_SECONDS, COLLECTION, cache_client, cache_key

qdrant = requests.get("http://qdrant:6333/collections/" + COLLECTION, timeout=10)
qdrant.raise_for_status()
count = requests.post("http://qdrant:6333/collections/" + COLLECTION + "/points/count",
                      json={"exact": True}, timeout=10)
count.raise_for_status()
assert count.json()["result"]["count"] == 1200, count.text
dashboard = requests.get("http://qdrant:6333/dashboard", timeout=10)
dashboard.raise_for_status()
assert "html" in dashboard.headers.get("content-type", ""), dashboard.headers

client = cache_client()
assert client.ping()
features = {"tenure_months": 17, "monthly_spend": 137.41, "support_tickets": 4}
limit = 5
key = cache_key(features, limit)
client.delete(key)
request = features | {"limit": limit}
first = requests.post("http://inference:8000/neighbors", json=request, timeout=20)
first.raise_for_status()
second = requests.post("http://inference:8000/neighbors", json=request, timeout=20)
second.raise_for_status()
miss, hit = first.json(), second.json()
assert miss["cache_status"] == "miss" and miss["cache_hit"] is False, miss
assert hit["cache_status"] == "hit" and hit["cache_hit"] is True, hit
assert hit["neighbors"] == miss["neighbors"] and len(hit["neighbors"]) == limit
assert all(1 <= point["customer_id"] <= 1200 for point in hit["neighbors"])
assert 0 < client.ttl(key) <= CACHE_TTL_SECONDS

for attempt in range(60):
    try:
        insight = requests.get("http://redisinsight:5540/api/health/", timeout=3)
        insight.raise_for_status()
        databases = requests.get("http://redisinsight:5540/api/databases", timeout=3)
        databases.raise_for_status()
        if any(db.get("host") == "redis" and db.get("port") == 6379
               for db in databases.json()):
            break
    except requests.RequestException:
        pass
    time.sleep(2)
else:
    raise TimeoutError("Redis Insight did not show the local Redis connection")
print("Stores OK: 1200 Qdrant points, nearest-neighbor results, Redis miss/hit/TTL, Qdrant and Redis Insight UIs")
