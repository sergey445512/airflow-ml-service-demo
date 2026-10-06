"""Make the local Redis visible in Redis Insight without manual setup."""
import time

import requests

BASE = "http://redisinsight:5540/api/databases"
for attempt in range(60):
    try:
        response = requests.get(BASE, timeout=3)
        response.raise_for_status()
        break
    except requests.RequestException:
        time.sleep(2)
else:
    raise TimeoutError("Redis Insight did not become ready")

connections = response.json()
if not any(db.get("host") == "redis" and db.get("port") == 6379 for db in connections):
    created = requests.post(BASE, json={"host": "redis", "port": 6379,
                                        "name": "ML demo cache"}, timeout=15)
    created.raise_for_status()
    connections = requests.get(BASE, timeout=10).json()
if not any(db.get("host") == "redis" and db.get("port") == 6379 for db in connections):
    raise RuntimeError("Redis Insight does not show the local Redis connection")
print("Redis Insight connection ready: ML demo cache", flush=True)
