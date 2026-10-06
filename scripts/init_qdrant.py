"""Prepare the example collection before Airflow and inference start."""
import time

import requests

from src.neighbors import qdrant_url, sync_customers

for attempt in range(60):
    try:
        response = requests.get(qdrant_url() + "/collections", timeout=3)
        response.raise_for_status()
        break
    except requests.RequestException:
        time.sleep(2)
else:
    raise TimeoutError("Qdrant did not become ready")
print(sync_customers(), flush=True)
