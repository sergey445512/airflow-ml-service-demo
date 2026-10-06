"""Finite synthetic traffic for the Grafana walkthrough; adds demo prediction rows."""
import argparse
import json
import random
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--url", default="http://inference:8000")
parser.add_argument("--seconds", type=int, default=120)
parser.add_argument("--scenario", choices=["normal", "high-risk"], default="normal")
args = parser.parse_args()
if not 1 <= args.seconds <= 3600:
    parser.error("seconds must be 1..3600")
rng = random.Random(42)
deadline = time.monotonic() + args.seconds
counts = {}
i = 0
while time.monotonic() < deadline:
    high = args.scenario == "high-risk"
    sample = {"tenure_months": rng.randint(1, 12) if high else rng.randint(1, 72),
              "monthly_spend": rng.randint(140, 200) if high else rng.randint(10, 200),
              "support_tickets": rng.randint(5, 8) if high else rng.randint(0, 8)}
    # Deliberate validation errors, visible as 422, every twentieth request.
    if i % 20 == 19:
        sample["tenure_months"] = -1
    request = urllib.request.Request(args.url + "/predict", data=json.dumps(sample).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
    counts[status] = counts.get(status, 0) + 1
    i += 1
    time.sleep(.5)
print(json.dumps({"scenario": args.scenario, "responses": counts}))
