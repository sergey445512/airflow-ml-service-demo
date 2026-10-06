"""Confirm that the long-running traffic service is generating predictions."""
import subprocess
import time
import urllib.request

from prometheus_client.parser import text_string_to_metric_families


def predictions():
    with urllib.request.urlopen("http://inference:8000/metrics", timeout=15) as response:
        body = response.read().decode()
    return sum(sample.value for family in text_string_to_metric_families(body)
               for sample in family.samples if sample.name == "demo_predictions_total")


before = predictions()
deadline = time.monotonic() + 24
while time.monotonic() < deadline:
    time.sleep(2)
    if predictions() >= before + 2:
        break
else:
    raise AssertionError("Traffic service did not produce two successful predictions in 24 seconds")

# Exercise the profile switch with a short finite run as well.
run = subprocess.run(["python", "scripts/demo_traffic.py", "--seconds", "4",
                      "--scenario", "cycle", "--switch-every", "1", "--interval", ".25"],
                     text=True, capture_output=True, check=True, timeout=20)
assert "Traffic scenario: normal" in run.stdout, run.stdout
assert "Traffic scenario: high-risk" in run.stdout, run.stdout
print("Continuous traffic OK: predictions keep increasing and both customer profiles run")
