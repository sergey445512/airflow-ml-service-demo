"""Send varied synthetic requests to keep the inference dashboard moving."""
import argparse
import json
import random
import time
import urllib.error
import urllib.request


def make_customer(rng, scenario):
    high = scenario == "high-risk"
    return {
        "tenure_months": rng.randint(1, 12) if high else rng.randint(1, 72),
        "monthly_spend": rng.randint(140, 200) if high else rng.randint(10, 200),
        "support_tickets": rng.randint(5, 8) if high else rng.randint(0, 8),
    }


def send(url, customer):
    request = urllib.request.Request(
        url + "/predict", data=json.dumps(customer).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
            return response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://inference:8000")
    parser.add_argument("--seconds", type=int, default=120,
                        help="Duration for a manual run; ignored with --forever")
    parser.add_argument("--forever", action="store_true",
                        help="Keep sending requests until the container is stopped")
    parser.add_argument("--scenario", choices=["normal", "high-risk", "cycle"], default="normal")
    parser.add_argument("--switch-every", type=float, default=180,
                        help="Seconds between normal and high-risk phases in cycle mode")
    parser.add_argument("--interval", type=float, default=.5, help="Seconds between requests")
    args = parser.parse_args()
    if not args.forever and not 1 <= args.seconds <= 3600:
        parser.error("seconds must be 1..3600")
    if not .1 <= args.interval <= 60 or args.switch_every < 1:
        parser.error("interval must be 0.1..60 and switch-every must be at least 1")

    rng = random.Random()
    started = time.monotonic()
    deadline = float("inf") if args.forever else started + args.seconds
    next_summary = started + 60
    current_phase = None
    counts = {}
    i = 0
    while time.monotonic() < deadline:
        now = time.monotonic()
        scenario = ("normal" if int((now - started) / args.switch_every) % 2 == 0 else "high-risk") if args.scenario == "cycle" else args.scenario
        if scenario != current_phase:
            current_phase = scenario
            print(f"Traffic scenario: {scenario}", flush=True)

        customer = make_customer(rng, scenario)
        # Every twentieth request demonstrates validation failures in Grafana.
        if i % 20 == 19:
            customer["tenure_months"] = -1
        try:
            status = send(args.url, customer)
            counts[status] = counts.get(status, 0) + 1
        except (urllib.error.URLError, TimeoutError) as exc:
            counts["connection_error"] = counts.get("connection_error", 0) + 1
            if not args.forever:
                raise RuntimeError("Inference is unavailable") from exc
        i += 1
        if time.monotonic() >= next_summary:
            print(json.dumps({"scenario": scenario, "responses_last_minute": counts}), flush=True)
            counts = {}
            next_summary = time.monotonic() + 60
        time.sleep(min(args.interval, max(0, deadline - time.monotonic())))
    print(json.dumps({"scenario": current_phase, "responses": counts}), flush=True)


if __name__ == "__main__":
    main()
