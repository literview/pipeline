#!/usr/bin/env python3
"""Daily check of the Neon production branch's compute usage (ledger task neon-cu-guard, 2026-10-27).

The branch's `compute_time_seconds` is a counter in compute-unit seconds since the start of the
billing period (the 1st of the month). The per-day history endpoint answers 403 on this plan, so a
day's usage is the difference of two daily readings. State is a small JSON file holding the last
10 readings, one per UTC date.

The verdict is for the latest day: `alert` when its usage exceeds 1.5 times the mean of the up to 7
computed days before it (at least 3 such days needed), `warming` with fewer than 3, else `ok`.
Only an `alert` sends e-mail, through alert_email.py.

This repository is public, so its Actions logs are public: stdout carries the verdict and the date
and never a usage figure. The numbers go in the e-mail only.

Env: NEON_API_KEY, NEON_PROJECT_ID. Exit 0 on any verdict, 1 when the API read fails (the
workflow's failure step then e-mails).

    python pipeline/neon_cu_guard.py --state neon_cu_state.json
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.request

API = "https://console.neon.tech/api/v2"
MAX_READINGS = 10
WINDOW = 7
MIN_DAYS = 3
FACTOR = 1.5


def api_get(path: str, key: str):
    """GET one Neon API path with a 30 s timeout and one retry."""
    last = None
    for attempt in range(2):
        try:
            req = urllib.request.Request(
                API + path,
                headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt == 0:
                time.sleep(3)
    raise RuntimeError(f"Neon API read failed for {path}: {type(last).__name__}")


def fetch_reading(project: str, key: str, today: str) -> dict:
    data = api_get(f"/projects/{project}/branches", key)
    default = [b for b in data["branches"] if b.get("default")]
    if len(default) != 1:
        raise RuntimeError("expected exactly one default branch")
    br = api_get(f"/projects/{project}/branches/{default[0]['id']}", key)["branch"]
    return {
        "date": today,
        "compute_time_seconds": br["compute_time_seconds"],
        "updated_at": br["updated_at"],
    }


def fetch_endpoint_cu(project: str, key: str) -> str:
    try:
        eps = api_get(f"/projects/{project}/endpoints", key)["endpoints"]
        return "; ".join(
            f"{e.get('id', '?')}: min {e.get('autoscaling_limit_min_cu', '?')} CU, "
            f"max {e.get('autoscaling_limit_max_cu', '?')} CU"
            for e in eps
        )
    except Exception:  # noqa: BLE001 - the alert must still go out
        return "(endpoint settings could not be read)"


def add_reading(readings: list, reading: dict) -> list:
    """One reading per UTC date (same date replaces), sorted, capped at the last 10."""
    out = [r for r in readings if r["date"] != reading["date"]] + [reading]
    out.sort(key=lambda r: r["date"])
    return out[-MAX_READINGS:]


def daily_usage(readings: list) -> dict:
    """Map date -> CU-hours for each date whose previous calendar day also has a reading.

    A counter that went down is a new billing period: the day's usage is the reading itself.
    """
    by_date = {r["date"]: r["compute_time_seconds"] for r in readings}
    out = {}
    for d, val in by_date.items():
        prev = (dt.date.fromisoformat(d) - dt.timedelta(days=1)).isoformat()
        if prev not in by_date:
            continue
        diff = val - by_date[prev]
        out[d] = (val if diff < 0 else diff) / 3600.0
    return out


def verdict(readings: list):
    """Return (verdict, latest_date, latest_usage, trailing_mean); numbers None when uncomputed."""
    if not readings:
        return "warming", None, None, None
    latest = max(r["date"] for r in readings)
    usage = daily_usage(readings)
    if latest not in usage:
        return "warming", latest, None, None
    prior = [usage[d] for d in sorted(usage) if d < latest][-WINDOW:]
    if len(prior) < MIN_DAYS:
        return "warming", latest, usage[latest], None
    mean = sum(prior) / len(prior)
    if usage[latest] > FACTOR * mean:
        return "alert", latest, usage[latest], mean
    return "ok", latest, usage[latest], mean


def load_state(path: str) -> list:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return []


def save_state(path: str, readings: list) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(readings, f, indent=1)


def send_alert(project: str, key: str, date: str, used: float, mean: float) -> None:
    body = (
        f"Neon production compute used {used:.1f} CU-hours on {date} (UTC), more than {FACTOR} "
        f"times the trailing mean of {mean:.1f} CU-hours.\n"
        f"Endpoint settings now: {fetch_endpoint_cu(project, key)}\n"
        f"Console: https://console.neon.tech/app/projects/{project}"
    )
    here = os.path.dirname(os.path.abspath(__file__))
    # alert_email.py prints the alert; its output is discarded because this repo's logs are public.
    subprocess.run(
        [sys.executable, os.path.join(here, "alert_email.py"),
         "--title", f"[LiterView] Neon compute: {used:.1f} CU-hours on {date} (trailing mean {mean:.1f})",
         "--body", body],
        stdout=subprocess.DEVNULL, check=False,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    args = ap.parse_args()

    key = os.environ.get("NEON_API_KEY", "").strip()
    project = os.environ.get("NEON_PROJECT_ID", "").strip()
    if not (key and project):
        print("NEON_API_KEY / NEON_PROJECT_ID not set")
        return 1
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    try:
        reading = fetch_reading(project, key, today)
    except Exception as e:  # noqa: BLE001
        print(f"neon-cu-guard: read failed ({e})")
        return 1

    readings = add_reading(load_state(args.state), reading)
    save_state(args.state, readings)
    v, date, used, mean = verdict(readings)
    print(f"verdict={v} date={date}")
    if v == "alert":
        send_alert(project, key, date, used, mean)
    return 0


if __name__ == "__main__":
    sys.exit(main())
