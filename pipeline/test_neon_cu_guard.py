"""Tests for neon_cu_guard (no network)."""
import datetime as dt
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
import neon_cu_guard as g  # noqa: E402

H = 3600  # one CU-hour in CU-seconds


def mk(start, cum_hours):
    """Readings on consecutive dates from `start` with the given cumulative CU-hours."""
    d0 = dt.date.fromisoformat(start)
    return [
        {"date": (d0 + dt.timedelta(days=i)).isoformat(),
         "compute_time_seconds": int(h * H), "updated_at": "x"}
        for i, h in enumerate(cum_hours)
    ]


def test_usage_consecutive():
    u = g.daily_usage(mk("2026-10-01", [10, 20, 35]))
    assert u == {"2026-10-02": 10.0, "2026-10-03": 15.0}


def test_usage_billing_reset():
    u = g.daily_usage(mk("2026-10-30", [300, 310, 4]))
    assert u["2026-11-01"] == 4.0


def test_usage_gap_uncomputed():
    r = mk("2026-10-01", [10, 20, 30])
    del r[1]
    assert g.daily_usage(r) == {}


def _series(last_increment, n_prior=4):
    # n_prior increments of 10 CU-hours, then the latest increment
    cum = [0]
    for _ in range(n_prior):
        cum.append(cum[-1] + 10)
    cum.append(cum[-1] + last_increment)
    return mk("2026-10-01", cum)


def test_warming_with_fewer_than_three_prior_days():
    assert g.verdict(_series(100, n_prior=2))[0] == "warming"  # 2 prior computed days


def test_ok_at_exactly_1_5x():
    assert g.verdict(_series(15))[0] == "ok"


def test_alert_above_1_5x():
    v, date, used, mean = g.verdict(_series(15.1))
    assert (v, date, mean) == ("alert", "2026-10-06", 10.0)
    assert abs(used - 15.1) < 1e-9


def test_exactly_three_prior_days_can_alert():
    assert g.verdict(_series(100, n_prior=4))[0] == "alert"


def test_cap_and_same_date_replacement():
    rs = []
    for r in mk("2026-10-01", range(0, 15)):
        rs = g.add_reading(rs, r)
    assert len(rs) == 10 and rs[0]["date"] == "2026-10-06"
    rep = dict(rs[-1], compute_time_seconds=1)
    rs = g.add_reading(rs, rep)
    assert len(rs) == 10 and rs[-1]["compute_time_seconds"] == 1


def test_stdout_has_no_number(tmp_path, monkeypatch, capsys):
    state = tmp_path / "s.json"
    today = dt.datetime.now(dt.timezone.utc).date()
    prior = mk((today - dt.timedelta(days=5)).isoformat(), [0, 10, 20, 30, 40])
    state.write_text(json.dumps(prior))
    monkeypatch.setenv("NEON_API_KEY", "k")
    monkeypatch.setenv("NEON_PROJECT_ID", "p")
    monkeypatch.setattr(g, "fetch_reading", lambda p, k, t: {
        "date": t, "compute_time_seconds": 999 * H, "updated_at": "x"})
    sent = []
    monkeypatch.setattr(g, "send_alert", lambda *a: sent.append(a))
    monkeypatch.setattr(sys, "argv", ["g", "--state", str(state)])
    assert g.main() == 0
    out = capsys.readouterr().out
    assert sent and "verdict=alert" in out
    assert not re.search(r"\d", out.replace(today.isoformat(), ""))
