"""Dependency-free unit tests for abstract_recovery pure/loop logic.

Run:  python pipeline/test_abstract_recovery.py
(No pytest dependency — plain asserts, prints PASS/FAIL and exits non-zero on failure.
Same style as LiterView's pipeline/test_cdp_drain.py.)

Scope: the s2_batch chunk loop. The network is stubbed; nothing here touches S2 or the DB.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import abstract_recovery as ar

_failures = []


def check(name, cond):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}")
    if not cond:
        _failures.append(name)


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _Stub:
    """Stands in for the `requests` module inside abstract_recovery."""

    def __init__(self, responses):
        self._responses = list(responses)   # consumed in order; last one repeats
        self.calls = 0

    def post(self, *args, **kwargs):
        self.calls += 1
        if len(self._responses) > 1:
            return self._responses.pop(0)
        return self._responses[0]


def run(dois, responses, keys):
    stub = _Stub(responses)
    real = ar.requests
    ar.requests = stub
    try:
        out = ar.s2_batch(dois, keys, rate=0.0)
    finally:
        ar.requests = real
    return out, stub.calls


BOUND = ar.S2_MAX_429_RETRIES_PER_CHUNK
ONE_CHUNK = [f"10.1000/p{n}" for n in range(3)]
TWO_CHUNKS = [f"10.1000/p{n}" for n in range(600)]

# ── A6: a permanently throttled chunk must terminate, not spin ───────────────
out, calls = run(ONE_CHUNK, [_Resp(429)], ["k1", "k2"])
check("permanent 429 terminates and yields nothing", out == {})
check(f"permanent 429 costs bound+1 = {BOUND + 1} requests for one chunk",
      calls == BOUND + 1)

out, calls = run(TWO_CHUNKS, [_Resp(429)], ["k1", "k2", "k3"])
check("429 budget is per chunk, not for the whole run",
      calls == 2 * (BOUND + 1))

# A single key never rotates, so the 429 falls straight through to the skip path.
out, calls = run(ONE_CHUNK, [_Resp(429)], ["only-key"])
check("single key spends no retries on 429", calls == 1)
out, calls = run(ONE_CHUNK, [_Resp(429)], [])
check("no key spends no retries on 429", calls == 1)

# ── the rotation itself still works below the bound ──────────────────────────
ok = [{"externalIds": {"DOI": "10.1000/P0"}, "abstract": "recovered"}]
out, calls = run(ONE_CHUNK, [_Resp(429), _Resp(200, ok)], ["k1", "k2"])
check("429 then 200 recovers on the next key", out == {"10.1000/p0": "recovered"})
check("429 then 200 costs exactly 2 requests", calls == 2)

# The budget must reset per chunk: chunk 1 burns one retry, chunk 2 must still
# get the full bound rather than inheriting a spent counter.
responses = [_Resp(429), _Resp(200, ok)] + [_Resp(429)]
out, calls = run(TWO_CHUNKS, responses, ["k1", "k2"])
check("budget resets after a chunk advances", calls == 2 + (BOUND + 1))

# ── happy path and malformed payloads are unchanged ─────────────────────────
out, calls = run(ONE_CHUNK, [_Resp(200, ok)], ["k1"])
check("200 maps normalized DOI to abstract", out == {"10.1000/p0": "recovered"})
out, calls = run(ONE_CHUNK, [_Resp(200, [None, {"abstract": None}])], ["k1"])
check("null entries are skipped", out == {})
out, calls = run(ONE_CHUNK, [_Resp(200)], ["k1"])
check("unparseable body skips the chunk", out == {} and calls == 1)
out, calls = run(ONE_CHUNK, [_Resp(500)], ["k1"])
check("500 skips the chunk", out == {} and calls == 1)

# ── is_valid_abstract: publisher previews (2026-10-22) ───────────────────────
ok, why = ar.is_valid_abstract("x" * 197 + "...", None)
check("200-char preview ending ... is refused", not ok and why == "truncated_preview")
ok, why = ar.is_valid_abstract("y" * 150 + "…", None)
check("preview ending in a unicode ellipsis is refused", not ok and why == "truncated_preview")
ok, _ = ar.is_valid_abstract("z" * 400 + " ...", None)
check("a long abstract ending in ... is kept", ok)
ok, _ = ar.is_valid_abstract("w" * 200 + ".", None)
check("a 201-char abstract ending in a period is kept", ok)

# ── summary ──────────────────────────────────────────────────────────────────
print()
if _failures:
    print(f"FAILED ({len(_failures)}): {_failures}")
    sys.exit(1)
print("ALL TESTS PASSED")
