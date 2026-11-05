"""backfill_arxiv_author_ids.py — write OpenAlex author IDs into the author elements of arXiv papers.

Specification: docs/tierb/ARXIV_AUTHOR_ID_BACKFILL_WORKORDER.md, sections 3 to 8 as amended by
section 11 (ledger task arxiv-author-id-backfill, docs/ledger/arxiv-author-id-backfill.md).

  python pipeline/backfill_arxiv_author_ids.py [--commit [--prod]] [--dsn URL]
      [--min-age-days 14] [--max-age-days N] [--limit N] [--created-from D] [--created-to D]
      [--paper-ids FILE] [--batch-size 50] [--canary N] [--workers 1..4] [--cache FILE]
      [--cache-max-age-days 7] [--checkpoint FILE] [--age-at-found-log FILE]
      [--retry-outcomes not-found,api-error,changed-underneath] [--json FILE]
  python pipeline/backfill_arxiv_author_ids.py --revert CHECKPOINT [--commit [--prod]]
      [--paper-ids FILE] [--limit N] [--checkpoint REVERT_LOG] [--json FILE]

Selection (section 3): source 'arXiv', created at least --min-age-days ago, at least one element of
author_elements() without an OpenAlex ID; ordered by created_at, id. --max-age-days, the daily
pass's window (docs/ledger/arxiv-daily-pass.md), also requires created_at newer than that many days,
so a paper OpenAlex never indexed is retried daily until it ages out. --created-from is inclusive,
--created-to exclusive. Papers listed in the checkpoint are skipped, except outcomes named in
--retry-outcomes and batches checkpointed as `failed`.

--age-at-found-log appends one line per found work with the paper's age in days at the lookup, since
published_date or, when that is NULL, created_at. A month of these settles whether the daily pass's
14-day floor can come down. It is written in a dry run too, since it records the lookup, not a write.

Per paper (section 4 and section 11): the stored array S and the OpenAlex work W, looked up by the
arXiv DOI and, after a 404, by the paper's DOI, both in the free /works/doi: form.
  count-differs   len(S) != len(W.authorships); nothing written
  no-change       no element qualifies; no UPDATE
  updated         at position i the element gets `openalex_id` (URL form) when it has none, the
                  authorship has an author ID and the names agree (author_names.name_agreement);
                  the same element also gets `orcid` and `affiliation` (institutions[0]) when OpenAlex
                  has them and the element lacks the key. An author ID that would appear at two
                  positions is written at neither (the other positions are written). The ORCID is left
                  out, and the pair reported, when the ID already resolves to a row holding another
                  ORCID, or when a row holding this ORCID has a name that does not agree. Name, order,
                  length and other keys never change.
  changed-underneath  the guarded UPDATE (WHERE authors = <array as read>) changed no row
  not-found / api-error  404 on every route / a 5xx or network error after one retry (5 s)
A 429, or a call charged credits, stops the run (exit 3); the batch in flight is not written.

Batches (--batch-size, default 50): read the arrays, look the works up (--workers threads behind a
shared 10-per-second limiter), then one transaction with lock_timeout 5s: the ORCID-holder and
ID-row reads, the guarded UPDATEs (each fires auto_link_paper_authors_update_trigger), and a read of
what the trigger did. A deadlock, lock timeout or serialization failure retries the batch up to 5
times with backoff, then the batch is checkpointed as `failed` and the run continues; any other
database error stops the run (exit 4). Checkpoint lines are appended only after the commit: one line
per paper (outcome, age, for a found work the position counts, and for `updated` the array before
the write and the changes, from which the array after the write follows) and one line per batch (the
provisional rows it dissolved, with the papers they held, and the redirects it
wrote or repointed). --canary N stops after N batches.

Dry run is the default: a read-only session, no UPDATE, no checkpoint lines; the cache is written.

--revert CHECKPOINT writes each listed paper's before-array back where the stored array still equals
the after-array (the same guarded UPDATE), batch by batch, and deletes the `provisional-dissolved`
redirects the run wrote for rows those papers were linked to. It reports what it cannot undo.

Guard: --commit (also with --revert) refuses a database whose host contains NEON_ENDPOINT_ID
(production), or any database when NEON_ENDPOINT_ID is unset, unless --prod is given. --commit also
requires migration 050 (author_link_moves). Every run requires the update trigger enabled and
author_openalex_ids present. Connection strings and hosts are never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
load_dotenv(ROOT / ".env.local", override=False)

from author_names import name_agreement  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

OPENALEX_API = "https://api.openalex.org"
MAILTO = os.environ.get("OPENALEX_EMAIL") or "literview@proton.me"
SELECT = "id,doi,publication_date,created_date,authorships"
RATE_PER_S = 10
MAX_WORKERS = 4
RETRY_DELAY_S = 5
TIMEOUT_S = 20
CREDITS_HEADER = "X-RateLimit-Credits-Used"
DB_RETRIES = 5
LOCK_TIMEOUT = "5s"
TRIGGER = "auto_link_paper_authors_update_trigger"
PD = "provisional-dissolved"
OUTCOMES = ("updated", "no-change", "not-found", "count-differs", "changed-underneath", "api-error", "failed")
RETRYABLE = (psycopg2.errors.DeadlockDetected, psycopg2.errors.LockNotAvailable,
             psycopg2.errors.SerializationFailure)
RETRYABLE_CODES = {"40P01", "55P03", "40001"}
AGE_BUCKETS = ((0, 30), (31, 60), (61, 180), (181, 365), (366, 10**6))
SLEEP = time.sleep


def backoff_s(attempt: int) -> int:
    return 2 ** attempt


def log_print(*a, **k):
    print(*a, **k, flush=True)


# ---------------------------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------------------------

def oa_url(x) -> str | None:
    """An OpenAlex author ID in the form author_openalex_ids holds (https://openalex.org/A...)."""
    if not isinstance(x, str) or not x.strip():
        return None
    tail = x.strip().rstrip("/").rsplit("/", 1)[-1]
    return f"https://openalex.org/{tail}" if re.fullmatch(r"[Aa]\d+", tail) else None


def norm_orcid(x) -> str | None:
    """author_norm_orcid() of migration 044."""
    if not isinstance(x, str):
        return None
    s = re.sub(r"^https?://orcid\.org/", "", x.strip())
    return s if len(s) >= 15 else None


def crawler_orcid(x) -> str | None:
    """The ORCID as normalize_openalex() keeps it (the value as given, dropped when malformed)."""
    if not isinstance(x, str) or not x:
        return None
    bare = x.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip()
    return x if bare and len(bare) >= 10 else None


def arxiv_doi(arxiv_id: str) -> str:
    return f"10.48550/arXiv.{re.sub(r'v[0-9]+$', '', arxiv_id)}"


def has_value(v) -> bool:
    return v is not None and not (isinstance(v, str) and not v.strip())


def as_date(v) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def iso(v) -> str | None:
    return v.isoformat() if hasattr(v, "isoformat") else (None if v is None else str(v))


def write_json(path, obj) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8", newline="") as f:
        f.write(json.dumps(obj, indent=1, ensure_ascii=False, default=str) + "\n")


def is_production(dsn: str) -> bool:
    endpoint = (os.environ.get("NEON_ENDPOINT_ID") or "").strip().lower()
    if not endpoint:
        return True
    host = psycopg2.extensions.parse_dsn(dsn).get("host") or ""
    return endpoint in host.lower()


def is_retryable(e: Exception) -> bool:
    return isinstance(e, RETRYABLE) or getattr(e, "pgcode", None) in RETRYABLE_CODES


# ---------------------------------------------------------------------------------------------
# element rules
# ---------------------------------------------------------------------------------------------

@dataclass
class Plan:
    outcome: str                       # update | no-change | count-differs
    new: list | None = None
    changes: list = field(default_factory=list)
    counts: dict = field(default_factory=dict)
    orcid_dropped: list = field(default_factory=list)
    authorships: int = 0
    authorships_with_id: int = 0


POSITION_COUNTS = ("given_id", "no_id", "name_mismatch", "duplicate_id", "has_id", "exact", "initials",
                   "variant", "orcid", "affiliation", "orcid_dropped")


def candidate_orcids(work: dict) -> set[str]:
    out = set()
    for s in (work or {}).get("authorships") or []:
        o = norm_orcid(crawler_orcid(((s or {}).get("author") or {}).get("orcid")))
        if o:
            out.add(o)
    return out


def candidate_ids(work: dict) -> set[str]:
    out = set()
    for s in (work or {}).get("authorships") or []:
        o = oa_url(((s or {}).get("author") or {}).get("id"))
        if o:
            out.add(o)
    return out


def plan_paper(stored, work: dict, holders: dict, id_rows: dict | None = None) -> Plan:
    """Section 4 and section 11 for one paper. `stored` is not modified.

    `holders` maps a normalized ORCID to the row holding it ({id, name}); `id_rows` maps an OpenAlex
    ID (URL form) to the row it resolves to through author_openalex_ids ({id, name, orcid}). An ORCID
    accepted here is registered in both, so later papers of the same batch are checked against it.
    A position qualifies when its element has no ID, the authorship has one and the names agree; an ID
    that would then appear at two positions of the paper (written or already stored) is written at
    neither. An ORCID is left out, and the pair logged, when the ID's row holds another ORCID or a row
    with another name holds this one."""
    counts = dict.fromkeys(POSITION_COUNTS, 0)
    id_rows = {} if id_rows is None else id_rows
    ships = (work or {}).get("authorships") or []
    stored = stored if isinstance(stored, list) else []
    plan = Plan("no-change", counts=counts, authorships=len(ships),
                authorships_with_id=sum(1 for s in ships if oa_url(((s or {}).get("author") or {}).get("id"))))
    if len(stored) != len(ships):
        plan.outcome = "count-differs"
        return plan

    cands: list[tuple[int, dict, dict, str, str]] = []
    stored_ids: list[str] = []
    for i, (el, ship) in enumerate(zip(stored, ships), 1):
        if not isinstance(el, dict) or not isinstance(el.get("name"), str) or not has_value(el.get("name")):
            continue
        if has_value(el.get("openalex_id")):
            counts["has_id"] += 1
            stored_ids.append(el["openalex_id"])
            continue
        ship = ship or {}
        author = ship.get("author") or {}
        oa = oa_url(author.get("id"))
        if not oa:
            counts["no_id"] += 1
            continue
        verdict = name_agreement(el["name"], author.get("display_name"), ship.get("raw_author_name"))
        if verdict == "mismatch":
            counts["name_mismatch"] += 1
            continue
        cands.append((i, el, ship, oa, verdict))

    all_ids = stored_ids + [c[3] for c in cands]
    new: list = list(stored)
    changes: list = []
    for i, el, ship, oa, verdict in cands:
        if all_ids.count(oa) > 1:
            counts["duplicate_id"] += 1
            continue
        counts[verdict] += 1
        author = ship.get("author") or {}
        out = dict(el)
        out["openalex_id"] = oa
        change = {"position": i, "openalex_id": oa}
        orcid = crawler_orcid(author.get("orcid"))
        if orcid and "orcid" not in el:
            key = norm_orcid(orcid)
            id_row = id_rows.get(oa)
            holder = holders.get(key) if key else None
            drop = None
            if key and id_row is not None and id_row.get("orcid") and id_row["orcid"] != key:
                drop = {"position": i, "name": el["name"], "orcid": key, "row_id": id_row.get("id"),
                        "row_name": id_row.get("name"), "row_orcid": id_row["orcid"],
                        "reason": "id-row-orcid-differs"}
            elif holder is not None and name_agreement(el["name"], holder.get("name"), None) == "mismatch":
                drop = {"position": i, "name": el["name"], "orcid": key, "row_id": holder.get("id"),
                        "row_name": holder.get("name"), "reason": "holder-name-differs"}
            if drop:
                counts["orcid_dropped"] += 1
                plan.orcid_dropped.append(drop)
            else:
                out["orcid"] = orcid
                change["orcid"] = orcid
                if key:
                    holders.setdefault(key, {"id": None, "name": el["name"]})
                    if id_row is None:
                        id_rows[oa] = {"id": None, "name": el["name"], "orcid": key}
                    elif not id_row.get("orcid"):
                        id_row["orcid"] = key
        insts = ship.get("institutions") or []
        aff = (insts[0] or {}).get("display_name") if insts else None
        if has_value(aff) and "affiliation" not in el:
            out["affiliation"] = aff
            change["affiliation"] = aff
        new[i - 1] = out
        changes.append(change)

    counts["given_id"] = len(changes)
    counts["orcid"] = sum(1 for c in changes if "orcid" in c)
    counts["affiliation"] = sum(1 for c in changes if "affiliation" in c)
    plan.new = new
    plan.changes = changes
    plan.outcome = "update" if changes else "no-change"
    return plan


def after_array(before: list, changes: list) -> list:
    """The array an `updated` checkpoint line implies."""
    out = [dict(e) if isinstance(e, dict) else e for e in before]
    for c in changes:
        el = out[c["position"] - 1]
        for k, v in c.items():
            if k != "position":
                el[k] = v
    return out


# ---------------------------------------------------------------------------------------------
# OpenAlex
# ---------------------------------------------------------------------------------------------

class RateLimiter:
    """Request starts at least 1/rate seconds apart, shared by every worker."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_at)
            self.next_at = slot + self.interval
        if slot > now:
            SLEEP(slot - now)


class HttpStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.requests = 0
        self.credits = 0.0
        self.charged_calls = 0
        self.statuses: dict[str, int] = {}
        self.latency: list[float] = []


_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_local, "session", None)
    if s is None:
        s = _local.session = requests.Session()
    return s


class OpenAlexClient:
    def __init__(self, rate: float = RATE_PER_S):
        self.limiter = RateLimiter(rate)
        self.stats = HttpStats()
        self.stop = threading.Event()
        self.stop_reason: str | None = None

    def halt(self, reason: str) -> None:
        with self.stats.lock:
            if self.stop_reason is None:
                self.stop_reason = reason
        self.stop.set()

    def get(self, doi: str) -> tuple[int | None, dict | None]:
        """One /works/doi: call with one retry on a 5xx or network error. Returns (status, body);
        status None after a network error, or when the run was stopped before the call."""
        url = f"{OPENALEX_API}/works/doi:{urllib.parse.quote(doi, safe='/:;()<>[]._-')}"
        status = None
        for attempt in (1, 2):
            if self.stop.is_set():
                return None, None
            self.limiter.wait()
            resp, status, body, used = None, None, None, 0.0
            t0 = time.monotonic()
            try:
                resp = _session().get(url, params={"mailto": MAILTO, "select": SELECT}, timeout=TIMEOUT_S)
                status = resp.status_code
                if status == 200:
                    body = resp.json()
            except (requests.RequestException, ValueError):
                status = None
            with self.stats.lock:
                self.stats.requests += 1
                self.stats.latency.append(time.monotonic() - t0)
                self.stats.statuses[str(status)] = self.stats.statuses.get(str(status), 0) + 1
                if resp is not None:
                    try:
                        used = float(resp.headers.get(CREDITS_HEADER) or 0)
                    except ValueError:
                        used = 0.0
                    self.stats.credits += used
                    if used > 0:
                        self.stats.charged_calls += 1
            if used > 0:
                self.halt("credits-charged")
            if status == 200 and isinstance(body, dict):
                return status, body
            if status == 429:
                self.halt("rate-limited")
                return status, None
            if status is not None and status < 500 and status != 200:
                return status, None
            if attempt == 1:
                SLEEP(RETRY_DELAY_S)
        return status, None


@dataclass
class Lookup:
    kind: str                     # found | not-found | api-error | stopped
    work: dict | None = None
    route: str | None = None
    statuses: list = field(default_factory=list)
    fetched_at: float | None = None
    cached: bool = False


def lookup(paper: dict, client: OpenAlexClient) -> Lookup:
    tries = [("arxiv_doi", arxiv_doi(paper["arxiv_id"]))]
    own = (paper.get("doi") or "").strip()
    if own and own.lower() != tries[0][1].lower():
        tries.append(("paper_doi", own))
    look = Lookup("not-found")
    for route, doi in tries:
        if client.stop.is_set():
            look.kind = "stopped"
            return look
        status, body = client.get(doi)
        look.statuses.append(status)
        if status == 200 and body is not None:
            look.kind, look.work, look.route, look.fetched_at = "found", body, route, time.time()
            return look
        if status == 429 or (status is None and client.stop.is_set()):
            look.kind = "stopped"
            return look
        if status != 404:
            look.kind = "api-error"
            return look
    return look


class Cache:
    """--cache: one JSON line per fetched work, keyed by paper id; the last line wins."""

    def __init__(self, path, max_age_days: float):
        self.path = Path(path)
        self.max_age_s = max_age_days * 86400
        self.entries: dict[str, dict] = {}
        if self.path.exists():
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        e = json.loads(line)
                        self.entries[e["paper_id"]] = e

    def get(self, pid: str) -> dict | None:
        e = self.entries.get(pid)
        if e and time.time() - e["fetched_at"] <= self.max_age_s:
            return e
        return None

    def add(self, items: list[dict]) -> None:
        if not items:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            for e in items:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
                self.entries[e["paper_id"]] = e


class Checkpoint:
    def __init__(self, path):
        self.path = Path(path)
        self.papers: dict[str, dict] = {}
        self.batches: list[dict] = []
        if self.path.exists():
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    e = json.loads(line)
                    if e.get("type") == "paper":
                        self.papers[e["id"]] = e
                    elif e.get("type") == "batch":
                        self.batches.append(e)

    def skip_ids(self, retry: set[str]) -> set[str]:
        return {pid for pid, e in self.papers.items() if e["outcome"] != "failed" and e["outcome"] not in retry}

    def append(self, lines: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            for e in lines:
                f.write(json.dumps(e, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        for e in lines:
            if e.get("type") == "paper":
                self.papers[e["id"]] = e
            elif e.get("type") == "batch":
                self.batches.append(e)


# ---------------------------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------------------------

@dataclass
class Selection:
    min_age_days: int
    max_age_days: int | None = None
    created_from: str | None = None
    created_to: str | None = None
    paper_ids: set | None = None


PROVISIONAL = """au.openalex_id IS NULL AND au.orcid IS NULL
   AND NOT EXISTS (SELECT 1 FROM author_openalex_ids a WHERE a.author_id = au.id)"""


class Store:
    """All SQL of the script. Reads outside a batch end their transaction at once."""

    def __init__(self, conn, readonly: bool):
        self.conn = conn
        self.readonly = readonly
        if readonly:
            conn.set_session(readonly=True)

    def _rows(self, sql, params=None):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()] if cur.description else []

    def _one(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            r = cur.fetchone()
            return r[0] if r else None

    def preflight(self) -> dict:
        out = {
            "update_trigger": self._one("""SELECT tgenabled::text FROM pg_trigger
                                            WHERE tgrelid = 'papers'::regclass AND tgname = %s""", [TRIGGER]),
            "alias_table": self._one("SELECT to_regclass('public.author_openalex_ids') IS NOT NULL"),
            "log_table": self._one("SELECT to_regclass('public.author_link_moves') IS NOT NULL"),
        }
        out["seed_entries"] = (self._one("SELECT count(*) FROM author_link_moves WHERE source = 'seed'")
                               if out["log_table"] else None)
        self.conn.rollback()
        return out

    def db_now(self):
        v = self._one("SELECT now()")
        self.conn.rollback()
        return v

    def select_ids(self, sel: Selection) -> list[str]:
        # core rows only (migration 067): a staged paper's authors are linked on promotion
        where = ["p.source = 'arXiv'", "p.corpus_tier = 'core'", "p.created_at <= now() - make_interval(days => %s)",
                 """EXISTS (SELECT 1 FROM author_elements(p.id, p.authors, p.published_year) e
                             WHERE e.openalex_id IS NULL)"""]
        params: list = [sel.min_age_days]
        if sel.max_age_days is not None:
            where.append("p.created_at > now() - make_interval(days => %s)")
            params.append(sel.max_age_days)
        if sel.created_from:
            where.append("p.created_at >= %s::timestamptz")
            params.append(sel.created_from)
        if sel.created_to:
            where.append("p.created_at < %s::timestamptz")
            params.append(sel.created_to)
        if sel.paper_ids is not None:
            where.append("p.id = ANY(%s::uuid[])")
            params.append(sorted(sel.paper_ids))
        rows = self._rows(f"SELECT p.id::text AS id FROM papers p WHERE {' AND '.join(where)} "
                          "ORDER BY p.created_at, p.id", params)
        self.conn.rollback()
        return [r["id"] for r in rows]

    def read_papers(self, ids: list[str]) -> dict[str, dict]:
        rows = self._rows("""SELECT id::text AS id, arxiv_id, doi, created_at, published_date, authors
                               FROM papers WHERE id = ANY(%s::uuid[])""", [ids])
        return {r["id"]: r for r in rows}

    def end_read(self) -> None:
        self.conn.rollback()

    def orcid_holders(self, orcids) -> dict:
        if not orcids:
            return {}
        rows = self._rows("SELECT orcid, id::text AS id, name FROM authors WHERE orcid = ANY(%s)",
                          [sorted(orcids)])
        return {r["orcid"]: {"id": r["id"], "name": r["name"]} for r in rows}

    def id_rows(self, oas) -> dict:
        """OpenAlex ID (URL form) -> the row it resolves to through author_openalex_ids."""
        if not oas:
            return {}
        rows = self._rows("""SELECT a.openalex_id, au.id::text AS id, au.name, au.orcid
                               FROM author_openalex_ids a JOIN authors au ON au.id = a.author_id
                              WHERE a.openalex_id = ANY(%s)""", [sorted(oas)])
        return {r["openalex_id"]: {"id": r["id"], "name": r["name"], "orcid": r["orcid"]} for r in rows}

    def begin_write(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")

    def tx_now(self) -> str:
        return iso(self._one("SELECT now()"))

    def snapshot_candidates(self, ids: list[str]) -> dict:
        rows = self._rows(f"""
            SELECT au.id::text AS id, au.slug, au.name,
                   ARRAY(SELECT pa2.paper_id::text FROM paper_authors pa2 WHERE pa2.author_id = au.id
                          ORDER BY 1) AS linked,
                   ARRAY(SELECT m.paper_id::text FROM author_link_moves m
                          WHERE m.author_id = au.id AND m.source = 'relink' ORDER BY 1) AS logged,
                   EXISTS (SELECT 1 FROM author_link_moves m
                            WHERE m.author_id = au.id AND m.source <> 'relink') AS seeded
              FROM authors au
             WHERE au.id IN (SELECT pa.author_id FROM paper_authors pa WHERE pa.paper_id = ANY(%s::uuid[]))
               AND {PROVISIONAL}""", [ids])
        cands = {r["id"]: r for r in rows}
        pointing = self._rows("""SELECT old_id::text AS old_id, old_slug, new_id::text AS new_id
                                   FROM author_redirects WHERE new_id = ANY(%s::uuid[])""", [list(cands)])
        return {"cands": cands, "pointing": pointing}

    def update_guarded(self, pid: str, new, old) -> int:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE papers SET authors = %s WHERE id = %s AND authors = %s",
                        [psycopg2.extras.Json(new), pid, psycopg2.extras.Json(old)])
            return cur.rowcount

    def batch_effects(self, snap: dict, ids: list[str]) -> dict:
        cands = snap["cands"]
        now_rows = self._rows(f"""SELECT au.id::text AS id, NOT ({PROVISIONAL}) AS keyed
                                    FROM authors au WHERE au.id = ANY(%s::uuid[])""", [list(cands)])
        alive = {r["id"]: r["keyed"] for r in now_rows}
        dissolved_ids = [c for c in cands if c not in alive]
        red = {r["old_id"]: r for r in self._rows(
            """SELECT old_id::text AS old_id, new_id::text AS new_id, old_slug, reason
                 FROM author_redirects WHERE old_id = ANY(%s::uuid[])""", [dissolved_ids])}
        dissolved = []
        for c in dissolved_ids:
            r = red.get(c)
            dissolved.append({"id": c, "slug": cands[c]["slug"], "name": cands[c]["name"],
                              "linked": cands[c]["linked"], "logged": cands[c]["logged"],
                              "seeded": cands[c]["seeded"],
                              "redirect": r["new_id"] if r and r["reason"] == PD else None})
        moved = [p for p in snap["pointing"] if p["new_id"] in set(dissolved_ids)]
        now_point = {r["old_id"]: r["new_id"] for r in self._rows(
            "SELECT old_id::text AS old_id, new_id::text AS new_id FROM author_redirects WHERE old_id = ANY(%s::uuid[])",
            [[p["old_id"] for p in moved]])}
        repointed = [{"old_id": p["old_id"], "old_slug": p["old_slug"], "from": p["new_id"],
                      "to": now_point.get(p["old_id"])} for p in moved]
        aliases = self._rows("""
            SELECT a.openalex_id, a.author_id::text AS row_id, a.source, a.evidence_paper_id::text AS paper_id,
                   au.name AS row_name,
                   (SELECT e.name FROM papers p CROSS JOIN LATERAL author_elements(p.id, p.authors, p.published_year) e
                     WHERE p.id = a.evidence_paper_id AND e.openalex_id = a.openalex_id
                     ORDER BY e.position LIMIT 1) AS element_name
              FROM author_openalex_ids a JOIN authors au ON au.id = a.author_id
             WHERE a.created_at = now() AND a.evidence_paper_id = ANY(%s::uuid[])
               AND a.source IN ('orcid', 'adopt')""", [ids])
        counts = self._rows("""
            SELECT (SELECT count(*) FROM authors WHERE created_at = now()
                     AND (openalex_id IS NOT NULL OR orcid IS NOT NULL)) AS keyed_created,
                   (SELECT count(*) FROM authors WHERE created_at = now()
                     AND openalex_id IS NULL AND orcid IS NULL) AS provisional_created,
                   (SELECT count(*) FROM paper_authors pa JOIN authors au ON au.id = pa.author_id
                     WHERE pa.paper_id = ANY(%s::uuid[]) AND au.created_at < now()
                       AND au.openalex_id IS NOT NULL) AS links_to_existing_keyed""", [ids])[0]
        conflicts = {r["kind"]: r["n"] for r in self._rows(
            """SELECT kind, count(*) AS n FROM author_link_conflicts
                WHERE created_at = now() AND paper_id = ANY(%s::uuid[]) GROUP BY kind""", [ids])}
        return {"dissolved": dissolved, "repointed": repointed, "aliases": aliases,
                "adopted_candidates": [c for c, keyed in alive.items() if keyed],
                "conflicts": conflicts, **counts}

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def rtt_ms(self) -> float:
        ts = []
        for _ in range(3):
            t0 = time.monotonic()
            self._one("SELECT 1")
            ts.append((time.monotonic() - t0) * 1000)
        self.conn.rollback()
        return statistics.median(ts)

    def stats(self, run_start) -> dict:
        r = self._rows(f"""
            SELECT (SELECT count(*) FROM paper_authors) AS links,
                   (SELECT count(*) FROM authors) AS authors,
                   (SELECT count(*) FROM authors au WHERE au.openalex_id IS NULL AND au.orcid IS NULL) AS provisional_plain,
                   (SELECT count(*) FROM authors au WHERE au.openalex_id IS NULL AND au.orcid IS NULL
                      AND au.created_at < %s) AS provisional_gated,
                   (SELECT count(*) FROM authors au WHERE {PROVISIONAL}) AS provisional_050,
                   (SELECT count(*) FROM author_redirects WHERE reason = %s) AS pd_redirects""",
                       [run_start, PD])[0]
        r["conflicts"] = {x["kind"]: x["n"] for x in self._rows(
            "SELECT kind, count(*) AS n FROM author_link_conflicts GROUP BY kind ORDER BY kind")}
        self.conn.rollback()
        return r

    def g14(self, records: list[dict]) -> dict:
        """For each dissolved row with a redirect: every paper it held (linked or logged at its batch)
        links to the redirect's current target. Redirects deleted since are skipped."""
        olds, papers = [], []
        for d in records:
            for p in set(d["linked"]) | set(d["logged"]):
                olds.append(d["id"])
                papers.append(p)
        if not olds:
            return {"checked": 0, "pairs": 0, "failures": []}
        checked = self._one("""SELECT count(*) FROM author_redirects
                                WHERE old_id = ANY(%s::uuid[]) AND reason = %s""",
                            [sorted({d["id"] for d in records}), PD])
        fails = self._rows("""
            SELECT x.old_id::text AS old_id, x.paper_id::text AS paper_id, r.new_id::text AS target
              FROM unnest(%s::uuid[], %s::uuid[]) AS x(old_id, paper_id)
              JOIN author_redirects r ON r.old_id = x.old_id AND r.reason = %s
             WHERE NOT EXISTS (SELECT 1 FROM paper_authors pa
                                WHERE pa.paper_id = x.paper_id AND pa.author_id = r.new_id)""",
                           [olds, papers, PD])
        pairs = self._one("""SELECT count(*) FROM unnest(%s::uuid[]) AS x(old_id)
                              JOIN author_redirects r ON r.old_id = x.old_id AND r.reason = %s""", [olds, PD])
        self.conn.rollback()
        return {"checked": checked, "pairs": pairs, "failures": fails[:50], "failure_count": len(fails)}

    def orcid_disagreement(self, written: list[tuple[str, int, str]]) -> dict:
        """Rows linked at a position where the run wrote an ORCID, whose own ORCID or another linked
        element's ORCID differs from it."""
        if not written:
            return {"rows": 0, "sample": []}
        rows = self._rows("""
            WITH w AS (SELECT * FROM unnest(%s::uuid[], %s::int[], %s::text[]) AS w(paper_id, position, orcid)),
                 r AS (SELECT DISTINCT pa.author_id, w.orcid FROM w
                         JOIN paper_authors pa ON pa.paper_id = w.paper_id AND pa.position = w.position),
                 own AS (SELECT r.author_id, r.orcid, au.orcid AS other FROM r JOIN authors au ON au.id = r.author_id
                          WHERE au.orcid IS NOT NULL AND au.orcid <> r.orcid),
                 linked AS (SELECT r.author_id, r.orcid, e.orcid AS other FROM r
                              JOIN paper_authors pa ON pa.author_id = r.author_id
                              JOIN papers p ON p.id = pa.paper_id
                              CROSS JOIN LATERAL author_elements(p.id, p.authors, p.published_year) e
                             WHERE e.position = pa.position AND e.orcid IS NOT NULL AND e.orcid <> r.orcid)
            SELECT x.author_id::text AS row_id, au.name, x.orcid, x.other
              FROM (SELECT * FROM own UNION SELECT * FROM linked) x JOIN authors au ON au.id = x.author_id
             ORDER BY 1, 3, 4""", [[w[0] for w in written], [w[1] for w in written], [w[2] for w in written]])
        self.conn.rollback()
        return {"rows": len({r["row_id"] for r in rows}), "sample": rows[:20]}

    # revert ------------------------------------------------------------------------------

    def revert_snapshot(self, ids: list[str]) -> list[dict]:
        return self._rows(f"""
            SELECT pa.paper_id::text AS paper_id, pa.position, au.id::text AS row_id, au.orcid,
                   NOT ({PROVISIONAL}) AS keyed
              FROM paper_authors pa JOIN authors au ON au.id = pa.author_id
             WHERE pa.paper_id = ANY(%s::uuid[])""", [ids])

    def delete_redirects(self, old_ids: list[str]) -> list[dict]:
        if not old_ids:
            return []
        return self._rows("""DELETE FROM author_redirects WHERE old_id = ANY(%s::uuid[]) AND reason = %s
                             RETURNING old_id::text AS old_id, old_slug, new_id::text AS new_id""",
                          [old_ids, PD])

    def revert_effects(self, keyed_ids: list[str], reverted: list[str], run_start) -> dict:
        empty = self._rows("""SELECT au.id::text AS id, au.name, au.slug FROM authors au
                               WHERE au.id = ANY(%s::uuid[])
                                 AND NOT EXISTS (SELECT 1 FROM paper_authors pa WHERE pa.author_id = au.id)""",
                           [keyed_ids])
        recreated = self._rows("""
            SELECT pa.paper_id::text AS paper_id, pa.position, au.id::text AS id, au.name, au.slug
              FROM paper_authors pa JOIN authors au ON au.id = pa.author_id
             WHERE pa.paper_id = ANY(%s::uuid[]) AND au.created_at = now()
               AND au.openalex_id IS NULL AND au.orcid IS NULL""", [reverted])
        aliases = {r["source"]: r["n"] for r in self._rows(
            """SELECT source, count(*) AS n FROM author_openalex_ids
                WHERE evidence_paper_id = ANY(%s::uuid[]) AND created_at >= %s GROUP BY source""",
            [reverted, run_start])} if run_start else {}
        return {"keyed_left_empty": empty, "recreated": recreated, "aliases_by_source": aliases}


# ---------------------------------------------------------------------------------------------
# forward run
# ---------------------------------------------------------------------------------------------

def age_bucket(days) -> str:
    if days is None:
        return "unknown"
    for lo, hi in AGE_BUCKETS:
        if lo <= days <= hi:
            return f"{lo}-{hi}" if hi < 10**6 else f"{lo}+"
    return "unknown"


def with_retries(fn, log):
    """Runs fn() up to 1 + DB_RETRIES times on a retryable error; fn.store is rolled back after each
    error. Returns (result, attempts, error) where error is None, 'failed' (retries exhausted) or the
    exception of a non-retryable error."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(), attempt, None
        except psycopg2.Error as e:
            try:
                fn.store.rollback()
            except Exception:  # noqa: BLE001
                pass
            if not is_retryable(e):
                return None, attempt, e
            if attempt > DB_RETRIES:
                log(f"  [batch] {type(e).__name__} after {attempt} attempts: checkpointed as failed")
                return None, attempt, "failed"
            log(f"  [batch] {type(e).__name__}: retry {attempt} in {backoff_s(attempt)} s")
            SLEEP(backoff_s(attempt))


class _Attempt:
    """One write attempt of a batch: ORCID holders and ID rows, plans, guarded updates, effects, commit."""

    def __init__(self, store, papers, looks, order, commit, carry):
        self.store, self.papers, self.looks, self.order, self.commit = store, papers, looks, order, commit
        # dry run: ORCIDs and ID rows registered in earlier batches, which a commit would have written
        self.carry = carry

    def __call__(self):
        store = self.store
        found = [pid for pid in self.order if self.looks[pid].kind == "found"]
        orcids, oas = set(), set()
        for pid in found:
            orcids |= candidate_orcids(self.looks[pid].work)
            oas |= candidate_ids(self.looks[pid].work)
        holders = store.orcid_holders(orcids)
        db_ids = store.id_rows(oas)
        id_rows = {k: dict(v) for k, v in db_ids.items()}
        if not self.commit:
            for key in orcids - set(holders):
                if key in self.carry["orcid"]:
                    holders[key] = self.carry["orcid"][key]
            for oa in oas:
                kept = self.carry["ids"].get(oa)
                if kept and not (id_rows.get(oa) or {}).get("orcid"):
                    id_rows[oa] = dict(kept)
        plans = {pid: plan_paper(self.papers[pid]["authors"], self.looks[pid].work, holders, id_rows)
                 for pid in found}
        if not self.commit:
            self.carry["orcid"].update({k: v for k, v in holders.items() if v.get("id") is None})
            self.carry["ids"].update({k: v for k, v in id_rows.items()
                                      if k not in db_ids or v.get("orcid") != db_ids[k].get("orcid")})
        result = {"plans": plans, "written": {}, "effects": None, "tx_at": None, "write_s": 0.0, "rtt_ms": None}
        changed = [pid for pid in found if plans[pid].outcome == "update"]
        if not self.commit or not changed:
            store.rollback()
            return result
        store.begin_write()          # SET LOCAL: the holder read above opened this transaction
        result["tx_at"] = store.tx_now()
        snap = store.snapshot_candidates(changed)
        t0 = time.monotonic()
        for pid in changed:
            result["written"][pid] = store.update_guarded(pid, plans[pid].new, self.papers[pid]["authors"])
        result["write_s"] = time.monotonic() - t0
        result["effects"] = store.batch_effects(snap, changed)
        store.commit()
        return result


def new_report(a, dry_run: bool) -> dict:
    return {
        "mode": "forward", "dry_run": dry_run, "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "options": {k: getattr(a, k) for k in ("min_age_days", "max_age_days", "limit", "created_from",
                                               "created_to", "paper_ids", "batch_size", "canary", "workers",
                                               "cache", "cache_max_age_days", "retry_outcomes")},
        "selected": 0, "eligible": 0, "skipped_checkpoint": 0, "batches": 0, "failed_batches": 0,
        "stopped": None, "outcomes": dict.fromkeys(OUTCOMES, 0),
        "positions": dict.fromkeys(POSITION_COUNTS, 0),
        "authorships": 0, "authorships_with_id": 0, "stored_elements_looked_up": 0,
        "orcid_dropped_pairs": [], "found_by_route": {"arxiv_doi": 0, "paper_doi": 0},
        "cache_hits": 0, "requests": 0, "credits": 0.0, "charged_calls": 0, "http_status": {},
        "api_s": 0.0, "write_s": 0.0, "wall_s": 0.0, "per_batch": [],
        "age_days": {"found": {}, "not_found": {}},
        "d5_slash_misses": 0, "misses": 0,
        "db": {"dissolved_with_redirect": 0, "dissolved_without_redirect": 0, "dissolved_seeded": 0,
               "repointed": 0, "adoptions": 0, "bridges": 0, "keyed_created": 0, "provisional_created": 0,
               "links_to_existing_keyed": 0, "conflicts_added": {}},
        "samples": [],
    }


def run_forward(store, client: OpenAlexClient, a, log=log_print) -> tuple[int, dict]:
    commit = bool(a.commit)
    rep = new_report(a, not commit)
    t_run = time.monotonic()
    pre = store.preflight()
    rep["preflight"] = pre
    if pre.get("update_trigger") != "O" or not pre.get("alias_table"):
        log(f"[backfill] refusing: {TRIGGER} enabled = {pre.get('update_trigger')!r}, "
            f"author_openalex_ids present = {pre.get('alias_table')}")
        return 2, rep
    if commit and not pre.get("log_table"):
        log("[backfill] refusing --commit: author_link_moves (migration 050) is missing")
        return 2, rep
    run_start = store.db_now()
    rep["run_start"] = iso(run_start)
    ckpt = Checkpoint(a.checkpoint) if a.checkpoint else None
    retry = {x.strip() for x in (a.retry_outcomes or "").split(",") if x.strip()}
    cache = Cache(a.cache, a.cache_max_age_days) if a.cache else None
    sel = Selection(a.min_age_days, a.max_age_days, a.created_from, a.created_to, load_ids(a.paper_ids))
    ids = store.select_ids(sel)
    rep["eligible"] = len(ids)
    skip = ckpt.skip_ids(retry) if ckpt else set()
    queue = [i for i in ids if i not in skip]
    rep["skipped_checkpoint"] = len(ids) - len(queue)
    if a.limit is not None:
        queue = queue[:a.limit]
    rep["selected"] = len(queue)
    batches = [queue[i:i + a.batch_size] for i in range(0, len(queue), a.batch_size)]
    if a.canary:
        batches = batches[:a.canary]
    log(f"[backfill] {'COMMIT' if commit else 'dry run'}: {len(ids)} eligible, {rep['skipped_checkpoint']} in "
        f"checkpoint, {len(queue)} selected, {len(batches)} batches of {a.batch_size}, workers {a.workers}")
    if commit:
        rep["db"]["before"] = store.stats(run_start)
    dissolved_all: list[dict] = []
    written_orcids: list[tuple[str, int, str]] = []
    carry: dict = {"orcid": {}, "ids": {}}
    code = 0
    today = datetime.now(timezone.utc)

    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        for k, order in enumerate(batches, 1):
            papers = store.read_papers(order)
            store.end_read()
            order = [pid for pid in order if pid in papers]
            t0 = time.monotonic()
            looks: dict[str, Lookup] = {}
            todo = []
            for pid in order:
                hit = cache.get(pid) if cache else None
                if hit:
                    looks[pid] = Lookup("found", hit["work"], hit.get("route"), [], hit["fetched_at"], True)
                else:
                    todo.append(pid)
            for pid, look in zip(todo, pool.map(lambda x: lookup(papers[x], client), todo)):
                looks[pid] = look
            api_s = time.monotonic() - t0
            if client.stop.is_set() or any(looks[p].kind == "stopped" for p in order):
                rep["stopped"] = client.stop_reason or "rate-limited"
                code = 3
                log(f"[backfill] stopped ({rep['stopped']}) in batch {k}: nothing written for it")
                if cache:
                    cache.add([cache_line(pid, papers[pid], looks[pid]) for pid in order
                               if looks[pid].kind == "found" and not looks[pid].cached])
                break
            if cache:
                cache.add([cache_line(pid, papers[pid], looks[pid]) for pid in order
                           if looks[pid].kind == "found" and not looks[pid].cached])
            rep["cache_hits"] += sum(1 for p in order if looks[p].cached)

            res, attempts, err = with_retries(_Attempt(store, papers, looks, order, commit, carry), log)
            if err is not None and err != "failed":
                rep["stopped"] = "db-error"
                rep["db_error"] = f"{type(err).__name__}: {str(err).strip()[:300]}"
                code = 4
                log(f"[backfill] database error in batch {k}, run stopped: {rep['db_error']}")
                break
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            lines = []
            age_lines: list[dict] = []
            batch_line = {"type": "batch", "batch": k, "at": now_iso, "papers": len(order),
                          "attempts": attempts, "api_s": round(api_s, 3)}
            if err == "failed":
                rep["failed_batches"] += 1
                batch_line["status"] = "failed"
                for pid in order:
                    lines.append({"type": "paper", "id": pid, "outcome": "failed", "at": now_iso, "batch": k})
                    rep["outcomes"]["failed"] += 1
            else:
                batch_line["status"] = "done"
                plans = res["plans"]
                for pid in order:
                    look, p = looks[pid], papers[pid]
                    line = {"type": "paper", "id": pid, "at": now_iso, "batch": k}
                    published = as_date(p.get("published_date"))
                    pub = published or as_date(p.get("created_at"))
                    fetched = datetime.fromtimestamp(look.fetched_at, timezone.utc) if look.fetched_at else today
                    age = (fetched.date() - pub).days if pub else None
                    line["age_days"] = age
                    if look.kind != "found":
                        outcome = look.kind
                        rep["misses"] += look.kind == "not-found"
                        rep["d5_slash_misses"] += look.kind == "not-found" and "/" in (p.get("arxiv_id") or "")
                        if look.kind == "not-found":
                            b = age_bucket(age)
                            rep["age_days"]["not_found"][b] = rep["age_days"]["not_found"].get(b, 0) + 1
                    else:
                        plan = plans[pid]
                        rep["found_by_route"][look.route or "arxiv_doi"] = rep["found_by_route"].get(look.route or "arxiv_doi", 0) + 1
                        b = age_bucket(age)
                        rep["age_days"]["found"][b] = rep["age_days"]["found"].get(b, 0) + 1
                        rep["authorships"] += plan.authorships
                        rep["authorships_with_id"] += plan.authorships_with_id
                        rep["stored_elements_looked_up"] += len(p["authors"] or [])
                        outcome = {"update": "updated"}.get(plan.outcome, plan.outcome)
                        if commit and plan.outcome == "update" and res["written"].get(pid) == 0:
                            outcome = "changed-underneath"
                        line.update({"route": look.route, "stored": len(p["authors"] or []),
                                     "ships": [plan.authorships, plan.authorships_with_id],
                                     "counts": {c: v for c, v in plan.counts.items() if v}})
                        if plan.orcid_dropped and outcome == "updated":
                            line["orcid_dropped"] = plan.orcid_dropped
                        if outcome in ("updated", "no-change"):
                            for key in ("no_id", "name_mismatch", "duplicate_id", "has_id"):
                                rep["positions"][key] += plan.counts[key]
                        if outcome == "updated":
                            for key in ("given_id", "exact", "initials", "variant", "orcid", "affiliation", "orcid_dropped"):
                                rep["positions"][key] += plan.counts[key]
                            rep["orcid_dropped_pairs"].extend({"paper_id": pid, **d} for d in plan.orcid_dropped)
                            line["before"] = p["authors"]
                            line["changes"] = plan.changes
                            for c in plan.changes:
                                if "orcid" in c:
                                    written_orcids.append((pid, c["position"], norm_orcid(c["orcid"])))
                            if len(rep["samples"]) < 5:
                                rep["samples"].append({"paper_id": pid, "arxiv_id": p["arxiv_id"],
                                                       "before": p["authors"], "after": plan.new})
                    line["outcome"] = outcome
                    rep["outcomes"][outcome] = rep["outcomes"].get(outcome, 0) + 1
                    lines.append(line)
                    if a.age_at_found_log and look.kind == "found":
                        age_lines.append({"paper_id": pid, "arxiv_id": p.get("arxiv_id"), "age_days": age,
                                          "basis": "published_date" if published else "created_at",
                                          "outcome": outcome, "at": now_iso, "cached": look.cached})
                eff = res["effects"]
                n_written = sum(1 for v in res["written"].values() if v)
                batch_line.update({"tx_at": res["tx_at"], "written": n_written,
                                   "write_s": round(res["write_s"], 3)})
                if eff is not None:
                    rtt = store.rtt_ms()
                    trig = max(0.0, res["write_s"] * 1000 - rtt * len(res["written"]))
                    batch_line.update({"rtt_ms": round(rtt, 1), "trigger_ms_est": round(trig, 1),
                                       "dissolved": eff["dissolved"], "repointed": eff["repointed"],
                                       "aliases": eff["aliases"], "keyed_created": eff["keyed_created"],
                                       "provisional_created": eff["provisional_created"],
                                       "links_to_existing_keyed": eff["links_to_existing_keyed"],
                                       "adopted_candidates": eff["adopted_candidates"],
                                       "conflicts": eff["conflicts"]})
                    db = rep["db"]
                    for d in eff["dissolved"]:
                        db["dissolved_with_redirect" if d["redirect"] else "dissolved_without_redirect"] += 1
                        db["dissolved_seeded"] += bool(d.get("seeded"))
                    dissolved_all.extend(d for d in eff["dissolved"] if d["redirect"])
                    db["repointed"] += len(eff["repointed"])
                    db["adoptions"] += sum(1 for x in eff["aliases"] if x["source"] == "adopt")
                    db["bridges"] += sum(1 for x in eff["aliases"] if x["source"] == "orcid")
                    for key in ("keyed_created", "provisional_created", "links_to_existing_keyed"):
                        db[key] += eff[key]
                    for kind, n in eff["conflicts"].items():
                        db["conflicts_added"][kind] = db["conflicts_added"].get(kind, 0) + n
                    rep["write_s"] += res["write_s"]
                    rep["per_batch"].append({"batch": k, "papers": len(order), "written": n_written,
                                             "api_s": round(api_s, 2), "write_s": round(res["write_s"], 2),
                                             "trigger_ms_est": round(trig, 1), "rtt_ms": round(rtt, 1),
                                             "attempts": attempts})
                else:
                    rep["per_batch"].append({"batch": k, "papers": len(order), "written": 0,
                                             "api_s": round(api_s, 2), "write_s": 0.0, "attempts": attempts})
            rep["api_s"] += api_s
            rep["batches"] += 1
            lines.append(batch_line)
            if commit and ckpt is not None:
                ckpt.append(lines)
            if a.age_at_found_log:
                append_jsonl(a.age_at_found_log, age_lines)
            if k % 10 == 0 or k == len(batches):
                o = rep["outcomes"]
                el = time.monotonic() - t_run
                log(f"  [{k}/{len(batches)}] updated {o['updated']} no-change {o['no-change']} not-found "
                    f"{o['not-found']} count-differs {o['count-differs']} dup-positions {rep['positions']['duplicate_id']} "
                    f"underneath {o['changed-underneath']} api-error {o['api-error']} failed {o['failed']}; "
                    f"requests {client.stats.requests} ({client.stats.requests / el:.2f}/s) credits "
                    f"{client.stats.credits:g}; dissolved {rep['db']['dissolved_with_redirect']}+"
                    f"{rep['db']['dissolved_without_redirect']}; {el:.0f}s")

    st = client.stats
    rep.update({"requests": st.requests, "credits": st.credits, "charged_calls": st.charged_calls,
                "http_status": dict(st.statuses),
                "latency_s": {"p50": round(statistics.median(st.latency), 3) if st.latency else None,
                              "max": round(max(st.latency), 3) if st.latency else None}})
    if commit:
        rep["db"]["after"] = store.stats(run_start)
        rep["db"]["g14"] = store.g14(dissolved_all)
        rep["db"]["orcid_disagreement"] = store.orcid_disagreement(written_orcids)
        b, af = rep["db"]["before"], rep["db"]["after"]
        rep["db"]["gated_fall"] = b["provisional_gated"] - af["provisional_gated"]
        rep["db"]["resolver_error_added"] = (af["conflicts"].get("resolver-error", 0)
                                             - b["conflicts"].get("resolver-error", 0))
    rep["wall_s"] = round(time.monotonic() - t_run, 1)
    rep["api_s"] = round(rep["api_s"], 1)
    rep["write_s"] = round(rep["write_s"], 1)
    rep["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return code, rep


def append_jsonl(path, items: list[dict]) -> None:
    """--age-at-found-log: append-only, so a month of daily runs accumulates in one file."""
    if not items:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8", newline="") as f:
        for e in items:
            f.write(json.dumps(e, ensure_ascii=False, default=str) + "\n")


def cache_line(pid: str, paper: dict, look: Lookup) -> dict:
    return {"paper_id": pid, "arxiv_id": paper.get("arxiv_id"), "route": look.route,
            "fetched_at": look.fetched_at or time.time(), "work": look.work}


def load_ids(path) -> set | None:
    if not path:
        return None
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


# ---------------------------------------------------------------------------------------------
# revert
# ---------------------------------------------------------------------------------------------

def run_revert(store, a, log=log_print) -> tuple[int, dict]:
    commit = bool(a.commit)
    src = Checkpoint(a.revert)
    t_run = time.monotonic()
    rep = {"mode": "revert", "dry_run": not commit, "source": str(a.revert),
           "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "outcomes": {"reverted": 0, "changed-since": 0}, "batches": 0, "failed_batches": 0,
           "stopped": None, "redirects_deleted": 0, "cannot_undo": {}}
    pre = store.preflight()
    if pre.get("update_trigger") != "O" or not pre.get("alias_table"):
        log("[revert] refusing: the update trigger is not enabled or author_openalex_ids is missing")
        return 2, rep
    updated = [e for e in src.papers.values() if e["outcome"] == "updated"]
    only = load_ids(a.paper_ids)
    if only is not None:
        updated = [e for e in updated if e["id"] in only]
    if a.limit is not None:
        updated = updated[:a.limit]
    rep["selected"] = len(updated)
    dissolved: list[dict] = [d for b in src.batches if b.get("status") == "done" for d in b.get("dissolved", [])]
    by_paper: dict[str, list[dict]] = {}
    for d in dissolved:
        for p in set(d["linked"]) | set(d["logged"]):
            by_paper.setdefault(p, []).append(d)
    tx_ats = [b["tx_at"] for b in src.batches if b.get("tx_at")]
    run_start = min(tx_ats) if tx_ats else None
    out = Checkpoint(a.checkpoint) if (a.checkpoint and commit) else None
    keyed_empty: dict[str, dict] = {}
    recreated: list[dict] = []
    aliases: dict[str, int] = {}
    orcids_filled = 0
    changed_since: list[str] = []
    repointed = [r for b in src.batches for r in b.get("repointed", [])]
    code = 0
    log(f"[revert] {'COMMIT' if commit else 'dry run'}: {len(updated)} updated papers in the checkpoint selected")

    for k in range(0, len(updated), a.batch_size):
        chunk = updated[k:k + a.batch_size]

        def attempt():
            res = {"n": {}, "deleted": [], "eff": None, "snap": []}
            if commit:
                store.begin_write()
            res["snap"] = store.revert_snapshot([e["id"] for e in chunk])
            for e in chunk:
                after = after_array(e["before"], e["changes"])
                if commit:
                    res["n"][e["id"]] = store.update_guarded(e["id"], e["before"], after)
                else:
                    cur = store.read_papers([e["id"]]).get(e["id"])
                    res["n"][e["id"]] = int(cur is not None and cur["authors"] == after)
            done = [e["id"] for e in chunk if res["n"][e["id"]]]
            olds = sorted({d["id"] for p in done for d in by_paper.get(p, []) if d.get("redirect")})
            if commit:
                res["deleted"] = store.delete_redirects(olds)
                keyed = sorted({s["row_id"] for s in res["snap"] if s["keyed"]})
                res["eff"] = store.revert_effects(keyed, done, run_start)
                store.commit()
            else:
                res["deleted"] = [{"old_id": o} for o in olds]
                store.rollback()
            return res

        attempt.store = store
        res, attempts, err = with_retries(attempt, log)
        if err is not None and err != "failed":
            rep["stopped"] = "db-error"
            rep["db_error"] = f"{type(err).__name__}: {str(err).strip()[:300]}"
            code = 4
            break
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        lines = []
        if err == "failed":
            rep["failed_batches"] += 1
            lines = [{"type": "revert", "id": e["id"], "outcome": "failed", "at": now_iso} for e in chunk]
        else:
            for e in chunk:
                oc = "reverted" if res["n"][e["id"]] else "changed-since"
                rep["outcomes"][oc] += 1
                if oc == "changed-since":
                    changed_since.append(e["id"])
                lines.append({"type": "revert", "id": e["id"], "outcome": oc, "at": now_iso})
            rep["redirects_deleted"] += len(res["deleted"])
            done = {e["id"] for e in chunk if res["n"][e["id"]]}
            written = {(e["id"], c["position"]): norm_orcid(c["orcid"])
                       for e in chunk if e["id"] in done for c in e["changes"] if "orcid" in c}
            orcids_filled += len({s["row_id"] for s in res["snap"]
                                  if s["keyed"] and s["orcid"] and written.get((s["paper_id"], s["position"])) == s["orcid"]})
            if res["eff"]:
                for r in res["eff"]["keyed_left_empty"]:
                    keyed_empty[r["id"]] = r
                for src_name, n in res["eff"]["aliases_by_source"].items():
                    aliases[src_name] = aliases.get(src_name, 0) + n
                for r in res["eff"]["recreated"]:
                    olds = [d for d in by_paper.get(r["paper_id"], []) if d["name"] == r["name"]]
                    r["old_slug"] = olds[0]["slug"] if olds else None
                    recreated.append(r)
            lines.append({"type": "revert-batch", "at": now_iso, "papers": len(chunk), "attempts": attempts,
                          "redirects_deleted": res["deleted"],
                          "recreated": (res["eff"] or {}).get("recreated", [])})
        rep["batches"] += 1
        if out is not None:
            out.append(lines)

    by_id = {}
    for r in recreated:
        by_id.setdefault(r["id"], r)
    with_old = [r for r in by_id.values() if r["old_slug"]]
    rep["cannot_undo"] = {
        "papers_changed_since": changed_since,
        "keyed_rows_left_without_links": len(keyed_empty),
        "keyed_rows_left_without_links_sample": list(keyed_empty.values())[:10],
        "aliases_created_by_the_run_by_source": aliases,
        "orcids_on_rows_matching_an_orcid_the_run_wrote": orcids_filled,
        "buckets_recreated_under_new_ids": len(by_id),
        "recreated_with_old_slug": sum(1 for r in with_old if r["slug"] == r["old_slug"]),
        "recreated_with_other_slug": sum(1 for r in with_old if r["slug"] != r["old_slug"]),
        "recreated_sample": list(by_id.values())[:10],
        "slug_rule": "generate_author_slug() checks only authors.slug (never author_redirects), so a recreated "
                     "bucket gets its old slug whenever no current row holds it, with or without the redirect row",
        "redirects_repointed_by_the_run_left_in_place": len(repointed),
    }
    rep["wall_s"] = round(time.monotonic() - t_run, 1)
    rep["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log(f"[revert] {rep['outcomes']}; redirects deleted {rep['redirects_deleted']}; "
        f"keyed rows left without links {len(keyed_empty)}; buckets recreated {len(by_id)} "
        f"({rep['cannot_undo']['recreated_with_old_slug']} with their old slug)")
    return code, rep


# ---------------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="arXiv author-ID backfill (dry run unless --commit)")
    p.add_argument("--dsn", default=None, help="database (default: DATABASE_URL)")
    p.add_argument("--commit", action="store_true", help="write (default: dry run)")
    p.add_argument("--prod", action="store_true", help="allow --commit on the production endpoint")
    p.add_argument("--min-age-days", type=int, default=14)
    p.add_argument("--max-age-days", type=int, default=None,
                   help="also require created_at newer than this many days (the daily pass's window)")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--created-from", default=None, help="created_at >= this (inclusive)")
    p.add_argument("--created-to", default=None, help="created_at < this (exclusive)")
    p.add_argument("--paper-ids", default=None, help="file with one paper id per line")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--canary", type=int, default=None, help="run N batches and stop")
    p.add_argument("--workers", type=int, default=1, help=f"lookup threads, 1 to {MAX_WORKERS}")
    p.add_argument("--cache", default=None, help="jsonl cache of fetched works")
    p.add_argument("--cache-max-age-days", type=float, default=7)
    p.add_argument("--checkpoint", default=None, help="jsonl checkpoint (the revert log with --revert)")
    p.add_argument("--age-at-found-log", default=None,
                   help="jsonl: one line per found work with the paper's age in days at the lookup")
    p.add_argument("--retry-outcomes", default="", help="comma-separated outcomes to look up again")
    p.add_argument("--revert", default=None, metavar="CHECKPOINT", help="write the before-arrays back")
    p.add_argument("--json", default=None, help="write the run report here")
    a = p.parse_args(argv)
    if not 1 <= a.workers <= MAX_WORKERS:
        p.error(f"--workers must be between 1 and {MAX_WORKERS}")
    if a.batch_size < 1:
        p.error("--batch-size must be at least 1")
    bad = {x.strip() for x in a.retry_outcomes.split(",") if x.strip()} - set(OUTCOMES)
    if bad:
        p.error(f"--retry-outcomes: unknown {sorted(bad)}")
    return a


def main(argv=None) -> int:
    a = parse_args(argv)
    dsn = a.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("[backfill] no database: pass --dsn or set DATABASE_URL", file=sys.stderr)
        return 2
    prod = is_production(dsn)
    if a.commit and prod and not a.prod:
        print("[backfill] refusing --commit: the target is the production endpoint (NEON_ENDPOINT_ID in the "
              "host, or NEON_ENDPOINT_ID unset); pass --prod after the owner approval", file=sys.stderr)
        return 2
    print(f"[backfill] target: {'production' if prod else 'non-production endpoint'}"
          f"{' (--prod)' if prod and a.prod else ''}", flush=True)
    conn = psycopg2.connect(dsn, connect_timeout=30)
    try:
        store = Store(conn, readonly=not a.commit)
        if a.revert:
            code, rep = run_revert(store, a)
        else:
            code, rep = run_forward(store, OpenAlexClient(), a)
    finally:
        conn.close()
    rep["target"] = "production" if prod else "non-production"
    rep["exit"] = code
    if a.json:
        write_json(a.json, rep)
    summary = {k: rep.get(k) for k in ("mode", "dry_run", "selected", "outcomes", "positions", "requests",
                                       "credits", "stopped", "wall_s")}
    print(f"[backfill] {json.dumps(summary, default=str)}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
