"""promote_captured.py — promote staged captured papers to the core corpus tier once their venue is covered.

Ledger task lp3-capture-proof (docs/ledger/lp3-capture-proof.md, change 6); migration 067. The capture
drain (drain_capture_queue.py) writes a captured paper as `corpus_tier = 'staged'` when
`source_discipline()` does not resolve its venue to a configured journal. A staged paper has no
page, is in no list or corpus-wide computation, and the guarded triggers skipped it: no author
links, no `journals.paper_count`. When the pipeline's journal list grows to cover its venue, this
job promotes it:

  * `UPDATE papers SET corpus_tier = 'core'`: the promote trigger (067,
    auto_link_paper_authors_promote_trigger) runs the author resolver, and
    papers_journal_count_trigger adds the paper to its journal's count;
  * `link_disciplines()` with the drain's inserted-row semantics, called for every promoted paper.

The venue is resolved as the drain resolves it, from the stored `papers.source` and, when that
does not resolve, the name of the paper's `journals` row. (`papers` has no column for the OpenAlex
source id the drain matches first; a staged paper whose stored name differs from the config's name
is not promoted by this job.) One transaction per paper; the UPDATE is guarded by
`corpus_tier = 'staged'`, so a second run, or a run racing another, changes nothing a first one
changed. Idempotent: a second run promotes nothing.

`--apply` writes one `promote_runs` row (036) with `policy = {"kind": "captured-tier"}` at open and
its counts at close. `--dry-run` (the default) writes nothing and prints what would be promoted.
Logs carry paper ids only.

  python pipeline/promote_captured.py [--dry-run | --apply] [--limit N]

DATABASE_URL comes from the environment (.env.local is loaded, never overriding). The schedule in
the crawler repository is not part of this task; the job needs one (see the task report).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
load_dotenv(ROOT / ".env.local", override=False)

from ingest_work import source_discipline  # noqa: E402
from drain_capture_queue import link_disciplines  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")   # Windows cp1252 console
except Exception:
    pass

POLICY = {"kind": "captured-tier"}

SELECT_SQL = ("SELECT p.id::text, p.source, j.name FROM papers p "
              "LEFT JOIN journals j ON j.id = p.journal_id "
              "WHERE p.corpus_tier = 'staged' ORDER BY p.created_at, p.id")
PROMOTE_SQL = "UPDATE papers SET corpus_tier = 'core' WHERE id = %s AND corpus_tier = 'staged'"
OPEN_SQL = "INSERT INTO promote_runs (run_id, policy, args) VALUES (%s, %s::jsonb, %s::jsonb)"
CLOSE_SQL = ("UPDATE promote_runs SET finished_at = now(), papers_changed = %s, notes = %s "
             "WHERE run_id = %s")


def resolve(source: str | None, journal_name: str | None) -> str | None:
    """The discipline slug for a staged paper's venue, or None while the corpus does not cover it."""
    for name in (source, journal_name):
        if name:
            slug, _config_name = source_discipline({"source": name})
            if slug:
                return slug
    return None


def new_run_id() -> str:
    return f"captured_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_{uuid.uuid4().hex[:6]}"


def run(conn, *, apply: bool = False, limit: int | None = None, run_id: str | None = None) -> dict:
    counts = {"staged": 0, "resolved": 0, "promoted": 0, "unresolved": 0, "lost": 0,
              "disciplines_linked": 0}
    cur = conn.cursor()
    if apply:
        run_id = run_id or new_run_id()
        cur.execute(OPEN_SQL, [run_id, json.dumps(POLICY), json.dumps({"limit": limit, "mode": "apply"})])
        conn.commit()

    sql = SELECT_SQL + (f" LIMIT {int(limit)}" if limit else "")
    cur.execute(sql)
    rows = cur.fetchall()
    conn.rollback()   # the read holds no transaction open across the per-paper writes
    counts["staged"] = len(rows)

    for paper_id, source, journal_name in rows:
        slug = resolve(source, journal_name)
        if not slug:
            counts["unresolved"] += 1
            continue
        counts["resolved"] += 1
        if not apply:
            print(f"  [would promote] paper={paper_id} discipline={slug}")
            continue
        try:
            cur.execute(PROMOTE_SQL, [paper_id])
            if cur.rowcount != 1:
                conn.rollback()
                counts["lost"] += 1
                print(f"  [skip]    paper={paper_id} (no longer staged)")
                continue
            counts["disciplines_linked"] += link_disciplines(cur, paper_id, slug)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        counts["promoted"] += 1
        print(f"  [promote] paper={paper_id} discipline={slug}")

    if apply:
        cur.execute(CLOSE_SQL, [counts["promoted"], json.dumps(counts, sort_keys=True), run_id])
        conn.commit()
    counts["run_id"] = run_id
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="print what would be promoted (default)")
    mode.add_argument("--apply", action="store_true", help="promote and write a promote_runs row")
    ap.add_argument("--limit", type=int, default=None, help="scan at most N staged papers")
    a = ap.parse_args(argv)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set (environment or .env.local)")
        return 2
    conn = psycopg2.connect(dsn, connect_timeout=30)
    conn.autocommit = False
    try:
        counts = run(conn, apply=a.apply, limit=a.limit)
    finally:
        conn.close()
    label = "apply" if a.apply else "dry-run"
    print(f"[promote_captured] {label}: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
