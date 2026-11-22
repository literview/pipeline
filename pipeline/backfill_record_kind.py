"""backfill_record_kind.py: write the stored record kind of non-research papers (ledger task list-nonresearch-filter,
change 3; spec docs/ledger/list-nonresearch-filter.md).

The kind is record_kind(title) from pipeline/record_kind.py (the same rule as lib/record-kind.ts), written by id for
flagged papers only; a paper whose title the rule no longer flags loses its stored kind. Three modes:

  python pipeline/backfill_record_kind.py --all          one-time: every papers row
  python pipeline/backfill_record_kind.py --since 3d     daily, after the crawl: rows created in the last 3 days
  python pipeline/backfill_record_kind.py --drift [--fix]
                                                         weekly: recompute every row, print each row whose stored
                                                         kind differs (a title edited after insert, a rule edit),
                                                         and with --fix repair them. Exit 3 when differences remain.

--store column (default) writes papers.record_kind (migration 080, the O6 option gate 4 chose); every UPDATE fires the
papers row triggers, so updated_at moves on each row written. --store table writes the side table paper_record_kind of
option (d), kept for the rehearsal's comparison (pipeline/rehearse_080.py --option d). --dry-run computes and prints
without writing. DATABASE_URL comes from the environment, then this checkout's .env.local; the endpoint id is
printed, the connection string never.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from record_kind import KINDS, RULE_VERSION, record_kind  # noqa: E402


def dsn() -> str:
    load_dotenv(HERE.parent / ".env.local", override=False)
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL missing")
    return url


def endpoint_id(uri: str) -> str:
    return uri.split("@", 1)[1].split("/", 1)[0].split(".")[0].replace("-pooler", "")


def read_titles(cur, since_days: int | None) -> list[tuple[str, str | None]]:
    if since_days is None:
        cur.execute("SELECT id::text, title FROM papers")
    else:
        cur.execute("SELECT id::text, title FROM papers WHERE created_at >= now() - make_interval(days => %s)", [since_days])
    return cur.fetchall()


def read_stored(cur, store: str, ids: list[str] | None) -> dict[str, tuple[str, str | None]]:
    """Stored (kind, rule_version) by paper id, for every flagged row (ids None) or for the given ids."""
    if store == "table":
        sql = "SELECT paper_id::text, kind, rule_version FROM paper_record_kind"
        if ids is not None:
            sql += " WHERE paper_id = ANY(%s::uuid[])"
    else:
        sql = "SELECT id::text, record_kind, NULL FROM papers WHERE record_kind IS NOT NULL"
        if ids is not None:
            sql += " AND id = ANY(%s::uuid[])"
    cur.execute(sql, [ids] if ids is not None else None)
    return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def differences(rows, stored, store: str) -> tuple[dict[str, str], list[str]]:
    """(to_set: id -> kind, to_clear: ids) that make the stored kinds equal record_kind(title) over these rows."""
    want = {pid: k for pid, t in rows if (k := record_kind(t)) is not None}
    to_set = {}
    for pid, k in want.items():
        have = stored.get(pid)
        if have is None or have[0] != k or (store == "table" and have[1] != RULE_VERSION):
            to_set[pid] = k
    in_scope = {pid for pid, _ in rows}
    to_clear = [pid for pid in stored if pid in in_scope and pid not in want]
    return to_set, to_clear


def write(cur, store: str, to_set: dict[str, str], to_clear: list[str]) -> None:
    items = sorted(to_set.items())
    if store == "table":
        if items:
            execute_values(cur, "INSERT INTO paper_record_kind (paper_id, kind, rule_version) VALUES %s "
                                "ON CONFLICT (paper_id) DO UPDATE SET kind = EXCLUDED.kind, "
                                "rule_version = EXCLUDED.rule_version, computed_at = now()",
                           [(pid, k, RULE_VERSION) for pid, k in items], template="(%s::uuid, %s, %s)", page_size=1000)
        if to_clear:
            cur.execute("DELETE FROM paper_record_kind WHERE paper_id = ANY(%s::uuid[])", [to_clear])
    else:
        if items:
            execute_values(cur, "UPDATE papers SET record_kind = v.kind FROM (VALUES %s) AS v(id, kind) "
                                "WHERE papers.id = v.id::uuid", items, page_size=1000)
        if to_clear:
            cur.execute("UPDATE papers SET record_kind = NULL WHERE id = ANY(%s::uuid[])", [to_clear])


def main() -> int:
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--all", action="store_true")
    mode.add_argument("--since", metavar="Nd")
    mode.add_argument("--drift", action="store_true")
    ap.add_argument("--fix", action="store_true", help="with --drift: repair the differences")
    ap.add_argument("--store", choices=("column", "table"), default="column")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.fix and not a.drift:
        ap.error("--fix goes with --drift")
    since_days = None
    if a.since is not None:
        m = re.fullmatch(r"(\d+)d", a.since)
        if not m:
            ap.error("--since takes a number of days, e.g. 3d")
        since_days = int(m.group(1))
    writes = not a.dry_run and (a.all or a.since is not None or a.fix)

    url = dsn()
    print(f"[db] endpoint {endpoint_id(url)}; store {a.store}; rule {RULE_VERSION}; "
          f"mode {'all' if a.all else 'since ' + a.since if a.since else 'drift'}{' (fix)' if a.fix else ''}"
          f"{' (dry run)' if a.dry_run else ''}")
    conn = psycopg2.connect(url, connect_timeout=30)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")
            t0 = time.monotonic()
            rows = read_titles(cur, since_days)
            stored = read_stored(cur, a.store, None if since_days is None else [r[0] for r in rows])
            to_set, to_clear = differences(rows, stored, a.store)
            kinds = Counter(k for _, t in rows if (k := record_kind(t)) is not None)
            print(f"[read] {len(rows)} papers, {sum(kinds.values())} flagged: "
                  + ", ".join(f"{k} {kinds.get(k, 0)}" for k in KINDS) + f" ({time.monotonic() - t0:.1f} s)")
            print(f"[diff] {len(to_set)} to set, {len(to_clear)} to clear")
            if a.drift:
                for pid in sorted(to_set)[:20]:
                    print(f"    {pid}: stored {stored.get(pid, (None,))[0]} -> {to_set[pid]}")
                for pid in sorted(to_clear)[:20]:
                    print(f"    {pid}: stored {stored[pid][0]} -> research")
            if writes and (to_set or to_clear):
                t1 = time.monotonic()
                write(cur, a.store, to_set, to_clear)
                conn.commit()
                print(f"[write] {len(to_set)} set, {len(to_clear)} cleared ({time.monotonic() - t1:.1f} s)")
            else:
                conn.rollback()
                print("[write] nothing written")
    finally:
        conn.close()
    if a.drift:
        remaining = 0 if writes else len(to_set) + len(to_clear)
        print(f"drift: {len(to_set) + len(to_clear)} differences, {remaining} remaining")
        return 3 if remaining else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
