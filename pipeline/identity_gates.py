"""identity_gates.py — the author-identity gates, read-only, in one file the crawler can run.

Specification: docs/ledger/arxiv-daily-pass.md (design item 2), from the gates of
docs/tierb/ARXIV_AUTHOR_ID_BACKFILL_WORKORDER.md section 7. The daily author-ID pass
(.github/workflows/author-ids.yml in the crawler repository) runs this before and after
backfill_arxiv_author_ids.py: the first run prints the timestamp the second passes back as
--since, and the second run's exit code fails the job.

  python pipeline/identity_gates.py [--since TIMESTAMP] [--json FILE]

The database is DATABASE_URL (pipeline/db.py). The session is read-only, so the script cannot
write even by accident, and nothing here needs rehearse_043.py or repair_author_links.py: G1's
element scan reads author_elements() through a lateral join instead of the temporary table those
scripts share.

Six lines, printed one per line. Three are gates and a failing one exits 1:

  G1              every link's element OpenAlex ID is an alias of the row it links to
  G2              every authors.openalex_id is an alias of its own row, and no aliased row is
                  missing its canonical ID
  resolver-error  no author_link_conflicts row of kind resolver-error since --since

Three are reported, and never fail the run:

  conflicts-added  author_link_conflicts rows since --since, by kind
  provisional      rows with no OpenAlex ID and no ORCID: the plain count, and the count gated on
                   created_at < --since, which leaves out the rows a concurrent crawl created
  leak             keyed rows (an OpenAlex ID) older than 7 days that hold no link at all, with up
                   to 20 of their ids. The daily pass creates keyed rows and the trigger moves the
                   links onto them; a keyed row left holding nothing is how a lost link shows up,
                   for instance if a re-crawl were to strip the IDs the pass wrote.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import db  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

SQL_NOW = "SELECT now()"

SQL_G1 = """
    SELECT count(*)
      FROM papers p
      CROSS JOIN LATERAL author_elements(p.id, p.authors, p.published_year) e
      JOIN paper_authors pa ON pa.paper_id = e.paper_id AND pa.position = e.position
     WHERE e.openalex_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM author_openalex_ids a
                        WHERE a.openalex_id = e.openalex_id AND a.author_id = pa.author_id)"""

SQL_G2_CANONICAL = """
    SELECT count(*) FROM authors au WHERE au.openalex_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM author_openalex_ids a
                        WHERE a.openalex_id = au.openalex_id AND a.author_id = au.id)"""

SQL_G2_ALIASED = """
    SELECT count(DISTINCT a.author_id) FROM author_openalex_ids a
      JOIN authors au ON au.id = a.author_id WHERE au.openalex_id IS NULL"""

SQL_RESOLVER_ERROR = """
    SELECT count(*) FROM author_link_conflicts
     WHERE kind = 'resolver-error' AND created_at >= %s"""

SQL_CONFLICTS_ADDED = """
    SELECT kind, count(*) FROM author_link_conflicts
     WHERE created_at >= %s GROUP BY kind ORDER BY kind"""

SQL_PROVISIONAL = """
    SELECT (SELECT count(*) FROM authors WHERE openalex_id IS NULL AND orcid IS NULL),
           (SELECT count(*) FROM authors WHERE openalex_id IS NULL AND orcid IS NULL
              AND created_at < %s)"""

LEAK_WHERE = """
     WHERE au.openalex_id IS NOT NULL
       AND au.created_at < now() - interval '7 days'
       AND NOT EXISTS (SELECT 1 FROM paper_authors pa WHERE pa.author_id = au.id)"""

SQL_LEAK = "SELECT count(*) FROM authors au" + LEAK_WHERE

SQL_LEAK_IDS = ("SELECT au.id::text FROM authors au" + LEAK_WHERE +
                " ORDER BY au.created_at, au.id LIMIT 20")


def log_print(*a):
    print(*a, flush=True)


def querier(cur):
    """The query function run_gates() works through: SQL and parameters in, rows out."""
    def q(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []
    return q


def connect():
    """The DATABASE_URL connection. Patched in the tests."""
    return db.get_connection()


def run_gates(q, since=None, log=log_print) -> dict:
    """Run the six lines through `q` and return the report. `since` defaults to the database clock,
    which makes the two dated lines empty by construction on a first run."""
    now = q(SQL_NOW)[0][0]
    since = since or now
    report: dict = {"now": now, "since": since, "lines": []}

    def add(key: str, ok, msg: str):
        report["lines"].append([key, ok, msg])
        log(f"  {'PASS' if ok else 'FAIL' if ok is False else 'INFO'}  {key}  {msg}")

    g1 = q(SQL_G1)[0][0]
    report["g1"] = g1
    add("G1", g1 == 0, f"links whose element ID is not an alias of the row: {g1}")

    canonical = q(SQL_G2_CANONICAL)[0][0]
    aliased = q(SQL_G2_ALIASED)[0][0]
    report["g2"] = {"canonical_outside_the_alias_table": canonical, "aliased_without_canonical": aliased}
    add("G2", canonical == 0 and aliased == 0,
        f"canonical IDs outside the alias table: {canonical}; rows with aliases but no canonical ID: {aliased}")

    added = q(SQL_RESOLVER_ERROR, [since])[0][0]
    report["resolver_error_added"] = added
    add("resolver-error", added == 0, f"rows added since {since}: {added}")

    conflicts = {k: n for k, n in q(SQL_CONFLICTS_ADDED, [since])}
    report["conflicts_added"] = conflicts
    add("conflicts-added", None, f"author_link_conflicts rows since {since}, by kind: {conflicts or 'none'}")

    plain, gated = q(SQL_PROVISIONAL, [since])[0]
    report["provisional"] = {"plain": plain, "gated": gated}
    add("provisional", None, f"rows with no OpenAlex ID and no ORCID: {plain:,} plain, "
                             f"{gated:,} created before {since}")

    leak = q(SQL_LEAK)[0][0]
    ids = [r[0] for r in q(SQL_LEAK_IDS)] if leak else []
    report["leak"] = {"count": leak, "ids": ids}
    add("leak", None, f"keyed rows older than 7 days holding no link: {leak}"
                      + (f"; first {len(ids)}: {', '.join(ids)}" if ids else ""))

    report["pass"] = all(l[1] is not False for l in report["lines"])
    return report


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="author-identity gates (read-only)")
    p.add_argument("--since", default=None,
                   help="the baseline timestamp: rows added at or after it are the run's own "
                        "(default: the database clock now)")
    p.add_argument("--json", default=None, help="write the report here")
    return p.parse_args(argv)


def main(argv=None) -> int:
    a = parse_args(argv)
    conn = connect()
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as cur:
            report = run_gates(querier(cur), a.since)
        conn.rollback()
    finally:
        conn.close()
    if a.json:
        path = Path(a.json)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(json.dumps(report, indent=1, ensure_ascii=False, default=str) + "\n")
    ok = report["pass"]
    n = sum(1 for l in report["lines"] if l[1] is not None)
    print(f"=== identity_gates {'PASS' if ok else 'FAIL'} "
          f"({sum(1 for l in report['lines'] if l[1] is True)}/{n} gates) ===", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
