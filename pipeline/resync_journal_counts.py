"""resync_journal_counts.py — recount journals.paper_count from the core rows of papers.

Ledger task journal-count-resync (docs/ledger/journal-count-resync.md). `journals.paper_count` is kept
by the trigger papers_journal_count_trigger, whose function update_journal_paper_count() (067) counts
a paper when `corpus_tier = 'core' AND journal_id IS NOT NULL`. The trigger is right for every write
it sees; production drifted above it through writes it did not see. This job recounts every journal
with that definition and writes back the journals that differ.

`--dry-run` (the default) recounts in a read-only transaction and prints what would change.

`--apply` runs one transaction: `SET LOCAL lock_timeout`, `statement_timeout` (60 s) and
`idle_in_transaction_session_timeout` (30 s, so a stalled client cannot keep the lock), then
`LOCK TABLE papers IN SHARE MODE`, which waits for every in-flight papers write to commit and keeps
new ones (and so every trigger increment of journals.paper_count) waiting until this transaction
ends. Writes that wait: the pipeline's, and the web app's upvotes and comments, whose triggers update
papers' counters (017); the app's foreign-key checks take ROW SHARE and do not wait. Under the lock:
the optional backup table (`--backup-table NAME`: id and paper_count of every journal, created fresh,
so an existing name aborts the run before anything is corrected), the recount SELECT, then one UPDATE
of the differing journals from the SELECT's values, compare-and-set on the old value. The lock is
held for about one recount (under a second at 197K papers). A lock not granted within the timeout
rolls back and is retried after `--retry-wait` seconds, up to `--attempts` times; between attempts
nothing waits on this job.

Journal names and counts are public; the log and `--summary-md` print them. The DSN is never printed,
nor any part of a connection error.

  python pipeline/resync_journal_counts.py [--dry-run | --apply] [--backup-table NAME]
      [--summary-md PATH] [--dsn DSN] [--lock-timeout 5s] [--attempts 3] [--retry-wait 30]

Exit codes: 0 done (corrections included); 1 the lock was not acquired, nothing written; 2 usage or
configuration; 3 a database or connection error or a compare-and-set mismatch, nothing written;
4 the commit's outcome is unknown, or the run committed and could not finish its report (re-run with
--dry-run to see the state).

DATABASE_URL comes from --dsn, else the environment (.env.local is loaded, never overriding). The
weekly run is academic-crawler/.github/workflows/journal-counts.yml, on a byte copy of this file.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2 import errors, sql
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local", override=False)

try:
    sys.stdout.reconfigure(encoding="utf-8")   # Windows cp1252 console
except Exception:
    pass

# 067's definition; the join already excludes NULL journal_id, the predicate states it anyway
RECOUNT_SQL = ("SELECT j.id::text, j.name, j.paper_count, "
               "count(p.id) FILTER (WHERE p.corpus_tier = 'core' AND p.journal_id IS NOT NULL)::int "
               "FROM journals j LEFT JOIN papers p ON p.journal_id = j.id "
               "GROUP BY j.id, j.name, j.paper_count ORDER BY j.id")
READ_ONLY_SQL = "SET TRANSACTION READ ONLY"
LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = %s"
STATEMENT_TIMEOUT_SQL = "SET LOCAL statement_timeout = '60s'"
IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '30s'"
LOCK_SQL = "LOCK TABLE papers IN SHARE MODE"
BACKUP_SQL = sql.SQL("CREATE TABLE {} AS SELECT id, paper_count, now() AS backed_up_at FROM journals")
BACKUP_PK_SQL = sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)")
UPDATE_SQL = ("UPDATE journals j SET paper_count = v.core "
              "FROM unnest(%s::uuid[], %s::int[], %s::int[]) AS v(id, old, core) "
              "WHERE j.id = v.id AND j.paper_count IS NOT DISTINCT FROM v.old RETURNING j.id::text")

TABLE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
LOCK_TIMEOUT_RE = re.compile(r"^(\d+)(ms|s|min)$")
STATEMENT_TIMEOUT_MS = 60_000      # a lock wait must end before the statement timeout cancels it
RETRYABLE = (errors.LockNotAvailable, errors.DeadlockDetected)


class LockNotAcquired(Exception):
    """Every attempt ended in lock_timeout (55P03) or a deadlock (40P01); nothing was written."""


class CasMismatch(Exception):
    """A journal's paper_count changed between the recount and the UPDATE; the run rolled back."""

    def __init__(self, ids: list[str]):
        super().__init__(f"{len(ids)} journal(s) changed under the lock: {', '.join(ids)}")
        self.ids = ids


class CommitUnknown(Exception):
    """COMMIT raised: the server may or may not have committed."""


def lock_timeout_ms(value: str) -> int | None:
    m = LOCK_TIMEOUT_RE.match(value)
    if not m:
        return None
    return int(m.group(1)) * {"ms": 1, "s": 1000, "min": 60_000}[m.group(2)]


def differing(rows) -> list[tuple[str, str, int | None, int]]:
    """(id, name, old, core) for every journal whose stored count is not its core count."""
    return [(jid, name, old, core) for jid, name, old, core in rows if old is None or old != core]


def gap(diff) -> int:
    return sum(abs((old or 0) - core) for _jid, _name, old, core in diff)


def _rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:   # a dead connection: the server has already discarded the transaction
        pass


def run(conn, *, apply: bool = False, backup_table: str | None = None, lock_timeout: str = "5s",
        attempts: int = 3, retry_wait: float = 30.0, _after_count=None, _sleep=time.sleep) -> dict:
    """Recount; under apply, correct. `_after_count` (rehearsal only) is called inside the apply
    transaction between the recount and the UPDATE."""
    cur = conn.cursor()
    if not apply:
        try:
            cur.execute(READ_ONLY_SQL)
            cur.execute(RECOUNT_SQL)
            rows = cur.fetchall()
        finally:
            _rollback(conn)
        diff = differing(rows)
        return {"mode": "dry-run", "journals": len(rows), "rows": diff, "gap": gap(diff), "attempts": 1}

    last: tuple[Exception, str] | None = None
    for attempt in range(1, attempts + 1):
        stage = "SET LOCAL"
        try:
            cur.execute(LOCK_TIMEOUT_SQL, [lock_timeout])
            cur.execute(STATEMENT_TIMEOUT_SQL)
            cur.execute(IDLE_TIMEOUT_SQL)
            stage = "LOCK TABLE papers"
            cur.execute(LOCK_SQL)
            if backup_table:
                stage = f"CREATE TABLE {backup_table}"
                cur.execute(BACKUP_SQL.format(sql.Identifier(backup_table)))
                cur.execute(BACKUP_PK_SQL.format(sql.Identifier(backup_table)))
            stage = "recount"
            cur.execute(RECOUNT_SQL)
            rows = cur.fetchall()
            diff = differing(rows)
            if _after_count is not None:
                _after_count()
            if diff:
                stage = "UPDATE journals"
                cur.execute(UPDATE_SQL, [[d[0] for d in diff], [d[2] for d in diff], [d[3] for d in diff]])
                written = {r[0] for r in cur.fetchall()}
                if len(written) != len(diff):
                    _rollback(conn)
                    raise CasMismatch(sorted({d[0] for d in diff} - written))
            stage = "COMMIT"
            try:
                conn.commit()
            except Exception as e:
                raise CommitUnknown(f"{type(e).__name__} during COMMIT") from e
            return {"mode": "apply", "journals": len(rows), "rows": diff, "gap": gap(diff),
                    "attempts": attempt}
        except RETRYABLE as e:
            _rollback(conn)
            last = (e, stage)
            print(f"[resync_journal_counts] attempt {attempt}/{attempts}: {type(e).__name__} "
                  f"(SQLSTATE {e.pgcode}) at {stage}, rolled back", flush=True)
            if attempt < attempts:
                _sleep(retry_wait)
        except (CasMismatch, CommitUnknown):
            raise
        except Exception:
            _rollback(conn)
            raise
    e, stage = last
    raise LockNotAcquired(f"{type(e).__name__} (SQLSTATE {e.pgcode}) at {stage} on all {attempts} attempt(s)")


def _cell(v) -> str:
    return "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")


def summary_markdown(result: dict) -> str:
    """The job-summary text: one sentence, then the corrected journals as a table."""
    rows, n = result["rows"], result["journals"]
    out = ["## journals.paper_count resync", ""]
    if not rows:
        out.append(f"No drift: every journal's `paper_count` equals its core count ({n} journals checked).")
    elif result["mode"] == "apply":
        out.append(f"Drift recurred: {len(rows)} journals corrected (total gap {result['gap']}).")
    else:
        out.append(f"Dry run: {len(rows)} journals differ from their core count (total gap {result['gap']}).")
    if rows:
        out += ["", "| journal id | name | old | new |", "|---|---|---:|---:|"]
        out += [f"| {jid} | {_cell(name)} | {_cell(old)} | {core} |" for jid, name, old, core in rows]
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="print what would change (default)")
    mode.add_argument("--apply", action="store_true", help="correct under a SHARE lock on papers")
    ap.add_argument("--backup-table", help="with --apply: create this table with every journal's id and paper_count first")
    ap.add_argument("--summary-md", help="append a markdown summary to this file (e.g. $GITHUB_STEP_SUMMARY)")
    ap.add_argument("--dsn", help="database URL (default: DATABASE_URL)")
    ap.add_argument("--lock-timeout", default="5s", help="per attempt, under 60s, e.g. 5s or 500ms (default 5s)")
    ap.add_argument("--attempts", type=int, default=3, help="lock attempts before giving up (default 3)")
    ap.add_argument("--retry-wait", type=float, default=30.0, help="seconds between attempts (default 30)")
    a = ap.parse_args(argv)

    if a.backup_table and not a.apply:
        print("--backup-table needs --apply")
        return 2
    if a.backup_table and not TABLE_NAME_RE.match(a.backup_table):
        print(f"--backup-table must match {TABLE_NAME_RE.pattern}")
        return 2
    ms = lock_timeout_ms(a.lock_timeout)
    if ms is None or not 0 < ms < STATEMENT_TIMEOUT_MS:
        print(f"--lock-timeout must match {LOCK_TIMEOUT_RE.pattern} and lie between 0 and 60s exclusive")
        return 2
    if a.attempts < 1 or a.retry_wait < 0:
        print("--attempts must be at least 1 and --retry-wait not negative")
        return 2
    dsn = a.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set (--dsn, environment or .env.local)")
        return 2

    label = "apply" if a.apply else "dry-run"
    try:
        conn = psycopg2.connect(dsn, connect_timeout=30)
    except psycopg2.Error as e:      # libpq's text names the host and role; print the class only
        print(f"[resync_journal_counts] {label}: could not connect ({type(e).__name__}), nothing written")
        return 3
    try:
        conn.autocommit = False
        try:
            result = run(conn, apply=a.apply, backup_table=a.backup_table, lock_timeout=a.lock_timeout,
                         attempts=a.attempts, retry_wait=a.retry_wait)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except LockNotAcquired as e:
        print(f"[resync_journal_counts] {label}: lock not acquired, nothing written: {e}")
        return 1
    except CasMismatch as e:
        print(f"[resync_journal_counts] {label}: compare-and-set mismatch, rolled back, nothing written: {e}")
        return 3
    except CommitUnknown as e:
        print(f"[resync_journal_counts] {label}: commit outcome unknown ({e}); re-run with --dry-run to see "
              f"whether the corrections landed")
        return 4
    except psycopg2.Error as e:
        # a server error's first line names only database objects; a client-side error may quote the DSN
        detail = (str(e).strip().splitlines() or [""])[0] if e.pgcode else ""
        print(f"[resync_journal_counts] {label}: database error {type(e).__name__} "
              f"(SQLSTATE {e.pgcode}), nothing written{': ' + detail if detail else ''}")
        return 3
    except Exception as e:  # noqa: BLE001
        print(f"[resync_journal_counts] {label}: unexpected {type(e).__name__}; outcome unknown, "
              f"re-run with --dry-run to see the state")
        return 4

    tag = "corrected" if a.apply else "would correct"
    for jid, name, old, core in result["rows"]:
        print(f"  [{tag}] journal={jid} name={name} old={old} new={core}")
    if a.backup_table:
        print(f"  [backup] {a.backup_table}: {result['journals']} journals")
    count_key = "corrected" if a.apply else "differing"
    print(f"[resync_journal_counts] {label}: journals={result['journals']} {count_key}={len(result['rows'])} "
          f"gap={result['gap']} attempts={result['attempts']}")
    if a.summary_md:
        try:
            with open(a.summary_md, "a", encoding="utf-8", newline="\n") as f:
                f.write(summary_markdown(result))
        except OSError as e:
            print(f"[resync_journal_counts] {label}: done, but the summary was not written ({type(e).__name__})")
            return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
