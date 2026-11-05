"""embed_new_papers.py -- embed navigable papers that have no paper_embeddings row yet
(ledger task `taste-pipeline`, item 2; design Section 7).

Runs daily in academic-crawler's classify.yml after the classifiers, and as the backfill through
`workflow_dispatch` with `embed_limit`. The selection is "navigable, no row for MODEL_ID yet",
newest `created_at` first, so the step is idempotent and resumable: a chunk that dies halfway
leaves its committed 500-row batches in place, and a merge winner without a vector is picked up
the next day.

  python embed_new_papers.py                 # at most 5,000 papers
  python embed_new_papers.py --limit 500
  python embed_new_papers.py --dry-run       # counts only: no model load, no write
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PIPELINE_DIR))
load_dotenv(dotenv_path=PIPELINE_DIR.parent / ".env.local", override=False)

import taste_embed as te  # noqa: E402
from db import execute, get_client  # noqa: E402

DEFAULT_LIMIT = 5000
NOTICE = "paper_embeddings absent: migration 060 not applied; skipping"

UNEMBEDDED = f"""FROM papers p
 WHERE {te.navigable_paper('p')}
   AND NOT EXISTS (SELECT 1 FROM paper_embeddings e WHERE e.paper_id = p.id AND e.model_id = %s)"""
SELECT_SQL = f"""SELECT p.id::text AS id, p.title, p.abstract {UNEMBEDDED}
 ORDER BY p.created_at DESC, p.id
 LIMIT %s"""
COUNT_SQL = f"SELECT count(*)::int AS n {UNEMBEDDED}"


def remaining(conn) -> int:
    n = execute(conn, COUNT_SQL, [te.MODEL_ID])[0]["n"]
    conn.commit()
    return n


def run(conn, limit: int, dry_run: bool, model=None) -> int:
    te.load_config()
    t0 = time.time()
    if not te.tables_present(conn, "paper_embeddings"):
        print(NOTICE)
        return 0
    dim = te.load_dim(conn)
    if dry_run:
        left = remaining(conn)
        print(f"[embed] dry run: selected {min(limit, left)}, remaining {left} "
              f"(model {te.MODEL_ID}, dim {dim})")
        return 0
    papers = execute(conn, SELECT_SQL, [te.MODEL_ID, limit])
    conn.commit()
    st = te.embed_rows(conn, papers, dim, model) if papers else {"written": 0, "dropped": 0}
    left = remaining(conn)
    print(f"[embed] selected {len(papers)}, written {st['written']}, dropped by resolve "
          f"{st['dropped']}, remaining {left}, wall {time.time() - t0:.1f}s")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    te.load_config()
    conn = get_client()
    try:
        return run(conn, a.limit, a.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
