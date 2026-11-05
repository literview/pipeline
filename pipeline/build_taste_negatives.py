"""build_taste_negatives.py -- the monthly negatives version `taste_negatives` (ledger task
`taste-pipeline`, item 4; contracts C3 and v1.2 "Negatives").

Version `neg-<UTC year-month>`; a version that already exists ends the run with exit 0, and no
version is ever deleted. `negatives.count` navigable papers of every publication year, drawn at
random with a generator seeded by the version string (the same version draws the same papers from
the same corpus). Drawn papers without a vector are embedded first (taste_embed.embed_missing);
the insert is one transaction, which asserts exactly `negatives.count` rows each with a vector
before it commits. Skips with a notice and exit 0 while taste_negatives or paper_embeddings is
absent (L5).
"""
from __future__ import annotations

import datetime as dt
import random
import sys
import time
from pathlib import Path

import psycopg2.extras
from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PIPELINE_DIR))
load_dotenv(dotenv_path=PIPELINE_DIR.parent / ".env.local", override=False)

import taste_embed as te  # noqa: E402
from db import execute, get_client  # noqa: E402

NOTICE = "taste_negatives or paper_embeddings absent: migration 060 not applied; skipping"

NAVIGABLE_IDS_SQL = f"SELECT p.id::text AS id FROM papers p WHERE {te.navigable_paper('p')} ORDER BY p.id"
CHECK_SQL = """SELECT count(*)::int AS n, count(e.paper_id)::int AS with_vec
  FROM taste_negatives n
  LEFT JOIN paper_embeddings e ON e.paper_id = n.paper_id AND e.model_id = %s
 WHERE n.version = %s"""


def negatives_version(now: dt.datetime | None = None) -> str:
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    return f"neg-{now.year:04d}-{now.month:02d}"


def draw(ids: list[str], version: str, count: int) -> list[str]:
    """`count` ids drawn without replacement, seeded by the version string; ids sorted first so the
    draw does not depend on the order the database returned them in."""
    return sorted(random.Random(version).sample(sorted(ids), count))


def run(conn, now: dt.datetime | None = None, model=None) -> int:
    cfg = te.load_config()
    count = cfg["negatives"]["count"]
    t0 = time.time()
    if not te.tables_present(conn, "taste_negatives", "paper_embeddings"):
        print(NOTICE)
        return 0
    dim = te.load_dim(conn)
    version = negatives_version(now)
    if execute(conn, "SELECT 1 AS x FROM taste_negatives WHERE version = %s LIMIT 1", [version]):
        conn.commit()
        print(f"[negatives] {version} exists; nothing to do")
        return 0
    ids = [r["id"] for r in execute(conn, NAVIGABLE_IDS_SQL)]
    conn.commit()
    if len(ids) < count:
        raise RuntimeError(f"only {len(ids)} navigable papers, negatives.count is {count}")
    chosen = draw(ids, version, count)
    st = te.embed_missing(conn, chosen, dim, model)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, "INSERT INTO taste_negatives (version, paper_id) VALUES %s",
            [(version, p) for p in chosen], template="(%s, %s::uuid)", page_size=1000)
        cur.execute(CHECK_SQL, [te.MODEL_ID, version])
        n, with_vec = cur.fetchone()
    if n != count or with_vec != count:
        conn.rollback()
        raise RuntimeError(f"{version}: {n} rows, {with_vec} with a vector, expected {count} of each; "
                           f"rolled back")
    conn.commit()
    print(f"[negatives] {version}: {n} rows, all with a vector; embedded {st['written']} "
          f"(dropped by resolve {st['dropped']}); wall {time.time() - t0:.1f}s")
    return 0


def main() -> int:
    te.load_config()
    conn = get_client()
    try:
        return run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
