"""build_taste_pool.py -- the weekly card pool `taste_cards` (ledger task `taste-pipeline`, item 3).

Version `pool-<UTC date>`; a version that already exists ends the run with exit 0. Per visible
subfield (the curated-visibility rule of lib/onboarding.ts:140-143), the `pool.perSubfield`
most-cited navigable papers published within `pool.years` with an abstract over 100 characters
(docs/ledger/reports/taste-test-offline/extract.py:31-36, restricted to navigable papers), minus
titles equal to a journal name and titles under three words (build_owner_test_v2.py:28-29, L6).
A paper tagged with two visible subfields gets a row in each (contracts C8).

Clusters: spherical k-means within each subfield, seed 0, 25 iterations, k = min(pool.k, members),
members in paper-id order, cluster ids 0.. renumbered by first appearance (as the offline build
did, without its global offset). `neighbours`: the cardSelector.nearNeighbours nearest distinct
papers of the version's whole pool by inner product, the paper itself excluded, ties to the lower
paper id. Members without a vector are embedded first (taste_embed.embed_missing).

The insert and the retention (pool versions older than the newest eight deleted, L7) are one
transaction. Skips with a notice and exit 0 while taste_cards or paper_embeddings is absent (L5).
"""
from __future__ import annotations

import datetime as dt
import sys
import time
from pathlib import Path

import numpy as np
import psycopg2.extras
from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PIPELINE_DIR))
load_dotenv(dotenv_path=PIPELINE_DIR.parent / ".env.local", override=False)

import taste_embed as te  # noqa: E402
from db import execute, get_client  # noqa: E402

NOTICE = "taste_cards or paper_embeddings absent: migration 060 not applied; skipping"
KEEP_VERSIONS = 8
KMEANS_SEED = 0
KMEANS_ITERS = 25

VISIBLE_SUBFIELDS_SQL = """
SELECT t.id::text AS id FROM tags t JOIN disciplines d ON d.id = t.discipline_id
 WHERE t.type = 'subfield' AND d.in_scope
   AND (t.cluster_label IS NOT NULL OR NOT EXISTS (
        SELECT 1 FROM tags t2 WHERE t2.discipline_id = t.discipline_id
           AND t2.type = 'subfield' AND t2.cluster_label IS NOT NULL))
 ORDER BY t.id"""

MEMBERS_SQL = f"""
SELECT x.subfield_id, p.id::text AS paper_id, p.title, coalesce(p.citation_count, 0) AS citations
  FROM (SELECT pt.tag_id::text AS subfield_id, pt.paper_id,
               row_number() OVER (PARTITION BY pt.tag_id
                                  ORDER BY p2.citation_count DESC NULLS LAST, p2.id) AS rn
          FROM paper_tags pt JOIN papers p2 ON p2.id = pt.paper_id
         WHERE pt.tag_id = ANY(%s::uuid[])
           AND p2.published_year BETWEEN %s AND %s
           AND p2.abstract IS NOT NULL AND length(p2.abstract) > 100
           AND {te.navigable_paper('p2')}) x
  JOIN papers p ON p.id = x.paper_id
 WHERE x.rn <= %s
 ORDER BY x.subfield_id, p.id"""


def pool_version(now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    return f"pool-{now.astimezone(dt.timezone.utc).date().isoformat()}"


def title_kept(title, journal_names: set[str]) -> bool:
    t = (title or "").strip()
    return t.lower() not in journal_names and len(t.split()) >= 3


def kmeans(X: np.ndarray, k: int, iters: int = KMEANS_ITERS, seed: int = KMEANS_SEED) -> list[int]:
    """Spherical k-means as in build_owner_test_v2.py:38-45, k = min(k, len(X)); labels renumbered
    0.. by first appearance."""
    k = min(k, len(X))
    g = np.random.default_rng(seed)
    C = X[g.choice(len(X), k, replace=False)].copy()
    for _ in range(iters):
        a = np.argmax(X @ C.T, 1)
        for c in range(len(C)):
            m = X[a == c]
            if len(m):
                v = m.mean(0)
                C[c] = v / np.linalg.norm(v)
    raw = np.argmax(X @ C.T, 1)
    remap: dict[int, int] = {}
    out = []
    for lab in raw.tolist():
        if lab not in remap:
            remap[lab] = len(remap)
        out.append(remap[lab])
    return out


def nearest_neighbours(ids: list[str], V: np.ndarray, n: int, chunk: int = 1000) -> dict[str, list[str]]:
    """ids sorted ascending, V row-aligned. The n nearest other ids by inner product."""
    out: dict[str, list[str]] = {}
    for s in range(0, len(ids), chunk):
        S = V[s:s + chunk] @ V.T
        for j in range(S.shape[0]):
            S[j, s + j] = -np.inf
        order = np.argsort(-S, axis=1, kind="stable")[:, :min(n, len(ids) - 1)]
        for j, row in enumerate(order):
            out[ids[s + j]] = [ids[i] for i in row]
    return out


def build_rows(version: str, members: list[dict], vectors: dict[str, np.ndarray], k: int,
               near: int) -> list[tuple]:
    """members: dicts with subfield_id, paper_id, citations (already title-filtered, each with a
    vector). Returns taste_cards rows (pool_version, subfield_id, paper_id, cluster, citations,
    neighbours)."""
    ids = sorted({m["paper_id"] for m in members})
    V = np.stack([vectors[i] for i in ids]).astype(np.float32) if ids else np.zeros((0, 1), np.float32)
    nbrs = nearest_neighbours(ids, V, near) if ids else {}
    by_sf: dict[str, list[dict]] = {}
    for m in members:
        by_sf.setdefault(m["subfield_id"], []).append(m)
    rows = []
    for sf in sorted(by_sf):
        ms = sorted(by_sf[sf], key=lambda m: m["paper_id"])
        X = np.stack([vectors[m["paper_id"]] for m in ms]).astype(np.float32)
        labels = kmeans(X, k)
        for m, lab in zip(ms, labels):
            rows.append((version, sf, m["paper_id"], int(lab), int(m["citations"]), nbrs[m["paper_id"]]))
    return rows


def versions_to_delete(versions: list[str], keep: int = KEEP_VERSIONS) -> list[str]:
    return sorted(set(versions), reverse=True)[keep:]


def run(conn, now: dt.datetime | None = None, model=None) -> int:
    cfg = te.load_config()
    pool, near = cfg["pool"], cfg["cardSelector"]["nearNeighbours"]
    t0 = time.time()
    if not te.tables_present(conn, "taste_cards", "paper_embeddings"):
        print(NOTICE)
        return 0
    dim = te.load_dim(conn)
    version = pool_version(now)
    if execute(conn, "SELECT 1 AS x FROM taste_cards WHERE pool_version = %s LIMIT 1", [version]):
        conn.commit()
        print(f"[pool] {version} exists; nothing to do")
        return 0

    sfs = [r["id"] for r in execute(conn, VISIBLE_SUBFIELDS_SQL)]
    journal_names = {(r["name"] or "").strip().lower() for r in execute(conn, "SELECT name FROM journals")}
    raw = execute(conn, MEMBERS_SQL, [sfs, pool["years"][0], pool["years"][1], pool["perSubfield"]])
    conn.commit()
    members = [m for m in raw if title_kept(m["title"], journal_names)]
    ids = sorted({m["paper_id"] for m in members})
    st = te.embed_missing(conn, ids, dim, model)
    vectors = te.load_vectors(conn, ids)
    conn.commit()
    no_vec = [m for m in members if m["paper_id"] not in vectors]
    members = [m for m in members if m["paper_id"] in vectors]
    rows = build_rows(version, members, vectors, pool["k"], near)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, "INSERT INTO taste_cards (pool_version, subfield_id, paper_id, cluster, citations, "
                 "neighbours) VALUES %s", rows,
            template="(%s, %s::uuid, %s::uuid, %s, %s, %s::uuid[])", page_size=1000)
        cur.execute("SELECT DISTINCT pool_version FROM taste_cards")
        old = versions_to_delete([r[0] for r in cur.fetchall()])
        if old:
            cur.execute("DELETE FROM taste_cards WHERE pool_version = ANY(%s)", [old])
    conn.commit()
    print(f"[pool] {version}: {len(rows)} rows over {len({r[1] for r in rows})} subfields, "
          f"{len({r[2] for r in rows})} distinct papers; dropped by title filters "
          f"{len(raw) - len(members) - len(no_vec)}, without a vector {len(no_vec)}; embedded "
          f"{st['written']}; versions deleted {old or 'none'}; wall {time.time() - t0:.1f}s")
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
