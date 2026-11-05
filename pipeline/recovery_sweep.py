"""Tier-1 abstract recovery sweep (GitHub Actions entry).

Selects freshly-ingested papers that have no abstract and that the sweep has
not touched yet, recovers them via the free layers (S2 → CrossRef → Unpaywall),
and marks the residual's abstract_recovery_state so it is never re-processed.

Runs as the final step of ingest.yml (immediately after new papers land) and/or
on its own schedule. Newest-first so just-ingested papers are filled ASAP.

Env (GHA secrets): DATABASE_URL, S2_API_KEY[, S2_API_KEY_2...], OPENALEX_EMAIL,
CROSSREF_EMAIL, UNPAYWALL_EMAIL.

Usage:
    python recovery_sweep.py [--limit 3000] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_client, execute  # noqa: E402
import abstract_recovery as ar  # noqa: E402


def trigger_paper_revalidation(paper_ids: list[str]) -> None:
    """Bust the LiterView ISR cache for the /paper/<id> pages whose abstract we
    just recovered, so they refresh immediately instead of waiting the 10m TTL.
    Best-effort: skips if site config is missing; never raises.
    """
    if not paper_ids:
        return
    base_url = os.environ.get("SITE_URL") or os.environ.get("NEXT_PUBLIC_SITE_URL")
    cron_secret = os.environ.get("CRON_SECRET")
    bypass = os.environ.get("VERCEL_PROTECTION_BYPASS")
    if not base_url or not cron_secret:
        print(f"[revalidate] skipped ({len(paper_ids)} papers): SITE_URL/CRON_SECRET not set")
        return
    url = base_url.rstrip("/") + "/api/revalidate"
    body = json.dumps({"paths": [f"/paper/{pid}" for pid in paper_ids]}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {cron_secret}")
    if bypass:
        req.add_header("x-vercel-protection-bypass", bypass)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"[revalidate] {len(paper_ids)} paper pages -> HTTP {resp.status}")
    except Exception as e:
        print(f"[revalidate] error: {e}")

# Scope gate (A5): only spend the recovery budget on in-scope papers (linked to an
# in-scope discipline). Out-of-scope arXiv/physics no longer starve the S2/CrossRef
# quota. Kept identical in SELECT_FRESH and STATE_DIST_SQL so the distribution matches.
IN_SCOPE = """EXISTS (SELECT 1 FROM paper_disciplines pd JOIN disciplines d
             ON d.id = pd.discipline_id WHERE pd.paper_id = papers.id AND d.in_scope)"""

SELECT_FRESH = f"""
SELECT id::text AS id, doi, title
FROM papers
WHERE abstract IS NULL AND doi IS NOT NULL
  AND (abstract_recovery_state IS NULL
       OR (abstract_recovery_state = 'pending'
           AND abstract_recovery_last < NOW() - INTERVAL '2 days'))
  AND {IN_SCOPE}
ORDER BY created_at DESC NULLS LAST
LIMIT %s
"""

STATE_DIST_SQL = f"""
SELECT COALESCE(abstract_recovery_state, '(null)') AS state, COUNT(*) c
FROM papers WHERE abstract IS NULL AND doi IS NOT NULL
  AND {IN_SCOPE}
GROUP BY 1 ORDER BY c DESC
"""


def collect_s2_keys() -> list[str]:
    keys = []
    if os.environ.get("S2_API_KEY"):
        keys.append(os.environ["S2_API_KEY"])
    for i in range(2, 10):
        k = os.environ.get(f"S2_API_KEY_{i}")
        if k:
            keys.append(k)
    return keys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=3000,
                    help="Max fresh papers to process this run")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_client()
    papers = execute(conn, SELECT_FRESH, [args.limit])
    print(f"[recovery] {len(papers)} fresh papers without abstract (state IS NULL)")
    if not papers:
        conn.close()
        return

    keys = collect_s2_keys()
    email = (os.environ.get("UNPAYWALL_EMAIL")
             or os.environ.get("CROSSREF_EMAIL")
             or os.environ.get("OPENALEX_EMAIL"))
    print(f"[recovery] S2 keys={len(keys)}  email={'set' if email else 'MISSING'}  "
          f"dry_run={args.dry_run}")

    stats = ar.recover_batch(conn, papers, s2_keys=keys, email=email,
                             dry_run=args.dry_run)

    print("[recovery] summary:")
    for k in sorted(stats):
        print(f"   {k:18s} {stats[k]}")
    recovered = sum(v for k, v in stats.items() if k.startswith("recovered_"))
    print(f"[recovery] TOTAL recovered: {recovered}/{stats.get('processed', 0)}")

    print("[recovery] residual state distribution (abstract still NULL):")
    for r in execute(conn, STATE_DIST_SQL):
        print(f"   {r['state']:16s} {r['c']}")

    # Of the papers we processed (all started abstract-NULL), those that now have
    # an abstract were filled this run — refresh just their cached detail pages.
    if not args.dry_run:
        input_ids = [p["id"] for p in papers]
        rows = execute(conn,
                       "SELECT id::text AS id FROM papers "
                       "WHERE id::text = ANY(%s) AND abstract IS NOT NULL",
                       [input_ids])
        trigger_paper_revalidation([r["id"] for r in rows])

    conn.close()


if __name__ == "__main__":
    main()
