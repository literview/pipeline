"""
Journal-based paper ingestion for LiterView.

Fetches papers from ALL journals in journals_config.py, batched by discipline.
Each discipline's journals are queried together using OpenAlex OR filter.
"""
import os
import time
import httpx
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from db import get_client, upsert_paper, link_paper_to_discipline, get_disciplines_map
from db import _match_existing as db_match
from journals_config import JOURNALS_BY_DISCIPLINE
# The per-work half of this file now lives in ingest_work.py, so the capture drain ingests
# under the same normalization (ledger task capture-drain-ingest, design item 2). Re-exported
# under their old names: every existing caller and test keeps working.
from ingest_work import is_valid_paper, normalize_openalex, reconstruct_abstract  # noqa: F401

PIPELINE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=PIPELINE_DIR.parent / ".env.local")
load_dotenv()  # fallback for .env

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "")


def trigger_revalidation(saved: int) -> None:
    """Bust the ISR cache for feed pages that depend on newly-ingested papers.

    POSTs to the site's /api/revalidate (Bearer CRON_SECRET). Vercel Deployment
    Protection is bypassed via x-vercel-protection-bypass when configured.
    Retries a non-2xx / network failure up to 3 times with a short backoff and
    then RAISES: a silently swallowed failure leaves every author and feed page
    stale until its TTL, with nothing in the cron result to show it. Missing
    config is still a logged skip (a local run has no deployed site to bust).
    """
    base_url = os.environ.get("SITE_URL") or os.environ.get("NEXT_PUBLIC_SITE_URL")
    cron_secret = os.environ.get("CRON_SECRET")
    bypass = os.environ.get("VERCEL_PROTECTION_BYPASS")

    if not base_url or not cron_secret:
        print("[Revalidate] Skipped: SITE_URL or CRON_SECRET not set")
        return

    url = base_url.rstrip("/") + "/api/revalidate"
    headers = {"Authorization": f"Bearer {cron_secret}"}
    if bypass:
        headers["x-vercel-protection-bypass"] = bypass

    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            resp = httpx.post(url, headers=headers, timeout=30)
            if 200 <= resp.status_code < 300:
                print(f"[Revalidate] OK ({saved} new papers), attempt {attempt}/{attempts}: "
                      f"{resp.json().get('revalidated')}")
                return
            print(f"[Revalidate] Attempt {attempt}/{attempts} failed "
                  f"({resp.status_code}): {resp.text[:200]}")
        except Exception as e:
            print(f"[Revalidate] Attempt {attempt}/{attempts} error: {e}")
        if attempt < attempts:
            time.sleep(2 * attempt)

    raise RuntimeError(f"[Revalidate] Giving up after {attempts} attempts: {url}")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_papers_by_sources(
    source_ids: list[str],
    from_date: str,
    to_date: str | None = None,
    per_page: int = 200,
    max_papers: int = 100000,
) -> list[dict]:
    """
    Fetch papers from multiple sources via OpenAlex within [from_date, to_date].
    Uses OR filter to batch all source IDs for a discipline into one query.
    Cursor pagination (OpenAlex page-based capped at 10k).
    """
    papers = []
    source_filter = "|".join(source_ids)

    filter_parts = [
        f"from_publication_date:{from_date}",
        f"primary_location.source.id:{source_filter}",
    ]
    if to_date:
        filter_parts.insert(1, f"to_publication_date:{to_date}")

    url = "https://api.openalex.org/works"
    params = {
        "filter": ",".join(filter_parts),
        "sort": "publication_date:desc",
        "per-page": per_page,
    }
    if OPENALEX_EMAIL:
        params["mailto"] = OPENALEX_EMAIL

    cursor = "*"
    while cursor and len(papers) < max_papers:
        params["cursor"] = cursor
        response = httpx.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            paper = normalize_openalex(work)
            if paper:
                papers.append(paper)

        cursor = data.get("meta", {}).get("next_cursor")
        time.sleep(0.1)

    return papers


def save_papers(client, papers: list[dict], historical: bool) -> tuple[int, int]:
    """Upsert each paper and link it to its discipline; returns (saved, linked).

    A paper that a --from/--to/--year run inserts is a deliberate past collection, origin
    'backfill' (owner decision O4, DECISIONS 2026-09-17), which papers_set_use_date() dates by
    publication on INSERT. `origin` goes into the paper only when no row matches, as the capture
    drain does: on a match upsert_paper updates every key it is given, so a matched row keeps its
    own. Before this, the 2026-04-16 period load went in as 'pipeline' and dated 125,860 papers of
    2020-2025 to that day (docs/ledger/use-date-april.md).
    """
    saved = 0
    linked = 0
    for paper in papers:
        try:
            discipline_id = paper.pop("_discipline_id")
            discipline_slug = paper.pop("_discipline_slug")

            if historical and db_match(client, paper) is None:
                paper["origin"] = "backfill"
            paper_id, _inserted = upsert_paper(client, paper)
            if paper_id:
                saved += 1
                link_paper_to_discipline(client, paper_id, discipline_id, source="journal")
                linked += 1

        except Exception as e:
            print(f"[Ingest] Error saving paper: {e}")
    return saved, linked


def main(
    from_date: str | None = None,
    to_date: str | None = None,
    days: int = 7,
    per_page: int = 200,
    dry_run: bool = False,
):
    """Run the journal-based ingestion pipeline.

    Either pass (from_date, to_date) for historical backfill, or leave them None
    to use the rolling --days window (default cron behavior).
    """
    historical = from_date is not None
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        window_desc = f"last {days} days (from {from_date})"
    else:
        window_desc = f"{from_date} to {to_date or 'now'}"

    print(f"[Ingest] Starting journal-based ingestion at {datetime.now().isoformat()}")
    print(f"[Ingest] Window: {window_desc}  dry_run={dry_run}")

    # Count total journals
    total_journals = sum(len(journals) for journals in JOURNALS_BY_DISCIPLINE.values())
    print(f"[Ingest] {len(JOURNALS_BY_DISCIPLINE)} disciplines, {total_journals} journals")

    client = get_client()
    disciplines_map = get_disciplines_map(client)
    print(f"[Ingest] Loaded {len(disciplines_map)} disciplines from DB")

    # Build reverse map: OpenAlex source URL -> config journal name
    source_id_to_name = {}
    for disc, journals in JOURNALS_BY_DISCIPLINE.items():
        for src_id, name in journals.items():
            source_id_to_name[f"https://openalex.org/{src_id}"] = name

    all_papers = []
    seen_ids = set()

    for disc_idx, (discipline_slug, journals) in enumerate(JOURNALS_BY_DISCIPLINE.items(), 1):
        discipline_id = disciplines_map.get(discipline_slug)
        if not discipline_id:
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: not in DB, skipping")
            continue

        source_ids = list(journals.keys())
        if not source_ids:
            continue

        try:
            papers = fetch_papers_by_sources(source_ids, from_date=from_date, to_date=to_date, per_page=per_page)
            new_count = 0

            for paper in papers:
                openalex_id = paper.get("openalex_id")
                if openalex_id and openalex_id not in seen_ids:
                    seen_ids.add(openalex_id)
                    # Normalize source name to match config
                    src_oa_id = paper.pop("source_openalex_id", None)
                    if src_oa_id and src_oa_id in source_id_to_name:
                        paper["source"] = source_id_to_name[src_oa_id]
                    paper["_discipline_id"] = discipline_id
                    paper["_discipline_slug"] = discipline_slug
                    all_papers.append(paper)
                    new_count += 1

            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug} ({len(source_ids)} journals): {new_count} papers")
            time.sleep(0.1)

        except Exception as e:
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: Error - {e}")

    print(f"\n[Ingest] Collected {len(all_papers)} unique papers from {len(JOURNALS_BY_DISCIPLINE)} disciplines")

    if dry_run:
        print("[Ingest] --dry-run: skipping DB writes")
        print(f"[Ingest] Completed at {datetime.now().isoformat()}")
        return

    # Save to database
    saved, linked = save_papers(client, all_papers, historical)

    print(f"[Ingest] Saved {saved} papers to database")
    print(f"[Ingest] Linked {linked} papers to disciplines")

    # Only bust the cache when papers were actually written this run.
    if saved > 0:
        trigger_revalidation(saved)

    print(f"[Ingest] Completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Rolling window (days back from today). Ignored if --from/--year set.")
    parser.add_argument("--from", dest="from_date", type=str, default=None, help="Start date YYYY-MM-DD (historical backfill)")
    parser.add_argument("--to", dest="to_date", type=str, default=None, help="End date YYYY-MM-DD (optional; pairs with --from)")
    parser.add_argument("--year", type=int, default=None, help="Shortcut: --year 2024 → --from 2024-01-01 --to 2024-12-31")
    parser.add_argument("--per-page", type=int, default=200, help="Papers per API page")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and count only; skip DB writes")
    args = parser.parse_args()

    from_date = args.from_date
    to_date = args.to_date
    if args.year is not None:
        if from_date or to_date:
            parser.error("--year cannot be combined with --from/--to")
        from_date = f"{args.year}-01-01"
        to_date = f"{args.year}-12-31"

    main(
        from_date=from_date,
        to_date=to_date,
        days=args.days,
        per_page=args.per_page,
        dry_run=args.dry_run,
    )
