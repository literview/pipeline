"""
Journal-based paper ingestion for LiterView.

Fetches papers from ALL journals in journals_config.py, batched by discipline.
Each discipline's journals are queried together using OpenAlex OR filter.
"""
import os
import sys
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

# Disciplines whose shared journals link a paper to every one of them (ledger task ingest-multi-discipline-link, owner
# "가" 2026-11-09): a journal listed under both political science and public policy gives its papers both links. A
# journal on any other pair of lists (Administrative Science Quarterly, the three finance journals) keeps its first
# listed discipline until a primary-discipline rule is decided.
MULTI_LINK_SLUGS = frozenset({"political-science", "public-policy"})


def link_slugs_for(slugs: list[str]) -> list[str]:
    """The disciplines a journal's papers link to, from its listed slugs in config order: every slug when all of them are
    in MULTI_LINK_SLUGS, otherwise the first only. Journal ingest, Crossref ingest and the backfill all call this."""
    if slugs and all(s in MULTI_LINK_SLUGS for s in slugs):
        return list(slugs)
    return list(slugs[:1])


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


def _describe(e: BaseException) -> str:
    """A failure for the log without the request URL: the URL carries the mailto address percent-encoded, which the
    Actions log of the public crawler repo would not mask."""
    if isinstance(e, httpx.HTTPStatusError):
        return f"HTTP {e.response.status_code}"
    return f"{type(e).__name__}: {e}"


class PartialFetchError(Exception):
    """A page fetch still failed after its retries. `papers` holds the normalized papers of the pages read before it
    (possibly none), `page` the 1-based number of the failed page, `cause` its last exception. main() keeps the papers
    and counts the failure (ledger task openalex-loud-failure)."""

    def __init__(self, papers: list[dict], page: int, cause: BaseException):
        super().__init__(f"page {page} failed after {len(papers)} papers: {_describe(cause)}")
        self.papers = papers
        self.page = page
        self.cause = cause


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
def _get_page(url: str, params: dict) -> dict:
    """One OpenAlex page, three attempts. The retry sits here rather than on the whole fetch so that a retry repeats
    only the failing page and a final failure keeps the pages already read."""
    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


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
    Raises PartialFetchError, carrying the papers already read, when a page still fails after its retries.
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
    page = 0
    while cursor and len(papers) < max_papers:
        params["cursor"] = cursor
        page += 1
        try:
            data = _get_page(url, params)
        except Exception as e:
            raise PartialFetchError(papers, page, e) from e

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
    """Upsert each paper and link it to each of its disciplines; returns (saved, linked), `linked` counting links.

    A paper carries `_discipline_ids` (main(), ingest-multi-discipline-link) or the older single `_discipline_id`
    (`_discipline_slug` beside it); all three keys are popped before the upsert, because upsert_paper writes every key it
    is given and an underscore key would fail the write inside this function's swallowed except.

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
            ids = paper.pop("_discipline_ids", None)
            one = paper.pop("_discipline_id", None)
            paper.pop("_discipline_slug", None)
            ids = ids or [one]

            if historical and db_match(client, paper) is None:
                paper["origin"] = "backfill"
            paper_id, _inserted = upsert_paper(client, paper)
            if paper_id:
                saved += 1
                for discipline_id in ids:
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

    Returns the number of disciplines whose fetch failed (0 when none did); the script exits 1 when it is not 0.
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

    # Build reverse maps: OpenAlex source URL -> config journal name, and -> the disciplines its papers link to
    # (link_slugs_for over the source's listed slugs in config order, only slugs present in the database)
    source_id_to_name = {}
    listed_slugs: dict[str, list[str]] = {}
    for disc, journals in JOURNALS_BY_DISCIPLINE.items():
        for src_id, name in journals.items():
            url = f"https://openalex.org/{src_id}"
            source_id_to_name[url] = name
            if disc in disciplines_map:
                listed_slugs.setdefault(url, []).append(disc)
    source_id_to_slugs = {url: link_slugs_for(slugs) for url, slugs in listed_slugs.items()}
    absent = 0

    all_papers = []
    seen_ids = set()
    failed: list[str] = []

    for disc_idx, (discipline_slug, journals) in enumerate(JOURNALS_BY_DISCIPLINE.items(), 1):
        discipline_id = disciplines_map.get(discipline_slug)
        if not discipline_id:
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: not in DB, skipping")
            continue

        source_ids = list(journals.keys())
        if not source_ids:
            continue

        try:
            try:
                papers = fetch_papers_by_sources(source_ids, from_date=from_date, to_date=to_date, per_page=per_page)
            except PartialFetchError as e:
                failed.append(discipline_slug)
                papers = e.papers
                print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: Error - {e}; "
                      f"keeping {len(papers)} papers already read")
            new_count = 0

            for paper in papers:
                openalex_id = paper.get("openalex_id")
                if openalex_id and openalex_id not in seen_ids:
                    seen_ids.add(openalex_id)
                    # Normalize source name to match config
                    src_oa_id = paper.pop("source_openalex_id", None)
                    if src_oa_id and src_oa_id in source_id_to_name:
                        paper["source"] = source_id_to_name[src_oa_id]
                    # the source's disciplines (a paper of a journal on both the Politics and the Policy lists links to
                    # both, whichever discipline's query returned it first); a source absent from the map links to the
                    # discipline that fetched it
                    slugs = source_id_to_slugs.get(src_oa_id) if src_oa_id else None
                    if slugs:
                        paper["_discipline_ids"] = [disciplines_map[s] for s in slugs]
                    else:
                        paper["_discipline_ids"] = [discipline_id]
                        absent += 1
                    all_papers.append(paper)
                    new_count += 1

            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug} ({len(source_ids)} journals): {new_count} papers")
            time.sleep(0.1)

        except Exception as e:
            if discipline_slug not in failed:
                failed.append(discipline_slug)
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: Error - {e}")

    print(f"\n[Ingest] Collected {len(all_papers)} unique papers from {len(JOURNALS_BY_DISCIPLINE)} disciplines")
    print(f"[Ingest] {absent} papers with a source id absent from the map")

    if dry_run:
        print("[Ingest] --dry-run: skipping DB writes")
        print(f"[Ingest] Completed at {datetime.now().isoformat()}")
        return _report_failures(failed)

    # Save to database
    saved, linked = save_papers(client, all_papers, historical)

    print(f"[Ingest] Saved {saved} papers to database")
    print(f"[Ingest] Linked {linked} paper-discipline links")

    # Only bust the cache when papers were actually written this run.
    if saved > 0:
        trigger_revalidation(saved)

    print(f"[Ingest] Completed at {datetime.now().isoformat()}")
    return _report_failures(failed)


def _report_failures(failed: list[str]) -> int:
    if failed:
        print(f"[Ingest] {len(failed)} discipline(s) failed: {', '.join(failed)}")
    return len(failed)


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

    failures = main(
        from_date=from_date,
        to_date=to_date,
        days=args.days,
        per_page=args.per_page,
        dry_run=args.dry_run,
    )
    if failures:
        sys.exit(1)
