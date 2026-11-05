#!/usr/bin/env python3
"""
Find duplicate papers in the database. REPORT-ONLY by default.

Detection (unchanged):
1. DOI matching (most reliable)
2. OpenAlex ID matching
3. Fuzzy title + author + year matching

RESOLUTION -- read this before touching the flags.

Until 2026-07-11 this script RESOLVED duplicates with a bare DELETE: `DELETE FROM paper_disciplines`,
`DELETE FROM paper_tags`, then `DELETE FROM papers`. That is precisely the corruption SCHEMA_EVOLUTION
_ADR D3 exists to forbid. The DELETE cascaded through every FK'd table (paper_subfields, summaries,
user_saved_papers, comments, upvotes) destroying their rows, left no `paper_redirects` row so the old
URL 404'd forever, and once the library lands (039: `library_items.paper_id` is ON DELETE RESTRICT) it
would have started throwing and broken the ingest cron outright. It ran DAILY in the public
academic-crawler's ingest.yml as `--yes --method doi --days 3`. It happened to be a prod no-op --
`papers` carries a UNIQUE index on normalize_doi(doi), so exact-DOI duplicates cannot exist, and the
one gap (this file's Python normalizer also strips a `doi:` prefix, the SQL one does not) matched zero
prod rows -- but it was a live path to silent data loss, not a safe one.

So the DELETE is GONE. The script now:
  * default            -> DETECTS and REPORTS. Writes nothing. This is what the cron runs.
  * --merge            -> resolves each pair through `merge_papers.merge()` (re-point every
                          referencing row, record a redirect, then delete the loser). ATTENDED runs
                          from the private LiterView repo only: `merge_papers` is imported lazily and
                          does not exist in the public crawler mirror, deliberately. The first real
                          merge in this system's history must be run by a human who has looked at the
                          pair, not minted by a cron at 3am with a winner picked by a data-richness
                          heuristic nobody has validated.
  * --merge --dry-run  -> rehearses every merge and rolls it back.

Usage:
    python deduplicate.py --method doi --days 3           # report (what the cron does)
    python deduplicate.py --method doi --merge --dry-run  # rehearse the resolution
    python deduplicate.py --method doi --merge            # resolve, attended
"""

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db import get_client, execute, execute_write


def normalize_doi(doi: str | None) -> str | None:
    """Normalize DOI for comparison."""
    if not doi:
        return None
    doi = doi.lower().strip()
    doi = re.sub(r'^https?://doi\.org/', '', doi)
    doi = re.sub(r'^doi:', '', doi)
    return doi


def normalize_title(title: str | None) -> str:
    """Normalize title for fuzzy comparison."""
    if not title:
        return ""
    # Lowercase
    title = title.lower()
    # Remove HTML tags
    title = re.sub(r'<[^>]+>', '', title)
    # Remove punctuation
    title = re.sub(r'[^\w\s]', '', title)
    # Normalize whitespace
    title = ' '.join(title.split())
    # Take first 100 chars
    return title[:100]


def get_first_author_lastname(authors: list | None) -> str:
    """Extract first author's last name."""
    if not authors or not isinstance(authors, list) or len(authors) == 0:
        return ""

    first_author = authors[0]
    if isinstance(first_author, dict):
        name = first_author.get("name", "")
    else:
        name = str(first_author)

    # Get last word as last name
    parts = name.strip().split()
    if parts:
        return parts[-1].lower()
    return ""


def create_signature(paper: dict) -> str:
    """Create a signature for fuzzy matching."""
    title = normalize_title(paper.get("title"))[:50]
    first_author = get_first_author_lastname(paper.get("authors"))
    published_date = paper.get("published_date") or ""
    year = published_date[:4] if published_date else ""
    return f"{title}|{first_author}|{year}"


def find_duplicates_by_doi(papers: list) -> list[tuple]:
    """Find duplicates by DOI."""
    doi_to_papers = defaultdict(list)

    for paper in papers:
        doi = normalize_doi(paper.get("doi"))
        if doi:
            doi_to_papers[doi].append(paper)

    duplicates = []
    for doi, paper_list in doi_to_papers.items():
        if len(paper_list) > 1:
            # Keep the one with more data (upvotes, comments, etc.)
            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "doi", doi))

    return duplicates


def find_duplicates_by_openalex_id(papers: list) -> list[tuple]:
    """Find duplicates by OpenAlex ID."""
    id_to_papers = defaultdict(list)

    for paper in papers:
        openalex_id = paper.get("openalex_id")
        if openalex_id:
            id_to_papers[openalex_id].append(paper)

    duplicates = []
    for oa_id, paper_list in id_to_papers.items():
        if len(paper_list) > 1:
            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "openalex_id", oa_id))

    return duplicates


def find_duplicates_by_signature(papers: list) -> list[tuple]:
    """Find duplicates by title + author + year signature."""
    sig_to_papers = defaultdict(list)

    for paper in papers:
        sig = create_signature(paper)
        if sig and sig != "||":  # Skip empty signatures
            sig_to_papers[sig].append(paper)

    duplicates = []
    for sig, paper_list in sig_to_papers.items():
        if len(paper_list) > 1:
            # Skip if papers have different DOIs (not actually duplicates)
            dois = {normalize_doi(p.get("doi")) for p in paper_list if p.get("doi")}
            if len(dois) > 1:
                continue

            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    1 if p.get("doi") else 0,  # Prefer papers with DOI
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "signature", sig))

    return duplicates


def main():
    parser = argparse.ArgumentParser(description="Find duplicate papers (report-only unless --merge)")
    parser.add_argument("--merge", action="store_true",
                        help="Resolve each pair via the D3 merge primitive (merge_papers.py). "
                             "ATTENDED, private-repo only — never enable this in a cron.")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --merge: rehearse every merge and roll it back.")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="With --merge: skip the confirmation prompt. No effect on a "
                             "report-only run (kept so the existing cron invocation still parses).")
    parser.add_argument("--method", choices=["all", "doi", "openalex", "signature"], default="all",
                       help="Deduplication method")
    parser.add_argument("--limit", type=int, default=10000, help="Max papers to process")
    parser.add_argument("--days", type=int, default=None,
                       help="Only consider papers created in the last N days (default: all)")
    args = parser.parse_args()

    print("=" * 60)
    print("Paper Deduplication")
    print("=" * 60)

    conn = get_client()

    try:
        # Fetch candidate papers
        cutoff = None
        if args.days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
            print(f"Fetching papers created after {cutoff} (limit: {args.limit})...")
        else:
            print(f"Fetching papers (limit: {args.limit})...")

        if cutoff:
            papers = execute(conn,
                """SELECT id, title, authors, doi, openalex_id, published_date,
                          abstract, upvote_count, comment_count, created_at
                   FROM papers
                   WHERE created_at >= %s
                   ORDER BY created_at DESC
                   LIMIT %s""",
                [cutoff, args.limit])
        else:
            papers = execute(conn,
                """SELECT id, title, authors, doi, openalex_id, published_date,
                          abstract, upvote_count, comment_count, created_at
                   FROM papers
                   ORDER BY created_at DESC
                   LIMIT %s""",
                [args.limit])

        print(f"Loaded {len(papers)} papers")

        # Find duplicates
        all_duplicates = []

        if args.method in ["all", "doi"]:
            print("\nFinding duplicates by DOI...")
            doi_dups = find_duplicates_by_doi(papers)
            print(f"  Found {len(doi_dups)} duplicates")
            all_duplicates.extend(doi_dups)

        if args.method in ["all", "openalex"]:
            print("\nFinding duplicates by OpenAlex ID...")
            oa_dups = find_duplicates_by_openalex_id(papers)
            print(f"  Found {len(oa_dups)} duplicates")
            all_duplicates.extend(oa_dups)

        if args.method in ["all", "signature"]:
            print("\nFinding duplicates by signature (title + author + year)...")
            sig_dups = find_duplicates_by_signature(papers)
            print(f"  Found {len(sig_dups)} duplicates")
            all_duplicates.extend(sig_dups)

        # Deduplicate the duplicate list (same paper might be flagged multiple times)
        seen_removals = set()
        unique_duplicates = []
        for keep_id, remove_id, method, value in all_duplicates:
            if remove_id not in seen_removals:
                unique_duplicates.append((keep_id, remove_id, method, value))
                seen_removals.add(remove_id)

        print(f"\nTotal unique duplicates to remove: {len(unique_duplicates)}")

        if not unique_duplicates:
            print("No duplicates found!")
            return

        # Show sample
        print("\nSample duplicates:")
        for keep_id, remove_id, method, value in unique_duplicates[:10]:
            print(f"  Keep: {keep_id[:8]}... Remove: {remove_id[:8]}... ({method}: {value[:40]}...)")

        if len(unique_duplicates) > 10:
            print(f"  ... and {len(unique_duplicates) - 10} more")

        if not args.merge:
            print(f"\n[report-only] {len(unique_duplicates)} duplicate pair(s) found. Nothing was "
                  f"written.")
            print("  Resolve them with an ATTENDED run from the LiterView repo:")
            print("    python pipeline/deduplicate.py --method <m> --merge --dry-run   # rehearse")
            print("    python pipeline/deduplicate.py --method <m> --merge             # execute")
            return

        # --merge: resolve each pair through the D3 merge primitive. Imported LAZILY -- the public
        # academic-crawler mirror of this file has no merge_papers.py, and must not: merging is an
        # attended, private-repo operation (see the module header).
        from merge_papers import merge, orphan_report

        print(f"\n[merge] resolving {len(unique_duplicates)} pair(s) "
              f"{'(dry-run: each merge is rolled back)' if args.dry_run else ''}")
        if not args.dry_run and not args.yes:
            confirm = input(f"Merge {len(unique_duplicates)} duplicate pair(s)? (y/n): ")
            if confirm.lower() != "y":
                print("Aborted.")
                return

        merged = 0
        errors = 0
        for keep_id, remove_id, method, value in unique_duplicates:
            # One transaction PER PAIR, on its own connection: merge() locks the per-user library
            # counter rows it touches, and batching many merges into one transaction would hold
            # many users' counters (blocking their pushes) at once.
            mconn = get_client()
            try:
                merge(mconn, str(remove_id), str(keep_id),
                      reason=f"deduplicate.py {method}={value}", dry=args.dry_run)
                merged += 1
                if merged % 25 == 0:
                    print(f"  merged {merged}/{len(unique_duplicates)}...")
            except SystemExit as e:
                # merge() aborts (not crashes) on a policy violation -- e.g. an in-scope loser whose
                # winner is out of scope, or a RESTRICT'd table with no bespoke pass. Surface it and
                # keep going; the pair simply stays unresolved.
                print(f"  [skip] {remove_id[:8]} -> {keep_id[:8]}: {e}")
                errors += 1
            except Exception as e:
                print(f"  [error] {remove_id[:8]} -> {keep_id[:8]}: {e}")
                errors += 1
            finally:
                mconn.close()

        print(f"\nDone! merged {merged}, skipped/errored {errors}"
              f"{' (dry-run — nothing committed)' if args.dry_run else ''}")
        if not args.dry_run and merged:
            orph = orphan_report(conn)
            print(f"[orphans] {orph or 'none — 0 across all referencing tables'}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
