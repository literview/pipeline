#!/usr/bin/env python3
"""
Ingest papers from Crossref API.

This script fetches recent papers from journals using their ISSNs.
Used as a secondary source to complement OpenAlex ingestion.

Usage:
    python ingest_crossref.py [--days 7] [--per-page 50]
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import requests
import httpx

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db import get_client, execute, execute_write, upsert_paper, link_paper_to_discipline, get_disciplines_map
from journals_config import JOURNAL_ISSNS, JOURNALS_BY_DISCIPLINE
from text_norm import is_truncated_preview
from ingest_work import biblio_from_crossref  # volume, issue, pages (migration 068)

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", os.environ.get("CROSSREF_EMAIL", ""))

# Rate limiting for Crossref API
# With polite pool (mailto header): 50 req/sec
# Without: ~1 req/sec
CROSSREF_RATE_LIMIT = 0.1  # seconds between requests with polite pool
CROSSREF_EMAIL = os.environ.get("CROSSREF_EMAIL", os.environ.get("OPENALEX_EMAIL", ""))

# User agent for Crossref (they prefer this)
USER_AGENT = f"LiterView/1.0 (mailto:{CROSSREF_EMAIL})" if CROSSREF_EMAIL else "LiterView/1.0"


def normalize_doi(doi: str | None) -> str | None:
    """Normalize DOI for deduplication."""
    if not doi:
        return None
    # Remove URL prefix
    doi = doi.lower().strip()
    doi = re.sub(r'^https?://doi\.org/', '', doi)
    doi = re.sub(r'^doi:', '', doi)
    return doi


def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not inverted_index:
        return None
    words = []
    for word, positions in inverted_index.items():
        for pos in positions:
            words.append((pos, word))
    words.sort(key=lambda x: x[0])
    return " ".join(word for _, word in words)


def fetch_abstract_from_openalex(doi: str) -> str | None:
    """Fetch abstract from OpenAlex using DOI as fallback."""
    try:
        params = {"mailto": OPENALEX_EMAIL} if OPENALEX_EMAIL else {}
        url = f"https://api.openalex.org/works/doi:{doi}"
        resp = httpx.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return None
        work = resp.json()
        abstract = work.get("abstract") or reconstruct_abstract(work.get("abstract_inverted_index"))
        # A publisher preview is not an abstract (text_norm.py).
        return None if is_truncated_preview(abstract) else abstract
    except Exception:
        return None


def get_existing_dois(conn) -> set:
    """Get all existing DOIs from the database for deduplication."""
    rows = execute(conn, "SELECT doi FROM papers WHERE doi IS NOT NULL")
    dois = set()
    for row in rows:
        doi = normalize_doi(row.get("doi"))
        if doi:
            dois.add(doi)
    return dois


def fetch_crossref_works(issn: str, from_date: str, rows: int = 50) -> list:
    """
    Fetch works from Crossref API for a specific journal ISSN.

    Args:
        issn: Journal ISSN
        from_date: Start date in YYYY-MM-DD format
        rows: Number of results per page

    Returns:
        List of work items
    """
    url = f"https://api.crossref.org/journals/{issn}/works"
    params = {
        "filter": f"from-pub-date:{from_date}",
        "rows": rows,
        "sort": "published",
        "order": "desc"
    }

    headers = {"User-Agent": USER_AGENT}
    if CROSSREF_EMAIL:
        headers["X-MAILTO"] = CROSSREF_EMAIL

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        time.sleep(CROSSREF_RATE_LIMIT)

        if response.status_code == 404:
            # Journal not found in Crossref
            return []

        response.raise_for_status()
        data = response.json()

        return data.get("message", {}).get("items", [])

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return []
        print(f"  HTTP error for ISSN {issn}: {e}")
        return []
    except Exception as e:
        print(f"  Error fetching ISSN {issn}: {e}")
        return []


def normalize_crossref_paper(item: dict, journal_name: str) -> dict | None:
    """
    Normalize a Crossref work item to our paper format.

    Returns:
        Normalized paper dict, or None if invalid
    """
    # Get DOI
    doi = item.get("DOI")
    if not doi:
        return None

    # Get title
    title_list = item.get("title", [])
    if not title_list:
        return None
    title = title_list[0] if isinstance(title_list, list) else str(title_list)

    if not title or len(title) < 20:
        return None

    # Skip if title is same as journal name
    if title.lower().strip() == journal_name.lower().strip():
        return None

    # Get abstract (Crossref first, then OpenAlex fallback)
    abstract = item.get("abstract", "")
    if abstract:
        # Clean JATS/XML tags
        abstract = re.sub(r'<[^>]+>', '', abstract)
        abstract = abstract.strip()
    if not abstract and doi:
        abstract = fetch_abstract_from_openalex(doi) or ""

    # Get authors
    authors = []
    for author in item.get("author", []):
        name_parts = []
        if author.get("given"):
            name_parts.append(author["given"])
        if author.get("family"):
            name_parts.append(author["family"])

        if name_parts:
            author_entry = {"name": " ".join(name_parts)}
            if author.get("ORCID"):
                author_entry["orcid"] = author["ORCID"].replace("http://orcid.org/", "").replace("https://orcid.org/", "")
            if author.get("affiliation"):
                affiliations = author["affiliation"]
                if affiliations and isinstance(affiliations, list) and affiliations[0].get("name"):
                    author_entry["affiliation"] = affiliations[0]["name"]
            authors.append(author_entry)

    if not authors:
        return None

    # Get published date
    published = item.get("published") or item.get("published-print") or item.get("published-online")
    published_date = None
    if published and published.get("date-parts"):
        date_parts = published["date-parts"][0]
        if len(date_parts) >= 1:
            year = date_parts[0]
            month = date_parts[1] if len(date_parts) >= 2 else 1
            day = date_parts[2] if len(date_parts) >= 3 else 1
            try:
                published_date = f"{year:04d}-{month:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass

    if not published_date:
        return None

    # Get URL
    url = f"https://doi.org/{doi}"

    # Get PDF URL if available
    pdf_url = None
    links = item.get("link", [])
    for link in links:
        if link.get("content-type") == "application/pdf":
            pdf_url = link.get("URL")
            break

    return {
        "title": title,
        "abstract": abstract or None,
        "authors": authors,
        "source": journal_name,
        "doi": doi,
        "url": url,
        "pdf_url": pdf_url,
        "published_date": published_date,
        **biblio_from_crossref(item),  # absent keys when Crossref has no value
    }


def get_discipline_for_journal(journal_name: str) -> str | None:
    """Find the discipline for a journal name."""
    journal_lower = journal_name.lower()
    for discipline, journals in JOURNALS_BY_DISCIPLINE.items():
        for source_id, name in journals.items():
            if name.lower() == journal_lower:
                return discipline
    return None


def main():
    parser = argparse.ArgumentParser(description="Ingest papers from Crossref")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--per-page", type=int, default=50, help="Papers per journal")
    parser.add_argument("--discipline", type=str, help="Only process specific discipline")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    args = parser.parse_args()

    print("=" * 60)
    print("Crossref Ingestion")
    print("=" * 60)

    if not CROSSREF_EMAIL:
        print("Warning: No CROSSREF_EMAIL set. Rate limits will be stricter.")

    # Calculate date range
    from_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    print(f"Fetching papers from {from_date}")

    # Get database connection
    conn = get_client()

    try:
        # Load disciplines map for linking
        disciplines_map = get_disciplines_map(conn)
        print(f"Loaded {len(disciplines_map)} disciplines")

        # Get existing DOIs for deduplication
        print("Loading existing DOIs...")
        existing_dois = get_existing_dois(conn)
        print(f"Found {len(existing_dois)} existing DOIs")

        # Build list of journals with ISSNs
        journals_to_fetch = []
        for journal_name, issn in JOURNAL_ISSNS.items():
            if not issn:
                continue
            discipline = get_discipline_for_journal(journal_name)
            if args.discipline and discipline != args.discipline:
                continue
            journals_to_fetch.append({
                "name": journal_name,
                "issn": issn,
                "discipline": discipline
            })

        print(f"Processing {len(journals_to_fetch)} journals with ISSNs")

        # Stats
        stats = {
            "journals_processed": 0,
            "papers_found": 0,
            "papers_new": 0,
            "papers_duplicate": 0,
            "papers_invalid": 0,
            "papers_inserted": 0,
        }

        for journal in journals_to_fetch:
            journal_name = journal["name"]
            issn = journal["issn"]
            discipline = journal["discipline"]

            print(f"\n[{discipline or 'unknown'}] {journal_name} (ISSN: {issn})...")

            works = fetch_crossref_works(issn, from_date, args.per_page)
            stats["journals_processed"] += 1
            stats["papers_found"] += len(works)

            if not works:
                print(f"  No papers found")
                continue

            print(f"  Found {len(works)} papers")

            for item in works:
                paper = normalize_crossref_paper(item, journal_name)

                if not paper:
                    stats["papers_invalid"] += 1
                    continue

                # Check for duplicate by DOI
                normalized_doi = normalize_doi(paper["doi"])
                if normalized_doi in existing_dois:
                    stats["papers_duplicate"] += 1
                    continue

                stats["papers_new"] += 1

                if args.dry_run:
                    print(f"    [DRY RUN] Would insert: {paper['title'][:60]}...")
                    continue

                # Insert into database
                try:
                    result, _inserted = upsert_paper(conn, paper)  # (paper_id, inserted) since capture-drain-ingest
                    if result:
                        stats["papers_inserted"] += 1
                        existing_dois.add(normalized_doi)  # Add to set to prevent duplicates in same run

                        # Link to discipline
                        if discipline and discipline in disciplines_map:
                            link_paper_to_discipline(conn, result, disciplines_map[discipline], source="crossref")
                except Exception as e:
                    print(f"    Error inserting paper: {e}")

        # Print summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Journals processed:  {stats['journals_processed']}")
        print(f"Papers found:        {stats['papers_found']}")
        print(f"Papers new:          {stats['papers_new']}")
        print(f"Papers duplicate:    {stats['papers_duplicate']}")
        print(f"Papers invalid:      {stats['papers_invalid']}")
        print(f"Papers inserted:     {stats['papers_inserted']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
