"""
arXiv paper ingestion script.

Fetches papers from arXiv API across multiple categories
and links them to disciplines.
"""
import argparse
from datetime import datetime

from db import get_client, upsert_paper, get_disciplines_map
from ingest import fetch_arxiv, link_arxiv_paper_disciplines, ARXIV_CATEGORIES_TO_FETCH


def main():
    parser = argparse.ArgumentParser(description='Ingest papers from arXiv')
    parser.add_argument('--max-per-category', type=int, default=50,
                        help='Max papers to fetch per category (default: 50)')
    parser.add_argument('--categories', type=str, default=None,
                        help='Comma-separated list of categories (default: all)')
    args = parser.parse_args()

    print(f"[arXiv Ingest] Starting at {datetime.now().isoformat()}")

    # Parse categories
    categories = None
    if args.categories:
        categories = [c.strip() for c in args.categories.split(',')]
        print(f"[arXiv Ingest] Categories: {categories}")
    else:
        print(f"[arXiv Ingest] Categories: {ARXIV_CATEGORIES_TO_FETCH}")

    # Fetch papers
    papers = fetch_arxiv(
        max_results_per_category=args.max_per_category,
        categories=categories
    )

    if not papers:
        print("[arXiv Ingest] No papers fetched")
        return

    # Save to database
    client = get_client()
    disciplines_map = get_disciplines_map(client)
    print(f"[arXiv Ingest] Loaded {len(disciplines_map)} disciplines")

    saved = 0
    linked = 0
    errors = 0

    for paper in papers:
        try:
            arxiv_categories = paper.get("arxiv_categories", [])

            paper_id, _inserted = upsert_paper(client, paper)  # (paper_id, inserted) since capture-drain-ingest
            if paper_id:
                saved += 1

                # Link to disciplines
                count = link_arxiv_paper_disciplines(
                    client, paper_id, arxiv_categories, disciplines_map
                )
                if count > 0:
                    linked += 1

        except Exception as e:
            errors += 1
            print(f"[arXiv Ingest] Error saving {paper.get('arxiv_id')}: {e}")

    print(f"[arXiv Ingest] Results:")
    print(f"  - Fetched: {len(papers)} papers")
    print(f"  - Saved: {saved} papers")
    print(f"  - Linked: {linked} papers to disciplines")
    print(f"  - Errors: {errors}")
    print(f"[arXiv Ingest] Completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
