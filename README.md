# academic-crawler

## What it does

- **Ingest** (every 6h): Fetches new papers from OpenAlex, Crossref, and arXiv APIs across 200+ journals and 32 disciplines. Deduplicates by DOI.
- **Classify** (daily): Tags papers with subfield labels using keyword matching and sentence-transformer embeddings.

## Data sources

- [OpenAlex API](https://openalex.org/) - primary source
- [Crossref API](https://www.crossref.org/) - secondary fallback
- [arXiv API](https://arxiv.org/) - preprints

## Setup

Secrets required (set in GitHub repo settings):
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `OPENALEX_EMAIL`

## Local development

```bash
pip install -r requirements-ingest.txt
cd pipeline
python ingest_journals.py --days 7
python deduplicate.py --yes --method doi --days 3
```
