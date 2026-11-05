"""
Paper ingestion pipeline for LitPulse.

Fetches papers from multiple sources:
- OpenAlex (primary)
- NBER RSS
- arXiv API
- Journal RSS feeds
"""
import os
import time
import httpx
import feedparser
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from db import get_client, upsert_paper, link_paper_to_discipline, link_paper_to_tag, get_disciplines_map, get_tags_map
from text_norm import is_truncated_preview
from ingest_work import biblio_from_openalex  # volume, issue, pages (migration 068)

load_dotenv()

# Configuration
OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "")

# ============================================
# OpenAlex Concept ID → Discipline Slug Mapping
# ============================================
# OpenAlex concept IDs from https://docs.openalex.org/about-the-data/concept

OPENALEX_DISCIPLINE_MAP = {
    # Social Sciences
    "C162324750": "political-science",
    "C17744445": "economics",
    "C144133560": "sociology",
    "C15744967": "psychology",
    "C167639068": "communications",
    "C147597530": "education",
    "C143118070": "business",
    "C199539241": "public-policy",

    # Natural Sciences
    "C121332964": "physics",
    "C185592680": "chemistry",
    "C86803240": "biology",
    "C127313418": "earth-sciences",
    "C39432304": "environmental-science",

    # Humanities
    "C95457728": "history",
    "C138885662": "philosophy",
    "C138496976": "law",
    "C124952713": "literature",
    "C41895202": "linguistics",
    "C142362112": "art-history",
    "C39549134": "religious-studies",

    # Engineering & Technology
    "C41008148": "computer-science",
    "C127413603": "electrical-engineering",
    "C78519656": "mechanical-engineering",
    "C67555558": "civil-engineering",
    "C192562407": "materials-science",

    # Health Sciences
    "C71924100": "medicine",
    "C118552586": "public-health",
    "C3001206": "nursing",
    "C55493867": "pharmacology",

    # Formal Sciences
    "C33923547": "mathematics",
    "C105795698": "statistics",
    "C136764020": "logic",
}

# OpenAlex concept → subfield tag mapping (more specific)
OPENALEX_SUBFIELD_MAP = {
    # Political Science
    "C2776168843": "american-politics",
    "C79316296": "comparative-politics",
    "C76697960": "international-relations",
    "C178229462": "political-theory",

    # Economics
    "C105795698": "econometrics",
    "C139719470": "macroeconomics",
    "C50522688": "microeconomics",
    "C73283319": "labor-economics",
    "C118084267": "public-economics",
    "C76716069": "development-economics",
    "C183700895": "behavioral-economics",
    "C193601281": "finance",

    # Computer Science
    "C154945302": "artificial-intelligence",
    "C119857082": "machine-learning-cs",
    "C31972630": "computer-vision",
    "C204321447": "natural-language-processing",
    "C11413529": "human-computer-interaction",

    # Physics
    "C109214941": "particle-physics",
    "C76932067": "condensed-matter",
    "C1965285": "astrophysics",
    "C62520636": "quantum-physics",
    "C160667983": "optics",

    # Biology
    "C153911025": "molecular-biology",
    "C95444343": "cell-biology",
    "C54355233": "genetics",
    "C18903297": "ecology",
    "C2908647359": "evolutionary-biology",
    "C134306372": "neuroscience",
}

# ============================================
# arXiv Category Prefix → Discipline Slug Mapping
# ============================================
# Maps arXiv category prefixes to our discipline slugs
# See: https://arxiv.org/category_taxonomy

ARXIV_TO_DISCIPLINE = {
    # Computer Science
    "cs": "computer-science",

    # Mathematics
    "math": "mathematics",

    # Statistics
    "stat": "statistics",

    # Economics
    "econ": "economics",

    # Quantitative Finance → Economics
    "q-fin": "economics",

    # Quantitative Biology → Biology
    "q-bio": "biology",

    # Electrical Engineering
    "eess": "electrical-engineering",

    # Physics (all physics-related categories)
    "physics": "physics",
    "astro-ph": "physics",
    "cond-mat": "physics",
    "gr-qc": "physics",
    "hep-ex": "physics",
    "hep-lat": "physics",
    "hep-ph": "physics",
    "hep-th": "physics",
    "math-ph": "physics",
    "nlin": "physics",
    "nucl-ex": "physics",
    "nucl-th": "physics",
    "quant-ph": "physics",
}

# All arXiv categories to fetch (main category prefixes)
ARXIV_CATEGORIES_TO_FETCH = [
    "cs",       # Computer Science (40 subcategories)
    "stat",     # Statistics (6 subcategories)
    "econ",     # Economics (3 subcategories)
    "q-fin",    # Quantitative Finance (9 subcategories)
    "q-bio",    # Quantitative Biology (10 subcategories)
    "math",     # Mathematics (32 subcategories)
    "eess",     # Electrical Engineering (4 subcategories)
    "physics",  # General Physics
    "astro-ph", # Astrophysics
    "cond-mat", # Condensed Matter
    "quant-ph", # Quantum Physics
    "hep-th",   # High Energy Physics - Theory
    "hep-ph",   # High Energy Physics - Phenomenology
    "gr-qc",    # General Relativity
]


# ============================================
# OpenAlex
# ============================================

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_openalex_by_concept(concept_id: str, days: int = 7, per_page: int = 100, max_pages: int = 5) -> list[dict]:
    """
    Fetch recent papers from OpenAlex for a specific concept.
    Uses cursor-based pagination.
    https://docs.openalex.org/
    """
    papers = []

    from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor = "*"  # Initial cursor for first page
    page = 0

    while cursor and page < max_pages:
        url = "https://api.openalex.org/works"
        params = {
            "filter": f"from_publication_date:{from_date},concepts.id:{concept_id}",
            "sort": "cited_by_count:desc",  # Sort by citations to get notable papers
            "per-page": per_page,
            "cursor": cursor,
            "mailto": OPENALEX_EMAIL,
        }

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
        page += 1
        time.sleep(0.1)  # Rate limiting

    return papers


def fetch_openalex(days: int = 7, per_page: int = 50) -> list[dict]:
    """
    Fetch recent papers from OpenAlex across all tracked disciplines.
    """
    all_papers = []
    seen_ids = set()

    # Fetch from each major discipline concept
    for concept_id, discipline_slug in OPENALEX_DISCIPLINE_MAP.items():
        try:
            papers = fetch_openalex_by_concept(concept_id, days=days, per_page=per_page)

            # Deduplicate
            for paper in papers:
                openalex_id = paper.get("openalex_id")
                if openalex_id and openalex_id not in seen_ids:
                    seen_ids.add(openalex_id)
                    all_papers.append(paper)

            print(f"[OpenAlex] Fetched {len(papers)} papers for {discipline_slug}")
            time.sleep(0.1)  # Be polite to API

        except Exception as e:
            print(f"[OpenAlex] Error fetching {discipline_slug}: {e}")

    print(f"[OpenAlex] Total unique papers: {len(all_papers)}")
    return all_papers


def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not inverted_index:
        return None

    # Build list of (position, word) tuples
    words = []
    for word, positions in inverted_index.items():
        for pos in positions:
            words.append((pos, word))

    # Sort by position and join
    words.sort(key=lambda x: x[0])
    return " ".join(word for _, word in words)


def is_valid_paper(title: str, source: str | None, authors: list) -> bool:
    """
    Validate that this is a real paper, not journal metadata or spurious data.
    Returns False for papers that should be filtered out.
    """
    if not title:
        return False

    title_lower = title.strip().lower()
    source_lower = (source or "").strip().lower()

    # 1. Title matches source (journal metadata, not a paper)
    if source_lower and title_lower == source_lower:
        return False

    # 2. Title is too short (likely not a real paper)
    if len(title) < 20:
        return False

    # 3. No authors at all
    if not authors or len(authors) == 0:
        return False

    # 4. All authors have empty names
    if all(not a.get("name", "").strip() for a in authors):
        return False

    # 5. Title looks like a journal/venue name (common patterns)
    journal_patterns = [
        "journal of",
        "proceedings of",
        "transactions on",
        "annals of",
        "bulletin of",
        "review of",
        "advances in",
        "reports",  # e.g., "Molecular Medicine Reports"
        "letters",
        "communications",
    ]
    # Only filter if title is short AND matches journal pattern
    if len(title) < 50:
        for pattern in journal_patterns:
            if title_lower == pattern or title_lower.endswith(f" {pattern}"):
                return False

    return True


def _fetch_crossref_authors(doi: str) -> list[dict] | None:
    """Fetch canonical author list from Crossref for a DOI.

    Returns list of {name, orcid} in order, or None on failure.
    Crossref is authoritative for DOI metadata; used to cross-check OpenAlex's
    author disambiguation, which occasionally merges distinct people with
    similar initials (e.g. Frances E. Lee vs F Eun-Hyung Lee).
    """
    if not doi:
        return None
    doi_clean = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    try:
        resp = httpx.get(
            f"https://api.crossref.org/works/{doi_clean}",
            headers={"User-Agent": "LiterView/1.0 (mailto:literview@proton.me)"},
            timeout=8.0,
        )
        if resp.status_code != 200:
            return None
        msg = resp.json().get("message", {})
        out = []
        for a in msg.get("author", []):
            given = (a.get("given") or "").strip()
            family = (a.get("family") or "").strip()
            name = f"{given} {family}".strip() if given else family
            if not name:
                continue
            orcid = a.get("ORCID") or ""
            orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip() or None
            out.append({"name": name, "orcid": orcid})
        return out or None
    except Exception:
        return None


def _verify_openalex_id_via_orcid(orcid: str) -> str | None:
    """Resolve an ORCID to the true OpenAlex author ID. Returns None on failure."""
    try:
        resp = httpx.get(
            f"https://api.openalex.org/authors/orcid:{orcid}",
            params={"mailto": "literview@proton.me"},
            timeout=8.0,
        )
        if resp.status_code == 200:
            return resp.json().get("id")
    except Exception:
        pass
    return None


def _cross_check_authors_with_crossref(doi: str, oa_authors: list[dict]) -> list[dict]:
    """Reconcile OpenAlex author list with Crossref by position.

    When Crossref has an ORCID that disagrees with the OpenAlex-reported author
    (different name or ID), trust Crossref: use its name/ORCID and re-verify the
    OpenAlex author ID via ORCID lookup. Prevents propagation of OpenAlex
    disambiguation errors.
    """
    cr_authors = _fetch_crossref_authors(doi)
    if not cr_authors:
        return oa_authors

    for i, oa in enumerate(oa_authors):
        if i >= len(cr_authors):
            break
        cr = cr_authors[i]
        cr_orcid = cr.get("orcid")
        oa_orcid = oa.get("orcid")
        oa_name = (oa.get("name") or "").strip()
        cr_name = (cr.get("name") or "").strip()

        # Name conflict signal: last tokens disagree or initials don't match
        def _last(n: str) -> str:
            return n.split()[-1].lower() if n else ""
        name_conflict = cr_name and oa_name and _last(cr_name) != _last(oa_name)

        if cr_orcid and (oa_orcid != cr_orcid or name_conflict):
            # Crossref has ORCID and OpenAlex either lacks it or disagrees.
            # Trust Crossref; re-verify OpenAlex ID via ORCID.
            verified_id = _verify_openalex_id_via_orcid(cr_orcid)
            if verified_id != oa.get("openalex_id"):
                print(f"  [CrossCheck] Author mismatch for {doi}: "
                      f"OA='{oa_name}' ({oa.get('openalex_id')}) -> "
                      f"CR='{cr_name}' (orcid:{cr_orcid}, verified_oa={verified_id})")
            oa["name"] = cr_name
            oa["orcid"] = cr_orcid
            oa["openalex_id"] = verified_id  # may be None; safer than a wrong ID
        elif cr_name and not oa_orcid and _last(cr_name) != _last(oa_name):
            # No ORCID either side but names disagree at family name — null the
            # OpenAlex ID to avoid linking to the wrong author profile.
            print(f"  [CrossCheck] Name-only mismatch for {doi}: "
                  f"OA='{oa_name}' -> CR='{cr_name}' (nulling openalex_id)")
            oa["name"] = cr_name
            oa["openalex_id"] = None

    return oa_authors


def normalize_openalex(work: dict) -> dict | None:
    """Convert OpenAlex work to our paper format."""
    title = work.get("title")
    if not title:
        return None

    # Extract abstract from inverted index
    abstract = work.get("abstract") or reconstruct_abstract(work.get("abstract_inverted_index"))
    if is_truncated_preview(abstract):  # a publisher preview, not an abstract (text_norm.py)
        abstract = None

    # Extract authors
    authors = []
    for authorship in work.get("authorships", [])[:10]:  # Limit to 10 authors
        author = authorship.get("author", {})
        institution = ""
        if authorship.get("institutions"):
            institution = authorship["institutions"][0].get("display_name", "")

        # Validate ORCID: filter out empty or malformed ORCIDs
        orcid = author.get("orcid")
        if orcid:
            # Strip URL prefix and check if there's an actual ID
            orcid_id = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip()
            if not orcid_id or len(orcid_id) < 10:
                orcid = None

        authors.append({
            "name": author.get("display_name", ""),
            "affiliation": institution,
            "orcid": orcid,
            "openalex_id": author.get("id"),  # e.g. "https://openalex.org/A5023888391"
        })

    # Cross-check authors against Crossref when DOI is available.
    doi_for_check = work.get("doi")
    if doi_for_check and authors:
        authors = _cross_check_authors_with_crossref(doi_for_check, authors)

    # Get best available URL
    url = work.get("doi") or work.get("id")
    pdf_url = None
    if work.get("open_access", {}).get("oa_url"):
        pdf_url = work["open_access"]["oa_url"]

    # Extract source (journal/venue name)
    source = None
    primary_location = work.get("primary_location") or {}
    if primary_location.get("source"):
        source = primary_location["source"].get("display_name")

    # Validate paper quality
    if not is_valid_paper(title, source, authors):
        return None

    # Extract concept IDs for discipline/subfield mapping
    concept_ids = []
    for concept in work.get("concepts", []):
        if concept.get("score", 0) >= 0.5:  # Only concepts with high confidence
            concept_id = concept.get("id", "").replace("https://openalex.org/", "")
            if concept_id:
                concept_ids.append(concept_id)

    return {
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "source": source,  # Journal/venue name
        "published_date": work.get("publication_date"),
        "published_year": work.get("publication_year"),
        "doi": work.get("doi"),
        "url": url,
        "pdf_url": pdf_url,
        "openalex_id": work.get("id"),
        "citation_count": work.get("cited_by_count", 0),
        "concept_ids": concept_ids,  # For discipline/subfield linking
        **biblio_from_openalex(work),  # absent keys when biblio has no value
    }


# ============================================
# NBER RSS
# ============================================

def fetch_nber_rss() -> list[dict]:
    """
    Fetch papers from NBER RSS feeds.
    https://www.nber.org/rss
    """
    papers = []

    # NBER program feeds relevant to social science
    feeds = [
        "https://www.nber.org/rss/new.xml",  # All new papers
        # Can add specific program feeds like:
        # "https://www.nber.org/rss/newpe.xml",  # Public Economics
        # "https://www.nber.org/rss/newpol.xml",  # Political Economy
    ]

    for feed_url in feeds:
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries[:50]:  # Limit per feed
                paper = normalize_nber(entry)
                if paper:
                    papers.append(paper)

            time.sleep(1)  # Be polite

        except Exception as e:
            print(f"[NBER] Error fetching {feed_url}: {e}")

    print(f"[NBER] Fetched {len(papers)} papers")
    return papers


def normalize_nber(entry: dict) -> dict | None:
    """Convert NBER RSS entry to our paper format."""
    title = entry.get("title")
    if not title:
        return None

    # Parse authors from title or description
    # NBER format varies, this is a basic extraction
    authors = []
    if entry.get("author"):
        authors = [{"name": entry["author"]}]

    # Validate paper quality
    if not is_valid_paper(title, None, authors):
        return None

    return {
        "title": title,
        "abstract": entry.get("summary"),
        "authors": authors,
        "source": "NBER Working Papers",
        "published_date": entry.get("published"),
        "url": entry.get("link"),
        "pdf_url": entry.get("link"),  # NBER links usually go to PDF
    }


# ============================================
# arXiv
# ============================================

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_arxiv(max_results_per_category: int = 50, categories: list[str] | None = None) -> list[dict]:
    """
    Fetch papers from arXiv API for specified categories.
    https://info.arxiv.org/help/api/

    Args:
        max_results_per_category: Max papers to fetch per category
        categories: List of arXiv category prefixes (e.g., ["cs", "math"]).
                   If None, uses ARXIV_CATEGORIES_TO_FETCH.
    """
    papers = []
    seen_ids = set()  # Dedupe by arxiv_id

    if categories is None:
        categories = ARXIV_CATEGORIES_TO_FETCH

    for cat in categories:
        url = "https://export.arxiv.org/api/query"
        params = {
            "search_query": f"cat:{cat}.*",
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "max_results": max_results_per_category,
        }

        try:
            response = httpx.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Parse Atom feed
            feed = feedparser.parse(response.text)

            cat_count = 0
            for entry in feed.entries:
                paper = normalize_arxiv(entry)
                if paper and paper.get("arxiv_id") not in seen_ids:
                    seen_ids.add(paper["arxiv_id"])
                    papers.append(paper)
                    cat_count += 1

            print(f"[arXiv] {cat}: {cat_count} papers")

        except Exception as e:
            print(f"[arXiv] Error fetching {cat}: {e}")

        time.sleep(3)  # arXiv rate limit: 1 req per 3 seconds

    print(f"[arXiv] Total: {len(papers)} papers from {len(categories)} categories")
    return papers


def normalize_arxiv(entry: dict) -> dict | None:
    """Convert arXiv entry to our paper format with all available metadata."""
    title = entry.get("title")
    if not title:
        return None

    title = title.replace("\n", " ").strip()

    # Extract arxiv ID from URL
    arxiv_id = None
    if entry.get("id"):
        arxiv_id = entry["id"].split("/abs/")[-1]

    # Parse authors with affiliations
    authors = []
    for author in entry.get("authors", []):
        author_data = {"name": author.get("name", "")}
        # feedparser stores arxiv:affiliation in author dict
        affiliation = author.get("arxiv_affiliation")
        if affiliation:
            author_data["affiliation"] = affiliation
        authors.append(author_data)

    # Validate paper quality
    if not is_valid_paper(title, None, authors):
        return None

    # Get PDF link and DOI link
    pdf_url = None
    doi_url = None
    for link in entry.get("links", []):
        if link.get("type") == "application/pdf":
            pdf_url = link.get("href")
        if link.get("title") == "doi":
            doi_url = link.get("href")

    # Extract categories
    arxiv_categories = []
    for tag in entry.get("tags", []):
        term = tag.get("term")
        if term:
            arxiv_categories.append(term)

    # Primary category (arxiv:primary_category)
    arxiv_primary_category = None
    primary_cat = entry.get("arxiv_primary_category")
    if primary_cat:
        arxiv_primary_category = primary_cat.get("term")

    # DOI (arxiv:doi)
    doi = entry.get("arxiv_doi")

    # Journal reference (arxiv:journal_ref)
    journal_ref = entry.get("arxiv_journal_ref")
    if journal_ref:
        journal_ref = journal_ref.replace("\n", " ").strip()

    # Author comment (arxiv:comment) - often contains conference info
    arxiv_comment = entry.get("arxiv_comment")
    if arxiv_comment:
        arxiv_comment = arxiv_comment.replace("\n", " ").strip()

    # Updated date (for versioned papers)
    updated_date = entry.get("updated")

    return {
        "title": title,
        "abstract": entry.get("summary", "").replace("\n", " ").strip(),
        "authors": authors,
        "source": "arXiv",
        "published_date": entry.get("published"),
        "updated_date": updated_date,
        "url": entry.get("link"),
        "pdf_url": pdf_url,
        "arxiv_id": arxiv_id,
        "doi": doi,
        "arxiv_categories": arxiv_categories,
        "arxiv_primary_category": arxiv_primary_category,
        "arxiv_comment": arxiv_comment,
        "journal_ref": journal_ref,
    }


# ============================================
# Main
# ============================================

def link_paper_concepts(client, paper_id: str, concept_ids: list[str], disciplines_map: dict, tags_map: dict) -> None:
    """Link a paper to disciplines and tags based on its OpenAlex concepts."""
    linked_disciplines = set()
    linked_tags = set()

    for concept_id in concept_ids:
        # Check if concept maps to a discipline
        if concept_id in OPENALEX_DISCIPLINE_MAP:
            discipline_slug = OPENALEX_DISCIPLINE_MAP[concept_id]
            if discipline_slug in disciplines_map:
                discipline_id = disciplines_map[discipline_slug]
                if discipline_id not in linked_disciplines:
                    link_paper_to_discipline(client, paper_id, discipline_id)
                    linked_disciplines.add(discipline_id)

        # Check if concept maps to a subfield tag
        if concept_id in OPENALEX_SUBFIELD_MAP:
            tag_name = OPENALEX_SUBFIELD_MAP[concept_id]
            if tag_name in tags_map:
                tag_id = tags_map[tag_name]
                if tag_id not in linked_tags:
                    link_paper_to_tag(client, paper_id, tag_id)
                    linked_tags.add(tag_id)


def get_discipline_from_arxiv_category(category: str) -> str | None:
    """
    Get discipline slug from arXiv category.

    Args:
        category: arXiv category like "cs.AI" or "math.AG"

    Returns:
        Discipline slug or None if not found
    """
    if not category:
        return None

    # Extract prefix (e.g., "cs" from "cs.AI", "astro-ph" from "astro-ph.CO")
    if "." in category:
        prefix = category.split(".")[0]
    else:
        prefix = category

    return ARXIV_TO_DISCIPLINE.get(prefix)


def link_arxiv_paper_disciplines(client, paper_id: str, arxiv_categories: list[str], disciplines_map: dict) -> int:
    """
    Link an arXiv paper to disciplines based on its categories.

    Args:
        client: Supabase client
        paper_id: Paper UUID
        arxiv_categories: List of arXiv categories (e.g., ["cs.AI", "cs.LG"])
        disciplines_map: Mapping of discipline slug -> discipline UUID

    Returns:
        Number of disciplines linked
    """
    if not arxiv_categories:
        return 0

    linked_disciplines = set()

    for category in arxiv_categories:
        discipline_slug = get_discipline_from_arxiv_category(category)
        if discipline_slug and discipline_slug in disciplines_map:
            discipline_id = disciplines_map[discipline_slug]
            if discipline_id not in linked_disciplines:
                link_paper_to_discipline(client, paper_id, discipline_id, source="arxiv")
                linked_disciplines.add(discipline_id)

    return len(linked_disciplines)


def main():
    """Run the ingestion pipeline."""
    print(f"[Ingest] Starting at {datetime.now().isoformat()}")

    client = get_client()

    # Load discipline and tag mappings
    disciplines_map = get_disciplines_map(client)  # slug -> id
    tags_map = get_tags_map(client)  # name -> id

    print(f"[Ingest] Loaded {len(disciplines_map)} disciplines and {len(tags_map)} tags")

    # Collect from all sources
    all_papers = []
    all_papers.extend(fetch_openalex(days=7))
    all_papers.extend(fetch_nber_rss())
    all_papers.extend(fetch_arxiv())

    print(f"[Ingest] Total collected: {len(all_papers)} papers")

    # Deduplicate and save
    saved = 0
    linked = 0
    for paper in all_papers:
        try:
            # Extract fields not stored directly in DB
            concept_ids = paper.pop("concept_ids", [])
            arxiv_categories = paper.get("arxiv_categories", [])

            paper_id, _inserted = upsert_paper(client, paper)  # (paper_id, inserted) since capture-drain-ingest
            if paper_id:
                saved += 1

                # Link to disciplines and tags based on OpenAlex concepts
                if concept_ids:
                    link_paper_concepts(client, paper_id, concept_ids, disciplines_map, tags_map)
                    linked += 1

                # Link to disciplines based on arXiv categories
                elif arxiv_categories:
                    count = link_arxiv_paper_disciplines(client, paper_id, arxiv_categories, disciplines_map)
                    if count > 0:
                        linked += 1

                # Link NBER papers to economics discipline
                elif paper.get("source") == "NBER Working Papers":
                    if "economics" in disciplines_map:
                        link_paper_to_discipline(client, paper_id, disciplines_map["economics"], source="nber")
                        linked += 1

        except Exception as e:
            print(f"[Ingest] Error saving paper: {e}")

    print(f"[Ingest] Saved {saved} papers to database")
    print(f"[Ingest] Linked {linked} papers to disciplines/tags")
    print(f"[Ingest] Completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
