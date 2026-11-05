"""Per-work normalization, shared by the journal crawl and the capture drain.

Ledger task capture-drain-ingest (T-A), design item 2. Before this file the three ingest
scripts each held their own normalizer, and the drain — which ingests ONE work at a time,
resolved by the capture route rather than pulled from a feed — had none. The per-work half is
extracted here so `papers` keeps one normalization whichever writer runs:

  * `normalize_openalex`, `is_valid_paper` and `reconstruct_abstract` come from
    `ingest_journals.py`, which now calls them (its outputs on its existing tests are unchanged);
  * `normalize_crossref` is `ingest_crossref.normalize_crossref_paper` without its OpenAlex
    abstract fallback (the drain must not spend a second network call inside a row's
    transaction) and with the journal name taken from the record's own `container-title`;
  * `normalize_arxiv` is `ingest.normalize_arxiv`, reading the same feedparser-shaped entry —
    which the capture route now produces from the arXiv Atom response
    (`lib/library/resolve-work.ts parseArxivAtom`, pinned by pipeline/fixtures).

`would_ingest(record, source)` is the second half: the test the crawl applies implicitly by
never fetching a non-article at all. The drain must apply it explicitly, because the capture
route resolves whatever the user was reading — a book, a dataset, a landing page. It answers
`(True, None)` or `(False, reason)`, and the reason reaches the user's card
(`not_article:monograph` renders as "Not a paper (book)").

No module-level side effects: importing this file opens no connection and reads no environment.
"""
from __future__ import annotations

import re

try:  # consumers that put pipeline/ on sys.path (every pipeline script)
    from journals_config import JOURNALS_BY_DISCIPLINE
    from text_norm import is_truncated_preview
except ImportError:  # consumers that import this as a package
    from pipeline.journals_config import JOURNALS_BY_DISCIPLINE
    from pipeline.text_norm import is_truncated_preview

SOURCES = ("openalex", "crossref", "arxiv")

# Crossref `type` values that are a paper, and the ones we name when refusing. A type outside
# BOTH sets is refused too, by name — an unknown type is not evidence of an article.
CROSSREF_ACCEPT = {"journal-article", "proceedings-article", "posted-content"}
CROSSREF_REJECT = {"book", "monograph", "book-chapter", "edited-book", "dataset"}

# OpenAlex `type` values that are NOT a paper (owner decision 2026-09-18). The rule runs the
# other way round from Crossref's: anything not named here passes on is_valid_paper alone, so
# journal content matches the crawl, which never looks at `type`. The set exists because the
# resolution chain asks OpenAlex FIRST and OpenAlex indexes books — a monograph with a title and
# an author clears is_valid_paper, so without this a captured book became a corpus paper and the
# card could never say "Not a paper".
OPENALEX_REJECT = {"book", "book-chapter", "edited-book", "monograph", "dataset",
                   "reference-entry", "libguides", "paratext", "supplementary-materials",
                   "grant", "standard"}

ARXIV_VERSION = re.compile(r"v[0-9]+$")

# OpenAlex source id (URL form) -> (discipline slug, the config's own journal name).
_SOURCE_INDEX: dict[str, tuple[str, str]] = {}
_NAME_INDEX: dict[str, tuple[str, str]] = {}
for _slug, _journals in JOURNALS_BY_DISCIPLINE.items():
    for _sid, _name in _journals.items():
        _SOURCE_INDEX[f"https://openalex.org/{_sid}"] = (_slug, _name)
        _NAME_INDEX[_name.strip().lower()] = (_slug, _name)


# ── shared helpers (verbatim from ingest_journals.py) ────────────────────────

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


def is_valid_paper(title: str, source: str | None, authors: list) -> bool:
    """Validate that this is a real paper."""
    if not title:
        return False

    title_lower = title.strip().lower()
    source_lower = (source or "").strip().lower()

    if source_lower and title_lower == source_lower:
        return False

    if len(title) < 20:
        return False

    if not authors or len(authors) == 0:
        return False

    if all(not a.get("name", "").strip() for a in authors):
        return False

    return True


# ── volume, issue and pages (migration 068) ──────────────────────────────────
# The four `papers` columns of migration 068 (ledger task lp3-biblio-columns). A key is present
# only when the source has a value, so a record without one never sends the column at all, and
# PRESERVE_IF_EMPTY in db.py keeps a stored value when a re-crawl sends an empty one.

BIBLIO_COLUMNS = ("volume", "issue", "first_page", "last_page")
_PAGE_SPLIT = re.compile("[-–]")  # a hyphen or an en dash, split on the first one only


def _biblio_value(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def biblio_from_openalex(work: dict) -> dict:
    """The present values of OpenAlex `biblio` (volume, issue, first_page, last_page), trimmed."""
    biblio = work.get("biblio") or {}
    out = {}
    for key in BIBLIO_COLUMNS:
        value = _biblio_value(biblio.get(key))
        if value is not None:
            out[key] = value
    return out


def biblio_from_crossref(item: dict) -> dict:
    """Crossref `volume` and `issue`, and `page` split on its first hyphen or en dash into
    first_page and last_page; `article-number` becomes first_page when `page` is absent (the
    way OpenAlex fills first_page for article-numbered journals)."""
    out = {}
    for key in ("volume", "issue"):
        value = _biblio_value(item.get(key))
        if value is not None:
            out[key] = value
    page = _biblio_value(item.get("page"))
    if page is not None:
        parts = _PAGE_SPLIT.split(page, maxsplit=1)
        first = _biblio_value(parts[0])
        last = _biblio_value(parts[1]) if len(parts) > 1 else None
        if first is not None:
            out["first_page"] = first
        if last is not None:
            out["last_page"] = last
    else:
        number = _biblio_value(item.get("article-number"))
        if number is not None:
            out["first_page"] = number
    return out


# ── normalizers ──────────────────────────────────────────────────────────────

def normalize_openalex(work: dict) -> dict | None:
    """Convert OpenAlex work to our paper format."""
    title = work.get("title")
    if not title:
        return None

    abstract = work.get("abstract") or reconstruct_abstract(work.get("abstract_inverted_index"))
    # A publisher preview is not an abstract. None keeps a stored abstract on a re-crawl
    # (upsert_paper's COALESCE) and leaves a new row in the recovery queue.
    if is_truncated_preview(abstract):
        abstract = None

    authors = []
    for authorship in work.get("authorships", [])[:10]:
        author = authorship.get("author", {}) or {}
        institution = ""
        if authorship.get("institutions"):
            institution = authorship["institutions"][0].get("display_name", "")

        authors.append({
            "name": author.get("display_name", ""),
            "affiliation": institution,
            "orcid": author.get("orcid"),
            "openalex_id": author.get("id"),  # e.g. "https://openalex.org/A5023888391"
        })

    url = work.get("doi") or work.get("id")
    pdf_url = None
    if (work.get("open_access") or {}).get("oa_url"):
        pdf_url = work["open_access"]["oa_url"]

    source = None
    source_openalex_id = None
    primary_location = work.get("primary_location") or {}
    if primary_location.get("source"):
        source = primary_location["source"].get("display_name")
        source_openalex_id = primary_location["source"].get("id")

    if not is_valid_paper(title, source, authors):
        return None

    return {
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "source": source,
        "source_openalex_id": source_openalex_id,
        "published_date": work.get("publication_date"),
        "published_year": work.get("publication_year"),
        "doi": work.get("doi"),
        "url": url,
        "pdf_url": pdf_url,
        "openalex_id": work.get("id"),
        "citation_count": work.get("cited_by_count", 0),
        **biblio_from_openalex(work),
    }


def normalize_crossref(item: dict, journal_name: str | None = None) -> dict | None:
    """A Crossref work in our paper format. `journal_name` defaults to the record's own
    container-title; unlike ingest_crossref.py this never falls back to OpenAlex for a missing
    abstract (one network call per row inside a transaction is not worth an abstract the daily
    recovery cron fills anyway)."""
    doi = item.get("DOI")
    if not doi:
        return None

    title_list = item.get("title", [])
    if not title_list:
        return None
    title = title_list[0] if isinstance(title_list, list) else str(title_list)

    if not title or len(title) < 20:
        return None

    if journal_name is None:
        ct = item.get("container-title") or []
        journal_name = (ct[0] if isinstance(ct, list) and ct else "") or ""

    if journal_name and title.lower().strip() == journal_name.lower().strip():
        return None

    abstract = item.get("abstract", "")
    if abstract:
        abstract = re.sub(r"<[^>]+>", "", abstract).strip()

    authors = []
    for author in item.get("author", []):
        name_parts = []
        if author.get("given"):
            name_parts.append(author["given"])
        if author.get("family"):
            name_parts.append(author["family"])

        if name_parts:
            entry = {"name": " ".join(name_parts)}
            if author.get("ORCID"):
                entry["orcid"] = (author["ORCID"].replace("http://orcid.org/", "")
                                                 .replace("https://orcid.org/", ""))
            affiliations = author.get("affiliation")
            if affiliations and isinstance(affiliations, list) and affiliations[0].get("name"):
                entry["affiliation"] = affiliations[0]["name"]
            authors.append(entry)

    if not authors:
        return None

    published = (item.get("published") or item.get("published-print")
                 or item.get("published-online"))
    published_date = None
    if published and published.get("date-parts"):
        parts = published["date-parts"][0]
        if len(parts) >= 1:
            year = parts[0]
            month = parts[1] if len(parts) >= 2 else 1
            day = parts[2] if len(parts) >= 3 else 1
            try:
                published_date = f"{year:04d}-{month:02d}-{day:02d}"
            except (ValueError, TypeError):
                pass

    if not published_date:
        return None

    pdf_url = None
    for link in item.get("link", []) or []:
        if link.get("content-type") == "application/pdf":
            pdf_url = link.get("URL")
            break

    return {
        "title": title,
        "abstract": abstract or None,
        "authors": authors,
        "source": journal_name or None,
        "doi": doi,
        "url": f"https://doi.org/{doi}",
        "pdf_url": pdf_url,
        "published_date": published_date,
        "published_year": int(published_date[:4]),
        **biblio_from_crossref(item),
    }


def normalize_arxiv(entry: dict, versionless: bool = False) -> dict | None:
    """An arXiv API entry in our paper format. `versionless=True` strips the vN suffix from the
    stored id: only the crawler's own arXiv ingest carries suffixes, and a drain-written row
    must not become a second row for the version a later crawl brings in (design item 4 (iii))."""
    title = entry.get("title")
    if not title:
        return None

    title = title.replace("\n", " ").strip()

    arxiv_id = None
    if entry.get("id"):
        arxiv_id = entry["id"].split("/abs/")[-1]
    if arxiv_id and versionless:
        arxiv_id = ARXIV_VERSION.sub("", arxiv_id)

    authors = []
    for author in entry.get("authors", []):
        data = {"name": author.get("name", "")}
        affiliation = author.get("arxiv_affiliation")
        if affiliation:
            data["affiliation"] = affiliation
        authors.append(data)

    if not is_valid_paper(title, None, authors):
        return None

    pdf_url = None
    for link in entry.get("links", []):
        if link.get("type") == "application/pdf":
            pdf_url = link.get("href")

    arxiv_categories = [t["term"] for t in entry.get("tags", []) if t.get("term")]

    primary = entry.get("arxiv_primary_category") or {}
    journal_ref = entry.get("arxiv_journal_ref")
    if journal_ref:
        journal_ref = journal_ref.replace("\n", " ").strip()
    comment = entry.get("arxiv_comment")
    if comment:
        comment = comment.replace("\n", " ").strip()

    published = entry.get("published")
    return {
        "title": title,
        "abstract": (entry.get("summary") or "").replace("\n", " ").strip() or None,
        "authors": authors,
        "source": "arXiv",
        "published_date": published,
        "published_year": int(published[:4]) if published and published[:4].isdigit() else None,
        "updated_date": entry.get("updated"),
        "url": entry.get("link"),
        "pdf_url": pdf_url,
        "arxiv_id": arxiv_id,
        "doi": entry.get("arxiv_doi"),
        "arxiv_categories": arxiv_categories,
        "arxiv_primary_category": primary.get("term"),
        "arxiv_comment": comment,
        "journal_ref": journal_ref,
    }


_NORMALIZERS = {
    "openalex": lambda r, **kw: normalize_openalex(r),
    "crossref": lambda r, **kw: normalize_crossref(r),
    "arxiv": lambda r, **kw: normalize_arxiv(r, versionless=kw.get("versionless", False)),
}


def normalize(record: dict | None, source: str, *, versionless: bool = False) -> dict | None:
    """The `papers` dict `upsert_paper()` takes, or None when the record is not a paper."""
    if not record or source not in _NORMALIZERS:
        return None
    return _NORMALIZERS[source](record, versionless=versionless)


# ── the accept test ──────────────────────────────────────────────────────────

def would_ingest(record: dict | None, source: str) -> tuple[bool, str | None]:
    """Would the crawl have taken this work? Returns (True, None) or (False, reason).

    Reasons, all machine-readable and safe to show a user:
      no_record              nothing resolved
      unknown_source:<s>     a source this file does not normalize
      not_article:<type>     a type that is not a paper (the card says "Not a paper (<type>)"):
                             a Crossref type outside CROSSREF_ACCEPT, or an OpenAlex type named
                             in OPENALEX_REJECT
      not_a_paper            the record is of an accepted kind but fails is_valid_paper
    """
    if not record:
        return False, "no_record"
    if source not in SOURCES:
        return False, f"unknown_source:{source}"

    if source == "crossref":
        ctype = record.get("type") or ""
        if ctype not in CROSSREF_ACCEPT:
            return False, f"not_article:{ctype}"

    if source == "openalex":
        otype = record.get("type") or ""
        if otype in OPENALEX_REJECT:
            return False, f"not_article:{otype}"

    return (True, None) if normalize(record, source) is not None else (False, "not_a_paper")


def source_discipline(paper: dict | None) -> tuple[str | None, str | None]:
    """(discipline slug, the config's journal name) when this paper's venue is one of the
    crawled journals, else (None, None). Matched on the OpenAlex source id first — the key
    `ingest_journals.py` uses — then on an exact (case-folded) journal name, which is how a
    Crossref record or an arXiv journal_ref names the same venue."""
    if not paper:
        return None, None
    sid = paper.get("source_openalex_id")
    if sid and sid in _SOURCE_INDEX:
        return _SOURCE_INDEX[sid]
    name = (paper.get("source") or "").strip().lower()
    if name and name in _NAME_INDEX:
        return _NAME_INDEX[name]
    return None, None
