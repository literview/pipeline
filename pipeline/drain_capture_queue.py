"""drain_capture_queue.py — turn a captured paper into a corpus paper.

Charter charters/2026-09-08-ext-we3-capture-charter.md decision D-W3-6 created this script as a
LINK-ONLY drain: a capture that missed the corpus left an EXTERNAL library item
(`paper_id IS NULL`, the scraped record in `ext_meta`) plus one `library_capture_queue` row, and
each run only re-ran the lookup to see whether a later crawl had brought the paper in. Owner
decision O1 (docs/CAPTURE_INGEST_VETTING_DESIGN.md section 4, 2026-09-17) supersedes D-W3-6's
"never writes to `papers`" and its test T3: this script now INGESTS the work as well, and it is
the only writer that does so for a captured paper. Design: docs/ledger/capture-drain-ingest.md
item 4.

Why here and not in the web app: `papers` keeps one normalization and one writer. The capture
route resolves the work seconds after the click (`lib/library/resolve-work.ts`) and stores the
raw record on the queue row; this script normalizes it with the same functions the journal crawl
uses (`ingest_work.py`) and inserts through the same `upsert_paper()`. A TypeScript writer would
be a second normalization, and would fire the author and journal triggers with scraped strings.

Invariants:
  * `origin`, `vetted` and `corpus_tier` are written ON INSERT ONLY. A new row is
    `corpus_tier = 'core'` when `source_discipline()` resolves its venue to a configured journal
    and `'staged'` otherwise (migration 067, docs/ledger/lp3-capture-proof.md): a staged paper has
    no page, is in no list or corpus-wide computation, and touches neither the author resolver nor
    `journals.paper_count` until `promote_captured.py` promotes it. When `upsert_paper()` MATCHES
    an existing row the drain links the item and changes nothing on the paper — it never lowers
    `vetted`, never rewrites `origin`, and never changes `corpus_tier` (a staged row hit again
    stays staged; only promote_captured.py promotes it). Writing `'staged'` needs 067's CHECK, so
    this file must not run against a database without 067.
  * Every version bump goes through the LOCKED COUNTER — `merge_papers.BUMP_SQL`, the statement
    `lib/library/version.ts` uses — taken BEFORE any row of that user's data is touched (the same
    lock order as the app and the corpus merge). A bump that bypasses the counter can be ordered
    below a concurrent web push, and every client's sync cursor then skips the drain's write.
  * One transaction per queue row, claimed with `FOR UPDATE SKIP LOCKED` so two runs (the
    30-minute schedule and a per-capture repository_dispatch) never take the same row. Inside it,
    `pg_advisory_xact_lock` on the identifier makes the second capture of one paper wait for the
    first's COMMIT instead of inserting a duplicate.
  * Linking is merge-aware and goes through the fold `merge_papers` already has
    (`_fold_library_rows`). Setting `paper_id` on the external row while the user already holds a
    row for that paper would violate UNIQUE (user_id, paper_id); instead the external row is
    folded into that row and tombstoned with `merged_into` naming it, which the sync client's
    AD-12 redirect map consumes to rewrite pending ops onto the winner.
  * Logs are public (the workflow runs in a public repository): they carry queue ids,
    DOIs, arXiv ids and paper ids only — never a user id, never a row, never `resolved_work`.

Status lifecycle (047): pending -> linked (re-point, ext_meta := NULL) | merged (fold +
tombstone) | closed (the item was tombstoned first, OR the work is not an article) | failed (14
days without a resolution). `linked` covers both tiers:
an ingested paper in an unconfigured venue is linked to the item as a staged paper, which the
library shows as a corpus item with no page (paper-meta `public` is false).

Resolution state, on the item's `ext_meta.resolution` and shown on the card (design item 7):
resolving -> not_indexed (nothing resolved yet; OpenAlex indexes new works 2-6 days after
publication) | incomplete (with the reason; `not_article:*` renders as "Not a paper") | resolved.

Import rows (ledger task lp3-import-enqueue, design docs/library/LP3_SWITCH_DESIGN_v2.md 5.7). The
import commit and pipeline/backfill_import_queue.py enqueue imported external items with
`scraped.origin = 'import'`; the capture route writes `origin = 'capture'`. An import row is resolved,
ingested, linked or folded exactly as a capture row, with two differences:
  * Claim order (`CLAIM_ORDER`): every claimable capture row before any import row, and import rows
    in `created_at` order, across users too (first in, first out: a second user's import waits
    behind a first user's whole backlog; round-robin by user is the known follow-up).
  * Logs: an import row prints no per-row line, so a public run never lists a person's library in
    submission order. A run prints the count of import rows per outcome.
  * Time budget: once a run has spent IMPORT_BUDGET_S seconds it claims no more import rows
    (capture rows are still claimed up to the row limit), so a large import keeps each run well
    inside the workflow's 20-minute timeout and a capture's dispatch never waits long behind it.
    The run then logs how many import rows are left for the next run.

A row whose processing raises (any row): its open transaction is rolled back, but what the row had
already committed stays (upsert_paper() commits the connection, so an ingested paper and the stored
`resolved_work` can already be in). The attempt is then recorded in its own transaction:
`attempts + 1`, `last_attempt_at = now()`, `last_error` = the exception's class name (never its
message, which can carry a DOI). The row stays pending, so the claim predicate's backoff skips it on
the next runs, until it is older than FAILED_AFTER_DAYS: then it becomes `failed` and its card
`incomplete` (not_indexed), as the expiry of a row that resolves nothing does. The run goes on with
the next row up to its limit. The log line names the queue id and the exception class only. The
process exits non-zero when a row raised on its first attempt (`attempts` was 0), so the workflow's
failure mail fires once per bad row, and when the run could not claim a row or record an attempt.

  python pipeline/drain_capture_queue.py [--dry-run] [--limit N] [--dsn URL] [--no-network]

`--dsn` defaults to DATABASE_URL from the environment (.env.local is loaded, never overriding).
`--dry-run` performs the lookups and prints each row's decision without writing.
`--no-network` skips the resolution arm (rows with no `resolved_work` are left pending) — for a
rehearsal that must not call OpenAlex.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
load_dotenv(ROOT / ".env.local", override=False)

from db import _match_existing as db_match, upsert_paper  # noqa: E402
from ingest_work import normalize, source_discipline, would_ingest  # noqa: E402
from merge_papers import BUMP_SQL, _fold_library_rows, has_column, has_table  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")   # Windows cp1252 console
except Exception:
    pass

# The row shape `_fold_library_rows` takes (merge_papers `cols`, verbatim).
FOLD_COLS = "id, deleted_at IS NULL, starred, read_status, memo, added_at, ext_meta"

# Both sides of the arXiv comparison are version-stripped (047: the crawler stores
# papers.arxiv_id version-suffixed, e.g. 2608.10993v1; the drain stores it versionless;
# idx_papers_arxiv_id_base backs the papers side).
ARXIV_VERSION = re.compile(r"v[0-9]+$")

OUTCOMES = ("pending", "linked", "merged", "closed", "failed")

DEFAULT_LIMIT = 200
FAILED_AFTER_DAYS = 14
HTTP_TIMEOUT = 3.0

# Exponential backoff on a row that resolved nothing: 30 minutes, then an hour, two, four …
# capped at a day. `attempts` is only incremented by the no-record arm, so a row that is simply
# waiting for OpenAlex to index the work is retried on a widening schedule rather than every run.
CLAIM_PREDICATE = ("status = 'pending' AND (last_attempt_at IS NULL OR last_attempt_at < "
                   "now() - least(interval '24 hours', interval '30 minutes' * power(2, attempts)))")

# An import row's `scraped.origin`; capture rows carry 'capture', rows written before
# lp3-import-enqueue carry none (and are capture rows).
IMPORT_ORIGIN = "coalesce(scraped->>'origin', '') = 'import'"

# Every claimable capture row before any import row (false sorts before true), then submission order.
CLAIM_ORDER = "(coalesce(scraped->>'origin', '') = 'import'), created_at"

_CLAIM = (f"SELECT id, library_item_id, user_id, doi, arxiv_id, resolved_work, resolved_source, "
          f"attempts, created_at < now() - interval '{FAILED_AFTER_DAYS} days' AS expired, "
          f"{IMPORT_ORIGIN} AS is_import "
          f"FROM library_capture_queue WHERE {CLAIM_PREDICATE} AND id <> ALL(%s::uuid[]){{only}} "
          f"ORDER BY {CLAIM_ORDER} FOR UPDATE SKIP LOCKED LIMIT 1")
CLAIM_SQL = _CLAIM.format(only="")
# After the import budget is spent: capture rows only.
CLAIM_CAPTURES_SQL = _CLAIM.format(only=f" AND NOT ({IMPORT_ORIGIN})")

# Seconds a run may spend before it stops claiming import rows (capture rows are still claimed up
# to the row limit). drain.yml times a run out at 20 minutes.
IMPORT_BUDGET_S = 240
_clock = time.monotonic   # the run's clock; a test replaces it


def _quiet(*_args, **_kwargs) -> None:
    """The per-row logger of an import row: prints nothing (see the module docstring)."""


def strip_arxiv_version(arxiv_id: str | None) -> str | None:
    return ARXIV_VERSION.sub("", arxiv_id) if arxiv_id else None


def advisory_key(doi: str | None, arxiv_base: str | None) -> str | None:
    """The string hashed into the per-paper transaction lock. One key per paper identity, so two
    captures of the same work serialize and the second sees the first's insert."""
    if doi:
        return "doi:" + doi.strip().lower()
    if arxiv_base:
        return "arxiv:" + arxiv_base
    return None


# ── resolution (the same chain the capture route's after() runs) ─────────────

def resolve_work(doi: str | None, arxiv_base: str | None) -> tuple[dict | None, str | None]:
    """OpenAlex by DOI (or by arXiv's own 10.48550 DOI), then Crossref by DOI (skipped for an
    arXiv identifier, which Crossref does not hold), then the arXiv API. Returns (record,
    source) or (None, None). Mirrors lib/library/resolve-work.ts; the route usually got there
    first and this arm only runs when its chain was killed or never ran."""
    import httpx

    lookup_doi = doi or (f"10.48550/arXiv.{arxiv_base}" if arxiv_base else None)
    mail = os.environ.get("OPENALEX_EMAIL", "")

    if lookup_doi:
        try:
            params = {"mailto": mail} if mail else {}
            r = httpx.get(f"https://api.openalex.org/works/doi:{lookup_doi}",
                          params=params, timeout=HTTP_TIMEOUT, follow_redirects=True)
            if r.status_code == 200:
                return r.json(), "openalex"
        except Exception:
            pass

    if doi and not doi.lower().startswith("10.48550/arxiv."):
        try:
            r = httpx.get(f"https://api.crossref.org/works/{doi}", timeout=HTTP_TIMEOUT,
                          headers={"User-Agent": f"LiterView/1.0 (mailto:{mail})" if mail else "LiterView/1.0"},
                          follow_redirects=True)
            if r.status_code == 200:
                msg = (r.json() or {}).get("message")
                if isinstance(msg, dict):
                    return msg, "crossref"
        except Exception:
            pass

    if arxiv_base:
        try:
            r = httpx.get("https://export.arxiv.org/api/query",
                          params={"id_list": arxiv_base, "max_results": 1}, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                entry = parse_arxiv_atom(r.text)
                if entry:
                    return entry, "arxiv"
        except Exception:
            pass

    return None, None


def parse_arxiv_atom(xml: str) -> dict | None:
    """One Atom `<entry>` as the record `ingest_work.normalize_arxiv()` reads — the same shape
    `lib/library/resolve-work.ts parseArxivAtom` produces, so a record stored by either path
    normalizes identically (pinned by pipeline/fixtures/arxiv_1706.03762.*)."""
    import xml.etree.ElementTree as ET  # noqa: N813  (stdlib, imported lazily)

    atom, arx = "{http://www.w3.org/2005/Atom}", "{http://arxiv.org/schemas/atom}"
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    e = root.find(atom + "entry")
    if e is None:
        return None

    def text(tag, parent=e):
        n = parent.find(tag)
        return n.text.strip() if n is not None and n.text and n.text.strip() else None

    authors = []
    for a in e.findall(atom + "author"):
        d = {"name": text(atom + "name", a) or ""}
        aff = text(arx + "affiliation", a)
        if aff:
            d["arxiv_affiliation"] = aff
        authors.append(d)

    links = []
    for l in e.findall(atom + "link"):
        d = {"href": l.get("href")}
        for k in ("type", "title", "rel"):
            if l.get(k) is not None:
                d[k] = l.get(k)
        links.append(d)

    pc = e.find(arx + "primary_category")
    out = {
        "id": text(atom + "id"),
        "title": text(atom + "title"),
        "summary": text(atom + "summary"),
        "published": text(atom + "published"),
        "updated": text(atom + "updated"),
        "authors": authors,
        "links": links,
        "link": next((l["href"] for l in links if l.get("rel") == "alternate"), None),
        "tags": [{"term": c.get("term")} for c in e.findall(atom + "category")],
        "arxiv_primary_category": {"term": pc.get("term")} if pc is not None else None,
        "arxiv_doi": text(arx + "doi"),
        "arxiv_journal_ref": text(arx + "journal_ref"),
        "arxiv_comment": text(arx + "comment"),
    }
    return {k: v for k, v in out.items() if v is not None}


# ── corpus lookup ────────────────────────────────────────────────────────────

def lookup_paper(cur, doi: str | None, arxiv_id: str | None,
                 openalex_id: str | None = None) -> str | None:
    """The capture route's lookup, plus the resolved record's OpenAlex id: papers by DOI, by
    version-stripped arXiv id and by openalex_id (a DOI variant can miss the capture lookup and
    still be a corpus paper), then the redirect table by DOI and arXiv id (newest redirect wins).
    An arm whose key is NULL is skipped. Returns the `papers.id` hit, or None."""
    base = strip_arxiv_version(arxiv_id)
    if doi:
        cur.execute("SELECT id FROM papers WHERE doi IS NOT NULL "
                    "AND normalize_doi(doi) = normalize_doi(%s)", [doi])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if base:
        cur.execute("SELECT id FROM papers WHERE arxiv_id IS NOT NULL "
                    "AND regexp_replace(arxiv_id, 'v[0-9]+$', '') = %s "
                    "ORDER BY coalesce(substring(arxiv_id from 'v([0-9]+)$')::int, 0) DESC, id "
                    "LIMIT 1", [base])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if openalex_id:
        cur.execute("SELECT id FROM papers WHERE openalex_id = %s", [openalex_id])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if doi:
        cur.execute("SELECT new_id FROM paper_redirects WHERE old_doi IS NOT NULL "
                    "AND normalize_doi(old_doi) = normalize_doi(%s) "
                    "ORDER BY merged_at DESC LIMIT 1", [doi])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if base:
        cur.execute("SELECT new_id FROM paper_redirects WHERE old_arxiv_id IS NOT NULL "
                    "AND regexp_replace(old_arxiv_id, 'v[0-9]+$', '') = %s "
                    "ORDER BY merged_at DESC LIMIT 1", [base])
        row = cur.fetchone()
        if row:
            return str(row[0])
    return None


def record_openalex_id(record: dict | None, source: str | None) -> str | None:
    if source == "openalex" and isinstance(record, dict):
        oid = record.get("id")
        return oid if isinstance(oid, str) and oid.startswith("https://openalex.org/") else None
    return None


# ── item writes ──────────────────────────────────────────────────────────────

def _resolve(cur, qid: str, status: str, *, error: str | None = None) -> None:
    cur.execute("UPDATE library_capture_queue SET status = %s, resolved_at = now(), "
                "last_error = COALESCE(%s, last_error) WHERE id = %s", [status, error, qid])


# Two jsonb_set traps, both caught by rehearse_051.py before this ever ran on production.
# (1) `create_missing` creates only the LAST key of a path, so setting '{resolution,state}' on a
# row whose ext_meta has no `resolution` object is a silent no-op. The innermost call therefore
# materializes `resolution` first — which every queue row written before this feature shipped
# needs, since only the new capture route seeds it.
# (2) to_jsonb(NULL::text) is SQL NULL, and jsonb_set with a NULL new_value returns NULL for the
# WHOLE document: one absent source would blank the item's ext_meta. Hence the coalesce to the
# JSON null literal.
RESOLUTION_SQL = """UPDATE library_items
   SET ext_meta = jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        jsonb_set(coalesce(ext_meta, '{}'::jsonb), '{resolution}',
                                  coalesce(ext_meta -> 'resolution', '{}'::jsonb), true),
                        '{resolution,state}', coalesce(to_jsonb(%s::text), 'null'::jsonb), true),
                      '{resolution,source}', coalesce(to_jsonb(%s::text), 'null'::jsonb), true),
                    '{resolution,last_attempt_at}', coalesce(to_jsonb(%s::text), 'null'::jsonb), true),
       version = %s, updated_at = now()
 WHERE id = %s AND paper_id IS NULL"""


def set_resolution(cur, item_id: str, version: int, state: str, *,
                   source: str | None = None, last_attempt_at: str | None = None) -> None:
    """One jsonb_set per key, so a concurrent client edit of another ext_meta key survives.
    Only ever applied to an item that is still external: once it is linked, ext_meta is NULL and
    the corpus row carries the metadata."""
    cur.execute(RESOLUTION_SQL, [state, source, last_attempt_at, version, item_id])


CARD_SQL = """UPDATE library_items
   SET ext_meta = jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        jsonb_set(coalesce(ext_meta, '{}'::jsonb), '{title}',
                                  coalesce(to_jsonb(%s::text), 'null'::jsonb), true),
                      '{authors}', %s::jsonb, true),
                    '{year}', %s::jsonb, true),
                  '{journal}', coalesce(to_jsonb(%s::text), 'null'::jsonb), true),
       version = %s, updated_at = now()
 WHERE id = %s AND paper_id IS NULL"""


def set_card(cur, item_id: str, version: int, paper: dict) -> None:
    """Re-apply the capture route's after() write, idempotently: the same four card keys, from
    the same record. A chain killed between its two writes leaves the queue row resolved and the
    card stale; this closes that gap on the next run."""
    authors = [a.get("name") for a in (paper.get("authors") or []) if a.get("name")]
    year = paper.get("published_year")
    if year is None and paper.get("published_date"):
        head = str(paper["published_date"])[:4]
        year = int(head) if head.isdigit() else None
    cur.execute(CARD_SQL, [paper.get("title"), json.dumps(authors), json.dumps(year),
                           paper.get("source"), version, item_id])


def link_item(cur, qid: str, item_id: str, user: str, paper: str, *,
              collections: bool, merged_into: bool, say=print) -> str:
    """Point the user's external row at `paper`, folding it into the row they already hold when
    there is one. BUMP FIRST (lock order). Returns 'linked' or 'merged'."""
    cur.execute(BUMP_SQL, [user])
    v = cur.fetchone()[0]
    cur.execute(f"SELECT {FOLD_COLS} FROM library_items WHERE user_id = %s AND paper_id = %s",
                [user, paper])
    w_row = cur.fetchone()   # UNIQUE (user_id, paper_id) => at most one
    cur.execute(f"SELECT {FOLD_COLS} FROM library_items WHERE id = %s", [item_id])
    l_row = cur.fetchone()
    if w_row is not None and str(w_row[0]) == str(item_id):
        # The item already points at the paper: a second drain linked it after this one's
        # upsert_paper() committed mid-row and released the claim. Folding it into itself would
        # delete its reading_state and memberships and tombstone it, so it is only recorded.
        say(f"  [linked]  queue={qid} paper={paper} v={v} (already linked)")
        _resolve(cur, qid, "linked")
        return "linked"

    stats = _fold_library_rows(cur, user, l_row, w_row, paper, v,
                               collections=collections, merged_into=merged_into)
    if w_row is None:
        say(f"  [linked]  queue={qid} paper={paper} v={v}")
        _resolve(cur, qid, "linked")
        return "linked"
    touched = {k: n for k, n in stats.items() if n}
    say(f"  [merged]  queue={qid} paper={paper} into={w_row[0]} v={v} {touched}")
    _resolve(cur, qid, "merged")
    return "merged"


# ── ingest ───────────────────────────────────────────────────────────────────

def link_disciplines(cur, paper_id: str, slug: str) -> int:
    cur.execute("SELECT id FROM disciplines WHERE slug = %s", [slug])
    row = cur.fetchone()
    if not row:
        return 0
    cur.execute("INSERT INTO paper_disciplines (paper_id, discipline_id, source) "
                "VALUES (%s, %s, 'journal') ON CONFLICT (paper_id, discipline_id) DO NOTHING",
                [paper_id, row[0]])
    return cur.rowcount


def trigger_revalidation(paper_id: str, *, from_import: bool = False) -> None:
    """Bust the paper page's cached 404. `notFound()` for an id that did not exist yet may be
    cached, so a newly-published paper needs its own path busted; the feed routes are busted by
    the crawl's own call at the end of each cycle. Best effort — never fails the row. A paper an
    import row inserted is not named in the warning."""
    base = os.environ.get("SITE_URL") or os.environ.get("NEXT_PUBLIC_SITE_URL")
    secret = os.environ.get("CRON_SECRET")
    if not base or not secret:
        return
    try:
        import httpx
        headers = {"Authorization": f"Bearer {secret}"}
        bypass = os.environ.get("VERCEL_PROTECTION_BYPASS")
        if bypass:
            headers["x-vercel-protection-bypass"] = bypass
        httpx.post(base.rstrip("/") + "/api/revalidate", headers=headers,
                   json={"paths": [f"/paper/{paper_id}"]}, timeout=10)
    except Exception as e:  # noqa: BLE001
        who = "an import row's paper" if from_import else f"paper={paper_id}"
        print(f"  [warn]    revalidate failed for {who}: {e.__class__.__name__}")


def ingest_record(conn, record: dict, source: str) -> tuple[str | None, bool, str | None, dict]:
    """(paper_id, inserted, discipline_slug, the normalized paper). The record has already
    passed would_ingest(). `origin`, `vetted` and `corpus_tier` ride in the dict only when no row
    matches, so they are written by the INSERT branch of upsert_paper() and by nothing else: on a
    match the UPDATE branch rewrites only the keys present here, and a matched row's own
    origin/vetted/corpus_tier are not among them. `corpus_tier` is 'core' when the venue resolves
    to a configured journal (a discipline slug) and 'staged' otherwise (migration 067)."""
    paper = normalize(record, source, versionless=True)
    if paper is None:
        return None, False, None, {}
    slug, config_name = source_discipline(paper)
    if config_name:
        paper["source"] = config_name
    card = dict(paper)
    paper.pop("source_openalex_id", None)   # a normalizer-only key, not a `papers` column

    existing = db_match(conn, paper)
    if existing is None:
        paper["origin"] = "user-library"
        paper["vetted"] = True
        paper["corpus_tier"] = "core" if slug else "staged"
    paper_id, inserted = upsert_paper(conn, paper)
    return (str(paper_id) if paper_id else None), inserted, slug, card


# ── one row ──────────────────────────────────────────────────────────────────

def process_row(conn, *, collections: bool, merged_into: bool, dry_run: bool,
                network: bool, seen: list[str], import_ids: set[str] | None = None,
                captures_only: bool = False) -> tuple[str | None, str | None]:
    """Claim and process ONE queue row in ONE transaction. Returns (queue id, outcome); the
    queue id is None when no row was claimable, which ends the run. Raises on any error; the
    caller owns the rollback and the `failed` stamp. An import row's id is added to `import_ids`
    as soon as it is claimed, and its per-row lines are not printed."""
    cur = conn.cursor()
    cur.execute(CLAIM_CAPTURES_SQL if captures_only else CLAIM_SQL, [seen])
    row = cur.fetchone()
    if row is None:
        return None, None
    (qid, item_id, user, doi, arxiv_id, resolved_work, resolved_source, attempts, expired,
     is_import) = row
    qid, item_id, user = str(qid), str(item_id), str(user)
    seen.append(qid)
    if is_import and import_ids is not None:
        import_ids.add(qid)
    say = _quiet if is_import else print
    base = strip_arxiv_version(arxiv_id)

    cur.execute("SELECT id, deleted_at, paper_id FROM library_items WHERE id = %s", [item_id])
    item = cur.fetchone()
    if item is None:
        say(f"  [skip]    queue={qid} item gone")
        return qid, None
    if item[1] is not None:
        say(f"  [closed]  queue={qid} (item tombstoned before the drain ran)")
        if not dry_run:
            _resolve(cur, qid, "closed")
        return qid, "closed"
    if item[2] is not None:
        say(f"  [linked]  queue={qid} paper={item[2]} (already linked)")
        if not dry_run:
            _resolve(cur, qid, "linked")
        return qid, "linked"

    # The per-paper lock, BEFORE the corpus lookup: the second capture of one work then sees the
    # first transaction's insert instead of racing it to a duplicate row.
    key = advisory_key(doi, base)
    if key and not dry_run:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", [key])

    # (i) already in the corpus — link and change NOTHING on the paper row
    oa_id = record_openalex_id(resolved_work, resolved_source)
    paper = lookup_paper(cur, doi, arxiv_id, oa_id)
    if paper is not None:
        if dry_run:
            cur.execute("SELECT id FROM library_items WHERE user_id = %s AND paper_id = %s",
                        [user, paper])
            outcome = "linked" if cur.fetchone() is None else "merged"
            say(f"  [{outcome}]  queue={qid} paper={paper}")
            return qid, outcome
        return qid, link_item(cur, qid, item_id, user, paper,
                              collections=collections, merged_into=merged_into, say=say)

    # (ii) a miss with no stored record — resolve it here (the route's chain was killed, or no
    # route ran at all, as for a row created before this feature shipped)
    if resolved_work is None:
        if not network:
            say(f"  [pending] queue={qid} doi={doi!r} arxiv={base!r} (no record; --no-network)")
            return qid, "pending"
        resolved_work, resolved_source = resolve_work(doi, base)
        if resolved_work is not None and not dry_run:
            cur.execute("UPDATE library_capture_queue SET resolved_work = %s::jsonb, "
                        "resolved_source = %s WHERE id = %s",
                        [json.dumps(resolved_work), resolved_source, qid])

    # (iv) nothing resolved anywhere
    if resolved_work is None:
        if dry_run:
            say(f"  [pending] queue={qid} doi={doi!r} arxiv={base!r} (no record)")
            return qid, "pending"
        cur.execute(BUMP_SQL, [user])
        v = cur.fetchone()[0]
        cur.execute("UPDATE library_capture_queue SET attempts = attempts + 1, "
                    "last_attempt_at = now() WHERE id = %s RETURNING to_char(last_attempt_at, "
                    "'YYYY-MM-DD\"T\"HH24:MI:SSOF')", [qid])
        stamp = cur.fetchone()[0]
        if expired:
            set_resolution(cur, item_id, v, "incomplete", source="not_indexed",
                           last_attempt_at=stamp)
            _resolve(cur, qid, "failed", error=f"unresolved after {FAILED_AFTER_DAYS} days")
            say(f"  [failed]  queue={qid} doi={doi!r} arxiv={base!r} "
                f"(unresolved after {FAILED_AFTER_DAYS} days)")
            return qid, "failed"
        set_resolution(cur, item_id, v, "not_indexed", last_attempt_at=stamp)
        say(f"  [pending] queue={qid} doi={doi!r} arxiv={base!r} attempts={attempts + 1}")
        return qid, "pending"

    # (iii) a record the crawl would not have taken
    ok, reason = would_ingest(resolved_work, resolved_source)
    if not ok:
        if dry_run:
            say(f"  [closed]  queue={qid} doi={doi!r} ({reason})")
            return qid, "closed"
        cur.execute(BUMP_SQL, [user])
        v = cur.fetchone()[0]
        set_resolution(cur, item_id, v, "incomplete", source=reason)
        _resolve(cur, qid, "closed", error=reason)
        say(f"  [closed]  queue={qid} doi={doi!r} ({reason})")
        return qid, "closed"

    # (iii) a record the crawl WOULD have taken — ingest it
    if dry_run:
        say(f"  [linked]  queue={qid} doi={doi!r} (would ingest from {resolved_source})")
        return qid, "linked"

    if is_import:
        # db.py's upsert can print a dedup warning naming the record's DOI and arXiv id
        with contextlib.redirect_stdout(io.StringIO()):
            paper_id, inserted, slug, card = ingest_record(conn, resolved_work, resolved_source)
    else:
        paper_id, inserted, slug, card = ingest_record(conn, resolved_work, resolved_source)
    if paper_id is None:
        cur.execute(BUMP_SQL, [user])
        v = cur.fetchone()[0]
        set_resolution(cur, item_id, v, "incomplete", source="not_a_paper")
        _resolve(cur, qid, "closed", error="not_a_paper")
        say(f"  [closed]  queue={qid} doi={doi!r} (not_a_paper on normalize)")
        return qid, "closed"

    linked_disc = 0
    if inserted and slug:
        linked_disc = link_disciplines(cur, paper_id, slug)
    cur.execute(BUMP_SQL, [user])
    v = cur.fetchone()[0]
    set_card(cur, item_id, v, card)
    set_resolution(cur, item_id, v, "resolved", source=resolved_source)
    outcome = link_item(cur, qid, item_id, user, paper_id,
                        collections=collections, merged_into=merged_into, say=say)
    say(f"  [ingest]  queue={qid} paper={paper_id} inserted={inserted} "
          f"source={resolved_source} discipline={slug or '-'}({linked_disc})")
    if inserted:
        _revalidate_after_commit.append((paper_id, bool(is_import)))
    return qid, outcome


_revalidate_after_commit: list[tuple[str, bool]] = []   # (paper id, inserted by an import row)


# ── the run ──────────────────────────────────────────────────────────────────

# A row whose processing raised: one more attempt, stamped now, so CLAIM_PREDICATE's backoff skips
# it; it stays pending until it is older than FAILED_AFTER_DAYS, then it is `failed` (resolved_at set,
# as the expiry path's _resolve does). `last_error` holds the exception's class name only. Returns the
# status, the attempts before this one, the item, the user and the stamp the card's resolution takes.
RECORD_ERROR_SQL = (
    "UPDATE library_capture_queue SET attempts = attempts + 1, last_attempt_at = now(), last_error = %s, "
    f"status = CASE WHEN created_at < now() - interval '{FAILED_AFTER_DAYS} days' THEN 'failed' ELSE status END, "
    f"resolved_at = CASE WHEN created_at < now() - interval '{FAILED_AFTER_DAYS} days' THEN now() ELSE resolved_at END "
    "WHERE id = %s RETURNING status, attempts - 1, library_item_id, user_id, "
    "to_char(last_attempt_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF')")


def record_error(conn, qid: str, name: str) -> int:
    """Record a raising row's attempt in its own transaction; returns the attempts before it. A row
    past FAILED_AFTER_DAYS becomes failed and its card incomplete (not_indexed), BUMP first."""
    cur = conn.cursor()
    cur.execute(RECORD_ERROR_SQL, [name, qid])
    status, before, item_id, user, stamp = cur.fetchone()
    if status == "failed":
        cur.execute(BUMP_SQL, [str(user)])
        v = cur.fetchone()[0]
        set_resolution(cur, str(item_id), v, "incomplete", source="not_indexed", last_attempt_at=stamp)
    conn.commit()
    return int(before)


def run(conn, *, dry_run: bool = False, limit: int | None = None,
        network: bool = True, outcomes: list | None = None) -> dict[str, int]:
    """Drain up to `limit` rows; returns the count per outcome over all rows, plus `raised` (the
    rows whose processing raised) and `alerts` (first failures, and a claim or record that failed;
    non-zero makes the process exit 1). When `outcomes` is a list, each claimed row's (queue id,
    outcome) is appended to it in claim order, with the outcome 'raised' for a row that raised."""
    cur = conn.cursor()
    # Capability probes, once per run. 045 and 051 are applied in production, so a missing
    # column is a mis-provisioned environment, not a supported state: abort before a row is
    # touched rather than half-draining.
    collections = has_table(cur, "public.library_collection_items")
    merged_into = has_column(cur, "library_items", "merged_into")
    have_051 = all(has_column(cur, "library_capture_queue", c)
                   for c in ("resolved_work", "resolved_source", "last_attempt_at"))
    conn.rollback()
    if not merged_into:
        raise SystemExit("[abort] library_items.merged_into is absent — migration 045 is applied "
                         "in production, so this environment is mis-provisioned; refusing to run")
    if not have_051:
        raise SystemExit("[abort] library_capture_queue is missing 051's resolved_work / "
                         "resolved_source / last_attempt_at; apply migration 051 first")

    cap = DEFAULT_LIMIT if limit is None else limit
    cur.execute(f"SELECT count(*) FROM library_capture_queue WHERE {CLAIM_PREDICATE}")
    print(f"[drain] {cur.fetchone()[0]} claimable queue row(s), limit {cap}"
          f"{' [dry-run]' if dry_run else ''}{'' if network else ' [no-network]'}")
    conn.rollback()

    counts = {k: 0 for k in OUTCOMES}
    import_counts = {k: 0 for k in OUTCOMES}
    raised = import_raised = alerts = 0
    import_ids: set[str] = set()
    seen: list[str] = []
    _revalidate_after_commit.clear()
    t0 = _clock()
    budget_spent = False
    for _ in range(cap):
        qid = None
        n_seen = len(seen)
        if not budget_spent and _clock() - t0 >= IMPORT_BUDGET_S:
            budget_spent = True
        try:
            qid, outcome = process_row(conn, collections=collections, merged_into=merged_into,
                                       dry_run=dry_run, network=network, seen=seen,
                                       import_ids=import_ids, captures_only=budget_spent)
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
            if qid is None:
                break
        except Exception as e:  # noqa: BLE001 — one row's failure must not stop the run
            conn.rollback()
            claimed = seen[-1] if len(seen) > n_seen else None
            name = e.__class__.__name__
            # The class name only: a message can carry a row's values (a constraint's DETAIL).
            print(f"  [error]   queue={claimed} {name}")
            raised += 1
            if claimed is None:
                alerts += 1
                break                    # nothing was claimed (the claim itself failed): stop
            if claimed in import_ids:
                import_raised += 1
            if outcomes is not None:
                outcomes.append((claimed, "raised"))
            try:
                if dry_run:
                    c2 = conn.cursor()
                    c2.execute("SELECT attempts FROM library_capture_queue WHERE id = %s", [claimed])
                    before = int(c2.fetchone()[0])
                    conn.rollback()
                else:
                    before = record_error(conn, claimed, name)
            except Exception as e2:  # noqa: BLE001
                conn.rollback()
                print(f"  [error]   queue={claimed} the attempt was not recorded: "
                      f"{e2.__class__.__name__}")
                alerts += 1
                break
            if before == 0:
                alerts += 1              # the row's first failure: the run exits non-zero
            continue
        if outcome is not None:
            counts[outcome] += 1
            if qid in import_ids:
                import_counts[outcome] += 1
            if outcomes is not None:
                outcomes.append((qid, outcome))

    for paper_id, from_import in _revalidate_after_commit:
        trigger_revalidation(paper_id, from_import=from_import)

    if budget_spent:
        cur = conn.cursor()
        cur.execute(f"SELECT count(*) FROM library_capture_queue WHERE {CLAIM_PREDICATE} AND {IMPORT_ORIGIN} "
                    "AND id <> ALL(%s::uuid[])", [seen])
        left = cur.fetchone()[0]
        conn.rollback()
        print(f"[drain] import budget of {IMPORT_BUDGET_S} s spent; {left} import row(s) left for the next run")

    if dry_run:
        print("[dry-run] no writes performed")
    print(" ".join(f"{k}={counts[k]}" for k in OUTCOMES) + f" raised={raised}")
    print("import rows: " + " ".join(f"{k}={import_counts[k]}" for k in OUTCOMES)
          + f" raised={import_raised}")
    return {**counts, "raised": raised, "alerts": alerts}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="resolve and ingest pending library_capture_queue rows")
    ap.add_argument("--dry-run", action="store_true", help="look up and print decisions; write nothing")
    ap.add_argument("--limit", type=int, help=f"process at most N rows (default {DEFAULT_LIMIT})")
    ap.add_argument("--dsn", help="database URL (default: DATABASE_URL from the environment)")
    ap.add_argument("--no-network", action="store_true",
                    help="do not resolve metadata; leave a row with no stored record pending")
    a = ap.parse_args(argv)

    dsn = a.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("[abort] no --dsn given and DATABASE_URL is not set")
        return 1
    try:
        conn = psycopg2.connect(dsn, connect_timeout=20)
    except psycopg2.Error as e:
        print(f"[abort] could not connect: {e.__class__.__name__}: {e}")
        return 1
    conn.autocommit = False
    try:
        counts = run(conn, dry_run=a.dry_run, limit=a.limit, network=not a.no_network)
    finally:
        conn.close()
    return 1 if counts["alerts"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
