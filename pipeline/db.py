"""
Database client for LiterView pipeline.
Uses psycopg2 to connect directly to Neon PostgreSQL.
"""
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

try:  # consumers that put pipeline/ on sys.path (every pipeline script)
    from author_names import merge_author_elements
    from text_norm import clean_title, decode_refs, normalize_text
except ImportError:  # consumers that import this as a package (classifier/manual_label/app.py)
    from pipeline.author_names import merge_author_elements
    from pipeline.text_norm import clean_title, decode_refs, normalize_text

load_dotenv()


# Enrichment TEXT columns that must never be clobbered back to empty/NULL by a
# weaker/later ingest source re-upserting a paper (e.g. abstract=None from
# OpenAlex overwriting an abstract already recovered by the recovery cron).
# These MUST be TEXT columns only -- the guard below uses NULLIF(%s, '').
# volume / issue / first_page / last_page (migration 068, ledger task lp3-biblio-columns) are here
# so a re-crawl whose record lacks them never blanks a value ingest or the backfill stored.
PRESERVE_IF_EMPTY = {"abstract", "pdf_url", "url", "volume", "issue", "first_page", "last_page"}


def get_connection():
    """Get a PostgreSQL connection."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Missing DATABASE_URL")
    return psycopg2.connect(url)


def get_client():
    """Get a database connection (backward-compatible name)."""
    return get_connection()


def execute(conn, sql, params=None):
    """Execute a query and return rows as list of dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        if cur.description:
            return [dict(row) for row in cur.fetchall()]
        return []


def execute_one(conn, sql, params=None):
    """Execute a query and return a single row dict, or None."""
    rows = execute(conn, sql, params)
    return rows[0] if rows else None


def execute_write(conn, sql, params=None):
    """Execute a write query (INSERT/UPDATE/DELETE) and commit. Rolls back on error."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if cur.description:
                rows = [dict(row) for row in cur.fetchall()]
            else:
                rows = []
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise


def _adapt(v):
    """Wrap dict / list-of-dict values with psycopg2 Json adapter for JSONB columns.
    Lists of scalars (e.g. arxiv categories -> TEXT[]) are left as-is so psycopg2
    uses its native array adaptation."""
    if isinstance(v, dict):
        return psycopg2.extras.Json(v)
    if isinstance(v, list) and v and isinstance(v[0], dict):
        return psycopg2.extras.Json(v)
    return v


def build_update_set_clause(cols: list[str]) -> str:
    """
    Build the comma-joined SET assignments for an UPDATE, one %s placeholder
    per column in `cols` order. Columns in PRESERVE_IF_EMPTY use
    COALESCE(NULLIF(%s, ''), col) so an incoming empty/NULL value does not
    clobber a previously-stored value; all other columns are a plain
    `col = %s` overwrite. Pure function -- no DB access, no side effects.
    """
    parts = []
    for c in cols:
        if c in PRESERVE_IF_EMPTY:
            parts.append(f"{c} = COALESCE(NULLIF(%s, ''), {c})")
        else:
            parts.append(f"{c} = %s")
    return ", ".join(parts)


def clean_doi(doi: str | None) -> str | None:
    """Strip a leading `doi:` scheme prefix (and surrounding whitespace) before a DOI is ever
    stored.

    This exists to keep ONE definition of DOI identity in the system. The DB's normalize_doi()
    (migration 020, and the UNIQUE functional index built on it) lowercases, trims, and strips a
    `https://doi.org/` prefix -- but NOT a `doi:` prefix. So `doi:10.1/x` and `10.1/x` are two
    DISTINCT rows to the UNIQUE index while being the same paper to any human and to the Python-side
    normalizer in deduplicate.py. Prod has zero such rows today; sanitizing here at the only write
    door means it stays that way, without having to rebuild a UNIQUE functional index.
    """
    if not doi:
        return None
    doi = doi.strip()
    if doi[:4].lower() == "doi:":
        doi = doi[4:].strip()
    return doi or None


def _redirect_winner(conn, paper: dict):
    """The live paper a merged-away incoming record now belongs to, or None.

    Called ONLY after all three `papers` lookups miss. `paper_redirects` preserves the loser's
    openalex_id / doi / arxiv_id (migrations 041 + 042) precisely so this probe can exist: without
    it, a merged-away paper is re-INSERTed as a fresh duplicate on the next multi-source re-ingest,
    and the dominant merge case (preprint <-> published) regenerates itself -- the redirect would
    still resolve, but a live duplicate would now shadow the winner in every feed.

    Each key is compared EXACTLY as the `papers` lookup above compares it (doi through
    normalize_doi() on both sides; the other two by equality), or the probe would miss precisely the
    messy-key rows that caused the duplicate in the first place.

    Precedence is openalex -> doi -> arxiv (the same order as the `papers` lookups), with merged_at
    DESC only as the tiebreaker WITHIN one key -- that is the same-key-merged-twice case. If two
    different keys of one incoming record resolve to two DIFFERENT winners, the corpus has a real
    identity conflict; take the highest-precedence match but say so loudly rather than swallowing it.
    """
    oa, doi, ax = paper.get("openalex_id"), paper.get("doi"), paper.get("arxiv_id")
    if not (oa or doi or ax):
        return None

    rows = execute(conn, """
        SELECT r.new_id,
               CASE WHEN %(oa)s IS NOT NULL AND r.old_openalex_id = %(oa)s THEN 1
                    WHEN %(doi)s IS NOT NULL AND r.old_doi IS NOT NULL
                         AND normalize_doi(r.old_doi) = normalize_doi(%(doi)s) THEN 2
                    ELSE 3 END AS match_priority
          FROM paper_redirects r
         WHERE (%(oa)s IS NOT NULL AND r.old_openalex_id = %(oa)s)
            OR (%(doi)s IS NOT NULL AND r.old_doi IS NOT NULL
                AND normalize_doi(r.old_doi) = normalize_doi(%(doi)s))
            OR (%(ax)s IS NOT NULL AND r.old_arxiv_id = %(ax)s)
         ORDER BY match_priority, r.merged_at DESC
    """, {"oa": oa, "doi": doi, "ax": ax})

    if not rows:
        return None

    winners = {str(r["new_id"]) for r in rows}
    if len(winners) > 1:
        print(f"[DB] WARNING: one incoming record's dedup keys resolve to {len(winners)} DIFFERENT "
              f"merge winners {sorted(winners)} — taking the highest-precedence match. "
              f"openalex_id={oa} doi={doi} arxiv_id={ax}")
    return rows[0]["new_id"]


def build_fill_empty_set_clause(cols: list[str]) -> str:
    """SET assignments that can only ever FILL A BLANK -- `col = COALESCE(NULLIF(col,''), NULLIF(%s,''))`.

    Note this is the MIRROR of build_update_set_clause(), not the same thing, and the difference is
    load-bearing. That one is `COALESCE(NULLIF(%s,''), col)`: the INCOMING value wins whenever it is
    non-empty, and the stored value survives only an empty incoming ("don't let a weaker source blank
    my abstract"). Here the STORED value wins whenever it is non-empty, and the incoming value is used
    only to fill a hole.

    That is what a merge winner needs. The incoming record is a paper the winner already ABSORBED, so
    its abstract/pdf_url must never overwrite the winner's own — but if the winner has none and the
    absorbed record does (an abstract recovered on the preprint side), taking it is pure gain.
    """
    return ", ".join(f"{c} = COALESCE(NULLIF({c}, ''), NULLIF(%s, ''))" for c in cols)


# The versionless arXiv lookup (ledger task capture-drain-ingest, design item 3). papers.arxiv_id
# is stored version-suffixed by the crawler and VERSIONLESS by the drain, so an equality match on
# the stored string would make `2409.01234` and `2409.01234v2` two rows for one paper. Both sides
# are stripped; idx_papers_arxiv_id_base (migration 047) is the index that backs it. 234 groups on
# production already hold more than one row (one per version); among them the HIGHEST version
# suffix wins, so an incoming record lands on the newest row rather than an arbitrary one. The
# duplicates themselves are reported by pipeline/rehearse_051.py, not merged here.
ARXIV_BASE_MATCH_SQL = """
    SELECT id FROM papers
     WHERE arxiv_id IS NOT NULL
       AND regexp_replace(arxiv_id, 'v[0-9]+$', '') = regexp_replace(%s, 'v[0-9]+$', '')
     ORDER BY coalesce(substring(arxiv_id from 'v([0-9]+)$')::int, 0) DESC, id
     LIMIT 1
"""


def _match_existing(conn, paper: dict):
    """The `papers` dedup lookups, in precedence order: openalex_id, doi, the versionless arXiv
    id, then the exact arXiv id. Returns the matched row dict, or None."""
    if paper.get("openalex_id"):
        row = execute_one(conn, "SELECT id FROM papers WHERE openalex_id = %s", [paper["openalex_id"]])
        if row:
            return row

    if paper.get("doi"):
        row = execute_one(
            conn,
            "SELECT id FROM papers WHERE normalize_doi(doi) = normalize_doi(%s)",
            [paper["doi"]],
        )
        if row:
            return row

    if paper.get("arxiv_id"):
        row = execute_one(conn, ARXIV_BASE_MATCH_SQL, [paper["arxiv_id"]])
        if row:
            return row
        row = execute_one(conn, "SELECT id FROM papers WHERE arxiv_id = %s", [paper["arxiv_id"]])
        if row:
            return row

    return None


def upsert_paper(conn, paper: dict) -> tuple[str | None, bool]:
    """
    Insert or update a paper. Returns (paper_id, inserted) -- `inserted` is True only when this
    call created the row, which is what the capture drain needs in order to link disciplines and
    revalidate for a NEW paper and to change nothing on a matched one.
    Deduplication is based on openalex_id, doi, or arxiv_id (the last both versionless and exact)
    -- checked against `papers` first, then against `paper_redirects` (a paper that was merged
    away must resolve to its winner, never be re-created).
    """

    # The one place well-formed HTML character references are decoded, and publisher
    # extraction defects repaired, on the write path
    # (docs/MARKUP_POLICY_WORKORDER.md T2). It lives here, not at the call sites, because
    # every ingest writer funnels through upsert_paper() and decode_refs() is NOT idempotent:
    # applying it twice turns "&amp;lt;" into "<" and destroys a genuinely double-escaped
    # string. Raw publisher markup is deliberately left alone; the render path interprets it.
    # Titles also get clean_title(): deposit whitespace and the book-review citation names
    # whose separators the publisher's markup flattening dropped (text_norm.py, name_split.py).
    if paper.get("title"):
        paper["title"] = clean_title(normalize_text(decode_refs(paper["title"])))
    if paper.get("abstract"):
        paper["abstract"] = normalize_text(decode_refs(paper["abstract"]))

    if paper.get("doi"):
        paper["doi"] = clean_doi(paper["doi"])

    existing = _match_existing(conn, paper)

    if not existing:
        winner = _redirect_winner(conn, paper)
        if winner:
            # A merge already decided that this record IS the winner. Do NOT run the normal column
            # update: it would write the LOSER's title / doi / openalex_id / published_date /
            # citation_count over the winner's, flipping the winner's URL identity and undoing the
            # merge decision.
            #
            # Only the PRESERVE_IF_EMPTY columns are touched, and through
            # build_fill_empty_set_clause() -- the MIRROR of the normal clause, so the winner's own
            # value always wins and the incoming one can only fill a hole. That keeps the single
            # genuinely useful flow alive (an abstract recovered on the preprint side reaching a
            # winner that has none) while making the update structurally non-destructive.
            fill = [c for c in paper if c in PRESERVE_IF_EMPTY]
            if fill:
                set_clause = build_fill_empty_set_clause(fill)
                execute_write(conn, f"UPDATE papers SET {set_clause} WHERE id = %s",
                              [_adapt(paper[c]) for c in fill] + [winner])
            return winner, False

    if existing:
        # Update existing paper
        if "authors" in paper:
            # A re-crawl must not strip author IDs a later pass wrote into the stored elements
            # (the arXiv crawler sends names only). merge_author_elements() copies openalex_id /
            # orcid / affiliation from the stored element only where the incoming element lacks
            # the key; when the result equals the stored list, the link trigger does not fire.
            # Not guarded against a write landing between this read and the UPDATE below
            # (docs/ledger/author-elements-merge-guard.md).
            stored = execute_one(conn, "SELECT authors FROM papers WHERE id = %s", [existing["id"]])
            paper["authors"] = merge_author_elements(stored["authors"] if stored else None,
                                                     paper["authors"])
        cols = [k for k in paper.keys() if k != "id"]
        if cols:
            set_clause = build_update_set_clause(cols)
            values = [_adapt(paper[c]) for c in cols] + [existing["id"]]
            execute_write(conn, f"UPDATE papers SET {set_clause} WHERE id = %s", values)
        return existing["id"], False
    else:
        # Insert new paper
        cols = list(paper.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        values = [_adapt(paper[c]) for c in cols]
        try:
            rows = execute_write(conn, f"INSERT INTO papers ({col_names}) VALUES ({placeholders}) RETURNING id", values)
        except psycopg2.errors.UniqueViolation:
            # A concurrent writer (a crawl racing the capture drain) inserted the same
            # openalex_id / doi / arxiv_id between the lookups above and this INSERT; each of
            # those keys carries a unique index. execute_write has already rolled back, so one
            # re-select answers who won. Any other integrity error is a real defect and is raised.
            raced = _match_existing(conn, paper)
            if raced:
                return raced["id"], False
            raise
        if rows:
            return rows[0]["id"], True

    return None, False


def resolve_paper_ids(conn, ids) -> dict[str, str | None]:
    """Map each snapshotted paper id to the live paper it now denotes.

    id -> itself      the paper is still there (the overwhelming majority)
    id -> winner id   the paper was merged away; `paper_redirects` says where it went
    id -> None        the paper is simply gone (deleted outright, never merged)

    THE STRADDLE PROBLEM. Classifier / HPC jobs snapshot paper UUIDs, spend minutes-to-days
    computing, and only then write those UUIDs back into FK'd tables (paper_tags, paper_subfields).
    A merge landing inside that window DELETEs the loser row, so the write raises a foreign-key
    violation and kills the whole execute_values chunk. Resolving the ids immediately before the
    write shrinks the exposure from days to milliseconds -- it does NOT eliminate it (a merge can
    still land inside that gap), but the residual failure stays loud and the batch is rerunnable.

    Chain compression in merge_papers.py guarantees `paper_redirects.new_id` always points at a LIVE
    paper, so one hop is always enough. One query regardless of batch size.
    """
    uniq = [str(i) for i in dict.fromkeys(ids) if i]
    if not uniq:
        return {}
    rows = execute(conn, """
        SELECT i.id::text AS old, COALESCE(p.id, r.new_id)::text AS live
          FROM unnest(%s::uuid[]) AS i(id)
          LEFT JOIN papers p          ON p.id     = i.id
          LEFT JOIN paper_redirects r ON r.old_id = i.id
    """, [uniq])
    return {r["old"]: r["live"] for r in rows}


def resolve_paper_rows(conn, rows: list[tuple], key: tuple[int, ...] | None = None):
    """Re-point a batch of about-to-be-written rows through resolve_paper_ids(). Element 0 of each
    row MUST be the paper_id. Returns (rows, stats).

    Rows whose paper is gone are DROPPED (there is nothing to attach them to). Rows whose paper was
    merged are re-pointed to the winner.

    `key` is the table's ON CONFLICT key as column indexes -- e.g. (0, 1) for
    (paper_id, tag_id). Passing it is REQUIRED wherever the write uses `ON CONFLICT ... DO UPDATE`:
    a batch that snapshotted BOTH the loser and the winner now maps two rows onto one key, and
    Postgres rejects a single INSERT that upserts the same key twice ("ON CONFLICT DO UPDATE command
    cannot affect row a second time"). DO NOTHING writes do not error, but pass `key` there too --
    the collapse rule below is a correctness choice, not just an error dodge.

    COLLAPSE RULE: the row whose paper_id was ALREADY the winner wins. Both rows carry a payload
    (a confidence, a label set) computed against a different paper's text; the winner's was computed
    against the text that survives, so it is the one that describes the row we are keeping. Ties (two
    moved rows, or two originals) break on first-seen, which is the caller's own batch order.
    """
    if not rows:
        return [], {"moved": 0, "dropped": 0, "collapsed": 0}

    mapping = resolve_paper_ids(conn, [r[0] for r in rows])
    resolved: list[tuple[tuple, bool]] = []      # (row, was_moved)
    moved = dropped = 0
    for r in rows:
        live = mapping.get(str(r[0]))
        if live is None:
            dropped += 1
            continue
        was_moved = str(live) != str(r[0])
        if was_moved:
            moved += 1
            r = (live,) + tuple(r[1:])
        resolved.append((tuple(r), was_moved))

    collapsed = 0
    if key:
        best: dict[tuple, tuple[tuple, bool]] = {}
        for row, was_moved in resolved:
            k = tuple(str(row[i]) for i in key)
            incumbent = best.get(k)
            if incumbent is None:
                best[k] = (row, was_moved)
            else:
                collapsed += 1
                # an original beats a moved row; otherwise first-seen stays
                if incumbent[1] and not was_moved:
                    best[k] = (row, was_moved)
        out = [row for row, _ in best.values()]
    else:
        out = [row for row, _ in resolved]

    stats = {"moved": moved, "dropped": dropped, "collapsed": collapsed}
    if moved or dropped or collapsed:
        print(f"[DB] resolve_paper_rows: {moved} re-pointed through a merge, {dropped} dropped "
              f"(paper gone), {collapsed} collapsed onto an existing key")
    return out, stats


def get_disciplines_map(conn) -> dict[str, str]:
    """Get mapping of discipline slug -> id."""
    rows = execute(conn, "SELECT id, slug FROM disciplines")
    return {d["slug"]: d["id"] for d in rows}


def get_tags_map(conn) -> dict[str, str]:
    """Get mapping of tag name -> id."""
    rows = execute(conn, "SELECT id, name FROM tags")
    return {t["name"]: t["id"] for t in rows}


def link_paper_to_discipline(conn, paper_id: str, discipline_id: str, source: str = "openalex") -> None:
    """Link a paper to a discipline."""
    try:
        execute_write(conn,
            "INSERT INTO paper_disciplines (paper_id, discipline_id, source) VALUES (%s, %s, %s) ON CONFLICT (paper_id, discipline_id) DO NOTHING",
            [paper_id, discipline_id, source])
    except Exception as e:
        print(f"[DB] Error linking paper to discipline: {e}")
        conn.rollback()


def link_paper_to_tag(conn, paper_id: str, tag_id: str, source: str = "openalex") -> None:
    """Link a paper to a tag."""
    try:
        execute_write(conn,
            "INSERT INTO paper_tags (paper_id, tag_id, source) VALUES (%s, %s, %s) ON CONFLICT (paper_id, tag_id) DO NOTHING",
            [paper_id, tag_id, source])
    except Exception as e:
        print(f"[DB] Error linking paper to tag: {e}")
        conn.rollback()


def get_disciplines(conn) -> list[dict]:
    """Get all disciplines."""
    return execute(conn, "SELECT * FROM disciplines ORDER BY display_order")


def get_recent_papers_by_discipline(conn, discipline_id: str, days: int = 7) -> list[dict]:
    """Get recent papers for a specific discipline."""
    return execute(conn,
        """SELECT p.* FROM paper_disciplines pd
           JOIN papers p ON p.id = pd.paper_id
           WHERE pd.discipline_id = %s AND p.created_at >= NOW() - interval '%s days'""",
        [discipline_id, days])


def get_papers_without_summary(conn, limit: int = 100) -> list[dict]:
    """Get papers that don't have a summary yet."""
    return execute(conn,
        """SELECT p.id, p.title, p.abstract FROM papers p
           LEFT JOIN summaries s ON s.paper_id = p.id
           WHERE s.id IS NULL AND p.abstract IS NOT NULL
           LIMIT %s""",
        [limit])


def save_summary(conn, paper_id: str, summary: dict, model: str) -> None:
    """Save an AI-generated summary."""
    execute_write(conn,
        """INSERT INTO summaries (paper_id, llm_model, so_what, contribution, methodology, data_info, key_finding, limitations)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (paper_id, llm_model) DO UPDATE SET
           so_what = EXCLUDED.so_what, contribution = EXCLUDED.contribution,
           methodology = EXCLUDED.methodology, data_info = EXCLUDED.data_info,
           key_finding = EXCLUDED.key_finding, limitations = EXCLUDED.limitations""",
        [paper_id, model,
         summary.get("so_what"), summary.get("contribution"),
         psycopg2.extras.Json(summary.get("methodology")),
         psycopg2.extras.Json(summary.get("data")),
         summary.get("key_finding"), summary.get("limitations")])


def save_rankings(conn, date: str, discipline_id: str | None, rankings: list[dict]) -> None:
    """Save daily rankings for a discipline."""
    for r in rankings:
        execute_write(conn,
            """INSERT INTO daily_rankings (paper_id, ranking_date, discipline_id, rank_position, score, score_breakdown)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (paper_id, ranking_date, discipline_id) DO UPDATE SET
               rank_position = EXCLUDED.rank_position, score = EXCLUDED.score, score_breakdown = EXCLUDED.score_breakdown""",
            [r["paper_id"], date, discipline_id, r["rank"], r["score"],
             psycopg2.extras.Json(r.get("breakdown"))])


def update_paper_latest_rank(conn, paper_id: str, rank: int, score: float) -> None:
    """Update denormalized rank on papers table."""
    execute_write(conn, "UPDATE papers SET latest_rank = %s, latest_score = %s WHERE id = %s",
                  [rank, score, paper_id])
