"""merge_papers.py — merge a duplicate paper into a survivor, in ONE transaction.

SCHEMA_EVOLUTION_ADR D3. Invariant: `papers.id` is NEVER rewritten in place. A merge
re-points every referencing row from the loser to the winner, records a redirect, and only
then deletes the loser row (by which point its cascades hit nothing).

Two failure classes this exists to prevent:
  * FK-less referencers → a bare DELETE silently orphans their rows.
  * CASCADE referencers → a bare DELETE silently DESTROYS their rows (this is the larger
    set, and it includes `paper_subfields`, contrary to the ADR's original text).

Table discovery is done at RUNTIME from `information_schema`, never from a hardcoded list:
every table with a `paper_id` column is re-pointed, whether or not it has an FK. A table
that forgets its FK is therefore still merged correctly, and is separately caught by
`pipeline/audit_paper_refs.py` (D4 invariant ii).

Collision policy is likewise DERIVED: for each table we read its PK/UNIQUE constraints that
include `paper_id`. A loser row whose re-pointed key would collide with an existing winner
row is DELETED (the winner's row is authoritative); the rest are UPDATEd. Tables whose only
key is a surrogate `id` can never collide, so they are a plain UPDATE.

  python pipeline/merge_papers.py <loser_id> <winner_id> [--reason "..."] [--dry-run]

A-D3-1 (library, 039/040) — CLOSED 2026-07-11. The generic collision policy is NOT safe for
the per-user library: `library_items` has UNIQUE(user_id, paper_id), so a user holding BOTH
the loser and the winner paper would have their loser row hard-DELETEd — and `annotations` /
`user_paper_files` / `reading_state` all hang off `library_item_id` ON DELETE CASCADE, so that
delete would destroy their highlights, uploaded-file pointers and reading position. It would
also be invisible to the sync protocol (no tombstone, no version bump → the client mirror
keeps a phantom row forever). `library_items` therefore gets a BESPOKE handler (below), and
any *other* table whose paper_id FK is ON DELETE RESTRICT — the schema's own marker for
"user data too precious to cascade" — aborts the merge until it gets one too.
Gate: `pipeline/rehearse_ad3_1.py` (throwaway Neon branch, 039+040 applied).

L1a (collections, 045) — the membership child. `library_collection_items` is a fourth child of
`library_items`, and it is merged by (4e) below. Two things make this pass unusual:

  * It ships BEFORE its own DDL. The forced order is L1a -> 045 -> app (spec §7): applying 045
    while this file still lacks the pass would abort EVERY corpus merge on the unknown-child
    guard. So (4e) and the `merged_into` stamp are guarded independently — the pass on table
    existence, the stamp on COLUMN existence (A5a) — and are plain no-ops until 045 lands.
  * Membership is a FIELD (`filed`/`filed_at`), not row existence, so there are no membership
    tombstones to revive; the merge folds by the same timestamp-LWW the clients use.

Gate: `pipeline/rehearse_l1a.py` (same throwaway-branch pattern, 039+040+045 applied, plus
prod-shaped runs with the table and the column absent).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env.local", override=False)

# Counters on `papers` that are denormalized from a referencing table; recomputed on the
# winner after the re-point. (column, source table)
DENORM_COUNTS = [("upvote_count", "paper_upvotes"), ("comment_count", "paper_comments")]

# Tables that reference papers.id WITHOUT an enforced FK. `audit_paper_refs.py` diffs the
# live DB against this set, in BOTH directions: an undeclared FK-less table is a silent-orphan
# risk, and a declared table that has since gained its FK means this set is stale.
# EMPTY since 038_paper_identity.sql restored the FKs on summaries + user_saved_papers
# (validated 2026-07-07, 0 orphans). Merge correctness does not depend on this set — table
# discovery is a runtime information_schema scan — it only orders FK-less tables first.
# 043 (T-B3) adds two, FK-less by design: `author_link_conflicts` (a log that outlives nothing it
# needs) and the UNLOGGED `author_link_staging` (bulk resolver input). A merge re-points both.
# `author_link_moves` (050) logs provisional-author link moves by (author_id, paper_id) with no FK to
# papers; the generic pass moves a loser's entries to the winner and drops one that collides on the
# primary key (the winner already has an entry for that author).
# `taste_negatives` (060) has no FK by design (taste engine contracts v1.2, "Negatives",
# docs/TASTE_ENGINE_CONTRACTS.md): a paper deleted outright must not delete a negative, and readers drop
# a negative whose paper is gone. A merge re-points it in `merge_taste_negatives`, before the generic pass.
FK_LESS_ALLOWLIST: set[str] = {"author_link_conflicts", "author_link_moves", "author_link_staging",
                               "taste_negatives"}

# Tables whose rows may outlive their paper by design: a paper deleted outright leaves its negatives
# behind (above). `orphan_report` leaves them out of its zero-orphan count, and `orphans_by_design`
# counts them on their own.
ORPHANS_BY_DESIGN: set[str] = {"taste_negatives"}

# Child tables of `library_items` (keyed by library_item_id, not paper_id, so the paper_id
# scan never sees them). The bespoke handler re-points them explicitly. A child table found
# in the DB but missing from this set aborts the merge — a new child must get a deliberate
# collision policy, not a default one. `library_collection_items` joined at 045 (L1a, pass 4e).
#
# `library_capture_queue` (047, W-E3 D-W3-6) is known WITHOUT a pass, and that is the policy: a
# corpus merge needs no pass over it, because queue rows hang only off EXTERNAL items
# (`paper_id IS NULL`), which `merge_library_items` never visits (it selects users by
# `paper_id = loser`); a queue row whose item the drain itself merges is resolved by the drain
# (status `merged`) in the same transaction (pipeline/drain_capture_queue.py); and its
# ON DELETE CASCADE covers hard-delete GC.
#
# `library_server_fetches` (058, a find-pdf fetch log) and the three server-only import tables of 066
# (`library_import_links`, `library_item_source_records`, `library_item_citekeys`) joined with ledger
# task lp3-import-tables (docs/ledger/lp3-import-tables.md): their rows follow the surviving item in
# passes (4f) to (4i) of `_fold_library_rows`, each guarded by table existence. Before this entry every
# merge stopped on 058's table.
KNOWN_LIBRARY_CHILDREN: set[str] = {"annotations", "user_paper_files", "reading_state",
                                    "library_collection_items", "library_capture_queue",
                                    "library_server_fetches", "library_import_links",
                                    "library_item_source_records", "library_item_citekeys"}

# FROZEN PROTOCOL CONSTANT — L1 spec §3. A membership row's id is derived, not chosen, so that
# two offline clients filing the same paper into the same collection converge on ONE row. The
# namespace is itself derivable (and so auditable) rather than a magic random value:
#   uuid5(NAMESPACE_URL, 'https://literview.com/ns/library-collection-item')
# Changing either the namespace or the key format is a new protocol version, i.e. never. The
# cross-language vectors that hold this file, the client TS and the server TS byte-equal live in
# `lib/library/collection-item-id.vectors.json`.
NS_LV_COLLECTION_ITEM = uuid.UUID("6cf1cc51-6eb6-5afe-9a3a-e785e1f6265a")


def collection_item_id(collection_id: str, library_item_id: str) -> str:
    """uuidv5 over `lower(collection_id):lower(library_item_id)`, canonical hyphenated text."""
    return str(uuid.uuid5(NS_LV_COLLECTION_ITEM,
                          f"{str(collection_id).lower()}:{str(library_item_id).lower()}"))

# read_status is a ladder; a merge keeps the furthest-along value.
READ_RANK = {"unread": 0, "reading": 1, "read": 2}
MEMO_SEP = "\n\n---\n\n"

# The per-user sync counter bump (02 §5 / lib/library/version.ts BUMP_CTE), verbatim. It is a
# LOCKED COUNTER ROW: taking it as the FIRST statement that touches a user's data — and holding
# it to COMMIT inside this one transaction — is what serializes the merge against that user's
# concurrent app pushes. Bumping AFTER writing their rows would invert the app's lock order
# (app: counter → rows) and deadlock.
BUMP_SQL = ("INSERT INTO library_version (user_id, v) VALUES (%s, 1) "
            "ON CONFLICT (user_id) DO UPDATE SET v = library_version.v + 1 RETURNING v")


# The A5 scope gate, mirrored from lib/queries.ts `inScopePaper()`. A paper is publicly
# reachable iff it links to an in-scope discipline; out-of-scope papers stay in the DB but 404.
IN_SCOPE = ("EXISTS (SELECT 1 FROM paper_disciplines pd JOIN disciplines d "
            "ON d.id = pd.discipline_id WHERE pd.paper_id = p.id AND d.in_scope)")


# Never re-point these: snapshots and the redirect table itself.
def _excluded(t: str) -> bool:
    # Snapshot tables are frozen copies, never merge targets. `_bak` was the 07-07 naming; the
    # 07-18 retire snapshots (`paper_tags_retirebak_*`) and the B4c `_b4c_nonarticle_backup`
    # slipped past it and turned audit_paper_refs red (found 2026-09-16, T-B3 rehearsal).
    return "_bak" in t or "bak_" in t or t.endswith("_backup") or t == "paper_redirects"


def paper_fk_actions(cur) -> dict[str, str]:
    """table -> ON DELETE action of its paper_id FK to papers ('c' cascade, 'r' restrict,
    'a' no action, 'n' set null, 'd' set default)."""
    cur.execute("""
        SELECT rel.relname, con.confdeltype
          FROM pg_constraint con
          JOIN pg_class rel  ON rel.oid  = con.conrelid
          JOIN pg_class frel ON frel.oid = con.confrelid
          JOIN LATERAL unnest(con.conkey) AS k(attnum) ON true
          JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = k.attnum
         WHERE con.contype = 'f' AND frel.relname = 'papers' AND a.attname = 'paper_id'""")
    return {t: d.decode() if isinstance(d, bytes) else d for t, d in cur.fetchall()}


def has_table(cur, qualified: str) -> bool:
    """BL-7 deploy-order guard. L1a ships BEFORE 045, so every collections-aware code path must
    no-op while the objects are absent."""
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", [qualified])
    return bool(cur.fetchone()[0])


def has_column(cur, table: str, column: str) -> bool:
    """A5a. `library_items.merged_into` arrives WITH 045, i.e. strictly after L1a ships. An
    ungated stamp would kill every corpus merge in the L1a->045 window on UndefinedColumn — the
    table-level to_regclass guard does not cover it, because the column lives on a table that
    already exists."""
    cur.execute("SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
                [table, column])
    return cur.fetchone()[0] > 0


def library_children(cur) -> list[str]:
    cur.execute("""
        SELECT table_name FROM information_schema.columns
         WHERE table_schema = 'public' AND column_name = 'library_item_id'
         ORDER BY table_name""")
    return [r[0] for r in cur.fetchall() if not _excluded(r[0])]


def connect(dsn: str | None = None):
    dsn = dsn or os.environ["DATABASE_URL"]
    c = psycopg2.connect(dsn, connect_timeout=20)
    c.autocommit = False
    return c


def referencing_tables(cur) -> list[str]:
    cur.execute("""
        SELECT table_name FROM information_schema.columns
         WHERE table_schema = 'public' AND column_name = 'paper_id'
         ORDER BY table_name""")
    return [r[0] for r in cur.fetchall() if not _excluded(r[0])]


def collision_keys(cur, table: str) -> list[list[str]]:
    """PK/UNIQUE constraint column-lists that include paper_id — the keys a re-pointed loser
    row could collide on."""
    cur.execute("""
        SELECT array_agg(a.attname ORDER BY k.ord) AS cols
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
          JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = k.attnum
         WHERE rel.relname = %s AND con.contype IN ('p', 'u')
         GROUP BY con.oid""", [table])
    return [r[0] for r in cur.fetchall() if "paper_id" in r[0]]


def pick_page_edits(w_val, l_val, keep):
    """Pass (4c)'s choice of `reading_state.page_edits` when both rows exist (052;
    docs/library/READER_PAGE_EDIT_DESIGN.md section 1, "Corpus merge", and L11(b)): the value whose
    `file_id` is `keep`, the primary file (4b) kept, with the winner's first when both are; else
    the winner's value; else the loser's. No kept file matches nothing."""
    def on_keep(val) -> bool:
        return (keep is not None and isinstance(val, dict) and val.get("file_id") is not None
                and str(val["file_id"]).lower() == str(keep).lower())
    if on_keep(w_val):
        return w_val
    if on_keep(l_val):
        return l_val
    return w_val if w_val is not None else l_val


def _merge_library_user(cur, user: str, loser: str, winner: str, *,
                        collections: bool = False, merged_into: bool = False,
                        page_edits: bool | None = None) -> dict[str, int]:
    """One user's library rows, loser paper -> winner paper. See module header for why this
    cannot use the generic delete-colliding policy.

    `collections` / `merged_into` are the 045 capability flags, probed once per merge by the
    caller (BL-7 / A5a). Both False = the pre-045 world, and every collections statement below
    is skipped."""
    # (1) BUMP FIRST — before any row of this user's data is touched (lock order; see BUMP_SQL).
    cur.execute(BUMP_SQL, [user])
    v = cur.fetchone()[0]

    cols = "id, deleted_at IS NULL, starred, read_status, memo, added_at, ext_meta"
    cur.execute(f"SELECT {cols} FROM library_items WHERE user_id = %s AND paper_id = %s", [user, loser])
    l_row = cur.fetchone()   # UNIQUE ⇒ at most 1
    cur.execute(f"SELECT {cols} FROM library_items WHERE user_id = %s AND paper_id = %s", [user, winner])
    w_row = cur.fetchone()
    return _fold_library_rows(cur, user, l_row, w_row, winner, v, collections=collections,
                              merged_into=merged_into, page_edits=page_edits)


def _fold_library_rows(cur, user: str, l_row, w_row, winner_paper_id: str | None, v: int, *,
                       collections: bool = False, merged_into: bool = False,
                       page_edits: bool | None = None) -> dict[str, int]:
    """Steps (2)-(4e) of the per-user library merge, on rows the caller has already read AFTER
    taking the user's counter lock (BUMP_SQL). Two callers: `_merge_library_user` (corpus merge —
    loser and winner rows are both paper-backed) and `pipeline/drain_capture_queue.py` (W-E3
    D-W3-6 — the loser is an EXTERNAL row, `paper_id IS NULL`, and the winner is the user's row
    for the corpus paper the queue resolved to, or absent).

    `l_row` / `w_row` are `(id, deleted_at IS NULL, starred, read_status, memo, added_at,
    ext_meta)` tuples (`w_row` may be None); `winner_paper_id` is the `papers.id` the surviving
    row points at; `v` is the already-bumped version. `page_edits` is the 052 capability flag
    (None = probe it in (4c)). A surviving row that points at a corpus
    paper never carries `ext_meta` (039: NULL when paper_id is set), so step (3) clears it on the
    re-point and step (4) never copies an external row's `ext_meta` onto the winner. Both are
    no-ops for the corpus merge, whose rows carry `ext_meta = NULL` already."""
    out = {"repointed": 0, "field_merged": 0, "tombstoned": 0,
           "annotations_moved": 0, "files_moved": 0, "reading_state_moved": 0,
           "collection_items_folded": 0, "collection_items_dropped": 0,
           "collection_items_revived": 0, "resurrect_children_restamped": 0,
           "import_links_moved": 0, "source_records_moved": 0, "source_records_dropped": 0,
           "citekeys_moved": 0, "citekeys_dropped": 0, "server_fetches_moved": 0}
    l_id, l_live, l_star, l_read, l_memo, l_added, l_ext = l_row
    w = w_row

    # (2) A tombstoned loser row carries DELETED user intent. It must never resurrect into the
    # winner (no field merge), and its children must NOT be re-pointed onto the winner's live
    # item — the client's orphan sweep (sync-client.ts) already drops children whose parent
    # tombstone it has seen, and server-side tombstone GC will cascade them away at 180d.
    # Releasing paper_id (nullable) is all that is needed: it clears the RESTRICT FK so the
    # loser `papers` row can be deleted, and NULLs are distinct under UNIQUE(user_id, paper_id).
    if not l_live:
        cur.execute("UPDATE library_items SET paper_id = NULL, version = %s, updated_at = now() "
                    "WHERE id = %s", [v, l_id])
        out["tombstoned"] += 1
        return out

    # (3) Live loser, no winner row for this user: a plain re-point. The version bump is NOT
    # optional — paper_id changed, so the client must re-pull the row.
    if w is None:
        cur.execute("UPDATE library_items SET paper_id = %s, ext_meta = NULL, version = %s, "
                    "updated_at = now() WHERE id = %s", [winner_paper_id, v, l_id])
        out["repointed"] += 1
        return out

    # (4) Live loser AND a winner row: fold the loser INTO the winner row, then tombstone it.
    w_id, w_live, w_star, w_read, w_memo, w_added, w_ext = w
    if w_live:
        star = bool(w_star) or bool(l_star)
        read = max(w_read, l_read, key=lambda s: READ_RANK.get(s, 0))
        memo = (w_memo + MEMO_SEP + l_memo) if (w_memo and l_memo and w_memo != l_memo) \
            else (w_memo or l_memo)
        ext = w_ext if w_ext is not None else l_ext
    else:
        # The winner row is a tombstone and the loser is live: the user's only live intent is
        # the loser's. Resurrect the winner row, but let the LIVE row's fields win wholesale —
        # ORing in a deleted row's starred/read_status/memo would resurrect deleted content.
        star, read, memo, ext = l_star, l_read, l_memo, l_ext
    if winner_paper_id is not None:
        ext = None   # a paper-backed survivor never carries ext_meta (039); see the docstring
    added = min(w_added, l_added)

    cur.execute("""UPDATE library_items
                      SET starred = %s, read_status = %s, memo = %s, ext_meta = %s,
                          added_at = %s, deleted_at = NULL, version = %s, updated_at = now()
                    WHERE id = %s""", [star, read, memo, ext, added, v, w_id])
    out["field_merged"] += 1

    # (4a) annotations: no unique key on library_item_id, so a plain re-point cannot collide.
    cur.execute("UPDATE annotations SET library_item_id = %s, version = %s, updated_at = now() "
                "WHERE library_item_id = %s", [w_id, v, l_id])
    out["annotations_moved"] = cur.rowcount

    # (4b) user_paper_files: UNIQUE is (user_id, provider, provider_file_id) — untouched by the
    # re-point, so no collision. But both items may carry a live is_primary file; the winner's
    # own primary stays authoritative and everything else is demoted (the schema does not
    # enforce ≤1 primary, so the loser may even bring two).
    cur.execute("SELECT id FROM user_paper_files WHERE library_item_id = %s AND deleted_at IS NULL "
                "AND is_primary ORDER BY added_at DESC, id LIMIT 1", [w_id])
    row = cur.fetchone()
    keep = row[0] if row else None
    cur.execute("UPDATE user_paper_files SET library_item_id = %s, version = %s "
                "WHERE library_item_id = %s", [w_id, v, l_id])
    out["files_moved"] = cur.rowcount
    if keep is None:
        cur.execute("SELECT id FROM user_paper_files WHERE library_item_id = %s AND deleted_at IS NULL "
                    "AND is_primary ORDER BY added_at DESC, id LIMIT 1", [w_id])
        row = cur.fetchone()
        keep = row[0] if row else None
    if keep is not None:
        cur.execute("UPDATE user_paper_files SET is_primary = false, version = %s "
                    "WHERE library_item_id = %s AND deleted_at IS NULL AND is_primary AND id <> %s",
                    [v, w_id, keep])

    # (4c) reading_state: PK (user_id, library_item_id) DOES collide. Most-recently-updated wins
    # — NOT the furthest page: a user who restarted the paper on the winner copy must not have
    # their fresh position clobbered by a stale page-40 bookmark on the loser copy. A row with no
    # page ranks below any row with one, whatever its updated_at (READER_PAGE_EDIT_DESIGN L11(a)):
    # a page-edits-only first write INSERTs a row with page NULL and updated_at = now(), and it
    # must never carry a null position onto the winner. There is no deleted_at on this table, so
    # the folded-away row is hard-deleted; that is safe precisely because the client drops it via
    # the parent item's tombstone (orphan sweep).
    #
    # page_edits (052): its own UPDATE, run whenever both rows exist and independent of the
    # position comparison, before the loser row is deleted. It keeps the value whose file_id is the
    # primary file (4b) kept, else the winner's, else the loser's (`pick_page_edits`). Guarded by
    # has_column so a merge before 052 is applied still runs; probed here when the caller did not
    # (drain_capture_queue.py calls this function without the flag).
    if page_edits is None:
        page_edits = has_column(cur, "reading_state", "page_edits")
    rs_cols = "page, scroll_frac, updated_at" + (", page_edits" if page_edits else "")
    cur.execute(f"SELECT {rs_cols} FROM reading_state "
                "WHERE user_id = %s AND library_item_id = %s", [user, l_id])
    rl = cur.fetchone()
    if rl:
        cur.execute(f"SELECT {rs_cols} FROM reading_state WHERE user_id = %s AND library_item_id = %s",
                    [user, w_id])
        rw = cur.fetchone()
        if rw is None:
            cur.execute("UPDATE reading_state SET library_item_id = %s, version = %s "
                        "WHERE user_id = %s AND library_item_id = %s", [w_id, v, user, l_id])
            out["reading_state_moved"] = 1
        else:
            # loser copy is the fresher read, and a real position
            if rl[0] is not None and (rw[0] is None or rl[2] > rw[2]):
                cur.execute("""UPDATE reading_state SET page = %s, scroll_frac = %s,
                                      updated_at = %s, version = %s
                                WHERE user_id = %s AND library_item_id = %s""",
                            [rl[0], rl[1], rl[2], v, user, w_id])
                out["reading_state_moved"] = 1
            if page_edits:
                # `version` only, never `updated_at`: the (4r) reason, and updated_at is what the
                # position comparison above reads.
                pick = pick_page_edits(rw[3], rl[3], keep)
                cur.execute("UPDATE reading_state SET page_edits = %s::jsonb, version = %s "
                            "WHERE user_id = %s AND library_item_id = %s",
                            [None if pick is None else json.dumps(pick), v, user, w_id])
            cur.execute("DELETE FROM reading_state WHERE user_id = %s AND library_item_id = %s",
                        [user, l_id])

    # (4r) THE RESURRECT CASE, versioned-child arm. When the winner row was a tombstone and has
    # just been brought back to life above (`not w_live`), the children that were ALREADY hanging
    # off it are in exactly the hole (4e-0) describes for memberships: every synced client dropped
    # them from its mirror by orphan-sweep when the winner's tombstone arrived, and they still
    # carry their OLD versions — below every client's cursor. The item returns at the new version
    # with its annotations, files and reading position permanently missing, and no protocol check
    # detects it. (4a)–(4c) only stamp `v` on rows re-pointed FROM the loser, so they do not cover
    # this; the blanket re-stamp does, and `version <> v` keeps it a no-op for rows those passes
    # already touched (no double write, and the rowcount stays meaningful).
    #
    # `version` ONLY — never `updated_at`. On annotations and reading_state that column is
    # content/LWW metadata the clients compare against (the same reason (4c) preserves the loser's
    # `updated_at` verbatim when it wins); bumping it here would fabricate an edit. `version` is
    # the sync cursor and is the only thing that is actually stale. Tombstoned children are
    # re-stamped too: a client that never received the child's own tombstone still needs it, and
    # re-delivering one it did receive is idempotent.
    if not w_live:
        for tbl in ("annotations", "user_paper_files"):
            cur.execute(f"UPDATE {tbl} SET version = %s "
                        "WHERE library_item_id = %s AND version <> %s", [v, w_id, v])
            out["resurrect_children_restamped"] += cur.rowcount
        cur.execute("UPDATE reading_state SET version = %s "
                    "WHERE user_id = %s AND library_item_id = %s AND version <> %s",
                    [v, user, w_id, v])
        out["resurrect_children_restamped"] += cur.rowcount

    # (4e) library_collection_items — numbered (4e) although it runs BEFORE (4d): "(4d)" is the
    # L1 spec's name for the tombstone pass and must not be renumbered under it.
    #
    # Modeled on (4c): the other child with no `deleted_at` that also collides on a per-parent
    # key. Membership is a FIELD (`filed` + `filed_at`), so this is not a re-point but a
    # per-collection fold by the SAME timestamp-LWW the clients use — and it merges `filed=false`
    # rows too, because an un-filing is user intent exactly like a filing.
    if collections:
        # (4e-0) THE RESURRECT CASE. If the winner row was a tombstone and has just been brought
        # back to life above (`not w_live`), its OWN membership rows come back with it — §0 says
        # memberships under a dead parent are hidden at read time, never destroyed, so a
        # resurrected item legitimately returns to the collections it was in. (Un-filing them
        # would invent an un-file the user never performed; the flip is the same convergent kind
        # (4c) already performs on reading position — LC-2.)
        #
        # But those rows still carry their OLD versions, and every synced client dropped them
        # from its mirror via the orphan sweep when the winner's tombstone arrived. Left
        # un-stamped they are below every client's cursor: the client re-adds the item at the new
        # version and NEVER receives its memberships again — a permanent, silent server/client
        # divergence that no protocol check detects. Re-stamping puts them back in the pull.
        if not w_live:
            cur.execute("UPDATE library_collection_items SET version = %s, updated_at = now() "
                        "WHERE library_item_id = %s", [v, w_id])
            out["collection_items_revived"] = cur.rowcount

        # A5b: skip collections that are themselves tombstoned. The membership would be invisible
        # either way, and propagating into a dead collection manufactures precisely the
        # post-tombstone child rows the GC deferral (A4) exists to survive.
        cur.execute("""SELECT rl.collection_id, rl.filed, rl.filed_at
                         FROM library_collection_items rl
                         JOIN library_collections c ON c.id = rl.collection_id
                        WHERE rl.library_item_id = %s AND c.deleted_at IS NULL""", [l_id])
        for coll_id, filed, filed_at in cur.fetchall():
            # A7c: the recency test is the WHERE on the DO UPDATE arm, so a winner row that the
            # rule leaves unchanged is NOT touched — no spurious version bump, no pull churn.
            # It is also what makes chained merges (L1, L2 -> W) idempotent.
            # The comparison is a ROW comparison, not `filed_at <`, because the client's rule
            # (spec §2) is "later filed_at wins; EXACT TIE -> filed=true wins (add-wins)". With
            # false < true in Postgres, `(filed_at, filed) < (excluded...)` is exactly that rule
            # in one operator — and a merge that resolved a tie the other way would reach a
            # DIFFERENT state than a client resolving the same pair, which is the one thing a
            # convergent protocol may not do. Ties are reachable: A1's Lamport bump and any
            # replayed op both produce equal timestamps.
            cur.execute("""INSERT INTO library_collection_items
                             (id, user_id, collection_id, library_item_id, filed, filed_at,
                              version, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                      ON CONFLICT (collection_id, library_item_id) DO UPDATE
                              SET filed = EXCLUDED.filed, filed_at = EXCLUDED.filed_at,
                                  version = EXCLUDED.version, updated_at = now()
                            WHERE (library_collection_items.filed_at,
                                   library_collection_items.filed)
                                < (EXCLUDED.filed_at, EXCLUDED.filed)""",
                        [collection_item_id(coll_id, w_id), user, coll_id, w_id, filed,
                         filed_at, v])
            out["collection_items_folded"] += cur.rowcount

        # Then hard-delete EVERY loser membership — including those in dead collections, which
        # were deliberately not propagated above. The (4c) precedent verbatim: safe because the
        # client drops the row via the loser item's tombstone (orphan sweep), and (4d) tombstones
        # the loser item in this same transaction at this same version.
        cur.execute("DELETE FROM library_collection_items WHERE library_item_id = %s", [l_id])
        out["collection_items_dropped"] += cur.rowcount

    # (4f)-(4i) the server-only children (066's import tables and 058's fetch log), ledger task
    # lp3-import-tables. None is a sync type, so no version is stamped and the resurrect case needs no
    # re-stamp. Each pass is guarded by table existence, so a database without 066 (and the crawler's
    # copy of this file before it is updated) behaves exactly as before.
    _fold_server_children(cur, l_id, w_id, out)

    # (4d) the loser item becomes a TOMBSTONE, never a hard DELETE: the client mirror drops a row
    # only when it sees deleted_at, and a hard delete here would also CASCADE the children we
    # just re-pointed had any been missed. paper_id = NULL releases the RESTRICT FK.
    #
    # BL-4: the tombstone also NAMES the winner (`merged_into`), which is what lets a client
    # rewrite an offline file/un-file that was queued against the loser identity onto the merged
    # one instead of letting it die silently against a dead row. Stamped only in this path: the
    # (2) branch's tombstone is the USER's own deletion, and a pending op on it must die, not
    # redirect.
    if merged_into:
        cur.execute("UPDATE library_items SET deleted_at = now(), paper_id = NULL, "
                    "merged_into = %s, version = %s, updated_at = now() WHERE id = %s",
                    [w_id, v, l_id])
    else:
        cur.execute("UPDATE library_items SET deleted_at = now(), paper_id = NULL, version = %s, "
                    "updated_at = now() WHERE id = %s", [v, l_id])
    out["tombstoned"] += 1
    return out


def loser_citekey_wins(w_key, l_key) -> bool:
    """Pass (4h)'s choice when both items hold a citekey; each argument is `(origin, exported_at)`.
    A key with `exported_at` set beats one without; between two exported keys the earlier
    `exported_at` wins; otherwise `imported` beats `generated`; otherwise the winner's key."""
    w_origin, w_exp = w_key
    l_origin, l_exp = l_key
    if (l_exp is not None) != (w_exp is not None):
        return l_exp is not None
    if l_exp is not None and l_exp != w_exp:
        return l_exp < w_exp
    if l_origin != w_origin:
        return l_origin == "imported"
    return False


def _fold_server_children(cur, l_id, w_id, out: dict[str, int]) -> None:
    """Passes (4f)-(4i) of the case-(4) fold: the loser item `l_id` folds into `w_id`
    (docs/ledger/lp3-import-tables.md, change 2). Counts go into `out`."""
    # (4f) library_import_links: the unique key (user_id, source, source_kind, source_id) does not
    # involve the item, so the re-point cannot conflict. An `item` row's target is the item itself.
    if has_table(cur, "public.library_import_links"):
        cur.execute("UPDATE library_import_links SET library_item_id = %s, "
                    "target_id = CASE WHEN source_kind = 'item' THEN %s ELSE target_id END, "
                    "updated_at = now() WHERE library_item_id = %s", [w_id, w_id, l_id])
        out["import_links_moved"] += cur.rowcount

    # (4g) library_item_source_records, PK (library_item_id, source): per source, the later
    # imported_at survives on the winner (a tie keeps the winner's). The losing row is deleted first,
    # because the primary key rejects the re-point while the winner's row still holds the source.
    if has_table(cur, "public.library_item_source_records"):
        cur.execute("SELECT source, imported_at FROM library_item_source_records "
                    "WHERE library_item_id = %s ORDER BY source", [l_id])
        for source, l_at in cur.fetchall():
            cur.execute("SELECT imported_at FROM library_item_source_records "
                        "WHERE library_item_id = %s AND source = %s", [w_id, source])
            row = cur.fetchone()
            if row is not None and not (l_at > row[0]):
                cur.execute("DELETE FROM library_item_source_records "
                            "WHERE library_item_id = %s AND source = %s", [l_id, source])
                out["source_records_dropped"] += cur.rowcount
                continue
            if row is not None:
                cur.execute("DELETE FROM library_item_source_records "
                            "WHERE library_item_id = %s AND source = %s", [w_id, source])
                out["source_records_dropped"] += cur.rowcount
            cur.execute("UPDATE library_item_source_records SET library_item_id = %s "
                        "WHERE library_item_id = %s AND source = %s", [w_id, l_id, source])
            out["source_records_moved"] += cur.rowcount

    # (4h) library_item_citekeys, PK library_item_id: one key survives (`loser_citekey_wins`). The
    # losing key is deleted first. A re-pointed key cannot collide with UNIQUE (user_id, citekey):
    # it is the same user's existing key.
    if has_table(cur, "public.library_item_citekeys"):
        cur.execute("SELECT origin, exported_at FROM library_item_citekeys WHERE library_item_id = %s",
                    [l_id])
        l_key = cur.fetchone()
        if l_key is not None:
            cur.execute("SELECT origin, exported_at FROM library_item_citekeys "
                        "WHERE library_item_id = %s", [w_id])
            w_key = cur.fetchone()
            if w_key is not None and not loser_citekey_wins(w_key, l_key):
                cur.execute("DELETE FROM library_item_citekeys WHERE library_item_id = %s", [l_id])
                out["citekeys_dropped"] += cur.rowcount
            else:
                if w_key is not None:
                    cur.execute("DELETE FROM library_item_citekeys WHERE library_item_id = %s", [w_id])
                    out["citekeys_dropped"] += cur.rowcount
                cur.execute("UPDATE library_item_citekeys SET library_item_id = %s "
                            "WHERE library_item_id = %s", [w_id, l_id])
                out["citekeys_moved"] += cur.rowcount

    # (4i) library_server_fetches (058): a fetch log with no unique key, a plain re-point.
    if has_table(cur, "public.library_server_fetches"):
        cur.execute("UPDATE library_server_fetches SET library_item_id = %s WHERE library_item_id = %s",
                    [w_id, l_id])
        out["server_fetches_moved"] += cur.rowcount


def merge_library_items(cur, loser: str, winner: str) -> dict[str, int]:
    """Bespoke pass for `library_items` (A-D3-1). Users are processed in sorted order so that
    two concurrent merges take the per-user counter locks in the same order."""
    children = set(library_children(cur))
    unknown = children - KNOWN_LIBRARY_CHILDREN
    if unknown:
        raise SystemExit(f"[abort] unknown library_items child table(s) with no merge policy: "
                         f"{sorted(unknown)} — give them a bespoke pass (A-D3-1)")

    # 045 capability probe, ONCE per merge rather than once per user (BL-7 / A5a). Both are
    # independent: the table and the column arrive together in 045, but a partially-applied or
    # hand-patched environment must degrade to a no-op rather than to an aborted merge.
    collections = has_table(cur, "public.library_collection_items")
    merged_into = has_column(cur, "library_items", "merged_into")
    page_edits = has_column(cur, "reading_state", "page_edits")   # 052

    cur.execute("SELECT DISTINCT user_id FROM library_items WHERE paper_id = %s ORDER BY user_id",
                [loser])
    users = [r[0] for r in cur.fetchall()]
    total: dict[str, int] = {"users": len(users)}
    for u in users:
        merged = _merge_library_user(cur, u, loser, winner, collections=collections,
                                     merged_into=merged_into, page_edits=page_edits)
        for k, n in merged.items():
            total[k] = total.get(k, 0) + n
    return total


# table -> handler(cur, loser, winner). The generic pass skips these.
BESPOKE_MERGERS = {"library_items": merge_library_items}

# The taste tables of migration 060 the generic pass leaves alone (contracts v1.2, "Paper merges"
# and "Negatives"; ledger task taste-pipeline item 5). `taste_negatives` gets its own pass below
# (a version is a frozen id list, so the loser's id becomes the winner's, and a version that
# already holds the winner drops the duplicate); `taste_cards` is rebuilt weekly and
# `paper_embeddings` is re-embedded for a winner that lacks a vector by the daily job, so the
# loser's rows of both go with the loser's cascade. `taste_events` and `feed_impressions` are NOT
# here: surrogate keys, so the generic pass re-points them with a plain UPDATE.
TASTE_NOT_GENERIC = {"taste_negatives", "taste_cards", "paper_embeddings"}


def merge_taste_negatives(cur, loser: str, winner: str) -> dict[str, int]:
    cur.execute("DELETE FROM taste_negatives l WHERE l.paper_id = %s AND EXISTS "
                "(SELECT 1 FROM taste_negatives w WHERE w.paper_id = %s AND w.version = l.version)",
                [loser, winner])
    dropped = cur.rowcount
    cur.execute("UPDATE taste_negatives SET paper_id = %s WHERE paper_id = %s", [winner, loser])
    return {"moved": cur.rowcount, "dropped_as_duplicate": dropped}


def merge(conn, loser: str, winner: str, reason: str | None, dry: bool = False) -> dict:
    cur = conn.cursor()
    # Fail fast instead of queueing behind a live user's push and holding their counter lock.
    cur.execute("SET LOCAL lock_timeout = '5s'")
    if loser == winner:
        raise SystemExit("[abort] loser and winner are the same id")

    cur.execute(f"SELECT doi, {IN_SCOPE}, corpus_tier FROM papers p WHERE p.id = %s", [winner])
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"[abort] winner {winner} does not exist in papers")
    winner_in_scope, winner_tier = row[1], row[2]
    cur.execute(f"SELECT doi, openalex_id, arxiv_id, {IN_SCOPE}, vetted, corpus_tier FROM papers p "
                "WHERE p.id = %s", [loser])
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"[abort] loser {loser} does not exist in papers")
    loser_doi, loser_oa, loser_arxiv, loser_in_scope, loser_vetted, loser_tier = row

    # OQ-2 / G-041: the merge leaves a 308 behind, so the winner must actually be reachable.
    # An in-scope loser merged into an out-of-scope winner would redirect every old URL into a
    # 404 (the A5 scope gate hides out-of-scope papers from the public surface) — strictly worse
    # than the pre-merge state. Two out-of-scope papers may merge freely: neither had a live URL.
    if loser_in_scope and not winner_in_scope:
        raise SystemExit(f"[abort] winner {winner} is out of scope but loser {loser} is in scope — "
                         f"the redirect would land on a 404 (A5 scope gate). Pick the in-scope "
                         f"paper as the winner, or bring the winner in scope first.")

    # The tier policy (ledger task lp3-import-tables, change 2): a core loser never merges into a
    # non-core winner. Only 'core' papers have a page and appear in search, and deduplicate.py keeps
    # the paper with more data, so a staged capture could otherwise win over an ingested paper and
    # leave its page redirecting to a page that does not exist.
    if loser_tier == "core" and winner_tier != "core":
        raise SystemExit(f"[abort] loser {loser} has corpus_tier '{loser_tier}' but winner {winner} has "
                         f"corpus_tier '{winner_tier}' — a core paper may not merge into a non-core one. "
                         f"Pick the core paper as the winner.")

    tables = referencing_tables(cur)

    # RESTRICT is the schema's own marker for "user data too precious to cascade" (039's
    # library_items.paper_id). Such a table's rows may not be run through the generic
    # delete-colliding policy — it would silently destroy user data — so it must have a
    # bespoke handler or the merge stops here. NO ACTION ('a') is deliberately NOT a trigger:
    # it is what Postgres records for a bare `REFERENCES papers(id)`, i.e. a default, not an
    # intent — treating it as one would abort every merge the moment such a table appears.
    actions = paper_fk_actions(cur)
    unguarded = sorted(t for t in tables
                       if actions.get(t) == "r" and t not in BESPOKE_MERGERS)
    if unguarded:
        raise SystemExit(f"[abort] ON DELETE RESTRICT on paper_id with no bespoke merge pass: "
                         f"{unguarded} — a generic merge would destroy user data (A-D3-1)")
    for t in tables:
        if actions.get(t) == "a":
            print(f"  [warn] {t}.paper_id FK is NO ACTION — merged generically; if it holds user "
                  f"data it needs a bespoke pass (A-D3-1)")

    stats: dict[str, dict[str, int]] = {}
    # Re-point FK-less tables FIRST (ADR D3 step 1): if anything below fails, the tables with
    # no DB-side protection are already consistent.
    ordered = sorted(tables, key=lambda t: (t not in FK_LESS_ALLOWLIST, t))

    # Taste negatives before the generic pass (contracts v1.2); taste_cards and paper_embeddings
    # are skipped by it (TASTE_NOT_GENERIC).
    if "taste_negatives" in tables:
        s = merge_taste_negatives(cur, loser, winner)
        if any(s.values()):
            stats["taste_negatives"] = s

    for t in ordered:
        if t in TASTE_NOT_GENERIC:
            continue
        if t in BESPOKE_MERGERS:
            s = BESPOKE_MERGERS[t](cur, loser, winner)
            if any(v for k, v in s.items() if k != "users"):
                stats[t] = s
            continue
        deleted = 0
        for cols in collision_keys(cur, t):
            others = [c for c in cols if c != "paper_id"]
            if others:
                pred = " AND ".join(f"w.{c} IS NOT DISTINCT FROM l.{c}" for c in others)
                sql = (f'DELETE FROM "{t}" l WHERE l.paper_id = %s AND EXISTS '
                       f'(SELECT 1 FROM "{t}" w WHERE w.paper_id = %s AND {pred})')
                cur.execute(sql, [loser, winner])
            else:
                # key is exactly (paper_id): at most one row per paper; winner's row wins
                cur.execute(f'DELETE FROM "{t}" l WHERE l.paper_id = %s AND EXISTS '
                            f'(SELECT 1 FROM "{t}" w WHERE w.paper_id = %s)', [loser, winner])
            deleted += cur.rowcount
        cur.execute(f'UPDATE "{t}" SET paper_id = %s WHERE paper_id = %s', [winner, loser])
        moved = cur.rowcount
        if moved or deleted:
            stats[t] = {"moved": moved, "dropped_as_duplicate": deleted}

    # denormalized counters on the winner
    for col, src in DENORM_COUNTS:
        cur.execute(f'UPDATE papers SET {col} = (SELECT count(*) FROM "{src}" WHERE paper_id = %s) '
                    f'WHERE id = %s', [winner, winner])

    # redirect + chain compression: anything that pointed at the loser now points at the winner.
    # The redirect row carries ALL THREE of the loser's dedup keys (042), not just the URL half:
    # `upsert_paper()` dedups by openalex_id / doi / arxiv_id, so a key that survives nowhere is a
    # key on which the next re-ingest RE-CREATES the loser as a live duplicate. The DELETE below is
    # the last moment those strings exist.
    #
    # A key can legitimately reach this line twice (merge D away -> D re-ingested as a new row ->
    # merged away again). The stale row is NOT deleted — its `old_id` is still a live redirect key
    # for anyone holding the first loser's UUID URL — it only surrenders the KEY, so exactly one
    # redirect row owns a given key. (Same reasoning as 041; each key compared exactly as the
    # `papers` lookup compares it: doi through normalize_doi(), the other two by equality.)
    released = {}
    for col, val, pred in (("old_doi", loser_doi, "normalize_doi(old_doi) = normalize_doi(%s)"),
                           ("old_openalex_id", loser_oa, "old_openalex_id = %s"),
                           ("old_arxiv_id", loser_arxiv, "old_arxiv_id = %s")):
        if not val:
            continue
        cur.execute(f"UPDATE paper_redirects SET {col} = NULL "
                    f" WHERE {col} IS NOT NULL AND old_id <> %s AND {pred}", [loser, val])
        if cur.rowcount:
            released[col] = cur.rowcount

    cur.execute("INSERT INTO paper_redirects (old_id, new_id, reason, old_doi, old_openalex_id, "
                "                             old_arxiv_id) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (old_id) DO UPDATE SET new_id = EXCLUDED.new_id, "
                "old_doi = EXCLUDED.old_doi, old_openalex_id = EXCLUDED.old_openalex_id, "
                "old_arxiv_id = EXCLUDED.old_arxiv_id",
                [loser, winner, reason, loser_doi, loser_oa, loser_arxiv])
    cur.execute("UPDATE paper_redirects SET new_id = %s WHERE new_id = %s", [winner, loser])
    chains = cur.rowcount

    cur.execute("DELETE FROM papers WHERE id = %s", [loser])
    deleted = cur.rowcount

    # KEY ADOPTION (owner decision 2026-07-11). A winner that has NO openalex_id / arxiv_id of its
    # own inherits the loser's, so every future re-ingest of that record hits `papers` directly
    # instead of hopping through the redirect table forever. Strictly additive: only ever NULL ->
    # value, and only AFTER the loser row is gone (both columns carry a UNIQUE partial index, so
    # adopting while the loser still holds the key would violate it).
    #
    # `doi` is deliberately NOT adopted: it is the paper's public URL identity — makePaperSlug(doi,
    # id) would flip the winner's canonical URL from its UUID to the loser's DOI path. The redirect
    # row's old_doi already covers that case, and the app's read path resolves it with a 308.
    # `vetted` is a disjunction, never a downgrade: the winner keeps its own true, and takes the
    # loser's (ledger task capture-drain-ingest, design item 10). `vetted` means "a writer
    # confirmed this work exists" (docs/ledger/navigation-scope.md), a fact that survives the
    # merge — and a merge that cleared it would take the surviving paper's page away.
    if loser_vetted:
        cur.execute("UPDATE papers SET vetted = true WHERE id = %s AND NOT vetted", [winner])
        if cur.rowcount:
            print(f"  [vetted]  winner {winner} takes the loser's vetted = true")

    adopted = {}
    for col, val in (("openalex_id", loser_oa), ("arxiv_id", loser_arxiv)):
        if not val:
            continue
        cur.execute(f"UPDATE papers SET {col} = %s WHERE id = %s AND {col} IS NULL", [val, winner])
        if cur.rowcount:
            adopted[col] = val

    stats["_meta"] = {"chains_compressed": chains, "papers_deleted": deleted,
                      "redirect_keys": {k: v for k, v in
                                        (("doi", loser_doi), ("openalex_id", loser_oa),
                                         ("arxiv_id", loser_arxiv)) if v},
                      "stale_keys_released": released or None,
                      "keys_adopted_by_winner": adopted or None}

    if dry:
        conn.rollback()
        print("[dry-run] rolled back")
    else:
        conn.commit()
    cur.close()
    return stats


def _orphans(conn, tables) -> dict[str, int]:
    """Rows whose non-null paper_id names no paper, per table (only tables with any)."""
    cur = conn.cursor()
    out = {}
    for t in tables:
        cur.execute(f'SELECT count(*) FROM "{t}" x WHERE x.paper_id IS NOT NULL AND NOT EXISTS '
                    f'(SELECT 1 FROM papers p WHERE p.id = x.paper_id)')
        n = cur.fetchone()[0]
        if n:
            out[t] = n
    cur.close()
    return out


def orphan_report(conn) -> dict[str, int]:
    """Zero-orphan assertion across every referencing table except ORPHANS_BY_DESIGN (the G-038
    closure artifact).

    `x.paper_id IS NOT NULL` is load-bearing since 039: `library_items.paper_id` is NULLABLE
    (external non-corpus items, and the merge's own released tombstones). Without the guard,
    NOT EXISTS treats every NULL as unmatched and reports legitimate rows as orphans.
    """
    cur = conn.cursor()
    tables = [t for t in referencing_tables(cur) if t not in ORPHANS_BY_DESIGN]
    cur.close()
    return _orphans(conn, tables)


def orphans_by_design(conn) -> dict[str, int]:
    """The rows of ORPHANS_BY_DESIGN whose paper is gone, counted apart from `orphan_report`."""
    cur = conn.cursor()
    tables = [t for t in referencing_tables(cur) if t in ORPHANS_BY_DESIGN]
    cur.close()
    return _orphans(conn, tables)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("loser", nargs="?")
    ap.add_argument("winner", nargs="?")
    ap.add_argument("--reason")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not (a.loser and a.winner):
        ap.error("loser and winner ids are required")

    # A merge competes with live user pushes for the per-user counter row lock. Losing that
    # race is transient, so retry the whole transaction; NEVER batch several paper merges into
    # one transaction, which would hold many users' counters (and block their pushes) at once.
    for attempt in range(3):
        conn = connect()
        try:
            stats = merge(conn, a.loser, a.winner, a.reason, dry=a.dry_run)
            break
        except (psycopg2.errors.DeadlockDetected, psycopg2.errors.LockNotAvailable,
                psycopg2.errors.SerializationFailure) as e:
            conn.rollback()
            conn.close()
            if attempt == 2:
                raise SystemExit(f"[abort] contended with live writers 3x: {e.__class__.__name__}")
            print(f"  [retry] {e.__class__.__name__} — attempt {attempt + 2}/3")
            time.sleep(1 + attempt)

    for t, s in sorted(stats.items()):
        print(f"  {t:<28} {s}")
    orph = orphan_report(conn)
    print(f"[orphans] {orph or 'none — 0 across all referencing tables'}")
    by_design = orphans_by_design(conn)
    if by_design:
        print(f"[orphans by design, not a failure] {by_design}")
    conn.close()
    return 1 if orph else 0


if __name__ == "__main__":
    raise SystemExit(main())
