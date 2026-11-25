"""Classify new/untagged papers with the production ModernBERT student (B3).

Deployable anywhere that reaches Neon (laptop OR GitHub Actions) — unlike Kimi
(HPC-internal only). The existing corpus is Kimi-labeled (0.75) via paper_subfields;
this student (~0.70) is the FORWARD-stream labeler for genuinely new papers the
daily cloud cron can't send to Kimi.

Loads a checkpoint dir saved by train_student_prod.py:
    <ckpt>/                 model + tokenizer (HF save_pretrained)
    <ckpt>/student_meta.json  {labels[], threshold, maxlen, use_journal, ...}

For each target paper: build `title [| journal] [| keywords: <kw>]` + abstract
(keywords pulled from paper_external_labels if present — empty for brand-new papers),
sigmoid(logits) >= threshold (argmax fallback) → label slugs → paper_tags rows
(paper_id, tag_id, confidence=score, source='student') via INSERT ... ON CONFLICT
DO NOTHING (insert-if-absent — never overwrites another source's existing tag).

Targets papers with NO paper_tags row at all (same filter as classify_new_papers.py),
so it only touches genuinely-untagged papers — never re-labels Kimi/embedding-tagged ones
(the DO NOTHING keeps that guarantee airtight even if a paper gets tagged mid-inference).

Chunked writes (ledger task classify-timeout-fix, 2026-11-24). The selection is newest first and
is processed CHUNK papers at a time: inference, then the chunk's rows are written and committed
before the next chunk's inference starts, so a run that stops early (time budget, job timeout,
crash) keeps every chunk it finished. Before each chunk's write, papers that acquired any tag since
the selection are dropped from the chunk, so no paper gets both a student tag and another writer's
tag from this run. `--max-minutes M` is a time budget measured from the script's start: before every
chunk, the first included (and before the model loads), the run stops when the elapsed time plus the
previous chunk's duration (FIRST_CHUNK_MIN for the first) would exceed M. At the end the script
prints `[student] remaining R` and `[student] oldest_hours H` (the unlabelled backlog left in the
selection, without --limit, and the age of its oldest paper) and appends both to GITHUB_OUTPUT when
that is set.

Deps: torch, transformers, numpy, psycopg2 (all already present via sentence-transformers).

Usage:
  python classify_student.py --ckpt student_prod_v1            # all untagged
  python classify_student.py --ckpt student_prod_v1 --limit 500
  python classify_student.py --ckpt student_prod_v1 --max-minutes 45
  python classify_student.py --ckpt student_prod_v1 --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from db import get_client, execute, resolve_paper_rows  # noqa: E402

load_dotenv(dotenv_path=HERE.parent / ".env.local")

KW_SOURCES = ("s2_field", "s2_field_fine", "openalex_keyword")
KW_ORDER = {"s2_field": 0, "s2_field_fine": 1, "openalex_keyword": 2}
BATCH = 64
CHUNK = 128             # papers per inference-then-write unit
FIRST_CHUNK_MIN = 4.0   # assumed duration of the first chunk for the budget check, in minutes

# The clock the time budget reads (seconds). A module attribute so the tests can replace it.
now = time.monotonic

# The student's selection: untagged, core, with an abstract, in scope. Shared by the selection and
# the remaining count so the two cannot drift apart.
SELECTION_WHERE = """
        WHERE NOT EXISTS (SELECT 1 FROM paper_tags pt WHERE pt.paper_id = p.id)
          AND p.corpus_tier = 'core'
          AND COALESCE(LENGTH(TRIM(p.abstract)), 0) >= 1
          -- Scope gate (A5): only classify in-scope papers (linked to an in-scope
          -- discipline). Stops minting new wrong tags on out-of-scope arXiv/physics.
          AND EXISTS (SELECT 1 FROM paper_disciplines pd JOIN disciplines d
                      ON d.id = pd.discipline_id
                      WHERE pd.paper_id = p.id AND d.in_scope)
"""


def selection_sql(limit) -> str:
    """Papers with no paper_tags row at all (genuinely unclassified), newest first."""
    sql = """
        SELECT p.id::text AS id, COALESCE(p.title,'') AS title,
               COALESCE(p.abstract,'') AS abstract, COALESCE(p.source,'') AS journal
        FROM papers p""" + SELECTION_WHERE + """
        ORDER BY p.created_at DESC, p.id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return sql


def get_untagged_papers(conn, limit):
    return execute(conn, selection_sql(limit), [])


def count_remaining(conn) -> tuple[int, float]:
    """(papers left in the selection without LIMIT, age in hours of the oldest of them; 0 if none)."""
    r = execute(conn, """
        SELECT count(*) AS n,
               COALESCE(EXTRACT(EPOCH FROM (now() - min(p.created_at))) / 3600.0, 0) AS oldest_hours
        FROM papers p""" + SELECTION_WHERE, [])[0]
    return int(r["n"]), round(float(r["oldest_hours"]), 1)


def get_keywords(conn, pids):
    """Per-paper external-label keyword string (s2 + openalex), '' if none —
    mirrors export_student_data.py's keyword assembly so input matches training."""
    if not pids:
        return {}
    rows = execute(conn, """
        SELECT paper_id::text AS pid, source, label_name
        FROM paper_external_labels
        WHERE paper_id = ANY(%s::uuid[]) AND source = ANY(%s)
    """, [pids, list(KW_SOURCES)])
    rows.sort(key=lambda r: (r["pid"], KW_ORDER.get(r["source"], 9), (r["label_name"] or "").lower()))
    out, seen = {}, {}
    for r in rows:
        pid, name = r["pid"], (r["label_name"] or "").strip()
        if not name:
            continue
        s = seen.setdefault(pid, set())
        if name.lower() in s:
            continue
        s.add(name.lower())
        out.setdefault(pid, []).append(name)
    return {pid: ", ".join(v) for pid, v in out.items()}


def get_tag_id_map(conn):
    # subfield tags only — the student emits subfield slugs; filtering by type avoids
    # mapping a slug to the wrong tag_id should a non-subfield tag ever share its name.
    return {r["name"]: r["id"]
            for r in execute(conn, "SELECT id, name FROM tags WHERE type = 'subfield'", [])}


def load_model(ckpt: Path, maxlen: int):
    """Load the checkpoint; return predict(seg1_list, abstracts) -> probs (n x labels)."""
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    dev = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
    tok = AutoTokenizer.from_pretrained(ckpt)
    model = AutoModelForSequenceClassification.from_pretrained(ckpt).to(dev).eval()

    def predict(seg1, abstracts):
        enc = tok(seg1, abstracts, padding=True, truncation=True, max_length=maxlen,
                  return_tensors="pt").to(dev)
        with torch.no_grad():
            return torch.sigmoid(model(**enc).logits).float().cpu().numpy()
    return predict


def label_chunk(chunk, predict, labels, thr, build_seg1):
    """Inference over one chunk in batches of BATCH. Returns ([(pid, slug, score)], n_argmax)."""
    assigned, n_nolabel = [], 0
    for i in range(0, len(chunk), BATCH):
        batch = chunk[i:i + BATCH]
        probs = predict([build_seg1(p) for p in batch], [p["abstract"] for p in batch])
        for p, row in zip(batch, probs):
            sel = [(labels[j], float(row[j])) for j in range(len(labels)) if row[j] >= thr]
            if not sel:
                j = int(np.argmax(row)); sel = [(labels[j], float(row[j]))]
                n_nolabel += 1
            for slug, sc in sel:
                assigned.append((p["id"], slug, sc))
    return assigned, n_nolabel


def prepare_rows(rows):
    """Make a chunk's rows safe to write, on a fresh connection.

    The paper ids were snapshotted at the selection, before the inference. A merge landing in that
    window DELETEs the loser, so writing its id would raise an FK violation and kill the whole
    execute_values batch: re-point through paper_redirects and collapse the loser/winner rows a merge
    may have folded onto one (paper_id, tag_id). Then drop every paper that has acquired any tag since
    the selection (another writer, or the winner of a merge), so no paper gets both a student tag and
    another tag from this run. Fresh connection: Neon's pooler drops a connection left idle through
    the inference (the pre-inference handle once raised InterfaceError here and lost every row).
    """
    rc = get_client()
    try:
        rows, _ = resolve_paper_rows(rc, rows, key=(0, 1))
        pids = sorted({str(r[0]) for r in rows})
        tagged = set()
        if pids:
            tagged = {r["pid"] for r in execute(rc, """
                SELECT DISTINCT paper_id::text AS pid FROM paper_tags WHERE paper_id = ANY(%s::uuid[])
            """, [pids])}
    finally:
        rc.close()
    kept = [r for r in rows if str(r[0]) not in tagged]
    return kept, len(tagged)


def write_batch(batch):
    """Insert-if-absent one batch; fresh connection per batch (survives a mid-write drop)."""
    for attempt in range(3):
        wc = get_client()
        try:
            execute_values(wc.cursor(), """
                INSERT INTO paper_tags (paper_id, tag_id, confidence, source)
                VALUES %s
                ON CONFLICT (paper_id, tag_id) DO NOTHING
            """, batch, page_size=1000)
            wc.commit(); wc.close(); return
        except psycopg2.OperationalError:
            try: wc.close()
            except Exception: pass
            if attempt == 2:
                raise


def report_remaining() -> None:
    """Print the backlog left in the selection (fresh connection, after the writes) and emit it."""
    rc = get_client()
    try:
        remaining, oldest_hours = count_remaining(rc)
    finally:
        rc.close()
    print(f"[student] remaining {remaining}")
    print(f"[student] oldest_hours {oldest_hours}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"remaining={remaining}\noldest_hours={oldest_hours}\n")


def main(argv=None) -> int:
    t0 = now()
    ap = argparse.ArgumentParser()
    ap.add_argument("--ckpt", default="student_prod_v1", help="checkpoint dir")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--max-minutes", type=float, default=None,
                    help="time budget in minutes from the script's start; absent means none")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    ckpt = (HERE / args.ckpt) if not os.path.isabs(args.ckpt) else Path(args.ckpt)
    meta = json.loads((ckpt / "student_meta.json").read_text())
    labels = meta["labels"]; thr = meta["threshold"]; maxlen = meta["maxlen"]
    use_journal = meta.get("use_journal", False)
    budget = "none" if args.max_minutes is None else f"{args.max_minutes:g} min"
    print(f"[student] ckpt={ckpt.name} labels={len(labels)} threshold={thr:.4f} "
          f"maxlen={maxlen} use_journal={use_journal} (opus_J={meta.get('opus_jaccard')}) "
          f"chunk={CHUNK} budget={budget}")

    conn = get_client()
    papers = get_untagged_papers(conn, args.limit)
    print(f"[student] {len(papers):,} untagged papers to classify")
    if not papers:
        conn.close()
        report_remaining()
        return 0
    kw = get_keywords(conn, [p["id"] for p in papers])
    tagmap = get_tag_id_map(conn)
    conn.close()  # release before the (minutes-long) inference; Neon drops idle pooler conns

    def build_seg1(p):
        seg = p["title"]
        if use_journal and p["journal"]:
            seg += " | journal: " + p["journal"]
        k = kw.get(p["id"])
        if k:
            seg += " | keywords: " + k
        return seg

    predict = None
    prev_min = None          # duration of the previous chunk, minutes
    n_chunks = n_labelled = n_rows = n_argmax = n_dropped = 0
    missing_seen = set()
    for start in range(0, len(papers), CHUNK):
        elapsed_min = (now() - t0) / 60.0
        est = FIRST_CHUNK_MIN if prev_min is None else prev_min
        if args.max_minutes is not None and elapsed_min + est > args.max_minutes:
            print(f"[student] budget reached after {n_chunks} chunks: labelled {n_labelled} papers, "
                  f"{len(papers) - start} selected papers not started")
            break
        if predict is None:
            predict = load_model(ckpt, maxlen)
        c0 = now()
        chunk = papers[start:start + CHUNK]
        assigned, n_nolabel = label_chunk(chunk, predict, labels, thr, build_seg1)
        n_argmax += n_nolabel

        missing = sorted({slug for _, slug, _ in assigned if slug not in tagmap} - missing_seen)
        if missing:
            missing_seen.update(missing)
            print(f"[student] WARNING {len(missing)} label slugs not in tags table (skipped): {missing[:10]}")
        rows = [(pid, tagmap[slug], sc, "student") for pid, slug, sc in assigned if slug in tagmap]

        n_chunks += 1
        if args.dry_run:
            n_papers = len({r[0] for r in rows})
            print(f"[dry-run] chunk {n_chunks}: {len(chunk)} papers, would upsert {len(rows):,} "
                  f"paper_tags rows over {n_papers:,} papers ({n_nolabel} argmax)")
            for r in rows[:3]:
                print(f"  {r[0][:8]} -> {r[1]}  conf={r[2]:.3f}")
            n_labelled += n_papers
        else:
            rows, dropped = prepare_rows(rows)
            n_dropped += dropped
            for i in range(0, len(rows), 1000):
                write_batch(rows[i:i + 1000])
            n_papers = len({str(r[0]) for r in rows})
            n_labelled += n_papers
            n_rows += len(rows)
            print(f"[student] chunk {n_chunks}: {len(chunk)} papers, wrote {len(rows):,} rows over "
                  f"{n_papers:,} papers ({n_nolabel} argmax, {dropped} already tagged, skipped)")
        prev_min = (now() - c0) / 60.0

    verb = "would label" if args.dry_run else "labelled"
    print(f"[student] {n_chunks} chunks: {verb} {n_labelled:,} papers, {n_rows:,} paper_tags rows "
          f"(source='student'); {n_argmax:,} fell back to argmax; {n_dropped:,} tagged by another "
          f"writer meanwhile, skipped")
    report_remaining()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
