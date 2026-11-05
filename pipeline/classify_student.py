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

Deps: torch, transformers, numpy, psycopg2 (all already present via sentence-transformers).

Usage:
  python classify_student.py --ckpt student_prod_v1            # all untagged
  python classify_student.py --ckpt student_prod_v1 --limit 500
  python classify_student.py --ckpt student_prod_v1 --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
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


def get_untagged_papers(conn, limit):
    """Papers with no paper_tags row at all (genuinely unclassified)."""
    sql = """
        SELECT p.id::text AS id, COALESCE(p.title,'') AS title,
               COALESCE(p.abstract,'') AS abstract, COALESCE(p.source,'') AS journal
        FROM papers p
        WHERE NOT EXISTS (SELECT 1 FROM paper_tags pt WHERE pt.paper_id = p.id)
          AND p.corpus_tier = 'core'
          AND COALESCE(LENGTH(TRIM(p.abstract)), 0) >= 1
          -- Scope gate (A5): only classify in-scope papers (linked to an in-scope
          -- discipline). Stops minting new wrong tags on out-of-scope arXiv/physics.
          AND EXISTS (SELECT 1 FROM paper_disciplines pd JOIN disciplines d
                      ON d.id = pd.discipline_id
                      WHERE pd.paper_id = p.id AND d.in_scope)
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return execute(conn, sql, [])


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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ckpt", default="student_prod_v1", help="checkpoint dir")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ckpt = (HERE / args.ckpt) if not os.path.isabs(args.ckpt) else Path(args.ckpt)
    meta = json.loads((ckpt / "student_meta.json").read_text())
    labels = meta["labels"]; thr = meta["threshold"]; maxlen = meta["maxlen"]
    use_journal = meta.get("use_journal", False)
    print(f"[student] ckpt={ckpt.name} labels={len(labels)} threshold={thr:.4f} "
          f"maxlen={maxlen} use_journal={use_journal} (opus_J={meta.get('opus_jaccard')})")

    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    dev = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
    tok = AutoTokenizer.from_pretrained(ckpt)
    model = AutoModelForSequenceClassification.from_pretrained(ckpt).to(dev).eval()

    conn = get_client()
    papers = get_untagged_papers(conn, args.limit)
    print(f"[student] {len(papers):,} untagged papers to classify")
    if not papers:
        conn.close(); return 0
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

    assigned = []  # (pid, slug, score)
    n_nolabel = 0
    for i in range(0, len(papers), BATCH):
        chunk = papers[i:i + BATCH]
        enc = tok([build_seg1(p) for p in chunk], [p["abstract"] for p in chunk],
                  padding=True, truncation=True, max_length=maxlen, return_tensors="pt").to(dev)
        with torch.no_grad():
            probs = torch.sigmoid(model(**enc).logits).float().cpu().numpy()
        for p, row in zip(chunk, probs):
            sel = [(labels[j], float(row[j])) for j in range(len(labels)) if row[j] >= thr]
            if not sel:
                j = int(np.argmax(row)); sel = [(labels[j], float(row[j]))]
                n_nolabel += 1
            for slug, sc in sel:
                assigned.append((p["id"], slug, sc))

    n_papers = len(set(a[0] for a in assigned))
    print(f"[student] {len(assigned):,} (paper,label) assignments over {n_papers:,} papers "
          f"(avg {len(assigned)/max(n_papers,1):.2f}); {n_nolabel:,} fell back to argmax")

    missing = sorted({slug for _, slug, _ in assigned if slug not in tagmap})
    if missing:
        print(f"[student] WARNING {len(missing)} label slugs not in tags table (skipped): {missing[:10]}")

    rows = [(pid, tagmap[slug], sc, "student") for pid, slug, sc in assigned if slug in tagmap]
    if args.dry_run:
        print(f"[dry-run] would upsert {len(rows):,} paper_tags rows (source='student'). Sample:")
        for r in rows[:5]:
            print(f"  {r[0][:8]} -> {r[1]}  conf={r[2]:.3f}")
        return 0

    # The paper ids in `rows` were snapshotted before the (minutes-long) inference. A merge landing
    # in that window DELETEs the loser, so writing its id now would raise an FK violation and kill
    # the whole execute_values chunk. Re-point through paper_redirects immediately before the write,
    # and collapse the loser/winner rows a merge may have folded onto one (paper_id, tag_id).
    #
    # On a FRESH connection: `conn` is deliberately closed above, before inference, so that Neon's
    # pooler never sees it idle. Passing that closed handle here raised
    # InterfaceError("connection already closed") and discarded every assignment. This resolve step
    # was added 2026-07-11 (merge-safety) between the pre-inference close and the per-batch
    # reconnect that write_batch already did — the one path left holding the dead handle.
    rc = get_client()
    try:
        rows, _ = resolve_paper_rows(rc, rows, key=(0, 1))
    finally:
        rc.close()

    # Fresh connection per batch — Neon drops the idle pooler connection during the
    # minutes-long inference, and per-batch reconnect also survives a mid-write drop.
    def write_batch(batch):
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

    for i in range(0, len(rows), 1000):
        write_batch(rows[i:i + 1000])
    print(f"[student] upserted {len(rows):,} paper_tags rows (source='student')")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
