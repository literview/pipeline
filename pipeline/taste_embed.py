"""taste_embed.py -- the shared half of the taste pipeline (ledger task `taste-pipeline`, item 1).

The daily embed step (embed_new_papers.py), the weekly card pool (build_taste_pool.py) and the
monthly negatives version (build_taste_negatives.py) all go through this module, so the embedding
they write is one thing: MiniLM over `title + ". " + abstract` (raw), max_seq_length 256,
unit-normalised, which is how the offline evidence was produced
(docs/ledger/reports/taste-test-offline/embed.py:14,18,23).

Config. `load_config()` reads pipeline/taste_config.json, which `npx tsx lib/taste/cli/config-export.ts`
generates from TASTE_CONFIG (contracts v1.2, K5), and refuses a file whose `config` object no longer
hashes to its CONFIG_HASH (sha256 of the canonical JSON: keys sorted recursively, no whitespace).
Every script calls it first and reads `pool`, `negatives.count`, `cardSelector.nearNeighbours` and
`embedModelId` from it.

Dimension guard. `write_vectors()` asserts every vector of a batch has `embed_models.dim` entries
before writing any of it (design Section 14, "the dimension guard").

The subfield classifiers are not touched by any of this and do not read stored vectors
(classify_new_papers.py keeps its own EMBEDDING_MODEL constant).
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import numpy as np
import psycopg2.extras

from db import execute, resolve_paper_rows

PIPELINE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = PIPELINE_DIR / "taste_config.json"

EXPECTED_MODEL_ID = "all-MiniLM-L6-v2@256"   # the only model id this module knows how to produce
MODEL_NAME = "all-MiniLM-L6-v2"
MAX_SEQ_LENGTH = 256
ENCODE_BATCH = 64
WRITE_BATCH = 500

# navigablePaper, copied verbatim from lib/queries.ts (with migration 067's core condition).
def navigable_paper(alias: str = "p") -> str:
    return (f"({alias}.vetted AND {alias}.corpus_tier = 'core' AND EXISTS (SELECT 1 FROM paper_disciplines pd JOIN disciplines d "
            f"ON d.id = pd.discipline_id WHERE pd.paper_id = {alias}.id AND d.in_scope))")


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_config(path: Path | str = CONFIG_FILE) -> dict:
    """The `config` object of taste_config.json, after the hash check. Raises on a missing file,
    a hash mismatch, or an embedModelId this module does not produce."""
    path = Path(path)
    if not path.exists():
        raise RuntimeError(f"{path} missing: run `npx tsx lib/taste/cli/config-export.ts` in LiterView")
    doc = json.loads(path.read_text(encoding="utf-8"))
    cfg = doc["config"]
    got = canonical_hash(cfg)
    if got != doc.get("CONFIG_HASH"):
        raise RuntimeError(f"{path.name}: config hashes to {got}, file says {doc.get('CONFIG_HASH')} "
                           f"(edited by hand, or stale against TASTE_CONFIG)")
    if cfg.get("embedModelId") != EXPECTED_MODEL_ID:
        raise RuntimeError(f"{path.name}: embedModelId {cfg.get('embedModelId')!r}, this pipeline "
                           f"produces {EXPECTED_MODEL_ID!r}")
    cfg["_version"] = doc.get("CONFIG_VERSION")
    return cfg


MODEL_ID = load_config()["embedModelId"]


def paper_text(title, abstract) -> str:
    return (title or "") + ". " + (abstract or "")


_MODEL = None


def get_model():
    global _MODEL
    if _MODEL is None:
        from sentence_transformers import SentenceTransformer
        _MODEL = SentenceTransformer(MODEL_NAME, device="cpu")
        _MODEL.max_seq_length = MAX_SEQ_LENGTH
    return _MODEL


def encode(texts: list[str], model=None) -> np.ndarray:
    model = model or get_model()
    return np.asarray(model.encode(texts, batch_size=ENCODE_BATCH, normalize_embeddings=True,
                                   show_progress_bar=False), dtype=np.float32)


def tables_present(conn, *names: str) -> bool:
    for n in names:
        rows = execute(conn, "SELECT to_regclass(%s) IS NOT NULL AS present", [f"public.{n}"])
        if not rows or not rows[0]["present"]:
            return False
    return True


def load_dim(conn) -> int:
    rows = execute(conn, "SELECT dim FROM embed_models WHERE model_id = %s", [MODEL_ID])
    if not rows:
        raise RuntimeError(f"embed_models has no row for {MODEL_ID}: migration 060 incomplete")
    return int(rows[0]["dim"])


def vec_literal(vec) -> str:
    return "[" + ",".join(f"{float(x):.7g}" for x in vec) + "]"


def parse_vec(text: str) -> np.ndarray:
    return np.array([float(x) for x in text.strip()[1:-1].split(",")], dtype=np.float32)


INSERT_SQL = ("INSERT INTO paper_embeddings (paper_id, model_id, vec) VALUES %s "
              "ON CONFLICT (paper_id, model_id) DO NOTHING")


def write_vectors(conn, rows: list[tuple], dim: int) -> dict:
    """rows: (paper_id, vector). Asserts every vector has `dim` entries before writing any, re-points
    ids through resolve_paper_rows, then inserts WRITE_BATCH rows per transaction with
    flush_batch's retry (three attempts, rollback and back off between them). Returns counts."""
    for pid, vec in rows:
        if len(vec) != dim:
            raise ValueError(f"vector for {pid} has {len(vec)} entries, embed_models.dim is {dim}")
    resolved, stats = resolve_paper_rows(conn, [(str(p), v) for p, v in rows], key=(0,))
    conn.commit()   # end the read transaction resolve_paper_rows opened
    written = 0
    for s in range(0, len(resolved), WRITE_BATCH):
        chunk = [(p, MODEL_ID, vec_literal(v)) for p, v in resolved[s:s + WRITE_BATCH]]
        for attempt in range(3):
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, INSERT_SQL, chunk,
                                                   template="(%s, %s, %s::halfvec)",
                                                   page_size=WRITE_BATCH)
                    n = cur.rowcount
                conn.commit()
                written += n
                break
            except Exception as e:
                conn.rollback()
                if attempt < 2:
                    print(f"  Retry {attempt + 1}: {e}")
                    time.sleep(2 * (attempt + 1))
                else:
                    raise
    return {"written": written, "dropped": stats["dropped"], "moved": stats["moved"],
            "collapsed": stats["collapsed"]}


def embed_rows(conn, papers: list[dict], dim: int, model=None) -> dict:
    """papers: dicts with id, title, abstract. Encodes and writes in WRITE_BATCH chunks, so each
    chunk is committed before the next is encoded."""
    out = {"written": 0, "dropped": 0, "moved": 0, "collapsed": 0}
    for s in range(0, len(papers), WRITE_BATCH):
        chunk = papers[s:s + WRITE_BATCH]
        vecs = encode([paper_text(p["title"], p["abstract"]) for p in chunk], model)
        st = write_vectors(conn, [(p["id"], v) for p, v in zip(chunk, vecs)], dim)
        for k in out:
            out[k] += st[k]
    return out


def embed_missing(conn, ids: list[str], dim: int, model=None) -> dict:
    """Embed every paper in `ids` that has no paper_embeddings row for MODEL_ID."""
    if not ids:
        return {"written": 0, "dropped": 0, "moved": 0, "collapsed": 0, "missing": 0}
    papers = execute(conn, """
        SELECT p.id::text AS id, p.title, p.abstract FROM papers p
         WHERE p.id = ANY(%s::uuid[])
           AND NOT EXISTS (SELECT 1 FROM paper_embeddings e
                            WHERE e.paper_id = p.id AND e.model_id = %s)
         ORDER BY p.id""", [list(ids), MODEL_ID])
    conn.commit()
    st = embed_rows(conn, papers, dim, model) if papers else {"written": 0, "dropped": 0, "moved": 0,
                                                             "collapsed": 0}
    st["missing"] = len(papers)
    return st


def load_vectors(conn, ids: list[str]) -> dict[str, np.ndarray]:
    rows = execute(conn, """
        SELECT paper_id::text AS id, vec::text AS vec FROM paper_embeddings
         WHERE model_id = %s AND paper_id = ANY(%s::uuid[])""", [MODEL_ID, list(ids)])
    return {r["id"]: parse_vec(r["vec"]) for r in rows}
