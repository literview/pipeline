"""text_norm.py — strict character-reference decoding, shared by ingest guards and backfill.

`html.unescape` alone is wrong for publisher metadata: it follows the HTML5 list, which
also decodes legacy references with no trailing semicolon. Verified 2026-09-01:

    html.unescape("policy&regulation")  -> "policy®ulation"   (bare "&reg" ate the "reg")
    html.unescape("if&notable")         -> "if¬able"          (bare "&not" ate the "not")
    html.unescape("a&copyright b")      -> "a©right b"        (bare "&copy" ate the "copy")

Each of those inputs is an ordinary ampersand followed by ordinary words ("policy & regulation",
"if & notable", "a & copyright b" with a missing space), not an escaped entity — but the HTML5
legacy list treats "&reg", "&not", "&copy" as complete references and silently eats the letters
that follow. `decode_refs` decodes ONLY well-formed references: numeric (`&#38;`, `&#x26;`) or
named with a trailing semicolon (`&amp;`, `&lt;`), so `R&amp;D` -> `R&D` and `p&lt;0.05` -> `p<0.05`
while `policy&regulation` passes through unchanged. This is the same well-formed definition used to
produce the 2026-09-01 1,632-row measurement in docs/MARKUP_POLICY_WORKORDER.md, so it needs no
restatement there.
"""
from __future__ import annotations

import html
import re

STRICT = re.compile(r'&(#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[a-zA-Z][a-zA-Z0-9]{1,31});')


def decode_refs(s: str) -> str:
    """Decode well-formed HTML character references in `s`; leave everything else untouched.

    `None` in, `None` out (write sites pass optional title/abstract values through unchanged).
    """
    if s is None:
        return None
    return STRICT.sub(lambda m: html.unescape(m.group(0)), s)


# --- Publisher-extraction repairs, measured against the live corpus 2026-09-02 ---
#
# Three defects that DECISIONS 2026-09-01 recorded and this pass re-measured. The
# re-measurement corrected the record in three places, and each correction changed a rule:
#
#   (a) U+21B5 does NOT uniformly stand for a lost "ff" ligature. Of its six occurrences
#       across five rows, four are the ligature ("o↵ers", "e↵ect", "di ↵ er") and two are
#       line breaks ("↵*Corresponding author", "↵† Miguel Sarzosa"). A blanket substitution
#       would corrupt the latter two, so the rule reads its neighbours.
#   (b) The ligature figure 273 was a sum of three per-codepoint row counts, not distinct
#       rows; the true figure is 176 rows, and U+FB03 (ffi) is present although the earlier
#       note listed only FB00/FB01/FB02. More consequentially, 141 of those 176 rows carry a
#       space beside the ligature ("signi ﬁ cant"), so decomposing the character alone leaves
#       most of them unsearchable.
#   (c) The Springer LaTeX block is not a leading preamble but an inline injection, and in
#       all seven spans across four rows the equivalent plain text ("N=45", "91%",
#       "r=.93-1.00", "r-t", an arrow) already precedes it. The span is duplication.
#
# Dry run over all 184 affected rows: every one changed, zero residual defects, and the 156
# that grew are a one-character ligature becoming two letters.

LIGATURES = {
    "ﬀ": "ff",
    "ﬁ": "fi",
    "ﬂ": "fl",
    "ﬃ": "ffi",
    "ﬄ": "ffl",
    "ﬅ": "st",
    "ﬆ": "st",
}

_LIG_CLASS = "".join(LIGATURES)

# Anchored on both ends and non-greedy: a `\documentclass` with no following
# `\end{document}` is left alone rather than eating the rest of the abstract.
_LATEX_SPAN = re.compile(r"\\documentclass.*?\\end\{document\}", re.DOTALL)

# Letters on both sides (one optional space each) means the dropped ligature; anything else
# means the line break the glyph actually denotes.
_RETURN_AS_FF = re.compile(r"(?<=[A-Za-z]) ?\u21b5 ?(?=[A-Za-z])")
_RETURN_AS_BREAK = re.compile(r"\s*\u21b5\s*")

# A space on BOTH sides split one word ("signi ﬁ cant") and the spaces go with the
# decomposition. A space on ONE side is a real word boundary ("and ﬁnance", "staﬀ members")
# and only the character is replaced. All 63 occurrences of the two-sided case were listed
# and read individually on 2026-09-02: no false positives.
_LIG_SPLIT_WORD = re.compile(rf"(?<=[A-Za-z]) ([{_LIG_CLASS}]) (?=[A-Za-z])")
_LIG_ANY = re.compile(rf"[{_LIG_CLASS}]")


def normalize_text(s: str) -> str:
    """Repair the three publisher-extraction defects above; leave everything else alone.

    `None` in, `None` out. Idempotent, unlike decode_refs(): every rule consumes the
    character or span it matches, so a second application is a no-op.

    Runs AFTER decode_refs() at the write choke point. No row in the corpus currently
    carries a ligature or the return symbol as a character reference (checked 2026-09-02,
    zero rows), so that ordering is defensive rather than load-bearing; it costs nothing and
    a publisher emitting `&#64257;` would otherwise slip past this pass.
    """
    if s is None:
        return None
    s = _LATEX_SPAN.sub("", s)
    s = _RETURN_AS_FF.sub("ff", s)
    s = _RETURN_AS_BREAK.sub(" ", s)
    s = _LIG_SPLIT_WORD.sub(lambda m: LIGATURES[m.group(1)], s)
    s = _LIG_ANY.sub(lambda m: LIGATURES[m.group(0)], s)
    return s


# --- Publisher previews posing as abstracts, measured against the live corpus 2026-10-22 ---
#
# Atypon publishers (Taylor & Francis 10.1080, Academy of Management 10.5465) put a 200-character
# preview in <meta name="dc.Description">: the opening of the abstract, or of the body when the
# item has none (a book review, an editorial), cut and ended with "...". OpenAlex and Semantic
# Scholar ingest that meta as the abstract, and so did cdp_drain's meta fallback. Of 195,880
# papers, 1,079 stored one: 1,078 exactly 200 characters, one Elsevier row at 201; none came from
# CrossRef. PREVIEW_SQL is the same test for queries.
PREVIEW_MAX_LEN = 205
PREVIEW_SQL = (r"(length(regexp_replace(abstract, '^\s+|\s+$', '', 'g')) <= 205"
               r" AND abstract ~ '(\.\.\.|…)\s*$')")


def is_truncated_preview(s: str | None) -> bool:
    """True when `s` is a publisher preview, not an abstract: at most 205 characters once
    stripped, ending in "..." or "…". `None` and empty strings are not previews."""
    if not s:
        return False
    t = s.strip()
    return len(t) <= PREVIEW_MAX_LEN and (t.endswith("...") or t.endswith("…"))


# --- Book-review titles whose citation names lost their separators, measured 2026-10-25 ---
#
# SAGE and Wiley title a book review with the reviewed book's citation, and the citation's names
# were JATS elements (<surname>Gehman</surname><given-names>J.</given-names>) whose tags were
# removed with no separator before the deposit reached Crossref. Crossref, OpenAlex and our rows
# all hold the same string ("GehmanJ.JarzabkowskiP.LangleyA.", "By David T.Buckley"); nothing on
# our side produced it. SAGE's one corrected redeposit (10.1177/01979183251400248, "RyanL., 2023."
# became "Ryan, L., 2023.") kept the citation and restored the separator, so these rules insert a
# space and keep the citation.
#
# Every rule is anchored on an initial (a capital and a period, or a bare capital between two
# glued words), because a name glued to an initial is the one form ordinary prose does not take:
#   given-first   "ReasonP.DeutscherColumbia" -> "Reason P. Deutscher Columbia" (Wiley: title,
#                 initials, surname and publisher in one run)
#   bare initial  "FrantzEKendall-TaylorA" -> "Frantz E Kendall-Taylor A"
#   left glue     "GehmanJ.", "CastañedaE." -> "Gehman J.", "Castañeda E."
#   right glue    "T.Buckley", "C.2024", "S.(eds)", "B.van" -> "T. Buckley", "C. 2024", ...
# A full given name glued to a surname ("NicoleMellow", "SharmaAmogh Dhar") has no initial to
# anchor on; name_split.py splits it with a name dictionary, which keeps "McLean",
# "PublicAffairs" and "MeToo" whole, and clean_title() runs it last
# (docs/ledger/reports/review-title-fullname/).
#
# The rules run only on a title that reads as a citation (a page count, an ISBN, an edited-by
# marker, a review heading, or a year with a publisher word), which keeps "EffectLiteR." (an R
# package) and "D.R.Congo" as they are. Against the 196,365 titles in production on 2026-10-25
# they change 325 rows, every change was read, and none is a false positive.

_LOWER = "a-zß-öø-ÿ"
_GIVEN_FIRST = re.compile(rf"([^\W\d_])((?:[A-Z]\. ?)+)([A-Z][{_LOWER}]{{3,}})(?=[A-Z][{_LOWER}])")
_BARE_INITIAL = re.compile(rf"([^\W\d_]+)([A-Z])(?=[A-Z][{_LOWER}])")
_LEFT_GLUE = re.compile(r"([^\W\d_]+)([A-Z]\.)")
_RIGHT_GLUE = re.compile(
    rf"(?<![^\W\d_])([A-Z]\.)(?=[A-Z][{_LOWER}]|\(|\d|(?:van|von|de|da|di|du|del|der|den|le|la|ten|ter) )")
# "PhD." and its kin end in a capital and a period after lowercase letters and are not names.
_DEGREES = {"Ph", "Ed", "Psy", "Sc", "Th", "Pharm"}

_YEAR = re.compile(r"(?<!\d)(?:1[89]|20)\d{2}(?!\d)")
_CITATION_MARK = re.compile(
    r"\b\d{2,4}\s*(?:pp|p|pages?)\b|\b[Pp]p\.|\bISBN|\(\s*[Ee]ds?\.?\s*\)|\b[Ee]dited [Bb]y\b"
    r"|\b(?:Book|Media) [Rr]eview\b|\bCompte [Rr]endu\b|\bReview [Ee]ssay\b")
_PUBLISHER_MARK = re.compile(
    r"\b(?:Press|University|Publish\w*|Routledge|Palgrave|Springer|Elgar|Macmillan|Verlag|Penguin"
    r"|Wiley|Blackwell|Foundation|Books)\b")


def reads_as_citation(s: str) -> bool:
    """True when `s` carries the marks of a book citation, the only titles the name rules touch."""
    return bool(_CITATION_MARK.search(s) or (_YEAR.search(s) and _PUBLISHER_MARK.search(s)))


def repair_flattened_names(s: str) -> str:
    """Insert the separators a flattened JATS name lost (see above). `None` in, `None` out.
    Idempotent: each rule fires only where a letter touches an initial, and its output is spaced."""
    if not s or not reads_as_citation(s):
        return s

    def given_first(m):
        if not m.group(1).islower():
            return m.group(0)
        initials = re.sub(r"\.(?=[A-Z])", ". ", m.group(2)).strip()
        return f"{m.group(1)} {initials} {m.group(3)} "

    def glued(word):
        return word[-1].islower() and word not in _DEGREES

    def bare(m):
        return f"{m.group(1)} {m.group(2)} " if glued(m.group(1)) else m.group(0)

    def left(m):
        return f"{m.group(1)} {m.group(2)}" if glued(m.group(1)) else m.group(0)

    s = _GIVEN_FIRST.sub(given_first, s)
    s = _BARE_INITIAL.sub(bare, s)
    s = _LEFT_GLUE.sub(left, s)
    s = _RIGHT_GLUE.sub(lambda m: m.group(1) + " ", s)
    return s


# Crossref keeps the deposit's pretty-printing: a newline and twenty spaces around every inline
# tag ("in the\n                    <i>Everyday</i>\n                    :"). HTML already shows
# one space there, so collapsing changes no rendered page, only plain-text surfaces (meta tags,
# JSON-LD, search). What the newline replaced cannot be recovered ("<u>Syn</u>\n thesis" was one
# word, "</i>\n :" had no space), so that is left as HTML shows it. A no-break space is kept.
_WS_RUN = re.compile(r"[ \t\r\n]+")


def collapse_whitespace(s: str) -> str:
    """Collapse runs of ASCII whitespace to one space and trim the ends. `None` in, `None` out."""
    if s is None:
        return None
    return _WS_RUN.sub(" ", s).strip(" ")


def clean_title(s: str) -> str:
    """Title-only repairs, run after normalize_text() at the write choke point: whitespace, the
    flattened citation names anchored on an initial, then the full names (name_split.py, with the
    name dictionary in name_parts.json). Abstracts never go through it (paragraph breaks, and no
    abstract carried the name defect on 2026-10-25). Idempotent."""
    try:  # imported here: name_split imports this module
        from name_split import load_dictionary, split_glued_names
    except ImportError:
        from pipeline.name_split import load_dictionary, split_glued_names
    s = repair_flattened_names(collapse_whitespace(s))
    if not s:
        return s
    names, given = load_dictionary()
    return split_glued_names(s, names, given)
