"""author_names.py — author-name normalization and the author-element merge guard.

norm_name() and name_agreement() moved here from probe_openalex_arxiv.py (behavior unchanged).

merge_author_elements() is the guard upsert_paper() applies when it rewrites the `authors` of a
paper that already exists (docs/ledger/author-elements-merge-guard.md; decision D4 of
docs/tierb/ARXIV_AUTHOR_ID_BACKFILL_WORKORDER.md). The arXiv crawler sends names only and re-fetches a
paper for up to 13 days, so without the guard a re-crawl would strip the OpenAlex IDs a later pass
wrote into the elements.
"""
from __future__ import annotations

import re
import unicodedata

CARRIED_KEYS = ("openalex_id", "orcid", "affiliation")

# Letters NFKD's canonical decomposition leaves whole (no base+combining-mark split exists for
# them), so the existing combining-mark removal never touches them, plus a Cyrillic table applied
# after NFKD, at which point й and ё have already arrived as и and е (their own NFKD decomposition
# strips the combining breve/diaeresis the same way any other combining mark is stripped). A
# matching aid, not a transliteration standard: ъ and ь are dropped rather than romanized, and кс/х
# are not disambiguated from ks/kh collisions.
_FOLD_MAP = {
    "ß": "ss", "ø": "o", "æ": "ae", "œ": "oe", "ł": "l", "ı": "i",
    "đ": "d", "ð": "d", "þ": "th", "ħ": "h",
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
    "ж": "zh", "з": "z", "и": "i", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh",
    "щ": "shch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu",
    "я": "ya", "і": "i", "є": "e", "ґ": "g",
}
_FOLD_TABLE = str.maketrans(_FOLD_MAP)


def norm_name(s: str | None) -> str:
    """Lowercase, accents and punctuation removed, 'Last, First' turned to 'First Last'."""
    if not s:
        return ""
    s = s.strip()
    if s.count(",") == 1:
        last, first = [p.strip() for p in s.split(",")]
        s = f"{first} {last}"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().translate(_FOLD_TABLE)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def name_agreement(stored: str, display: str | None, raw: str | None) -> str:
    """'exact': equal after norm_name to the display or raw name; 'variant': the same tokens in a
    different order, or equal once spaces are removed (e.g. a split surname); 'initials': same last
    token and same first letter (e.g. 'W. J. Percival' vs 'Will J. Percival'); else 'mismatch'."""
    a = norm_name(stored)
    cands = [c for c in (norm_name(display), norm_name(raw)) if c]
    if not a or not cands:
        return "mismatch"
    if a in cands:
        return "exact"
    at = a.split()
    a_joined = a.replace(" ", "")
    for c in cands:
        ct = c.split()
        if at and ct and sorted(at) == sorted(ct):
            return "variant"
        if a_joined and a_joined == c.replace(" ", ""):
            return "variant"
    for c in cands:
        ct = c.split()
        if at and ct and at[-1] == ct[-1] and at[0][0] == ct[0][0]:
            return "initials"
    return "mismatch"


def _element_name(el) -> str | None:
    """The normalized name of a dict element, None for anything that cannot be paired."""
    if not isinstance(el, dict):
        return None
    name = el.get("name")
    return norm_name(name if isinstance(name, str) else None)


def _has_value(v) -> bool:
    return v is not None and not (isinstance(v, str) and not v.strip())


def merge_author_elements(stored, incoming):
    """Return `incoming` with `openalex_id`, `orcid` and `affiliation` carried over from `stored`.

    The result has the length, order and names of `incoming`. A stored field is copied only when
    the paired incoming element does not have that key at all and the stored value is non-empty;
    an incoming None or empty string is a real value and is kept (normalize_openalex() always sends
    the three keys, and the Crossref cross-check sets openalex_id = None on purpose).

    Pairing: when both lists have the same length, element i pairs with element i, and only if
    their normalized names are equal. Otherwise an incoming element pairs with the one stored
    element of the same normalized name, provided that name occurs exactly once in each list.
    Unpaired and non-dict elements pass through unchanged. Neither argument is modified.
    """
    if not isinstance(stored, list) or not isinstance(incoming, list):
        return incoming

    in_names = [_element_name(el) for el in incoming]
    st_names = [_element_name(el) for el in stored]

    pairs: dict[int, int] = {}
    if len(stored) == len(incoming):
        for i, (a, b) in enumerate(zip(in_names, st_names)):
            if a is not None and b is not None and a == b:
                pairs[i] = i
    else:
        in_count: dict[str, int] = {}
        st_index: dict[str, list[int]] = {}
        for a in in_names:
            if a is not None:
                in_count[a] = in_count.get(a, 0) + 1
        for j, b in enumerate(st_names):
            if b is not None:
                st_index.setdefault(b, []).append(j)
        for i, a in enumerate(in_names):
            if a is not None and in_count[a] == 1 and len(st_index.get(a, ())) == 1:
                pairs[i] = st_index[a][0]

    out = []
    for i, el in enumerate(incoming):
        j = pairs.get(i)
        if j is None:
            out.append(el)
            continue
        src = stored[j]
        merged = dict(el)
        for key in CARRIED_KEYS:
            if key not in el and _has_value(src.get(key)):
                merged[key] = src[key]
        out.append(merged)
    return out
