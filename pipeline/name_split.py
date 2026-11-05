"""name_split.py — split full names that flattened JATS markup glued together in book-review titles.

text_norm.repair_flattened_names() handles every glue that touches an initial. What it leaves is a
name written in full on both sides of the lost separator: SAGE's surname-first lists
("BuckinghamSusanHultmanMartin", "SharmaAmogh Dhar") and Wiley's given-first ones ("By DavidCoen,
AlexanderKatsaitis"), plus a title or surname glued to the publisher or the next word
("InstitutionsColumbia", "SachsThe", "AquinoJr"). The same shape is also a real word
("PublicAffairs", "HarperCollins", "McLean", "#MeToo"), so the split needs to know names: a boundary
inside a glued capitalised run is split only when one of its two sides is a name part the corpus
knows (a first or last word of `authors.name`, at least NAME_MIN times), a word that also stands
alone elsewhere in the same title, or, on the right, a publisher or joining word (JOINERS).

The rules run only where text_norm.reads_as_citation() holds. Against production on 2026-10-25 they
change the rows listed in docs/ledger/reports/review-title-fullname/, every one of which was read.
On the ingest path through text_norm.clean_title() (owner 2026-10-25), with the dictionary from
name_parts.json beside this file (build_name_parts.py writes it; the crawler gets a copy).
"""
from __future__ import annotations

import json
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

try:
    from text_norm import reads_as_citation
except ImportError:  # imported as pipeline.name_split
    from pipeline.text_norm import reads_as_citation

NAME_MIN = 5
DICTIONARY_PATH = Path(__file__).with_name("name_parts.json")

# Surname prefixes that start a capitalised part of the same name: "McLean", "MacAskill",
# "DeFrank", "FitzGerald", "VanderWeele". The prefix and the next part stay one word.
PREFIXES = {"Mc", "Mac", "De", "Di", "Da", "Du", "La", "Le", "Lo", "Van", "Von", "Vander", "Vanden",
            "Der", "Den", "Del", "Della", "Fitz", "St", "Te", "Ten", "Ter", "O"}

# Words that follow a glued name or title in these citations: publishers, their cities, and the
# joining words of a citation. Split before them whatever stands on the left.
JOINERS = {
    "The", "By", "How", "On", "An", "One", "Jr", "Sr", "St", "New", "East", "Little", "Random",
    "University", "Press", "Oxford", "Cambridge", "Columbia", "Harvard", "Yale", "Princeton",
    "Stanford", "Chicago", "Routledge", "Palgrave", "Polity", "Verso", "Penguin", "Bloomsbury",
    "Bristol", "Manchester", "Ithaca", "Oakland", "London", "Abingdon", "Cheltenham", "Newcastle",
    "Geneva", "Rochester", "Scribner", "Hachette", "Zed", "Pitchstone", "Haymarket", "Baylor",
    "Metropolitan", "Knoxville", "Washington", "Durham", "Kingston", "Springer", "Edward", "Boca",
}

# Real words of this shape found in production book-review titles on 2026-10-25, plus the common
# brand names of the same shape. Never split.
KEEP = {"PublicAffairs", "HarperCollins", "HarperOne", "SpringerLink", "UniPress", "GoFailMe", "MeToo",
        "YouTube", "LinkedIn", "BlackRock", "WhatsApp", "TikTok", "WeChat", "PayPal", "PlayStation",
        "JavaScript", "PowerPoint", "GitHub", "SoundCloud", "BlackBerry", "DreamWorks", "WikiLeaks",
        "BuzzFeed", "FaceTime", "QAnon"}

# A lowercase particle glued to the given name before it ("Maartende Jong", "Michaelde Percy"),
# unglued only when the part is no known name and what precedes the particle is a given name of
# four letters or more that opens at least GIVEN_MIN author names. "da", "la", "der", "ter" and the
# like are left out, and the bar is high, because the same letters end real names: on 2026-10-25 a
# looser rule turned "Kanter" into "Kan ter", "Biede" into "Bie de" and "Vander" into "Van der".
GIVEN_MIN = 20
_PARTICLE = re.compile(r"([A-Z][^\W\d_A-Z]{3,}?)(della|van|von|del|de)")

_RUN = re.compile(r"(?<![^\W\d_])[A-Z][^\W\d_A-Z]+(?:[A-Z][^\W\d_A-Z]+)+(?![^\W\d_])")
_PART = re.compile(r"[A-Z][^\W\d_A-Z]+")
_WORD = re.compile(r"(?<![^\W\d_])[A-Z][^\W\d_A-Z]+(?![^\W\d_])")


def name_dictionary(author_names) -> tuple[set[str], set[str]]:
    """(name parts, given names) from author name strings: the first and last words occurring at
    least NAME_MIN times, and the first words occurring at least GIVEN_MIN times."""
    first, last = Counter(), Counter()
    word = re.compile(r"[^\W\d_][^\W\d_'’-]*")
    for n in author_names:
        parts = [p for p in (n or "").split() if word.fullmatch(p)]
        if len(parts) >= 2:
            first[parts[0]] += 1
            last[parts[-1]] += 1
    names = {p for c in (first, last) for p, k in c.items() if k >= NAME_MIN and len(p) >= 2}
    given = {p for p, k in first.items() if k >= GIVEN_MIN}
    return names, given


@lru_cache(maxsize=1)
def load_dictionary() -> tuple[frozenset[str], frozenset[str]]:
    """(name parts, given names) from name_parts.json, read once per process."""
    doc = json.loads(DICTIONARY_PATH.read_text(encoding="utf-8"))
    return frozenset(doc["names"]), frozenset(doc["given"])


def _parts(run: str) -> list[str]:
    out: list[str] = []
    for p in _PART.findall(run):
        if out and out[-1] in PREFIXES:
            out[-1] += p
        else:
            out.append(p)
    return out


def split_glued_names(s: str, names: set[str], given: set[str] = frozenset()) -> str:
    """Insert the space a flattened name lost between two capitalised parts, with the dictionary
    from name_dictionary(). `None` in, `None` out. Idempotent: a split part no longer touches its
    neighbour."""
    if not s or not reads_as_citation(s):
        return s
    standalone = set(_WORD.findall(s))

    def known(p: str) -> bool:
        return p in names or p in standalone

    def unglue_particle(p: str) -> str:
        m = _PARTICLE.fullmatch(p)
        if m and p not in names and p not in PREFIXES and m.group(1) in given:
            return f"{m.group(1)} {m.group(2)}"
        return p

    def fix(m: re.Match) -> str:
        run = m.group(0)
        if run in KEEP or (m.start() > 0 and s[m.start() - 1] in "#@"):
            return run
        # Each boundary is judged on the words next to it once particles are unglued, so that
        # "AlbertazziDanielevan" splits at once into "Albertazzi Daniele van".
        parts = [unglue_particle(p) for p in _parts(run)]
        out = parts[0]
        for a, b in zip(parts, parts[1:]):
            left, right = a.split(" ")[-1], b.split(" ")[0]
            out += (" " if known(left) or known(right) or right in JOINERS else "") + b
        return out

    return _RUN.sub(fix, s)
