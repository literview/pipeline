"""record_kind.py: non-research records by title (ledger task list-nonresearch-filter, change 3).

Book reviews, errata (corrections, retractions, expressions of concern), supplements deposited as
their own record, issue furniture (paratext) and editorials. Each class is one regular expression
in the subset Python and JavaScript share; a title takes the first class that matches, case-
insensitive, on the stripped title. lib/record-kind.ts holds the same five expressions in the same
order and text, and lib/record-kind.fixtures.json holds both copies to the same answers
(pipeline/test_record_kind.py, lib/record-kind.test.ts). Change RULE_VERSION with every rule edit,
here and in lib/record-kind.ts.

Moved from docs/ledger/reports/list-nonresearch-filter/rules.py (2026-10-30 measurement), which now
imports this module. Pure: no database, no network.
"""
import re

RULE_VERSION = "2026-11-22"

RULES = [
    # corrections and retractions: the notice word opens or closes the title. A research title may
    # use "retraction" or "withdrawal" as a subject ("The Retraction of Policy Benefits ...",
    # "Withdrawal symptoms: ..."), so those words count only in a notice's own shape.
    ("erratum", re.compile(
        r"^(online\s+)?(errat(um|a)|corrigend(um|a))\b|\b(errat(um|a)|corrigend(um|a))\s*$|\bcorrigend(um|a) to\b"
        r"|^((author|publisher)\s+)?corrections?\s*(to\b|:)"
        r"|^(retraction|retracted)\b|\bretraction (note|notice)\b|^withdrawal(\s+notice\b|\s*[–—:-])"
        r"|\bexpression of concern\b|^addend(um|a)\s*(to\b|:)"
        r"|^(notice of )?(publisher'?s?|editorial) (note|correction)\b", re.I)),
    # book reviews: the title says so, or carries the reviewed book's citation
    ("book-review", re.compile(
        r"\bbook reviews?\b|\breview essays?\b|\breviewed by\b|\bbook symposium\b"
        r"|^review of\b(?!\s+(the\s+|recent\s+|empirical\s+|existing\s+)?(literature|research|evidence|studies|methods|theories)\b)"
        r"|^(media|film|exhibition) review\b|^\s*:\s*<i>"
        r"|\bISBN\b|\b\d{2,4}\s?pp\b\.?|\bpp\.\s?\d|\b(hardcover|hardback|paperback|cloth|e-?book)\b[,:]?\s*(\$|£|€|us\$|\d)"
        r"|[$£€]\s?\d+(\.\d\d)?\s*(\(|,|;|\.|$)"
        r"|\b(university press|routledge|palgrave|edward elgar|polity|bloomsbury|brill|verso|zed books)\b[^.]{0,40}\b(19|20)\d\d\b"
        # "Title. By Author. City: Press, year." (record-kind-book-review-form, 2026-11-22): the year after a
        # separator keeps out "Comment on (Un)pleasant ... by Bond et al (2020)"
        r"|\.\s+(edited\s+)?by\s+.+[.,:]\s*(19|20)\d\d\b", re.I)),
    # a supplement deposited as its own record (APA "Supplemental Material for ...", DOI ending
    # .supp; found by aea-appx-pdf-url, 2026-10-30): the parent article is in the corpus
    ("supplement", re.compile(
        r"^supplementa(l|ry) (materials?|information|data|files?|appendix|appendices) (for|to)\b", re.I)),
    # issue furniture
    ("paratext", re.compile(
        r"^(cover and )?(front|back)\s?matter\b|^issue information\b|^editorial board\b|^masthead\b"
        r"|^(table of )?contents\b|^(subject |author )?index( to volume.*| of authors)?$|^in this issue\b"
        r"|^(list of )?(reviewers|referees)\b|acknowledg(e)?ments? (to|of) (our )?(reviewers|referees)"
        r"|^(notes on )?contributors\b|^call for (papers|proposals|submissions)\b|^announcements?\b"
        r"|^obituar(y|ies)\b|\bin memoriam\b|^volume \d+,? issue \d+", re.I)),
    # editor-written front pieces: not research, but some are substantive (symposium introductions)
    ("editorial", re.compile(
        r"^editorial\b|^(guest )?editors?'?s?'? (introduction|note|preface|foreword)\b|^from the editors?\b"
        r"|\bintroduction to (the |a |this )?(special issue|symposium|special section|forum|virtual special issue)\b"
        r"|^(special issue|symposium) introduction\b", re.I)),
]

KINDS = tuple(name for name, _ in RULES)

# Owner decision O5 (2026-10-30): editorials are labelled only, never hidden.
HIDDEN_KINDS = ("erratum", "book-review", "supplement", "paratext")


def record_kind(title):
    t = (title or "").strip()
    for name, rx in RULES:
        if rx.search(t):
            return name
    return None
