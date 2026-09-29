"""Gate 18: factual-KB suitability (SUITABILITY_FILTER_VERSION ``suit1``).

Answers "is this document useful material for a factual knowledge base?",
NOT "is it true": a PASS never means the content was verified.

Deterministic rules over cleaned text. Signals are rates per 1,000 words,
so length does not decide by itself.

Positive: definitional/explanatory sentences ("is a", "refers to", "is
defined as", "consists of", "is used to"), quantities with units, dates,
technical vocabulary (long words), reference apparatus (``[1]``, DOI, ISBN,
"et al.", References), documentation cues (parameters, returns, usage,
install, configuration), and structure (headings, lists, tables, code).

Negative: first-person chatter, opinion markers, promotional/call-to-action
language, conversational/forum phrases, emoji/hashtag/mention density,
question-heavy text and very short lines.

Code: base value plus documentation (comment/docstring share, doc cues).

score in [0, 1]; PASS >= ``pass_threshold``, FAIL < ``fail_threshold``,
otherwise UNRESOLVED.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from app.processing.quality.scorer import TextFeatures

VERSION = "suit1"

PASS, FAIL, UNRESOLVED = "PASS", "FAIL", "UNRESOLVED"

_DEFINITION = re.compile(r"\b(is|are|was|were) (a|an|the|one of the) \w+|\brefers? to\b|\bis defined as\b|"
                         r"\bconsists? of\b|\bis known as\b|\b(is|are) used (to|for|in)\b|\bis a type of\b|"
                         r"\bdescribes?\b|\bcontains?\b|\bincludes?\b|\bcomposed of\b|\bdenotes?\b|\bmeans\b", re.I)
_QUANTITY = re.compile(r"\b\d+(?:[.,]\d+)?\s?(?:%|km|kg|mg|g|m|cm|mm|nm|µm|ml|l|°c|°f|k|hz|khz|mhz|ghz|kb|mb|gb|tb"
                       r"|ms|s|min|h|mol|j|kj|w|kw|v|a|pa|kpa|n|ev|au|ly|bits?|bytes?)\b", re.I)
_YEAR = re.compile(r"\b(1[5-9]\d\d|20\d\d)\b")
_REFERENCE = re.compile(r"\[\d{1,3}\]|\bdoi:|\bisbn\b|\bet al\.|^#+ (references|bibliography|further reading|"
                        r"see also|sources)\b|\bretrieved\b", re.I | re.M)
_DOC_CUE = re.compile(r"\b(returns?|parameters?|arguments?|usage|example|examples|install(ation)?|configur(e|ation)|"
                      r"function|method|class|module|api|option|default|syntax|command|interface|attribute|"
                      r"specification|algorithm|theorem|equation|definition|procedure|overview|introduction)\b", re.I)
_FIRST_PERSON = re.compile(r"\b(i|me|my|mine|i'm|i've|i'd|i'll)\b", re.I)
_OPINION = re.compile(r"\b(i think|i feel|i believe|in my opinion|imo|imho|i love|i hate|i like|best ever|so cool"
                      r"|amazing|awesome|terrible|worst|lol|lmao|omg|wtf|haha)\b|!{2,}", re.I)
_PROMO = re.compile(r"(\bbuy\b|\bsale\b|discount|% off|free shipping|order now|shop now|\bdeals?\b|coupon|"
                    r"limited time|best price|add to cart|special offer|subscribe now|sign up|call now|"
                    r"don't miss|act now|exclusive offer)", re.I)
_CHAT = re.compile(r"\b(thanks|thank you|thx|hi all|hey guys|hello everyone|reply|replied|posted by|quote|"
                   r"upvote|downvote|dm me|pm me|follow me|like and subscribe)\b", re.I)
_SOCIAL = re.compile(r"(?<!\w)[#@][A-Za-z]\w+|[\U0001F300-\U0001FAFF☀-➿]")


@dataclass(frozen=True)
class SuitabilityResult:
    decision: str
    score: float
    reasons: tuple[str, ...]
    signals: dict[str, float]


def _rate(pattern: re.Pattern[str], text: str, words: int) -> float:
    return len(pattern.findall(text)) * 1000.0 / max(words, 1)


def _squash(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def classify(text: str, f: TextFeatures, *, kind: str, pass_threshold: float, fail_threshold: float,
             prose_words: int = 0) -> SuitabilityResult:
    words = max(f.words, 1)
    long_words = sum(1 for w in re.findall(r"[A-Za-z]{9,}", text)) * 1000.0 / words
    signals = {
        "definition": _rate(_DEFINITION, text, words), "quantity": _rate(_QUANTITY, text, words),
        "year": _rate(_YEAR, text, words), "reference": _rate(_REFERENCE, text, words),
        "doc_cue": _rate(_DOC_CUE, text, words), "long_words": long_words,
        "first_person": _rate(_FIRST_PERSON, text, words), "opinion": _rate(_OPINION, text, words),
        "promo": _rate(_PROMO, text, words), "chat": _rate(_CHAT, text, words),
        "social": _rate(_SOCIAL, text, words),
        "questions": text.count("?") * 1000.0 / words,
        "structure": float(f.headings + f.bullet_lines // 3 + f.code_blocks),
    }
    reasons: list[str] = []
    if kind in ("code", "config"):
        comment_share = prose_words / words
        value = 0.45 + 0.35 * min(1.0, comment_share / 0.15) + 0.2 * min(1.0, signals["doc_cue"] / 15)
        value -= 0.3 * min(1.0, (signals["promo"] + signals["chat"]) / 20)
        if comment_share < 0.02:
            reasons.append("undocumented_code")
        if kind == "config":
            value -= 0.15
            reasons.append("configuration")
    else:
        positive = (min(signals["definition"], 40) / 40 * 1.2 + min(signals["quantity"] + signals["year"], 30) / 30 * 0.6
                    + min(signals["reference"], 10) / 10 * 0.6 + min(signals["doc_cue"], 25) / 25 * 0.5
                    + min(long_words, 120) / 120 * 0.6 + min(signals["structure"], 6) / 6 * 0.4)
        negative = (min(signals["first_person"], 60) / 60 * 1.2 + min(signals["opinion"], 15) / 15 * 1.0
                    + min(signals["promo"], 30) / 30 * 1.4 + min(signals["chat"], 15) / 15 * 0.8
                    + min(signals["social"], 20) / 20 * 0.8 + min(signals["questions"], 60) / 60 * 0.6)
        if f.mean_line_len < 40 and f.lines >= 10:
            negative += 0.4
            reasons.append("short_lines")
        value = _squash(2.2 * (positive - negative) - 0.2)
        for name, limit in (("first_person", 30), ("opinion", 6), ("promo", 12), ("chat", 8), ("social", 10),
                            ("questions", 40)):
            if signals[name] >= limit:
                reasons.append(f"high_{name}")
        for name, limit in (("definition", 8), ("reference", 2), ("doc_cue", 8), ("quantity", 5)):
            if signals[name] >= limit:
                reasons.append(f"has_{name}")
    value = round(max(0.0, min(1.0, value)), 4)
    decision = PASS if value >= pass_threshold else FAIL if value < fail_threshold else UNRESOLVED
    return SuitabilityResult(decision, value, tuple(reasons), {k: round(v, 2) for k, v in signals.items()})
