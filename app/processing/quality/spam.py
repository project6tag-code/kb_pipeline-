"""Gate 16: spam / error / template / noise filter (NOISE_FILTER_VERSION ``noise2``).

Every check is deterministic and returns a reason code; any hit fails the
gate. Page-type checks (error, bot wall, parked domain, placeholder) only
apply to short documents, where the phrase *is* the page rather than a
mention inside real content.

| reason                  | rule (prose)                                                          |
|-------------------------|-----------------------------------------------------------------------|
| too_little_text         | fewer than ``min_words`` words                                         |
| soft_404 / error_page   | error/not-found/access-denied phrasing in a short page                 |
| bot_wall                | "enable JavaScript", "checking your browser", CAPTCHA pages           |
| parked_domain           | domain-for-sale / parking pages                                       |
| placeholder             | lorem ipsum, "coming soon", "under construction", server default pages |
| navigation_only         | link text >= 50% of the kept text, or a list of 1-3 word items        |
| cookie_consent_only     | cookie/consent vocabulary dominates a short page                      |
| advertisement_only      | promotional vocabulary >= 4% of words                                  |
| link_farm               | URLs >= 10% of tokens                                                  |
| keyword_stuffing        | one content word >= 6% of words (>= 20 times), or top bigram >= 20% of text |
| excessive_repetition    | >= 30% of line characters in duplicated lines, or >= 60% duplicated 5-grams (code: 45% / 80%) |
| gibberish               | unpronounceable words >= 25%, or no function words in long prose        |
| symbol_noise            | symbols >= 25% of non-space characters (code: 45%)                     |
| low_information         | unique-word ratio < 0.15 over the first 1,000 words (>= 100 words), or |
|                         | character entropy < 3.0. The window keeps the ratio length-independent: |
|                         | over a whole long article it always falls (noise1 rejected long         |
|                         | encyclopedia entries)                                                   |
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.processing.quality.scorer import _WORD, TextFeatures

VERSION = "noise2"
LOW_INFORMATION_WINDOW = 1000

_ERROR = re.compile(r"\b(404|page not found|not found|error 404|page (does not|doesn't|could not be found|cannot "
                    r"be found)|no longer (available|exists)|has been (removed|deleted)|access denied|403 forbidden"
                    r"|forbidden|unauthori[sz]ed|internal server error|500 error|service unavailable|bad gateway"
                    r"|something went wrong|an error (has )?occurred|this page isn.t available)\b", re.I)
_BOT_WALL = re.compile(r"(enable javascript|javascript is (disabled|required|not enabled)|checking your browser"
                       r"|verify (that )?you are (a )?human|are you a robot|captcha|please turn on javascript"
                       r"|browser is not supported|cookies must be enabled|ddos protection|just a moment\.\.\.)", re.I)
_PARKED = re.compile(r"(domain (name )?(is |may be )?for sale|buy this domain|this domain (name )?(may be|is) for"
                     r"|parked (free|domain|by)|domain parking|make an offer on this domain|inquire about this"
                     r" domain|this web page is parked)", re.I)
_PLACEHOLDER = re.compile(r"(lorem ipsum|dolor sit amet|coming soon|under construction|website is under"
                          r"|default web page|it works!|welcome to nginx|apache2 (ubuntu|debian) default page"
                          r"|test page for|this is a placeholder|site is being built)", re.I)
_COOKIE = re.compile(r"\b(cookies?|consent|gdpr|preferences|opt[- ]?out|tracking|third[- ]party|accept|reject)\b",
                     re.I)
_PROMO = re.compile(r"(\bbuy\b|\bsale\b|discount|% off|free shipping|order now|shop now|\bdeals?\b|coupon|promo code"
                    r"|limited time|best price|lowest price|\bcheap\b|add to cart|checkout|special offer"
                    r"|save \d+%|call now|act now|click here|sign up today|money back|guarantee)", re.I)
_URL = re.compile(r"(https?://|www\.)\S+", re.I)
_UNPRONOUNCEABLE = re.compile(r"[bcdfghjklmnpqrstvwxz]{5,}", re.I)
_VOWEL = re.compile(r"[aeiouy]", re.I)
_ALPHA_WORD = re.compile(r"[A-Za-z]{4,}")


def _window_unique_ratio(text: str) -> float:
    words = [w.lower() for w in _WORD.findall(text)[:LOW_INFORMATION_WINDOW]]
    return len(set(words)) / len(words) if words else 1.0


@dataclass(frozen=True)
class NoiseResult:
    passed: bool
    reasons: tuple[str, ...]


def check(text: str, f: TextFeatures, *, kind: str, min_words: int, link_ratio: float | None = None) -> NoiseResult:
    reasons: list[str] = []
    code_like = kind in ("code", "config")
    short = f.words < 300
    head = text[:600]
    if f.words < min_words:
        reasons.append("too_little_text")
    if not code_like:
        if short and _ERROR.search(head):
            reasons.append("soft_404_or_error_page")
        if short and _BOT_WALL.search(text):
            reasons.append("bot_wall")
        if f.words < 500 and _PARKED.search(text):
            reasons.append("parked_domain")
        if _PLACEHOLDER.search(head) and (short or "lorem ipsum" in text.lower()):
            reasons.append("placeholder")
        if link_ratio is not None and link_ratio >= 0.5:
            reasons.append("navigation_only")
        if f.lines >= 5 and f.bullet_lines / f.lines >= 0.9 and f.words / max(1, f.lines) < 4:
            reasons.append("navigation_only_list")
        if short and len(_COOKIE.findall(text)) / max(1, f.words) >= 0.05:
            reasons.append("cookie_consent_only")
        if len(_PROMO.findall(text)) / max(1, f.words) >= 0.04:
            reasons.append("advertisement_only")
        words = _ALPHA_WORD.findall(text)
        if len(words) >= 30:
            bad = sum(1 for w in words if _UNPRONOUNCEABLE.search(w) or not _VOWEL.search(w))
            if bad / len(words) >= 0.25:
                reasons.append("gibberish")
        if f.words >= 100 and f.stopword_ratio < 0.03:
            reasons.append("gibberish_no_function_words")
        if f.symbol_ratio >= 0.25:
            reasons.append("symbol_noise")
    elif f.symbol_ratio >= 0.45:
        reasons.append("symbol_noise")
    if len(_URL.findall(text)) / max(1, f.words) >= 0.10:
        reasons.append("link_farm")
    if (f.top_word_count >= 20 and f.top_word_count / max(1, f.words) >= 0.06 and not code_like) \
            or f.top_bigram_frac >= 0.20:
        reasons.append("keyword_stuffing")
    line_limit, gram_limit = (0.45, 0.80) if code_like else (0.30, 0.60)
    if f.dup_line_frac >= line_limit or f.dup_5gram_frac >= gram_limit:
        reasons.append("excessive_repetition")
    if (f.words >= 100 and _window_unique_ratio(text) < 0.15) or (f.chars >= 200 and f.char_entropy < 3.0):
        reasons.append("low_information")
    return NoiseResult(not reasons, tuple(dict.fromkeys(reasons)))
