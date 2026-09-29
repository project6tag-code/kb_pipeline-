"""Gate 17: general document quality (QUALITY_FILTER_VERSION ``qual1``) and shared text features.

``features`` computes deterministic surface statistics once per document;
the noise filter (gate 16), this scorer and the suitability classifier
(gate 18) all read them.

Quality score = weighted mean of component scores in [0, 1]:

| component    | prose weight | signal                                                  |
|--------------|-------------:|---------------------------------------------------------|
| length       | 0.20 | useful words, log-scaled 40 -> 1000                           |
| alphabetic   | 0.10 | letters / non-space characters                                 |
| structure    | 0.20 | sentence-terminated lines, sentence length, paragraphs, headings/lists |
| repetition   | 0.15 | duplicate-line and top-bigram character fractions (Gopher-style) |
| information  | 0.15 | unique-word ratio                                              |
| naturalness  | 0.10 | English function-word ratio                                    |
| residue      | 0.05 | boilerplate phrases left after cleaning                        |
| integrity    | 0.05 | replacement / control characters                               |

Code and configuration use code-appropriate components (comment prose,
line-length sanity, repetition, information, alphanumeric ratio). PASS iff
score >= threshold; component scores < 0.4 become reason codes.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass

from app.processing.language.detector import ENGLISH_FUNCTION_WORDS as _EN

VERSION = "qual1"

_WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)?")
_SENTENCE_END = re.compile(r"[.!?][\"')\]]*(?:\s|$)")
_TERMINAL = re.compile(r"[.!?:;][\"')\]]*$")
_BULLET = re.compile(r"^\s*(?:[-*+•]|\d+[.)])\s")
_RESIDUE = re.compile(r"(cookie|subscribe|newsletter|sign up|log in|share this|follow us|all rights reserved|"
                      r"privacy policy|terms of (use|service)|click here|read more|advertisement)", re.I)


@dataclass(frozen=True)
class TextFeatures:
    chars: int
    words: int
    unique_words: int
    lines: int
    prose_lines: int
    terminal_lines: int
    paragraphs: int
    sentences: int
    headings: int
    bullet_lines: int
    code_blocks: int
    alpha_ratio: float
    alnum_ratio: float
    digit_ratio: float
    symbol_ratio: float
    upper_ratio: float
    stopword_ratio: float
    dup_line_frac: float
    top_bigram_frac: float
    dup_5gram_frac: float
    max_line_len: int
    mean_line_len: float
    replacement_chars: int
    control_chars: int
    residue_hits: int
    top_word: str | None
    top_word_count: int
    char_entropy: float

    @property
    def unique_ratio(self) -> float:
        return self.unique_words / self.words if self.words else 0.0

    @property
    def mean_sentence_words(self) -> float:
        return self.words / self.sentences if self.sentences else float(self.words)


def _dup_ngram_frac(words: list[str], n: int) -> float:
    if len(words) < n * 2:
        return 0.0
    grams = [" ".join(words[i:i + n]) for i in range(len(words) - n + 1)]
    counts = Counter(grams)
    covered = [False] * len(words)
    for i, gram in enumerate(grams):
        if counts[gram] > 1:
            for j in range(i, i + n):
                covered[j] = True
    return sum(len(w) for w, c in zip(words, covered) if c) / max(1, sum(len(w) for w in words))


def features(text: str) -> TextFeatures:
    lines = [line.strip() for line in text.split("\n")]
    non_empty = [line for line in lines if line]
    tokens = _WORD.findall(text)
    lower = [t.lower() for t in tokens]
    non_space = [ch for ch in text if not ch.isspace()]
    n_ns = max(1, len(non_space))
    letters = sum(1 for ch in non_space if ch.isalpha())
    digits = sum(1 for ch in non_space if ch.isdigit())
    upper = sum(1 for ch in non_space if ch.isupper())
    line_counts = Counter(non_empty)
    dup_chars = sum(len(line) * count for line, count in line_counts.items() if count > 1 and len(line) > 1)
    total_line_chars = max(1, sum(len(line) for line in non_empty))
    bigrams = Counter(zip(lower, lower[1:]))
    top_bigram_frac = 0.0
    if bigrams:
        (a, b), count = bigrams.most_common(1)[0]
        top_bigram_frac = count * (len(a) + len(b)) / max(1, sum(len(w) for w in lower))
    content = Counter(w for w in lower if w not in _EN and len(w) > 2 and not w.isdigit())
    top_word, top_count = content.most_common(1)[0] if content else (None, 0)
    prose = [line for line in non_empty if len(line.split()) >= 5 and not line.startswith(("#", "|", "```"))]
    char_counts = Counter(non_space)
    entropy = -sum(c / n_ns * math.log2(c / n_ns) for c in char_counts.values()) if non_space else 0.0
    return TextFeatures(
        chars=len(text), words=len(tokens), unique_words=len(set(lower)), lines=len(non_empty),
        prose_lines=len(prose), terminal_lines=sum(1 for line in prose if _TERMINAL.search(line)),
        paragraphs=len([p for p in text.split("\n\n") if p.strip()]), sentences=len(_SENTENCE_END.findall(text)),
        headings=sum(1 for line in non_empty if line.startswith("#")),
        bullet_lines=sum(1 for line in non_empty if _BULLET.match(line)),
        code_blocks=text.count("```") // 2, alpha_ratio=letters / n_ns,
        alnum_ratio=(letters + digits) / n_ns, digit_ratio=digits / n_ns,
        symbol_ratio=(n_ns - letters - digits) / n_ns, upper_ratio=upper / max(1, letters),
        stopword_ratio=sum(1 for w in lower if w in _EN) / max(1, len(lower)),
        dup_line_frac=dup_chars / total_line_chars, top_bigram_frac=top_bigram_frac,
        dup_5gram_frac=_dup_ngram_frac(lower, 5),
        max_line_len=max((len(line) for line in non_empty), default=0),
        mean_line_len=total_line_chars / max(1, len(non_empty)),
        replacement_chars=text.count("�"), control_chars=sum(1 for ch in text if ord(ch) < 32 and ch not in "\n\t"),
        residue_hits=len(_RESIDUE.findall(text)), top_word=top_word, top_word_count=top_count,
        char_entropy=entropy)


def _ramp(value: float, low: float, high: float) -> float:
    if high == low:
        return 1.0 if value >= high else 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


@dataclass(frozen=True)
class QualityResult:
    score: float
    passed: bool
    components: dict[str, float]
    reasons: tuple[str, ...]


def score(f: TextFeatures, *, kind: str, threshold: float, prose_words: int = 0,
          kept_ratio: float | None = None) -> QualityResult:
    repetition = 1.0 - max(_ramp(f.dup_line_frac, 0.1, 0.35), _ramp(f.top_bigram_frac, 0.08, 0.2),
                           _ramp(f.dup_5gram_frac, 0.25, 0.6))
    information = _ramp(f.unique_ratio, 0.12, 0.35) if f.words >= 200 else _ramp(f.unique_ratio, 0.25, 0.55)
    integrity = 1.0 - _ramp((f.replacement_chars + f.control_chars) / max(1, f.chars), 0.0, 0.002)
    if kind in ("code", "config"):
        comments = _ramp(prose_words / max(1, f.words), 0.02, 0.15)
        lines = 1.0 - max(_ramp(f.max_line_len, 300, 1000), _ramp(f.mean_line_len, 80, 160))
        components = {"length": _ramp(math.log(max(f.words, 1)), math.log(30), math.log(400)),
                      "alnum": _ramp(f.alnum_ratio, 0.35, 0.6), "comments": comments, "lines": lines,
                      "repetition": repetition, "information": information, "integrity": integrity}
        weights = {"length": 0.15, "alnum": 0.10, "comments": 0.20, "lines": 0.15, "repetition": 0.15,
                   "information": 0.15, "integrity": 0.10}
    else:
        terminal = f.terminal_lines / f.prose_lines if f.prose_lines else 0.0
        sentence_len = f.mean_sentence_words
        sentences = 1.0 - max(_ramp(4 - sentence_len, 0, 3), _ramp(sentence_len, 45, 90))
        structure = 0.5 * _ramp(terminal, 0.2, 0.6) + 0.3 * sentences + 0.2 * min(
            1.0, (_ramp(f.paragraphs, 1, 4) + (0.5 if f.headings or f.bullet_lines else 0.0)))
        residue = 1.0 - _ramp(f.residue_hits * 1000 / max(1, f.words), 2, 15)
        components = {"length": _ramp(math.log(max(f.words, 1)), math.log(40), math.log(1000)),
                      "alphabetic": _ramp(f.alpha_ratio, 0.55, 0.78), "structure": structure,
                      "repetition": repetition, "information": information,
                      "naturalness": _ramp(f.stopword_ratio, 0.08, 0.25), "residue": residue,
                      "integrity": integrity}
        weights = {"length": 0.20, "alphabetic": 0.10, "structure": 0.20, "repetition": 0.15,
                   "information": 0.15, "naturalness": 0.10, "residue": 0.05, "integrity": 0.05}
        if kept_ratio is not None:
            components["completeness"] = _ramp(kept_ratio, 0.02, 0.15)
            weights = {k: v * 0.95 for k, v in weights.items()}
            weights["completeness"] = 0.05
    total = sum(components[k] * weights[k] for k in weights)
    rounded = {k: round(v, 3) for k, v in sorted(components.items())}
    reasons = tuple(f"low_{k}" for k, v in sorted(components.items()) if v < 0.4)
    return QualityResult(round(total, 4), total >= threshold, rounded, reasons)
