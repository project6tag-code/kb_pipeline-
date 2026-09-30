"""Chunking token counter ``kbp-approx-bpe`` version ``1`` (deterministic, dependency-free).

The final embedding model (and so its tokenizer) is not chosen yet (O-2), so
chunk sizes are measured with a stable, model-independent counter that
approximates modern subword (BPE/WordPiece) tokenizers on English text:

* a letter run of length ``n`` counts ``ceil(n / 4)`` tokens (``the`` = 1,
  ``photosynthesis`` = 4);
* a digit run of length ``n`` counts ``ceil(n / 3)`` tokens;
* every other non-space character (punctuation, symbols, CJK, emoji) counts 1.

It is an estimate, not the embedding tokenizer: chunk ids never depend on it
(identity is the canonical text), and the size limits keep head-room below a
512-token model input (see ``ChunkingConfig``). The embedding stage re-checks
against the real tokenizer (``embedding_models.max_input_tokens``).

Tokens are also the unit of the token-boundary fallback split: ``split_tokens``
returns character offsets at whitespace or letter/digit/symbol boundaries, never
inside a letter run shorter than the per-token width.
"""

from __future__ import annotations

import re

NAME = "kbp-approx-bpe"
VERSION = "1"
IDENTITY = f"{NAME}:{VERSION}"

_PIECE = re.compile(r"[^\W\d_]+|\d+|\S", re.UNICODE)


def _cost(piece: str) -> int:
    if piece[0].isalpha():
        return -(-len(piece) // 4)
    if piece[0].isdigit():
        return -(-len(piece) // 3)
    return 1


def count_tokens(text: str) -> int:
    return sum(_cost(m.group(0)) for m in _PIECE.finditer(text))


def token_boundaries(text: str, max_tokens: int) -> list[int]:
    """Character offsets that cut ``text`` into windows of at most ``max_tokens`` (fallback split).

    Cuts are placed at piece boundaries; a single piece longer than the window
    (e.g. a 10,000-letter run) is cut every ``4 * max_tokens`` characters.
    """
    if max_tokens < 1:
        raise ValueError("max_tokens must be >= 1")
    cuts: list[int] = []
    used = 0
    for m in _PIECE.finditer(text):
        cost = _cost(m.group(0))
        if cost > max_tokens:  # a pathological single run: hard cut inside it
            if used:
                cuts.append(m.start())
            step = 4 * max_tokens if m.group(0)[0].isalpha() else 3 * max_tokens
            inner = list(range(m.start() + step, m.end(), step))
            cuts.extend(inner)
            used = _cost(text[inner[-1] if inner else m.start():m.end()])
            continue
        if used + cost > max_tokens:
            cuts.append(m.start())
            used = 0
        used += cost
    return cuts
