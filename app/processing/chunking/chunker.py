"""Chunking V1: structure-aware, deterministic chunking of accepted Silver documents.

Input: ``document_registry.canonical_text`` of an ``accepted`` document (plus
its Silver kind). Output: ordered ``ChunkCandidate`` spans, each a CONTIGUOUS
span ``[char_start, char_end)`` of that text, so that
``canonicalize_text(text[char_start:char_end]) == chunk_text`` (schema §5.15).
Heading and table-header context is never injected into the chunk text; it is
carried in ``section_path`` (the embedding stage may add it as an
embedding-version prefix, schema §5.16).

Algorithm (all sizes in ``kbp-approx-bpe`` tokens):

1. Parse blocks (``structure.parse_blocks``): heading, paragraph, list, table,
   fenced code, code group.
2. Split any block larger than ``piece_max = max_tokens - heading_reserve``:
   prose at sentence ends -> clause punctuation -> token boundary; lists between
   items; tables between rows (continuation pieces record the header row as
   context); code between lines, preferring top-level definitions.
3. Pack pieces in order: a chunk closes at a heading once it holds at least
   ``min_tokens``; tiny sections merge forward into their child or sibling
   sections but never across a higher-level heading; a chunk closes before it
   would exceed ``target_tokens`` (if it already holds ``min_tokens``) and always
   before ``max_tokens``. A heading always travels with the content after it.
4. Tail merge: a chunk below ``min_tokens`` joins the previous chunk when the
   result stays within ``max_tokens`` and it does not open a higher-level section.
5. Accept or reject each chunk (integrity/usefulness only; the document already
   passed Silver's quality gates). Rejected chunks keep their ordinal (gaps are
   allowed, schema §5.15).

No overlap (V1): boundaries are semantic and context lives in ``section_path``;
overlap would duplicate text inside chunk ids and defeat exact chunk dedup.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from app.processing.chunking import tokenizer
from app.processing.chunking.structure import TOP_LEVEL_CODE, Block, parse_blocks
from app.processing.extraction.html import CHROME_PHRASES
from app.processing.normalization.text import CANONICALIZATION_VERSION, canonicalize_text

ALGORITHM_VERSION = "c1"
_SENTENCE_END = re.compile(r"[.!?][\"')\]]*(?=\s+[\"'(\[]?[A-Z0-9])")
_CLAUSE_END = re.compile(r"[;:,](?=\s)")
_LIST_START = re.compile(r"^ ?(?:[-*+•]|\d{1,9}[.)]) +\S")


class ChunkingLimit(Exception):
    """A document exceeds a hard bound; it is not chunked (no silent truncation)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ChunkingConfig:
    target_tokens: int = 256
    min_tokens: int = 64
    max_tokens: int = 400
    hard_min_tokens: int = 12
    heading_reserve_tokens: int = 32
    overlap_tokens: int = 0
    max_document_chars: int = 1_000_000
    max_chunks_per_document: int = 2_000

    def validate(self) -> None:
        if not 0 < self.hard_min_tokens <= self.min_tokens <= self.target_tokens <= self.max_tokens:
            raise ValueError("require 0 < hard_min <= min <= target <= max tokens")
        if not 0 <= self.heading_reserve_tokens < self.max_tokens // 2:
            raise ValueError("heading_reserve_tokens must be below half of max_tokens")
        if self.overlap_tokens != 0:
            raise ValueError("Chunking V1 supports no overlap (overlap_tokens = 0)")
        if self.max_document_chars < 1 or self.max_chunks_per_document < 1:
            raise ValueError("document bounds must be positive")

    def parameters(self) -> dict:
        """Every output-affecting parameter (frozen in kbp.chunking_versions.parameters)."""
        return {
            "algorithm": "kbp-structure-chunker", "algorithm_version": ALGORITHM_VERSION,
            "tokenizer": tokenizer.IDENTITY, "canonicalizer": CANONICALIZATION_VERSION,
            "target_tokens": self.target_tokens, "min_tokens": self.min_tokens, "max_tokens": self.max_tokens,
            "hard_min_tokens": self.hard_min_tokens, "heading_reserve_tokens": self.heading_reserve_tokens,
            "overlap": {"policy": "none", "tokens": self.overlap_tokens},
            "boundaries": ["heading", "paragraph", "list", "table", "code", "sentence", "clause", "token"],
            "headings": "section_path JSON; a heading stays with the content that follows; never injected",
            "small_sections": "merge forward into child/sibling sections, never across a higher-level heading",
            "lists": "kept whole when they fit; split between items",
            "tables": "kept whole when they fit; split between rows; header recorded in section_path",
            "code": "fenced blocks and code groups kept whole when they fit; split between lines, "
                    "preferring top-level definitions",
            "long_blocks": "sentence -> clause -> token boundary",
            "max_document_chars": self.max_document_chars,
            "max_chunks_per_document": self.max_chunks_per_document,
        }

    @property
    def name(self) -> str:
        raw = json.dumps(self.parameters(), sort_keys=True, separators=(",", ":"))
        return f"kbp-chunker-{ALGORITHM_VERSION}-{hashlib.sha256(raw.encode()).hexdigest()[:10]}"


@dataclass(frozen=True)
class Piece:
    start: int
    end: int
    tokens: int
    heading: Block | None = None      # set for heading pieces
    table_header: str | None = None   # set for table continuation pieces


@dataclass
class ChunkCandidate:
    ordinal: int
    char_start: int
    char_end: int
    chunk_text: str
    token_count: int
    heading_path: tuple[str, ...]
    table_header: str | None = None
    rejected: str | None = None

    @property
    def section_path(self) -> str | None:
        """JSON context: {"headings": [...], "table_header": "..."} or None when there is no structure."""
        if not self.heading_path and self.table_header is None:
            return None
        value: dict = {"headings": list(self.heading_path)}
        if self.table_header is not None:
            value["table_header"] = self.table_header
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@dataclass
class _Chunk:
    pieces: list[Piece] = field(default_factory=list)
    path: tuple[str, ...] = ()
    level: int = 7                    # root level: lowest heading level the chunk opens/holds (7 = no heading)

    @property
    def tokens(self) -> int:
        return sum(p.tokens for p in self.pieces)

    @property
    def content_tokens(self) -> int:
        return sum(p.tokens for p in self.pieces if p.heading is None)


# --------------------------------------------------------------------------- #
# Splitting oversize blocks
# --------------------------------------------------------------------------- #

def _trimmed(doc: str, start: int, end: int) -> tuple[int, int] | None:
    while start < end and (doc[start].isspace() or doc[start] == "﻿"):
        start += 1
    while end > start and doc[end - 1].isspace():
        end -= 1
    return (start, end) if start < end else None


def _cut(doc: str, start: int, end: int, offsets: list[int]) -> list[tuple[int, int]]:
    spans, last = [], start
    for off in [*offsets, end]:
        span = _trimmed(doc, last, off)
        if span:
            spans.append(span)
        last = off
    return spans


def _pack(doc: str, spans: list[tuple[int, int]], limit: int, refine) -> list[tuple[int, int]]:
    """Greedy packing of consecutive sub-spans up to ``limit``; oversize sub-spans go to ``refine``."""
    out: list[tuple[int, int]] = []
    cur: tuple[int, int] | None = None
    cur_tokens = 0
    for s, e in spans:
        t = tokenizer.count_tokens(doc[s:e])
        if t > limit:
            if cur:
                out.append(cur)
                cur, cur_tokens = None, 0
            out.extend(refine(s, e))
            continue
        if cur and cur_tokens + t > limit:
            out.append(cur)
            cur, cur_tokens = None, 0
        cur = (cur[0], e) if cur else (s, e)
        cur_tokens += t
    if cur:
        out.append(cur)
    return out


def _token_split(doc: str, start: int, end: int, limit: int) -> list[tuple[int, int]]:
    cuts = [start + c for c in tokenizer.token_boundaries(doc[start:end], limit)]
    return _cut(doc, start, end, cuts)


def _split_prose(doc: str, start: int, end: int, limit: int) -> list[tuple[int, int]]:
    def clauses(s: int, e: int) -> list[tuple[int, int]]:
        parts = _cut(doc, s, e, [s + m.end() for m in _CLAUSE_END.finditer(doc[s:e])])
        return _pack(doc, parts, limit, lambda a, b: _token_split(doc, a, b, limit))

    sentences = _cut(doc, start, end, [start + m.end() for m in _SENTENCE_END.finditer(doc[start:end])])
    return _pack(doc, sentences, limit, clauses)


def _line_starts(doc: str, start: int, end: int, predicate) -> list[int]:
    offsets, pos = [], start
    for line in doc[start:end].split("\n"):
        if pos != start and predicate(line):
            offsets.append(pos)
        pos += len(line) + 1
    return offsets


def _split_block(doc: str, block: Block, limit: int) -> list[Piece]:
    if block.kind == "list":
        items = _cut(doc, block.start, block.end, _line_starts(doc, block.start, block.end, _LIST_START.match))
        spans = _pack(doc, items, limit, lambda s, e: _split_prose(doc, s, e, limit))
    elif block.kind == "table":
        rows = _cut(doc, block.start, block.end, _line_starts(doc, block.start, block.end, lambda _l: True))
        header = doc[rows[0][0]:rows[0][1]]
        spans = _pack(doc, rows, limit, lambda s, e: _token_split(doc, s, e, limit))
        return [Piece(s, e, tokenizer.count_tokens(doc[s:e]), table_header=None if i == 0 or s == rows[0][0]
                      else header) for i, (s, e) in enumerate(spans)]
    elif block.kind in ("fence", "code"):
        lines = _cut(doc, block.start, block.end, _line_starts(doc, block.start, block.end, lambda _l: True))
        # Prefer to start a new piece at a top-level definition: pack definition-led groups first.
        groups = _cut(doc, block.start, block.end,
                      _line_starts(doc, block.start, block.end, lambda l: bool(TOP_LEVEL_CODE.match(l))))
        spans = _pack(doc, groups, limit, lambda s, e: _pack(
            doc, [ln for ln in lines if s <= ln[0] and ln[1] <= e], limit,
            lambda a, b: _token_split(doc, a, b, limit)))
    else:
        spans = _split_prose(doc, block.start, block.end, limit)
    return [Piece(s, e, tokenizer.count_tokens(doc[s:e])) for s, e in spans]


# --------------------------------------------------------------------------- #
# Packing
# --------------------------------------------------------------------------- #

def _pieces(doc: str, blocks: list[Block], config: ChunkingConfig) -> list[Piece]:
    piece_max = config.max_tokens - config.heading_reserve_tokens
    out: list[Piece] = []
    for block in blocks:
        tokens = tokenizer.count_tokens(block.text(doc))
        if block.kind == "heading":
            out.append(Piece(block.start, block.end, tokens, heading=block))
        elif tokens <= piece_max:
            out.append(Piece(block.start, block.end, tokens))
        else:
            out.extend(_split_block(doc, block, piece_max))
    return out


def _pack_chunks(pieces: list[Piece], config: ChunkingConfig) -> list[_Chunk]:
    chunks: list[_Chunk] = []
    path: list[tuple[int, str]] = []
    cur = _Chunk()

    def close() -> None:
        nonlocal cur
        if cur.pieces:
            chunks.append(cur)
        cur = _Chunk(path=tuple(t for _l, t in path), level=path[-1][0] if path else 7)

    for piece in pieces:
        if piece.heading is not None:
            level = piece.heading.level
            # A heading joins the current chunk only when it is nested under the chunk's root heading
            # (tiny sections merge into their own subsections, never into siblings or other sections).
            if cur.pieces and (cur.content_tokens >= config.min_tokens or level <= cur.level
                               or cur.tokens + piece.tokens > config.max_tokens):
                close()
            while path and path[-1][0] >= level:
                path.pop()
            path.append((level, piece.heading.title))
            if not cur.pieces:
                cur.path = tuple(t for _l, t in path)
            cur.level = min(cur.level, level) if cur.pieces else level
            cur.pieces.append(piece)
            continue
        if cur.pieces and cur.content_tokens and (
                (cur.tokens + piece.tokens > config.target_tokens and cur.content_tokens >= config.min_tokens)
                or cur.tokens + piece.tokens > config.max_tokens):
            close()
        elif cur.pieces and not cur.content_tokens and cur.tokens + piece.tokens > config.max_tokens:
            close()  # headings that cannot fit with this piece
        if not cur.content_tokens:
            cur.path = tuple(t for _l, t in path)  # the section in which the chunk's content starts
        cur.pieces.append(piece)
    close()
    # Tail merge: a small chunk joins the previous one when it continues that chunk's section, or opens a
    # subsection or a (non-top-level) sibling section of it; top-level sections never merge.
    merged: list[_Chunk] = []
    for chunk in chunks:
        prev = merged[-1] if merged else None
        heading = chunk.pieces[0].heading
        related = heading is None or (prev is not None and heading.level >= max(prev.level, 2))
        if (prev is not None and chunk.content_tokens < config.min_tokens and related
                and prev.tokens + chunk.tokens <= config.max_tokens and chunk.pieces[0].table_header is None):
            prev.pieces.extend(chunk.pieces)
        else:
            merged.append(chunk)
    return merged


# --------------------------------------------------------------------------- #
# Acceptance
# --------------------------------------------------------------------------- #

def _rejection(doc: str, start: int, end: int, text: str, tokens: int, config: ChunkingConfig,
               covered: list[Block], only: bool) -> str | None:
    """Integrity/usefulness of one chunk, judged on its CONTENT (headings alone carry no knowledge)."""
    if not text:
        return "empty"
    if "\x00" in text:
        return "nul"
    if tokens > config.max_tokens:
        return "over_max_tokens"  # defensive: the packer never produces this
    content_blocks = [b for b in covered if b.kind != "heading"]
    if not content_blocks:
        return "heading_only"
    content = "\n".join(doc[max(b.start, start):min(b.end, end)] for b in content_blocks)
    if not any(ch.isalnum() for ch in content):
        return "no_alphanumeric"
    if len(content_blocks) == 1 and content_blocks[0].kind == "table" and "\n" not in content.strip():
        return "table_fragment"
    if not only and tokenizer.count_tokens(content) < config.hard_min_tokens:
        return "tiny_fragment"
    if len(content.split()) <= 40 and CHROME_PHRASES.match(content.lstrip("-> 0123456789.").strip()):
        return "boilerplate_residue"
    return None


def chunk_document(doc: str, *, kind: str = "prose", config: ChunkingConfig = ChunkingConfig()) -> list[ChunkCandidate]:
    """Deterministic chunk candidates (accepted and rejected) of one canonical Silver text."""
    config.validate()
    if len(doc) > config.max_document_chars:
        raise ChunkingLimit("document_too_large", f"document exceeds {config.max_document_chars} characters")
    if "\x00" in doc:
        raise ChunkingLimit("document_contains_nul", "Silver text contains U+0000 (corrupt row)")
    blocks = parse_blocks(doc, kind)
    chunks = _pack_chunks(_pieces(doc, blocks, config), config)
    if len(chunks) > config.max_chunks_per_document:
        raise ChunkingLimit("chunk_count_limit", f"document would produce {len(chunks)} chunks "
                            f"(limit {config.max_chunks_per_document})")
    out: list[ChunkCandidate] = []
    for ordinal, chunk in enumerate(chunks):
        span = _trimmed(doc, chunk.pieces[0].start, chunk.pieces[-1].end)
        start, end = span if span else (chunk.pieces[0].start, chunk.pieces[-1].end)
        text = canonicalize_text(doc[start:end])
        tokens = tokenizer.count_tokens(text)
        covered = [b for b in blocks if b.start < end and start < b.end]
        candidate = ChunkCandidate(ordinal, start, end, text, tokens, chunk.path, chunk.pieces[0].table_header)
        candidate.rejected = _rejection(doc, start, end, text, tokens, config, covered, only=len(chunks) == 1)
        out.append(candidate)
    return out

