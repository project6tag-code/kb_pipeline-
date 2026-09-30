"""Exact verification and the per-chunk decision (candidate generation never decides).

For a chunk C and its candidate representatives R1..Rk, all read back from
PostgreSQL, each pair is scored by exact Jaccard similarity of the comparison
features:

* NEAR: J >= 0.70 AND equal fact signatures;
* RELATED: J >= 0.30, including high-similarity pairs whose facts differ;
* otherwise nothing.

The decision for C is then:

* NEAR_DUPLICATE of the NEAR match with the highest similarity, ties by
  chunk_hash ascending;
* else RELATED, scored by the best RELATED similarity;
* else UNIQUE, scored by the best similarity seen, or NULL with no candidates.

Relationship edges go to every NEAR and RELATED match, in a fixed order.
Edges are stored as ordered pairs (``chunk_id < related_chunk_id``), as
Migration 001B requires.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from app.processing.chunk_dedup import method
from app.processing.chunk_dedup.facts import FactSignature, fact_signature

NEAR_DUPLICATE, RELATED, UNIQUE = "NEAR_DUPLICATE", "RELATED", "UNIQUE"


@dataclass(frozen=True)
class ChunkText:
    chunk_id: uuid.UUID
    chunk_hash: bytes
    chunk_text: str

    def comparison(self) -> "Comparable":
        return Comparable(self.chunk_id, self.chunk_hash, method.features(self.chunk_text),
                          fact_signature(self.chunk_text))


@dataclass(frozen=True)
class Comparable:
    chunk_id: uuid.UUID
    chunk_hash: bytes
    features: frozenset[int]
    facts: FactSignature


@dataclass(frozen=True)
class Relationship:
    related_chunk_id: uuid.UUID
    relationship_type: str
    similarity: float

    def ordered(self, chunk_id: uuid.UUID) -> tuple[uuid.UUID, uuid.UUID]:
        return (chunk_id, self.related_chunk_id) if chunk_id < self.related_chunk_id \
            else (self.related_chunk_id, chunk_id)


@dataclass(frozen=True)
class Decision:
    classification: str
    representative_chunk_id: uuid.UUID | None
    similarity: float | None
    relationships: tuple[Relationship, ...] = field(default_factory=tuple)
    candidates_verified: int = 0
    fact_blocked: int = 0  # candidates with J >= NEAR threshold kept apart by the fact guard

    def same_decision(self, classification: str, representative: uuid.UUID | None,
                      similarity: float | None) -> bool:
        if (classification, representative) != (self.classification, self.representative_chunk_id):
            return False
        if similarity is None or self.similarity is None:
            return similarity is None and self.similarity is None
        return abs(float(similarity) - self.similarity) <= 10 ** -method.SCORE_DIGITS  # stored as real


def classify_pair(current: Comparable, candidate: Comparable) -> tuple[str | None, float, bool]:
    """(NEAR_DUPLICATE | RELATED | None, exact J, blocked-by-fact-guard)."""
    similarity = method.jaccard(current.features, candidate.features)
    if similarity >= method.NEAR_THRESHOLD:
        if current.facts.compatible(candidate.facts):
            return NEAR_DUPLICATE, similarity, False
        return RELATED, similarity, True
    if similarity >= method.RELATED_THRESHOLD:
        return RELATED, similarity, False
    return None, similarity, False


def decide(current: Comparable, candidates: list[Comparable]) -> Decision:
    seen: set[uuid.UUID] = set()
    scored = []
    blocked = 0
    for candidate in candidates:
        if candidate.chunk_id == current.chunk_id or candidate.chunk_id in seen:
            continue
        seen.add(candidate.chunk_id)
        kind, similarity, was_blocked = classify_pair(current, candidate)
        blocked += was_blocked
        scored.append((kind, similarity, candidate))
    order = sorted(scored, key=lambda s: (-s[1], s[2].chunk_hash))
    relationships = tuple(Relationship(c.chunk_id, kind, method.score(similarity))
                          for kind, similarity, c in order if kind is not None)
    near = [(similarity, c) for kind, similarity, c in order if kind == NEAR_DUPLICATE]
    if near:
        similarity, representative = near[0]
        return Decision(NEAR_DUPLICATE, representative.chunk_id, method.score(similarity), relationships,
                        len(scored), blocked)
    related = [similarity for kind, similarity, _c in order if kind == RELATED]
    if related:
        return Decision(RELATED, None, method.score(related[0]), relationships, len(scored), blocked)
    best = method.score(order[0][1]) if order else None
    return Decision(UNIQUE, None, best, (), len(scored), blocked)
