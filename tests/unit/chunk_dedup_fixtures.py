"""Offline fixtures for Chunk Near-Dedup V1 tests (no database).

``InMemoryChunkDedupStore`` implements ``ChunkDedupStore`` with the Migration
001B rules that matter:

* decisions: PK (chunk_id, method), FK to chunk_registry, CHECK
  representative iff NEAR_DUPLICATE, CHECK not self, and the INSERT-level
  guard that a representative holds a confirmed non-NEAR decision;
* relationships: PK (chunk_id, related_chunk_id, method), CHECK
  chunk_id < related_chunk_id, FKs;
* ON CONFLICT DO NOTHING.

``transaction()`` is atomic: rollback restores every table. It also
serializes whole transactions, like the method's advisory lock. Failures can
be injected into any store call.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import threading
import uuid

from app.processing.chunk_dedup import method
from app.processing.chunk_dedup.verify import NEAR_DUPLICATE, ChunkText
from app.utils.ids import chunk_content_hash, chunk_id_from_hash


def chunk(text: str) -> ChunkText:
    digest = chunk_content_hash(text)
    return ChunkText(chunk_id_from_hash(digest), bytes.fromhex(digest), text)


class InjectedCrash(ConnectionError):
    pass


class InMemoryChunkDedupStore:
    _TABLES = ("decisions", "relationships", "dead_letters", "_sequence")

    def __init__(self, chunks=(), *, registered: bool = True, parameters: dict | None = None) -> None:
        self.registry: dict[uuid.UUID, ChunkText] = {}
        self.occurrences: dict[uuid.UUID, list[tuple[str, int]]] = {}
        self.registered = registered
        self.parameters = parameters if parameters is not None else method.PARAMETERS
        self.decisions: dict[uuid.UUID, dict] = {}
        self.relationships: dict[tuple[uuid.UUID, uuid.UUID], dict] = {}
        self.dead_letters: dict[str, dict] = {}
        self._sequence = 0
        self.lock = threading.RLock()
        self.fail: dict[str, int] = {}  # call name -> fail on the Nth call
        self.calls: dict[str, int] = {}
        self.method_locks = 0
        for c in chunks:
            self.add_chunk(c)

    # ------------------------------------------------------------ helpers
    def add_chunk(self, c: ChunkText, *, document: str = "doc", ordinal: int = 0) -> ChunkText:
        assert hashlib.sha256(c.chunk_text.encode("utf-8")).digest() == c.chunk_hash, "hash CHECK"
        self.registry[c.chunk_id] = c
        self.occurrences.setdefault(c.chunk_id, []).append((document, ordinal))
        return c

    def _tick(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1
        if self.fail.get(name) == self.calls[name]:
            raise InjectedCrash(f"injected crash in {name}")

    @contextlib.contextmanager
    def transaction(self):
        with self.lock:
            saved = {name: copy.deepcopy(getattr(self, name)) for name in self._TABLES}
            try:
                yield None
            except BaseException:
                for name, value in saved.items():
                    setattr(self, name, value)
                raise

    def state(self) -> tuple:
        """Everything durable, independent of run ids and timestamps."""
        return (tuple(sorted((str(k), v["classification"], str(v["representative"]), v["similarity"])
                             for k, v in self.decisions.items())),
                tuple(sorted((str(a), str(b), v["type"], v["similarity"]) for (a, b), v in self.relationships.items())),
                tuple(sorted(self.dead_letters)))

    def audit(self) -> dict[str, int]:
        near = {k: v for k, v in self.decisions.items() if v["classification"] == NEAR_DUPLICATE}
        return {
            "representative_is_suppressed_violations": sum(
                1 for v in near.values() if self.decisions.get(v["representative"], {}).get("classification")
                in (None, NEAR_DUPLICATE)),
            "near_without_representative_edge_violations": sum(
                1 for k, v in near.items()
                if self.relationships.get(tuple(sorted((k, v["representative"]))), {}).get("type") != NEAR_DUPLICATE),
            "edge_without_decisions_violations": sum(
                1 for a, b in self.relationships if a not in self.decisions or b not in self.decisions),
            "near_edge_between_representatives_violations": sum(
                1 for (a, b), v in self.relationships.items() if v["type"] == NEAR_DUPLICATE
                and self.decisions[a]["classification"] != NEAR_DUPLICATE
                and self.decisions[b]["classification"] != NEAR_DUPLICATE),
        }

    def decisions_in_order(self) -> list[tuple]:
        return [(k, self.registry[k].chunk_hash, self.registry[k].chunk_text, v["classification"],
                 v["representative"], v["similarity"])
                for k, v in sorted(self.decisions.items(), key=lambda kv: kv[1]["seq"])]

    def stored_relationships(self) -> list[tuple]:
        return [(a, b, v["type"], v["similarity"]) for (a, b), v in self.relationships.items()]

    # ----------------------------------------------------------- protocol
    def method(self, conn):
        return (1, self.parameters) if self.registered else None

    def lock_method(self, conn, method_id) -> None:
        self.method_locks += 1  # transaction() already serializes

    def pending_chunks(self, conn, method_id, after, limit):
        self._tick("pending_chunks")
        rows = sorted((c for c in self.registry.values() if c.chunk_id not in self.decisions
                       and str(c.chunk_id) not in self.dead_letters and c.chunk_hash > after),
                      key=lambda c: c.chunk_hash)
        return rows[:limit]

    def resolve_representatives(self, conn, method_id, keys):
        wanted = set(keys)
        return sorted((c for c in self.registry.values() if method.item_key(c.chunk_hash) in wanted
                       and self.decisions.get(c.chunk_id, {}).get("classification") not in (None, NEAR_DUPLICATE)),
                      key=lambda c: c.chunk_hash)

    def insert_decision(self, conn, method_id, chunk_id, decision, run_id) -> bool:
        self._tick("insert_decision")
        assert chunk_id in self.registry, "FK chunk_id"
        rep = decision.representative_chunk_id
        assert (decision.classification == NEAR_DUPLICATE) == (rep is not None), "CHECK representative"
        assert rep != chunk_id, "CHECK not self"
        assert decision.classification == "UNIQUE" or decision.similarity is not None, "CHECK score"
        assert run_id is not None, "NOT NULL run_id"
        if rep is not None and self.decisions.get(rep, {}).get("classification") in (None, NEAR_DUPLICATE):
            return False  # the INSERT ... WHERE guard selects no row
        if chunk_id in self.decisions:
            return False
        self._sequence += 1
        self.decisions[chunk_id] = {"classification": decision.classification, "representative": rep,
                                    "similarity": decision.similarity, "status": "confirmed", "run_id": run_id,
                                    "seq": self._sequence}
        return True

    def get_decision(self, conn, method_id, chunk_id):
        from app.processing.chunk_dedup.repository import StoredDecision

        row = self.decisions.get(chunk_id)
        return None if row is None else StoredDecision(row["classification"], row["representative"],
                                                       row["similarity"], row["status"])

    def insert_relationship(self, conn, method_id, chunk_id, relationship, run_id) -> bool:
        self._tick("insert_relationship")
        low, high = relationship.ordered(chunk_id)
        assert low < high, "CHECK chunk_id < related_chunk_id"
        assert low in self.registry and high in self.registry, "FK"
        if (low, high) in self.relationships:
            return False
        self.relationships[(low, high)] = {"type": relationship.relationship_type,
                                           "similarity": relationship.similarity, "run_id": run_id}
        return True

    def get_relationship_type(self, conn, method_id, pair):
        row = self.relationships.get(pair)
        return None if row is None else row["type"]

    def representative_count(self, conn, method_id) -> int:
        return sum(1 for v in self.decisions.values() if v["classification"] != NEAR_DUPLICATE)

    def representatives_page(self, conn, method_id, after, limit):
        reps = sorted((self.registry[k].chunk_hash, self.registry[k].chunk_text) for k, v in self.decisions.items()
                      if v["classification"] != NEAR_DUPLICATE and self.registry[k].chunk_hash > after)
        return reps[:limit]

    def dead_letter(self, conn, *, chunk_id, error_code, error_message, run_id) -> None:
        self.dead_letters[str(chunk_id)] = {"error_code": error_code, "error_message": error_message}
