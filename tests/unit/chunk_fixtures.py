"""Offline fixtures for Chunking V1 tests (no database).

``InMemoryChunkStore`` implements ``ChunkStore`` with the Schema V1 rules that
matter: chunk_registry PK(chunk_id) + UNIQUE(chunk_hash) + hash/char_length
CHECKs + FKs; chunk_occurrences PK(document_id, version, ordinal) + CHECK
0 <= start < end + FK to chunk_registry; ON CONFLICT DO NOTHING; ``atomic()``
rollback; a lock so concurrent writers behave like serialized inserts.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import threading
import uuid

from app.processing.chunking import tokenizer
from app.processing.chunking.chunker import ChunkingConfig
from app.processing.chunking.repository import ChunkRow, OccurrenceRow, SilverDocument
from app.processing.normalization.text import canonicalize_text
from app.utils.ids import document_content_hash, document_id_from_hash

OBSERVATION = uuid.UUID("00000000-0000-4000-8000-000000000001")


def silver_document(text: str, *, kind: str = "html", status: str = "accepted") -> SilverDocument:
    canonical = canonicalize_text(text)
    digest = document_content_hash(canonical)
    return SilverDocument(document_id_from_hash(digest), bytes.fromhex(digest), canonical, len(canonical), status,
                          f"accepted|01_integrity=PASS:bronze_verified;03_mime=PASS:{kind}", OBSERVATION)


class InMemoryChunkStore:
    _TABLES = ("chunks", "hashes", "occurrences", "dead_letters")

    def __init__(self, documents: set[uuid.UUID] | None = None) -> None:
        self.versions: dict[str, tuple[int, str, dict]] = {}
        self.documents = documents if documents is not None else set()
        self.chunks: dict[uuid.UUID, dict] = {}
        self.hashes: dict[bytes, uuid.UUID] = {}
        self.occurrences: dict[tuple, dict] = {}
        self.dead_letters: dict[tuple, dict] = {}
        self.lock = threading.RLock()
        self.fail_on_occurrence: int | None = None
        self._occurrence_inserts = 0

    # helpers
    def register(self, config: ChunkingConfig) -> int:
        version_id = self.versions.get(config.name, (len(self.versions) + 1,))[0]
        self.versions[config.name] = (version_id, tokenizer.IDENTITY, config.parameters())
        return version_id

    def add_documents(self, *documents: SilverDocument) -> None:
        self.documents.update(d.document_id for d in documents)

    def snapshot(self) -> tuple[int, int, int]:
        return len(self.chunks), len(self.occurrences), len(self.dead_letters)

    def ordered(self, document_id: uuid.UUID, version_id: int) -> list[tuple]:
        return [(o, r["chunk_id"], r["char_start"], r["char_end"], r["section_path"])
                for (d, v, o), r in sorted(self.occurrences.items(), key=lambda kv: kv[0][2])
                if d == document_id and v == version_id]

    @contextlib.contextmanager
    def atomic(self):
        with self.lock:
            saved = {name: copy.deepcopy(getattr(self, name)) for name in self._TABLES}
            try:
                yield None
            except BaseException:
                for name, value in saved.items():
                    setattr(self, name, value)
                raise

    # protocol
    def chunking_version(self, conn, name):
        return self.versions.get(name)

    def lock_document(self, conn, document_id) -> None:
        pass  # the store lock serializes whole transactions (see atomic)

    def existing_occurrences(self, conn, document_id, version_id):
        return self.ordered(document_id, version_id)

    def insert_chunk(self, conn, row: ChunkRow) -> bool:
        with self.lock:
            assert len(row.chunk_hash) == 32
            assert hashlib.sha256(row.chunk_text.encode("utf-8")).digest() == row.chunk_hash, "hash CHECK"
            assert len(row.chunk_text) > 0 and "\x00" not in row.chunk_text
            assert row.document_id in self.documents, "FK first_document_id"
            if row.chunk_id in self.chunks:
                return False
            if row.chunk_hash in self.hashes:
                raise AssertionError("UNIQUE(chunk_hash) violated under a different chunk_id")
            self.chunks[row.chunk_id] = {"chunk_text": row.chunk_text, "token_count": row.token_count,
                                         "version_id": row.version_id, "first_document_id": row.document_id}
            self.hashes[row.chunk_hash] = row.chunk_id
            return True

    def insert_occurrence(self, conn, row: OccurrenceRow) -> bool:
        with self.lock:
            self._occurrence_inserts += 1
            if self.fail_on_occurrence == self._occurrence_inserts:
                raise ConnectionError("injected crash")
            assert 0 <= row.char_start < row.char_end, "CHECK 0 <= char_start < char_end"
            assert row.chunk_id in self.chunks, "FK chunk_id"
            assert row.document_id in self.documents, "FK document_id"
            key = (row.document_id, row.version_id, row.ordinal)
            if key in self.occurrences:
                return False
            self.occurrences[key] = {"chunk_id": row.chunk_id, "char_start": row.char_start,
                                     "char_end": row.char_end, "section_path": row.section_path}
            return True

    def pending_documents(self, conn, version_id, after, limit):
        raise NotImplementedError("backfill tests use BackfillStore")

    def dead_letter(self, conn, *, document_id, error_class, error_code, error_message, job_id, run_id) -> None:
        row = self.dead_letters.setdefault(("document", str(document_id), "SILVER"), {"occurrence_count": 0})
        row.update(error_class=str(error_class), error_code=error_code, error_message=error_message)
        row["occurrence_count"] += 1


class BackfillStore(InMemoryChunkStore):
    """Adds the accepted-document registry so ``pending_documents`` can be answered."""

    def __init__(self, documents: list[SilverDocument]) -> None:
        super().__init__({d.document_id for d in documents})
        self.registry = {d.document_id: d for d in documents}
        self.pending_calls = 0

    def pending_documents(self, conn, version_id, after, limit):
        self.pending_calls += 1
        done = {d for (d, v, _o) in self.occurrences if v == version_id}
        failed = {uuid.UUID(k[1]) for k in self.dead_letters}
        rows = sorted((d for d in self.registry.values()
                       if d.status == "accepted" and d.document_id not in done and d.document_id not in failed
                       and (after is None or d.document_id > after)), key=lambda d: d.document_id)
        return rows[:limit]
