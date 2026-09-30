"""Chunk writer (one accepted document, inside the caller's transaction) and the backfill runner.

``ChunkWriter.chunk_accepted(conn, document)`` — schema §10.3:

1. verify the Silver row (accepted, text present, hash and id re-derive, no U+0000);
2. take a transaction-scoped advisory lock on the document (two writers of the
   SAME document serialize; different documents never wait on each other);
3. if the document already has occurrences for this chunking version, the
   recomputed chunks must equal them (else an ``internal_bug`` dead letter:
   nondeterminism is reported, never overwritten) -> no-op;
4. otherwise insert every accepted chunk ``ON CONFLICT (chunk_id) DO NOTHING``
   (global exact dedup: concurrent writers of one chunk serialize on the
   unique index, a rolled-back first writer never leaves it missing) and its
   occurrence ``ON CONFLICT (document_id, chunking_version_id, ordinal) DO NOTHING``.

Chunks and occurrences are written in the caller's transaction, so they commit
or roll back together with the document (Silver stage) or the batch (backfill).

``run_backfill`` chunks accepted documents that have no occurrences for this
chunking version, in bounded batches (one transaction each), resumable and
rerunnable: the pending set is derived from the database, so a crash or a rerun
simply continues. Transient database errors retry the batch (bounded).
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.processing.chunking import tokenizer
from app.processing.chunking.chunker import ChunkingConfig, ChunkingLimit, chunk_document
from app.processing.chunking.repository import ChunkRow, ChunkStore, OccurrenceRow, SilverDocument
from app.queue.retry import JobFailure
from app.queue.states import ErrorClass
from app.utils.ids import chunk_content_hash, chunk_id_from_hash, document_id_from_hash

log = logging.getLogger(__name__)

_KIND = re.compile(r"(?:^|;|\|)03_mime=PASS:([a-z_/+.-]+)")
TEXT_KINDS = {"code": "code", "config": "config"}


def silver_kind(status_reason: str | None) -> str:
    """The Silver document kind recorded by gate 03 (``code``/``config`` change block parsing)."""
    match = _KIND.search(status_reason or "")
    return TEXT_KINDS.get(match.group(1), "prose") if match else "prose"


@dataclass(frozen=True)
class ResolvedVersion:
    version_id: int
    name: str


class ChunkWriter:
    def __init__(self, store: ChunkStore, config: ChunkingConfig = ChunkingConfig(), *,
                 run_id: uuid.UUID | None = None) -> None:
        config.validate()
        self.store, self.config, self.run_id = store, config, run_id
        self._version: ResolvedVersion | None = None

    def version(self, conn: Any) -> ResolvedVersion:
        """The registered chunking version for this exact configuration (bootstrap registers it)."""
        if self._version is None:
            found = self.store.chunking_version(conn, self.config.name)
            if found is None:
                raise JobFailure(ErrorClass.INTERNAL_BUG, f"chunking version {self.config.name} is not registered "
                                 "(run: python -m app.processing.chunking.cli register-version "
                                 "--expect-database <db>)", code="chunking_version_not_registered")
            version_id, tokenizer_name, parameters = found
            if parameters != self.config.parameters() or tokenizer_name != tokenizer.IDENTITY:
                raise JobFailure(ErrorClass.INTERNAL_BUG, f"chunking version {self.config.name} is registered with "
                                 "different parameters", code="chunking_version_mismatch")
            self._version = ResolvedVersion(version_id, self.config.name)
        return self._version

    def chunk_accepted(self, conn: Any, document: SilverDocument, *, job_id: int | None = None) -> Counter:
        counts: Counter = Counter()
        version = self.version(conn)
        problem = self._corrupt(document)
        if problem is not None:
            self.store.dead_letter(conn, document_id=document.document_id, error_class=ErrorClass.DATA_INVALID,
                                   error_code=f"chunking_{problem}", error_message=f"{version.name}: {problem}",
                                   job_id=job_id, run_id=self.run_id)
            counts[f"chunk_document_{problem}"] += 1
            return counts
        try:
            candidates = chunk_document(document.canonical_text, kind=silver_kind(document.status_reason),
                                        config=self.config)
        except ChunkingLimit as exc:
            self.store.dead_letter(conn, document_id=document.document_id, error_class=ErrorClass.DATA_INVALID,
                                   error_code=f"chunking_{exc.code}", error_message=f"{version.name}: {exc}",
                                   job_id=job_id, run_id=self.run_id)
            counts[f"chunk_document_{exc.code}"] += 1
            return counts
        accepted = [c for c in candidates if c.rejected is None]
        for c in candidates:
            if c.rejected is not None:
                counts[f"chunk_rejected_{c.rejected}"] += 1
        if not accepted:
            self.store.dead_letter(conn, document_id=document.document_id, error_class=ErrorClass.DATA_INVALID,
                                   error_code="chunking_no_accepted_chunks",
                                   error_message=f"{version.name}: all {len(candidates)} chunk candidates rejected",
                                   job_id=job_id, run_id=self.run_id)
            counts["chunk_document_no_accepted_chunks"] += 1
            return counts
        rows = []
        for c in accepted:
            digest = chunk_content_hash(c.chunk_text)  # locked Identity V1 (canonicalizes, then SHA-256)
            chunk_id = chunk_id_from_hash(digest)
            rows.append((ChunkRow(chunk_id, bytes.fromhex(digest), c.chunk_text, c.token_count, version.version_id,
                                  document.document_id, document.first_observation_id),
                         OccurrenceRow(document.document_id, version.version_id, c.ordinal, chunk_id, c.char_start,
                                       c.char_end, c.section_path)))
        self.store.lock_document(conn, document.document_id)
        existing = self.store.existing_occurrences(conn, document.document_id, version.version_id)
        if existing:
            if [tuple(e) for e in existing] != [o.key() for _c, o in rows]:
                self.store.dead_letter(conn, document_id=document.document_id, error_class=ErrorClass.INTERNAL_BUG,
                                       error_code="chunking_nondeterministic",
                                       error_message=f"{version.name}: recomputed chunks differ from stored "
                                                     "occurrences", job_id=job_id, run_id=self.run_id)
                counts["chunk_document_nondeterministic"] += 1
            else:
                counts["chunk_document_already_chunked"] += 1
            return counts
        for chunk, occurrence in rows:
            counts["chunk_new" if self.store.insert_chunk(conn, chunk) else "chunk_existing"] += 1
            counts["chunk_occurrence_new" if self.store.insert_occurrence(conn, occurrence)
                   else "chunk_occurrence_existing"] += 1
        counts["chunk_document_chunked"] += 1
        return counts

    @staticmethod
    def _corrupt(document: SilverDocument) -> str | None:
        text = document.canonical_text
        if document.status != "accepted":
            return "not_accepted"
        if not text:
            return "no_canonical_text"
        if "\x00" in text:
            return "contains_nul"
        if len(text) != document.char_length:
            return "char_length_mismatch"
        digest = hashlib.sha256(text.encode("utf-8", "surrogatepass")).digest()
        if digest != document.content_hash or document_id_from_hash(digest.hex()) != document.document_id:
            return "identity_mismatch"
        return None


# --------------------------------------------------------------------------- #
# Backfill
# --------------------------------------------------------------------------- #

@dataclass
class BackfillReport:
    chunking_version: str = ""
    documents: int = 0
    batches: int = 0
    retries: int = 0
    stopped: bool = False
    counts: Counter = field(default_factory=Counter)


def _transient(exc: BaseException) -> bool:
    try:
        from sqlalchemy.exc import DBAPIError, OperationalError
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, OperationalError) or (isinstance(exc, DBAPIError) and exc.connection_invalidated)


def run_backfill(transaction: Callable[[], Any], writer: ChunkWriter, *, limit: int, batch_size: int = 25,
                 max_attempts: int = 3, stop_event: threading.Event | None = None,
                 sleep: Callable[[float], None] = time.sleep, is_transient: Callable[[BaseException], bool] = _transient,
                 progress: Callable[[BackfillReport], None] | None = None) -> BackfillReport:
    """Chunk up to ``limit`` accepted, not-yet-chunked documents in batches of ``batch_size``."""
    if not 1 <= batch_size <= 500 or limit < 1:
        raise ValueError("batch_size must be 1..500 and limit >= 1")
    report = BackfillReport()
    after: uuid.UUID | None = None
    while report.documents < limit:
        if stop_event is not None and stop_event.is_set():
            report.stopped = True
            break
        size = min(batch_size, limit - report.documents)
        for attempt in range(1, max_attempts + 1):
            batch: Counter = Counter()
            try:
                with transaction() as conn:
                    report.chunking_version = writer.version(conn).name
                    documents = writer.store.pending_documents(conn, writer.version(conn).version_id, after, size)
                    for document in documents:
                        batch.update(writer.chunk_accepted(conn, document))
                break
            except Exception as exc:
                if attempt == max_attempts or not is_transient(exc):
                    raise
                report.retries += 1
                log.warning("chunk backfill batch failed transiently (attempt %s): %s", attempt, type(exc).__name__)
                sleep(min(30.0, 2.0 ** attempt))
        if not documents:
            break
        after = documents[-1].document_id
        report.documents += len(documents)
        report.batches += 1
        report.counts.update(batch)
        if progress is not None:
            progress(report)
    return report
