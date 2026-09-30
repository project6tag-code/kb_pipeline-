"""Future embedding eligibility under a chunk near-dedup method (read-only; no embeddings are created here).

For the active method (``resolve_method_id``):

* **eligible**: the chunk has a ``confirmed`` decision whose classification is
  not NEAR_DUPLICATE (UNIQUE or RELATED): it is a representative, so embed it;
* **suppressed**: a ``confirmed`` NEAR_DUPLICATE. Do not embed it; its
  ``representative_chunk_id`` is the chunk that is embedded instead;
* **undecided** / **not_confirmed**: not eligible yet (no decision, or a
  ``proposed``/``overridden`` one needs review).

``chunk_registry`` is not touched. Suppression only changes eligibility:
occurrences, documents and source provenance of every chunk stay intact.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from app.processing.chunk_dedup import method

ELIGIBLE, SUPPRESSED, UNDECIDED, NOT_CONFIRMED = "eligible", "suppressed", "undecided", "not_confirmed"

METHOD_ID_SQL = """
SELECT dedup_method_id FROM kbp.dedup_methods WHERE algorithm = :algorithm AND algorithm_version = :version
"""

ELIGIBILITY_SQL = """
SELECT i.chunk_id, d.classification, d.representative_chunk_id, d.decision_status
FROM unnest(CAST(:chunk_ids AS uuid[])) AS i(chunk_id)
LEFT JOIN kbp.chunk_dedup_decisions AS d ON d.chunk_id = i.chunk_id AND d.dedup_method_id = :method_id
"""

ELIGIBLE_CHUNKS_SQL = """
SELECT d.chunk_id
FROM kbp.chunk_dedup_decisions AS d
WHERE d.dedup_method_id = :method_id AND d.classification <> 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
  AND d.chunk_id > CAST(:after AS uuid)
ORDER BY d.chunk_id
LIMIT :limit
"""

SUPPRESSED_CHUNKS_SQL = """
SELECT d.chunk_id, d.representative_chunk_id, d.similarity_score
FROM kbp.chunk_dedup_decisions AS d
WHERE d.dedup_method_id = :method_id AND d.classification = 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
  AND d.chunk_id > CAST(:after AS uuid)
ORDER BY d.chunk_id
LIMIT :limit
"""

_MIN_UUID = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True)
class Eligibility:
    chunk_id: uuid.UUID
    status: str
    representative_chunk_id: uuid.UUID | None

    @property
    def eligible(self) -> bool:
        return self.status == ELIGIBLE

    @property
    def embed_chunk_id(self) -> uuid.UUID | None:
        """The chunk whose embedding serves this chunk (itself, or its representative)."""
        return self.chunk_id if self.status == ELIGIBLE else \
            self.representative_chunk_id if self.status == SUPPRESSED else None


def classify(chunk_id: uuid.UUID, classification: str | None, representative: uuid.UUID | None,
             decision_status: str | None) -> Eligibility:
    if classification is None:
        return Eligibility(chunk_id, UNDECIDED, None)
    if decision_status != "confirmed":
        return Eligibility(chunk_id, NOT_CONFIRMED, None)
    if classification == "NEAR_DUPLICATE":
        return Eligibility(chunk_id, SUPPRESSED, representative)
    return Eligibility(chunk_id, ELIGIBLE, None)


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


def resolve_method_id(conn: Any, algorithm: str = method.ALGORITHM, version: str = method.VERSION) -> int:
    found = conn.execute(_text(METHOD_ID_SQL), {"algorithm": algorithm, "version": version}).scalar_one_or_none()
    if found is None:
        raise LookupError(f"dedup method {algorithm}:{version} is not registered")
    return int(found)


def eligibility(conn: Any, method_id: int, chunk_ids: list[uuid.UUID]) -> dict[uuid.UUID, Eligibility]:
    rows = conn.execute(_text(ELIGIBILITY_SQL), {"chunk_ids": list(chunk_ids), "method_id": method_id})
    return {r[0]: classify(r[0], r[1], r[2], r[3]) for r in rows}


def eligible_chunks(conn: Any, method_id: int, *, after: uuid.UUID | None = None, limit: int = 1000) -> list[uuid.UUID]:
    return [r[0] for r in conn.execute(_text(ELIGIBLE_CHUNKS_SQL), {
        "method_id": method_id, "after": str(after) if after else _MIN_UUID, "limit": limit})]


def suppressed_chunks(conn: Any, method_id: int, *, after: uuid.UUID | None = None,
                      limit: int = 1000) -> list[tuple[uuid.UUID, uuid.UUID, float]]:
    return [(r[0], r[1], float(r[2])) for r in conn.execute(_text(SUPPRESSED_CHUNKS_SQL), {
        "method_id": method_id, "after": str(after) if after else _MIN_UUID, "limit": limit})]
