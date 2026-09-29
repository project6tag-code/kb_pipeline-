"""Gate results, failure classes and the in-memory evaluation of one observation."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.queue.states import ErrorClass, Stage


class Outcome(enum.StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNRESOLVED = "UNRESOLVED"
    QUARANTINE = "QUARANTINE"


class FailureKind(enum.StrEnum):
    """How a non-passing result is handled (spec §23)."""

    TRANSIENT = "TRANSIENT"                  # retried by the Runtime Engine (unit level)
    PERMANENT_INVALID = "PERMANENT_INVALID"  # malformed/corrupt input
    POLICY_REJECT = "POLICY_REJECT"          # deterministic rejection by a policy gate
    QUARANTINE = "QUARANTINE"                # held; sensitive or unresolved
    INTERNAL_ERROR = "INTERNAL_ERROR"        # a bug; dead-lettered, retried on reprocess


ERROR_CLASS: dict[FailureKind, ErrorClass] = {
    FailureKind.TRANSIENT: ErrorClass.TRANSIENT,
    FailureKind.PERMANENT_INVALID: ErrorClass.DATA_INVALID,
    FailureKind.QUARANTINE: ErrorClass.POLICY_QUARANTINE,
    FailureKind.INTERNAL_ERROR: ErrorClass.INTERNAL_BUG,
}


class Gate(enum.StrEnum):
    """The 22 quality gates, in order (value = persisted short name)."""

    INTEGRITY = "01_integrity"
    PAYLOAD = "02_payload"
    MIME = "03_mime"
    DECODE = "04_decode"
    PARSE = "05_parse"
    ACTIVE = "06_active_hidden"
    BOILERPLATE = "07_boilerplate"
    CHROME = "08_chrome"
    ADS = "09_ads"
    STRUCTURE = "10_structure"
    CANONICAL = "11_canonical"
    CORRUPTION = "12_corruption"
    LANGUAGE = "13_language"
    PRIVACY = "14_privacy"
    SECRETS = "15_secrets"
    NOISE = "16_noise"
    QUALITY = "17_quality"
    SUITABILITY = "18_suitability"
    EXACT_DEDUP = "19_exact_dedup"
    NEAR_DEDUP = "20_near_dedup"
    DOMAIN = "21_domain"
    FINAL = "22_final"


# The last completed stage (schema §16.3) when a gate fails: the ``dead_letters.stage``.
GATE_STAGE: dict[Gate, Stage] = {
    Gate.INTEGRITY: Stage.ACQUIRED, Gate.PAYLOAD: Stage.ACQUIRED, Gate.MIME: Stage.ACQUIRED,
    Gate.DECODE: Stage.ACQUIRED, Gate.PARSE: Stage.ACQUIRED, Gate.ACTIVE: Stage.PARSED,
    Gate.BOILERPLATE: Stage.PARSED, Gate.CHROME: Stage.PARSED, Gate.ADS: Stage.PARSED,
    Gate.STRUCTURE: Stage.PARSED, Gate.CANONICAL: Stage.PARSED, Gate.CORRUPTION: Stage.NORMALIZED,
    Gate.LANGUAGE: Stage.NORMALIZED, Gate.PRIVACY: Stage.LANGUAGE_VALIDATED,
    Gate.SECRETS: Stage.LANGUAGE_VALIDATED, Gate.NOISE: Stage.LANGUAGE_VALIDATED,
    Gate.QUALITY: Stage.LANGUAGE_VALIDATED, Gate.SUITABILITY: Stage.QUALITY_VALIDATED,
    Gate.EXACT_DEDUP: Stage.QUALITY_VALIDATED, Gate.NEAR_DEDUP: Stage.EXACT_DEDUP_VALIDATED,
    Gate.DOMAIN: Stage.SOURCE_CLASSIFIED, Gate.FINAL: Stage.DOMAIN_CLASSIFIED,
}


@dataclass(frozen=True)
class GateResult:
    gate: Gate
    outcome: Outcome
    version: str
    reasons: tuple[str, ...] = ()
    score: float | None = None
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.outcome is Outcome.PASS

    def code(self) -> str:
        """Compact, machine-readable ``gate=OUTCOME[:score][:reason,...]`` (never contains source text)."""
        parts = [f"{self.gate.value}={self.outcome.value}"]
        if self.score is not None:
            parts.append(f"{self.score:.3f}")
        if self.reasons:
            parts.append(",".join(self.reasons))
        return ":".join(parts)


class ExtractionOutcome(enum.StrEnum):
    """``kbp.observation_extractions.outcome`` (schema §5.11)."""

    DOCUMENT = "document"
    EMPTY = "empty"
    REJECTED_LANGUAGE = "rejected_language"
    REJECTED_PREFILTER = "rejected_prefilter"
    REJECTED_NUL = "rejected_nul"
    EXTRACTION_FAILED = "extraction_failed"


class DocumentStatus(enum.StrEnum):
    """``kbp.document_registry.status`` (schema §5.12)."""

    ACCEPTED = "accepted"
    REJECTED_QUALITY = "rejected_quality"
    REJECTED_NEAR_DUPLICATE = "rejected_near_duplicate"
    REJECTED_FACTUALITY = "rejected_factuality"
    REJECTED_POLICY = "rejected_policy"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class SourceRef:
    """Immutable source coordinates of one observation, as known by acquisition."""

    observation_id: uuid.UUID
    channel: str                       # 'common_crawl' | 'github'
    source_url: str
    # Common Crawl
    crawl_id: str | None = None
    warc_filename: str | None = None
    warc_offset: int | None = None
    warc_length: int | None = None
    # GitHub
    repository_id: int | None = None
    commit_sha: str | None = None
    path: str | None = None
    blob_sha: str | None = None

    @property
    def lineage_key(self) -> str:
        """Same logical source across versions: the URL (CC) or repository + path (GitHub)."""
        if self.channel == "github":
            return f"github:{self.repository_id}:{self.path}"
        return f"url:{self.source_url}"


@dataclass
class Evaluation:
    """Everything the in-memory gates (1-18, 21) decided for one observation."""

    ref: SourceRef
    results: list[GateResult] = field(default_factory=list)
    outcome: ExtractionOutcome | None = None       # set once the extraction outcome is known
    failure: FailureKind | None = None             # an item failure (dead letter), if any
    failure_gate: Gate | None = None
    failure_code: str | None = None
    failure_message: str | None = None             # safe: never contains source text or secrets
    record_extraction: bool = True                 # False: nothing durable except a dead letter (retryable)
    canonical_text: str | None = None
    content_hash: str | None = None
    document_id: uuid.UUID | None = None
    shingle_signature: tuple[int, ...] | None = None
    shingles: frozenset[int] | None = None
    text_kind: str = "prose"                       # 'prose' | 'markdown' | 'code' | 'config'

    def add(self, result: GateResult) -> GateResult:
        self.results.append(result)
        return result

    def result(self, gate: Gate) -> GateResult | None:
        return next((r for r in self.results if r.gate is gate), None)

    def fail(self, gate: Gate, kind: FailureKind, code: str, message: str, *,
             outcome: ExtractionOutcome | None, record_extraction: bool = True) -> "Evaluation":
        self.failure, self.failure_gate, self.failure_code = kind, gate, code
        self.failure_message = message[:500]
        self.outcome = outcome
        self.record_extraction = record_extraction and outcome is not None
        return self

    @property
    def registrable(self) -> bool:
        return self.outcome is ExtractionOutcome.DOCUMENT and self.document_id is not None
