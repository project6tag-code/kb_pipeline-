"""Bronze -> Silver processor: gates 19, 20 and 22 plus persistence (schema §9.3).

Used by the acquisition work units (``cc_index_partition``,
``gh_repository_snapshot``) as their Silver stage, in two steps per batch:

* ``prepare(ctx, refs)``: outside any transaction. Skips observations already
  extracted under this ``extraction_version`` and runs the in-memory gates
  (01-18, 21) on verified Bronze. CPU only; nothing durable.
* ``persist(conn, prepared, ...)``: inside the SAME transaction that inserts
  the batch's observations and advances the job checkpoint, so observation,
  extraction, document, dedup decisions and checkpoint commit atomically.

Inside ``persist`` a transaction-scoped advisory lock serializes the registry
section across workers, so exact-duplicate lookups and near-duplicate
candidate search see every committed or same-transaction document:

* 19 exact dedup: the document id is the Identity V1 hash of the Silver text;
  an existing id is an exact duplicate -> ``document_was_new = FALSE``, no
  re-evaluation (§9.3). Cross-source duplicates collapse the same way.
* 20 near dedup: only for documents that passed every other gate (for the
  rest it is "not required"); decision + relationships are written.
* 22 final gate: the status is decided once, from all gate results, with
  explicit precedence; only a document with every mandatory check passing
  becomes ``accepted``.

Reruns are no-ops: extraction rows are keyed by (observation, version) and a
rerun that would link a different document/outcome is reported as an
``internal_bug`` dead letter (nondeterminism), never overwritten.
"""

from __future__ import annotations

import logging
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from app.processing.dedup import near
from app.processing.silver import config as versions
from app.processing.silver.config import SilverConfig
from app.processing.silver.model import (
    ERROR_CLASS,
    GATE_STAGE,
    DocumentStatus,
    Evaluation,
    ExtractionOutcome,
    FailureKind,
    Gate,
    GateResult,
    Outcome,
    SourceRef,
)
from app.processing.silver.pipeline import SilverEvaluator
from app.processing.silver.repository import DocumentRow, SilverStore
from app.queue.retry import JobFailure
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, Stage
from app.utils.hashing import sha256_text
from app.utils.ids import document_id_from_hash

log = logging.getLogger(__name__)

MAX_STATUS_REASON_CHARS = 2000
MANDATORY_PASS = (Gate.INTEGRITY, Gate.PAYLOAD, Gate.MIME, Gate.DECODE, Gate.PARSE, Gate.ACTIVE, Gate.BOILERPLATE,
                  Gate.CHROME, Gate.ADS, Gate.STRUCTURE, Gate.CANONICAL, Gate.CORRUPTION, Gate.LANGUAGE,
                  Gate.PRIVACY, Gate.SECRETS, Gate.NOISE, Gate.QUALITY, Gate.SUITABILITY, Gate.DOMAIN)


@dataclass
class Prepared:
    evaluations: list[Evaluation] = field(default_factory=list)
    skipped: int = 0


@dataclass(frozen=True)
class FinalDecision:
    status: DocumentStatus
    primary: str
    keep_text: bool
    quarantine_code: str | None = None     # sensitive quarantine -> dead letter (policy_quarantine)


def final_gate(ev: Evaluation, near_decision: near.NearDecision | None, config: SilverConfig) -> FinalDecision:
    """Gate 22: one status from all gate results, strictest first. Pure."""
    policy = config.policy
    get = ev.result
    secret, privacy = get(Gate.SECRETS), get(Gate.PRIVACY)
    if secret is None or privacy is None:
        return FinalDecision(DocumentStatus.QUARANTINED, "final:missing_policy_result", False, "missing_policy_result")
    if secret.outcome is Outcome.QUARANTINE:
        return FinalDecision(DocumentStatus.QUARANTINED, "secret:high_confidence", False, "secret_detected")
    if secret.outcome is Outcome.FAIL:
        return FinalDecision(DocumentStatus.REJECTED_POLICY, "secret:credential_assignment", False)
    if privacy.outcome is Outcome.FAIL:
        return FinalDecision(DocumentStatus.REJECTED_POLICY, "privacy:high_risk_identifier", False)
    if privacy.outcome is Outcome.QUARANTINE:
        return FinalDecision(DocumentStatus.QUARANTINED, "privacy:identifier_density", False, "pii_density")
    keep_rejected = policy.retain_rejected_text
    for gate, status, name in ((Gate.NOISE, DocumentStatus.REJECTED_QUALITY, "noise"),
                               (Gate.QUALITY, DocumentStatus.REJECTED_QUALITY, "quality"),
                               (Gate.SUITABILITY, DocumentStatus.REJECTED_FACTUALITY, "suitability")):
        result = get(gate)
        if result is None:
            return FinalDecision(DocumentStatus.QUARANTINED, f"final:missing_{name}", False, f"missing_{name}")
        if result.outcome is Outcome.FAIL:
            reason = result.reasons[0] if result.reasons else "below_threshold"
            return FinalDecision(status, f"{name}:{reason}", keep_rejected)
        if result.outcome is Outcome.UNRESOLVED:
            return FinalDecision(DocumentStatus.QUARANTINED, f"unresolved:{name}", policy.retain_unresolved_text)
    missing = [g.value for g in MANDATORY_PASS if get(g) is None or get(g).outcome is not Outcome.PASS]
    if missing:
        return FinalDecision(DocumentStatus.QUARANTINED, f"final:gate_not_passed:{missing[0]}", False,
                             "gate_not_passed")
    if near_decision is None:
        return FinalDecision(DocumentStatus.QUARANTINED, "final:near_dedup_not_evaluated", False,
                             "near_dedup_not_evaluated")
    if near_decision.classification == near.NEAR_DUPLICATE:
        return FinalDecision(DocumentStatus.REJECTED_NEAR_DUPLICATE, "near_duplicate", keep_rejected)
    # Identity, NUL and language invariants are re-verified on the exact text to be stored.
    text = ev.canonical_text or ""
    if (not text or "\x00" in text or sha256_text(text) != ev.content_hash
            or document_id_from_hash(ev.content_hash) != ev.document_id
            or get(Gate.LANGUAGE).data.get("decision") != "ENGLISH"):
        return FinalDecision(DocumentStatus.QUARANTINED, "final:identity_invariant", False, "identity_invariant")
    return FinalDecision(DocumentStatus.ACCEPTED, "accepted", True)


def _status_reason(primary: str, results: list[GateResult]) -> str:
    reason = primary + "|" + ";".join(r.code() for r in results)
    return reason if len(reason) <= MAX_STATUS_REASON_CHARS else reason[:MAX_STATUS_REASON_CHARS - 3] + "..."


class SilverProcessor:
    def __init__(self, config: SilverConfig, store: SilverStore, near_index: near.NearIndex, *, cc_bronze=None,
                 gh_bronze=None, run_id: uuid.UUID | None = None, evaluator: SilverEvaluator | None = None) -> None:
        config.validate()
        self.config, self.store, self.index, self.run_id = config, store, near_index, run_id
        self.evaluator = evaluator or SilverEvaluator(config, cc_bronze=cc_bronze, gh_bronze=gh_bronze)
        self.extraction_version = config.extraction_version
        self.policy_version = config.policy_version
        self.component_versions = dict(config.component_versions(), extraction_version=self.extraction_version,
                                       policy_version=self.policy_version)

    # -- step 1: outside the transaction ------------------------------------------ #

    def prepare(self, ctx: Any, refs: list[SourceRef]) -> Prepared:
        if not refs:
            return Prepared()
        with ctx.transaction() as conn:
            done = self.store.existing_extractions(conn, [r.observation_id for r in refs], self.extraction_version)
        prepared = Prepared(skipped=sum(1 for r in refs if r.observation_id in done))
        for ref in refs:
            if ref.observation_id not in done:
                prepared.evaluations.append(self.evaluator.evaluate(ref))
                if hasattr(ctx, "heartbeat") and len(prepared.evaluations) % 25 == 0:
                    ctx.heartbeat()
        return prepared

    # -- step 2: inside the batch transaction --------------------------------------- #

    def persist(self, conn: Any, prepared: Prepared, *, job_id: int | None, attempt_count: int) -> Counter:
        counts: Counter = Counter(silver_already_extracted=prepared.skipped)
        evaluations = prepared.evaluations
        if not evaluations:
            return counts
        ids = [ev.ref.observation_id for ev in evaluations]
        existing = self.store.existing_extractions(conn, ids, self.extraction_version)
        observed = self.store.existing_observations(conn, ids)
        method_id = None
        if any(ev.registrable and ev.ref.observation_id not in existing for ev in evaluations):
            self.store.lock_registry(conn)
            method_id = self.store.dedup_method_id(conn, near.ALGORITHM, near.VERSION)
            if method_id is None:
                raise JobFailure(ErrorClass.INTERNAL_BUG, f"dedup method {near.ALGORITHM}:{near.VERSION} is not "
                                 "registered (run: python -m app.processing.silver.cli register-methods)",
                                 code="dedup_method_not_registered")
        for ev in evaluations:
            oid = ev.ref.observation_id
            if oid in existing:
                self._verify_rerun(conn, ev, existing[oid], counts, job_id, attempt_count)
                continue
            if oid not in observed:
                counts["silver_not_observed"] += 1
                continue
            if ev.outcome is None or not ev.record_extraction:
                self._item_dead_letter(conn, ev, job_id, attempt_count)
                counts[f"silver_dead_letter_{ev.failure_code}"] += 1
                continue
            if ev.outcome is not ExtractionOutcome.DOCUMENT:
                self.store.insert_extraction(conn, oid, self.extraction_version, ev.outcome.value, None, None)
                counts[f"silver_{ev.outcome.value}"] += 1
                if ev.failure is FailureKind.PERMANENT_INVALID:
                    self._item_dead_letter(conn, ev, job_id, attempt_count)
                continue
            counts.update(self._register(conn, ev, method_id, job_id, attempt_count))
        log.info("silver batch: %s", dict(sorted((k, v) for k, v in counts.items() if v)))
        return counts

    # -- helpers ---------------------------------------------------------------------- #

    def _register(self, conn: Any, ev: Evaluation, method_id: int, job_id: int | None,
                  attempt_count: int) -> Counter:
        counts: Counter = Counter()
        oid, doc_id = ev.ref.observation_id, ev.document_id
        if doc_id in self.store.existing_documents(conn, [doc_id]):
            ev.add(GateResult(Gate.EXACT_DEDUP, Outcome.PASS, versions.EXACT_DEDUP_VERSION, ("exact_duplicate",)))
            self.store.insert_extraction(conn, oid, self.extraction_version, "document", doc_id, False)
            counts["silver_exact_duplicate"] += 1
            return counts
        ev.add(GateResult(Gate.EXACT_DEDUP, Outcome.PASS, versions.EXACT_DEDUP_VERSION, ("new_document",)))
        pre = final_gate(ev, near.NearDecision(near.UNIQUE, None, None), self.config)
        decision = None
        if pre.status is DocumentStatus.ACCEPTED:
            decision = self._near(conn, ev)
            ev.add(GateResult(Gate.NEAR_DEDUP, Outcome.PASS, versions.NEAR_DEDUP_VERSION,
                              (decision.classification.lower(), f"candidates={decision.candidates_checked}"),
                              decision.similarity))
        else:
            ev.add(GateResult(Gate.NEAR_DEDUP, Outcome.PASS, versions.NEAR_DEDUP_VERSION, ("not_required",)))
        final = final_gate(ev, decision, self.config) if decision is not None else pre
        ev.add(GateResult(Gate.FINAL, Outcome.PASS if final.status is DocumentStatus.ACCEPTED else
                          Outcome.QUARANTINE if final.status is DocumentStatus.QUARANTINED else Outcome.FAIL,
                          versions.SILVER_GATE_VERSION, (final.status.value, final.primary)))
        language = ev.result(Gate.LANGUAGE)
        domain = ev.result(Gate.DOMAIN)
        row = DocumentRow(
            document_id=doc_id, content_hash=ev.content_hash,
            canonical_text=ev.canonical_text if final.keep_text else None, char_length=len(ev.canonical_text),
            language="en", language_confidence=language.score if language else None,
            quality_score=ev.result(Gate.QUALITY).score if ev.result(Gate.QUALITY) else None,
            factuality_score=ev.result(Gate.SUITABILITY).score if ev.result(Gate.SUITABILITY) else None,
            subject_domain=domain.data.get("label") if domain else None, status=final.status.value,
            status_reason=_status_reason(final.primary, ev.results), policy_version=self.policy_version,
            component_versions=self.component_versions, first_observation_id=oid)
        if not self.store.insert_document(conn, row):  # unreachable under the registry lock; stay correct anyway
            self.store.insert_extraction(conn, oid, self.extraction_version, "document", doc_id, False)
            counts["silver_exact_duplicate"] += 1
            return counts
        if decision is not None:
            self.store.insert_decision(conn, doc_id, method_id, decision.classification,
                                       decision.canonical_document_id, decision.similarity, self.run_id)
            for related, kind, score in decision.relationships:
                self.store.insert_relationship(conn, doc_id, related, kind, method_id, round(score, 4))
            if final.status is DocumentStatus.ACCEPTED:
                self.index.add(doc_id, ev.shingle_signature or ())
        if final.quarantine_code is not None:
            self.store.dead_letter(
                conn, item_type=DeadLetterItemType.DOCUMENT, item_key=str(doc_id), stage=Stage.POLICY_VALIDATED,
                disposition=Disposition.QUARANTINED, error_class=ErrorClass.POLICY_QUARANTINE,
                error_code=final.quarantine_code, error_message=self._safe_summary(ev, final),
                attempt_count=attempt_count, job_id=job_id, run_id=self.run_id, payload_ref=str(oid))
        self.store.insert_extraction(conn, oid, self.extraction_version, "document", doc_id, True)
        counts[f"silver_{final.status.value}"] += 1
        return counts

    def _near(self, conn: Any, ev: Evaluation) -> near.NearDecision:
        limits, policy = self.config.limits, self.config.policy
        ids = [d for d in self.index.candidates(ev.shingle_signature or (), max_bucket_rows=limits.max_bucket_rows,
                                                max_candidates=limits.max_near_candidates + 1)
               if d != ev.document_id][:limits.max_near_candidates]
        texts = self.store.candidate_texts(conn, ids)
        lineage = self.store.lineage(conn, list(texts), limit=50 * max(1, len(texts)))
        candidates, budget = [], limits.max_candidate_chars
        for doc in ids:
            text = texts.get(doc)
            if text is None or len(text) > budget:
                continue
            budget -= len(text)
            candidates.append(near.Candidate(doc, near.shingles(text), frozenset(lineage.get(doc, ()))))
        return near.decide(ev.shingles or frozenset(), ev.ref.lineage_key, candidates,
                           near_threshold=policy.near_duplicate_threshold, version_threshold=policy.version_threshold,
                           related_threshold=policy.related_threshold)

    @staticmethod
    def _safe_summary(ev: Evaluation, final: FinalDecision) -> str:
        """Categories, counts and fingerprints only: never source text or secret values."""
        parts = [final.primary]
        for gate in (Gate.SECRETS, Gate.PRIVACY):
            result = ev.result(gate)
            if result is not None:
                parts.append(result.code())
                fingerprints = result.data.get("fingerprints")
                if fingerprints:
                    parts.append("fp=" + ",".join(fingerprints))
        return "; ".join(parts)

    def _item_dead_letter(self, conn: Any, ev: Evaluation, job_id: int | None, attempt_count: int) -> None:
        kind = ev.failure or FailureKind.INTERNAL_ERROR
        quarantine = kind is FailureKind.QUARANTINE
        self.store.dead_letter(
            conn, item_type=DeadLetterItemType.OBSERVATION, item_key=str(ev.ref.observation_id),
            stage=GATE_STAGE.get(ev.failure_gate or Gate.INTEGRITY, Stage.ACQUIRED),
            disposition=Disposition.QUARANTINED if quarantine else Disposition.DEAD_LETTER,
            error_class=ERROR_CLASS.get(kind, ErrorClass.DATA_INVALID), error_code=f"silver_{ev.failure_code}",
            error_message=ev.failure_message or "", attempt_count=attempt_count, job_id=job_id, run_id=self.run_id,
            payload_ref=ev.ref.lineage_key)

    def _verify_rerun(self, conn: Any, ev: Evaluation, existing: tuple[str, uuid.UUID | None], counts: Counter,
                      job_id: int | None, attempt_count: int) -> None:
        outcome, doc = existing
        counts["silver_already_extracted"] += 1
        if ev.outcome is None:
            return  # a retryable failure now; the earlier outcome stands
        expected_doc = ev.document_id if ev.outcome is ExtractionOutcome.DOCUMENT else None
        if outcome != ev.outcome.value or doc != expected_doc:
            counts["silver_nondeterminism"] += 1
            self.store.dead_letter(
                conn, item_type=DeadLetterItemType.OBSERVATION, item_key=str(ev.ref.observation_id),
                stage=Stage.NORMALIZED, disposition=Disposition.DEAD_LETTER, error_class=ErrorClass.INTERNAL_BUG,
                error_code="silver_nondeterministic_extraction",
                error_message=f"rerun under {self.extraction_version} gave {ev.outcome.value}/{expected_doc}, "
                              f"stored {outcome}/{doc}", attempt_count=attempt_count, job_id=job_id,
                run_id=self.run_id, payload_ref=ev.ref.lineage_key)
