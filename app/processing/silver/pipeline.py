"""The in-memory Silver gates for one observation (gates 01-18 and 21).

Pure computation over verified Bronze evidence: no database access. Gates
19 (exact dedup), 20 (near-duplicate) and 22 (final Silver gate) need the
registry and run in ``processor`` inside the batch transaction.

Outcome mapping (schema §5.11 / §5.12, D13):

| gates                     | failure                        | durable record                                 |
|---------------------------|--------------------------------|------------------------------------------------|
| 01-02 evidence missing/corrupt/mismatched | PERMANENT_INVALID | dead letter only (no extraction row: re-acquisition can repair it) |
| 02-03 non-200, encoded, binary, MIME, generated/vendored/license | POLICY_REJECT | ``rejected_prefilter`` |
| 04-10 undecodable, unparseable, over a deterministic bound | PERMANENT_INVALID | ``extraction_failed`` (+ dead letter) |
| 11 empty canonical text   | POLICY_REJECT                  | ``empty``                                      |
| 12 U+0000                 | POLICY_REJECT                  | ``rejected_nul`` (never reaches PostgreSQL)    |
| 12 replacement characters | PERMANENT_INVALID              | ``extraction_failed``                          |
| 13 not ENGLISH            | POLICY_REJECT                  | ``rejected_language``                          |
| 14-18, 21                 | decided by the final gate      | ``document_registry.status``                   |
| wall-clock parser deadline| TRANSIENT                      | dead letter only (not deterministic, retried)  |
| unexpected exception      | INTERNAL_ERROR                 | dead letter only                               |
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable

from app.processing.classification import domain as domain_mod
from app.processing.dedup import near
from app.processing.extraction import code as code_mod
from app.processing.extraction.decoding import DecodeError, declared_charset, decode_bytes, sniff_binary
from app.processing.extraction.html import HtmlDeadline, HtmlExtractionError, extract_html, extract_plain_text
from app.processing.extraction.markdown import extract_markdown, extract_rst
from app.processing.factuality import classifier as suitability
from app.processing.language import detector
from app.processing.normalization import encoding, markup, symbols
from app.processing.normalization.text import CANONICALIZATION_VERSION, canonicalize_text
from app.processing.privacy import pii, secrets
from app.processing.quality import page_type, scorer, spam
from app.processing.silver import config as versions
from app.processing.silver.bronze_reader import BronzeIntegrityError, Payload, read_common_crawl, read_github
from app.processing.silver.config import SilverConfig
from app.processing.silver.model import (
    Evaluation,
    ExtractionOutcome,
    FailureKind,
    Gate,
    GateResult,
    Outcome,
    SourceRef,
)
from app.utils.ids import document_content_hash, document_id_from_hash

HTML_MIMES = frozenset({"text/html", "application/xhtml+xml"})


class _Stop(Exception):
    """Internal control flow: the evaluation has its outcome."""


class SilverEvaluator:
    def __init__(self, config: SilverConfig, *, cc_bronze=None, gh_bronze=None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.config, self.cc_bronze, self.gh_bronze, self.clock = config, cc_bronze, gh_bronze, clock
        self.taxonomy = domain_mod.load_taxonomy(config.taxonomy_path)
        self.domain_version = config.component_versions()["domain_classifier"]

    # -- entry point ------------------------------------------------------------ #

    def evaluate(self, ref: SourceRef) -> Evaluation:
        ev = Evaluation(ref)
        gate = [Gate.INTEGRITY]
        try:
            self._run(ev, gate)
        except _Stop:
            pass
        except HtmlDeadline as exc:
            ev.fail(Gate.PARSE, FailureKind.TRANSIENT, "parse_deadline", str(exc), outcome=None)
        except Exception as exc:  # a bug: dead-letter the item, never guess an outcome
            ev.fail(gate[0], FailureKind.INTERNAL_ERROR, type(exc).__name__, f"{type(exc).__name__} in "
                    f"{gate[0].value}", outcome=None)
        return ev

    def _stop(self, ev: Evaluation, gate: Gate, kind: FailureKind, code: str, message: str,
              outcome: ExtractionOutcome | None, version: str, *, record: bool = True) -> None:
        result = Outcome.QUARANTINE if kind is FailureKind.QUARANTINE else Outcome.FAIL
        ev.add(GateResult(gate, result, version, (code,)))
        ev.fail(gate, kind, code, message, outcome=outcome, record_extraction=record)
        raise _Stop

    # -- gates ------------------------------------------------------------------- #

    def _run(self, ev: Evaluation, gate: list[Gate]) -> None:
        cfg, limits, policy, ref = self.config, self.config.limits, self.config.policy, ev.ref
        # 01-02 integrity + payload
        try:
            payload = (read_github(ref, self.gh_bronze, limits) if ref.channel == "github"
                       else read_common_crawl(ref, self.cc_bronze, limits))
        except BronzeIntegrityError as exc:
            if exc.kind == "outcome":
                self._stop(ev, Gate.PAYLOAD, FailureKind.PERMANENT_INVALID, exc.code, str(exc),
                           ExtractionOutcome.EXTRACTION_FAILED, versions.EXTRACTOR_VERSION)
            if exc.kind == "prefilter":
                self._stop(ev, Gate.PAYLOAD, FailureKind.POLICY_REJECT, exc.code, str(exc),
                           ExtractionOutcome.REJECTED_PREFILTER, versions.EXTRACTOR_VERSION)
            self._stop(ev, Gate.INTEGRITY, FailureKind.PERMANENT_INVALID, exc.code, str(exc), None,
                       versions.EXTRACTOR_VERSION, record=False)
        ev.add(GateResult(Gate.INTEGRITY, Outcome.PASS, versions.EXTRACTOR_VERSION, ("bronze_verified",)))
        if ref.channel != "github" and payload.http_status != 200:
            self._stop(ev, Gate.PAYLOAD, FailureKind.POLICY_REJECT, f"http_status_{payload.http_status}",
                       "only HTTP 200 payloads are extracted", ExtractionOutcome.REJECTED_PREFILTER,
                       versions.EXTRACTOR_VERSION)
        ev.add(GateResult(Gate.PAYLOAD, Outcome.PASS, versions.EXTRACTOR_VERSION, (),
                          data={"bytes": len(payload.body)}))

        # 03 MIME / binary
        gate[0] = Gate.MIME
        kind = self._mime(ev, payload)
        ev.text_kind = kind

        # 04 decode
        gate[0] = Gate.DECODE
        try:
            decoded = decode_bytes(payload.body, declared=declared_charset(payload.content_type),
                                   html=kind == "html", max_replacement_ratio=policy.max_replacement_ratio,
                                   max_control_ratio=policy.max_control_ratio)
        except DecodeError as exc:
            self._stop(ev, Gate.DECODE, FailureKind.PERMANENT_INVALID, exc.code, str(exc),
                       ExtractionOutcome.EXTRACTION_FAILED, versions.DECODER_VERSION)
        if kind not in ("code", "config"):
            decoded = dataclasses.replace(decoded, text=encoding.repair(decoded.text))  # mojibake -> real text
            if encoding.corruption_count(decoded.text) > encoding.MAX_REMAINING:
                self._stop(ev, Gate.DECODE, FailureKind.PERMANENT_INVALID, "encoding_corruption",
                           "text still holds mojibake or replacement characters after repair",
                           ExtractionOutcome.EXTRACTION_FAILED, versions.DECODER_VERSION)
        ev.add(GateResult(Gate.DECODE, Outcome.PASS, versions.DECODER_VERSION,
                          (f"{decoded.source}:{decoded.encoding}",), data={"replacements": decoded.replacements}))

        # 05-10 structure
        gate[0] = Gate.PARSE
        link_ratio = kept_ratio = None
        prose_text = ""
        try:
            if kind == "html":
                result = extract_html(decoded.text, max_nodes=limits.max_html_nodes, max_depth=limits.max_html_depth,
                                      max_chars=limits.max_extracted_chars,
                                      deadline_seconds=limits.parse_deadline_seconds, clock=self.clock)
                text = result.text
                removed = result.removed
                for g, prefixes in ((Gate.ACTIVE, ("active", "hidden")), (Gate.BOILERPLATE, ("boilerplate",)),
                                    (Gate.CHROME, ("chrome", "duplicate")), (Gate.ADS, ("ads",))):
                    codes = tuple(f"{k}={v}" for k, v in sorted(removed.items()) if k.startswith(prefixes))
                    ev.add(GateResult(g, Outcome.PASS, versions.HTML_CLEANER_VERSION, codes))
                stats = result.stats
                kept_ratio = stats["kept_chars"] / stats["raw_text_chars"] if stats["raw_text_chars"] else None
                link_ratio = stats["link_chars"] / stats["kept_chars"] if stats["kept_chars"] else None
                ev.add(GateResult(Gate.PARSE, Outcome.PASS, versions.HTML_CLEANER_VERSION, ("html",)))
            else:
                text = self._text_document(ev, kind, decoded.text)
                prose_text = code_mod.prose(text, ref.path or "") if kind in ("code", "config") else ""
                for g in (Gate.ACTIVE, Gate.BOILERPLATE, Gate.CHROME, Gate.ADS):
                    ev.add(GateResult(g, Outcome.PASS, versions.HTML_CLEANER_VERSION, (f"{kind}:not_html",)))
                ev.add(GateResult(Gate.PARSE, Outcome.PASS, versions.EXTRACTOR_VERSION, (kind,)))
        except HtmlExtractionError as exc:
            self._stop(ev, Gate.PARSE, FailureKind.PERMANENT_INVALID, exc.code, str(exc),
                       ExtractionOutcome.EXTRACTION_FAILED, versions.HTML_CLEANER_VERSION)
        except code_mod.CodeRejection as exc:
            self._stop(ev, Gate.PARSE, FailureKind.POLICY_REJECT, exc.code, str(exc),
                       ExtractionOutcome.REJECTED_PREFILTER, versions.EXTRACTOR_VERSION)
        ev.add(GateResult(Gate.STRUCTURE, Outcome.PASS if text.strip() else Outcome.FAIL,
                          versions.HTML_CLEANER_VERSION, () if text.strip() else ("no_content",)))

        # 11 locked canonicalization
        gate[0] = Gate.CANONICAL
        if kind not in ("code", "config"):
            text = markup.normalize(text)  # raw LaTeX / wiki markup -> readable text (exact readings only)
            text = symbols.clean(text)  # emojis / decorative symbols out, before the identity-bearing text
        canonical = canonicalize_text(text)
        if not canonical:
            self._stop(ev, Gate.CANONICAL, FailureKind.POLICY_REJECT, "empty_canonical_text",
                       "canonical text is empty", ExtractionOutcome.EMPTY, CANONICALIZATION_VERSION)
        ev.add(GateResult(Gate.CANONICAL, Outcome.PASS, CANONICALIZATION_VERSION, (),
                          data={"chars": len(canonical)}))

        # 12 NUL / corruption
        gate[0] = Gate.CORRUPTION
        if "\x00" in canonical:
            self._stop(ev, Gate.CORRUPTION, FailureKind.POLICY_REJECT, "contains_nul",
                       "canonical text contains U+0000", ExtractionOutcome.REJECTED_NUL, versions.SILVER_GATE_VERSION)
        if any(0xD800 <= ord(ch) <= 0xDFFF for ch in canonical):
            self._stop(ev, Gate.CORRUPTION, FailureKind.PERMANENT_INVALID, "surrogates",
                       "canonical text has unpaired surrogates", ExtractionOutcome.EXTRACTION_FAILED,
                       versions.SILVER_GATE_VERSION)
        if canonical.count("�") > max(1, len(canonical)) * policy.max_replacement_ratio * 5:
            self._stop(ev, Gate.CORRUPTION, FailureKind.PERMANENT_INVALID, "replacement_characters",
                       "too many U+FFFD replacement characters", ExtractionOutcome.EXTRACTION_FAILED,
                       versions.SILVER_GATE_VERSION)
        ev.add(GateResult(Gate.CORRUPTION, Outcome.PASS, versions.SILVER_GATE_VERSION))

        # 13 English
        gate[0] = Gate.LANGUAGE
        if kind in ("code", "config"):
            lang = detector.detect(prose_text, min_tokens=policy.code_min_prose_tokens,
                                   min_confidence=policy.language_min_confidence,
                                   mixed_fraction=policy.language_mixed_fraction)
        else:
            lang = detector.detect(canonical, min_tokens=policy.language_min_tokens,
                                   min_confidence=policy.language_min_confidence,
                                   mixed_fraction=policy.language_mixed_fraction, hint=payload.language_hint)
        lang_data = {"decision": lang.decision, "confidence": lang.confidence, "tokens": lang.tokens}
        if lang.decision != detector.Decision.ENGLISH:
            ev.add(GateResult(Gate.LANGUAGE, Outcome.UNRESOLVED if lang.decision == detector.Decision.UNRESOLVED
                              else Outcome.FAIL, versions.LANGUAGE_DETECTOR_VERSION,
                              (lang.decision.lower(),) + lang.reasons, lang.confidence, lang_data))
            ev.fail(Gate.LANGUAGE, FailureKind.POLICY_REJECT, lang.decision.lower(), "not accepted English",
                    outcome=ExtractionOutcome.REJECTED_LANGUAGE)
            return
        ev.add(GateResult(Gate.LANGUAGE, Outcome.PASS, versions.LANGUAGE_DETECTOR_VERSION, ("english",) + lang.reasons,
                          lang.confidence, lang_data))

        # 14 privacy (redaction is deterministic and changes the Silver text, hence the identity)
        gate[0] = Gate.PRIVACY
        privacy = pii.analyze(canonical, quarantine_count=policy.pii_quarantine_count)
        final = canonical
        if privacy.decision == pii.REDACT:
            final = canonicalize_text(privacy.text)
        codes = tuple(f"{k}={v}" for k, v in privacy.categories.items()) + privacy.reasons
        ev.add(GateResult(Gate.PRIVACY, {pii.ALLOW: Outcome.PASS, pii.REDACT: Outcome.PASS, pii.REJECT: Outcome.FAIL,
                                         pii.QUARANTINE: Outcome.QUARANTINE}[privacy.decision],
                          versions.PRIVACY_DETECTOR_VERSION, (privacy.decision.lower(),) + codes,
                          data={"decision": privacy.decision, "redactions": privacy.redactions}))
        ev.canonical_text = final
        ev.content_hash = document_content_hash(final)
        ev.document_id = document_id_from_hash(ev.content_hash)
        ev.outcome = ExtractionOutcome.DOCUMENT

        # 15 secrets (values never leave the detector)
        gate[0] = Gate.SECRETS
        found = secrets.scan(canonical)
        fingerprints = sorted({f.fingerprint for f in found.findings if f.fingerprint})
        ev.add(GateResult(Gate.SECRETS, {secrets.ALLOW: Outcome.PASS, secrets.REJECT: Outcome.FAIL,
                                         secrets.QUARANTINE: Outcome.QUARANTINE}[found.decision],
                          versions.SECRET_DETECTOR_VERSION,
                          (found.decision.lower(),) + tuple(f"{k}={v}" for k, v in found.categories.items()),
                          data={"fingerprints": fingerprints[:10]}))

        # 16-18 noise, quality, suitability
        features = scorer.features(final)
        prose_words = len(prose_text.split()) if prose_text else 0
        gate[0] = Gate.NOISE
        noise = spam.check(final, features, kind=kind, min_words=policy.noise_min_words, link_ratio=link_ratio)
        noise_reasons = noise.reasons
        if ref.channel == "common_crawl":
            declared = page_type.schema_types(decoded.text) if kind == "html" else frozenset()
            page = page_type.check(ref.source_url, payload.title, final, features, declared)
            noise_reasons = tuple(dict.fromkeys(noise_reasons + page.reasons))
        ev.add(GateResult(Gate.NOISE, Outcome.FAIL if noise_reasons else Outcome.PASS, versions.NOISE_FILTER_VERSION,
                          noise_reasons))
        gate[0] = Gate.QUALITY
        quality = scorer.score(features, kind=kind, threshold=policy.quality_threshold, prose_words=prose_words,
                               kept_ratio=kept_ratio)
        ev.add(GateResult(Gate.QUALITY, Outcome.PASS if quality.passed else Outcome.FAIL,
                          versions.QUALITY_FILTER_VERSION, quality.reasons, quality.score,
                          data={"components": quality.components}))
        gate[0] = Gate.SUITABILITY
        suit = suitability.classify(final, features, kind=kind, pass_threshold=policy.suitability_pass,
                                    fail_threshold=policy.suitability_fail, prose_words=prose_words)
        ev.add(GateResult(Gate.SUITABILITY, {suitability.PASS: Outcome.PASS, suitability.FAIL: Outcome.FAIL,
                                             suitability.UNRESOLVED: Outcome.UNRESOLVED}[suit.decision],
                          versions.SUITABILITY_FILTER_VERSION, suit.reasons, suit.score))

        # 21 subject domain (after cleaning: only the Silver text is classified)
        gate[0] = Gate.DOMAIN
        dom = domain_mod.classify(final, min_evidence=policy.domain_min_evidence,
                                  min_confidence=policy.domain_min_confidence, multi_ratio=policy.domain_multi_ratio,
                                  taxonomy=self.taxonomy, title=payload.title,
                                  url=ref.source_url if ref.channel == "common_crawl" else None)
        ev.add(GateResult(Gate.DOMAIN, Outcome.PASS, self.domain_version,
                          (dom.decision.lower(),) + dom.reasons, dom.confidence,
                          data={"label": dom.label, "domain": dom.domain, "subdomain": dom.subdomain}))

        # near-duplicate signature (compared in the processor)
        ev.shingles = near.shingles(final)
        ev.shingle_signature = near.signature(ev.shingles)

    # -- helpers ------------------------------------------------------------------ #

    def _mime(self, ev: Evaluation, payload: Payload) -> str:
        policy, ref = self.config.policy, ev.ref
        binary = sniff_binary(payload.body)
        if ref.channel == "github":
            if binary is not None:
                self._stop(ev, Gate.MIME, FailureKind.POLICY_REJECT, "binary_masquerading_as_text", binary,
                           ExtractionOutcome.REJECTED_PREFILTER, versions.DECODER_VERSION)
            reason = code_mod.path_rejection(ref.path or "")
            kind = code_mod.file_kind(ref.path or "")
            if reason is None and kind in ("license", "unknown"):
                reason = "license_boilerplate" if kind == "license" else "unsupported_file_type"
            if reason is not None:
                self._stop(ev, Gate.MIME, FailureKind.POLICY_REJECT, reason, f"file rejected: {reason}",
                           ExtractionOutcome.REJECTED_PREFILTER, versions.EXTRACTOR_VERSION)
            ev.add(GateResult(Gate.MIME, Outcome.PASS, versions.DECODER_VERSION, (kind,)))
            return kind
        if binary is not None:
            self._stop(ev, Gate.MIME, FailureKind.POLICY_REJECT, binary, "binary payload",
                       ExtractionOutcome.REJECTED_PREFILTER, versions.DECODER_VERSION)
        declared = (payload.content_type or "").split(";", 1)[0].strip().lower() or None
        detected = (payload.index_mime or "").split(";", 1)[0].strip().lower() or None
        head = payload.body[:4096].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
        looks_html = head.startswith((b"<!doctype html", b"<html", b"<?xml")) or any(
            tag in head for tag in (b"<body", b"<head", b"<p>", b"<div", b"<title"))
        looks_data = head[:1] in (b"{", b"[") and not looks_html
        mime = declared if declared in policy.accepted_mimes else None
        if mime is None and detected in policy.accepted_mimes and (looks_html or detected == "text/plain"):
            mime = detected  # an unhelpful declared type, confirmed by the index sniffer and the bytes
        if mime is None or looks_data or (mime in HTML_MIMES and not looks_html and detected not in HTML_MIMES):
            self._stop(ev, Gate.MIME, FailureKind.POLICY_REJECT, f"mime_{declared or 'missing'}",
                       "payload is not an accepted text type", ExtractionOutcome.REJECTED_PREFILTER,
                       versions.DECODER_VERSION)
        kind = "html" if mime in HTML_MIMES else "plain"
        ev.add(GateResult(Gate.MIME, Outcome.PASS, versions.DECODER_VERSION, (mime,)))
        return kind

    def _text_document(self, ev: Evaluation, kind: str, text: str) -> str:
        limits, path = self.config.limits, ev.ref.path or ""
        if len(text) > limits.max_extracted_chars:
            raise HtmlExtractionError("extracted_too_large", "document exceeds max_extracted_chars")
        if kind == "plain":
            return extract_plain_text(text, max_chars=limits.max_extracted_chars)
        if kind == "markdown":
            return extract_markdown(text, mdx=path.lower().endswith(".mdx"))
        if kind == "rst":
            return extract_rst(text)
        if kind == "code":
            code_mod.check_code(text, path)
            return text
        if kind == "config":
            code_mod.check_config(text)
            return text
        return text
