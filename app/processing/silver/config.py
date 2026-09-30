"""Configuration and versioning for Bronze -> Silver Preprocessing V1.

Every output-affecting component has an explicit code version (below). The
thresholds and limits are part of the configuration, and both are folded into
two deterministic labels that are persisted:

* ``extraction_version`` (``kbp.observation_extractions.extraction_version``):
  everything that decides an observation's extraction outcome and the text,
  hence the ``document_id`` it links to: extractor, HTML cleaner, decoder,
  locked canonicalizer, NUL rule, language gate and privacy redaction.
* ``policy_version`` (``kbp.document_registry.policy_version``): the whole gate
  bundle that set the document status.

Changing a component version or a threshold changes the label, so earlier
provenance rows are never overwritten (new extraction rows are added under
the new label; schema §19).
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.processing.factuality.classifier import VERSION as SUITABILITY_FILTER_VERSION
from app.processing.language.detector import VERSION as LANGUAGE_DETECTOR_VERSION
from app.processing.normalization.text import CANONICALIZATION_VERSION

# Code versions: bump when the component's output can change. The language and
# suitability versions are defined once, in their component modules (imported above).
EXTRACTOR_VERSION = "ext1"
HTML_CLEANER_VERSION = "html1"
DECODER_VERSION = "dec1"
PRIVACY_DETECTOR_VERSION = "pii1"
SECRET_DETECTOR_VERSION = "sec1"
NOISE_FILTER_VERSION = "noise1"
QUALITY_FILTER_VERSION = "qual1"
EXACT_DEDUP_VERSION = "exact1"          # Identity V1 content hash (document_was_new)
NEAR_DEDUP_ALGORITHM = "minhash_lsh"     # kbp.dedup_methods.algorithm
NEAR_DEDUP_VERSION = "oph-mh128-b32r4-w5-v1"
DOMAIN_CLASSIFIER_VERSION = "dom1"
SILVER_GATE_VERSION = "gate1"
IDENTITY_VERSION = "v1"


def _digest(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False, default=str)
    return hashlib.sha256(raw.encode("ascii")).hexdigest()[:10]


@dataclass(frozen=True)
class LimitsConfig:
    """Resource bounds (one PC). Exceeding a deterministic bound is an outcome, never a truncation."""

    max_bronze_bytes: int = 8 * 1024 * 1024            # compressed CC record / GitHub blob on disk
    max_decompressed_bytes: int = 32 * 1024 * 1024     # WARC record after gunzip
    max_payload_bytes: int = 8 * 1024 * 1024           # HTTP body / file content considered for extraction
    max_html_nodes: int = 200_000
    max_html_depth: int = 256
    max_extracted_chars: int = 1_000_000               # structured text produced by the extractor
    parse_deadline_seconds: float = 30.0               # safety net only (not an outcome: see pipeline)
    max_near_candidates: int = 20
    max_bucket_rows: int = 64
    max_candidate_chars: int = 4_000_000               # total candidate text fetched for one document
    min_free_bytes: int = 1024 * 1024 * 1024           # near-duplicate index disk guard


@dataclass(frozen=True)
class PolicyConfig:
    """Thresholds (O-16/O-17: benchmark and tune; every value is part of the version labels)."""

    accepted_mimes: tuple[str, ...] = ("text/html", "application/xhtml+xml", "text/plain")
    max_replacement_ratio: float = 0.001
    max_control_ratio: float = 0.001
    language_min_tokens: int = 25
    language_min_confidence: float = 0.80
    language_mixed_fraction: float = 0.20
    code_min_prose_tokens: int = 25
    pii_quarantine_count: int = 25                     # personal identifiers per document -> contact dump
    noise_min_words: int = 40
    quality_threshold: float = 0.55
    suitability_pass: float = 0.55
    suitability_fail: float = 0.35
    near_duplicate_threshold: float = 0.80
    version_threshold: float = 0.50
    related_threshold: float = 0.50
    domain_min_evidence: float = 4.0
    domain_min_confidence: float = 0.45
    domain_multi_ratio: float = 0.80
    retain_unresolved_text: bool = True                # O-14: quarantined-but-not-sensitive text is kept
    retain_rejected_text: bool = False                 # rejected documents keep hash + decision only


@dataclass(frozen=True)
class SilverConfig:
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    taxonomy_path: Path | None = None                  # None: the packaged taxonomy_v1.json
    near_index_path: Path = Path("data/silver/near_index.sqlite3")

    def taxonomy_digest(self) -> str:
        from app.processing.classification.domain import load_taxonomy

        return load_taxonomy(self.taxonomy_path).digest

    def component_versions(self) -> dict[str, str]:
        return {
            "extractor": EXTRACTOR_VERSION,
            "html_cleaner": HTML_CLEANER_VERSION,
            "decoder": DECODER_VERSION,
            "canonicalizer": CANONICALIZATION_VERSION,
            "language_detector": LANGUAGE_DETECTOR_VERSION,
            "privacy_detector": PRIVACY_DETECTOR_VERSION,
            "secret_detector": SECRET_DETECTOR_VERSION,
            "noise_filter": NOISE_FILTER_VERSION,
            "quality_filter": QUALITY_FILTER_VERSION,
            "suitability_filter": SUITABILITY_FILTER_VERSION,
            "exact_dedup": EXACT_DEDUP_VERSION,
            "near_dedup": f"{NEAR_DEDUP_ALGORITHM}:{NEAR_DEDUP_VERSION}",
            "domain_classifier": f"{DOMAIN_CLASSIFIER_VERSION}-{self.taxonomy_digest()}",
            "silver_gate": SILVER_GATE_VERSION,
            "identity": IDENTITY_VERSION,
        }

    @property
    def extraction_version(self) -> str:
        versions = self.component_versions()
        keys = ("extractor", "html_cleaner", "decoder", "canonicalizer", "language_detector", "privacy_detector")
        policy = self.policy
        params = {k: versions[k] for k in keys}
        params.update(limits=dataclasses.asdict(self.limits), mimes=policy.accepted_mimes,
                      decode=(policy.max_replacement_ratio, policy.max_control_ratio),
                      language=(policy.language_min_tokens, policy.language_min_confidence,
                                policy.language_mixed_fraction, policy.code_min_prose_tokens),
                      pii=policy.pii_quarantine_count)
        params["limits"].pop("parse_deadline_seconds")
        return f"x1-{_digest(params)}"

    @property
    def policy_version(self) -> str:
        return f"s1-{_digest({'versions': self.component_versions(), 'policy': dataclasses.asdict(self.policy)})}"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "SilverConfig":
        env = dict(os.environ if env is None else env)
        policy = PolicyConfig()
        overrides: dict[str, Any] = {}
        for name, key, kind in (("quality_threshold", "KB_SILVER_QUALITY_THRESHOLD", float),
                                ("suitability_pass", "KB_SILVER_SUITABILITY_PASS", float),
                                ("suitability_fail", "KB_SILVER_SUITABILITY_FAIL", float),
                                ("language_min_confidence", "KB_SILVER_LANGUAGE_MIN_CONFIDENCE", float),
                                ("near_duplicate_threshold", "KB_SILVER_NEAR_DUPLICATE_THRESHOLD", float)):
            if env.get(key):
                overrides[name] = kind(env[key])
        policy = dataclasses.replace(policy, **overrides)
        taxonomy = env.get("KB_SILVER_TAXONOMY") or None
        index = env.get("KB_SILVER_NEAR_INDEX") or "data/silver/near_index.sqlite3"
        config = cls(policy=policy, taxonomy_path=Path(taxonomy) if taxonomy else None, near_index_path=Path(index))
        config.validate()
        return config

    def validate(self) -> None:
        p = self.policy
        for name in ("quality_threshold", "suitability_pass", "suitability_fail", "language_min_confidence",
                     "near_duplicate_threshold", "version_threshold", "related_threshold", "domain_min_confidence"):
            if not 0.0 <= getattr(p, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if p.suitability_fail > p.suitability_pass:
            raise ValueError("suitability_fail must not exceed suitability_pass")
        if not p.related_threshold <= p.near_duplicate_threshold:
            raise ValueError("related_threshold must not exceed near_duplicate_threshold")
        if self.limits.max_near_candidates < 1 or self.limits.max_bucket_rows < 1:
            raise ValueError("near-duplicate candidate bounds must be positive")
