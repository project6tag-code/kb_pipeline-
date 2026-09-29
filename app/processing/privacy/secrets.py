"""Gate 15: secret / credential detection (SECRET_DETECTOR_VERSION ``sec1``).

Stricter than PII. Findings carry a category, a confidence and - for
high-entropy provider tokens only - a safe fingerprint
(``sha256:<first 16 hex>`` of the value). The matched value itself is never
returned, logged or persisted (a short password could be brute-forced from a
hash, so generic assignments get no fingerprint at all).

High confidence (provider formats, private-key blocks, credentialed
connection strings) -> QUARANTINE. Medium confidence (a generic
``password = "..."`` / ``api_key: ...`` assignment with a high-entropy,
non-placeholder value) -> REJECT. Documented examples and placeholders
(``AKIAIOSFODNN7EXAMPLE``, ``changeme``, ``<token>``, ``${VAR}``, ``xxxx``) are
not secrets -> ALLOW.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from dataclasses import dataclass, field

VERSION = "sec1"

ALLOW, REJECT, QUARANTINE = "ALLOW", "REJECT", "QUARANTINE"

_PROVIDERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private_key", re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP |ENCRYPTED )?PRIVATE KEY(?: BLOCK)?-----"
                               r"\s*([A-Za-z0-9+/=\s:,-]{64,}?)-----END", re.S)),
    ("aws_access_key", re.compile(r"(?<![A-Z0-9])((?:AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA|AIPA)[A-Z0-9]{16})(?![A-Z0-9])")),
    ("aws_secret_key", re.compile(r"(?i)aws.{0,20}?(?:secret|private).{0,20}?[\"'\s:=]+([A-Za-z0-9/+=]{40})(?![A-Za-z0-9/+=])")),
    ("github_token", re.compile(r"(?<![A-Za-z0-9_])((?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{82})"
                                r"(?![A-Za-z0-9_])")),
    ("gitlab_token", re.compile(r"(?<![A-Za-z0-9_-])(glpat-[A-Za-z0-9_-]{20})(?![A-Za-z0-9_-])")),
    ("slack_token", re.compile(r"(?<![A-Za-z0-9-])(xox[abposr]-[A-Za-z0-9-]{10,72})(?![A-Za-z0-9-])")),
    ("slack_webhook", re.compile(r"(https://hooks\.slack\.com/services/T[A-Z0-9]{6,12}/B[A-Z0-9]{6,12}/[A-Za-z0-9]{20,32})")),
    ("stripe_key", re.compile(r"(?<![A-Za-z0-9_])((?:sk|rk)_live_[A-Za-z0-9]{20,99})(?![A-Za-z0-9_])")),
    ("google_api_key", re.compile(r"(?<![A-Za-z0-9_-])(AIza[0-9A-Za-z_-]{35})(?![A-Za-z0-9_-])")),
    ("openai_key", re.compile(r"(?<![A-Za-z0-9_-])(sk-(?:proj-)?[A-Za-z0-9_-]{20,}T3BlbkFJ[A-Za-z0-9_-]{20,}"
                              r"|sk-proj-[A-Za-z0-9_-]{40,})(?![A-Za-z0-9_-])")),
    ("anthropic_key", re.compile(r"(?<![A-Za-z0-9_-])(sk-ant-(?:api|admin)\d{2}-[A-Za-z0-9_-]{80,})(?![A-Za-z0-9_-])")),
    ("twilio_key", re.compile(r"(?<![A-Za-z0-9])(SK[0-9a-f]{32})(?![A-Za-z0-9])")),
    ("sendgrid_key", re.compile(r"(?<![A-Za-z0-9_-])(SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43})(?![A-Za-z0-9_-])")),
    ("npm_token", re.compile(r"(?<![A-Za-z0-9_])(npm_[A-Za-z0-9]{36})(?![A-Za-z0-9_])")),
    ("pypi_token", re.compile(r"(?<![A-Za-z0-9_-])(pypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{50,})(?![A-Za-z0-9_-])")),
    ("jwt", re.compile(r"(?<![A-Za-z0-9_-])(eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{16,})"
                       r"(?![A-Za-z0-9_.-])")),
    ("connection_string", re.compile(r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|amqp|mssql|"
                                     r"sqlserver|oracle|ftp|smtp)://[^\s:/@\"'<>]{1,64}:([^\s@/\"'<>]{4,128})@"
                                     r"[^\s/\"'<>]+")),
)
_ASSIGNMENT = re.compile(
    r"(?i)(?<![A-Za-z0-9])(password|passwd|pwd|pass|secret|secret_key|client_secret|api_key|apikey|api-key|"
    r"access_token|auth_token|access_key|private_key|token|bearer)\s*[\"']?\s*[:=]\s*[\"']([^\"'\s]{8,128})[\"']")
_BEARER = re.compile(r"(?i)authorization\s*[:=]\s*[\"']?bearer\s+([A-Za-z0-9._~+/-]{20,}=*)")
_PLACEHOLDER = re.compile(
    r"(?i)(example|sample|dummy|fake|placeholder|changeme|change_me|change-me|your|xxxx|\*\*\*|<[^>]*>|\$\{|\{\{|"
    r"%s|%\(|test|redacted|secret123|password123|123456|abcdef|qwerty|todo|insert|replace|here|foobar|notreal|"
    r"invalid|none|null|undefined|env\[|os\.environ|getenv|process\.env)")
_EXAMPLE_VALUES = frozenset({"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})


@dataclass(frozen=True)
class SecretFinding:
    category: str
    confidence: str      # 'high' | 'medium' | 'example'
    fingerprint: str | None


@dataclass(frozen=True)
class SecretResult:
    decision: str
    categories: dict[str, int] = field(default_factory=dict)
    findings: tuple[SecretFinding, ...] = ()
    reasons: tuple[str, ...] = ()


def entropy(value: str) -> float:
    counts = Counter(value)
    return -sum(c / len(value) * math.log2(c / len(value)) for c in counts.values()) if value else 0.0


def fingerprint(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8", "surrogatepass")).hexdigest()[:16]


def _is_placeholder(value: str) -> bool:
    return value in _EXAMPLE_VALUES or bool(_PLACEHOLDER.search(value)) or len(set(value)) <= 4


def scan(text: str) -> SecretResult:
    findings: list[SecretFinding] = []
    for category, pattern in _PROVIDERS:
        for m in pattern.finditer(text):
            value = m.group(1)
            if category == "private_key":
                body = re.sub(r"\s", "", value)
                if len(body) < 64 or _is_placeholder(body[:64]) or entropy(body) < 4.0:
                    findings.append(SecretFinding(category, "example", None))
                    continue
                findings.append(SecretFinding(category, "high", fingerprint(body)))
                continue
            if _is_placeholder(value):
                findings.append(SecretFinding(category, "example", None))
            elif category == "connection_string":
                findings.append(SecretFinding(category, "high", None))
            else:
                findings.append(SecretFinding(category, "high", fingerprint(value)))
    for pattern in (_ASSIGNMENT, _BEARER):
        for m in pattern.finditer(text):
            value = m.group(m.lastindex)
            if _is_placeholder(value) or entropy(value) < 3.0:
                continue
            findings.append(SecretFinding("credential_assignment", "medium", None))
    categories = Counter(f.category for f in findings if f.confidence != "example")
    if any(f.confidence == "high" for f in findings):
        decision = QUARANTINE
    elif any(f.confidence == "medium" for f in findings):
        decision = REJECT
    else:
        decision = ALLOW
    reasons = tuple(sorted({f"{f.category}:{f.confidence}" for f in findings}))
    return SecretResult(decision, dict(sorted(categories.items())), tuple(findings), reasons)
