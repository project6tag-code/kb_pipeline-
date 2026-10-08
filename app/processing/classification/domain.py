"""Gate 21: subject domain / subdomain classification (DOMAIN_CLASSIFIER_VERSION ``dom1``).

Runs on the CLEANED canonical text only (navigation, headers, footers and
other chrome were removed by gates 06-09), never on raw HTML. Headings count
twice.

The taxonomy is data (``taxonomy_v1.json`` or ``KB_SILVER_TAXONOMY``): domains
-> subdomains -> keyword/phrase lists. It can grow without code changes; the
classifier version includes the taxonomy digest.

Tokens are lower-cased and lightly singularized (``molecules`` -> ``molecule``;
``-ss``/``-us``/``-is`` endings kept) on both sides. Scoring: each keyword occurrence contributes ``w * (1 + ln(count))`` with
``w = 1 + 0.5 * (words - 1)`` (phrases are stronger evidence); domain-level
keywords are generic and count half. Domain evidence = its own keywords +
all its subdomains.

Decision:

* top evidence < ``min_evidence`` or confidence < ``min_confidence``
  -> UNKNOWN (subject_domain NULL; nothing is invented);
* ``confidence = top/(top+second) * min(1, top/(1.5*min_evidence))``;
* second domain >= ``multi_ratio`` x top -> MULTI: the primary domain is kept
  (without subdomain) and the secondary is recorded as a reason code;
* the subdomain is assigned only when it clearly leads inside the domain.

Label: ``"Domain/Subdomain"`` or ``"Domain"`` (<= 100 characters, schema CHECK).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlsplit

VERSION = "dom3"  # dom2 + ambiguous software words down-weighted, URL slug evidence, health-publisher prior
TITLE_BOOST = 3
# Single words that appear in almost every domain's vocabulary ("system", "state", "life", "element", ...):
# alone they say little about the subject, so their evidence counts a quarter. Multi-word phrases and every
# other keyword keep full weight.
GENERIC_WEIGHT = 0.25
GENERIC_WORDS = frozenset({
    "state", "system", "design", "life", "element", "capital", "space", "team", "code", "library", "application",
    "market", "region", "country", "force", "mass", "energy", "cell", "language", "art", "era", "company",
    "organization", "policy", "treatment",
    # dom3: words that are Software keywords in the taxonomy but everyday vocabulary in statistics, survey and
    # data documentation ("class variable", "table", "index", "test", "function", "object")
    "variable", "function", "class", "object", "table", "index", "query", "test", "tests", "testing", "join",
    "schema", "process", "thread", "branch", "merge", "commit", "coverage", "container", "server", "cloud",
    "pipeline", "mock", "assert", "transaction", "repository", "kernel", "syntax",
})
URL_BOOST = 2          # words of the URL path (slug) count like a heading word, twice
HOST_PRIOR = 3.0       # below min_evidence (4.0): a publisher alone never assigns a domain, it only tips the balance
# Publishers whose whole site is one subject. Longest-suffix host match; the domain must exist in the taxonomy.
HOST_PRIORS = {
    "cdc.gov": "Medicine and Health", "nih.gov": "Medicine and Health", "medlineplus.gov": "Medicine and Health",
    "who.int": "Medicine and Health", "nhs.uk": "Medicine and Health", "mayoclinic.org": "Medicine and Health",
    "fda.gov": "Medicine and Health", "cancer.gov": "Medicine and Health",
}
DEFAULT_TAXONOMY = Path(__file__).with_name("taxonomy_v1.json")
ASSIGNED, MULTI, UNKNOWN = "ASSIGNED", "MULTI", "UNKNOWN"

_TOKEN = re.compile(r"[a-z0-9]+(?:[-'][a-z0-9]+)*")


def _norm(token: str) -> str:
    if len(token) > 3 and token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-3] + "y" if token.endswith("ies") and len(token) > 4 else token[:-1]
    return token


def _tokens(text: str) -> list[str]:
    return [_norm(t) for t in _TOKEN.findall(text.lower())]


@dataclass(frozen=True)
class Subdomain:
    name: str
    keywords: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Domain:
    name: str
    keywords: tuple[tuple[str, ...], ...]
    subdomains: tuple[Subdomain, ...]


@dataclass(frozen=True)
class Taxonomy:
    version: str
    digest: str
    domains: tuple[Domain, ...]


@dataclass(frozen=True)
class DomainResult:
    decision: str
    label: str | None
    domain: str | None
    subdomain: str | None
    confidence: float
    reasons: tuple[str, ...] = ()
    evidence: dict[str, float] = field(default_factory=dict)


def _phrases(words: list[str]) -> tuple[tuple[str, ...], ...]:
    out = []
    for word in words:
        tokens = tuple(_tokens(word))
        if tokens and tokens not in out:
            out.append(tokens)
    return tuple(out)


@lru_cache(maxsize=8)
def load_taxonomy(path: Path | None = None) -> Taxonomy:
    raw = (path or DEFAULT_TAXONOMY).read_bytes()
    data = json.loads(raw.decode("utf-8"))
    domains = []
    names: set[str] = set()
    for d in data["domains"]:
        subs = tuple(Subdomain(s["name"], _phrases(s["keywords"])) for s in d.get("subdomains", []))
        for label in [d["name"]] + [f"{d['name']}/{s.name}" for s in subs]:
            if "/" in d["name"] or len(label) > 100 or label in names:
                raise ValueError(f"invalid or duplicate taxonomy label {label!r}")
            names.add(label)
        domains.append(Domain(d["name"], _phrases(d.get("keywords", [])), subs))
    return Taxonomy(str(data.get("version", "unversioned")), hashlib.sha256(raw).hexdigest()[:10], tuple(domains))


def _ngrams(text: str) -> Counter:
    body = "\n".join(line.lstrip("#").strip() for line in text.split("\n"))
    headings = "\n".join(line.lstrip("#").strip() for line in text.split("\n") if line.startswith("#"))
    counts: Counter = Counter()
    for part in (body, headings):
        tokens = _tokens(part)
        for n in (1, 2, 3):
            counts.update(tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1))
    return counts


def _evidence(keywords: tuple[tuple[str, ...], ...], counts: Counter, scale: float = 1.0) -> float:
    total = 0.0
    for phrase in keywords:
        c = counts.get(phrase, 0)
        if c:
            weight = GENERIC_WEIGHT if len(phrase) == 1 and phrase[0] in GENERIC_WORDS else 1.0
            total += scale * weight * (1 + 0.5 * (len(phrase) - 1)) * (1 + math.log(c))
    return total


def _title_ngrams(title: str | None) -> Counter:
    counts: Counter = Counter()
    tokens = _tokens(title or "")
    for n in (1, 2, 3):
        counts.update(tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1))
    return counts


def _url_parts(url: str | None) -> tuple[str, str]:
    """(lower-cased host without ``www.``, path text) of ``url``; empty strings when unusable."""
    if not url:
        return "", ""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "", ""
    return (parts.hostname or "").lower().removeprefix("www."), unquote(parts.path).replace("/", " ")


def _host_prior(host: str) -> str | None:
    best = max((suffix for suffix in HOST_PRIORS if host == suffix or host.endswith("." + suffix)),
               key=len, default=None)
    return HOST_PRIORS[best] if best else None


def classify(text: str, *, min_evidence: float, min_confidence: float, multi_ratio: float,
             taxonomy: Taxonomy | None = None, title: str | None = None, url: str | None = None) -> DomainResult:
    taxonomy = taxonomy or load_taxonomy()
    counts = _ngrams(text)
    for gram, n in _title_ngrams(title).items():  # the page title states the subject: heading weight x TITLE_BOOST
        counts[gram] += n * TITLE_BOOST
    host, path = _url_parts(url)
    for gram, n in _title_ngrams(path).items():  # URL slug words (/flu/, /diabetes/) name the subject too
        counts[gram] += n * URL_BOOST
    prior_domain = _host_prior(host)
    scores: dict[str, float] = {}
    sub_scores: dict[str, dict[str, float]] = {}
    for d in taxonomy.domains:
        subs = {s.name: _evidence(s.keywords, counts) for s in d.subdomains}
        sub_scores[d.name] = subs
        scores[d.name] = _evidence(d.keywords, counts, 0.5) + sum(subs.values())
        if d.name == prior_domain and scores[d.name] > 0:  # a prior only tips a page that has some own evidence
            scores[d.name] += HOST_PRIOR
    ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
    (top_name, top), (second_name, second) = ranked[0], ranked[1] if len(ranked) > 1 else ("", 0.0)
    evidence = {k: round(v, 2) for k, v in ranked[:3] if v > 0}
    if top < min_evidence:
        return DomainResult(UNKNOWN, None, None, None, 0.0, ("insufficient_evidence",), evidence)
    confidence = top / (top + second) * min(1.0, top / (1.5 * min_evidence))
    if confidence < min_confidence:
        return DomainResult(UNKNOWN, None, None, None, round(confidence, 4), ("low_confidence",), evidence)
    if second >= multi_ratio * top:
        return DomainResult(MULTI, top_name, top_name, None, round(confidence, 4),
                            (f"multi_domain:{second_name}",), evidence)
    subs = sorted(sub_scores[top_name].items(), key=lambda kv: (-kv[1], kv[0]))
    subdomain = None
    if subs and subs[0][1] >= min_evidence / 2:
        runner = subs[1][1] if len(subs) > 1 else 0.0
        if subs[0][1] >= 1.5 * runner:
            subdomain = subs[0][0]
    label = f"{top_name}/{subdomain}" if subdomain else top_name
    return DomainResult(ASSIGNED, label, top_name, subdomain, round(confidence, 4),
                        () if subdomain else ("subdomain_unknown",), evidence)
