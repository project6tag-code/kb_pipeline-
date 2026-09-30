"""Offline benchmark prototypes for chunk near-duplicate candidate indexes.

This script only reads chunk-pilot rows from kb_pipeline_test. All indexes are
temporary SQLite files and are deleted after the run. It never writes pipeline
tables or changes chunk identity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import statistics
import struct
import tempfile
import time
import tracemalloc
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

REQUIRED_DATABASE = "kb_pipeline_test"
REQUIRED_ROLE = "kb_pipeline_app"
PILOT_REPOSITORY_ID = 900_000_000_002
PILOT_URL_MARKER = "%chunk-pilot.example%"
NEAR_THRESHOLD = 0.78
RELATED_THRESHOLD = 0.32
VERIFY_THRESHOLDS = (0.65, 0.70, 0.75, 0.78, 0.82)
MAX_BUCKET_ROWS = 256
MAX_CANDIDATES = 64
MAX_QUERY_FEATURES = 512
EXACT_VERIFY_SAMPLE_LIMIT = 1024
_WORD = re.compile(r"[\w]+", re.UNICODE)
_NUMBER = re.compile(r"(?<!\w)(?:[$£€]\s*)?\d+(?:,\d{3})*(?:\.\d+)?(?:\s*(?:million|billion|thousand|percent|%))?", re.I)
_CAPITALIZED = re.compile(r"\b[A-Z](?:[a-z]+|\b)(?:\s+[A-Z](?:[a-z]+|\b))*")
_NEGATION = re.compile(r"\b(?:not|never|no|neither|without|cannot|can't|doesn't|didn't|isn't|wasn't)\b", re.I)
_PRIME = (1 << 61) - 1

PILOT_SQL = """
SELECT DISTINCT c.chunk_id, c.chunk_text
FROM kbp.chunk_registry AS c
WHERE EXISTS (
    SELECT 1
    FROM kbp.chunk_occurrences AS co
    JOIN kbp.observation_extractions AS e
      ON e.document_id = co.document_id AND e.outcome = 'document'
    JOIN kbp.source_observations AS s ON s.observation_id = e.observation_id
    WHERE co.chunk_id = c.chunk_id AND s.source_url LIKE :url_marker
)
OR EXISTS (
    SELECT 1
    FROM kbp.chunk_occurrences AS co
    JOIN kbp.observation_extractions AS e
      ON e.document_id = co.document_id AND e.outcome = 'document'
    JOIN kbp.github_file_observations AS gh ON gh.observation_id = e.observation_id
    WHERE co.chunk_id = c.chunk_id AND gh.repository_id = :repository_id
)
ORDER BY c.chunk_id
LIMIT 128
"""


@dataclass(frozen=True)
class Item:
    item_id: uuid.UUID
    text: str
    features: frozenset[int]


@dataclass(frozen=True)
class Query:
    text: str
    target_id: uuid.UUID
    kind: str


def _h64(data: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def _signed(value: int) -> int:
    value &= (1 << 64) - 1
    return value if value < (1 << 63) else value - (1 << 64)


def features(text_value: str) -> frozenset[int]:
    words = [token.casefold() for token in _WORD.findall(text_value)]
    if not words:
        return frozenset()
    if len(words) < 3:
        return frozenset({_h64(" ".join(words).encode("utf-8"))})
    return frozenset(_h64(" ".join(words[i:i + 3]).encode("utf-8"))
                     for i in range(len(words) - 2))


def jaccard(left: frozenset[int], right: frozenset[int]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def fact_signature(text_value: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    numbers = tuple(sorted(match.group(0).casefold().replace(",", "") for match in _NUMBER.finditer(text_value)))
    entities: list[str] = []
    for match in _CAPITALIZED.finditer(text_value):
        prefix = text_value[:match.start()].rstrip()
        first = match.group(0).split()[0].casefold()
        words = match.group(0).split()
        line_start = text_value.rfind("\n", 0, match.start()) + 1
        markdown_heading = text_value[line_start:match.start()].lstrip().startswith("#")
        sentence_start = not prefix or prefix[-1] in ".!?\n"
        if markdown_heading:
            continue
        if sentence_start and (first in {"the", "a", "an", "revenue", "treaty", "officials"}
                               or len(words) == 1):
            continue
        entities.append(" ".join(match.group(0).casefold().split()))
    negations = tuple(sorted(match.group(0).casefold() for match in _NEGATION.finditer(text_value)))
    return numbers, tuple(sorted(entities)), negations


def verified_class(left: str, right: str) -> tuple[str, float]:
    score = jaccard(features(left), features(right))
    if score >= NEAR_THRESHOLD and fact_signature(left) == fact_signature(right):
        return "NEAR_DUPLICATE", score
    if score >= RELATED_THRESHOLD:
        return "RELATED", score
    return "UNIQUE", score


def load_pilot(url: str) -> list[tuple[uuid.UUID, str]]:
    engine = create_engine(url, poolclass=NullPool, pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql("BEGIN READ ONLY")
            try:
                database, role = connection.execute(text("SELECT current_database(), current_user")).one()
                if database != REQUIRED_DATABASE:
                    raise RuntimeError(f"pilot read refused database {database!r}; expected {REQUIRED_DATABASE!r}")
                if role != REQUIRED_ROLE:
                    raise RuntimeError(f"pilot read refused role {role!r}; expected {REQUIRED_ROLE!r}")
                rows = connection.execute(text(PILOT_SQL), {
                    "url_marker": PILOT_URL_MARKER,
                    "repository_id": PILOT_REPOSITORY_ID,
                }).all()
                return [(row[0], row[1]) for row in rows]
            finally:
                connection.rollback()
    finally:
        engine.dispose()


def _replace_once(text_value: str, old: str, new: str) -> str:
    if old not in text_value:
        return text_value
    return text_value.replace(old, new, 1)


def synthetic_bases(count: int) -> list[tuple[uuid.UUID, str, dict[str, str]]]:
    given_names = ("Ardin", "Borin", "Cevan", "Demer", "Elora", "Faron", "Galen", "Hedra")
    regions = ("North Vale", "Red Coast", "East March", "Silver Basin", "West Reach", "Green Delta")
    templates = (
        ("{entity} operates a hydroelectric station in {region}. The station opened in {year} and generates "
         "{capacity} megawatts of electricity each year. It supplies nearby farms and homes. Operators monitor "
         "river flow to keep the regional grid stable.", "supplies", "supply"),
        ("Researchers established {entity}, a coastal laboratory in {region}, in {year}. The team processes "
         "{capacity} water samples each season. It serves university teams and local agencies. Staff calibrate "
         "instruments to preserve accurate marine observations.", "serves", "serve"),
        ("Near {region}, {entity} maintains an agricultural seed bank. The bank opened in {year} and stores "
         "{capacity} varieties of drought-resistant wheat. It distributes seed to farms and cooperatives. Curators "
         "monitor germination rates to protect regional crop diversity.", "distributes", "distribute"),
        ("{entity} runs a rural clinic in {region}. The clinic opened in {year} and records {capacity} patient "
         "visits each month. It treats families from nearby villages. Medical teams track medicine stocks to "
         "maintain continuous care.", "treats", "treat"),
        ("A rail terminal operated by {entity} connects {region} with the national freight network. It began "
         "service in {year} and handles {capacity} containers each week. It links farms with coastal ports. "
         "Dispatchers coordinate departures to reduce delays.", "links", "link"),
        ("{entity} manages a mineral processing plant in {region}. The facility opened in {year} and processes "
         "{capacity} tonnes of copper ore each year. It provides refined material to regional manufacturers. "
         "Engineers monitor water quality and equipment safety.", "provides", "provide"),
        ("The {region} weather observatory was founded by {entity} in {year}. Its instruments record "
         "{capacity} atmospheric readings each day. It supports local flood warnings and climate research. "
         "Scientists validate sensor data before publishing forecasts.", "supports", "support"),
        ("{entity} operates a water treatment works serving {region}. The plant opened in {year} and treats "
         "{capacity} litres of water each day. It delivers clean water to homes and schools. Technicians test "
         "samples to ensure the supply meets safety standards.", "delivers", "deliver"),
    )
    rows = []
    for index in range(count):
        entity = f"{given_names[index % len(given_names)]} Works {index:04d}"
        year = str(1980 + index % 44)
        capacity = str(40 + (index * 17) % 900)
        region = regions[(index // len(given_names)) % len(regions)]
        template, service_verb, service_base = templates[index % len(templates)]
        value = template.format(entity=entity, region=region, year=year, capacity=capacity)
        rows.append((uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-bench:{index}"), value,
                     {"entity": entity, "year": year, "capacity": capacity,
                      "service_verb": service_verb, "service_base": service_base}))
    return rows


def fact_examples() -> list[tuple[str, str, str]]:
    examples = [
        ("Revenue was $10 million after strong annual sales. The company published the figures in its yearly financial statement.",
         "Revenue was $20 million after strong annual sales. The company published the figures in its yearly financial statement.",
         "number"),
        ("The treaty was signed in 1998 after several years of negotiations. Officials released the agreement to the public.",
         "The treaty was signed in 2008 after several years of negotiations. Officials released the agreement to the public.",
         "date"),
        ("Company A acquired Company B in a transaction announced on Tuesday. The deal combined their regional manufacturing operations.",
         "Company C acquired Company B in a transaction announced on Tuesday. The deal combined their regional manufacturing operations.",
         "entity"),
        ("Company A acquired Company B in a transaction announced on Tuesday. The deal combined their regional manufacturing operations.",
         "Company A did not acquire Company B in a transaction announced on Tuesday. The deal combined their regional manufacturing operations.",
         "negation"),
    ]
    return examples


def make_corpus(pilot_rows: Iterable[tuple[uuid.UUID, str]], synthetic_count: int) -> tuple[list[Item], list[Query], dict[str, int]]:
    base_rows: list[tuple[uuid.UUID, str, dict[str, str]]] = [
        (item_id, text_value, {}) for item_id, text_value in pilot_rows
    ]
    base_rows.extend(synthetic_bases(synthetic_count))
    for index, (left, _right, _kind) in enumerate(fact_examples()):
        base_rows.append((uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-fact:{index}"), left, {}))

    items = [Item(item_id, value, features(value)) for item_id, value, _facts in base_rows]
    queries: list[Query] = []
    positive_variants = 0
    for item_id, value, facts in base_rows:
        exact = value
        queries.append(Query(exact, item_id, "exact"))

        punctuation = re.sub(r"\.\s+", "; ", value)
        whitespace = re.sub(r"\s+", "  ", value).strip()
        wording = value
        for old, new in (("operates", "runs"), ("generates", "produces"), ("supplies", "provides"),
                         ("processes", "handles"), ("stores", "keeps"), ("distributes", "delivers")):
            wording = _replace_once(wording, old, new)
        sentences = re.split(r"(?<=[.!?])\s+", value)
        reordered = " ".join((sentences[0], sentences[2], sentences[1], *sentences[3:])) if len(sentences) >= 3 else value
        heading = "## Regional infrastructure\n\n" + value
        seen_variants = {exact}
        variants = ((punctuation, "punctuation"), (whitespace, "whitespace"), (wording, "wording"),
                    (reordered, "reordered"), (heading, "heading"))
        for variant, variant_kind in variants:
            if variant in seen_variants:
                continue
            seen_variants.add(variant)
            queries.append(Query(variant, item_id, f"near_{variant_kind}"))
            positive_variants += 1

        if facts:
            changed_number = _replace_once(value, facts["capacity"], str(int(facts["capacity"]) + 37))
            changed_date = _replace_once(value, facts["year"], str(int(facts["year"]) + 10))
            changed_entity = value.replace(facts["entity"], "Keldran Systems")
            changed_negation = re.sub(
                rf"\bIt {re.escape(facts['service_verb'])}\b",
                f"It does not {facts['service_base']}", value, count=1,
            )
            for changed, kind in ((changed_number, "fact_number"), (changed_date, "fact_date"),
                                  (changed_entity, "fact_entity"), (changed_negation, "fact_negation")):
                queries.append(Query(changed, item_id, kind))

    for index, (original, changed, kind) in enumerate(fact_examples()):
        item_id = uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-fact:{index}")
        queries.append(Query(changed, item_id, f"fact_{kind}"))

    for index, (item_id, value, _facts) in enumerate(base_rows):
        other_id, other_text, _ = base_rows[(index + max(1, len(base_rows) // 3)) % len(base_rows)]
        if other_id != item_id:
            queries.append(Query(other_text, item_id, "unrelated"))

    counts = Counter(query.kind for query in queries)
    counts["positive_variants"] = positive_variants
    return items, queries, dict(sorted(counts.items()))


class CandidateIndex:
    name = "abstract"

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        raise NotImplementedError

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        raise NotImplementedError


class ReducedMinHash(CandidateIndex):
    name = "reduced_minhash_lsh_8x2"
    rows = 16
    bands = 8
    band_rows = 2

    def __init__(self) -> None:
        self.coefficients = []
        for seed in range(self.rows):
            raw = hashlib.blake2b(f"minhash:{seed}".encode(), digest_size=16).digest()
            a = 1 + int.from_bytes(raw[:8], "big") % (_PRIME - 1)
            b = int.from_bytes(raw[8:], "big") % _PRIME
            self.coefficients.append((a, b))

    def _signature(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        unsigned = tuple(value & ((1 << 64) - 1) for value in feature_set)
        if not unsigned:
            return ()
        return tuple(min((a * (value % _PRIME) + b) % _PRIME for value in unsigned)
                     for a, b in self.coefficients)

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        signature = self._signature(feature_set)
        return tuple(_signed(_h64(b"mh" + bytes([band]) + b"".join(
            struct.pack(">Q", value) for value in signature[band * self.band_rows:(band + 1) * self.band_rows]
        ))) for band in range(self.bands)) if signature else ()

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        return self.postings(feature_set)


class BoundedSimHash(CandidateIndex):
    name = "simhash_64_multindex_8x8"
    bands = 8

    def _signature(self, feature_set: frozenset[int]) -> int:
        accumulators = [0] * 64
        for value in feature_set:
            digest = _h64(struct.pack(">Q", value & ((1 << 64) - 1)))
            for bit in range(64):
                accumulators[bit] += 1 if (digest >> bit) & 1 else -1
        return sum((1 << bit) for bit, total in enumerate(accumulators) if total >= 0)

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        signature = self._signature(feature_set)
        return tuple(_signed((band << 8) | ((signature >> (band * 8)) & 0xFF)) for band in range(self.bands))

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        return self.postings(feature_set)


class SparsePrefix(CandidateIndex):
    name = "sparse_prefix_filter_078"
    threshold = NEAR_THRESHOLD

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        ordered = sorted(feature_set, key=lambda value: hashlib.blake2b(
            struct.pack(">Q", value & ((1 << 64) - 1)), digest_size=8, person=b"prefix-v1").digest())
        prefix_length = len(ordered) - math.ceil(self.threshold * len(ordered)) + 1
        return tuple(_signed(value) for value in ordered[:min(prefix_length, MAX_QUERY_FEATURES)])

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        ordered = sorted(feature_set, key=lambda value: hashlib.blake2b(
            struct.pack(">Q", value & ((1 << 64) - 1)), digest_size=8, person=b"prefix-v1").digest())
        return tuple(_signed(value) for value in ordered[:MAX_QUERY_FEATURES])


class SQLitePostings:
    def __init__(self, indexer: CandidateIndex) -> None:
        self.indexer = indexer
        handle = tempfile.NamedTemporaryFile(prefix="kbp-chunk-index-", suffix=".sqlite3", delete=False)
        self.path = Path(handle.name)
        handle.close()
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA cache_size=-4096")
        self.connection.execute("CREATE TABLE postings (key INTEGER NOT NULL, item BLOB NOT NULL, PRIMARY KEY (key, item)) WITHOUT ROWID")

    def build(self, items: list[Item]) -> tuple[float, int, int]:
        entry_count = 0
        start = time.perf_counter()
        tracemalloc.start()
        with self.connection:
            batch: list[tuple[int, bytes]] = []
            for item in items:
                for key in self.indexer.postings(item.features):
                    batch.append((key, item.item_id.bytes))
                if len(batch) >= 8192:
                    self.connection.executemany("INSERT OR IGNORE INTO postings VALUES (?, ?)", batch)
                    entry_count += len(batch)
                    batch.clear()
            if batch:
                self.connection.executemany("INSERT OR IGNORE INTO postings VALUES (?, ?)", batch)
                entry_count += len(batch)
        elapsed = time.perf_counter() - start
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.connection.execute("VACUUM")
        self.connection.commit()
        return elapsed, entry_count, peak

    def lookup(self, query: Query) -> tuple[list[uuid.UUID], int]:
        keys = self.indexer.probes(features(query.text))
        counts: Counter[bytes] = Counter()
        probes = 0
        for key in keys:
            rows = self.connection.execute(
                "SELECT item FROM postings WHERE key = ? ORDER BY item LIMIT ?",
                (key, MAX_BUCKET_ROWS),
            ).fetchall()
            probes += len(rows)
            counts.update(row[0] for row in rows)
        ranked = sorted(counts.items(), key=lambda entry: (-entry[1], entry[0]))[:MAX_CANDIDATES]
        return [uuid.UUID(bytes=item) for item, _hits in ranked], len(keys)

    def close(self) -> int:
        self.connection.close()
        size = self.path.stat().st_size
        self.path.unlink(missing_ok=True)
        return size


def _percentile95(values: list[float]) -> float:
    if not values:
        return 0.0
    return sorted(values)[max(0, math.ceil(0.95 * len(values)) - 1)]


def benchmark(indexer: CandidateIndex, items: list[Item], queries: list[Query]) -> dict[str, object]:
    posting_counts = [len(indexer.postings(item.features)) for item in items]
    index = SQLitePostings(indexer)
    elapsed, entry_count, peak_bytes = index.build(items)
    item_map = {item.item_id: item for item in items}
    item_facts = {item.item_id: fact_signature(item.text) for item in items}
    latencies_ms: list[float] = []
    counts: list[int] = []
    probe_counts: list[int] = []
    near_queries = [query for query in queries if query.kind.startswith("near_")]
    exact_queries = [query for query in queries if query.kind == "exact"]
    fact_queries = [query for query in queries if query.kind.startswith("fact_")]
    near_hits = 0
    near_hits_by_variant: Counter[str] = Counter()
    exact_hits = 0
    false_candidate_pairs = 0
    sampled_candidate_pairs = 0
    fact_candidates = 0
    near_target_scores: list[tuple[float, bool, bool]] = []
    fact_target_scores: list[tuple[float, bool, bool]] = []
    verification_latencies_ms: list[float] = []
    verified_per_query: list[int] = []
    verification_stride = max(1, math.ceil(len(queries) / EXACT_VERIFY_SAMPLE_LIMIT))
    verification_sample_queries = 0

    for query_number, query in enumerate(queries):
        start = time.perf_counter()
        candidate_ids, key_count = index.lookup(query)
        latencies_ms.append((time.perf_counter() - start) * 1000)
        counts.append(len(candidate_ids))
        probe_counts.append(key_count)
        returned = set(candidate_ids)
        if query.kind.startswith("near_") and query.target_id in returned:
            near_hits += 1
            near_hits_by_variant[query.kind] += 1
        if query.kind == "exact" and query.target_id in returned:
            exact_hits += 1
        if query.kind.startswith("fact_") and query.target_id in returned:
            fact_candidates += 1

        query_features = features(query.text)
        query_facts = fact_signature(query.text)
        target_item = item_map[query.target_id]
        target_score = jaccard(query_features, target_item.features)
        same_target_facts = query_facts == item_facts[query.target_id]
        if query.kind.startswith("near_"):
            near_target_scores.append((target_score, same_target_facts, query.target_id in returned))
        elif query.kind.startswith("fact_"):
            fact_target_scores.append((target_score, same_target_facts, query.target_id in returned))

        if query_number % verification_stride == 0:
            verification_sample_queries += 1
            verify_start = time.perf_counter()
            accepted_near = 0
            for candidate_id in candidate_ids:
                candidate = item_map[candidate_id]
                if (jaccard(query_features, candidate.features) >= NEAR_THRESHOLD
                        and query_facts == item_facts[candidate_id]):
                    accepted_near += 1
            verification_latencies_ms.append((time.perf_counter() - verify_start) * 1000)
            verified_per_query.append(accepted_near)
            sampled_candidate_pairs += len(candidate_ids)
            false_candidate_pairs += len(candidate_ids) - accepted_near

    file_bytes = index.close()
    fact_direct_scores = [verified_class(query.text, item_map[query.target_id].text)
                          for query in fact_queries]
    fact_false_near = sum(classification == "NEAR_DUPLICATE" for classification, _score in fact_direct_scores)
    fact_similarity_values = [score for _classification, score in fact_direct_scores]
    threshold_sweep = []
    for threshold in VERIFY_THRESHOLDS:
        positive_hits = sum(hit and fact_match and score >= threshold
                            for score, fact_match, hit in near_target_scores)
        lexical_fact_false = sum(hit and score >= threshold for score, _fact_match, hit in fact_target_scores)
        guarded_fact_false = sum(hit and fact_match and score >= threshold
                                 for score, fact_match, hit in fact_target_scores)
        threshold_sweep.append({
            "near_threshold": threshold,
            "end_to_end_near_recall": round(positive_hits / len(near_target_scores), 6)
            if near_target_scores else 0.0,
            "fact_pairs_candidate_hit": sum(hit for _score, _fact_match, hit in fact_target_scores),
            "lexical_only_fact_false_near": lexical_fact_false,
            "fact_guarded_fact_false_near": guarded_fact_false,
        })

    return {
        "approach": indexer.name,
        "corpus_chunks": len(items),
        "index_entries": entry_count,
        "postings_per_chunk_avg": round(entry_count / len(items), 3),
        "postings_per_chunk_p95": _percentile95([float(value) for value in posting_counts]),
        "signature_bytes_per_chunk": {
            "reduced_minhash_lsh_8x2": 16 * 8,
            "simhash_64_multindex_8x8": 8,
            "sparse_prefix_filter_078": 0,
        }[indexer.name],
        "sqlite_index_bytes": file_bytes,
        "sqlite_bytes_per_chunk": round(file_bytes / len(items), 2),
        "build_seconds": round(elapsed, 6),
        "build_chunks_per_second": round(len(items) / elapsed, 1) if elapsed else None,
        "python_builder_peak_mib_excluding_corpus": round(peak_bytes / (1024 * 1024), 3),
        "queries": len(queries),
        "known_near_queries": len(near_queries),
        "known_near_recall": round(near_hits / len(near_queries), 6) if near_queries else 0.0,
        "near_recall_by_variant": {
            kind: round(near_hits_by_variant[kind] / sum(query.kind == kind for query in near_queries), 6)
            for kind in sorted({query.kind for query in near_queries})
        },
        "exact_query_recall": round(exact_hits / len(exact_queries), 6) if exact_queries else 0.0,
        "false_candidate_rate": round(false_candidate_pairs / sampled_candidate_pairs, 6)
        if sampled_candidate_pairs else 0.0,
        "candidates_per_query_avg": round(statistics.fmean(counts), 3) if counts else 0.0,
        "candidates_per_query_p95": _percentile95([float(value) for value in counts]),
        "candidates_per_query_max": max(counts, default=0),
        "query_keys_avg": round(statistics.fmean(probe_counts), 3) if probe_counts else 0.0,
        "lookup_ms_avg": round(statistics.fmean(latencies_ms), 4) if latencies_ms else 0.0,
        "lookup_ms_p95": round(_percentile95(latencies_ms), 4),
        "fact_mismatch_queries": len(fact_queries),
        "fact_mismatch_candidate_rate": round(fact_candidates / len(fact_queries), 6) if fact_queries else 0.0,
        "fact_mismatch_false_near_decisions": fact_false_near,
        "fact_pair_similarity_avg": round(statistics.fmean(fact_similarity_values), 6) if fact_similarity_values else 0.0,
        "exact_verification": {
            "method": "word-trigram Jaccard plus exact salient-fact signature equality",
            "sampled_queries": verification_sample_queries,
            "sampled_candidate_pairs": sampled_candidate_pairs,
            "comparisons_avg_per_query": round(statistics.fmean(verified_per_query), 3) if verified_per_query else 0.0,
            "comparisons_p95_per_query": _percentile95([float(value) for value in verified_per_query]),
            "latency_ms_avg_per_query": round(statistics.fmean(verification_latencies_ms), 4)
            if verification_latencies_ms else 0.0,
            "latency_ms_p95_per_query": round(_percentile95(verification_latencies_ms), 4),
            "threshold_sweep": threshold_sweep,
        },
        "limits": {"bucket_rows": MAX_BUCKET_ROWS, "candidates": MAX_CANDIDATES},
    }


def project(result: dict[str, object]) -> list[dict[str, object]]:
    bytes_per_chunk = float(result["sqlite_bytes_per_chunk"])
    rows = []
    for count in (1_000_000, 100_000_000, 1_000_000_000, 7_000_000_000):
        projected = bytes_per_chunk * count
        rows.append({
            "chunks": count,
            "projected_bytes": round(projected),
            "projected_gib": round(projected / (1024 ** 3), 3),
            "basis": "EXTRAPOLATED from this run's measured SQLite bytes/chunk; not measured at this scale",
        })
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-url-env", default="KB_INTEGRATION_DATABASE_URL",
                        help="environment variable containing a password-free kb_pipeline_test runtime URL")
    parser.add_argument("--synthetic-chunks", type=int, default=512)
    parser.add_argument("--output", type=Path, help="optional path for aggregate JSON results (no pilot text is written)")
    args = parser.parse_args(argv)
    if not 32 <= args.synthetic_chunks <= 100_000:
        parser.error("--synthetic-chunks must be between 32 and 100000")
    pilot_url = os.environ.get(args.pilot_url_env, "").strip()
    if not pilot_url:
        parser.error(f"{args.pilot_url_env} is required; pilot rows are read from kb_pipeline_test only")

    pilot_rows = load_pilot(pilot_url)
    items, queries, query_counts = make_corpus(pilot_rows, args.synthetic_chunks)
    results = [benchmark(indexer, items, queries) for indexer in
               (ReducedMinHash(), BoundedSimHash(), SparsePrefix())]
    output = {
        "database_source": REQUIRED_DATABASE,
        "pilot_chunks_loaded": len(pilot_rows),
        "synthetic_chunks": args.synthetic_chunks,
        "corpus_chunks_indexed": len(items),
        "query_categories": query_counts,
        "measurement_note": "Timings, SQLite bytes and builder heap are measured on this corpus and machine. "
                            "Scale projections are extrapolations, not 1M+ measurements.",
        "candidate_approaches": [
            {**result, "scale_projections": project(result)} for result in results
        ],
        "verification": {
            "method": "exact Jaccard of canonical-text word-trigram sets; near threshold 0.78; "
                       "related threshold 0.32",
            "fact_guard": "near-duplicate requires exact equality of extracted numeric/date tokens, "
                          "mid-sentence capitalized entity tokens, and negation tokens",
            "fact_mismatch_cases": len([query for query in queries if query.kind.startswith("fact_")]),
        },
    }
    rendered = json.dumps(output, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())