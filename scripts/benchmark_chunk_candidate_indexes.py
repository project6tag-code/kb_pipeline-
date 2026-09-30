"""Offline benchmark of chunk near-duplicate candidate indexes (design phase only).

It reads the chunk-pilot rows READ ONLY from kb_pipeline_test (runtime role,
``BEGIN READ ONLY``) and adds a deterministic synthetic corpus. Every index is a
temporary SQLite file that is deleted after the run. Nothing is written to
PostgreSQL, and chunk identity is never touched.

Corpus
------
* pilot chunks (the real CC and GitHub chunks);
* templated families: many chunks share one template with different
  entity, year and quantity. This is the "same topic, different facts" stress;
* a diverse background: Zipfian vocabulary plus a Zipfian function-word stream,
  with numbers, units, years and named entities. It is used at several index
  sizes, so bucket growth with N is MEASURED, not assumed;
* distinct normalized-duplicate items: different chunk_text (so a different
  chunk_id), with identical comparison features;
* fixed fact-mismatch examples.

Queries
-------
* ``near_*`` positives (the target must be found and verified NEAR):
  punctuation, whitespace, small wording change, sentence reorder, heading
  context, CC->GitHub markdown reformatting (with and without a word edit), and
  distinct normalized duplicates;
* ``fact_*`` negatives against the target: number, unit/quantity, date,
  entity, negation. They must never be NEAR;
* ``novel``: fresh background text that is not indexed. It must never be NEAR,
  and it measures false collision load.

Measured numbers are separated from extrapolations, which are labelled as such.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import random
import re
import sqlite3
import statistics
import struct
import tempfile
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REQUIRED_DATABASE = "kb_pipeline_test"
REQUIRED_ROLE = "kb_pipeline_app"
PILOT_REPOSITORY_ID = 900_000_000_002
PILOT_URL_MARKER = "%chunk-pilot.example%"
NEAR_THRESHOLD = 0.70
RELATED_THRESHOLD = 0.30
VERIFY_THRESHOLDS = (0.60, 0.65, 0.70, 0.75, 0.78, 0.82, 0.86)
MAX_BUCKET_ROWS = 256
MAX_CANDIDATES = 64
MAX_QUERY_FEATURES = 512
SCALE_POINTS = (1_000_000, 100_000_000, 1_000_000_000, 7_000_000_000)
_M64 = (1 << 64) - 1
_PRIME = (1 << 61) - 1
_WORD = re.compile(r"\w+", re.UNICODE)

# ---------------------------------------------------------------- comparison features


def _h64(data: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def _signed(value: int) -> int:
    value &= _M64
    return value if value < (1 << 63) else value - (1 << 64)


def _mix64(x: int) -> int:  # splitmix64 finalizer
    x = (x + 0x9E3779B97F4A7C15) & _M64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _M64
    return x ^ (x >> 31)


def features(text_value: str) -> frozenset[int]:
    """Case-folded word 3-gram hashes. Punctuation, markup and whitespace are ignored (comparison only)."""
    words = [token.casefold() for token in _WORD.findall(text_value)]
    if not words:
        return frozenset()
    if len(words) < 3:
        return frozenset({_h64(" ".join(words).encode("utf-8"))})
    return frozenset(_h64(" ".join(words[i:i + 3]).encode("utf-8")) for i in range(len(words) - 2))


def jaccard(left: frozenset[int], right: frozenset[int]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


# ---------------------------------------------------------------- fact-preservation guard

_UNITS = frozenset("""
    percent per cent % million billion trillion thousand hundred dozen
    mm cm m km kilometre kilometres kilometer kilometers metre metres meter meters mile miles ft feet foot inch inches
    mg g kg gram grams kilogram kilograms tonne tonnes ton tons lb lbs pound pounds ounce ounces
    ml l litre litres liter liters gallon gallons
    w kw mw gw watt watts kilowatt kilowatts megawatt megawatts gigawatt gigawatts kwh mwh gwh volt volts v
    hz khz mhz ghz b kb mb gb tb pb byte bytes bit bits
    s ms sec second seconds minute minutes min hour hours h day days week weeks month months year years decade decades
    century centuries °c °f c f k celsius fahrenheit kelvin degree degrees
    usd eur gbp dollars dollar euros euro pounds cents people persons""".split())
_MONTHS = frozenset("january february march april may june july august september october november december "
                    "jan feb mar apr jun jul aug sep sept oct nov dec".split())
_NUMBER = re.compile(r"(?<![\w.])([$£€¥]\s?)?(\d+(?:,\d{3})*(?:\.\d+)?)(?:\s?(%|°[cfCF]|[A-Za-z]+))?")
_ENTITY_WORD = r"[A-Z][\w'’&-]*(?:\.[\w'’&-]+)*"  # internal dots only (U.S), never a sentence-final one
_ENTITY_RUN = re.compile(rf"(?<![\w'’]){_ENTITY_WORD}(?:\s+(?:of\s+|de\s+|the\s+)?{_ENTITY_WORD})*")
_NEGATION = re.compile(r"\b(?:not|no|never|neither|nor|none|nobody|nothing|nowhere|without|cannot|\w+n['’]t)\b",
                       re.I)
_SENTENCE_FUNCTION = frozenset("""
    the a an this that these those it its in on at for by with from near during after before when while however
    our their his her we they there each every some many most all both as if but and or so also then thus
    although because since until unlike like unless according""".split())
_MARKUP = " \t*_`#>|-([{\"'“‘"
_LIST_MARKER = re.compile(r"^\s*(?:[-*+>]|\d{1,3}[.)])\s+")
_HSPACE = re.compile(r"[^\S\n]+")
_EMPHASIS = re.compile(r"(?<!\w)[*_`]+|[*_`]+(?!\w)")


def fact_signature(text_value: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """(numbers+units+months, named entities, negations): each is a sorted multiset.

    NEAR_DUPLICATE requires equality, so any change in a number, unit, date,
    entity or negation keeps two chunks apart. Heading lines are context, not
    content, and are ignored. Markdown markup is ignored when deciding sentence
    starts, so CC prose and its GitHub markdown form get the same signature.
    """
    lines = [_LIST_MARKER.sub("", line) for line in text_value.split("\n") if not line.lstrip().startswith("#")]
    body = _HSPACE.sub(" ", _EMPHASIS.sub("", "\n".join(lines)))
    numbers: list[str] = []
    for match in _NUMBER.finditer(body):
        currency, value, unit = match.group(1) or "", match.group(2).replace(",", ""), (match.group(3) or "")
        unit = unit.casefold()
        numbers.append(f"{currency.strip()}{value}{(' ' + unit) if unit in _UNITS else ''}")
    numbers.extend(f"month:{w.casefold()}" for w in _WORD.findall(body) if w.casefold() in _MONTHS and w[0].isupper())
    entities: list[str] = []
    for match in _ENTITY_RUN.finditer(body):
        words = match.group(0).split()
        prefix = body[:match.start()].rstrip(_MARKUP)
        if not prefix or prefix[-1] in ".!?:;\n":  # sentence start: first capital is grammar, not a name
            if words[0].casefold() in _SENTENCE_FUNCTION or len(words) == 1:
                words = words[1:]
        words = [w.strip(".") for w in words if w.strip(".")]
        if words and not all(w.casefold() in _MONTHS for w in words):
            entities.append(" ".join(w.casefold() for w in words))
    negations = [m.group(0).casefold().replace("’", "'") for m in _NEGATION.finditer(body)]
    return tuple(sorted(numbers)), tuple(sorted(entities)), tuple(sorted(negations))


def verified_class(left: str, right: str, near_threshold: float = NEAR_THRESHOLD) -> tuple[str, float]:
    return classify(features(left), features(right), fact_signature(left), fact_signature(right), near_threshold)


def classify(left: frozenset[int], right: frozenset[int], left_facts: tuple, right_facts: tuple,
             near_threshold: float = NEAR_THRESHOLD) -> tuple[str, float]:
    score = jaccard(left, right)
    if score >= near_threshold and left_facts == right_facts:
        return "NEAR_DUPLICATE", score
    if score >= RELATED_THRESHOLD:
        return "RELATED", score
    return "UNIQUE", score


# ---------------------------------------------------------------- corpus


@dataclass(frozen=True)
class Item:
    item_id: uuid.UUID
    text: str
    features: frozenset[int]
    group: str = ""

    @property
    def key(self) -> int:  # production form: first 8 bytes of chunk_hash (sha256 of chunk_text)
        return int.from_bytes(hashlib.sha256(self.text.encode("utf-8")).digest()[:8], "big", signed=True)


@dataclass(frozen=True)
class Query:
    text: str
    target_id: uuid.UUID | None
    kind: str
    own_id: uuid.UUID | None = None


@dataclass
class Base:
    item_id: uuid.UUID
    text: str
    group: str
    origin: str
    facts: dict[str, str]


def load_pilot(url: str) -> list[tuple[uuid.UUID, str]]:
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool

    pilot_sql = """
    SELECT DISTINCT c.chunk_id, c.chunk_text
    FROM kbp.chunk_registry AS c
    JOIN kbp.chunk_occurrences AS co ON co.chunk_id = c.chunk_id
    JOIN kbp.observation_extractions AS e ON e.document_id = co.document_id AND e.outcome = 'document'
    LEFT JOIN kbp.source_observations AS s ON s.observation_id = e.observation_id
    LEFT JOIN kbp.github_file_observations AS gh ON gh.observation_id = e.observation_id
    WHERE s.source_url LIKE :url_marker OR gh.repository_id = :repository_id
    ORDER BY c.chunk_id
    LIMIT 128
    """
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
                rows = connection.execute(text(pilot_sql), {"url_marker": PILOT_URL_MARKER,
                                                            "repository_id": PILOT_REPOSITORY_ID}).all()
                return [(row[0], row[1]) for row in rows]
            finally:
                connection.rollback()
    finally:
        engine.dispose()


def synthetic_bases(count: int) -> list[tuple[uuid.UUID, str, dict[str, str]]]:
    """Templated families: same topic and wording, different entity, region, year and quantity."""
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
                     {"entity": entity, "year": year, "number": capacity,
                      "service_verb": service_verb, "service_base": service_base}))
    return rows


_FUNCTION_WORDS = ("the of and to in a is that for it as was with on by are this be from at or an which its also "
                   "has were their can more one other such used between after into most these during than many "
                   "some where while each first both through over two new only under may when about").split()
_UNIT_CHOICES = ("megawatts", "tonnes", "kilometres", "litres", "percent", "people", "hectares", "kg", "years")


def _vocabulary(rng: random.Random, size: int) -> list[str]:
    onsets = ("b c d f g h k l m n p r s t v w z br cr dr fl gr pl st tr sh ch th sp").split()
    vowels = ("a e i o u ai ea io ou ei").split()
    codas = ("", "", "n", "r", "s", "t", "l", "m", "nd", "st", "rt", "ng", "x")
    words: set[str] = set()
    while len(words) < size:
        syllables = rng.choice((1, 2, 2, 2, 3, 3, 4))
        words.add("".join(rng.choice(onsets) + rng.choice(vowels) for _ in range(syllables)) + rng.choice(codas))
    ordered = sorted(words)
    rng.shuffle(ordered)
    return ordered


class BackgroundGenerator:
    """Deterministic diverse prose: Zipfian content words and function words, numbers, units, years, names."""

    def __init__(self, seed: int, vocabulary_size: int = 60_000) -> None:
        self.rng = random.Random(seed)
        vocab_rng = random.Random(20260930)  # one shared language for every seed
        self.vocab = _vocabulary(vocab_rng, vocabulary_size)
        self.vocab_cum = list(itertools.accumulate(1 / (rank + 1) ** 1.05 for rank in range(len(self.vocab))))
        self.function_cum = list(itertools.accumulate(1 / (rank + 1) ** 0.9 for rank in range(len(_FUNCTION_WORDS))))
        self.names = [w.capitalize() for w in self.vocab[5_000:15_000]]

    def chunk(self) -> tuple[str, dict[str, str]]:
        rng = self.rng
        facts: dict[str, str] = {}
        sentences = []
        for position in range(rng.randint(3, 7)):
            length = rng.randint(8, 20)
            content = rng.choices(self.vocab, cum_weights=self.vocab_cum, k=length)
            function = rng.choices(_FUNCTION_WORDS, cum_weights=self.function_cum, k=length)
            words = [f if rng.random() < 0.45 else c for c, f in zip(content, function)]
            if position == 0:
                words.insert(min(3, len(words)), "was")
            if rng.random() < 0.35:
                if rng.random() < 0.5:
                    year = str(rng.randint(1850, 2025))
                    facts.setdefault("year", year)
                    words.insert(rng.randint(1, len(words)), f"in {year}")
                else:
                    number = str(rng.randint(2, 99_999))
                    facts.setdefault("number", number)
                    words.insert(rng.randint(1, len(words)), f"{number} {rng.choice(_UNIT_CHOICES)}")
            if rng.random() < 0.30:
                name = f"{rng.choice(self.names)} {rng.choice(self.names)}"
                facts.setdefault("entity", name)
                words.insert(rng.randint(1, len(words)), name)
            sentence = " ".join(words)
            sentences.append(sentence[0].upper() + sentence[1:] + ".")
        return " ".join(sentences), facts


def fact_examples() -> list[tuple[str, str, str]]:
    return [
        ("Revenue was $10 million after strong annual sales. The company published the figures in its yearly "
         "financial statement.",
         "Revenue was $20 million after strong annual sales. The company published the figures in its yearly "
         "financial statement.", "number"),
        ("The treaty was signed in 1998 after several years of negotiations. Officials released the agreement "
         "to the public.",
         "The treaty was signed in 2008 after several years of negotiations. Officials released the agreement "
         "to the public.", "date"),
        ("The station was commissioned on March 3, 2020 after a long review. Engineers connected it to the grid "
         "the following week.",
         "The station was commissioned on April 3, 2020 after a long review. Engineers connected it to the grid "
         "the following week.", "date"),
        ("Company A acquired Company B in a transaction announced on Tuesday. The deal combined their regional "
         "manufacturing operations.",
         "Company C acquired Company B in a transaction announced on Tuesday. The deal combined their regional "
         "manufacturing operations.", "entity"),
        ("The library is maintained by the NASA archive team and is updated every quarter with new mission "
         "records and images.",
         "The library is maintained by the ESA archive team and is updated every quarter with new mission "
         "records and images.", "entity"),
        ("Company A acquired Company B in a transaction announced on Tuesday. The deal combined their regional "
         "manufacturing operations.",
         "Company A did not acquire Company B in a transaction announced on Tuesday. The deal combined their "
         "regional manufacturing operations.", "negation"),
        ("The dam generates 900 megawatts of electricity for the surrounding region and its three largest "
         "cities every year.",
         "The dam generates 900 kilowatts of electricity for the surrounding region and its three largest "
         "cities every year.", "unit"),
    ]


def _replace_once(text_value: str, old: str, new: str) -> str:
    return text_value.replace(old, new, 1) if old in text_value else text_value


def _markdown(text_value: str, heading: str = "Overview") -> str:
    """GitHub README rendering of CC prose: heading, one bullet per sentence, first word bold."""
    sentences = re.split(r"(?<=[.!?])\s+", text_value.strip())
    first, _, rest = sentences[0].partition(" ")
    bullets = [f"- **{first}** {rest}".rstrip()] + [f"- {s}" for s in sentences[1:]]
    return f"## {heading}\n\n" + "\n".join(bullets)


def _content_edit(text_value: str, rng: random.Random, vocab: list[str], edits: int) -> str:
    words = text_value.split(" ")
    positions = [i for i, w in enumerate(words) if w.isalpha() and w.islower() and w not in _FUNCTION_WORDS
                 and w != "not" and w not in _UNITS]
    for i in sorted(rng.sample(positions, min(edits, len(positions)))):
        words[i] = rng.choice(vocab[:20_000])
    return " ".join(words)


def make_corpus(pilot_rows: Iterable[tuple[uuid.UUID, str]], template_count: int, background_count: int,
                queries_per_kind: int, seed: int = 7) -> tuple[list[Item], list[Query], dict[str, int]]:
    background = BackgroundGenerator(seed)
    edit_rng = random.Random(seed + 1)
    bases: list[Base] = [Base(item_id, value, f"pilot:{item_id}", "pilot", {}) for item_id, value in pilot_rows]
    bases += [Base(i, v, f"template:{i}", "template", f) for i, v, f in synthetic_bases(template_count)]
    for index, (left, _right, _kind) in enumerate(fact_examples()):
        item_id = uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-fact:{index}")
        if all(b.item_id != item_id for b in bases) and all(b.text != left for b in bases):
            bases.append(Base(item_id, left, f"fact:{index}", "fact", {}))
    for index in range(background_count):
        value, facts = background.chunk()
        item_id = uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-background:{seed}:{index}")
        bases.append(Base(item_id, value, f"background:{index}", "background", facts))

    items = [Item(b.item_id, b.text, features(b.text), b.group) for b in bases]
    queries: list[Query] = []
    # distinct normalized duplicates: different chunk_text (another chunk_id), identical comparison features
    for index, base in enumerate(bases):
        if index % 10 != 3:
            continue
        variant = base.text.replace(". ", "; ", 1) if ". " in base.text else base.text + "."
        if variant == base.text:
            continue
        dup_id = uuid.uuid5(uuid.NAMESPACE_URL, f"chunk-index-dup:{base.item_id}")
        items.append(Item(dup_id, variant, features(variant), base.group))
        queries.append(Query(variant, base.item_id, "near_distinct_normalized_duplicate", own_id=dup_id))

    template_synonyms = (("operates", "runs"), ("generates", "produces"), ("supplies", "provides"),
                         ("processes", "handles"), ("stores", "keeps"), ("distributes", "delivers"))
    for base in bases:
        value = base.text
        sentences = re.split(r"(?<=[.!?])\s+", value)
        variants = [
            (re.sub(r"\.\s+", "; ", value), "near_punctuation"),
            (re.sub(r"\s+", "  ", value).strip(), "near_whitespace"),
            ("## Regional infrastructure\n\n" + value, "near_heading"),
            (_markdown(value), "near_cross_source_markdown"),
        ]
        if len(sentences) >= 3:
            variants.append((" ".join((sentences[0], sentences[2], sentences[1], *sentences[3:])), "near_reordered"))
        if base.origin == "template":
            wording = value
            for old, new in template_synonyms:
                wording = _replace_once(wording, old, new)
            variants.append((wording, "near_wording_template_synonyms"))
        else:
            variants.append((_content_edit(value, edit_rng, background.vocab, 2), "near_wording_2_words"))
            variants.append((_markdown(_content_edit(value, edit_rng, background.vocab, 1)),
                             "near_cross_source_markdown_edit"))
        for variant, kind in variants:
            if variant != value:
                queries.append(Query(variant, base.item_id, kind))

        facts = base.facts
        mutations = []
        if "number" in facts:
            mutations.append((_replace_once(value, facts["number"], str(int(facts["number"]) + 37)), "fact_number"))
        if "year" in facts:
            mutations.append((_replace_once(value, facts["year"], str(int(facts["year"]) + 10)), "fact_date"))
        if "entity" in facts:
            mutations.append((value.replace(facts["entity"], "Keldran Systems"), "fact_entity"))
        if "service_verb" in facts:
            mutations.append((re.sub(rf"\bIt {re.escape(facts['service_verb'])}\b",
                                     f"It does not {facts['service_base']}", value, count=1), "fact_negation"))
        elif base.origin == "background":
            mutations.append((_replace_once(value, " was ", " was not "), "fact_negation"))
        for changed, kind in mutations:
            if changed != value:
                queries.append(Query(changed, base.item_id, kind))
    for index, (original, changed, kind) in enumerate(fact_examples()):
        target = next(b.item_id for b in bases if b.text == original)
        queries.append(Query(changed, target, f"fact_{kind}_fixed"))

    novel = BackgroundGenerator(seed + 1000)
    for _ in range(queries_per_kind):
        queries.append(Query(novel.chunk()[0], None, "novel"))

    sampled: list[Query] = []
    by_kind: dict[str, list[Query]] = {}
    for query in queries:
        by_kind.setdefault(query.kind, []).append(query)
    for kind in sorted(by_kind):
        ordered = sorted(by_kind[kind], key=lambda q: hashlib.sha256(q.text.encode()).digest())
        sampled.extend(ordered if kind.endswith("_fixed") else ordered[:queries_per_kind])
    counts = Counter(query.kind for query in sampled)
    return items, sampled, dict(sorted(counts.items()))


# ---------------------------------------------------------------- candidate indexes


class CandidateIndex:
    name = "abstract"
    signature_bytes = 0

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        raise NotImplementedError

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        return self.postings(feature_set)


class ReducedMinHash(CandidateIndex):
    """Copilot prototype: classic k-hash MinHash, 8 bands x 2 rows."""

    name = "reduced_minhash_lsh_8x2"
    rows = 16
    bands = 8
    band_rows = 2
    signature_bytes = 16 * 8

    def __init__(self) -> None:
        self.coefficients = []
        for seed in range(self.rows):
            raw = hashlib.blake2b(f"minhash:{seed}".encode(), digest_size=16).digest()
            self.coefficients.append((1 + int.from_bytes(raw[:8], "big") % (_PRIME - 1),
                                      int.from_bytes(raw[8:], "big") % _PRIME))

    def _signature(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        unsigned = tuple(value & _M64 for value in feature_set)
        if not unsigned:
            return ()
        return tuple(min((a * (value % _PRIME) + b) % _PRIME for value in unsigned) for a, b in self.coefficients)

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        signature = self._signature(feature_set)
        return tuple(_signed(_h64(b"mh" + bytes([band]) + b"".join(
            struct.pack(">Q", value) for value in signature[band * self.band_rows:(band + 1) * self.band_rows]
        ))) for band in range(self.bands)) if signature else ()


class OnePermutationMinHash(CandidateIndex):
    """One-permutation MinHash (one hash per feature, rotation densification) with configurable b x r LSH bands."""

    def __init__(self, bands: int, band_rows: int) -> None:
        self.bands, self.band_rows = bands, band_rows
        self.bins = bands * band_rows
        self.name = f"minhash_oph_lsh_{bands}x{band_rows}"
        self.signature_bytes = self.bins * 8

    def _signature(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        if not feature_set:
            return ()
        bins: list[int | None] = [None] * self.bins
        for value in feature_set:
            mixed = _mix64(value & _M64)
            slot, rank = mixed % self.bins, mixed // self.bins
            current = bins[slot]
            if current is None or rank < current:
                bins[slot] = rank
        out = []
        for i in range(self.bins):
            if bins[i] is not None:
                out.append(bins[i])
                continue
            for step in range(1, self.bins):
                j = (i + step) % self.bins
                if bins[j] is not None:
                    out.append(bins[j] + step * (1 << 64))
                    break
        return tuple(out)

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        signature = self._signature(feature_set)
        if not signature:
            return ()
        r = self.band_rows
        return tuple(_signed(_h64(f"oph{self.bins}:{band}:{','.join(map(str, signature[band * r:(band + 1) * r]))}"
                                  .encode("ascii"))) for band in range(self.bands))


class BoundedSimHash(CandidateIndex):
    """Copilot prototype: 64-bit SimHash, 8 exact-match bands of 8 bits."""

    name = "simhash_64_multindex_8x8"
    bands = 8
    signature_bytes = 8

    def _signature(self, feature_set: frozenset[int]) -> int:
        accumulators = [0] * 64
        for value in feature_set:
            digest = _h64(struct.pack(">Q", value & _M64))
            for bit in range(64):
                accumulators[bit] += 1 if (digest >> bit) & 1 else -1
        return sum((1 << bit) for bit, total in enumerate(accumulators) if total >= 0)

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        signature = self._signature(feature_set)
        return tuple(_signed((band << 8) | ((signature >> (band * 8)) & 0xFF)) for band in range(self.bands))


class SparsePrefix(CandidateIndex):
    """Copilot prototype: prefix filtering at the near threshold, with a global hash order (no frequency order)."""

    name = "sparse_prefix_filter"
    threshold = NEAR_THRESHOLD

    @staticmethod
    def _ordered(feature_set: frozenset[int]) -> list[int]:
        return sorted(feature_set, key=lambda value: hashlib.blake2b(
            struct.pack(">Q", value & _M64), digest_size=8, person=b"prefix-v1").digest())

    def postings(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        ordered = self._ordered(feature_set)
        prefix_length = len(ordered) - math.ceil(self.threshold * len(ordered)) + 1
        return tuple(_signed(value) for value in ordered[:min(prefix_length, MAX_QUERY_FEATURES)])

    def probes(self, feature_set: frozenset[int]) -> tuple[int, ...]:
        return tuple(_signed(value) for value in self._ordered(feature_set)[:MAX_QUERY_FEATURES])


def default_indexers() -> list[CandidateIndex]:
    return [ReducedMinHash(), OnePermutationMinHash(8, 2), OnePermutationMinHash(10, 4), BoundedSimHash(),
            SparsePrefix()]


# ---------------------------------------------------------------- SQLite posting store


class SQLitePostings:
    """(band key, item key) postings. Item key = int64 chunk-hash prefix (production form) or a 16-byte UUID."""

    def __init__(self, indexer: CandidateIndex, *, item_encoding: str = "int64", directory: str | None = None) -> None:
        self.indexer = indexer
        self.item_encoding = item_encoding
        handle = tempfile.NamedTemporaryFile(prefix="kbp-chunk-index-", suffix=".sqlite3", delete=False,
                                             dir=directory)
        self.path = Path(handle.name)
        handle.close()
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA cache_size=-65536")
        item_type = "INTEGER" if item_encoding == "int64" else "BLOB"
        self.connection.execute(f"CREATE TABLE postings (key INTEGER NOT NULL, item {item_type} NOT NULL, "
                                "PRIMARY KEY (key, item)) WITHOUT ROWID")
        self.by_key: dict[object, uuid.UUID] = {}

    def _encode(self, item: Item) -> object:
        return item.key if self.item_encoding == "int64" else item.item_id.bytes

    def insert(self, items: list[Item], postings: list[tuple[int, ...]]) -> int:
        entry_count = 0
        rows: list[tuple[int, object]] = []
        with self.connection:
            for item, keys in zip(items, postings):
                encoded = self._encode(item)
                self.by_key[encoded] = item.item_id
                rows.extend((key, encoded) for key in keys)
                if len(rows) >= 65_536:
                    self.connection.executemany("INSERT OR IGNORE INTO postings VALUES (?, ?)", rows)
                    entry_count += len(rows)
                    rows.clear()
            if rows:
                self.connection.executemany("INSERT OR IGNORE INTO postings VALUES (?, ?)", rows)
                entry_count += len(rows)
        return entry_count

    def build(self, items: list[Item]) -> tuple[float, int]:
        start = time.perf_counter()
        entries = self.insert(items, [self.indexer.postings(item.features) for item in items])
        return time.perf_counter() - start, entries

    def compact(self) -> None:
        self.connection.execute("VACUUM")

    def bucket_stats(self) -> dict[str, float]:
        sizes = sorted(row[0] for row in self.connection.execute("SELECT count(*) FROM postings GROUP BY key"))
        if not sizes:
            return {"distinct_keys": 0, "mean": 0.0, "p99": 0.0, "max": 0}
        return {"distinct_keys": len(sizes), "mean": round(statistics.fmean(sizes), 3),
                "p99": sizes[max(0, math.ceil(0.99 * len(sizes)) - 1)], "max": sizes[-1]}

    def lookup(self, query: Query, query_features: frozenset[int] | None = None,
               own: uuid.UUID | None = None) -> tuple[list[uuid.UUID], dict[str, int]]:
        keys = self.indexer.probes(features(query.text) if query_features is None else query_features)
        counts: Counter[object] = Counter()
        uncapped = truncated = 0
        for key in keys:
            size = self.connection.execute("SELECT count(*) FROM postings WHERE key = ?", (key,)).fetchone()[0]
            uncapped += size
            if size > MAX_BUCKET_ROWS:
                truncated += 1
            rows = self.connection.execute("SELECT item FROM postings WHERE key = ? ORDER BY item LIMIT ?",
                                           (key, MAX_BUCKET_ROWS)).fetchall()
            counts.update(row[0] for row in rows)
        own = own if own is not None else query.own_id
        ranked = sorted(((item, hits) for item, hits in counts.items() if self.by_key[item] != own),
                        key=lambda entry: (-entry[1], entry[0]))[:MAX_CANDIDATES]
        return ([self.by_key[item] for item, _hits in ranked],
                {"probes": len(keys), "rows_uncapped": uncapped, "truncated_probes": truncated})

    def close(self) -> int:
        self.connection.close()
        size = self.path.stat().st_size
        self.path.unlink(missing_ok=True)
        return size


# ---------------------------------------------------------------- benchmark


def _p95(values: list[float]) -> float:
    return sorted(values)[max(0, math.ceil(0.95 * len(values)) - 1)] if values else 0.0


def benchmark(indexer: CandidateIndex, items: list[Item], queries: list[Query], *,
              directory: str | None = None) -> dict[str, object]:
    signature_start = time.perf_counter()
    postings = [indexer.postings(item.features) for item in items]
    signature_seconds = time.perf_counter() - signature_start

    index = SQLitePostings(indexer, directory=directory)
    insert_start = time.perf_counter()
    entry_count = index.insert(items, postings)
    insert_seconds = time.perf_counter() - insert_start
    index.compact()
    uuid_index = SQLitePostings(indexer, item_encoding="uuid16", directory=directory)
    uuid_index.insert(items, postings)
    uuid_index.compact()
    uuid_bytes = uuid_index.close()
    buckets = index.bucket_stats()

    item_map = {item.item_id: item for item in items}
    facts = {item.item_id: fact_signature(item.text) for item in items}
    per_kind: dict[str, Counter[str]] = {}
    lookup_ms: list[float] = []
    verify_ms: list[float] = []
    candidate_counts: list[int] = []
    novel_rows_uncapped: list[int] = []
    total_candidates = verified_near = irrelevant = false_near = truncated_probes = total_probes = 0
    sweep_positive: list[tuple[float, bool, bool]] = []
    sweep_fact: list[tuple[float, bool, bool]] = []
    sweep_other_near: list[float] = []  # best cross-group similarity with equal facts, per query

    for query in queries:
        query_features = features(query.text)
        query_facts = fact_signature(query.text)
        start = time.perf_counter()
        candidates, stats = index.lookup(query, query_features)
        lookup_ms.append((time.perf_counter() - start) * 1000)
        candidate_counts.append(len(candidates))
        total_probes += stats["probes"]
        truncated_probes += stats["truncated_probes"]
        if query.kind == "novel":
            novel_rows_uncapped.append(stats["rows_uncapped"])

        target_group = item_map[query.target_id].group if query.target_id else None
        positive = query.kind.startswith("near_")
        verify_start = time.perf_counter()
        decisions = {c: classify(query_features, item_map[c].features, query_facts, facts[c]) for c in candidates}
        verify_ms.append((time.perf_counter() - verify_start) * 1000)
        best_other = 0.0
        for candidate, (classification, score) in decisions.items():
            total_candidates += 1
            if classification == "NEAR_DUPLICATE":
                verified_near += 1
                if not (positive and item_map[candidate].group == target_group):
                    false_near += 1
            elif score < RELATED_THRESHOLD:
                irrelevant += 1
            if item_map[candidate].group != target_group and query_facts == facts[candidate]:
                best_other = max(best_other, score)
        sweep_other_near.append(best_other)

        counter = per_kind.setdefault(query.kind, Counter())
        counter["queries"] += 1
        if query.target_id is not None:
            hit = query.target_id in decisions
            counter["candidate_hit"] += hit
            classification, score = (decisions[query.target_id] if hit else
                                     classify(query_features, item_map[query.target_id].features,
                                              query_facts, facts[query.target_id]))
            counter[f"target_{classification}"] += 1
            if hit and classification == "NEAR_DUPLICATE":
                counter["end_to_end_near"] += 1
            same_facts = query_facts == facts[query.target_id]
            (sweep_positive if positive else sweep_fact).append((score, same_facts, hit))
        counter["false_near"] += sum(1 for c, (cls, _s) in decisions.items() if cls == "NEAR_DUPLICATE"
                                     and not (positive and item_map[c].group == target_group))

    sqlite_bytes = index.close()
    positives = [k for k in per_kind if k.startswith("near_")]
    fact_kinds = [k for k in per_kind if k.startswith("fact_")]
    positive_queries = sum(per_kind[k]["queries"] for k in positives)

    def rate(kinds: list[str], field: str) -> float:
        total = sum(per_kind[k]["queries"] for k in kinds)
        return round(sum(per_kind[k][field] for k in kinds) / total, 6) if total else 0.0

    sweep = []
    for threshold in VERIFY_THRESHOLDS:
        sweep.append({
            "near_threshold": threshold,
            "end_to_end_near_recall": round(sum(hit and same and score >= threshold
                                                for score, same, hit in sweep_positive) / len(sweep_positive), 6)
            if sweep_positive else 0.0,
            "fact_mismatch_near_without_guard": sum(hit and score >= threshold for score, _same, hit in sweep_fact),
            "fact_mismatch_near_with_guard": sum(hit and same and score >= threshold
                                                 for score, same, hit in sweep_fact),
            "cross_group_near_with_guard": sum(score >= threshold for score in sweep_other_near),
        })
    build_seconds = signature_seconds + insert_seconds
    return {
        "approach": indexer.name,
        "corpus_chunks": len(items),
        "measured": {
            "postings_per_chunk_avg": round(entry_count / len(items), 3),
            "signature_bytes_per_chunk_in_memory": indexer.signature_bytes,
            "sqlite_bytes_per_chunk_int64_item": round(sqlite_bytes / len(items), 2),
            "sqlite_bytes_per_chunk_uuid16_item": round(uuid_bytes / len(items), 2),
            "sqlite_bytes_per_posting_int64_item": round(sqlite_bytes / max(1, entry_count), 2),
            "bucket_occupancy": buckets,
            "build_chunks_per_second_single_process": round(len(items) / build_seconds, 1),
            "signature_share_of_build": round(signature_seconds / build_seconds, 3),
            "queries": len(queries),
            "candidate_recall_near": rate(positives, "candidate_hit"),
            "end_to_end_near_recall": rate(positives, "end_to_end_near"),
            "near_positive_queries": positive_queries,
            "false_near_decisions": false_near,
            "false_candidate_rate": round(1 - verified_near / total_candidates, 6) if total_candidates else 0.0,
            "irrelevant_candidate_rate_below_related": round(irrelevant / total_candidates, 6)
            if total_candidates else 0.0,
            "candidates_per_query_avg": round(statistics.fmean(candidate_counts), 3),
            "candidates_per_query_p95": _p95([float(v) for v in candidate_counts]),
            "candidates_per_query_max": max(candidate_counts, default=0),
            "truncated_probe_rate": round(truncated_probes / total_probes, 6) if total_probes else 0.0,
            "novel_query_bucket_rows_uncapped_avg": round(statistics.fmean(novel_rows_uncapped), 3)
            if novel_rows_uncapped else 0.0,
            "lookup_ms_avg": round(statistics.fmean(lookup_ms), 4),
            "lookup_ms_p95": round(_p95(lookup_ms), 4),
            "verify_ms_avg": round(statistics.fmean(verify_ms), 4),
            "verify_ms_p95": round(_p95(verify_ms), 4),
            "fact_mismatch_queries": sum(per_kind[k]["queries"] for k in fact_kinds),
            "fact_mismatch_candidate_hit": rate(fact_kinds, "candidate_hit"),
            "fact_mismatch_false_near": sum(per_kind[k]["false_near"] for k in fact_kinds),
            "by_kind": {k: dict(sorted(v.items())) for k, v in sorted(per_kind.items())},
            "threshold_sweep": sweep,
        },
    }


def project(runs: list[dict[str, object]]) -> dict[str, object]:
    """EXTRAPOLATED scale figures, from the largest measured run of one approach, plus the measured growth slope."""
    runs = sorted(runs, key=lambda r: r["corpus_chunks"])
    largest = runs[-1]
    m = largest["measured"]
    n = largest["corpus_chunks"]
    per_chunk_load = m["novel_query_bucket_rows_uncapped_avg"] / n
    growth = [{"corpus_chunks": r["corpus_chunks"],
               "novel_rows_uncapped_per_query": r["measured"]["novel_query_bucket_rows_uncapped_avg"],
               "rows_per_query_per_indexed_chunk": round(r["measured"]["novel_query_bucket_rows_uncapped_avg"]
                                                         / r["corpus_chunks"], 9),
               "bucket_mean": r["measured"]["bucket_occupancy"]["mean"],
               "bucket_max": r["measured"]["bucket_occupancy"]["max"]} for r in runs]
    rows = []
    for count in SCALE_POINTS:
        rows.append({
            "chunks": count,
            "postings": round(m["postings_per_chunk_avg"] * count),
            "index_gib_int64_item": round(m["sqlite_bytes_per_chunk_int64_item"] * count / 1024 ** 3, 1),
            "index_gib_uuid16_item": round(m["sqlite_bytes_per_chunk_uuid16_item"] * count / 1024 ** 3, 1),
            "rebuild_hours_1_process": round(count / m["build_chunks_per_second_single_process"] / 3600, 1),
            "rebuild_hours_24_processes_ideal": round(count / m["build_chunks_per_second_single_process"]
                                                      / 3600 / 24, 1),
            "unrelated_bucket_rows_per_query": round(per_chunk_load * count, 1),
        })
    return {"basis": f"EXTRAPOLATED linearly from the measured {n}-chunk run; not measured at these sizes",
            "measured_growth": growth, "projections": rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pilot-url-env", default="KB_INTEGRATION_DATABASE_URL",
                        help="environment variable with a password-free kb_pipeline_test runtime URL")
    parser.add_argument("--no-pilot", action="store_true", help="skip the read-only pilot rows (offline run)")
    parser.add_argument("--template-chunks", type=int, default=512)
    parser.add_argument("--background-sizes", default="5000,20000,50000")
    parser.add_argument("--queries-per-kind", type=int, default=300)
    parser.add_argument("--approaches", default="", help="comma-separated subset of approach names")
    parser.add_argument("--temp-dir", help="directory for the temporary SQLite indexes (deleted after use)")
    parser.add_argument("--output", type=Path, help="optional path for aggregate JSON results (no pilot text)")
    args = parser.parse_args(argv)
    sizes = [int(v) for v in args.background_sizes.split(",") if v]
    if not sizes or min(sizes) < 100 or max(sizes) > 500_000:
        parser.error("--background-sizes values must be between 100 and 500000")
    pilot_rows: list[tuple[uuid.UUID, str]] = []
    if not args.no_pilot:
        pilot_url = os.environ.get(args.pilot_url_env, "").strip()
        if not pilot_url:
            parser.error(f"{args.pilot_url_env} is required (or pass --no-pilot)")
        pilot_rows = load_pilot(pilot_url)
    indexers = default_indexers()
    if args.approaches:
        wanted = set(args.approaches.split(","))
        indexers = [i for i in indexers if i.name in wanted]

    runs: dict[str, list[dict[str, object]]] = {i.name: [] for i in indexers}
    query_counts: dict[int, dict[str, int]] = {}
    for size in sizes:
        items, queries, counts = make_corpus(pilot_rows, args.template_chunks, size, args.queries_per_kind)
        query_counts[len(items)] = counts
        for indexer in indexers:
            result = benchmark(indexer, items, queries, directory=args.temp_dir)
            runs[indexer.name].append(result)
            m = result["measured"]
            print(f"[{len(items):>7} chunks] {indexer.name:<28} recall={m['end_to_end_near_recall']:.4f} "
                  f"cand={m['candidates_per_query_avg']:.1f} false_near={m['false_near_decisions']} "
                  f"B/chunk={m['sqlite_bytes_per_chunk_int64_item']} build/s="
                  f"{m['build_chunks_per_second_single_process']} lookup_ms={m['lookup_ms_avg']}", flush=True)
    output = {
        "database_source": REQUIRED_DATABASE if pilot_rows else None,
        "pilot_chunks_loaded": len(pilot_rows),
        "template_chunks": args.template_chunks,
        "background_sizes": sizes,
        "query_categories_by_corpus": query_counts,
        "limits": {"bucket_rows": MAX_BUCKET_ROWS, "candidates": MAX_CANDIDATES,
                   "near_threshold": NEAR_THRESHOLD, "related_threshold": RELATED_THRESHOLD},
        "measurement_note": "All 'measured' values come from these corpora on this machine. 'projections' are "
                            "linear extrapolations, not measurements at 1M+ chunks.",
        "approaches": {name: {"runs": results, "scale": project(results)} for name, results in runs.items()},
    }
    rendered = json.dumps(output, indent=2, sort_keys=True, default=str)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
