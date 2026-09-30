"""Fact-preservation guard for chunk near-duplicate decisions (``fg-v1``).

Two chunks may be NEAR_DUPLICATE only when their fact signatures are EQUAL.
A signature is four sorted multisets extracted from the chunk body:

* **quantities**: digit numbers, with currency, a normalized unit and ordinal
  suffix (``$10 million``, ``900 megawatt``, ``25 %``, ``ord:3``); number
  words (``ten`` -> ``10``, ``twenty-five`` -> ``25``, ``ten million`` ->
  ``10 million``); ordinal words (``third`` -> ``ord:3``); month names;
* **entities**: capitalized runs, acronyms and CamelCase names. A capital at
  a sentence start is grammar, not a name, and is ignored;
* **negations**: ``not``, ``no``, ``never``, ``n't`` and similar;
* **directions**: directional and comparative words mapped to an axis and a
  sign (``increased``/``rose`` -> ``change+``; ``lower``/``less`` -> ``level-``;
  ``before`` -> ``time-``). ``rose`` vs ``increased`` stays compatible;
  ``increased`` vs ``decreased`` does not.

Heading lines (``#``) are context, not content, and are ignored. Markdown
emphasis, list markers and horizontal whitespace are neutralized, so CC prose
and its GitHub markdown rendering get the same signature.

The guard is deliberately conservative. Any difference, including a
harmless-looking one such as ``km`` vs ``kilometres`` or a dropped
``first``, keeps the pair RELATED. It is a lexical guard: it does not
understand paraphrase, and it is never used to MERGE, only to prevent merging.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import re
from dataclasses import dataclass

FACT_GUARD_VERSION = "fg-v1"

_WORD = re.compile(r"\w+", re.UNICODE)
_HSPACE = re.compile(r"[^\S\n]+")
_SENTENCE_BREAK = re.compile(r"(?<=[.!?;:])\s+|\n+")
_LIST_MARKER = re.compile(r"^\s*(?:[-*+>]|\d{1,3}[.)])\s+")
_EMPHASIS = re.compile(r"(?<!\w)[*_`]+|[*_`]+(?!\w)")
_MARKUP = " \t*_`#>|-([{\"'“‘"

UNITS: frozenset[str] = frozenset("""
    % percent million billion trillion thousand hundred dozen
    mm cm m km kilometre kilometres kilometer kilometers metre metres meter meters mile miles ft feet foot
    inch inches mg g kg gram grams kilogram kilograms tonne tonnes ton tons lb lbs pound pounds ounce ounces
    ml l litre litres liter liters gallon gallons
    w kw mw gw watt watts kilowatt kilowatts megawatt megawatts gigawatt gigawatts kwh mwh gwh volt volts v
    hz khz mhz ghz b kb mb gb tb pb byte bytes bit bits
    s ms sec second seconds minute minutes min hour hours h day days week weeks month months year years
    decade decades century centuries °c °f c f k celsius fahrenheit kelvin degree degrees
    usd eur gbp dollar dollars euro euros cent cents people persons""".split())
MONTHS: frozenset[str] = frozenset("january february march april may june july august september october "
                                   "november december jan feb mar apr jun jul aug sep sept oct nov dec".split())
_ORDINAL_SUFFIXES = frozenset({"st", "nd", "rd", "th"})
_NUMBER = re.compile(r"(?<![\w.])([$£€¥]\s?)?(\d+(?:,\d{3})*(?:\.\d+)?)(?:(\s?)(%|°[cfCF]|[A-Za-z]+))?")

_ONES = {"zero": 0, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
         "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
         "seventeen": 17, "eighteen": 18, "nineteen": 19}
_DIGIT_WORDS = {"one": 1, **{k: v for k, v in _ONES.items() if 2 <= v <= 9}}
_TENS = {"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80,
         "ninety": 90}
_SCALES = {"hundred": 100}
_ORDINAL_WORDS = {"first": 1, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6, "seventh": 7, "eighth": 8,
                  "ninth": 9, "tenth": 10, "eleventh": 11, "twelfth": 12}  # "second" is too ambiguous (time unit)

_ENTITY_WORD = r"[A-Z][\w'’&-]*(?:\.[\w'’&-]+)*"  # internal dots only (U.S), never a sentence-final one
_ENTITY_RUN = re.compile(rf"(?<![\w'’]){_ENTITY_WORD}(?:\s+(?:of\s+|de\s+|the\s+)?{_ENTITY_WORD})*")
_SENTENCE_FUNCTION = frozenset("""
    the a an this that these those it its in on at for by with from near during after before when while however
    our their his her we they there each every some many most all both as if but and or so also then thus
    although because since until unlike like unless according""".split())
_NEGATION = re.compile(r"\b(?:not|no|never|neither|nor|none|nobody|nothing|nowhere|without|cannot|\w+n['’]t)\b",
                       re.I)

DIRECTIONS: dict[str, str] = {}
for _axis, _words in {
    "change+": "increase increased increases increasing rise rises rose risen rising grow grows grew grown growing "
               "gain gains gained gaining climb climbs climbed climbing surge surged expand expanded expanding "
               "jump jumped",
    "change-": "decrease decreased decreases decreasing fall falls fell fallen falling decline declines declined "
               "declining drop drops dropped dropping shrink shrinks shrank shrunk shrinking reduce reduced reduces "
               "reducing lose loses lost losing loss losses plunge plunged contract contracted",
    "level+": "higher highest high above greater greatest larger largest bigger biggest more most maximum exceed "
              "exceeds exceeded exceeding",
    "level-": "lower lowest low below less least fewer fewest smaller smallest minimum",
    "time+": "after later afterwards",
    "time-": "before earlier prior previously",
    "quality+": "better best positive success successful win wins won",
    "quality-": "worse worst negative failure failed fail fails",
}.items():
    for _w in _words.split():
        assert _w not in DIRECTIONS, _w
        DIRECTIONS[_w] = _axis


def lexicon_sha256() -> str:
    """Digest of every lexicon the guard uses: part of the frozen method parameters."""
    payload = {"units": sorted(UNITS), "months": sorted(MONTHS), "ones": _ONES, "tens": _TENS,
               "ordinals": _ORDINAL_WORDS, "directions": DIRECTIONS, "sentence_function": sorted(_SENTENCE_FUNCTION),
               "negation": _NEGATION.pattern, "number": _NUMBER.pattern, "entity": _ENTITY_RUN.pattern,
               "sentence_break": _SENTENCE_BREAK.pattern, "signature": "per-sentence ordered, sentences sorted"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


KINDS = ("quantities", "entities", "negations", "directions")


@dataclass(frozen=True)
class FactSignature:
    """Per sentence, the ORDERED quantities, entities, negations and directions; the sentences are then sorted.

    Keeping the order inside a sentence catches role swaps that a bag of words
    misses: ``A acquired B`` vs ``B acquired A``, ``from 10 to 20`` vs ``from
    20 to 10``, ``lost more than it gained`` vs ``gained more than it lost``.
    Sorting the sentences keeps pure sentence reordering compatible. Keeping
    the sentence as the unit stops two different edits in two different
    sentences from cancelling out.
    """

    sentences: tuple[tuple[tuple[str, ...], ...], ...]

    def compatible(self, other: "FactSignature") -> bool:
        return self.sentences == other.sentences

    def _flat(self, kind: int) -> tuple[str, ...]:
        return tuple(sorted(token for sentence in self.sentences for token in sentence[kind]))

    @property
    def quantities(self) -> tuple[str, ...]:
        return self._flat(0)

    @property
    def entities(self) -> tuple[str, ...]:
        return self._flat(1)

    @property
    def negations(self) -> tuple[str, ...]:
        return self._flat(2)

    @property
    def directions(self) -> tuple[str, ...]:
        return self._flat(3)

    def differences(self, other: "FactSignature") -> list[str]:
        found = [name for name in KINDS if getattr(self, name) != getattr(other, name)]
        return found or (["order"] if not self.compatible(other) else [])


def _unit(raw: str) -> str | None:
    unit = raw.casefold()
    if unit in {"percent", "%"}:
        return "%"
    if unit not in UNITS:
        return None
    if len(unit) > 3 and unit.endswith("s") and unit[:-1] in UNITS:
        return unit[:-1]
    return unit


def _body(text: str) -> str:
    lines = [_LIST_MARKER.sub("", line) for line in text.split("\n") if not line.lstrip().startswith("#")]
    return _HSPACE.sub(" ", _EMPHASIS.sub("", "\n".join(lines)))


def _digit_quantities(body: str) -> list[tuple[int, str]]:
    out = []
    for match in _NUMBER.finditer(body):
        currency, value, space, tail = (match.group(1) or "").strip(), match.group(2).replace(",", ""), \
            match.group(3), match.group(4) or ""
        if tail and not space and tail.casefold() in _ORDINAL_SUFFIXES and not currency:
            out.append((match.start(), f"ord:{value}"))
            continue
        unit = _unit(tail) if tail else None
        out.append((match.start(), f"{currency}{value}{' ' + unit if unit else ''}"))
    return out


def _word_quantities(body: str) -> list[tuple[int, str]]:
    tokens = [(m.group(0).casefold(), m.start(), m.end()) for m in _WORD.finditer(body)]
    out: list[tuple[int, str]] = []
    i = 0
    while i < len(tokens):
        word = tokens[i][0]
        if word in _ORDINAL_WORDS:
            out.append((tokens[i][1], f"ord:{_ORDINAL_WORDS[word]}"))
            i += 1
            continue
        value: int | None = None
        consumed = 1
        if word in _TENS:
            value = _TENS[word]
            if i + 1 < len(tokens) and tokens[i + 1][0] in _DIGIT_WORDS and _joined(body, tokens[i], tokens[i + 1]):
                value += _DIGIT_WORDS[tokens[i + 1][0]]
                consumed = 2
        elif word in _ONES or (word == "one" and i + 1 < len(tokens) and tokens[i + 1][0] in _SCALES):
            value = _ONES.get(word, 1)
        if value is None:
            i += 1
            continue
        j = i + consumed
        if j < len(tokens) and tokens[j][0] in _SCALES:
            value *= _SCALES[tokens[j][0]]
            j += 1
        unit = _unit(tokens[j][0]) if j < len(tokens) else None
        out.append((tokens[i][1], f"{value}{' ' + unit if unit else ''}"))
        i = j
    return out


def _joined(body: str, left: tuple[str, int, int], right: tuple[str, int, int]) -> bool:
    return body[left[2]:right[1]] in {"-", " "}


def _entities(body: str) -> list[tuple[int, str]]:
    out = []
    for match in _ENTITY_RUN.finditer(body):
        words = match.group(0).split()
        prefix = body[:match.start()].rstrip(_MARKUP)
        if not prefix or prefix[-1] in ".!?:;\n":  # sentence start: the first capital is grammar
            if words[0].casefold() in _SENTENCE_FUNCTION or len(words) == 1:
                words = words[1:]
        words = [w.strip(".") for w in words if w.strip(".")]
        if words and not all(w.casefold() in MONTHS or w.casefold() in _ONES or w.casefold() in _TENS
                             for w in words):
            out.append((match.start(), " ".join(w.casefold() for w in words)))
    return out


def fact_signature(text: str) -> FactSignature:
    body = _body(text)
    words = list(_WORD.finditer(body))
    quantities = _digit_quantities(body) + _word_quantities(body)
    quantities += [(m.start(), f"month:{m.group(0).casefold()}") for m in words
                   if m.group(0).casefold() in MONTHS and m.group(0)[0].isupper()]
    negations = [(m.start(), m.group(0).casefold().replace("’", "'")) for m in _NEGATION.finditer(body)]
    directions = [(m.start(), DIRECTIONS[m.group(0).casefold()]) for m in words if m.group(0).casefold() in DIRECTIONS]
    starts = [0] + [m.end() for m in _SENTENCE_BREAK.finditer(body)]
    sentences: dict[int, list[list[tuple[int, str]]]] = {}
    for kind, found in enumerate((quantities, _entities(body), negations, directions)):
        for position, token in found:
            slot = bisect.bisect_right(starts, position) - 1
            sentences.setdefault(slot, [[], [], [], []])[kind].append((position, token))
    return FactSignature(tuple(sorted(tuple(tuple(t for _p, t in sorted(kind)) for kind in groups)
                                      for groups in sentences.values())))
