"""Structure of a canonical Silver document: typed blocks with exact code-point offsets.

The Silver text is the locked canonical form produced by the Silver extractors:
blocks separated by one blank line, headings as ``#``-lines, lists as
``-``/``N.`` lines, tables as ``a | b`` rows, preformatted/code as fenced
blocks (which may contain blank lines, so fences are tracked). Documents whose
Silver kind is ``code``/``config`` (GitHub source files) have no Markdown
structure: ``#`` there is a comment, so they become ``code`` blocks split at
blank lines.

Every block's ``[start, end)`` is a span of the canonical text itself, so a
chunk made of consecutive blocks is always a contiguous span (schema §5.15).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

HEADING = re.compile(r"^(#{1,6}) +(\S.*)$")
LIST_ITEM = re.compile(r"^ ?(?:[-*+•]|\d{1,9}[.)]) +\S")
FENCE = re.compile(r"^ ?(```|~~~)")
TOP_LEVEL_CODE = re.compile(
    r"^(?:def |async def |class |func |fn |pub |impl |struct |enum |interface |trait |module |function |"
    r"export |public |private |protected |static |package |import |from |#include|@|CREATE |SELECT |"
    r"[A-Za-z_][\w<>\[\], *&:]* [A-Za-z_]\w*\s*\()")

CODE_KINDS = frozenset({"code", "config"})


@dataclass(frozen=True)
class Block:
    kind: str                 # heading | paragraph | list | table | fence | code
    start: int
    end: int
    level: int = 0            # heading level (1-6)
    title: str = ""           # heading text

    def text(self, doc: str) -> str:
        return doc[self.start:self.end]


def _lines(doc: str) -> list[tuple[int, int]]:
    spans, start = [], 0
    for line in doc.split("\n"):
        spans.append((start, start + len(line)))
        start += len(line) + 1
    return spans


def _classify(doc: str, lines: list[tuple[int, int]]) -> str:
    texts = [doc[s:e] for s, e in lines]
    if len(texts) >= 2 and all(" | " in t or t.startswith("|") for t in texts):
        return "table"
    if LIST_ITEM.match(texts[0]):
        return "list"
    return "paragraph"


def parse_blocks(doc: str, kind: str = "prose") -> list[Block]:
    """Blocks in document order; headings are single-line blocks."""
    lines = _lines(doc)
    blocks: list[Block] = []
    group: list[tuple[int, int]] = []
    code = kind in CODE_KINDS

    def flush() -> None:
        if group:
            blocks.append(Block("code" if code else _classify(doc, group), group[0][0], group[-1][1]))
            group.clear()

    i = 0
    while i < len(lines):
        start, end = lines[i]
        line = doc[start:end]
        if not code and FENCE.match(line):
            flush()
            j = i + 1
            while j < len(lines) and not FENCE.match(doc[lines[j][0]:lines[j][1]]):
                j += 1
            last = min(j, len(lines) - 1)
            blocks.append(Block("fence", start, lines[last][1]))
            i = last + 1
            continue
        if not line.strip():
            flush()
        elif not code and (match := HEADING.match(line)):
            flush()
            blocks.append(Block("heading", start, end, len(match.group(1)), match.group(2).strip()))
        else:
            group.append((start, end))
        i += 1
    flush()
    return blocks
