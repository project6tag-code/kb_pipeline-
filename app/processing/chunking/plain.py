"""Plain-text rendering of one chunk span (PLAIN_RENDER_VERSION ``plain2``).

Silver text keeps Markdown-like structure (``#`` headings, ``-``/``N.`` lists,
``a | b`` table rows, ``>`` quotes, fenced code, and inline Markdown in GitHub
documentation) because the chunker splits on it. The stored chunk text, which
is the chunk's identity, its embedding input and the Gold ``public.kb.chunk``,
is the span rendered as plain text:

| structure            | plain text                                                       |
|----------------------|------------------------------------------------------------------|
| ``## Heading``       | ``Heading`` (its own line)                                       |
| ``- item`` / ``* item`` | ``item`` (indentation dropped); ``3. item`` keeps its number |
| ``> quote``          | ``quote``                                                         |
| ``a | b`` row        | 2 cells: ``a: b``; more: ``a; b; c``; separator / empty rows dropped |
| code fence lines     | dropped; the code inside is kept verbatim                         |
| ``[text](url)``, ``![alt](url)`` | ``text`` / ``alt``                                    |
| ``**x**``, ``__x__``, ``*x*``, ``_x_``, `` `x` `` | ``x``                               |
| ``[5]``, ``[a]``, ``[citation needed]``, ``[edit]`` | removed (citation / editorial marks) |

Code and config documents are code; they are returned unchanged.
Invariant (replaces schema §5.15 for chunking ``c2``):
``chunk_text == canonicalize_text(render(text[char_start:char_end], kind))``.
"""

from __future__ import annotations

import re

PLAIN_RENDER_VERSION = "plain2"  # plain1 + citation brackets removed

_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^\s*#{1,6}\s+(.*?)\s*#*\s*$")
_QUOTE = re.compile(r"^\s*(?:>\s?)+")
_BULLET = re.compile(r"^\s*[-*+]\s+")
_NUMBERED = re.compile(r"^\s+(\d{1,9}[.)]\s+)")
_TABLE_SEPARATOR_CELL = re.compile(r"^:?-{1,}:?$")
_IMAGE = re.compile(r"!\[([^\]]*)\]\([^)]*\)")
_LINK = re.compile(r"\[([^\]]+)\]\((?:[^()\s]|\([^()\s]*\))*(?:\s+\"[^\"]*\")?\)")
_STRONG = re.compile(r"(\*\*|__)(\S(?:.*?\S)?)\1")
_EMPHASIS_STAR = re.compile(r"(?<![\w*])\*(\S(?:[^*\n]*?\S)?)\*(?![\w*])")
_EMPHASIS_UNDERSCORE = re.compile(r"(?<![\w_])_(\S(?:[^_\n]*?\S)?)_(?![\w_])")
_CODE_SPAN = re.compile(r"`([^`\n]+)`")
# Citation / editorial brackets of encyclopedias: "[5]", "[a]", "[note 3]", "[citation needed]", "[edit]".
# Removed only from the stored chunk text: Silver's suitability gate counts "[1]" as reference evidence.
# Case-sensitive, and never "[0]": an index such as "x[0]" or a label such as "[Edit]" is kept.
_CITATION = re.compile(r"\[(?:[1-9]\d{0,3}|[a-z]|note \d{1,3}|nb \d{1,3}|citation needed|clarification needed|edit"
                       r"|update|when\?|who\?|which\?|where\?|according to whom\?|by whom\?|dubious[^\]]{0,40}"
                       r"|failed verification|better source needed|page needed|verification needed"
                       r"|original research\??|unreliable source\??|not in citation given)\]")


def _inline(text: str) -> str:
    text = _CITATION.sub("", text)
    text = _IMAGE.sub(r"\1", text)
    text = _LINK.sub(r"\1", text)
    text = _CODE_SPAN.sub(r"\1", text)
    text = _STRONG.sub(r"\2", text)
    text = _EMPHASIS_STAR.sub(r"\1", text)
    return _EMPHASIS_UNDERSCORE.sub(r"\1", text)


def _table_row(line: str) -> str | None:
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    cells = [c for c in cells if c and not _TABLE_SEPARATOR_CELL.match(c)]
    if not cells:
        return None
    return f"{cells[0]}: {cells[1]}" if len(cells) == 2 else "; ".join(cells)


def render(span: str, kind: str = "prose") -> str:
    """Plain text of one Silver span (deterministic; the caller canonicalizes)."""
    if kind in ("code", "config"):
        return span
    out: list[str] = []
    in_fence = False
    for line in span.split("\n"):
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            out.append(line)
            continue
        heading = _HEADING.match(line)
        if heading:
            out.append(_inline(heading.group(1)))
            continue
        line = _QUOTE.sub("", line)
        line = _BULLET.sub("", line)
        line = _NUMBERED.sub(r"\1", line)
        if "|" in line and (line.lstrip().startswith("|") or " | " in line):
            row = _table_row(line)
            if row is None:
                continue
            line = row
        out.append(_inline(line).strip())
    return "\n".join(out)
