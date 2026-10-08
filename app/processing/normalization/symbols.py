"""Emoji and decorative-symbol cleanup of extracted text (SYMBOL_CLEANER_VERSION ``sym1``).

Runs in Silver on the extracted, structured text of prose / HTML / Markdown
documents, BEFORE the locked canonicalization (gate 11), so the document id is
derived from the cleaned text. Code and config files are not touched.

Removed (they carry no factual content):

| what                         | examples / Unicode                                                   |
|------------------------------|-----------------------------------------------------------------------|
| emoji and pictographs        | 😀 🔥 🚀 flags, skin tones (U+1F000-1FAFF), ☀-➿ symbols and dingbats    |
|                              | (✅ ✔ ★ ☎ ❤ ➤), ⭐ ⬆ (U+2B00-2BFF), ⌚ ⏰ ⏩, keycaps, variation selectors |
| decorative shapes            | ■ ● ► ◆ ▪ (U+25A0-25FF); ™ ®                                          |
| icon-font glyphs             | Private Use Area (U+E000-F8FF and planes 15-16)                       |
| invisible characters         | zero-width space/joiners, soft hyphen, word joiner, bidi controls,     |
|                              | interior U+FEFF                                                       |
| bullets                      | • ‣ ⁃ ∙ : a line-leading bullet becomes a ``- `` list item, an inline |
|                              | bullet a separator space                                              |
| decorative runs              | 3+ of ``= * _ + ^ \\ / -`` and 4+ dots (rules, form blanks ``____``)    |
| hashtag signs                | ``#topic`` -> ``topic`` (``C#``, ``#1``, ``page#anchor``, headings kept) |
| footnote debris              | ``^`` back-links, footnote ``*`` (``Album*``, a lone `` * ``), ``† ‡``;  |
|                              | ``2*3``, ``x^2`` and a line's ``* item`` list marker are kept           |
| symbol-only lines            | lines with no letter or digit (except code fences)                     |

Kept, because they carry meaning: ``$ € £ % # _ ° @ & + = / | · → ≤ ± ∑``
in context ("US $7.99", "50%", "C#", "#1", "snake_case", "25 °C", "N·m", "x → y"),
Markdown structure markers (``#`` headings, ``-``/``N.`` lists, ``|`` tables,
code fences: the chunker needs them and renders chunks as plain text), and all
text inside fenced code.
"""

from __future__ import annotations

import re

SYMBOL_CLEANER_VERSION = "sym1"

_PICTOGRAPHS = re.compile(
    "["
    "\U0001F000-\U0001FAFF"   # emoji, pictographs, flags (regional indicators), skin tones, mahjong/cards
    "☀-➿"           # miscellaneous symbols + dingbats
    "⬀-⯿"           # miscellaneous symbols and arrows (stars, heavy arrows)
    "⌚⌛⌨⏏⏩-⏳⏸-⏺"  # emoji in Miscellaneous Technical
    "■-◿"           # geometric shapes
    "™®"            # trademark / registered signs
    "-"           # private use (icon fonts)
    "\U000F0000-\U0010FFFD"   # supplementary private use
    "︀-️"           # variation selectors
    "⃣"                  # combining enclosing keycap
    "\U000E0000-\U000E007F"   # tag characters (emoji flag sequences)
    "]"
)
_INVISIBLE = re.compile("[­​-‏‪-‮⁠-⁤⁦-⁩﻿]")
_BULLETS = "•‣⁃∙"
_LEADING_BULLET = re.compile(rf"^(\s*)[{_BULLETS}]\s*")
_INLINE_BULLET = re.compile(rf"\s*[{_BULLETS}]\s*")
_DECORATIVE_RUN = re.compile(r"([=*_+^\\/\-])\1{2,}|\.{4,}")
# Footnote debris (Wikipedia back-links "^ a b", "Album*", "‡"): a caret before a space or at a line
# start, a standalone or word-final asterisk, and dagger marks. "2*3", "x^2" and "a * b" spacing in
# math are kept (an operator has a digit or letter on its right).
_FOOTNOTE = re.compile(r"(?:^|(?<=\s)|(?<=\d))\^(?=[\s|]|$)|(?<=\w)\*(?=[\s.,;:)|]|$)|(?<=\S )\*(?=\s|$)"
                       r"|\(\*\)|[†‡]")
_HASHTAG = re.compile(r"(?<![\w#&/])#(?=[A-Za-z][\w-]*)")
_HEADING = re.compile(r"^#{1,6} ")
_FENCE = re.compile(r"^\s*(```|~~~)")
_SPACES = re.compile(r"[ \t]{2,}")


def _clean_line(line: str) -> str | None:
    """The cleaned line, or None to drop it. Original indentation (nested lists) is kept, never added."""
    original = line
    indent = line[:len(line) - len(line.lstrip(" \t"))]
    line = _INVISIBLE.sub("", line)
    line = _PICTOGRAPHS.sub(" ", line)
    line = _LEADING_BULLET.sub(lambda m: m.group(1) + "- ", line)
    line = _INLINE_BULLET.sub(" ", line)
    line = _DECORATIVE_RUN.sub(" ", line)
    line = _FOOTNOTE.sub("", line)
    if _HEADING.match(line):
        line = line[:line.index(" ") + 1] + _HASHTAG.sub("", line[line.index(" ") + 1:])
    else:
        line = _HASHTAG.sub("", line)
    body = _SPACES.sub(" ", line).strip()
    if not body:
        return "" if not original.strip() else None  # keep real blank lines; drop lines emptied by cleanup
    if not any(ch.isalnum() for ch in body):
        return None  # a line of only symbols / punctuation
    return indent + body


def clean(text: str) -> str:
    """Remove emojis and decorative symbols outside fenced code; deterministic and idempotent."""
    out: list[str] = []
    in_fence = False
    for line in text.split("\n"):
        if _FENCE.match(line):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        cleaned = _clean_line(line)
        if cleaned is not None:
            out.append(cleaned)
    return "\n".join(out)
