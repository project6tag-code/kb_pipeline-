"""Canonical text normalization (V1).

``canonicalize_text`` produces the canonical form from which document and chunk
identities are derived. It is intentionally conservative: case, punctuation,
accents, and word order carry factual meaning and are never altered.

Changing this algorithm changes content hashes and therefore every
deterministic ID. Any change must be treated as a new version (V2) and a
breaking migration, not as a bug fix.
"""

from __future__ import annotations

import re
import unicodedata

CANONICALIZATION_VERSION = "v1"

# Leading run of whitespace and/or U+FEFF at the very start of the input.
# ``\s`` on str patterns matches the same characters as ``str.isspace()``.
_LEADING_BOM_AND_WS_RE = re.compile(r"\A[\s﻿]+")
_NBSP = " "
_HORIZONTAL_WS_RUN_RE = re.compile(r"[ \t]+")
_EXCESS_NEWLINES_RE = re.compile(r"\n{3,}")


def canonicalize_text(text: str) -> str:
    """Return the V1 canonical form of ``text``.

    Steps, applied in order:

    1. Require ``str`` input.
    2. Remove a leading input BOM: drop the entire leading run of whitespace
       and U+FEFF characters. Whitespace is consumed together with the BOM so
       that ``"  \\ufeffx"`` canonicalizes to ``"x"`` in one pass (step 8 would
       strip that whitespace anyway, so no other output changes). U+FEFF
       anywhere after the first non-whitespace, non-BOM character is content
       and is preserved.
    3. Unicode NFC normalization.
    4. Convert CRLF and lone CR to LF.
    5. Replace U+00A0 (no-break space) with U+0020.
    6. Per line: collapse runs of spaces/tabs to a single space, then remove
       trailing whitespace (``str.rstrip()``).
    7. Collapse 3+ consecutive newlines to exactly two (at most one blank line).
    8. Strip leading/trailing whitespace from the whole text.

    The function is deterministic and idempotent:
    ``canonicalize_text(canonicalize_text(x)) == canonicalize_text(x)``.

    Raises:
        TypeError: if ``text`` is not a ``str``.
    """
    if not isinstance(text, str):
        raise TypeError(f"canonicalize_text() expects str, got {type(text).__name__}")

    text = _LEADING_BOM_AND_WS_RE.sub("", text)
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace(_NBSP, " ")
    text = "\n".join(
        _HORIZONTAL_WS_RUN_RE.sub(" ", line).rstrip() for line in text.split("\n")
    )
    text = _EXCESS_NEWLINES_RE.sub("\n\n", text)
    return text.strip()
