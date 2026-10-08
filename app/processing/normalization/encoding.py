"""Mojibake repair (ENCODING_REPAIR_VERSION ``enc1``).

Text whose UTF-8 bytes were decoded as Windows-1252 / Latin-1 (a page that
declares the wrong charset, or text re-encoded twice) looks like ``Ã©`` for
``é``, ``â€™`` for ``’`` or ``Â `` for a no-break space. ``repair`` re-encodes
exactly those runs back to bytes and decodes them as UTF-8, only when the
result is valid UTF-8 and shorter, so correct text is never touched. Up to two
rounds handle double encoding. ``corruption_count`` counts the runs that
remain, so Silver can refuse a document that is still corrupt.
"""

from __future__ import annotations

import re

ENCODING_REPAIR_VERSION = "enc1"
MAX_ROUNDS = 2
MAX_REMAINING = 2

# A UTF-8 lead byte read as a Latin-1 letter (U+00C2-U+00F4) followed by 1-3 continuation bytes read as
# Latin-1 controls / symbols (U+0080-U+00BF) or as the Windows-1252 glyphs of bytes 0x80-0x9F.
_CP1252_EXTRA = "ŒœŠšŸŽžƒˆ˜–—‘’‚“”„" \
                "†‡•…‰‹›€™"
_RUN = re.compile(rf"[Â-ô][\u0080-¿{_CP1252_EXTRA}]{{1,3}}")
_UNDEFINED_CP1252 = {0x81, 0x8D, 0x8F, 0x90, 0x9D}


def _to_bytes(run: str) -> bytes | None:
    out = bytearray()
    for ch in run:
        try:
            out += ch.encode("cp1252")
        except UnicodeEncodeError:
            if ord(ch) in _UNDEFINED_CP1252 or 0x80 <= ord(ch) <= 0xFF:
                out.append(ord(ch))
            else:
                return None
    return bytes(out)


def _fix_run(match: re.Match[str]) -> str:
    run = match.group(0)
    raw = _to_bytes(run)
    if raw is not None:
        # the match may include trailing characters that belong to the next word: try shorter prefixes too
        for end in range(len(raw), 1, -1):
            try:
                fixed = raw[:end].decode("utf-8")
            except UnicodeDecodeError:
                continue
            if len(fixed) < len(run[:end]):
                return fixed + run[end:]
    return run


def repair(text: str) -> str:
    """Return ``text`` with mojibake runs repaired (identity for text that has none)."""
    for _ in range(MAX_ROUNDS):
        fixed = _RUN.sub(_fix_run, text)
        if fixed == text:
            break
        text = fixed
    return text


def corruption_count(text: str) -> int:
    """Mojibake-looking runs still present after ``repair`` (and U+FFFD replacement characters)."""
    return len(_RUN.findall(text)) + text.count("�")
