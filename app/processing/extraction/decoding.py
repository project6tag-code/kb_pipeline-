"""Gates 03-04: binary/MIME sniffing and safe decoding (DECODER_VERSION).

Decoding order (first strict success wins; nothing is guessed beyond it):

1. a byte-order mark (UTF-8/16/32) is authoritative;
2. the declared charset (HTTP ``Content-Type``), then an HTML ``<meta charset>``
   in the first 4 KiB, when it names an allow-listed web text encoding
   (``latin-1``/``ascii`` are read as ``windows-1252``, as browsers do);
3. UTF-8.

If no candidate decodes strictly, the first candidate is decoded with
replacement characters and accepted only when the replacement ratio is at
most ``max_replacement_ratio``; otherwise the payload is rejected
(``undecodable``). Unsafe or unknown declared charsets (``utf-7``, transform
codecs) are rejected, never guessed around. C0/C1 control characters beyond
``max_control_ratio`` reject the text as corrupt; U+0000 is left for the NUL
gate (12), which has its own outcome.
"""

from __future__ import annotations

import codecs
import re
from dataclasses import dataclass

SNIFF_BYTES = 8192

_SIGNATURES: tuple[tuple[bytes, str], ...] = (
    (b"%PDF-", "pdf"), (b"\x89PNG\r\n\x1a\n", "png"), (b"GIF87a", "gif"), (b"GIF89a", "gif"),
    (b"\xff\xd8\xff", "jpeg"), (b"PK\x03\x04", "zip"), (b"PK\x05\x06", "zip"), (b"\x1f\x8b", "gzip"),
    (b"BZh", "bzip2"), (b"\xfd7zXZ\x00", "xz"), (b"7z\xbc\xaf\x27\x1c", "7z"), (b"Rar!\x1a\x07", "rar"),
    (b"\x7fELF", "elf"), (b"\xca\xfe\xba\xbe", "java_class"), (b"SQLite format 3\x00", "sqlite"),
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "ole"), (b"wOFF", "woff"), (b"wOF2", "woff2"), (b"OggS", "ogg"),
    (b"ID3", "mp3"), (b"fLaC", "flac"), (b"\x00asm", "wasm"), (b"\x28\xb5\x2f\xfd", "zstd"),
    (b"MSCF", "cab"), (b"%!PS", "postscript"), (b"\x00\x00\x01\x00", "ico"),
)
_BOMS: tuple[tuple[bytes, str], ...] = (
    (b"\xef\xbb\xbf", "utf-8"), (b"\xff\xfe\x00\x00", "utf-32-le"), (b"\x00\x00\xfe\xff", "utf-32-be"),
    (b"\xff\xfe", "utf-16-le"), (b"\xfe\xff", "utf-16-be"),
)
# Allow-listed text encodings (Python codec names after codecs.lookup().name).
_ALLOWED = frozenset({
    "utf-8", "utf-16", "utf-16-le", "utf-16-be", "utf-32", "utf-32-le", "utf-32-be", "ascii",
    "cp1250", "cp1251", "cp1252", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257", "cp1258",
    "iso8859-1", "iso8859-2", "iso8859-3", "iso8859-4", "iso8859-5", "iso8859-6", "iso8859-7", "iso8859-8",
    "iso8859-9", "iso8859-10", "iso8859-13", "iso8859-14", "iso8859-15", "iso8859-16", "koi8-r", "koi8-u",
    "mac-roman", "shift_jis", "euc_jp", "iso2022_jp", "gb2312", "gbk", "gb18030", "big5", "big5hkscs",
    "euc_kr", "cp949", "tis-620", "cp874", "cp932", "cp936", "cp950",
})
_WINDOWS_1252_ALIASES = frozenset({"ascii", "iso8859-1"})
_CHARSET_PARAM = re.compile(r"charset\s*=\s*[\"']?([A-Za-z0-9._:-]{1,40})", re.I)
_META_CHARSET = re.compile(rb"<meta[^>]{0,512}?charset\s*=\s*[\"']?([A-Za-z0-9._:-]{1,40})", re.I)
_CONTROL = re.compile("[\x01-\x08\x0b\x0e-\x1f\x7f-\x9f]")


class DecodeError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class DecodedText:
    text: str
    encoding: str
    source: str            # 'bom' | 'declared' | 'meta' | 'default' | 'lossy'
    replacements: int


def sniff_binary(data: bytes) -> str | None:
    """A reason if ``data`` is binary content (magic number, NUL/control bytes), else None."""
    head = data[:SNIFF_BYTES]
    for signature, name in _SIGNATURES:
        if head.startswith(signature):
            return f"binary_{name}"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return "binary_iso_media"
    if any(head.startswith(bom) for bom, enc in _BOMS if enc.startswith(("utf-16", "utf-32"))):
        return None  # NUL bytes are expected in UTF-16/32
    if b"\x00" in head:
        return "binary_nul_bytes"
    controls = sum(1 for b in head if b < 9 or 13 < b < 32 and b != 27)
    if head and controls / len(head) > 0.01:
        return "binary_control_bytes"
    return None


def normalize_charset(name: str | None) -> str | None:
    """The allow-listed codec name for ``name``; None when absent. Raises DecodeError when unsafe/unknown."""
    if not name:
        return None
    try:
        codec = codecs.lookup(name.strip().lower()).name
    except LookupError:
        raise DecodeError("unknown_charset", "declared charset is unknown") from None
    if codec not in _ALLOWED:
        raise DecodeError("unsupported_charset", f"declared charset {codec!r} is not an allowed text encoding")
    return "cp1252" if codec in _WINDOWS_1252_ALIASES else codec


def declared_charset(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = _CHARSET_PARAM.search(content_type)
    return match.group(1) if match else None


def meta_charset(data: bytes) -> str | None:
    match = _META_CHARSET.search(data[:4096])
    return match.group(1).decode("ascii") if match else None


def decode_bytes(data: bytes, *, declared: str | None = None, html: bool = False,
                 max_replacement_ratio: float = 0.001, max_control_ratio: float = 0.001) -> DecodedText:
    """Decode ``data`` per the module rules. Raises DecodeError."""
    for bom, encoding in _BOMS:
        if data.startswith(bom):
            try:
                text = data[len(bom):].decode(encoding, errors="strict")
            except UnicodeDecodeError:
                raise DecodeError("bom_mismatch", f"content is not valid {encoding} despite its BOM") from None
            return _validated(DecodedText(text, encoding, "bom", 0), max_control_ratio)
    candidates: list[tuple[str, str]] = []
    for source, raw in (("declared", declared), ("meta", meta_charset(data) if html else None)):
        codec = normalize_charset(raw)
        if codec is not None and codec not in (c for c, _ in candidates):
            candidates.append((codec, source))
    if "utf-8" not in (c for c, _ in candidates):
        candidates.append(("utf-8", "default"))
    for codec, source in candidates:
        try:
            return _validated(DecodedText(data.decode(codec, errors="strict"), codec, source, 0), max_control_ratio)
        except UnicodeDecodeError:
            continue
    codec = candidates[0][0]
    text = data.decode(codec, errors="replace")
    replacements = text.count("�") - (data.count(b"\xef\xbf\xbd") if codec == "utf-8" else 0)
    if replacements > max(1, len(text)) * max_replacement_ratio:
        raise DecodeError("undecodable", f"{replacements} undecodable byte sequences under {codec}")
    return _validated(DecodedText(text, codec, "lossy", max(replacements, 0)), max_control_ratio)


def _validated(decoded: DecodedText, max_control_ratio: float) -> DecodedText:
    text = decoded.text
    if any(0xD800 <= ord(ch) <= 0xDFFF for ch in text):  # pragma: no cover - strict codecs never produce these
        raise DecodeError("surrogates", "decoded text contains surrogate code points")
    controls = len(_CONTROL.findall(text))
    if controls > max(1, len(text)) * max_control_ratio:
        raise DecodeError("control_chars", f"{controls} control characters (corrupt or binary text)")
    return decoded
