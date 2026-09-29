"""Strict, bounded decoding of one gzip member (CDX index blocks and WARC records).

Common Crawl stores every WARC record, and every ~3000-line CDX index block,
as an independent gzip member addressed by (offset, length). A byte range must
therefore contain exactly one complete member: no truncation, no trailing
bytes. Output is capped to defend against decompression bombs.
"""

from __future__ import annotations

import zlib


class CompressionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def decompress_single_member(data: bytes, max_output: int) -> bytes:
    """Decompress exactly one gzip member of at most ``max_output`` bytes of output.

    Raises CompressionError with code ``not_gzip``, ``corrupt``, ``truncated``,
    ``trailing_data`` or ``decompression_limit``.
    """
    if max_output < 1:
        raise ValueError("max_output must be >= 1")
    if len(data) < 18 or data[:2] != b"\x1f\x8b":
        raise CompressionError("not_gzip", "data is not a gzip member")
    decoder = zlib.decompressobj(zlib.MAX_WBITS | 16)
    out: list[bytes] = []
    total = 0
    pending = data
    try:
        while True:
            chunk = decoder.decompress(pending, max_output - total + 1)
            pending = decoder.unconsumed_tail
            total += len(chunk)
            if total > max_output:
                raise CompressionError("decompression_limit",
                                       f"decompressed output exceeds the {max_output}-byte limit")
            out.append(chunk)
            if decoder.eof:
                break
            if not chunk and not pending:
                raise CompressionError("truncated", "gzip member is incomplete")
    except zlib.error as exc:
        raise CompressionError("corrupt", f"corrupt gzip data: {exc}") from exc
    if decoder.unused_data:
        raise CompressionError("trailing_data", "bytes follow the gzip member (range covers more than one record)")
    return b"".join(out)
