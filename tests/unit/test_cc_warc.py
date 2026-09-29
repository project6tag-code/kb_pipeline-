"""WARC record validation, gzip-member handling, decompression limits and source metadata (offline)."""

import gzip
import os
import unittest
import uuid
from datetime import datetime, timezone

from app.sources.commoncrawl.compression import CompressionError, decompress_single_member
from app.sources.commoncrawl.index import parse_cdx_line
from app.sources.commoncrawl.warc import WarcRecordError, extract_canonical, extract_title, parse_warc_record

from tests.unit.cc_fixtures import CRAWL, html_page, make_cdx_line, make_warc_record, sha1_b32, warc_filename

URL = "https://example.com/articles/one"
LIMIT = 1024 * 1024


def expected_for(record: bytes, *, url: str = URL, digest: str | None = None, mime: str = "text/html",
                 status: str = "200"):
    return parse_cdx_line(make_cdx_line(url, warc_filename(), 0, len(record), digest=digest, mime_detected=mime,
                                        status=status, urlkey="com,example)/articles/one"), CRAWL)


def parse(record: bytes, **kwargs):
    expected = kwargs.pop("expected", None) or expected_for(record)
    return parse_warc_record(record, expected, max_decompressed=kwargs.pop("max_decompressed", LIMIT), **kwargs)


class CompressionTests(unittest.TestCase):
    def test_single_member(self) -> None:
        self.assertEqual(decompress_single_member(gzip.compress(b"abc" * 1000), 10_000), b"abc" * 1000)

    def test_rejections(self) -> None:
        member = gzip.compress(b"hello world" * 100)
        cases = {
            "not_gzip": b"not gzip data at all....",
            "truncated": member[:-12],
            "trailing_data": member + gzip.compress(b"second record"),
            "corrupt": member[:10] + bytes(b ^ 0xFF for b in member[10:30]) + member[30:],
            "decompression_limit": gzip.compress(b"\x00" * 200_000),
        }
        for code, data in cases.items():
            with self.subTest(code=code), self.assertRaises(CompressionError) as ctx:
                decompress_single_member(data, 100_000)
            self.assertEqual(ctx.exception.code, code)

    def test_bomb_is_stopped_without_inflating_everything(self) -> None:
        bomb = gzip.compress(b"\x00" * (50 * 1024 * 1024))  # ~50 KiB compressed, 50 MiB inflated
        self.assertLess(len(bomb), 100_000)
        with self.assertRaises(CompressionError) as ctx:
            decompress_single_member(bomb, 1024 * 1024)
        self.assertEqual(ctx.exception.code, "decompression_limit")


class ParseTests(unittest.TestCase):
    def test_valid_response_record(self) -> None:
        rid = uuid.uuid4()
        body = html_page("  Hello &amp;\n  World  ", '<link rel="canonical" href="/articles/one?ref=c">')
        record = make_warc_record(URL, body, record_id=rid, http_headers={"Location": "https://example.com/x"})
        capture = parse(record, expected=expected_for(record, digest=sha1_b32(body)))
        self.assertEqual((capture.warc_type, capture.target_uri, capture.record_id, capture.http_status),
                         ("response", URL, rid, 200))
        self.assertEqual(capture.warc_date, datetime(2024, 8, 5, 23, 12, 34, tzinfo=timezone.utc))
        self.assertEqual(capture.payload_digest, f"sha1:{sha1_b32(body)}")
        self.assertTrue(capture.payload_digest_verified)
        self.assertEqual(capture.http_content_type, "text/html; charset=utf-8")
        self.assertEqual(capture.redirect_location, "https://example.com/x")
        self.assertEqual(capture.source_title, "Hello & World")
        self.assertEqual(capture.canonical_url, "https://example.com/articles/one?ref=c")
        self.assertEqual(capture.payload_bytes, len(body))

    def test_warc_1_1_and_bracketed_target(self) -> None:
        record = make_warc_record(URL, version="WARC/1.1", target=f"<{URL}>")
        self.assertEqual(parse(record).warc_version, "WARC/1.1")

    def test_invalid_records(self) -> None:
        body = html_page()
        cases = {
            "length_mismatch": None,
            "unsupported_warc_version": make_warc_record(URL, version="WARC/0.9"),
            "unsupported_warc_type": make_warc_record(URL, warc_type="request"),
            "target_mismatch": make_warc_record(URL, target="https://evil.example/"),
            "status_mismatch": make_warc_record(URL, status=404),
            "payload_digest_mismatch": make_warc_record(URL, body, payload_digest="sha1:" + "A" * 32),
            "trailing_data": make_warc_record(URL) + gzip.compress(b"next"),
            "truncated": make_warc_record(URL)[:-8],
            "not_gzip": make_warc_record(URL, compress=False),
            "bad_warc_header": gzip.compress(b"WARC/1.0\r\nWARC-Type: response\r\n\r\nbody"),
            "truncated_block": gzip.compress(b"WARC/1.0\r\nWARC-Type: response\r\nContent-Length: 999\r\n"
                                             b"Content-Type: application/http\r\nWARC-Target-URI: " + URL.encode()
                                             + b"\r\n\r\nshort"),
        }
        for code, record in cases.items():
            with self.subTest(code=code), self.assertRaises(WarcRecordError) as ctx:
                if record is None:
                    good = make_warc_record(URL)
                    parse_warc_record(good + b"x", expected_for(good), max_decompressed=LIMIT)
                else:
                    parse(record)
            self.assertEqual(ctx.exception.code, code)

    def test_index_digest_mismatch(self) -> None:
        record = make_warc_record(URL)
        with self.assertRaises(WarcRecordError) as ctx:
            parse(record, expected=expected_for(record, digest="B" * 32))
        self.assertEqual(ctx.exception.code, "index_digest_mismatch")

    def test_decompression_limit(self) -> None:
        record = make_warc_record(URL, html_page(body="x" * 300_000))
        with self.assertRaises(WarcRecordError) as ctx:
            parse(record, max_decompressed=100_000)
        self.assertEqual(ctx.exception.code, "decompression_limit")

    def test_digest_verification_skipped_when_transfer_coded_or_disabled(self) -> None:
        record = make_warc_record(URL, payload_digest="sha1:" + "C" * 32, http_headers={"Transfer-Encoding": "chunked"})
        capture = parse(record)
        self.assertFalse(capture.payload_digest_verified)
        self.assertEqual(capture.payload_digest, "sha1:" + "C" * 32)
        self.assertIsNone(capture.source_title)  # not decodable as plain HTML: nothing extracted
        record = make_warc_record(URL, payload_digest="sha1:" + "D" * 32)
        self.assertFalse(parse(record, verify_payload_digest=False).payload_digest_verified)

    def test_digest_absent_falls_back_to_the_recorded_index_digest(self) -> None:
        record = make_warc_record(URL, payload_digest=None)
        capture = parse(record, expected=expected_for(record, digest="E" * 32))
        self.assertEqual((capture.payload_digest, capture.payload_digest_verified), ("sha1:" + "E" * 32, False))
        self.assertIsNone(parse(record).payload_digest)

    def test_unparseable_optional_headers_become_null(self) -> None:
        raw = gzip.decompress(make_warc_record(URL))
        raw = raw.replace(b"2024-08-05T23:12:34Z", b"not-a-date").replace(b"<urn:uuid:", b"<urn:x:")
        capture = parse(gzip.compress(raw))
        self.assertIsNone(capture.warc_date)
        self.assertIsNone(capture.record_id)


class MetadataTests(unittest.TestCase):
    def test_title_is_never_synthesized(self) -> None:
        self.assertIsNone(parse(make_warc_record(URL, html_page(title=None))).source_title)
        self.assertIsNone(parse(make_warc_record(URL, html_page(title="   "))).source_title)
        self.assertIsNone(parse(make_warc_record(URL, html_page(title="x" * 3000))).source_title)
        self.assertIsNone(extract_title("<title>a\x07b</title>"))

    def test_title_not_extracted_for_non_html(self) -> None:
        record = make_warc_record(URL, b"<title>Plain</title>", content_type="text/plain")
        self.assertIsNone(parse(record, expected=expected_for(record, mime="text/plain")).source_title)

    def test_charset_handling(self) -> None:
        body = "<html><head><title>Café</title></head></html>".encode("latin-1")
        record = make_warc_record(URL, body, content_type="text/html; charset=ISO-8859-1")
        self.assertEqual(parse(record).source_title, "Café")
        meta = b'<html><head><meta charset="windows-1252"><title>Na\xefve</title></head></html>'
        self.assertEqual(parse(make_warc_record(URL, meta, content_type="text/html")).source_title, "Naïve")
        unknown = make_warc_record(URL, html_page("T"), content_type="text/html; charset=x-unknown-9")
        self.assertIsNone(parse(unknown).source_title)  # unknown declared charset: not guessed
        broken = make_warc_record(URL, b"<title>\xff\xfe bad utf8</title>", content_type="text/html")
        self.assertIsNone(parse(broken).source_title)

    def test_canonical_rules(self) -> None:
        self.assertEqual(extract_canonical('<link href="https://a.example/x" rel="Canonical">', URL),
                         "https://a.example/x")
        self.assertEqual(extract_canonical("<link rel='alternate canonical' href='/p'>", URL), "https://example.com/p")
        self.assertIsNone(extract_canonical('<link rel="canonical" href="javascript:alert(1)">', URL))
        self.assertIsNone(extract_canonical('<link rel="stylesheet" href="/s.css">', URL))
        self.assertIsNone(extract_canonical('<link rel="canonical" href="">', URL))

    def test_huge_body_scans_only_a_bounded_prefix(self) -> None:
        body = b"<html><head></head><body>" + os.urandom(10).hex().encode() * 50_000 + b"<title>late</title>"
        self.assertIsNone(parse(make_warc_record(URL, body), max_decompressed=4 * LIMIT).source_title)


if __name__ == "__main__":
    unittest.main()
