"""CDX index parsing, validation, candidate filtering and frozen observation identities (offline)."""

import json
import unittest
import uuid

from app.sources.commoncrawl.config import FilterConfig
from app.sources.commoncrawl.index import (
    IndexRowError,
    InvalidSourceUrl,
    parse_cdx_line,
    rejection_reason,
    source_url_host,
    split_block_lines,
)
from app.utils.ids import cc_observation_id

from tests.unit.cc_fixtures import CRAWL, make_cdx_line, warc_filename

FILE = warc_filename()
URL = "https://Example.com/page"
FILTER = FilterConfig()


def line(**kwargs) -> bytes:
    params = dict(url=URL, filename=FILE, offset=1000, length=500, urlkey="com,example)/page")
    params.update(kwargs)
    return make_cdx_line(params.pop("url"), params.pop("filename"), params.pop("offset"), params.pop("length"),
                         **params)


class ParseTests(unittest.TestCase):
    def test_valid_line(self) -> None:
        record = parse_cdx_line(line(digest="A" * 32, languages="eng,fra"), CRAWL)
        self.assertEqual((record.url, record.url_host, record.filename, record.offset, record.length, record.status),
                         (URL, "example.com", FILE, 1000, 500, 200))
        self.assertEqual((record.record_folder, record.effective_mime, record.languages, record.digest),
                         ("warc", "text/html", ("eng", "fra"), "A" * 32))
        self.assertEqual(record.index_fields["url"], URL)

    def test_integer_json_fields_and_mime_parameters(self) -> None:
        raw = b'com,example)/ 20240805231234 ' + json.dumps({
            "url": URL, "filename": FILE, "offset": 5, "length": 7, "status": 200,
            "mime": "text/html; charset=UTF-8"}).encode()
        record = parse_cdx_line(raw, CRAWL)
        self.assertEqual((record.offset, record.length, record.effective_mime), (5, 7, "text/html"))

    def test_malformed_rows_are_rejected_with_a_code(self) -> None:
        cases = {
            "not_utf8": b"com,example)/ 20240805231234 {\xff}",
            "bad_layout": b"only-two 20240805231234",
            "bad_timestamp": line().replace(b"20240805231234", b"2024-08-05"),
            "bad_json": b"com,example)/ 20240805231234 {not json",
            "line_too_long": b"x" * 70_000,
        }
        cases["bad_json_nan"] = b'com,example)/ 20240805231234 {"url": NaN}'
        cases["bad_json_list"] = b"com,example)/ 20240805231234 [1, 2]"
        expected = {"bad_json_nan": "bad_json", "bad_json_list": "bad_json"}
        for name, raw in cases.items():
            with self.subTest(case=name), self.assertRaises(IndexRowError) as ctx:
                parse_cdx_line(raw, CRAWL)
            self.assertEqual(ctx.exception.code, expected.get(name, name))

    def test_hostile_fields_are_rejected(self) -> None:
        bad = {
            "invalid_url": [dict(url="javascript:alert(1)"), dict(url="file:///etc/passwd"), dict(url="http://"),
                            dict(url="http://a b/"), dict(url="http://example.com:99999/"), dict(url=""),
                            dict(url="https://example.com/\x00"), dict(url="http://x/" + "a" * 5000)],
            "invalid_filename": [dict(filename="crawl-data/CC-MAIN-2024-33/../../etc/passwd"),
                                 dict(filename="/crawl-data/CC-MAIN-2024-33/segments/1.0/warc/a.warc.gz"),
                                 dict(filename="https://evil.example/crawl-data/CC-MAIN-2024-33/x.warc.gz"),
                                 dict(filename="crawl-data/CC-MAIN-2024-33/segments/1.0/warc/a b.warc.gz"),
                                 dict(filename="crawl-data/CC-MAIN-2024-33/segments/1.0/warc/..warc.gz/../x.warc.gz")],
            "crawl_mismatch": [dict(filename=warc_filename(crawl="CC-MAIN-2024-38"))],
            "invalid_field": [dict(offset=-1), dict(offset="12a"), dict(offset=True), dict(length=0),
                              dict(length=2 ** 31), dict(offset=2 ** 63), dict(status="abc"), dict(digest="xyz"),
                              dict(mime="x" * 300)],
        }
        for code, variants in bad.items():
            for variant in variants:
                with self.subTest(code=code, variant=str(variant)[:60]), self.assertRaises(IndexRowError) as ctx:
                    parse_cdx_line(line(**variant), CRAWL)
                self.assertEqual(ctx.exception.code, code)

    def test_source_url_host(self) -> None:
        self.assertEqual(source_url_host("HTTP://WWW.Example.COM:8080/a?b#c"), "www.example.com")
        for bad in ("ftp://x/", "//x/", "http://x/\n", 5, None):
            with self.subTest(bad=bad), self.assertRaises(InvalidSourceUrl):
                source_url_host(bad)

    def test_split_block_lines(self) -> None:
        self.assertEqual(split_block_lines(b"a\nb\n"), [b"a", b"b"])
        self.assertEqual(split_block_lines(b"a\nb"), [b"a", b"b"])
        self.assertEqual(split_block_lines(b""), [])


class FilterTests(unittest.TestCase):
    def reason(self, config: FilterConfig = FILTER, **kwargs) -> str | None:
        return rejection_reason(parse_cdx_line(line(**kwargs), CRAWL), config)

    def test_candidate_accepted(self) -> None:
        self.assertIsNone(self.reason())
        self.assertIsNone(self.reason(languages=None))  # no language hint: kept (never guessed)
        self.assertIsNone(self.reason(mime="application/octet-stream", mime_detected="application/xhtml+xml"))

    def test_rejections(self) -> None:
        cases = {
            "status": [dict(status="404"), dict(status="301"), dict(status=None)],
            "mime": [dict(mime="image/png", mime_detected="image/png"), dict(mime_detected="application/pdf")],
            "language": [dict(languages="fra,deu")],
            "truncated": [dict(truncated="length")],
            "too_large": [dict(length=FILTER.max_record_bytes + 1)],
            "record_type": [dict(filename=warc_filename(folder="crawldiagnostics")),
                            dict(filename=warc_filename(folder="robotstxt"))],
        }
        for reason, variants in cases.items():
            for variant in variants:
                with self.subTest(reason=reason, variant=variant):
                    self.assertEqual(self.reason(**variant), reason)

    def test_configurable_filters(self) -> None:
        permissive = FilterConfig(mime_types=("text/plain",), languages=(), reject_truncated=False)
        self.assertIsNone(self.reason(permissive, mime="text/plain", mime_detected="text/plain", languages="fra",
                                      truncated="length"))

    def test_filter_version_is_deterministic_and_parameter_bound(self) -> None:
        self.assertEqual(FilterConfig().version, FilterConfig(mime_types=("text/html", "application/xhtml+xml")).version)
        self.assertRegex(FilterConfig().version, r"^f1\.[0-9a-f]{12}$")
        variants = [FilterConfig(languages=()), FilterConfig(reject_truncated=False),
                    FilterConfig(max_record_bytes=1024 * 1024), FilterConfig(mime_types=("text/html",))]
        self.assertEqual(len({v.version for v in variants} | {FilterConfig().version}), 5)


class FrozenObservationIdentityTests(unittest.TestCase):
    """The acquisition derives observation ids ONLY through the LOCKED Identity Extension V1."""

    def test_frozen_vector(self) -> None:
        name = "crawl-data/CC-MAIN-2024-33/segments/1722640353668.0/warc/CC-MAIN-20240805230130-20240806020130-00042.warc.gz"
        raw = make_cdx_line("https://example.com/", name, 123456789, 18220)
        record = parse_cdx_line(raw, CRAWL)
        self.assertEqual(cc_observation_id(CRAWL, record.filename, record.offset, record.length),
                         uuid.UUID("afb1a4e1-acd0-5e13-96e2-94eb6aec8299"))

    def test_identity_ignores_url_timestamp_and_metadata(self) -> None:
        a = parse_cdx_line(line(url="https://a.example/", timestamp="20240101000000", languages="eng"), CRAWL)
        b = parse_cdx_line(line(url="https://b.example/", timestamp="20240202000000", languages="fra"), CRAWL)
        ida = cc_observation_id(CRAWL, a.filename, a.offset, a.length)
        self.assertEqual(ida, cc_observation_id(CRAWL, b.filename, b.offset, b.length))
        self.assertNotEqual(ida, cc_observation_id(CRAWL, a.filename, a.offset + 1, a.length))


if __name__ == "__main__":
    unittest.main()
