"""Page publication / modification dates (app.sources.commoncrawl.dates, ccdate-v1), offline."""

import gzip
import unittest
from datetime import datetime, timezone

from app.sources.commoncrawl.dates import extract_dates, parse_http_date, parse_iso
from app.sources.commoncrawl.index import parse_cdx_line
from app.sources.commoncrawl.warc import parse_warc_record

from tests.unit.cc_fixtures import CRAWL, make_cdx_line, make_warc_record, warc_filename

CRAWLED = datetime(2024, 8, 5, 23, 12, 34, tzinfo=timezone.utc)
UTC = timezone.utc


def page(head: str = "", body: str = "<p>Text</p>") -> str:
    return f"<html><head><title>T</title>{head}</head><body>{body}</body></html>"


class ParseTests(unittest.TestCase):
    def test_iso_forms(self) -> None:
        self.assertEqual(parse_iso("2021-03-04"), datetime(2021, 3, 4, tzinfo=UTC))
        self.assertEqual(parse_iso("20210304"), datetime(2021, 3, 4, tzinfo=UTC))
        self.assertEqual(parse_iso("2021-03-04T10:20:30Z"), datetime(2021, 3, 4, 10, 20, 30, tzinfo=UTC))
        self.assertEqual(parse_iso("2021-03-04T10:20:30+02:00"), datetime(2021, 3, 4, 8, 20, 30, tzinfo=UTC))
        self.assertEqual(parse_iso("2021-03-04T10:20:30.123-0500"), datetime(2021, 3, 4, 15, 20, 30, tzinfo=UTC))
        self.assertEqual(parse_iso("2021-03-04 10:20"), datetime(2021, 3, 4, 10, 20, tzinfo=UTC))

    def test_unparseable_values_are_none(self) -> None:
        for value in ("March 4, 2021", "2021-13-01", "2021-02-30", "yesterday", "", None, 20210304, "2021-03-04T25:00",
                      "x" * 100):
            with self.subTest(value=value):
                self.assertIsNone(parse_iso(value))

    def test_http_date(self) -> None:
        self.assertEqual(parse_http_date("Tue, 15 Nov 1994 08:12:31 GMT"),
                         datetime(1994, 11, 15, 8, 12, 31, tzinfo=UTC))
        self.assertIsNone(parse_http_date("not a date"))
        self.assertIsNone(parse_http_date(None))


class ExtractTests(unittest.TestCase):
    def test_jsonld_wins_and_records_its_evidence(self) -> None:
        html = page('<script type="application/ld+json">{"@context":"https://schema.org","@type":"Article",'
                    '"datePublished":"2002-01-15T10:00:00Z","dateModified":"2024-07-01T09:00:00Z"}</script>'
                    '<meta property="article:published_time" content="2010-01-01">')
        dates = extract_dates(html, http_last_modified=None, crawled_at=CRAWLED)
        self.assertEqual((dates.published_at, dates.published_evidence),
                         (datetime(2002, 1, 15, 10, tzinfo=UTC), "jsonld:datePublished"))
        self.assertEqual((dates.modified_at, dates.modified_evidence),
                         (datetime(2024, 7, 1, 9, tzinfo=UTC), "jsonld:dateModified"))

    def test_jsonld_graph_and_lists(self) -> None:
        html = page('<script type="application/ld+json">[{"@graph":[{"@type":"WebPage"},'
                    '{"@type":"NewsArticle","datePublished":"2019-05-06"}]}]</script>')
        self.assertEqual(extract_dates(html, http_last_modified=None, crawled_at=CRAWLED).published_at,
                         datetime(2019, 5, 6, tzinfo=UTC))

    def test_meta_and_time_tags(self) -> None:
        html = page('<meta name="citation_publication_date" content="2015-02-03">'
                    '<meta property="og:updated_time" content="2016-04-05T06:07:08+00:00">')
        dates = extract_dates(html, http_last_modified=None, crawled_at=CRAWLED)
        self.assertEqual((dates.published_at, dates.published_evidence),
                         (datetime(2015, 2, 3, tzinfo=UTC), "meta:citation_publication_date"))
        self.assertEqual(dates.modified_evidence, "meta:og:updated_time")
        html = page(body='<time itemprop="datePublished" datetime="2011-11-11">Nov 11</time>')
        self.assertEqual(extract_dates(html, http_last_modified=None, crawled_at=CRAWLED).published_evidence,
                         "time:datePublished")

    def test_implausible_dates_are_discarded_never_clamped(self) -> None:
        html = page('<meta property="article:published_time" content="1970-01-01">'
                    '<meta property="article:modified_time" content="2030-01-01">')
        dates = extract_dates(html, http_last_modified="Wed, 01 Jan 2031 00:00:00 GMT", crawled_at=CRAWLED)
        self.assertEqual((dates.published_at, dates.modified_at, dates.http_last_modified), (None, None, None))
        self.assertEqual((dates.published_evidence, dates.modified_evidence), (None, None))

    def test_modified_before_published_is_dropped(self) -> None:
        html = page('<meta property="article:published_time" content="2020-01-01">'
                    '<meta property="article:modified_time" content="2019-01-01">')
        dates = extract_dates(html, http_last_modified=None, crawled_at=CRAWLED)
        self.assertEqual(dates.published_at, datetime(2020, 1, 1, tzinfo=UTC))
        self.assertIsNone(dates.modified_at)

    def test_url_or_text_dates_are_never_used(self) -> None:
        html = page(body="<p>Published on 2020-01-01. Updated 2021-02-02.</p>")
        dates = extract_dates(html, http_last_modified=None, crawled_at=CRAWLED)
        self.assertEqual((dates.published_at, dates.modified_at), (None, None))

    def test_http_last_modified_and_no_html(self) -> None:
        dates = extract_dates(None, http_last_modified="Mon, 01 Jul 2024 00:00:00 GMT", crawled_at=CRAWLED)
        self.assertEqual(dates.http_last_modified, datetime(2024, 7, 1, tzinfo=UTC))
        self.assertIsNone(dates.published_at)

    def test_malformed_jsonld_is_ignored(self) -> None:
        html = page('<script type="application/ld+json">{not json</script>'
                    '<meta name="dcterms.modified" content="2022-02-02">')
        dates = extract_dates(html, http_last_modified=None, crawled_at=CRAWLED)
        self.assertEqual(dates.modified_evidence, "meta:dcterms.modified")


class WarcIntegrationTests(unittest.TestCase):
    def test_parse_warc_record_carries_page_dates(self) -> None:
        url = "https://en.wikipedia.org/wiki/Oxygen"
        body = page('<script type="application/ld+json">{"datePublished":"2001-11-09T00:00:00Z",'
                    '"dateModified":"2024-07-30T12:00:00Z"}</script>').encode()
        record = make_warc_record(url, body)
        expected = parse_cdx_line(make_cdx_line(url, warc_filename(), 0, len(record)), CRAWL)
        capture = parse_warc_record(record, expected, max_decompressed=len(gzip.decompress(record)) + 1)
        self.assertEqual(capture.warc_date, CRAWLED)
        self.assertEqual(capture.page_dates.published_at, datetime(2001, 11, 9, tzinfo=UTC))
        self.assertEqual(capture.page_dates.modified_at, datetime(2024, 7, 30, 12, tzinfo=UTC))

    def test_json_ld_at_the_end_of_a_large_page_is_found(self) -> None:
        # Wikipedia writes its JSON-LD at the very end of the HTML (byte ~293,700 of ~294,500 for a real page),
        # beyond the 256 KiB title window; the date scan covers the whole page.
        url = "https://en.wikipedia.org/wiki/Oxygen"
        body = ("<html><head><title>Oxygen - Wikipedia</title></head><body>" + "<p>Oxygen is an element.</p>" * 12000
                + '<script type="application/ld+json">{"@type":"Article","datePublished":"2001-11-09T00:00:00Z",'
                  '"dateModified":"2024-08-01T00:00:00Z"}</script></body></html>').encode()
        self.assertGreater(len(body), 300 * 1024)
        record = make_warc_record(url, body)
        expected = parse_cdx_line(make_cdx_line(url, warc_filename(), 0, len(record)), CRAWL)
        capture = parse_warc_record(record, expected, max_decompressed=len(gzip.decompress(record)) + 1)
        self.assertEqual(capture.source_title, "Oxygen - Wikipedia")
        self.assertEqual((capture.page_dates.published_evidence, capture.page_dates.modified_at),
                         ("jsonld:datePublished", datetime(2024, 8, 1, tzinfo=UTC)))


if __name__ == "__main__":
    unittest.main()
