"""Bronze -> Silver extraction: integrity, WARC payload, decoding, HTML cleaning, GitHub files."""

import gzip
import unittest

from app.processing.extraction.decoding import DecodeError, decode_bytes, sniff_binary
from app.processing.extraction.html import extract_html
from app.processing.normalization.text import canonicalize_text
from app.processing.silver.config import LimitsConfig, SilverConfig
from app.processing.silver.model import ExtractionOutcome, FailureKind, Gate, Outcome
from app.processing.silver.pipeline import SilverEvaluator
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.github.bronze import GitHubBronze

from tests.unit.silver_fixtures import article_html, store_cc, store_gh, tmpdir


def _html(text, **kw):
    return extract_html(text, max_nodes=10_000, max_depth=256, max_chars=1_000_000, deadline_seconds=5, **kw).text


class Base(unittest.TestCase):
    def setUp(self) -> None:
        root = tmpdir(self)
        self.cc, self.gh = BronzeStore(root / "cc"), GitHubBronze(root / "gh")
        self.config = SilverConfig()
        self.evaluator = SilverEvaluator(self.config, cc_bronze=self.cc, gh_bronze=self.gh)


class CommonCrawlExtractionTests(Base):
    def test_real_like_warc_article_is_cleaned_with_structure(self) -> None:
        ev = self.evaluator.evaluate(store_cc(self.cc, "https://example.com/photosynthesis", article_html()))
        self.assertIs(ev.outcome, ExtractionOutcome.DOCUMENT)
        text = ev.canonical_text
        # meaningful structure is preserved
        self.assertTrue(text.startswith("# Photosynthesis\n\nPhotosynthesis is the biological process"))
        self.assertIn("\n\n## Stages of photosynthesis\n\n", text)
        self.assertIn("1. The light-dependent reactions split water molecules and release oxygen.\n2. The Calvin",
                      text)
        self.assertIn("Input | Output\nCarbon dioxide and water | Glucose", text)
        # active content, chrome, cookie banner, ads, sidebars and footer are gone
        for noise in ("window.track", "createElement", "font-family", "Stock market news", "We use cookies",
                      "Accept all", "Home", "Plant Science Hub", "fertilizer", "Cellular respiration",
                      "All rights reserved", "Privacy policy"):
            self.assertNotIn(noise, text, noise)
        removed = {r.gate: r.reasons for r in ev.results}
        self.assertIn("active_script=1", removed[Gate.ACTIVE])
        self.assertIn("boilerplate_site_footer=1", removed[Gate.BOILERPLATE])
        self.assertIn("chrome_consent=1", removed[Gate.CHROME])
        self.assertIn("ads_container=1", removed[Gate.ADS])
        self.assertEqual(text, canonicalize_text(text))  # the LOCKED canonical form

    def test_hidden_nav_link_dense_and_repeated_template_blocks_are_removed(self) -> None:
        page = ("<body><div aria-hidden='true'>hidden promo</div><p hidden>seo keywords</p>"
                "<div style='display: none'>invisible</div><span class='sr-only'>Skip to content</span>"
                "<div class='d-none d-md-block'><p>Responsive but visible paragraph about rivers.</p></div>"
                "<div><a href='/1'>One</a> <a href='/2'>Two</a> <a href='/3'>Three</a> <a href='/4'>Four</a></div>"
                + "<p>Read more</p>" * 3 +
                "<p>Rivers carry water from higher ground to the sea and shape valleys over time.</p></body>")
        text = _html(page)
        self.assertEqual(text, "Responsive but visible paragraph about rivers.\n\n"
                               "Rivers carry water from higher ground to the sea and shape valleys over time.")

    def test_code_and_definition_structures_are_preserved(self) -> None:
        text = _html("<body><dl><dt>Latency</dt><dd>The delay before a transfer begins.</dd></dl>"
                     "<pre>for i in range(3):\n    print(i)</pre><blockquote>Quoted fact.</blockquote></body>")
        self.assertEqual(text, "Latency: The delay before a transfer begins.\n\n"
                               "```\nfor i in range(3):\n    print(i)\n```\n\n> Quoted fact.")

    def test_malformed_warc_is_dead_lettered_not_parsed(self) -> None:
        ref = store_cc(self.cc, "https://example.com/a", article_html())
        record_path, _manifest = self.cc.paths(ref.crawl_id, ref.observation_id)
        data = bytearray(record_path.read_bytes())
        data[len(data) // 2] ^= 0xFF  # corrupt the committed evidence
        record_path.write_bytes(bytes(data))
        ev = self.evaluator.evaluate(ref)
        self.assertIs(ev.failure, FailureKind.PERMANENT_INVALID)
        self.assertEqual(ev.failure_code, "bronze_record_damaged")
        self.assertFalse(ev.record_extraction)  # no outcome is recorded for corrupt evidence
        self.assertIsNone(ev.canonical_text)

    def test_identity_mismatch_and_incomplete_evidence_are_rejected(self) -> None:
        ref = store_cc(self.cc, "https://example.com/a", article_html())
        from dataclasses import replace
        ev = self.evaluator.evaluate(replace(ref, warc_offset=ref.warc_offset + 1))
        self.assertEqual(ev.failure_code, "identity_mismatch")
        _record, manifest = self.cc.paths(ref.crawl_id, ref.observation_id)
        manifest.unlink()  # crash before the manifest: the record alone is not complete evidence
        ev = self.evaluator.evaluate(ref)
        self.assertEqual(ev.failure_code, "bronze_record_only")
        self.assertFalse(ev.record_extraction)

    def test_decompression_limit_is_a_deterministic_outcome(self) -> None:
        body = article_html(body="<p>" + "word " * 200_000 + "</p>")
        ref = store_cc(self.cc, "https://example.com/big", body)
        small = SilverConfig(limits=LimitsConfig(max_decompressed_bytes=256 * 1024))
        ev = SilverEvaluator(small, cc_bronze=self.cc, gh_bronze=self.gh).evaluate(ref)
        self.assertIs(ev.outcome, ExtractionOutcome.EXTRACTION_FAILED)
        self.assertEqual(ev.failure_code, "warc_decompression_limit")

    def test_non_text_and_encoded_payloads_are_prefiltered(self) -> None:
        cases = [
            (store_cc(self.cc, "https://example.com/doc.pdf", b"%PDF-1.7 binary", content_type="application/pdf"),
             "binary_pdf"),
            (store_cc(self.cc, "https://example.com/data", b'{"k": 1}', content_type="application/json",
                      offset=2000), "mime_application/json"),
            (store_cc(self.cc, "https://example.com/gone", article_html(), status=404, offset=3000),
             "http_status_404"),
            (store_cc(self.cc, "https://example.com/gz", article_html(), offset=4000,
                      http_headers={"Content-Encoding": "gzip"}), "encoded_payload"),
        ]
        for ref, code in cases:
            ev = self.evaluator.evaluate(ref)
            self.assertIs(ev.outcome, ExtractionOutcome.REJECTED_PREFILTER, code)
            self.assertEqual(ev.failure_code, code)


class DecodingTests(unittest.TestCase):
    def test_bom_declared_charset_and_fallback(self) -> None:
        self.assertEqual(decode_bytes("é".encode("utf-16-le").join([b"\xff\xfe", b""])).text, "é")
        self.assertEqual(decode_bytes("café".encode("cp1252"), declared="iso-8859-1").text, "café")
        self.assertEqual(decode_bytes("naïve".encode("utf-8")).source, "default")
        meta = b"<meta charset='windows-1252'><p>\x93quoted\x94</p>"
        self.assertIn("“quoted”", decode_bytes(meta, html=True).text)

    def test_corrupt_and_unsafe_encodings_are_rejected(self) -> None:
        with self.assertRaises(DecodeError) as ctx:
            decode_bytes(b"abc" + b"\xc3\x28\xfd" * 100)
        self.assertEqual(ctx.exception.code, "undecodable")
        with self.assertRaises(DecodeError) as ctx:
            decode_bytes(b"hello", declared="utf-7")
        self.assertEqual(ctx.exception.code, "unsupported_charset")
        with self.assertRaises(DecodeError) as ctx:
            decode_bytes(b"text \x01\x02\x03\x04 with controls " * 3)
        self.assertEqual(ctx.exception.code, "control_chars")
        self.assertEqual(sniff_binary(gzip.compress(b"x")), "binary_gzip")
        self.assertIsNone(sniff_binary("plain text".encode("utf-16")))


class GitHubExtractionTests(Base):
    README = ("---\ntitle: kvstore\n---\n# kvstore\n\n<!-- build badges -->\n"
              "[![CI](https://img.shields.io/badge/ci-passing-green.svg)](https://ci.example.org)\n\n"
              "kvstore is an embedded key-value database written in Go. It stores data in a log-structured merge "
              "tree and supports atomic batch writes, snapshots and range queries. See the [design notes](docs/"
              "design.md) for details.\n\n## Usage\n\nOpen a database, then call Put to insert a key and Get to "
              "read it back. Each write is appended to the write-ahead log before it is applied to the memory "
              "table, so a crash never loses an acknowledged write.\n\n```go\ndb, err := kvstore.Open(\"data\")\n"
              "```\n\n[1]: https://example.org/lsm\n")

    def test_markdown_to_clean_structured_text(self) -> None:
        ev = self.evaluator.evaluate(store_gh(self.gh, "README.md", self.README))
        self.assertIs(ev.outcome, ExtractionOutcome.DOCUMENT)
        text = ev.canonical_text
        self.assertTrue(text.startswith("# kvstore\n\nkvstore is an embedded key-value database"))
        self.assertIn("See the design notes for details.", text)
        self.assertIn("```go\ndb, err := kvstore.Open(\"data\")\n```", text)
        for gone in ("title: kvstore", "shields.io", "build badges", "example.org/lsm", "docs/design.md"):
            self.assertNotIn(gone, text)

    def test_documented_source_code_is_kept_verbatim(self) -> None:
        source = ('"""Utilities for computing checksums of files.\n\nThe functions in this module read files in '
                  'fixed-size blocks so that memory use stays constant for very large inputs.\n"""\n\nimport '
                  'hashlib\n\n\ndef sha256_file(path, block_size=65536):\n    """Return the SHA-256 hex digest of '
                  'the file at path, reading it in blocks of block_size bytes."""\n    digest = hashlib.sha256()\n'
                  '    with open(path, "rb") as handle:\n        # Read until the end of the file is reached.\n'
                  '        for block in iter(lambda: handle.read(block_size), b""):\n            digest.update(block)\n'
                  '    return digest.hexdigest()\n')
        ev = self.evaluator.evaluate(store_gh(self.gh, "src/checksum.py", source))
        self.assertIs(ev.outcome, ExtractionOutcome.DOCUMENT, [r.code() for r in ev.results])
        self.assertIn("def sha256_file(path, block_size=65536):", ev.canonical_text)
        self.assertEqual(ev.text_kind, "code")

    def test_generated_vendored_minified_license_and_binary_files_are_rejected(self) -> None:
        cases = {
            "api/service.pb.go": ("// Code generated by protoc-gen-go. DO NOT EDIT.\npackage api\n", "generated_name"),
            "gen/model.py": ("# @generated by schema tool\nX = 1\n", "generated"),
            "node_modules/lib/index.js": ("module.exports = 1;\n", "excluded_dir"),
            "static/app.js": ("var a=1;" * 300 + "\n", "minified"),
            "LICENSE": ("MIT License\n\nPermission is hereby granted...\n", "license_boilerplate"),
            "docs/image.md": (b"\x89PNG\r\n\x1a\n" + b"\x00" * 64, "binary_masquerading_as_text"),
        }
        for path, (content, code) in cases.items():
            ev = self.evaluator.evaluate(store_gh(self.gh, path, content))
            self.assertIs(ev.outcome, ExtractionOutcome.REJECTED_PREFILTER, path)
            self.assertEqual(ev.failure_code, code, path)
            self.assertEqual(ev.result(ev.failure_gate).outcome, Outcome.FAIL)

    def test_github_blob_tampering_is_detected(self) -> None:
        ref = store_gh(self.gh, "README.md", self.README)
        self.gh.blob_path(ref.blob_sha).write_bytes(b"tampered")
        ev = self.evaluator.evaluate(ref)
        self.assertEqual(ev.failure_code, "bronze_blob_missing")
        self.assertFalse(ev.record_extraction)


class NormalizationGateTests(Base):
    def test_empty_and_nul_are_rejected_before_postgresql(self) -> None:
        empty = self.evaluator.evaluate(store_cc(self.cc, "https://example.com/e",
                                                 b"<html><body><nav><a href='/'>Home</a></nav></body></html>"))
        self.assertIs(empty.outcome, ExtractionOutcome.EMPTY)
        nul = self.evaluator.evaluate(store_gh(self.gh, "notes.txt", "Water boils at 100 degrees.\x00" * 20))
        self.assertIn(nul.outcome, (ExtractionOutcome.REJECTED_PREFILTER, ExtractionOutcome.REJECTED_NUL))
        text = ("Line one with a NUL here \x00 inside a longer English sentence about the water cycle and "
                "evaporation. ") + "The water cycle moves water between the oceans, the air and the land. " * 10
        nul_late = self.evaluator.evaluate(store_gh(self.gh, "notes2.txt", ("x" * 9000) + "\n" + text))
        self.assertIs(nul_late.outcome, ExtractionOutcome.REJECTED_NUL)
        self.assertIsNone(nul_late.canonical_text)

    def test_unicode_edge_cases_use_the_locked_canonicalizer(self) -> None:
        body = ("<p>﻿Café costs 3 euros.\r\nZero​width stays.</p>"
                "<p>Photosynthesis converts light energy into chemical energy in plants.</p>")
        text = _html(f"<body>{body}</body>")
        self.assertEqual(canonicalize_text(text), canonicalize_text(canonicalize_text(text)))
        self.assertIn("Café costs 3 euros.", canonicalize_text(text))


if __name__ == "__main__":
    unittest.main()
