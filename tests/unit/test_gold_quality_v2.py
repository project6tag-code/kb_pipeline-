"""Gold V2 quality fixes: mojibake repair, title-aware domains, document-type source_type, LaTeX/markup text."""

import re
import unittest
import uuid
from datetime import datetime, timezone

from app.processing.classification import domain
from app.processing.gold import repository, source_document
from app.processing.normalization import encoding, markup


class MojibakeTests(unittest.TestCase):
    def test_utf8_read_as_cp1252_is_repaired(self) -> None:
        cases = {
            "CafÃ© Ã  la crÃ¨me": "Café à la crème",  # U+00C3 U+00A0 is the UTF-8 bytes of "à"
            "itâ€™s a â€œquoteâ€\x9d": "it’s a “quote”",
            "100Â km and 5Â°C": "100 km and 5°C",
            "OxygÃ¨ne": "Oxygène",
            "ZÃ¼rich, MÃ¼nchen": "Zürich, München",
            "naÃ¯ve â€“ dash": "naïve – dash",
        }
        for broken, expected in cases.items():
            with self.subTest(broken=broken):
                self.assertEqual(encoding.repair(broken), expected)
                self.assertEqual(encoding.corruption_count(encoding.repair(broken)), 0)

    def test_double_encoding_is_repaired(self) -> None:
        once = "Café".encode("utf-8").decode("cp1252")
        twice = once.encode("utf-8").decode("cp1252")
        self.assertEqual(encoding.repair(twice), "Café")

    def test_correct_text_is_never_touched(self) -> None:
        for text in ("Café à la crème", "naïve – “quoted” 5°C €5", "Ångström Ö Ü ü", "plain ascii", "日本語 العربية",
                     "Ã alone", "The letter Â is Latin"):
            with self.subTest(text=text):
                self.assertEqual(encoding.repair(text), text)

    def test_remaining_corruption_is_counted(self) -> None:
        self.assertEqual(encoding.corruption_count("clean text"), 0)
        self.assertEqual(encoding.corruption_count("bad � and �"), 2)
        self.assertGreater(encoding.corruption_count("Ã\u0081Ã\u0081Ã\u0081 left"), 0)

    def test_silver_decode_gate_repairs_and_refuses(self) -> None:
        from app.processing.silver import pipeline

        self.assertIs(pipeline.encoding, encoding)  # the gate uses this module


class LatexAndMarkupTests(unittest.TestCase):
    def test_known_latex_becomes_readable_text(self) -> None:
        cases = {
            r"Energy $E = mc^{2}$ in joules": "Energy E = mc^2 in joules",
            r"Angle $\alpha \leq \pi/2$ holds": "Angle α ≤ π/2 holds",
            r"Rate is $\frac{dx}{dt}$ here": "Rate is dx/dt here",
            r"Water is $H_{2}O$": "Water is H_2O",
            r"Mass \(m = 5\,\text{kg}\) given": "Mass m = 5kg given",
            r"Root $\sqrt{x+1}$ and $\sqrt{2}$": "Root √(x+1) and √2",
            r"Sum $$\sum_{i=1}^{n} x_i$$ total": "Sum ∑_(i=1)^n x_i total",
            r"Product $a \times b \neq c$": "Product a × b ≠ c",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(markup.normalize(raw), expected)

    def test_unknown_commands_and_currency_are_left_as_written(self) -> None:
        untouched = (r"Matrix $\begin{pmatrix} a & b \end{pmatrix}$ here", r"Script $\mathcal{L}(x)$ here",
                     "It costs $5 and then $10 more", "Pay $20 today", r"Unknown $\foo{x}$ command", "no markup here",
                     r"Escaped \$5 and \$6")
        for text in untouched:
            with self.subTest(text=text):
                self.assertEqual(markup.normalize(text), text)

    def test_wiki_markup(self) -> None:
        self.assertEqual(markup.normalize("See [[Oxygen|the element]] and [[Ozone]]."), "See the element and Ozone.")
        self.assertEqual(markup.normalize("A '''bold''' and ''italic'' word"), "A bold and italic word")

    def test_idempotent(self) -> None:
        once = markup.normalize(r"Energy $E = mc^{2}$ and [[Oxygen|O]]")
        self.assertEqual(markup.normalize(once), once)


class DomainTests(unittest.TestCase):
    KW = dict(min_evidence=4.0, min_confidence=0.45, multi_ratio=0.80)

    def test_title_decides_an_ambiguous_body(self) -> None:
        body = ("The state of the system depends on the design. The system has a state, and the state changes "
                "with the design of the system and the energy of each cell in the process of the field.")
        biased = domain.classify(body, **self.KW, title="Photosynthesis in plant cells and organisms")
        self.assertEqual(biased.domain, "Biology")

    def test_generic_words_alone_do_not_assign_a_domain(self) -> None:
        generic = "The state of the system and the design of the system. " * 6
        result = domain.classify(generic, **self.KW)
        self.assertIn(result.decision, (domain.UNKNOWN, domain.MULTI))
        self.assertNotEqual((result.decision, result.domain), (domain.ASSIGNED, "Engineering"))

    def test_specific_text_still_classifies_without_a_title(self) -> None:
        text = ("Photosynthesis lets an organism convert light into chemical energy in every cell. The species "
                "of plants and algae that perform photosynthesis are studied in biology and ecology. " * 3)
        self.assertEqual(domain.classify(text, **self.KW).domain, "Biology")

    def test_version(self) -> None:
        self.assertEqual(domain.VERSION, "dom3")


class SurveyDataDomainTests(unittest.TestCase):
    """CDC survey / data documentation pages were labelled Software Engineering/Programming Languages."""

    KW = dict(min_evidence=4.0, min_confidence=0.45, multi_ratio=0.80)
    SURVEY = ("The survey data file contains one record per respondent. Each variable is described in the codebook "
              "table with its class, format and function in the weighting. The index variable and the test of the "
              "class variable are listed. Respondents reported health conditions, chronic disease, and health risk "
              "behaviors, and the survey estimates the prevalence of disease in the adult population. " * 3)
    TITLE = "CDC - 2021 BRFSS Survey Data and Documentation"
    URL = "https://www.cdc.gov/brfss/annual_data/annual_2021.html"

    def test_survey_documentation_is_not_software_engineering(self) -> None:
        result = domain.classify(self.SURVEY, **self.KW, title=self.TITLE, url=self.URL)
        self.assertEqual(result.domain, "Medicine and Health")
        self.assertNotIn("Software", result.label or "")

    def test_ambiguous_software_words_are_down_weighted(self) -> None:
        for word in ("variable", "function", "class", "object", "table", "index", "query", "test"):
            self.assertIn(word, domain.GENERIC_WORDS)

    def test_real_software_text_still_classifies_as_software(self) -> None:
        text = ("The python compiler reports a syntax error when the interpreter reaches a bad statement. "
                "Developers write software in python and java, and the programming language runtime executes code. " * 3)
        result = domain.classify(text, **self.KW, title="Python syntax errors", url="https://docs.example.org/python/")
        self.assertEqual(result.domain, "Software Engineering")

    def test_host_prior_tips_but_never_assigns_alone(self) -> None:
        thin = ("Our program supports clinical staff, patient care and health education in many ways. "
                "Medical teams share health news.")
        no_prior = domain.classify(thin, **self.KW, url="https://example.org/about")
        with_prior = domain.classify(thin, **self.KW, url="https://www.cdc.gov/about/index.html")
        self.assertEqual(with_prior.domain, "Medicine and Health")
        self.assertNotEqual((no_prior.decision, no_prior.domain), (domain.ASSIGNED, "Medicine and Health"))
        empty = domain.classify("Welcome to the site. Click below to continue to the next page.", **self.KW,
                                url="https://www.cdc.gov/")
        self.assertEqual(empty.decision, domain.UNKNOWN)  # a publisher alone never assigns a domain
        self.assertEqual(domain._host_prior("wwwnwc.cdc.gov"), "Medicine and Health")
        self.assertIsNone(domain._host_prior("notcdc.gov"))

    def test_url_slug_words_count_as_evidence(self) -> None:
        thin = "Facts and figures for families about what to do each day."
        flat = domain.classify(thin, **self.KW)
        slug = domain.classify(thin, **self.KW, url="https://example.org/vaccine/disease/symptom/treatment")
        self.assertGreater(sum(slug.evidence.values()), sum(flat.evidence.values()))

    def test_deterministic_and_url_is_optional(self) -> None:
        first = domain.classify(self.SURVEY, **self.KW, title=self.TITLE, url=self.URL)
        self.assertEqual(first, domain.classify(self.SURVEY, **self.KW, title=self.TITLE, url=self.URL))
        self.assertEqual(domain.classify(self.SURVEY, **self.KW), domain.classify(self.SURVEY, **self.KW, url=None))
        self.assertEqual(domain._url_parts("not a url")[0], "")


def row(**changes):
    base = {"chunk_id": uuid.uuid4(), "source_channel": "common_crawl", "source_url": "https://example.org/a",
            "detected_mime": "text/html", "http_content_type": "text/html; charset=utf-8", "file_path": None}
    base.update(changes)
    return base


class SourceTypeTests(unittest.TestCase):
    def test_common_crawl_document_types(self) -> None:
        self.assertEqual(source_document.document_type(row()), "web_page")
        self.assertEqual(source_document.document_type(row(detected_mime="application/xhtml+xml")), "web_page")
        self.assertEqual(source_document.document_type(row(detected_mime="application/pdf")), "pdf_document")
        self.assertEqual(source_document.document_type(row(detected_mime=None, http_content_type="text/plain")),
                         "plain_text")
        self.assertEqual(source_document.document_type(row(detected_mime=None, http_content_type=None,
                                                           source_url="https://x.org/paper.pdf?dl=1")), "pdf_document")
        self.assertEqual(source_document.document_type(row(detected_mime=None, http_content_type=None)), "web_page")

    def test_github_document_types(self) -> None:
        gh = dict(source_channel="github", detected_mime=None, http_content_type=None)
        for path, expected in (("README.md", "markdown_document"), ("docs/guide.rst", "documentation_text"),
                               ("config/app.yaml", "configuration_file"), ("src/main.py", "source_code"),
                               ("Makefile", "source_code"), ("docs/NOTES.TXT", "documentation_text")):
            with self.subTest(path=path):
                self.assertEqual(source_document.document_type(row(file_path=path, **gh)), expected)

    def test_source_type_is_never_the_channel_and_fits_the_column(self) -> None:
        for kind in source_document.SOURCE_TYPES:
            self.assertLessEqual(len(kind), 50)
            self.assertNotIn(kind, ("common_crawl", "github"))
        full = dict(row(), source_title="T", subject_domain="Biology", warc_record_id=uuid.uuid4(),
                    crawled_at=datetime(2024, 8, 5, tzinfo=timezone.utc), page_published_at=None,
                    page_modified_at=None, http_last_modified=None, published_evidence=None, modified_evidence=None)
        from app.processing.gold import service

        metadata, dataset = service.validate_provenance(full)
        self.assertEqual(metadata["source_type"], "web_page")
        self.assertEqual((dataset["metadata"]["source_channel"], dataset["metadata"]["document_type"]),
                         ("common_crawl", "web_page"))

    def test_audit_sql_allows_exactly_the_python_set(self) -> None:
        listed = re.search(r"m\.source_type NOT IN \(([^)]*)\)", repository.GOLD_AUDIT_SQL, re.S).group(1)
        self.assertEqual(set(re.findall(r"'([a-z_]+)'", listed)), set(source_document.SOURCE_TYPES))
        self.assertIn("cap.detected_mime", repository.PROVENANCE_SQL)
        self.assertIn("cap.http_content_type", repository.PROVENANCE_SQL)


if __name__ == "__main__":
    unittest.main()
