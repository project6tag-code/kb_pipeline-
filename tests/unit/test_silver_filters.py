"""Silver gates 13-18 and 21: language, privacy, secrets, noise, quality, suitability, domain."""

import logging
import unittest

from app.processing.classification import domain
from app.processing.factuality import classifier as suitability
from app.processing.language.detector import Decision, detect
from app.processing.privacy import pii, secrets
from app.processing.quality import scorer, spam

EN = ("Photosynthesis is the process by which green plants convert light energy into chemical energy. It takes "
      "place mainly in the chloroplasts of leaf cells, where chlorophyll absorbs light. The light-dependent "
      "reactions produce ATP and NADPH, which are then used by the Calvin cycle to fix carbon dioxide into sugars.")
FR = ("La photosynthèse est le processus par lequel les plantes vertes convertissent l'énergie lumineuse en "
      "énergie chimique. Elle se déroule principalement dans les chloroplastes des cellules des feuilles, où la "
      "chlorophylle absorbe la lumière et produit de l'oxygène pour les autres organismes.")
DE = ("Die Photosynthese ist der Prozess, bei dem grüne Pflanzen Lichtenergie in chemische Energie umwandeln. "
      "Sie findet hauptsächlich in den Chloroplasten der Blattzellen statt, wo das Chlorophyll das Licht absorbiert "
      "und Sauerstoff für die Atmosphäre freigesetzt wird.")

REFERENCE = """# Binary search

Binary search is an algorithm that finds the position of a target value within a sorted array. It compares the
target value to the middle element of the array. If they are not equal, the half in which the target cannot lie is
eliminated and the search continues on the remaining half. The algorithm runs in logarithmic time, making O(log n)
comparisons, where n is the number of elements in the array.

## Procedure

1. Set the lower bound to 0 and the upper bound to n - 1.
2. If the lower bound exceeds the upper bound, the search terminates as unsuccessful.
3. Set the middle position to the floor of the mean of the bounds and compare the element at that position.

Binary search is used to implement lookups in sorted arrays and database indexes. It was first described in 1946,
and a correct version for arrays of every length was published in 1960 [1]. The method requires random access to
the elements, so it is not used on linked lists."""


def lang(text: str):
    return detect(text, min_tokens=25, min_confidence=0.8, mixed_fraction=0.2)


class LanguageTests(unittest.TestCase):
    def test_english_non_english_mixed_and_short(self) -> None:
        english = lang(EN)
        self.assertEqual((english.decision, english.language), (Decision.ENGLISH, "en"))
        self.assertGreaterEqual(english.confidence, 0.8)
        self.assertEqual(lang(FR).decision, Decision.NON_ENGLISH)
        self.assertEqual(lang(FR).language, "fr")
        self.assertEqual(lang(DE).language, "de")
        self.assertEqual(lang("Фотосинтез — это процесс преобразования энергии света " * 6).decision,
                         Decision.NON_ENGLISH)
        self.assertEqual(lang("\n\n".join([EN, FR, DE])).decision, Decision.MIXED)
        short = lang("Water boils at one hundred degrees.")
        self.assertEqual((short.decision, short.reasons[-1]), (Decision.UNRESOLVED, "too_short"))

    def test_common_crawl_language_hint_is_metadata_only(self) -> None:
        result = detect(EN, min_tokens=25, min_confidence=0.8, mixed_fraction=0.2, hint="fra")
        self.assertEqual(result.decision, Decision.ENGLISH)
        self.assertIn("cc_hint_not_eng", result.reasons)


class PrivacyTests(unittest.TestCase):
    def test_public_business_contacts_and_names_are_allowed(self) -> None:
        text = ("Marie Curie won the Nobel Prize twice. For press enquiries contact press@nobelprize.org or "
                "info@example.com. The museum is at 11 Rue Pierre Street and serves 192.0.2.1 users.")
        result = pii.analyze(text, quarantine_count=25)
        self.assertEqual(result.decision, pii.ALLOW)
        self.assertEqual(result.text, text)
        self.assertIn("Marie Curie", result.text)

    def test_personal_contact_is_redacted_deterministically(self) -> None:
        text = "Contact the maintainer John Smith at john.smith@gmail.com or call (415) 555-2671 for details."
        first, second = (pii.analyze(text, quarantine_count=25) for _ in range(2))
        self.assertEqual(first.decision, pii.REDACT)
        self.assertEqual(first.text, second.text)
        self.assertEqual(first.text, "Contact the maintainer John Smith at [REDACTED_PERSONAL_EMAIL] or call "
                                     "[REDACTED_PHONE] for details.")
        self.assertEqual(first.categories, {"personal_email": 1, "phone": 1})

    def test_sensitive_identifiers_are_rejected_and_test_cards_allowed(self) -> None:
        self.assertEqual(pii.analyze("Customer SSN: 123-45-6789 on file.", quarantine_count=25).decision,
                         pii.REJECT)
        card = pii.analyze("Card 4556 7375 8689 9855 was charged.", quarantine_count=25)
        self.assertEqual((card.decision, card.categories), (pii.REJECT, {"card_number": 1}))
        self.assertNotIn("4556", "".join(card.reasons))
        test_card = pii.analyze("Use the Stripe test card 4242 4242 4242 4242 in sandbox mode.", quarantine_count=25)
        self.assertEqual(test_card.decision, pii.ALLOW)
        dump = " ".join(f"user{i}.name@gmail.com" for i in range(30))
        self.assertEqual(pii.analyze(dump, quarantine_count=25).decision, pii.QUARANTINE)


class SecretTests(unittest.TestCase):
    KEY = ("-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA3Tz2mr7SZiAMfQyuvBjM9Oi8Ev1pF4kx7sUHnP0zQWc5r+Vd\n"
           "b8qJXyL1eT0NoWmZ2hG4f6RkP9sYcA3uVxE7tBnQ1wLjHd5KiO8gUzMpRaS2vFyC\n-----END RSA PRIVATE KEY-----")
    GITHUB = "ghp_" + "Zx8Kq2Lm9Pw4Rt6Yv1Bn3Cd5Fg7Hj0Aa2Ss4"

    def test_high_confidence_secrets_quarantine_without_leaking_values(self) -> None:
        for text, category in ((self.KEY, "private_key"), (f"token = {self.GITHUB}", "github_token"),
                               ("aws_access_key_id = AKIAZ7QXJ4LMNP2RT6VW", "aws_access_key"),
                               ("DATABASE_URL=postgres://app:S3cr3tPw0rd@db.internal:5432/prod",
                                "connection_string")):
            result = secrets.scan(text)
            self.assertEqual(result.decision, secrets.QUARANTINE, category)
            self.assertEqual(result.categories, {category: 1})
            rendered = repr(result)
            for fragment in ("MIIEpAIBAAKCAQEA", self.GITHUB, "AKIAZ7QXJ4LMNP2RT6VW", "S3cr3tPw0rd"):
                self.assertNotIn(fragment, rendered)
        fp = secrets.scan(f"key: {self.GITHUB}").findings[0].fingerprint
        self.assertRegex(fp, r"^sha256:[0-9a-f]{16}$")

    def test_medium_confidence_and_documented_examples(self) -> None:
        self.assertEqual(secrets.scan('password = "Tr0ub4dor&3xYz!q"').decision, secrets.REJECT)
        for benign in ("aws_access_key_id = AKIAIOSFODNN7EXAMPLE", 'password = "changeme"',
                       'api_key: "${API_KEY}"', 'token = "<your-token-here>"'):
            self.assertEqual(secrets.scan(benign).decision, secrets.ALLOW, benign)


def feats(text):
    return scorer.features(text)


def noise(text, **kw):
    return spam.check(text, feats(text), kind=kw.pop("kind", "prose"), min_words=40, **kw)


class NoiseAndQualityTests(unittest.TestCase):
    def test_good_factual_document_passes(self) -> None:
        self.assertTrue(noise(REFERENCE).passed, noise(REFERENCE).reasons)
        quality = scorer.score(feats(REFERENCE), kind="prose", threshold=0.55)
        self.assertTrue(quality.passed, quality)

    def test_spam_error_template_and_noise_pages_fail_with_reasons(self) -> None:
        filler = " The page you requested may have been moved or deleted, please check the address."
        cases = {
            "soft_404_or_error_page": "# 404 Page not found\n\nSorry, this page does not exist." + filler * 4,
            "navigation_only_list": "\n".join(f"- {w}" for w in ("Home", "About", "Blog", "Shop", "Contact",
                                                                  "Careers", "Press", "Help") * 6),
            "excessive_repetition": "\n".join(["Buy the best shoes in town today."] * 30),
            "gibberish": " ".join(["xkqzv plmrtw bnvcx qwrtp zxcvb mnbvc lkjhg fdsaq"] * 10),
            "advertisement_only": ("Huge sale! Buy now and get 50% off. Free shipping on every order, shop now. "
                                   "Best price guaranteed, add to cart today with coupon SAVE50. ") * 4,
            "keyword_stuffing": "cheap flights " * 60 + "and more cheap flights to cheap destinations " * 5,
            "cookie_consent_only": ("We use cookies and similar tracking technologies. Accept or reject cookies in "
                                    "your consent preferences. Third-party cookies help us. ") * 3,
            "placeholder": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor. " * 6,
        }
        for reason, text in cases.items():
            result = noise(text)
            self.assertFalse(result.passed, reason)
            self.assertIn(reason, result.reasons, (reason, result.reasons))
        self.assertIn("navigation_only", noise(REFERENCE, link_ratio=0.8).reasons)

    def test_low_quality_text_scores_below_threshold(self) -> None:
        choppy = "\n".join(f"item {i} ok" for i in range(60))
        quality = scorer.score(feats(choppy), kind="prose", threshold=0.55)
        self.assertFalse(quality.passed)
        self.assertIn("low_structure", quality.reasons)


class SuitabilityTests(unittest.TestCase):
    def classify(self, text, kind="prose", prose_words=0):
        return suitability.classify(text, feats(text), kind=kind, pass_threshold=0.55, fail_threshold=0.35,
                                    prose_words=prose_words)

    def test_reference_and_technical_content_passes(self) -> None:
        result = self.classify(REFERENCE)
        self.assertEqual(result.decision, suitability.PASS, result)
        self.assertIn("has_definition", result.reasons)
        technical = self.classify("Quicksort runs in O(n log n) time on average and in quadratic time in the worst "
                                  "case. The pivot choice determines the balance of the partitions. " * 3)
        self.assertEqual((technical.decision, technical.signals["personal_share"]), (suitability.PASS, 0.0))

    def test_conversational_noise_fails(self) -> None:
        chatter = ("lol I think this is the best ever!!! I love it so much, omg. Thanks everyone, hey guys reply "
                   "below and follow me @sam #blessed. I'm so happy I bought it, amazing!! ") * 5
        result = self.classify(chatter)
        self.assertEqual(result.decision, suitability.FAIL, result)
        self.assertIn("high_opinion", result.reasons)

    def test_uncertain_middle_is_unresolved(self) -> None:
        mixed = ("The station opened in 1998 and has two platforms. I think the new café is nice and I love the "
                 "coffee there. Trains run every 15 min to the city centre. What do you think? I visited last "
                 "week with my friend and we had a great time. ") * 3
        result = self.classify(mixed)
        self.assertEqual(result.decision, suitability.UNRESOLVED, result)


class DomainTests(unittest.TestCase):
    QUANTUM = ("# Quantum superposition\n\nIn quantum mechanics, superposition is the principle that a quantum "
               "system can exist in several states at once. The wave function of a particle, a solution of the "
               "Schrödinger equation, describes this superposition. A qubit uses superposition and entanglement. "
               "Measurement collapses the wave function into an eigenstate of the Hamiltonian, as described by "
               "the uncertainty principle and Planck's constant.")

    def classify(self, text):
        return domain.classify(text, min_evidence=4.0, min_confidence=0.45, multi_ratio=0.8)

    def test_clear_domain_and_subdomain(self) -> None:
        result = self.classify(self.QUANTUM)
        self.assertEqual((result.decision, result.label), (domain.ASSIGNED, "Physics/Quantum Mechanics"))
        accounting = self.classify("# Depreciation\n\nDepreciation in accounting allocates the cost of an asset "
                                   "over its useful life. The balance sheet reports the asset net of accumulated "
                                   "depreciation, and the income statement records depreciation as an expense. "
                                   "Auditors check the ledger for each liability and asset.")
        self.assertEqual(accounting.label, "Finance/Accounting")

    def test_ambiguous_document_is_unknown(self) -> None:
        result = self.classify("The meeting starts at nine. Please bring the documents and a pen. We will "
                               "discuss the plan and then take a short break before lunch.")
        self.assertEqual((result.decision, result.label), (domain.UNKNOWN, None))

    AI = ("A neural network is a machine learning model trained on a dataset with gradient descent; deep "
          "learning uses transformer models for classification and reinforcement learning. ")

    def test_multi_domain_is_explicit(self) -> None:
        # Comparable evidence (second >= multi_ratio x top): primary kept, secondary recorded, no subdomain.
        result = self.classify(self.QUANTUM + "\n\n" + self.AI)
        self.assertEqual((result.decision, result.label), (domain.MULTI, "Physics"))
        self.assertIn("multi_domain:Computer Science", result.reasons)
        self.assertIsNone(result.subdomain)
        # A clearly dominant domain is assigned, not reported as multi-domain.
        dominant = self.classify(self.QUANTUM + "\n\n" + self.AI * 3)
        self.assertEqual((dominant.decision, dominant.domain), (domain.ASSIGNED, "Computer Science"))

    def test_taxonomy_is_configurable_data(self) -> None:
        taxonomy = domain.load_taxonomy()
        self.assertGreaterEqual(len(taxonomy.domains), 20)
        self.assertRegex(taxonomy.digest, r"^[0-9a-f]{10}$")


class NoSecretInLogsTests(unittest.TestCase):
    def test_processing_logs_never_contain_secret_values(self) -> None:
        from tests.unit.test_silver_processor import ProcessorHarness

        harness = ProcessorHarness(self)
        secret = SecretTests.GITHUB
        ref = harness.gh_doc("deploy/notes.md", REFERENCE + f"\n\nThe CI token is {secret} for the deploy job.")
        with self.assertLogs(level=logging.DEBUG) as logs:
            logging.getLogger("app").debug("start")
            counts = harness.run([ref])
        self.assertEqual(counts["silver_quarantined"], 1)
        joined = "\n".join(logs.output) + repr(harness.store.dead_letters) + repr(harness.store.documents)
        self.assertNotIn(secret, joined)
        self.assertNotIn(secret[4:20], joined)
        doc = harness.store.document_for(ref, harness.processor.extraction_version)
        self.assertIsNone(doc["canonical_text"])  # sensitive text is never stored


if __name__ == "__main__":
    unittest.main()
