"""Unit tests for app.processing.normalization.text."""

import random
import unicodedata
import unittest

from app.processing.normalization.text import canonicalize_text


class CanonicalizeTextTests(unittest.TestCase):
    def test_crlf_cr_lf_equivalent(self) -> None:
        lf = "line one\nline two\n\nline three"
        self.assertEqual(canonicalize_text("line one\r\nline two\r\n\r\nline three"), lf)
        self.assertEqual(canonicalize_text("line one\rline two\r\rline three"), lf)
        self.assertEqual(canonicalize_text(lf), lf)

    def test_horizontal_whitespace_collapsed(self) -> None:
        self.assertEqual(canonicalize_text("a  b\t\tc \t d"), "a b c d")
        self.assertEqual(canonicalize_text("x\n   indented\ty"), "x\n indented y")

    def test_trailing_whitespace_removed_per_line(self) -> None:
        self.assertEqual(canonicalize_text("a   \nb\t\nc"), "a\nb\nc")

    def test_nbsp_replaced(self) -> None:
        self.assertEqual(canonicalize_text("100 km"), "100 km")
        self.assertEqual(canonicalize_text("a   b"), "a b")

    def test_leading_bom_removed(self) -> None:
        self.assertEqual(canonicalize_text("﻿Hello"), "Hello")
        self.assertEqual(canonicalize_text("﻿﻿Hello"), "Hello")
        self.assertEqual(canonicalize_text("﻿\r\n  Hello"), "Hello")

    def test_leading_whitespace_then_bom_removed(self) -> None:
        for text in ("  ﻿Hello", "\t\n﻿Hello", " ﻿ ﻿Hello", "\r\n﻿\r\nHello"):
            with self.subTest(text=text):
                once = canonicalize_text(text)
                self.assertEqual(once, "Hello")
                self.assertEqual(canonicalize_text(once), once)

    def test_embedded_bom_preserved(self) -> None:
        cases = {
            "Hel﻿lo": "Hel﻿lo",
            "Hello﻿": "Hello﻿",
            "a ﻿ b": "a ﻿ b",
            "line one\n﻿line two": "line one\n﻿line two",
            "﻿a﻿b": "a﻿b",
            "  x\n\n\n﻿\n\n\ny": "x\n\n﻿\n\ny",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(canonicalize_text(text), expected)

    def test_bom_handling_repeated_canonicalization(self) -> None:
        samples = [
            "﻿Hello",
            "  ﻿Hello",
            " ﻿ ﻿ Hello﻿",
            "Hel﻿lo  \n﻿\n\n\n﻿",
            "﻿́x",
            "﻿",
            " ﻿ \t",
        ]
        for text in samples:
            with self.subTest(text=text):
                once = canonicalize_text(text)
                self.assertTrue(not once.startswith("﻿"))
                for _ in range(3):
                    self.assertEqual(canonicalize_text(once), once)

    def test_nfc(self) -> None:
        decomposed = "Café"
        composed = "Café"
        self.assertEqual(canonicalize_text(decomposed), composed)
        self.assertEqual(canonicalize_text(composed), composed)
        self.assertTrue(unicodedata.is_normalized("NFC", canonicalize_text("Å ring")))

    def test_nfc_not_nfkc(self) -> None:
        # Compatibility characters carry meaning and must survive.
        for text in ("x²", "ﬁle", "①", "Ａ"):
            with self.subTest(text=text):
                self.assertEqual(canonicalize_text(text), text)

    def test_blank_lines_capped_at_two_newlines(self) -> None:
        self.assertEqual(canonicalize_text("a\n\n\n\n\nb"), "a\n\nb")
        self.assertEqual(canonicalize_text("a\n\nb"), "a\n\nb")
        self.assertEqual(canonicalize_text("a\n \t \n  \n\nb"), "a\n\nb")

    def test_outer_whitespace_stripped(self) -> None:
        self.assertEqual(canonicalize_text("\n\n  \t hello world \n\n"), "hello world")

    def test_capitalization_preserved(self) -> None:
        text = "NASA vs Nasa vs nasa; US is not us."
        self.assertEqual(canonicalize_text(text), text)

    def test_punctuation_and_accents_preserved(self) -> None:
        text = 'Is 3.14 < 22/7? Yes! "Quoted" (paren) [br] {c}; a-b — éèñ ¿¡'
        self.assertEqual(canonicalize_text(text), text)

    def test_word_order_preserved(self) -> None:
        self.assertNotEqual(canonicalize_text("dog bites man"), canonicalize_text("man bites dog"))

    def test_empty_and_whitespace_only(self) -> None:
        self.assertEqual(canonicalize_text(""), "")
        self.assertEqual(canonicalize_text(" \t\r\n﻿ "), "")

    def test_rejects_non_str(self) -> None:
        for bad in (b"text", None, 42, 1.5, ["a"]):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                canonicalize_text(bad)  # type: ignore[arg-type]

    def test_idempotent_examples(self) -> None:
        samples = [
            "",
            "plain",
            "﻿  A  b\t\r\n\r\n\r\nC d  \n",
            "  ﻿x",
            "e\ń",
            "Café  \t ﻿́",
            "\t\n\x0c\x0b a \x0c\n\n\n b　 ",
        ]
        for text in samples:
            with self.subTest(text=text):
                once = canonicalize_text(text)
                self.assertEqual(canonicalize_text(once), once)

    def test_idempotent_randomized(self) -> None:
        alphabet = [
            "a", "B", "e", ".", "!", " ", "  ", "\t", "\n", "\r", "\r\n",
            " ", "﻿", "́", "̈", "̊", "\x0b", "\x0c",
            " ", "　", "é", "Å", "ᄀ", "ᅡ",
        ]
        rng = random.Random(20260926)
        for _ in range(5000):
            text = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 30)))
            once = canonicalize_text(text)
            self.assertEqual(canonicalize_text(once), once, msg=ascii(text))

    def test_deterministic(self) -> None:
        text = "Some  text\r\nwith\tstuff "
        self.assertEqual(canonicalize_text(text), canonicalize_text(text))


if __name__ == "__main__":
    unittest.main()
