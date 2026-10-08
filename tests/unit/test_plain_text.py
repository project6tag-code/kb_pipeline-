"""Emoji / symbol cleanup (sym1), plain-text chunk rendering (plain1) and the title-aware fact guard (fg-v2)."""

import unittest

from app.processing.chunk_dedup import facts
from app.processing.chunking import plain
from app.processing.chunking.chunker import chunk_document
from app.processing.normalization import symbols
from app.processing.normalization.text import canonicalize_text


class SymbolCleanupTests(unittest.TestCase):
    def test_emojis_pictographs_and_decoration_are_removed(self) -> None:
        cases = {
            "Oxygen 🔥 is vital ✅": "Oxygen is vital",
            "Rated ★★★★☆ by users 👍🏽": "Rated by users",
            "Flag 🇺🇸 and keycap 1️⃣ done": "Flag and keycap 1 done",
            "Icon  font glyph": "Icon font glyph",
            "Learn more ► now ➤": "Learn more now",
            "Brand™ and Mark®": "Brand and Mark",
            "zero​width soft­hyphen": "zerowidth softhyphen",
            "Name: ________": "Name:",
            "Wait.......": "Wait",
            "Trending #science #climate_change": "Trending science climate_change",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(canonicalize_text(symbols.clean(raw)), expected)

    def test_meaningful_symbols_are_kept(self) -> None:
        text = ("It costs US $7.99 (50% off), C# and F# are languages, the #1 hit, snake_case_name, "
                "25 °C, 5 N·m, x → y, a ≤ b ± c, page#anchor, a+b=c, 1/2, 3.14..., R&D, user@example")
        self.assertEqual(symbols.clean(text), text)

    def test_structure_lines_and_code_are_kept(self) -> None:
        text = "## Heading 🔥\n\n- item ✅\n  - nested\n1. step\nA | B\n```\nx = 1  # ⭐ comment\n=====\n```"
        cleaned = symbols.clean(text)
        self.assertEqual(cleaned, "## Heading\n\n- item\n  - nested\n1. step\nA | B\n```\nx = 1  # ⭐ comment\n=====\n```")

    def test_bullets_symbol_lines_and_idempotence(self) -> None:
        text = "• first\n• second\nA • B • C\n====\n* * *\n---\nEnd."
        cleaned = symbols.clean(text)
        self.assertEqual(cleaned, "- first\n- second\nA B C\nEnd.")
        self.assertEqual(symbols.clean(cleaned), cleaned)

    def test_heading_marker_is_not_a_hashtag(self) -> None:
        self.assertEqual(symbols.clean("# Oxygen #element"), "# Oxygen element")

    def test_footnote_debris_is_removed_but_math_and_list_markers_stay(self) -> None:
        # Real Wikipedia chunks (CC-MAIN-2024-33) carried "^", "*" and "‡" footnote marks.
        cases = {
            "^ a b Jarre interview.": "a b Jarre interview.",
            "Released 1976* in France.": "Released 1976 in France.",
            "Gold ‡ certified † note": "Gold certified note",
            "Canada | Platinum | 100,000^": "Canada | Platinum | 100,000",  # certification-table footnote
            "(*) designates unordered lists.": "designates unordered lists.",
            "Compute 2*3 and x^2 and a * b.": "Compute 2*3 and x^2 and a b.",
            "* item one\n  * nested item": "* item one\n * nested item",  # canonical text: one-space indent
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(canonicalize_text(symbols.clean(raw)), expected)


class PlainRenderTests(unittest.TestCase):
    def test_markdown_structure_becomes_plain_text(self) -> None:
        span = ("## Physical properties\n\nOxygen is a **chemical element** with symbol _O_ and "
                "[atomic number](https://example.org/z) 8.\n\n- Colourless gas\n  - Odourless\n1. First step\n"
                "> A quoted line\nMelting point | 54.36 K\nElement | Symbol | Number\n| --- | --- |\n"
                "![Liquid oxygen](lox.png)\nUse `pip install` now.")
        self.assertEqual(canonicalize_text(plain.render(span)),
                         "Physical properties\n\nOxygen is a chemical element with symbol O and atomic number 8.\n\n"
                         "Colourless gas\nOdourless\n1. First step\nA quoted line\nMelting point: 54.36 K\n"
                         "Element; Symbol; Number\nLiquid oxygen\nUse pip install now.")

    def test_citation_brackets_are_removed_from_chunk_text(self) -> None:
        span = ("Oxygène was released in 1976.[5] It sold well[a][citation needed] and was remastered.[note 2] "
                "Arrays use x[0] and [Edit] links[edit] vanish.")
        self.assertEqual(plain.render(span), "Oxygène was released in 1976. It sold well and was remastered. "
                                             "Arrays use x[0] and [Edit] links vanish.")

    def test_code_inside_fences_is_verbatim_and_fences_are_dropped(self) -> None:
        span = "Intro.\n\n```\n## not a heading\nx = a * b * c  # keep\n- not a list\n```"
        self.assertEqual(plain.render(span), "Intro.\n\n## not a heading\nx = a * b * c  # keep\n- not a list")

    def test_code_documents_are_unchanged(self) -> None:
        source = "# comment\ndef f():\n    return a | b\n"
        self.assertEqual(plain.render(source, "code"), source)

    def test_arithmetic_and_identifiers_are_not_emphasis(self) -> None:
        self.assertEqual(plain.render("2*3*4 and snake_case_name and a * b"), "2*3*4 and snake_case_name and a * b")

    def test_chunks_carry_no_markers(self) -> None:
        doc = canonicalize_text("# Glaciers\n\n## Formation\n\n" + "Ice forms from snow over many years. " * 30
                                + "\n\n- Accumulation zone\n- Ablation zone\n\nArea | Size\nLambert | 1 km2")
        for chunk in chunk_document(doc):
            for line in chunk.chunk_text.split("\n"):
                self.assertFalse(line.startswith(("#", "- ", "> ", "```", "|")), line)
                self.assertNotIn(" | ", line)


class TitleAwareFactGuardTests(unittest.TestCase):
    BODY = "The dam was completed in 1959 on the Zambezi River and holds 180 cubic kilometres of water."

    def test_plain_titles_are_context_not_facts(self) -> None:
        a = facts.fact_signature(f"Kariba Dam\n\n{self.BODY}")
        b = facts.fact_signature(f"Zambezi scheme\n\n{self.BODY}")
        self.assertTrue(a.compatible(b))

    def test_lines_with_numbers_or_sentences_are_never_skipped(self) -> None:
        with_year = facts.fact_signature(f"Built 1959\n\n{self.BODY}")
        other_year = facts.fact_signature(f"Built 1961\n\n{self.BODY}")
        self.assertFalse(with_year.compatible(other_year))
        sentence_a = facts.fact_signature(f"Lake Kariba lies upstream.\n\n{self.BODY}")
        sentence_b = facts.fact_signature(f"Lake Victoria lies upstream.\n\n{self.BODY}")
        self.assertFalse(sentence_a.compatible(sentence_b))

    def test_list_items_inside_a_list_are_content(self) -> None:
        a = facts.fact_signature(f"{self.BODY}\nKariba Dam\nCahora Bassa")
        b = facts.fact_signature(f"{self.BODY}\nKariba Dam\nHoover Dam")
        self.assertFalse(a.compatible(b))


if __name__ == "__main__":
    unittest.main()
