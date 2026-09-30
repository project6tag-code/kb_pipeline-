"""Chunking V1 chunker: structure-aware boundaries, sizes, offsets, heading paths, acceptance."""

import json
import unittest

from app.processing.chunking import tokenizer
from app.processing.chunking.chunker import ChunkingConfig, ChunkingLimit, chunk_document
from app.processing.chunking.structure import parse_blocks
from app.processing.normalization.text import canonicalize_text

CONFIG = ChunkingConfig()


def sentence(i: int, topic: str = "glaciers") -> str:
    return f"Sentence {i} explains one measured fact about {topic} and the ice that forms them over many years."


def section(title: str, sentences: int, level: int = 2, topic: str = "glaciers") -> str:
    return f"{'#' * level} {title}\n\n" + " ".join(sentence(i, topic) for i in range(sentences))


def chunks(doc: str, kind: str = "prose", config: ChunkingConfig = CONFIG):
    doc = canonicalize_text(doc)
    return doc, chunk_document(doc, kind=kind, config=config)


def accepted(result):
    return [c for c in result if c.rejected is None]


class InvariantMixin:
    def assertInvariants(self, doc: str, result, config: ChunkingConfig = CONFIG) -> None:
        last_end = 0
        for c in result:
            self.assertEqual(canonicalize_text(doc[c.char_start:c.char_end]), c.chunk_text)  # schema §5.15
            self.assertEqual(doc[c.char_start:c.char_end], c.chunk_text)  # spans are already canonical
            self.assertLess(c.char_start, c.char_end)
            self.assertGreaterEqual(c.char_start, last_end)  # ordered, non-overlapping (no overlap in V1)
            last_end = c.char_end
            self.assertLessEqual(c.token_count, config.max_tokens)
            self.assertEqual(c.token_count, tokenizer.count_tokens(c.chunk_text))
            self.assertTrue(c.chunk_text.strip())
        self.assertEqual([c.ordinal for c in result], list(range(len(result))))


class StructureTests(InvariantMixin, unittest.TestCase):
    def test_deterministic_and_version_name_is_stable(self) -> None:
        doc = "# Glaciers\n\n" + "\n\n".join(section(f"Part {i}", 10) for i in range(5))
        first, second = chunks(doc)[1], chunks(doc)[1]
        self.assertEqual([(c.char_start, c.char_end, c.chunk_text) for c in first],
                         [(c.char_start, c.char_end, c.chunk_text) for c in second])
        self.assertEqual(ChunkingConfig().name, ChunkingConfig().name)
        self.assertNotEqual(ChunkingConfig(target_tokens=300).name, ChunkingConfig().name)
        self.assertRegex(CONFIG.name, r"^kbp-chunker-c1-[0-9a-f]{10}$")

    def test_headings_start_chunks_and_heading_paths_are_recorded(self) -> None:
        doc, result = chunks("# Glaciers\n\n" + section("Formation", 10) + "\n\n" + section("Movement", 10)
                             + "\n\n" + section("Basal sliding", 8, level=3))
        self.assertInvariants(doc, result)
        texts = [c.chunk_text for c in result]
        self.assertTrue(texts[0].startswith("# Glaciers\n\n## Formation"))  # tiny title merges forward
        self.assertTrue(texts[1].startswith("## Movement"))
        self.assertEqual(json.loads(result[0].section_path), {"headings": ["Glaciers", "Formation"]})
        self.assertEqual(json.loads(result[1].section_path)["headings"], ["Glaciers", "Movement"])
        last = json.loads(result[-1].section_path)["headings"]
        self.assertIn(last, (["Glaciers", "Movement"], ["Glaciers", "Movement", "Basal sliding"]))
        self.assertFalse(any(c.chunk_text.split("\n")[-1].startswith("#") for c in result))  # no trailing heading

    def test_small_sections_merge_but_not_across_a_higher_level_heading(self) -> None:
        doc, result = chunks("# Ice\n\n## A\n\nShort fact one about ice.\n\n## B\n\nShort fact two about ice.\n\n"
                             "# Rivers\n\n" + section("Flow", 12, topic="rivers"))
        self.assertInvariants(doc, result)
        self.assertTrue(result[0].chunk_text.startswith("# Ice\n\n## A") and "## B" in result[0].chunk_text)
        self.assertFalse(any("# Ice" in c.chunk_text and "# Rivers" in c.chunk_text for c in result))

    def test_oversized_paragraph_splits_at_sentence_boundaries(self) -> None:
        doc, result = chunks("## Long\n\n" + " ".join(sentence(i) for i in range(80)))
        self.assertInvariants(doc, result)
        self.assertGreater(len(result), 2)
        for c in result:
            self.assertTrue(c.chunk_text.endswith("years."), c.chunk_text[-40:])  # never mid-sentence
        self.assertTrue(all(c.token_count >= CONFIG.min_tokens for c in result[:-1]))

    def test_giant_sentence_and_giant_word_fall_back_safely(self) -> None:
        run_on = "The list includes " + ", ".join(f"item number {i}" for i in range(400)) + "."
        doc, result = chunks("## Run-on\n\n" + run_on + "\n\n" + "x" * 20_000)
        self.assertInvariants(doc, result)
        self.assertTrue(all(c.token_count <= CONFIG.max_tokens for c in result))

    def test_lists_stay_whole_or_split_between_items(self) -> None:
        small = "## Steps\n\n" + "\n".join(f"{i}. Step {i} of the procedure moves ice forward." for i in range(1, 6))
        doc, result = chunks(small + "\n\n" + " ".join(sentence(i) for i in range(6)))
        self.assertInvariants(doc, result)
        self.assertEqual(sum(1 for c in result if "1. Step 1" in c.chunk_text and "5. Step 5" in c.chunk_text), 1)
        big = "## Glossary\n\n" + "\n".join(f"- Term {i}: a moderately long definition of glacial feature number "
                                             f"{i} used in the field." for i in range(120))
        doc, result = chunks(big)
        self.assertInvariants(doc, result)
        self.assertGreater(len(result), 1)
        for c in result:
            self.assertTrue(c.chunk_text.split("\n")[-1].endswith("field."))  # whole items only
            body = [line for line in c.chunk_text.split("\n") if line and not line.startswith("#")]
            self.assertTrue(all(line.startswith("- Term") for line in body))

    def test_tables_keep_headers_with_row_groups(self) -> None:
        rows = "\n".join(f"Glacier {i} | {1000 + i} km2 | Retreating since {1900 + i}" for i in range(150))
        doc, result = chunks("## Glaciers by area\n\nName | Area | Status\n" + rows)
        self.assertInvariants(doc, result)
        self.assertGreater(len(result), 1)
        self.assertTrue(result[0].chunk_text.endswith(result[0].chunk_text.split("\n")[-1]))
        self.assertIn("Name | Area | Status", result[0].chunk_text)
        for c in result[1:]:
            context = json.loads(c.section_path)
            self.assertEqual(context["table_header"], "Name | Area | Status")  # header meaning is kept
            self.assertTrue(all(" | " in line for line in c.chunk_text.split("\n")))
        small_doc, small = chunks("## Inputs\n\nInput | Output\nWater | Oxygen\nCarbon dioxide | Glucose\n\n"
                                  + " ".join(sentence(i) for i in range(5)))
        self.assertIn("Input | Output\nWater | Oxygen\nCarbon dioxide | Glucose", small[0].chunk_text)

    def test_code_prefers_definition_boundaries_and_never_splits_lines(self) -> None:
        functions = "\n\n".join(
            f"def function_{i}(value):\n \"\"\"Return value scaled by {i}.\"\"\"\n result = value * {i}\n"
            + "\n".join(f" result = result + {j} # step {j} of the computation" for j in range(12))
            + "\n return result" for i in range(20))
        doc, result = chunks(functions, kind="code")
        self.assertInvariants(doc, result)
        self.assertGreater(len(result), 1)
        for c in result:
            self.assertTrue(c.chunk_text.startswith("def function_"), c.chunk_text[:30])
            lines = set(doc.split("\n"))
            self.assertTrue(all(line in lines for line in c.chunk_text.split("\n")))  # whole lines only
        self.assertIsNone(result[0].section_path)  # '#' in code is not a heading
        fenced_doc, fenced = chunks("## Example\n\nRun the sample below to compute checksums of large files.\n\n"
                                    "```\nimport hashlib\n\ndigest = hashlib.sha256()\n```")
        self.assertIn("```\nimport hashlib\n\ndigest = hashlib.sha256()\n```", fenced[0].chunk_text)

    def test_blocks_parse_fences_with_blank_lines(self) -> None:
        kinds = [b.kind for b in parse_blocks("# T\n\ntext\n\n```\na\n\nb\n```\n\n- x\n- y\n\nA | B\n1 | 2")]
        self.assertEqual(kinds, ["heading", "paragraph", "fence", "list", "table"])


class AcceptanceAndBoundsTests(InvariantMixin, unittest.TestCase):
    def test_useless_fragments_are_rejected_with_reasons_and_keep_ordinal_gaps(self) -> None:
        doc = ("# Glaciers\n\n" + section("Formation", 12) + "\n\n# Appendix\n\n" + "*** --- ***\n\n"
               + "# Credits\n\nAll rights reserved.")
        doc, result = chunks(doc)
        self.assertInvariants(doc, result)
        reasons = {c.rejected for c in result if c.rejected}
        self.assertTrue(reasons & {"no_alphanumeric", "tiny_fragment", "boilerplate_residue"}, reasons)
        self.assertEqual([c.ordinal for c in result], list(range(len(result))))
        self.assertTrue(accepted(result))

    def test_a_short_document_is_one_chunk(self) -> None:
        doc, result = chunks("Water boils at 100 degrees Celsius at sea level.")
        self.assertEqual([(c.chunk_text, c.rejected) for c in result],
                         [("Water boils at 100 degrees Celsius at sea level.", None)])

    def test_nul_oversize_and_chunk_count_guards(self) -> None:
        with self.assertRaises(ChunkingLimit) as ctx:
            chunk_document("text\x00more", config=CONFIG)
        self.assertEqual(ctx.exception.code, "document_contains_nul")
        with self.assertRaises(ChunkingLimit) as ctx:
            chunk_document("a" * 50, config=ChunkingConfig(max_document_chars=10))
        self.assertEqual(ctx.exception.code, "document_too_large")
        pathological = "\n\n".join(f"# H{i}\n\n" + " ".join(sentence(j) for j in range(8)) for i in range(50))
        with self.assertRaises(ChunkingLimit) as ctx:
            chunk_document(pathological, config=ChunkingConfig(max_chunks_per_document=10))
        self.assertEqual(ctx.exception.code, "chunk_count_limit")

    def test_config_rejects_overlap_and_inconsistent_sizes(self) -> None:
        with self.assertRaises(ValueError):
            ChunkingConfig(overlap_tokens=20).validate()
        with self.assertRaises(ValueError):
            ChunkingConfig(min_tokens=500).validate()


class TokenizerTests(unittest.TestCase):
    def test_counts_and_boundaries(self) -> None:
        self.assertEqual(tokenizer.count_tokens("the photosynthesis 2024, ok!"), 1 + 4 + 2 + 1 + 1 + 1)
        text = " ".join(["word"] * 100)
        cuts = tokenizer.token_boundaries(text, 30)
        pieces = [text[a:b] for a, b in zip([0, *cuts], [*cuts, len(text)])]
        self.assertTrue(all(tokenizer.count_tokens(p) <= 30 for p in pieces))
        self.assertEqual("".join(pieces), text)


if __name__ == "__main__":
    unittest.main()
