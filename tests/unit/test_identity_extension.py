"""Unit tests for Identity Extension V1 (bridge datasets, source observations)."""

import json
import unittest
import uuid

from app.utils.ids import (
    KB_NAMESPACE,
    cc_observation_id,
    cc_observation_payload,
    github_observation_id,
    github_observation_payload,
    make_dataset_bridge_id,
)

# ---------------------------------------------------------------------------
# FROZEN REGRESSION VECTORS — Identity Extension V1 (docs/identity_extension_v1.md).
# Computed once from the implemented specification. If any assertion below
# fails, a prefix, field, validation rule or the serialization changed, which
# would re-key provenance and Gold bridge rows. Do NOT update these values;
# a change requires Identity Extension V2.
# ---------------------------------------------------------------------------
FROZEN_NAMESPACE = uuid.UUID("dd5af6b8-db26-4170-945d-557b16114d80")

BRIDGE_VECTORS = {
    "common_crawl": (
        "dataset-bridge:v1:common_crawl",
        uuid.UUID("a7d8d2e1-8400-5916-9440-b492e8e692ce"),
    ),
    "github": (
        "dataset-bridge:v1:github",
        uuid.UUID("b487f559-5f8e-56ba-9746-28658b9d09d1"),
    ),
}

CC_INPUT = (
    "CC-MAIN-2024-33",
    "crawl-data/CC-MAIN-2024-33/segments/1722640353668.0/warc/"
    "CC-MAIN-20240805230130-20240806020130-00042.warc.gz",
    123456789,
    18220,
)
CC_PAYLOAD = (
    '{"crawl_id":"CC-MAIN-2024-33",'
    '"warc_filename":"crawl-data/CC-MAIN-2024-33/segments/1722640353668.0/warc/'
    'CC-MAIN-20240805230130-20240806020130-00042.warc.gz",'
    '"warc_offset":123456789,"warc_length":18220}'
)
CC_NAME = "observation:v1:common_crawl:" + CC_PAYLOAD
CC_UUID = uuid.UUID("afb1a4e1-acd0-5e13-96e2-94eb6aec8299")

GH_SHA1_COMMIT = "0123456789abcdef0123456789abcdef01234567"
GH_SHA1_BLOB = "89abcdef0123456789abcdef0123456789abcdef"
GH_INPUT = (123456789, GH_SHA1_COMMIT, "docs/intro.md", GH_SHA1_BLOB)
GH_PAYLOAD = (
    '{"repository_id":123456789,'
    '"commit_sha":"0123456789abcdef0123456789abcdef01234567",'
    '"path":"docs/intro.md",'
    '"blob_sha":"89abcdef0123456789abcdef0123456789abcdef"}'
)
GH_NAME = "observation:v1:github:" + GH_PAYLOAD
GH_UUID = uuid.UUID("bf308fc0-77e3-5540-bcf0-5def36127e04")

GH_NON_ASCII_INPUT = (123456789, GH_SHA1_COMMIT, "docs/Café Überblick.md", GH_SHA1_BLOB)
GH_NON_ASCII_PAYLOAD = (
    '{"repository_id":123456789,'
    '"commit_sha":"0123456789abcdef0123456789abcdef01234567",'
    '"path":"docs/Caf\\u00e9 \\u00dcberblick.md",'
    '"blob_sha":"89abcdef0123456789abcdef0123456789abcdef"}'
)
GH_NON_ASCII_UUID = uuid.UUID("7ce8865a-f6d9-54d1-8933-6989729e5583")

GH_SHA256_INPUT = (987654321, "a" * 64, "src/lib.rs", "b" * 64)
GH_SHA256_PAYLOAD = (
    '{"repository_id":987654321,"commit_sha":"' + "a" * 64 + '",'
    '"path":"src/lib.rs","blob_sha":"' + "b" * 64 + '"}'
)
GH_SHA256_UUID = uuid.UUID("4119c85b-bde5-55b9-b978-194249414444")


class FrozenVectorTests(unittest.TestCase):
    def test_namespace_unchanged(self) -> None:
        self.assertEqual(KB_NAMESPACE, FROZEN_NAMESPACE)

    def test_bridge_vectors(self) -> None:
        for source, (name, expected) in BRIDGE_VECTORS.items():
            with self.subTest(source=source):
                self.assertEqual(make_dataset_bridge_id(source), expected)
                # Independent check: UUIDv5 of the literal frozen name string.
                self.assertEqual(uuid.uuid5(FROZEN_NAMESPACE, name), expected)

    def test_common_crawl_vector(self) -> None:
        self.assertEqual(cc_observation_payload(*CC_INPUT), CC_PAYLOAD)
        self.assertEqual(cc_observation_id(*CC_INPUT), CC_UUID)
        self.assertEqual(uuid.uuid5(FROZEN_NAMESPACE, CC_NAME), CC_UUID)

    def test_github_vector(self) -> None:
        self.assertEqual(github_observation_payload(*GH_INPUT), GH_PAYLOAD)
        self.assertEqual(github_observation_id(*GH_INPUT), GH_UUID)
        self.assertEqual(uuid.uuid5(FROZEN_NAMESPACE, GH_NAME), GH_UUID)

    def test_github_non_ascii_path_vector(self) -> None:
        payload = github_observation_payload(*GH_NON_ASCII_INPUT)
        self.assertEqual(payload, GH_NON_ASCII_PAYLOAD)
        self.assertTrue(payload.isascii())
        self.assertEqual(github_observation_id(*GH_NON_ASCII_INPUT), GH_NON_ASCII_UUID)
        self.assertEqual(
            uuid.uuid5(FROZEN_NAMESPACE, "observation:v1:github:" + GH_NON_ASCII_PAYLOAD),
            GH_NON_ASCII_UUID,
        )

    def test_github_sha256_object_format_vector(self) -> None:
        self.assertEqual(github_observation_payload(*GH_SHA256_INPUT), GH_SHA256_PAYLOAD)
        self.assertEqual(github_observation_id(*GH_SHA256_INPUT), GH_SHA256_UUID)
        self.assertEqual(
            uuid.uuid5(FROZEN_NAMESPACE, "observation:v1:github:" + GH_SHA256_PAYLOAD),
            GH_SHA256_UUID,
        )

    def test_all_frozen_ids_distinct(self) -> None:
        ids = [v[1] for v in BRIDGE_VECTORS.values()]
        ids += [CC_UUID, GH_UUID, GH_NON_ASCII_UUID, GH_SHA256_UUID]
        self.assertEqual(len(set(ids)), len(ids))
        for value in ids:
            self.assertEqual(value.version, 5)


class DatasetBridgeTests(unittest.TestCase):
    def test_deterministic(self) -> None:
        for source in ("common_crawl", "github"):
            with self.subTest(source=source):
                self.assertEqual(make_dataset_bridge_id(source), make_dataset_bridge_id(source))

    def test_sources_differ(self) -> None:
        self.assertNotEqual(make_dataset_bridge_id("common_crawl"), make_dataset_bridge_id("github"))

    def test_unsupported_source_rejected(self) -> None:
        for bad in ("", "commoncrawl", "Common_Crawl", "GitHub", "github ", "web", "kaggle", "cc"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                make_dataset_bridge_id(bad)

    def test_non_str_rejected(self) -> None:
        for bad in (None, 1, b"github", ["github"]):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                make_dataset_bridge_id(bad)  # type: ignore[arg-type]


class CommonCrawlObservationTests(unittest.TestCase):
    def test_repeated_calls_identical(self) -> None:
        first = cc_observation_id(*CC_INPUT)
        for _ in range(50):
            self.assertEqual(cc_observation_id(*CC_INPUT), first)

    def test_each_field_changes_id(self) -> None:
        crawl, name, offset, length = CC_INPUT
        other_crawl = "CC-MAIN-2024-38"
        variants = [
            (other_crawl, name.replace(crawl, other_crawl), offset, length),
            (crawl, name.replace("00042", "00043"), offset, length),
            (crawl, name, offset + 1, length),
            (crawl, name, offset, length + 1),
        ]
        base = cc_observation_id(*CC_INPUT)
        seen = {base}
        for variant in variants:
            with self.subTest(variant=variant):
                value = cc_observation_id(*variant)
                self.assertNotEqual(value, base)
                seen.add(value)
        self.assertEqual(len(seen), len(variants) + 1)

    def test_zero_offset_accepted(self) -> None:
        crawl, name, _, length = CC_INPUT
        cc_observation_id(crawl, name, 0, length)

    def test_empty_values_rejected(self) -> None:
        crawl, name, offset, length = CC_INPUT
        with self.assertRaises(ValueError):
            cc_observation_id("", name, offset, length)
        with self.assertRaises(ValueError):
            cc_observation_id(crawl, "", offset, length)

    def test_malformed_crawl_id_rejected(self) -> None:
        _, name, offset, length = CC_INPUT
        for bad in ("cc-main-2024-33", "CC-MAIN-2024-3", "CC-MAIN-2024-33 ", " CC-MAIN-2024-33", "CC-MAIN-24-33"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                cc_observation_id(bad, name, offset, length)

    def test_malformed_warc_filename_rejected(self) -> None:
        crawl, name, offset, length = CC_INPUT
        bad_names = [
            "s3://commoncrawl/" + name,
            "https://data.commoncrawl.org/" + name,
            "/" + name,
            name.replace("CC-MAIN-2024-33/", "CC-MAIN-2024-38/", 1),
            name + " ",
            name + "\n",
            "crawl-data/CC-MAIN-2024-33",
        ]
        for bad in bad_names:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                cc_observation_id(crawl, bad, offset, length)

    def test_negative_offset_rejected(self) -> None:
        crawl, name, _, length = CC_INPUT
        with self.assertRaises(ValueError):
            cc_observation_id(crawl, name, -1, length)

    def test_non_positive_length_rejected(self) -> None:
        crawl, name, offset, _ = CC_INPUT
        for bad in (0, -1):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                cc_observation_id(crawl, name, offset, bad)

    def test_bool_and_non_int_rejected(self) -> None:
        crawl, name, offset, length = CC_INPUT
        for bad in (True, False, 1.0, "1", None):
            with self.subTest(field="offset", bad=bad), self.assertRaises(TypeError):
                cc_observation_id(crawl, name, bad, length)  # type: ignore[arg-type]
            with self.subTest(field="length", bad=bad), self.assertRaises(TypeError):
                cc_observation_id(crawl, name, offset, bad)  # type: ignore[arg-type]

    def test_out_of_bigint_range_rejected(self) -> None:
        crawl, name, _, length = CC_INPUT
        with self.assertRaises(ValueError):
            cc_observation_id(crawl, name, 2**63, length)

    def test_non_str_rejected(self) -> None:
        crawl, name, offset, length = CC_INPUT
        with self.assertRaises(TypeError):
            cc_observation_id(None, name, offset, length)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            cc_observation_id(crawl, name.encode(), offset, length)  # type: ignore[arg-type]


class GitHubObservationTests(unittest.TestCase):
    def test_repeated_calls_identical(self) -> None:
        first = github_observation_id(*GH_INPUT)
        for _ in range(50):
            self.assertEqual(github_observation_id(*GH_INPUT), first)

    def test_each_field_changes_id(self) -> None:
        repo, commit, path, blob = GH_INPUT
        variants = [
            (repo + 1, commit, path, blob),
            (repo, commit[:-1] + "8", path, blob),
            (repo, commit, "docs/intro2.md", blob),
            (repo, commit, path, blob[:-1] + "0"),
        ]
        base = github_observation_id(*GH_INPUT)
        seen = {base}
        for variant in variants:
            with self.subTest(variant=variant):
                value = github_observation_id(*variant)
                self.assertNotEqual(value, base)
                seen.add(value)
        self.assertEqual(len(seen), len(variants) + 1)

    def test_path_case_and_unicode_form_preserved(self) -> None:
        repo, commit, _, blob = GH_INPUT
        ids = {
            github_observation_id(repo, commit, p, blob)
            for p in ("docs/Intro.md", "docs/intro.md", "docs/Café.md", "docs/Café.md")
        }
        self.assertEqual(len(ids), 4)

    def test_empty_values_rejected(self) -> None:
        repo, commit, path, blob = GH_INPUT
        for args in ((repo, "", path, blob), (repo, commit, "", blob), (repo, commit, path, "")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                github_observation_id(*args)

    def test_invalid_repository_id_rejected(self) -> None:
        _, commit, path, blob = GH_INPUT
        for bad in (0, -5, 2**63):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                github_observation_id(bad, commit, path, blob)
        for bad in (True, "123456789", "MDEwOlJlcG9zaXRvcnkxMjM0NTY3ODk=", "R_kgDOABCDEF", 1.5, None):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                github_observation_id(bad, commit, path, blob)  # type: ignore[arg-type]

    def test_invalid_sha_rejected(self) -> None:
        repo, commit, path, blob = GH_INPUT
        bad_shas = [commit.upper(), commit[:-1], commit + "0", "g" * 40, "a" * 63, " " + commit[1:], commit[:39] + "\n"]
        for bad in bad_shas:
            with self.subTest(field="commit", bad=bad), self.assertRaises(ValueError):
                github_observation_id(repo, bad, path, blob)
            with self.subTest(field="blob", bad=bad), self.assertRaises(ValueError):
                github_observation_id(repo, commit, path, bad)

    def test_mixed_object_formats_rejected(self) -> None:
        repo, commit, path, _ = GH_INPUT
        with self.assertRaises(ValueError):
            github_observation_id(repo, commit, path, "b" * 64)

    def test_invalid_paths_rejected(self) -> None:
        repo, commit, _, blob = GH_INPUT
        bad_paths = [
            "/docs/intro.md",
            "docs/intro.md/",
            "docs//intro.md",
            "./docs/intro.md",
            "docs/../intro.md",
            "docs/./intro.md",
            ".",
            "..",
            "/",
            "docs/in\x00tro.md",
            "docs/\ud800.md",
        ]
        for bad in bad_paths:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                github_observation_id(repo, commit, bad, blob)

    def test_unusual_but_valid_paths_accepted_verbatim(self) -> None:
        repo, commit, _, blob = GH_INPUT
        for path in ("README", ".github/workflows/ci.yml", "a b/c.md", "dir\\file.md", "..hidden", "x.."):
            with self.subTest(path=path):
                self.assertIn(
                    '"path":' + json.dumps(path, ensure_ascii=True),
                    github_observation_payload(repo, commit, path, blob),
                )

    def test_non_str_rejected(self) -> None:
        repo, commit, path, blob = GH_INPUT
        with self.assertRaises(TypeError):
            github_observation_id(repo, commit.encode(), path, blob)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            github_observation_id(repo, commit, None, blob)  # type: ignore[arg-type]


class DomainSeparationTests(unittest.TestCase):
    def test_observation_ids_differ_from_identity_v1_and_each_other(self) -> None:
        from app.utils.ids import chunk_id_from_hash, document_id_from_hash

        h = "af959c5a0629ec4c4c35a5802d9fb4e5edebea74c881b8c147cff8f89bd3a704"
        ids = {
            document_id_from_hash(h),
            chunk_id_from_hash(h),
            cc_observation_id(*CC_INPUT),
            github_observation_id(*GH_INPUT),
            make_dataset_bridge_id("common_crawl"),
            make_dataset_bridge_id("github"),
        }
        self.assertEqual(len(ids), 6)


if __name__ == "__main__":
    unittest.main()
