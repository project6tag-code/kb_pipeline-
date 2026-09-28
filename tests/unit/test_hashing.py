"""Unit tests for app.utils.hashing."""

import hashlib
import unittest

from app.utils.hashing import InvalidHashError, sha256_text, validate_sha256_hex


class Sha256TextTests(unittest.TestCase):
    def test_known_vectors(self) -> None:
        self.assertEqual(
            sha256_text(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        self.assertEqual(
            sha256_text("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )

    def test_uses_utf8(self) -> None:
        text = "Café 東京 \U0001f600"
        self.assertEqual(sha256_text(text), hashlib.sha256(text.encode("utf-8")).hexdigest())

    def test_lowercase_64_hex(self) -> None:
        digest = sha256_text("Hello, World!")
        self.assertEqual(len(digest), 64)
        self.assertEqual(digest, digest.lower())
        int(digest, 16)

    def test_deterministic(self) -> None:
        self.assertEqual(sha256_text("same"), sha256_text("same"))

    def test_different_text_different_hash(self) -> None:
        self.assertNotEqual(sha256_text("a"), sha256_text("A"))

    def test_rejects_non_str(self) -> None:
        for bad in (b"abc", None, 123, ["abc"], bytearray(b"abc")):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                sha256_text(bad)  # type: ignore[arg-type]

    def test_rejects_lone_surrogate(self) -> None:
        with self.assertRaises(UnicodeEncodeError):
            sha256_text("\ud800")


class ValidateSha256HexTests(unittest.TestCase):
    VALID = "af959c5a0629ec4c4c35a5802d9fb4e5edebea74c881b8c147cff8f89bd3a704"

    def test_accepts_valid_lowercase(self) -> None:
        self.assertEqual(validate_sha256_hex(self.VALID), self.VALID)

    def test_normalizes_uppercase(self) -> None:
        self.assertEqual(validate_sha256_hex(self.VALID.upper()), self.VALID)

    def test_rejects_malformed(self) -> None:
        bad_values = [
            "",
            self.VALID[:-1],
            self.VALID + "0",
            self.VALID[:-1] + "g",
            " " + self.VALID,
            self.VALID + "\n",
            "0x" + self.VALID[:62],
            "-" * 64,
            "０" * 64,  # full-width digits must not pass as hex
        ]
        for bad in bad_values:
            with self.subTest(bad=bad), self.assertRaises(InvalidHashError):
                validate_sha256_hex(bad)

    def test_invalid_hash_error_is_value_error(self) -> None:
        self.assertTrue(issubclass(InvalidHashError, ValueError))

    def test_rejects_non_str(self) -> None:
        for bad in (self.VALID.encode(), None, 0):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                validate_sha256_hex(bad)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
