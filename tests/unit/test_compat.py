"""Windows Application Control compatibility shim: resolves only sqlalchemy *_cy modules to their .py source."""

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

from app import _compat


class FinderTests(unittest.TestCase):
    def test_resolves_cy_module_to_python_source_only_within_sqlalchemy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "_thing_cy.py").write_text("VALUE = 1\n")
            finder = _compat._PurePythonCyFinder()
            spec = finder.find_spec("sqlalchemy.util._thing_cy", [tmp])
            self.assertIsNotNone(spec)
            self.assertTrue(spec.origin.endswith("_thing_cy.py"))
            self.assertIsNone(finder.find_spec("sqlalchemy.util._missing_cy", [tmp]))
            self.assertIsNone(finder.find_spec("other.pkg._thing_cy", [tmp]))
            self.assertIsNone(finder.find_spec("sqlalchemy.util.plain", [tmp]))

    def test_opt_out_never_installs(self) -> None:
        os.environ["KB_SQLALCHEMY_PURE_PYTHON"] = "0"
        try:
            self.assertFalse(_compat.ensure_sqlalchemy_importable())
        finally:
            del os.environ["KB_SQLALCHEMY_PURE_PYTHON"]

    def test_sqlalchemy_is_importable_in_this_environment(self) -> None:
        self.assertIsNotNone(importlib.util.find_spec("sqlalchemy"))
        import sqlalchemy  # noqa: F401  (the shim, when needed, ran at `import app`)


if __name__ == "__main__":
    unittest.main()
