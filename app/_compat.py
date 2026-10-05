"""Windows Application Control compatibility for SQLAlchemy's optional compiled extensions.

On some managed Windows machines an Application Control policy refuses to load SQLAlchemy's Cython
extension modules (``*_cy.pyd``), which makes ``import sqlalchemy`` fail. Each of those modules also ships
as plain Python source with identical behaviour. When (and only when) the policy blocks the binaries, this
module resolves the ``*_cy`` modules to their ``.py`` source instead. The blocked binaries are never loaded,
no policy is changed and no package is modified. Where the binaries load normally this is a no-op.
Set ``KB_SQLALCHEMY_PURE_PYTHON=0`` to disable, ``=1`` to force.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
from pathlib import Path


class _PurePythonCyFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if not fullname.startswith("sqlalchemy.") or not fullname.rsplit(".", 1)[-1].endswith("_cy"):
            return None
        for entry in path or ():
            source = Path(entry) / (fullname.rsplit(".", 1)[-1] + ".py")
            if source.is_file():
                return importlib.util.spec_from_file_location(fullname, source)
        return None


def _extensions_blocked() -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes

        spec = importlib.util.find_spec("sqlalchemy")
        roots = list(spec.submodule_search_locations or ()) if spec else []
        for root in roots:
            for binary in sorted(Path(root).rglob("*_cy.*.pyd")):
                try:
                    ctypes.WinDLL(str(binary))
                except OSError:
                    return True
    except Exception:  # noqa: BLE001 - detection must never break startup
        return False
    return False


def ensure_sqlalchemy_importable() -> bool:
    """Install the pure-Python resolver if the policy blocks the compiled extensions. Returns True if installed."""
    mode = os.environ.get("KB_SQLALCHEMY_PURE_PYTHON", "").strip()
    if mode == "0" or "sqlalchemy" in sys.modules or any(isinstance(f, _PurePythonCyFinder) for f in sys.meta_path):
        return False
    if mode == "1" or _extensions_blocked():
        sys.meta_path.insert(0, _PurePythonCyFinder())
        return True
    return False
