"""The database this installation may operate on (KB_EXPECTED_DATABASE, default ``kb_pipeline_test``).

Every operator and publisher command still requires ``--expect-database <name>``; the name must equal the
configured expected database AND the database the connection actually reached. The check is unchanged; only the
fixed name became configuration, so a production database can have its own name.

``data_collection_pipeline`` (the legacy system) and PostgreSQL's built-in databases are never accepted.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

ENV_NAME = "KB_EXPECTED_DATABASE"
DEFAULT_DATABASE = "kb_pipeline_test"
FORBIDDEN_DATABASES = frozenset({"data_collection_pipeline", "postgres", "template0", "template1"})
_VALID = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class TargetConfigError(ValueError):
    """KB_EXPECTED_DATABASE is not a usable, safe database name."""


def expected_database(environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    name = (env.get(ENV_NAME) or "").strip() or DEFAULT_DATABASE
    if not _VALID.match(name):
        raise TargetConfigError(f"{ENV_NAME}={name!r} is not a valid database name (lower-case letters, digits, underscore)")
    if name in FORBIDDEN_DATABASES:
        raise TargetConfigError(f"{ENV_NAME}={name!r} is a protected database and is never accepted")
    return name
