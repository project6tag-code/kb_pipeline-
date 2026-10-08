"""Release feature switches.

This release is Common Crawl only. GitHub acquisition is unfinished and stays DISABLED unless the operator sets
``KB_ENABLE_GITHUB=1`` (development only). The guard sits at every entry point that could start GitHub work:
``app.pipeline.cli run --sources github``, the GitHub worker, and the GitHub planner. It does not touch any
GitHub logic.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

GITHUB_ENV = "KB_ENABLE_GITHUB"


class FeatureDisabled(RuntimeError):
    """A feature that is not enabled in this release was requested."""


def github_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return (env.get(GITHUB_ENV) or "").strip().lower() in ("1", "true", "yes")


def require_github(what: str = "GitHub ingestion", environ: Mapping[str, str] | None = None) -> None:
    if not github_enabled(environ):
        raise FeatureDisabled(f"{what} is disabled in this release (Common Crawl only). "
                              f"It is unfinished; development builds may set {GITHUB_ENV}=1.")
