"""Runtime settings, read from the environment (docs/database_privileges_v1.md §10).

Connection variables (values never committed, never logged):

* ``KB_DATABASE_URL``           runtime role ``kb_pipeline_app`` (this module)
* ``KB_MIGRATION_DATABASE_URL`` migrator role; only checked here so that the
  runtime refuses to reuse it.

Optional tuning (defaults in parentheses):

* ``KB_DB_POOL_SIZE`` (5, 1..50): bounded pool, no overflow connections
* ``KB_DB_POOL_TIMEOUT_SECONDS`` (30)
* ``KB_WORKER_LEASE_SECONDS`` (300), ``KB_WORKER_POLL_SECONDS`` (5),
  ``KB_WORKER_CLAIM_BATCH`` (1, 1..100)
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

RUNTIME_URL_ENV = "KB_DATABASE_URL"
MIGRATION_URL_ENV = "KB_MIGRATION_DATABASE_URL"


class SettingsError(ValueError):
    """Invalid or missing runtime configuration (never includes secret values)."""


def _int(env: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise SettingsError(f"{name} must be an integer") from None
    if not low <= value <= high:
        raise SettingsError(f"{name} must be between {low} and {high}")
    return value


def _float(env: Mapping[str, str], name: str, default: float, low: float, high: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise SettingsError(f"{name} must be a number") from None
    if not low <= value <= high:
        raise SettingsError(f"{name} must be between {low} and {high}")
    return value


@dataclass(frozen=True)
class DatabaseSettings:
    url: str = field(repr=False)  # contains credentials when not using pgpass: never printed
    pool_size: int = 5
    pool_timeout_seconds: float = 30.0
    pool_recycle_seconds: int = 1800
    application_name: str = "kb_pipeline:runtime"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, *,
                 application_name: str = "kb_pipeline:runtime", url_env: str = RUNTIME_URL_ENV) -> "DatabaseSettings":
        env = os.environ if environ is None else environ
        url = env.get(url_env, "").strip()
        if not url:
            raise SettingsError(f"{url_env} is not set; configure it from your secrets store (see .env.example)")
        if url == env.get(MIGRATION_URL_ENV, "").strip():
            raise SettingsError(f"{url_env} must not equal {MIGRATION_URL_ENV}: the runtime connects as kb_pipeline_app")
        return cls(
            url=url,
            pool_size=_int(env, "KB_DB_POOL_SIZE", 5, 1, 50),
            pool_timeout_seconds=_float(env, "KB_DB_POOL_TIMEOUT_SECONDS", 30.0, 1.0, 600.0),
            application_name=application_name,
        )


@dataclass(frozen=True)
class WorkerSettings:
    lease_seconds: float = 300.0
    poll_interval_seconds: float = 5.0
    claim_batch_size: int = 1
    # Restricts claiming/recovery to identifiers with this prefix. Used to
    # isolate integration tests from real work; None in production.
    source_identifier_prefix: str | None = None

    def __post_init__(self) -> None:
        if not 1.0 <= self.lease_seconds <= 86400.0:
            raise SettingsError("lease_seconds must be between 1 and 86400")
        if not 0.0 <= self.poll_interval_seconds <= 3600.0:
            raise SettingsError("poll_interval_seconds must be between 0 and 3600")
        if not 1 <= self.claim_batch_size <= 100:
            raise SettingsError("claim_batch_size must be between 1 and 100")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "WorkerSettings":
        env = os.environ if environ is None else environ
        return cls(
            lease_seconds=_float(env, "KB_WORKER_LEASE_SECONDS", 300.0, 1.0, 86400.0),
            poll_interval_seconds=_float(env, "KB_WORKER_POLL_SECONDS", 5.0, 0.0, 3600.0),
            claim_batch_size=_int(env, "KB_WORKER_CLAIM_BATCH", 1, 1, 100),
        )
