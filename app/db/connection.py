"""Runtime database connection layer (role ``kb_pipeline_app`` only).

* bounded SQLAlchemy pool (``pool_size``, no overflow), pre-ping, recycling;
* every session runs with ``TimeZone = UTC`` and an ``application_name``;
* ``transaction()``: one short transaction, committed on success, rolled back
  on any exception;
* ``health_check()``: read-only identity/readiness probe. The runtime refuses
  to run as any role other than ``kb_pipeline_app``, as a superuser, or
    against a database whose ``kbp.alembic_version`` is not ``kbp_001c``.

The connection URL is never logged or included in an error message.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.config.settings import DatabaseSettings

RUNTIME_ROLE = "kb_pipeline_app"
EXPECTED_KBP_REVISION = "kbp_001c"


class RuntimeDatabaseError(RuntimeError):
    """The runtime database is unreachable or not the expected identity/revision."""


HEALTH_SQL = """
SELECT current_user                                                            AS role,
       (SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user) AS is_superuser,
       current_setting('TimeZone')                                              AS timezone,
       (SELECT v.version_num FROM kbp.alembic_version AS v)                     AS kbp_revision,
       now()                                                                    AS server_time
"""


@dataclass(frozen=True)
class HealthReport:
    role: str
    is_superuser: bool
    timezone: str
    kbp_revision: str | None
    server_time: datetime

    @property
    def problems(self) -> tuple[str, ...]:
        problems = []
        if self.role != RUNTIME_ROLE:
            problems.append(f"connected as {self.role}, expected {RUNTIME_ROLE}")
        if self.is_superuser:
            problems.append("the runtime role must not be a superuser")
        if self.kbp_revision != EXPECTED_KBP_REVISION:
            problems.append(f"kbp.alembic_version is {self.kbp_revision!r}, expected {EXPECTED_KBP_REVISION!r}")
        if self.timezone.upper() not in ("UTC", "ETC/UTC"):
            problems.append(f"session TimeZone is {self.timezone}, expected UTC")
        return tuple(problems)

    @property
    def ok(self) -> bool:
        return not self.problems


def url_role(url: str) -> str | None:
    """The user name of a SQLAlchemy URL (no secret is returned)."""
    from sqlalchemy.engine import make_url

    return make_url(url).username


def create_runtime_engine(settings: DatabaseSettings) -> Any:
    """Create the bounded runtime engine. Refuses URLs for any role but kb_pipeline_app."""
    from sqlalchemy import create_engine

    if url_role(settings.url) != RUNTIME_ROLE:
        raise RuntimeDatabaseError(f"the runtime URL must connect as {RUNTIME_ROLE}")
    return create_engine(
        settings.url,
        pool_size=settings.pool_size,
        max_overflow=0,
        pool_timeout=settings.pool_timeout_seconds,
        pool_recycle=settings.pool_recycle_seconds,
        pool_pre_ping=True,
        connect_args={
            "application_name": settings.application_name,
            "options": "-c TimeZone=UTC",
        },
    )


class Database:
    """Transaction and health helpers around the runtime engine."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    @classmethod
    def from_settings(cls, settings: DatabaseSettings) -> "Database":
        return cls(create_runtime_engine(settings))

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """One transaction: COMMIT on normal exit, ROLLBACK on any exception."""
        with self.engine.begin() as connection:
            yield connection

    def health_check(self) -> HealthReport:
        from sqlalchemy import text

        try:
            with self.engine.connect() as connection:
                row = connection.execute(text(HEALTH_SQL)).mappings().one()
                connection.rollback()
        except Exception as exc:  # the URL is never part of the message
            raise RuntimeDatabaseError(f"runtime database health check failed: {type(exc).__name__}") from exc
        return HealthReport(
            role=row["role"],
            is_superuser=bool(row["is_superuser"]),
            timezone=row["timezone"],
            kbp_revision=row["kbp_revision"],
            server_time=row["server_time"],
        )

    def verify_runtime_identity(self) -> HealthReport:
        report = self.health_check()
        if not report.ok:
            raise RuntimeDatabaseError("runtime database is not ready: " + "; ".join(report.problems))
        return report

    def dispose(self) -> None:
        self.engine.dispose()
