"""Deterministic job identities (schema §16.2).

A logical job is identified by ``(job_type, source_identifier)``, which is
UNIQUE in ``kbp.pipeline_jobs``. Building ``source_identifier`` only through
these functions guarantees that the same work unit always gets the same key,
so enqueueing it twice can never create a second job.
"""

from __future__ import annotations

import re
import uuid

from app.queue.states import JOB_TYPE_SOURCE, JobSource, JobType

_CRAWL_ID = re.compile(r"^CC-MAIN-[0-9]{4}-[0-9]{2}$")
_GIT_OBJECT_ID = re.compile(r"^[0-9a-f]{40}([0-9a-f]{24})?$")
_TOKEN = re.compile(r"^[A-Za-z0-9._:/+-]+$")
MAX_SOURCE_IDENTIFIER_CHARS = 2048


def _token(name: str, value: str) -> str:
    if not isinstance(value, str) or not value or not _TOKEN.match(value) or "#" in value:
        raise ValueError(f"{name} must be a non-empty token of [A-Za-z0-9._:/+-] without '#': {value!r}")
    return value


def _non_negative_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def cc_index_partition(crawl_id: str, partition_path: str, filter_version: str,
                       row_range: tuple[int, int] | None = None) -> str:
    """``<crawl_id>/<index partition path>#filter=<filter_version>[#range=<a>-<b>]``."""
    if not _CRAWL_ID.match(crawl_id or ""):
        raise ValueError(f"crawl_id must match CC-MAIN-YYYY-WW: {crawl_id!r}")
    identifier = f"{crawl_id}/{_token('partition_path', partition_path).lstrip('/')}#filter={_token('filter_version', filter_version)}"
    if row_range is not None:
        start, end = (_non_negative_int("range start", row_range[0]), _non_negative_int("range end", row_range[1]))
        if start > end:
            raise ValueError("range start must be <= range end")
        identifier += f"#range={start}-{end}"
    return identifier


def gh_repository_discovery(query_id: str, page: int) -> str:
    """``<discovery query id>#page=<n>``."""
    return f"{_token('query_id', query_id)}#page={_non_negative_int('page', page)}"


def gh_repository_snapshot(repository_id: int, commit_sha: str) -> str:
    """``<repository_id>@<commit_sha>`` (GitHub numeric repository id; lowercase hex sha)."""
    _non_negative_int("repository_id", repository_id)
    if not _GIT_OBJECT_ID.match(commit_sha or ""):
        raise ValueError("commit_sha must be 40 or 64 lowercase hex characters")
    return f"{repository_id}@{commit_sha}"


def _uuid_text(name: str, value: uuid.UUID | str) -> str:
    return str(uuid.UUID(str(value)))


def embedding_enqueue(embedding_model_id: int, chunk_id_from: uuid.UUID | str, chunk_id_to: uuid.UUID | str) -> str:
    """``<embedding_model_id>#chunk_id=<from>..<to>``."""
    return (f"{_non_negative_int('embedding_model_id', embedding_model_id)}"
            f"#chunk_id={_uuid_text('from', chunk_id_from)}..{_uuid_text('to', chunk_id_to)}")


def policy_reevaluation(policy_version: str, document_id_from: uuid.UUID | str,
                        document_id_to: uuid.UUID | str) -> str:
    """``<policy_version>#document_id=<from>..<to>``."""
    return (f"{_token('policy_version', policy_version)}"
            f"#document_id={_uuid_text('from', document_id_from)}..{_uuid_text('to', document_id_to)}")


def legacy_reconciliation(reconciliation_version: str) -> str:
    """``<reconciliation_version>``."""
    return _token("reconciliation_version", reconciliation_version)


def validate_job_identity(job_type: JobType | str, source: JobSource | str, source_identifier: str) -> None:
    """Reject identities that could not have come from the builders above."""
    job_type = JobType(job_type)
    if JobSource(source) is not JOB_TYPE_SOURCE[job_type]:
        raise ValueError(f"job type {job_type} belongs to source {JOB_TYPE_SOURCE[job_type]}, not {source}")
    if not isinstance(source_identifier, str) or not source_identifier.strip():
        raise ValueError("source_identifier must be a non-empty string")
    if source_identifier != source_identifier.strip() or len(source_identifier) > MAX_SOURCE_IDENTIFIER_CHARS:
        raise ValueError("source_identifier must be trimmed and at most 2048 characters")
    if "\x00" in source_identifier:
        raise ValueError("source_identifier must not contain U+0000")
