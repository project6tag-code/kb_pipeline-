"""Acquisition worker process (Common Crawl Acquisition V1).

Runs the Runtime Engine worker loop with the ``cc_index_partition`` handler
only. One process works on one partition at a time with a bounded fetch pool
(``KB_CC_FETCH_CONCURRENCY``); run several processes for more throughput.

Lifecycle: verify the runtime identity (role, revision, UTC) -> open a
``pipeline_runs`` row (``ingest``) -> sweep expired leases periodically ->
on SIGINT/SIGTERM stop claiming, hand the in-flight job back with its
checkpoint -> finish the run.

``silver=True`` (CLI: ``python -m app.processing.silver.cli work``) attaches the
Bronze -> Silver processor, so each unit continues to the SILVER stage.
"""

from __future__ import annotations

import logging
import socket
import uuid

from app.config.settings import DatabaseSettings, WorkerSettings
from app.db import repositories
from app.db.connection import Database
from app.queue.job_queue import JobQueue
from app.queue.states import RunKind, RunStatus
from app.sources.commoncrawl.config import CCSettings
from app.sources.commoncrawl.connector import build_client, build_handler, register_cc_handler
from app.sources.commoncrawl.http import HttpClient
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker, WorkerStats

log = logging.getLogger(__name__)


def build_cc_worker(queue: JobQueue, settings: CCSettings, worker_settings: WorkerSettings, *,
                    client: HttpClient, run_id: uuid.UUID | None = None, recover_every: int = 10,
                    silver=None) -> Worker:
    registry = HandlerRegistry()
    register_cc_handler(registry, build_handler(settings, client=client, run_id=run_id, silver=silver))
    return Worker(queue, registry, settings=worker_settings, recover_every=recover_every)


def _silver(run_id: uuid.UUID, *, cc_bronze=None, gh_bronze=None):
    from app.processing.silver.config import SilverConfig
    from app.workers.processing_worker import build_silver_processor

    return build_silver_processor(SilverConfig.from_env(), cc_bronze=cc_bronze, gh_bronze=gh_bronze, run_id=run_id)


def run_cc_worker(*, max_iterations: int | None = None, recover_every: int = 10, silver: bool = False) -> WorkerStats:
    """Run until stopped (or ``max_iterations``). Requires KB_DATABASE_URL (kb_pipeline_app)."""
    settings = CCSettings.from_env()
    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:cc_acquisition"))
    client = build_client(settings)
    try:
        db.verify_runtime_identity()
        with db.transaction() as conn:
            run = repositories.create_run(conn, RunKind.INGEST, code_version="cc_acquisition_v1",
                                          host=socket.gethostname(),
                                          notes=f"common crawl acquisition; filter {settings.acquisition.filter.version}")
        processor = None
        if silver:
            from app.sources.commoncrawl.connector import build_bronze

            processor = _silver(run.run_id, cc_bronze=build_bronze(settings))
        worker = build_cc_worker(JobQueue(db), settings, WorkerSettings.from_env(), client=client,
                                 run_id=run.run_id, recover_every=recover_every, silver=processor)
        worker.install_signal_handlers()
        status, notes = RunStatus.FAILED, "worker crashed"
        try:
            stats = worker.run(max_iterations=max_iterations)
            status = RunStatus.SUCCEEDED if not worker.stopping else RunStatus.ABORTED
            notes = f"stats: {stats}"
            return stats
        finally:
            with db.transaction() as conn:
                repositories.finish_run(conn, run.run_id, status, notes=notes)
    finally:
        client.close()
        db.dispose()


def run_github_worker(*, max_iterations: int | None = None, recover_every: int = 10,
                      silver: bool = False) -> WorkerStats:
    """GitHub Acquisition V1 worker (``gh_repository_discovery`` + ``gh_repository_snapshot``).

    Requires KB_DATABASE_URL (kb_pipeline_app); KB_GH_TOKEN is optional (never logged).
    """
    from app.config import features

    features.require_github("The GitHub worker")  # release guard: Common Crawl only
    from app.sources.github.config import GitHubSettings
    from app.sources.github.connector import build_client as build_gh_client
    from app.sources.github.connector import register_github_handlers

    settings = GitHubSettings.from_env()
    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:gh_acquisition"))
    client = build_gh_client(settings)
    try:
        db.verify_runtime_identity()
        with db.transaction() as conn:
            run = repositories.create_run(conn, RunKind.INGEST, code_version="gh_acquisition_v1",
                                          host=socket.gethostname(),
                                          notes=f"github acquisition; files {settings.acquisition.files.version}; "
                                                f"authenticated={client.authenticated}")
        registry = HandlerRegistry()
        processor = None
        if silver:
            from app.sources.github.connector import build_bronze as build_gh_bronze

            processor = _silver(run.run_id, gh_bronze=build_gh_bronze(settings))
        register_github_handlers(registry, settings, client=client, run_id=run.run_id, silver=processor)
        worker = Worker(JobQueue(db), registry, settings=WorkerSettings.from_env(), recover_every=recover_every)
        worker.install_signal_handlers()
        status, notes = RunStatus.FAILED, "worker crashed"
        try:
            stats = worker.run(max_iterations=max_iterations)
            status = RunStatus.SUCCEEDED if not worker.stopping else RunStatus.ABORTED
            notes = f"stats: {stats}"
            return stats
        finally:
            with db.transaction() as conn:
                repositories.finish_run(conn, run.run_id, status, notes=notes)
    finally:
        client.close()
        db.dispose()
