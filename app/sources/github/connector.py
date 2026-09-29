"""Wiring of GitHub Acquisition V1 components (no I/O at import time).

Production components always use ``GITHUB_API`` (https://api.github.com);
tests inject a transport or loopback endpoints explicitly.
"""

from __future__ import annotations

import uuid

from app.queue.states import JobType
from app.sources.commoncrawl.http import Transport
from app.sources.github.acquisition import RepositorySnapshotHandler
from app.sources.github.bronze import GitHubBronze
from app.sources.github.client import GITHUB_API, GitHubClient, GitHubEndpoints
from app.sources.github.common import GH_POLICY
from app.sources.github.config import GitHubSettings
from app.sources.github.discovery import RepositoryDiscoveryHandler
from app.sources.github.repository import GitHubStore, PostgresGitHubStore
from app.workers.registry import HandlerRegistry


def build_client(settings: GitHubSettings, *, endpoints: GitHubEndpoints = GITHUB_API,
                 transport: Transport | None = None) -> GitHubClient:
    return GitHubClient(settings.http, settings.credentials, endpoints=endpoints, transport=transport)


def build_bronze(settings: GitHubSettings) -> GitHubBronze:
    return GitHubBronze(settings.acquisition.bronze_root, min_free_bytes=settings.acquisition.min_free_bytes)


def register_github_handlers(registry: HandlerRegistry, settings: GitHubSettings, *, client: GitHubClient,
                             store: GitHubStore | None = None, bronze: GitHubBronze | None = None,
                             run_id: uuid.UUID | None = None, silver=None) -> None:
    """``silver``: an optional Bronze -> Silver processor (app.processing.silver.processor)."""
    store = store or PostgresGitHubStore()
    registry.register(JobType.GH_REPOSITORY_DISCOVERY,
                      RepositoryDiscoveryHandler(client=client, store=store, credentials=settings.credentials,
                                                 run_id=run_id), policy=GH_POLICY)
    registry.register(JobType.GH_REPOSITORY_SNAPSHOT,
                      RepositorySnapshotHandler(client=client, store=store, bronze=bronze or build_bronze(settings),
                                                config=settings.acquisition, credentials=settings.credentials,
                                                run_id=run_id, silver=silver), policy=GH_POLICY)
