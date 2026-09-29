"""Wiring of Common Crawl Acquisition V1 components (no I/O at import time).

Production components always use the fixed ``COMMON_CRAWL`` endpoints; tests
inject a transport or loopback endpoints explicitly.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable

from app.queue.states import JobType
from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY, CCPartitionHandler
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.commoncrawl.config import CCSettings
from app.sources.commoncrawl.discovery import CrawlPlanner, PlannerQueue
from app.sources.commoncrawl.http import COMMON_CRAWL, Endpoints, HttpClient, Transport
from app.sources.commoncrawl.repository import CaptureStore, PostgresCaptureStore
from app.workers.registry import HandlerRegistry


def build_client(settings: CCSettings, *, endpoints: Endpoints = COMMON_CRAWL,
                 transport: Transport | None = None) -> HttpClient:
    return HttpClient(settings.http, endpoints=endpoints, transport=transport)


def build_bronze(settings: CCSettings) -> BronzeStore:
    return BronzeStore(settings.acquisition.bronze_root, min_free_bytes=settings.acquisition.min_free_bytes)


def build_handler(settings: CCSettings, *, client: HttpClient, store: CaptureStore | None = None,
                  bronze: BronzeStore | None = None, run_id: uuid.UUID | None = None,
                  silver=None) -> CCPartitionHandler:
    """``silver``: an optional Bronze -> Silver processor (app.processing.silver.processor)."""
    return CCPartitionHandler(client=client, store=store or PostgresCaptureStore(),
                              bronze=bronze or build_bronze(settings), config=settings.acquisition, run_id=run_id,
                              silver=silver)


def register_cc_handler(registry: HandlerRegistry, handler: CCPartitionHandler) -> None:
    registry.register(JobType.CC_INDEX_PARTITION, handler, policy=CC_PARTITION_POLICY)


def build_planner(settings: CCSettings, *, client: HttpClient, queue: PlannerQueue,
                  store: CaptureStore | None = None, run_id: uuid.UUID | None = None,
                  progress: Callable[[str], None] | None = None) -> CrawlPlanner:
    return CrawlPlanner(client, queue, store or PostgresCaptureStore(), planning=settings.planning,
                        filter_config=settings.acquisition.filter, policy=CC_PARTITION_POLICY,
                        max_block_bytes=settings.acquisition.max_index_block_bytes, run_id=run_id,
                        progress=progress)
