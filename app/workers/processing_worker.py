"""Bronze -> Silver wiring for the acquisition work units (Runtime Engine V1).

Silver processing runs inside the existing ``cc_index_partition`` and
``gh_repository_snapshot`` units (schema §9.3/§16.3: the job types are fixed
by Schema V1; there is no per-document job). ``build_silver_processor`` creates
the processor that those handlers call per batch.
"""

from __future__ import annotations

import uuid

from app.processing.dedup.near import NearIndex
from app.processing.silver.config import SilverConfig
from app.processing.silver.processor import SilverProcessor
from app.processing.silver.repository import PostgresSilverStore, SilverStore
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.github.bronze import GitHubBronze


def build_silver_processor(config: SilverConfig, *, cc_bronze: BronzeStore | None = None,
                           gh_bronze: GitHubBronze | None = None, store: SilverStore | None = None,
                           near_index: NearIndex | None = None, run_id: uuid.UUID | None = None) -> SilverProcessor:
    index = near_index or NearIndex(config.near_index_path, min_free_bytes=config.limits.min_free_bytes)
    return SilverProcessor(config, store or PostgresSilverStore(), index, cc_bronze=cc_bronze, gh_bronze=gh_bronze,
                           run_id=run_id)
