"""Bronze -> Silver wiring for the acquisition work units (Runtime Engine V1).

Silver processing runs inside the existing ``cc_index_partition`` and
``gh_repository_snapshot`` units (schema §9.3/§16.3: the job types are fixed
by Schema V1; there is no per-document job). ``build_silver_processor`` creates
the processor that those handlers call per batch; with a ``ChunkWriter``
attached (the default for workers), newly accepted documents are chunked in the
same transaction (schema §10.3, Chunking V1).
"""

from __future__ import annotations

import uuid

from app.processing.chunking.repository import PostgresChunkStore
from app.processing.chunking.service import ChunkWriter
from app.processing.dedup.near import NearIndex
from app.processing.silver.config import SilverConfig
from app.processing.silver.processor import SilverProcessor
from app.processing.silver.repository import PostgresSilverStore, SilverStore
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.github.bronze import GitHubBronze


def build_silver_processor(config: SilverConfig, *, cc_bronze: BronzeStore | None = None,
                           gh_bronze: GitHubBronze | None = None, store: SilverStore | None = None,
                           near_index: NearIndex | None = None, run_id: uuid.UUID | None = None,
                           chunker: ChunkWriter | None | bool = True) -> SilverProcessor:
    """``chunker=True`` attaches the default Chunking V1 writer; ``None``/``False`` disables chunking."""
    index = near_index or NearIndex(config.near_index_path, min_free_bytes=config.limits.min_free_bytes)
    if chunker is True:
        chunker = ChunkWriter(PostgresChunkStore(), run_id=run_id)
    return SilverProcessor(config, store or PostgresSilverStore(), index, cc_bronze=cc_bronze, gh_bronze=gh_bronze,
                           run_id=run_id, chunker=chunker or None)
