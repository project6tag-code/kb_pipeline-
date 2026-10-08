"""Crawl discovery and index partition planning (index-first, schema §7.1).

1. **Crawls.** ``collinfo.json`` on the Common Crawl index server lists the
   published crawl collections. Only ids matching ``CC-MAIN-YYYY-WW`` are
   accepted; the newest ``crawl_window`` crawls are selected (or an explicit
   list, each of which must be listed). No crawl id is hard-coded.
2. **Partitions.** Each crawl's CDX index is a set of zipnum shards
   ``cdx-NNNNN.gz`` made of independent gzip blocks (~3000 index lines each).
   ``cluster.idx`` has one line per block::

       <urlkey> <timestamp>\\t<cdx file>\\t<offset>\\t<length>\\t<sequence>

   It is streamed line by line (never loaded whole). Blocks of one shard are
   numbered 0, 1, 2 ... in file order and grouped into fixed, aligned
   partitions of ``blocks_per_partition`` blocks: partition ``k`` of a shard is
   blocks ``[k*B, k*B + B - 1]``. Its job identity (schema §16.2) is::

       <crawl_id>/cc-index/collections/<crawl_id>/indexes/<cdx file>#filter=<filter_version>#range=<first>-<last>

   The block byte ranges and the filter parameters are stored in the job's
   initial checkpoint, so a worker needs no further index metadata.
3. **Enqueue.** ``INSERT ... ON CONFLICT DO NOTHING`` on the job identity:
   re-planning is always safe and never duplicates work.

Planning is deterministic: ``cluster.idx`` of a published crawl is immutable,
and ``partition_stride`` / ``max_partitions_per_crawl`` select partitions by
their ordinal, never at random.

4. **Key ranges (filter profile).** ``cluster.idx`` is sorted by SURT key, and
   block ``i`` covers the keys from its own first key up to the next block's
   first key. A partition is planned only when one of its blocks can hold a
   wanted key (``block_selector``):
   * ``reference`` profile: the SURT host prefixes of the curated reference
     domains (``org,wikipedia,en)`` ...), so only their index ranges are read;
   * ``open`` profile: every block outside the raw-IP region (keys starting with
     a digit or ``[``), which the URL policy rejects anyway.
   Ordinals, stride and the partition cap count planned partitions only.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from app.queue import identity
from app.queue.job_queue import EnqueueResult, JobSpec
from app.queue.retry import RetryPolicy
from app.queue.states import JobSource, JobType, Stage
from app.sources.commoncrawl.config import FilterConfig, PlanningConfig
from app.sources.commoncrawl.http import HttpClient

CHECKPOINT_VERSION = 1
MAX_COLLINFO_BYTES = 4 * 1024 * 1024
MAX_CLUSTER_LINE_BYTES = 16 * 1024
MAX_CLUSTER_IDX_BYTES = 2 * 1024 ** 3
REFERENCE_PRIORITY = 50  # lower = claimed first: reference partitions before open-crawl ones
OPEN_PRIORITY = 100

_CRAWL_ID = re.compile(r"^CC-MAIN-([0-9]{4})-([0-9]{2})$")
_CDX_FILE = re.compile(r"^cdx-[0-9]{5}\.gz$")
_DIGITS = re.compile(r"^[0-9]{1,19}$")


class DiscoveryError(ValueError):
    """Malformed discovery metadata. Planning stops instead of guessing."""


# --------------------------------------------------------------------------- #
# Crawls
# --------------------------------------------------------------------------- #

def parse_collinfo(data: bytes) -> list[str]:
    """Crawl ids from ``collinfo.json``, newest first. Entries that are not CC-MAIN-YYYY-WW are ignored."""
    try:
        entries = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise DiscoveryError(f"collinfo.json is not valid JSON: {exc}") from None
    if not isinstance(entries, list):
        raise DiscoveryError("collinfo.json is not a JSON list")
    crawls = {entry["id"] for entry in entries
              if isinstance(entry, dict) and isinstance(entry.get("id"), str) and _CRAWL_ID.match(entry["id"])}
    if not crawls:
        raise DiscoveryError("collinfo.json lists no CC-MAIN-YYYY-WW crawl")
    return sorted(crawls, key=lambda c: tuple(int(x) for x in _CRAWL_ID.match(c).groups()), reverse=True)


def select_crawls(available: Sequence[str], planning: PlanningConfig) -> list[str]:
    """Newest ``crawl_window`` crawls, or the explicit list (each must be published)."""
    if planning.explicit_crawls:
        missing = [c for c in planning.explicit_crawls if c not in available]
        if missing:
            raise DiscoveryError(f"crawls not listed by the index server: {', '.join(missing)}")
        return sorted(set(planning.explicit_crawls), reverse=True)
    return list(available[: planning.crawl_window])


def cdx_key(crawl_id: str, cdx_file: str) -> str:
    return f"cc-index/collections/{crawl_id}/indexes/{cdx_file}"


def cluster_idx_key(crawl_id: str) -> str:
    return f"cc-index/collections/{crawl_id}/indexes/cluster.idx"


# --------------------------------------------------------------------------- #
# Partitions
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ClusterEntry:
    cdx_file: str
    offset: int
    length: int
    key: str = ""


def parse_cluster_line(line: bytes, *, max_block_bytes: int) -> ClusterEntry:
    try:
        text = line.decode("ascii").rstrip("\r")
    except UnicodeDecodeError:
        # The SURT key may hold non-ASCII bytes; the fields we use are ASCII.
        text = line.decode("utf-8", errors="replace").rstrip("\r")
    fields = text.split("\t")
    if len(fields) < 4:
        raise DiscoveryError("cluster.idx line does not have the <key>\\t<file>\\t<offset>\\t<length> layout")
    cdx_file, offset, length = fields[1], fields[2], fields[3]
    if not _CDX_FILE.match(cdx_file) or not _DIGITS.match(offset) or not _DIGITS.match(length):
        raise DiscoveryError("cluster.idx line has an invalid file, offset or length")
    entry = ClusterEntry(cdx_file, int(offset), int(length), fields[0].split(" ", 1)[0])
    if not 1 <= entry.length <= max_block_bytes:
        raise DiscoveryError(f"cluster.idx block length {entry.length} outside 1..{max_block_bytes}")
    return entry


@dataclass(frozen=True)
class PartitionSpec:
    crawl_id: str
    cdx_file: str
    first_block: int
    last_block: int
    blocks: tuple[tuple[int, int], ...]
    ordinal: int  # position among all partitions of the crawl (for deterministic sampling)

    def source_identifier(self, filter_config: FilterConfig) -> str:
        return identity.cc_index_partition(self.crawl_id, cdx_key(self.crawl_id, self.cdx_file),
                                           filter_config.version, (self.first_block, self.last_block))

    def initial_checkpoint(self, filter_config: FilterConfig) -> dict[str, Any]:
        return {
            "v": CHECKPOINT_VERSION,
            "cdx_key": cdx_key(self.crawl_id, self.cdx_file),
            "blocks": [list(b) for b in self.blocks],
            "filter": json.loads(filter_config.canonical()),
            "block": 0,
            "line": 0,
        }


def surt_host_prefixes(domain: str) -> tuple[str, ...]:
    """SURT key prefixes of ``domain`` and its subdomains (Common Crawl drops a leading ``www.``)."""
    reversed_host = ",".join(reversed(domain.lower().removeprefix("www.").split(".")))
    return (reversed_host + ")", reversed_host + ",")


BlockSelector = Callable[[str, str | None], bool]


def _ip_region(key: str) -> bool:
    return not key or key[0].isdigit() or key[0] == "["


def block_selector(filter_config: FilterConfig) -> BlockSelector:
    """Whether a block whose keys run from ``key`` to ``next_key`` (None: end of index) may hold candidates."""
    if filter_config.profile == "reference":
        prefixes = tuple(sorted(p for d in filter_config.reference_domains for p in surt_host_prefixes(d)))

        def reference(key: str, next_key: str | None) -> bool:
            # [key, next_key] intersects [prefix, prefix + U+FFFF] for some prefix
            return any(key < prefix + "￿" and (next_key is None or next_key >= prefix) for prefix in prefixes)

        return reference

    def open_profile(key: str, next_key: str | None) -> bool:
        return not (_ip_region(key) and (next_key is None or _ip_region(next_key)))

    return open_profile


def _with_next_key(entries: Iterable[ClusterEntry]) -> Iterator[tuple[ClusterEntry, str | None]]:
    previous: ClusterEntry | None = None
    for entry in entries:
        if previous is not None:
            yield previous, entry.key
        previous = entry
    if previous is not None:
        yield previous, None


def plan_partitions(lines: Iterable[bytes], crawl_id: str, planning: PlanningConfig, *,
                    max_block_bytes: int, selector: BlockSelector | None = None) -> Iterator[PartitionSpec]:
    """Group ``cluster.idx`` lines into aligned partitions (streaming, bounded memory).

    With a ``selector``, a partition is planned only when at least one of its blocks is selected.
    Raises DiscoveryError on a malformed line or non-contiguous shard layout:
    partitions must cover exact, contiguous byte ranges.
    """
    size = planning.blocks_per_partition
    seen_files: set[str] = set()
    current_file: str | None = None
    local_index = 0
    expected_offset = 0
    group: list[tuple[int, int]] = []
    group_first = 0
    group_selected = False
    ordinal = 0
    emitted = 0

    def emit() -> PartitionSpec | None:
        nonlocal ordinal, emitted
        if selector is not None and not group_selected:
            return None
        spec = PartitionSpec(crawl_id, current_file, group_first, group_first + len(group) - 1, tuple(group), ordinal)
        ordinal += 1
        if spec.ordinal % planning.partition_stride != 0:
            return None
        if planning.max_partitions_per_crawl and emitted >= planning.max_partitions_per_crawl:
            return None
        emitted += 1
        return spec

    entries = (parse_cluster_line(raw, max_block_bytes=max_block_bytes) for raw in lines if raw.strip())
    for entry, next_key in _with_next_key(entries):
        if entry.cdx_file != current_file:
            if group:
                spec = emit()
                if spec:
                    yield spec
            if entry.cdx_file in seen_files:
                raise DiscoveryError(f"cluster.idx lines of {entry.cdx_file} are not contiguous")
            seen_files.add(entry.cdx_file)
            current_file, local_index, expected_offset, group = entry.cdx_file, 0, 0, []
            group_selected = False
        if entry.offset != expected_offset:
            raise DiscoveryError(f"{entry.cdx_file}: block {local_index} starts at {entry.offset}, "
                                 f"expected {expected_offset} (blocks must be contiguous)")
        if not group:
            group_first = local_index
            group_selected = False
        group.append((entry.offset, entry.length))
        if selector is not None and not group_selected:
            group_selected = selector(entry.key, next_key)
        expected_offset = entry.offset + entry.length
        local_index += 1
        if len(group) == size:
            spec = emit()
            if spec:
                yield spec
            group = []
        if planning.max_partitions_per_crawl and emitted >= planning.max_partitions_per_crawl:
            return
    if group:
        spec = emit()
        if spec:
            yield spec


# --------------------------------------------------------------------------- #
# Planner service
# --------------------------------------------------------------------------- #

class PlannerQueue(Protocol):
    def transaction(self) -> Any: ...
    def enqueue_many(self, specs: Iterable[JobSpec]) -> list[EnqueueResult]: ...


class CrawlRegistry(Protocol):
    def register_crawl(self, conn: Any, crawl_id: str) -> bool: ...


@dataclass
class CrawlPlanReport:
    crawl_id: str
    crawl_registered: bool = False
    partitions: int = 0
    created: int = 0
    existing: int = 0


@dataclass
class PlanReport:
    filter_version: str
    crawls: list[CrawlPlanReport] = field(default_factory=list)


class CrawlPlanner:
    def __init__(self, client: HttpClient, queue: PlannerQueue, registry: CrawlRegistry, *,
                 planning: PlanningConfig, filter_config: FilterConfig, policy: RetryPolicy,
                 max_block_bytes: int, run_id: uuid.UUID | None = None,
                 progress: Callable[[str], None] | None = None) -> None:
        self.client, self.queue, self.registry = client, queue, registry
        self.planning, self.filter_config, self.policy = planning, filter_config, policy
        self.max_block_bytes = max_block_bytes
        self.run_id = run_id
        self.progress = progress or (lambda _msg: None)

    def available_crawls(self) -> list[str]:
        return parse_collinfo(self.client.get_bytes(self.client.endpoints.collinfo_url(),
                                                    max_bytes=MAX_COLLINFO_BYTES))

    def selected_crawls(self) -> list[str]:
        return select_crawls(self.available_crawls(), self.planning)

    def plan(self, crawl_ids: Sequence[str] | None = None) -> PlanReport:
        """Plan the configured window, or ``crawl_ids``. Every crawl must be listed by the index server
        before anything is written (``cc_crawls`` rows are insert-only)."""
        available = self.available_crawls()
        planning = self.planning if crawl_ids is None else replace(self.planning, explicit_crawls=tuple(crawl_ids))
        report = PlanReport(self.filter_config.version)
        for crawl_id in select_crawls(available, planning):
            report.crawls.append(self._plan_crawl(crawl_id))
        return report

    def _plan_crawl(self, crawl_id: str) -> CrawlPlanReport:
        result = CrawlPlanReport(crawl_id)
        with self.queue.transaction() as conn:
            result.crawl_registered = self.registry.register_crawl(conn, crawl_id)
        lines = self.client.iter_lines(self.client.endpoints.data_url(cluster_idx_key(crawl_id)),
                                       max_line_bytes=MAX_CLUSTER_LINE_BYTES, max_total_bytes=MAX_CLUSTER_IDX_BYTES)
        batch: list[JobSpec] = []
        selector = block_selector(self.filter_config)
        priority = REFERENCE_PRIORITY if self.filter_config.profile == "reference" else OPEN_PRIORITY
        for spec in plan_partitions(lines, crawl_id, self.planning, max_block_bytes=self.max_block_bytes,
                                    selector=selector):
            batch.append(JobSpec(
                job_type=JobType.CC_INDEX_PARTITION, source=JobSource.COMMON_CRAWL, priority=priority,
                source_identifier=spec.source_identifier(self.filter_config), max_attempts=self.policy.max_attempts,
                stage=Stage.DISCOVERED, checkpoint=spec.initial_checkpoint(self.filter_config), run_id=self.run_id))
            result.partitions += 1
            if len(batch) >= self.planning.enqueue_batch_size:
                self._flush(batch, result)
        self._flush(batch, result)
        return result

    def _flush(self, batch: list[JobSpec], result: CrawlPlanReport) -> None:
        if not batch:
            return
        for outcome in self.queue.enqueue_many(batch):
            if outcome.created:
                result.created += 1
            else:
                result.existing += 1
        batch.clear()
        self.progress(f"{result.crawl_id}: {result.partitions} partitions planned "
                      f"({result.created} new, {result.existing} existing)")
