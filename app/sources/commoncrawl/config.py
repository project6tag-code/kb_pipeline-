"""Common Crawl Acquisition V1 configuration (environment variables, bounded).

Every value has a safe default for one-PC operation and is range-checked.
Nothing here widens the endpoint allow-list: the Common Crawl hosts are fixed
in ``app.sources.commoncrawl.http`` and cannot be configured.

Environment variables (defaults in parentheses):

Crawl selection and planning
* ``KB_CC_CRAWL_WINDOW`` (1, 1..50): plan the latest N crawls listed by the
  Common Crawl index server (no crawl id is hard-coded).
* ``KB_CC_CRAWLS`` (unset): optional comma-separated explicit crawl ids; each
  must also be listed by the index server.
* ``KB_CC_BLOCKS_PER_PARTITION`` (10, 1..200): CDX blocks (~3000 index lines
  each) per ``cc_index_partition`` job.
* ``KB_CC_PARTITION_STRIDE`` (1, 1..100000): plan every Nth partition
  (deterministic sampling for bounded pilots).
* ``KB_CC_MAX_PARTITIONS_PER_CRAWL`` (0 = no limit).

Candidate filter (part of the filter version, hence of the job identity)
* ``KB_CC_PROFILE`` (``reference``): ``reference`` plans and keeps only the curated
  reference publishers of ``reference_domains_v1.json`` (or ``KB_CC_REFERENCE_DOMAINS``,
  a comma list); ``open`` samples the whole crawl. Both apply the strict URL policy
  (``app.sources.commoncrawl.index.url_rejection``).
* ``KB_CC_MIME_TYPES`` (``text/html,application/xhtml+xml``)
* ``KB_CC_LANGUAGES`` (``eng``; empty disables): keep a record when the index
  lists one of these languages, or lists no language at all.
* ``KB_CC_REJECT_TRUNCATED`` (true)
* ``KB_CC_MAX_RECORD_BYTES`` (5242880, 1024..67108864): compressed WARC record limit.

Acquisition resources
* ``KB_CC_BRONZE_ROOT`` (``data/bronze/commoncrawl``)
* ``KB_CC_MIN_FREE_BYTES`` (10 GiB): disk-space guard for Bronze writes.
* ``KB_CC_FETCH_CONCURRENCY`` (4, 1..32): parallel WARC range requests per worker.
* ``KB_CC_BATCH_SIZE`` (50, 1..1000): index lines committed per transaction.
* ``KB_CC_CONNECT_TIMEOUT_SECONDS`` (10), ``KB_CC_READ_TIMEOUT_SECONDS`` (30),
  ``KB_CC_REQUEST_DEADLINE_SECONDS`` (120): per-request limits.
* ``KB_CC_MAX_DECOMPRESSED_RECORD_BYTES`` (33554432): decompression limit per record.
* ``KB_CC_ITEM_TRANSIENT_LIMIT`` (3, 1..20): unit retries caused by the same
  record before that record is dead-lettered.
* ``KB_CC_USER_AGENT``: HTTP User-Agent.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from app.config.settings import SettingsError

_CRAWL_ID = re.compile(r"^CC-MAIN-[0-9]{4}-[0-9]{2}$")
_MIME = re.compile(r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$")
_LANGUAGE = re.compile(r"^[a-z]{3}$")

FILTER_SPEC_VERSION = "f2"
URL_POLICY_VERSION = "url2"  # url1 + bare site homepages
PROFILES = ("open", "reference")
REFERENCE_DOMAINS_FILE = Path(__file__).with_name("reference_domains_v1.json")
_DOMAIN = re.compile(r"^(?=.{1,253}$)([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")


def load_reference_domains(path: Path = REFERENCE_DOMAINS_FILE) -> tuple[str, ...]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return tuple(sorted({str(d).strip().lower() for d in data["domains"] if str(d).strip()}))
DEFAULT_USER_AGENT = "kb-pipeline-cc-acquisition/1.0 (bounded research crawler; byte-range fetches only)"


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


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise SettingsError(f"{name} must be true or false")


def _list(env: Mapping[str, str], name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    if name not in env:
        return default
    return tuple(sorted({item.strip().lower() for item in env[name].split(",") if item.strip()}))


@dataclass(frozen=True)
class FilterConfig:
    """Index-level candidate filter. Its ``version`` is part of every partition job identity.

    Changing any field yields a new ``version`` and therefore new
    ``cc_index_partition`` jobs; observations stay idempotent regardless.
    """

    mime_types: tuple[str, ...] = ("application/xhtml+xml", "text/html")
    languages: tuple[str, ...] = ("eng",)
    reject_truncated: bool = True
    max_record_bytes: int = 5 * 1024 * 1024
    required_status: int = 200
    profile: str = "open"
    reference_domains: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "mime_types", tuple(sorted(set(self.mime_types))))
        object.__setattr__(self, "languages", tuple(sorted(set(self.languages))))
        object.__setattr__(self, "reference_domains", tuple(sorted({d.strip().lower().removeprefix("www.")
                                                                    for d in self.reference_domains if d.strip()})))
        if self.profile not in PROFILES:
            raise SettingsError(f"profile must be one of {PROFILES}, got {self.profile!r}")
        if self.profile == "reference" and not self.reference_domains:
            raise SettingsError("the reference profile needs at least one reference domain")
        if self.profile == "open" and self.reference_domains:
            raise SettingsError("the open profile takes no reference domains")
        for domain in self.reference_domains:
            if not _DOMAIN.match(domain):
                raise SettingsError(f"invalid reference domain {domain!r}")
        if not self.mime_types:
            raise SettingsError("at least one MIME type must be allowed")
        for mime in self.mime_types:
            if not _MIME.match(mime):
                raise SettingsError(f"invalid MIME type {mime!r}")
        for language in self.languages:
            if not _LANGUAGE.match(language):
                raise SettingsError(f"languages must be ISO 639-3 codes such as 'eng', got {language!r}")
        if not 1024 <= self.max_record_bytes <= 64 * 1024 * 1024:
            raise SettingsError("max_record_bytes must be between 1 KiB and 64 MiB")
        if self.required_status != 200:
            raise SettingsError("Acquisition V1 accepts HTTP 200 captures only")

    def canonical(self) -> str:
        return json.dumps({
            "spec": FILTER_SPEC_VERSION,
            "status": self.required_status,
            "mime_types": list(self.mime_types),
            "languages": list(self.languages),
            "reject_truncated": self.reject_truncated,
            "max_record_bytes": self.max_record_bytes,
            "url_policy": URL_POLICY_VERSION,
            "profile": self.profile,
            "reference_domains": list(self.reference_domains),
        }, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

    @property
    def version(self) -> str:
        """Deterministic, e.g. ``f2.3c9a0e1b2d4f``: spec version + hash of the parameters."""
        return f"{FILTER_SPEC_VERSION}.{hashlib.sha256(self.canonical().encode('ascii')).hexdigest()[:12]}"


@dataclass(frozen=True)
class PlanningConfig:
    crawl_window: int = 1
    explicit_crawls: tuple[str, ...] = ()
    blocks_per_partition: int = 10
    partition_stride: int = 1
    max_partitions_per_crawl: int = 0  # 0 = unlimited
    enqueue_batch_size: int = 200

    def __post_init__(self) -> None:
        if not 1 <= self.crawl_window <= 50:
            raise SettingsError("crawl_window must be between 1 and 50")
        for crawl_id in self.explicit_crawls:
            if not _CRAWL_ID.match(crawl_id):
                raise SettingsError(f"crawl ids must match CC-MAIN-YYYY-WW, got {crawl_id!r}")
        if not 1 <= self.blocks_per_partition <= 200:
            raise SettingsError("blocks_per_partition must be between 1 and 200")
        if not 1 <= self.partition_stride <= 100_000:
            raise SettingsError("partition_stride must be between 1 and 100000")
        if self.max_partitions_per_crawl < 0:
            raise SettingsError("max_partitions_per_crawl must be >= 0")
        if not 1 <= self.enqueue_batch_size <= 1000:
            raise SettingsError("enqueue_batch_size must be between 1 and 1000")


@dataclass(frozen=True)
class HttpConfig:
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0
    request_deadline_seconds: float = 120.0
    max_connections_per_host: int = 4
    user_agent: str = DEFAULT_USER_AGENT

    def __post_init__(self) -> None:
        if not 0 < self.connect_timeout_seconds <= 120:
            raise SettingsError("connect timeout must be in (0, 120] seconds")
        if not 0 < self.read_timeout_seconds <= 600:
            raise SettingsError("read timeout must be in (0, 600] seconds")
        if not self.read_timeout_seconds <= self.request_deadline_seconds <= 3600:
            raise SettingsError("request deadline must be >= read timeout and <= 3600 seconds")
        if not 1 <= self.max_connections_per_host <= 64:
            raise SettingsError("max_connections_per_host must be between 1 and 64")
        if not self.user_agent or len(self.user_agent) > 200 or any(ord(c) < 32 or ord(c) > 126 for c in self.user_agent):
            raise SettingsError("user agent must be 1..200 printable ASCII characters")


@dataclass(frozen=True)
class AcquisitionConfig:
    bronze_root: Path = Path("data/bronze/commoncrawl")
    min_free_bytes: int = 10 * 1024 ** 3
    fetch_concurrency: int = 4
    batch_size: int = 50
    max_decompressed_record_bytes: int = 32 * 1024 * 1024
    max_index_block_bytes: int = 16 * 1024 * 1024
    max_index_block_decompressed_bytes: int = 128 * 1024 * 1024
    max_index_line_bytes: int = 64 * 1024
    item_transient_limit: int = 3
    max_dead_letters_per_attempt: int = 1000
    verify_payload_digest: bool = True
    filter: FilterConfig = field(default_factory=FilterConfig)

    def __post_init__(self) -> None:
        object.__setattr__(self, "bronze_root", Path(self.bronze_root))
        if self.min_free_bytes < 0:
            raise SettingsError("min_free_bytes must be >= 0")
        if not 1 <= self.fetch_concurrency <= 32:
            raise SettingsError("fetch_concurrency must be between 1 and 32")
        if not 1 <= self.batch_size <= 1000:
            raise SettingsError("batch_size must be between 1 and 1000")
        if not self.filter.max_record_bytes <= self.max_decompressed_record_bytes <= 512 * 1024 * 1024:
            raise SettingsError("max_decompressed_record_bytes must be >= max_record_bytes and <= 512 MiB")
        if not 1024 <= self.max_index_block_bytes <= 256 * 1024 * 1024:
            raise SettingsError("max_index_block_bytes must be between 1 KiB and 256 MiB")
        if not self.max_index_block_bytes <= self.max_index_block_decompressed_bytes <= 1024 ** 3:
            raise SettingsError("max_index_block_decompressed_bytes must be >= max_index_block_bytes and <= 1 GiB")
        if not 1024 <= self.max_index_line_bytes <= 1024 * 1024:
            raise SettingsError("max_index_line_bytes must be between 1 KiB and 1 MiB")
        if not 1 <= self.item_transient_limit <= 20:
            raise SettingsError("item_transient_limit must be between 1 and 20")
        if not 0 <= self.max_dead_letters_per_attempt <= 100_000:
            raise SettingsError("max_dead_letters_per_attempt must be between 0 and 100000")

    @property
    def window(self) -> int:
        """In-flight fetches (submitted, not yet consumed): bounded backpressure."""
        return self.fetch_concurrency * 2


@dataclass(frozen=True)
class CCSettings:
    planning: PlanningConfig = field(default_factory=PlanningConfig)
    acquisition: AcquisitionConfig = field(default_factory=AcquisitionConfig)
    http: HttpConfig = field(default_factory=HttpConfig)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "CCSettings":
        env = os.environ if environ is None else environ
        explicit = tuple(c.strip() for c in env.get("KB_CC_CRAWLS", "").split(",") if c.strip())
        languages = _list(env, "KB_CC_LANGUAGES", ("eng",))
        profile = env.get("KB_CC_PROFILE", "").strip().lower() or "reference"
        reference_domains: tuple[str, ...] = ()
        if profile == "reference":
            reference_domains = (_list(env, "KB_CC_REFERENCE_DOMAINS", ()) if env.get("KB_CC_REFERENCE_DOMAINS", "").strip()
                                 else load_reference_domains())
        filter_config = FilterConfig(
            mime_types=_list(env, "KB_CC_MIME_TYPES", ("application/xhtml+xml", "text/html")),
            languages=languages,
            reject_truncated=_bool(env, "KB_CC_REJECT_TRUNCATED", True),
            max_record_bytes=_int(env, "KB_CC_MAX_RECORD_BYTES", 5 * 1024 * 1024, 1024, 64 * 1024 * 1024),
            profile=profile,
            reference_domains=reference_domains,
        )
        concurrency = _int(env, "KB_CC_FETCH_CONCURRENCY", 4, 1, 32)
        return cls(
            planning=PlanningConfig(
                crawl_window=_int(env, "KB_CC_CRAWL_WINDOW", 1, 1, 50),
                explicit_crawls=explicit,
                blocks_per_partition=_int(env, "KB_CC_BLOCKS_PER_PARTITION", 10, 1, 200),
                partition_stride=_int(env, "KB_CC_PARTITION_STRIDE", 1, 1, 100_000),
                max_partitions_per_crawl=_int(env, "KB_CC_MAX_PARTITIONS_PER_CRAWL", 0, 0, 10_000_000),
            ),
            acquisition=AcquisitionConfig(
                bronze_root=Path(env.get("KB_CC_BRONZE_ROOT", "").strip() or "data/bronze/commoncrawl"),
                min_free_bytes=_int(env, "KB_CC_MIN_FREE_BYTES", 10 * 1024 ** 3, 0, 2 ** 62),
                fetch_concurrency=concurrency,
                batch_size=_int(env, "KB_CC_BATCH_SIZE", 50, 1, 1000),
                max_decompressed_record_bytes=_int(env, "KB_CC_MAX_DECOMPRESSED_RECORD_BYTES",
                                                   32 * 1024 * 1024, 1024, 512 * 1024 * 1024),
                item_transient_limit=_int(env, "KB_CC_ITEM_TRANSIENT_LIMIT", 3, 1, 20),
                filter=filter_config,
            ),
            http=HttpConfig(
                connect_timeout_seconds=_float(env, "KB_CC_CONNECT_TIMEOUT_SECONDS", 10.0, 0.1, 120.0),
                read_timeout_seconds=_float(env, "KB_CC_READ_TIMEOUT_SECONDS", 30.0, 0.1, 600.0),
                request_deadline_seconds=_float(env, "KB_CC_REQUEST_DEADLINE_SECONDS", 120.0, 0.1, 3600.0),
                max_connections_per_host=concurrency,
                user_agent=env.get("KB_CC_USER_AGENT", "").strip() or DEFAULT_USER_AGENT,
            ),
        )
