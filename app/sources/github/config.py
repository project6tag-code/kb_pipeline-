"""GitHub Acquisition V1 configuration (environment variables, bounded, token-safe).

Authentication
* ``KB_GH_TOKEN`` (optional): a GitHub token for production throughput. Never
  logged, never part of ``repr``; only sent in the ``Authorization`` header to
  ``api.github.com``. Without it the API allows ~60 requests/hour (smoke tests only).
* ``KB_GH_TOKEN_IDENTITY`` (optional): a non-secret label for the token (e.g. the
  owning account) used for the ``source_rate_limits.credential_fingerprint``;
  default: a hash of a hash of the token.

Discovery (automated; no topic query) - part of the discovery policy version
* ``KB_GH_CREATED_FROM`` (2010-01-01), ``KB_GH_WINDOW_DAYS`` (30): creation-date windows up to today
* ``KB_GH_MIN_STARS`` (50), ``KB_GH_MAX_REPO_SIZE_KB`` (200000)
* ``KB_GH_ALLOW_FORKS`` (false), ``KB_GH_ALLOW_ARCHIVED`` (false)
* ``KB_GH_LANGUAGES`` (any), ``KB_GH_LICENSES`` (any SPDX id, incl. none; O-7 open)
* ``KB_GH_DISCOVERY_EPOCH`` (``auto`` = ISO year-week): re-discovery generation
* ``KB_GH_MAX_WINDOWS`` (0 = all)

Files - part of the file policy version
* ``KB_GH_FILE_EXTENSIONS``, ``KB_GH_EXCLUDED_DIRS``: comma-separated overrides
* ``KB_GH_MAX_FILE_BYTES`` (1048576), ``KB_GH_MAX_FILES_PER_SNAPSHOT`` (2000),
  ``KB_GH_MAX_TREE_ENTRIES`` (100000)

Resources
* ``KB_GH_BRONZE_ROOT`` (``data/bronze/github``), ``KB_GH_MIN_FREE_BYTES`` (10 GiB)
* ``KB_GH_FETCH_CONCURRENCY`` (2, 1..16), ``KB_GH_BATCH_SIZE`` (50)
* ``KB_GH_QUOTA_RESERVE`` (50): stop and defer when this many requests remain
* ``KB_GH_CONNECT_TIMEOUT_SECONDS`` (10), ``KB_GH_READ_TIMEOUT_SECONDS`` (30),
  ``KB_GH_REQUEST_DEADLINE_SECONDS`` (120)
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from app.config.settings import SettingsError

DEFAULT_USER_AGENT = "kb-pipeline-gh-acquisition/1.0 (bounded public-repository snapshots)"

DEFAULT_EXTENSIONS = (
    # documentation
    ".md", ".markdown", ".mdx", ".rst", ".txt", ".adoc", ".asciidoc", ".org", ".tex",
    # source code
    ".py", ".js", ".mjs", ".ts", ".java", ".go", ".rs", ".c", ".h", ".cc", ".cpp", ".hpp", ".cs", ".rb",
    ".php", ".swift", ".kt", ".scala", ".sh", ".sql", ".r", ".jl", ".hs", ".lua", ".pl", ".ex", ".erl",
    ".clj", ".dart", ".ml",
    # structured text
    ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".xml", ".csv",
)
DEFAULT_FILENAMES = ("readme", "license", "copying", "notice", "authors", "contributing", "changelog",
                     "history", "install", "makefile", "dockerfile")
DEFAULT_EXCLUDED_DIRS = (
    ".git", ".github", ".svn", ".hg", ".idea", ".vscode", ".venv", "venv", "env", "node_modules",
    "bower_components", "jspm_packages", "vendor", "vendors", "third_party", "third-party", "thirdparty",
    "3rdparty", "external", "deps", "site-packages", "__pycache__", "dist", "build", "out", "target",
    "bin", "obj", "coverage", ".next", ".nuxt", ".cache", "pods", "carthage", "generated",
    "test", "tests", "__tests__", "spec", "specs", "fixtures", "mock", "mocks",
    "example", "examples", "demo", "demos", "sample", "samples",
)


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
    if name not in env or not env[name].strip():
        return default
    return tuple(sorted({item.strip().lower() for item in env[name].split(",") if item.strip()}))


def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:10]


@dataclass(frozen=True)
class RepositoryPolicy:
    """Search qualifiers + post-search qualification. ``version`` is part of every discovery job id."""

    min_stars: int = 50
    max_size_kb: int = 200_000
    allow_forks: bool = False
    allow_archived: bool = False
    languages: tuple[str, ...] = ()   # lower-case GitHub language names; empty = any
    licenses: tuple[str, ...] = ()    # lower-case SPDX ids; empty = any (O-7: user decision)

    def __post_init__(self) -> None:
        if not 0 <= self.min_stars <= 10_000_000:
            raise SettingsError("min_stars must be between 0 and 10000000")
        if not 1 <= self.max_size_kb <= 100_000_000:
            raise SettingsError("max_size_kb must be between 1 and 100000000")
        for value in self.languages + self.licenses:
            if not re.fullmatch(r"[a-z0-9+#._ -]{1,50}", value):
                raise SettingsError(f"invalid language/license filter {value!r}")

    def search_qualifiers(self) -> str:
        parts = ["is:public", f"fork:{'true' if self.allow_forks else 'false'}", f"stars:>={self.min_stars}",
                 f"size:<={self.max_size_kb}"]
        if not self.allow_archived:
            parts.append("archived:false")
        return " ".join(parts)

    @property
    def version(self) -> str:
        return "r1" + _hash({"q": self.search_qualifiers(), "languages": list(self.languages),
                             "licenses": list(self.licenses)})


@dataclass(frozen=True)
class DiscoveryConfig:
    created_from: date = date(2010, 1, 1)
    window_days: int = 30
    epoch: str = "auto"
    max_windows: int = 0
    policy: RepositoryPolicy = field(default_factory=RepositoryPolicy)

    def __post_init__(self) -> None:
        if not 1 <= self.window_days <= 3660:
            raise SettingsError("window_days must be between 1 and 3660")
        if self.created_from < date(2007, 10, 1):
            raise SettingsError("created_from predates GitHub")
        if self.epoch != "auto" and not re.fullmatch(r"[A-Za-z0-9]{1,20}", self.epoch):
            raise SettingsError("epoch must be 'auto' or 1..20 alphanumerics")
        if self.max_windows < 0:
            raise SettingsError("max_windows must be >= 0")

    def resolved_epoch(self, today: date | None = None) -> str:
        if self.epoch != "auto":
            return self.epoch
        year, week, _ = (today or datetime.now(timezone.utc).date()).isocalendar()
        return f"{year}w{week:02d}"


@dataclass(frozen=True)
class FilePolicy:
    extensions: tuple[str, ...] = DEFAULT_EXTENSIONS
    filenames: tuple[str, ...] = DEFAULT_FILENAMES
    excluded_dirs: tuple[str, ...] = DEFAULT_EXCLUDED_DIRS
    max_file_bytes: int = 1024 * 1024
    max_files_per_snapshot: int = 2000

    def __post_init__(self) -> None:
        for name in ("extensions", "filenames", "excluded_dirs"):
            object.__setattr__(self, name, tuple(sorted({v.lower() for v in getattr(self, name)})))
        if any(not e.startswith(".") for e in self.extensions):
            raise SettingsError("file extensions must start with '.'")
        if not 1 <= self.max_file_bytes <= 50 * 1024 * 1024:
            raise SettingsError("max_file_bytes must be between 1 and 50 MiB")
        if not 1 <= self.max_files_per_snapshot <= 1_000_000:
            raise SettingsError("max_files_per_snapshot must be between 1 and 1000000")

    @property
    def version(self) -> str:
        return "p1" + _hash({"ext": list(self.extensions), "names": list(self.filenames),
                             "dirs": list(self.excluded_dirs), "max": self.max_file_bytes,
                             "files": self.max_files_per_snapshot})


@dataclass(frozen=True)
class GitHubHttpConfig:
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0
    request_deadline_seconds: float = 120.0
    max_connections: int = 2
    user_agent: str = DEFAULT_USER_AGENT
    quota_reserve: int = 50

    def __post_init__(self) -> None:
        if not 0 < self.connect_timeout_seconds <= 120 or not 0 < self.read_timeout_seconds <= 600:
            raise SettingsError("timeouts must be positive and bounded")
        if not self.read_timeout_seconds <= self.request_deadline_seconds <= 3600:
            raise SettingsError("request deadline must be >= read timeout and <= 3600 seconds")
        if not 1 <= self.max_connections <= 16:
            raise SettingsError("max_connections must be between 1 and 16")
        if not 0 <= self.quota_reserve <= 1000:
            raise SettingsError("quota_reserve must be between 0 and 1000")


@dataclass(frozen=True)
class GitHubAcquisitionConfig:
    bronze_root: Path = Path("data/bronze/github")
    min_free_bytes: int = 10 * 1024 ** 3
    fetch_concurrency: int = 2
    batch_size: int = 50
    max_tree_entries: int = 100_000
    max_tree_bytes: int = 64 * 1024 * 1024
    files: FilePolicy = field(default_factory=FilePolicy)

    def __post_init__(self) -> None:
        object.__setattr__(self, "bronze_root", Path(self.bronze_root))
        if not 1 <= self.fetch_concurrency <= 16:
            raise SettingsError("fetch_concurrency must be between 1 and 16")
        if not 1 <= self.batch_size <= 1000:
            raise SettingsError("batch_size must be between 1 and 1000")
        if not 1 <= self.max_tree_entries <= 1_000_000:
            raise SettingsError("max_tree_entries must be between 1 and 1000000")
        if self.min_free_bytes < 0:
            raise SettingsError("min_free_bytes must be >= 0")

    @property
    def window(self) -> int:
        return self.fetch_concurrency * 2


@dataclass(frozen=True)
class GitHubCredentials:
    token: str | None = field(default=None, repr=False)
    identity: str | None = None

    def __post_init__(self) -> None:
        if self.token is not None and (not self.token or any(c.isspace() for c in self.token)
                                       or len(self.token) > 500):
            raise SettingsError("KB_GH_TOKEN is malformed (value not shown)")

    def __repr__(self) -> str:
        return f"GitHubCredentials(token={'<set>' if self.token else None}, identity={self.identity!r})"

    @property
    def fingerprint_identity(self) -> str | None:
        """Non-secret identity for source_rate_limits (never the token itself)."""
        if self.token is None:
            return None
        return self.identity or "token-sha256:" + hashlib.sha256(self.token.encode()).hexdigest()


@dataclass(frozen=True)
class GitHubSettings:
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    acquisition: GitHubAcquisitionConfig = field(default_factory=GitHubAcquisitionConfig)
    http: GitHubHttpConfig = field(default_factory=GitHubHttpConfig)
    credentials: GitHubCredentials = field(default_factory=GitHubCredentials)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "GitHubSettings":
        env = os.environ if environ is None else environ
        created = env.get("KB_GH_CREATED_FROM", "").strip()
        try:
            created_from = date.fromisoformat(created) if created else date(2010, 1, 1)
        except ValueError:
            raise SettingsError("KB_GH_CREATED_FROM must be YYYY-MM-DD") from None
        concurrency = _int(env, "KB_GH_FETCH_CONCURRENCY", 2, 1, 16)
        files = FilePolicy(
            extensions=_list(env, "KB_GH_FILE_EXTENSIONS", DEFAULT_EXTENSIONS),
            excluded_dirs=_list(env, "KB_GH_EXCLUDED_DIRS", DEFAULT_EXCLUDED_DIRS),
            max_file_bytes=_int(env, "KB_GH_MAX_FILE_BYTES", 1024 * 1024, 1, 50 * 1024 * 1024),
            max_files_per_snapshot=_int(env, "KB_GH_MAX_FILES_PER_SNAPSHOT", 2000, 1, 1_000_000),
        )
        token = env.get("KB_GH_TOKEN", "").strip() or None
        return cls(
            discovery=DiscoveryConfig(
                created_from=created_from,
                window_days=_int(env, "KB_GH_WINDOW_DAYS", 30, 1, 3660),
                epoch=env.get("KB_GH_DISCOVERY_EPOCH", "").strip() or "auto",
                max_windows=_int(env, "KB_GH_MAX_WINDOWS", 0, 0, 1_000_000),
                policy=RepositoryPolicy(
                    min_stars=_int(env, "KB_GH_MIN_STARS", 50, 0, 10_000_000),
                    max_size_kb=_int(env, "KB_GH_MAX_REPO_SIZE_KB", 200_000, 1, 100_000_000),
                    allow_forks=_bool(env, "KB_GH_ALLOW_FORKS", False),
                    allow_archived=_bool(env, "KB_GH_ALLOW_ARCHIVED", False),
                    languages=_list(env, "KB_GH_LANGUAGES", ()),
                    licenses=_list(env, "KB_GH_LICENSES", ()),
                ),
            ),
            acquisition=GitHubAcquisitionConfig(
                bronze_root=Path(env.get("KB_GH_BRONZE_ROOT", "").strip() or "data/bronze/github"),
                min_free_bytes=_int(env, "KB_GH_MIN_FREE_BYTES", 10 * 1024 ** 3, 0, 2 ** 62),
                fetch_concurrency=concurrency,
                batch_size=_int(env, "KB_GH_BATCH_SIZE", 50, 1, 1000),
                max_tree_entries=_int(env, "KB_GH_MAX_TREE_ENTRIES", 100_000, 1, 1_000_000),
                files=files,
            ),
            http=GitHubHttpConfig(
                connect_timeout_seconds=_float(env, "KB_GH_CONNECT_TIMEOUT_SECONDS", 10.0, 0.1, 120.0),
                read_timeout_seconds=_float(env, "KB_GH_READ_TIMEOUT_SECONDS", 30.0, 0.1, 600.0),
                request_deadline_seconds=_float(env, "KB_GH_REQUEST_DEADLINE_SECONDS", 120.0, 0.1, 3600.0),
                max_connections=concurrency,
                quota_reserve=_int(env, "KB_GH_QUOTA_RESERVE", 50, 0, 1000),
            ),
            credentials=GitHubCredentials(token=token,
                                          identity=env.get("KB_GH_TOKEN_IDENTITY", "").strip() or None),
        )
