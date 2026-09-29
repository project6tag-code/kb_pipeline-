"""Bronze storage for GitHub Acquisition V1 (immutable evidence).

Layout (deterministic; built only from validated identifiers, never from repository paths)::

    <root>/blobs/<b[0:2]>/<b[2:4]>/<blob_sha>                         file content (content-addressed)
    <root>/rejected/<b[0:2]>/<blob_sha>.json                         downloaded but not text (negative cache)
    <root>/repos/<repository_id>/<commit_sha>/snapshot.json          repository + commit + full tree listing
    <root>/repos/<repository_id>/<commit_sha>/files/<observation_id>.json   per-file provenance manifest

* A blob file is valid only if it hashes to its git blob id, so identical
  content is stored once across paths, commits and repositories, and a stored
  blob can never hold conflicting bytes. A damaged blob file is detected on
  read and restored only from content that verifies against the id.
* Manifests are written once (atomic temp + no-clobber hard link); a later
  write for the same identity is a no-op, so the first retrieval's provenance
  is kept. An unparseable manifest is ``BronzeCorrupt``.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from app.sources.commoncrawl.bronze import BronzeCorrupt, DiskSpaceError, UnsafePath
from app.sources.github.files import git_blob_hash
from app.utils.files import publish_file

SNAPSHOT_FORMAT = "kbp-bronze-gh-snapshot-v1"
OBSERVATION_FORMAT = "kbp-bronze-gh-file-v1"
_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
MAX_MANIFEST_BYTES = 256 * 1024 * 1024


def _json(obj: Mapping[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


class GitHubBronze:
    def __init__(self, root: Path | str, *, min_free_bytes: int = 0,
                 disk_usage: Callable[[str], Any] = shutil.disk_usage) -> None:
        self.root = Path(os.path.normpath(os.path.abspath(root)))
        self.min_free_bytes = min_free_bytes
        self._disk_usage = disk_usage

    # -- paths ---------------------------------------------------------------- #

    @staticmethod
    def _sha(value: str) -> str:
        if not isinstance(value, str) or not _SHA.match(value):
            raise UnsafePath("git object id must be 40 or 64 lowercase hex characters")
        return value

    @staticmethod
    def _repo(repository_id: int) -> str:
        if isinstance(repository_id, bool) or not isinstance(repository_id, int) or not 1 <= repository_id < 2 ** 63:
            raise UnsafePath("repository_id must be a positive 64-bit integer")
        return str(repository_id)

    def blob_path(self, blob_sha: str) -> Path:
        sha = self._sha(blob_sha)
        return self.root / "blobs" / sha[0:2] / sha[2:4] / sha

    def rejected_path(self, blob_sha: str) -> Path:
        sha = self._sha(blob_sha)
        return self.root / "rejected" / sha[0:2] / f"{sha}.json"

    def commit_dir(self, repository_id: int, commit_sha: str) -> Path:
        return self.root / "repos" / self._repo(repository_id) / self._sha(commit_sha)

    def snapshot_path(self, repository_id: int, commit_sha: str) -> Path:
        return self.commit_dir(repository_id, commit_sha) / "snapshot.json"

    def observation_path(self, repository_id: int, commit_sha: str, observation_id: uuid.UUID | str) -> Path:
        try:
            name = str(uuid.UUID(str(observation_id)))
        except ValueError:
            raise UnsafePath("observation_id must be a UUID") from None
        return self.commit_dir(repository_id, commit_sha) / "files" / f"{name}.json"

    # -- guards --------------------------------------------------------------- #

    def check_space(self, needed_bytes: int) -> None:
        probe = self.root
        while not probe.exists() and probe.parent != probe:
            probe = probe.parent
        free = self._disk_usage(str(probe)).free
        if free - needed_bytes < self.min_free_bytes:
            raise DiskSpaceError(f"free disk space {free} bytes would fall below the {self.min_free_bytes}-byte guard")

    # -- blobs ---------------------------------------------------------------- #

    def read_blob(self, blob_sha: str) -> bytes | None:
        """The stored content if it verifies against its id; None if absent or damaged."""
        path = self.blob_path(blob_sha)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            return None
        return data if git_blob_hash(data, len(blob_sha)) == blob_sha else None

    def store_blob(self, blob_sha: str, content: bytes) -> str:
        """Store verified content. Returns 'created', 'existing' or 'repaired'."""
        if git_blob_hash(content, len(blob_sha)) != blob_sha:
            raise ValueError("refusing to store content that does not match its blob id")
        path = self.blob_path(blob_sha)
        if self.read_blob(blob_sha) is not None:
            return "existing"
        self.check_space(len(content))
        if path.exists():  # damaged: restore the verified bytes
            publish_file(path, content, replace=True)
            return "repaired"
        return "created" if publish_file(path, content) else "existing"

    def mark_rejected(self, blob_sha: str, reason: str) -> None:
        publish_file(self.rejected_path(blob_sha), _json({"blob_sha": blob_sha, "reason": reason}))

    def rejected_reason(self, blob_sha: str) -> str | None:
        manifest = self.read_json(self.rejected_path(blob_sha))
        return None if manifest is None else str(manifest.get("reason", "rejected"))

    # -- manifests ------------------------------------------------------------ #

    def read_json(self, path: Path) -> dict[str, Any] | None:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            if len(raw) > MAX_MANIFEST_BYTES:
                raise ValueError("too large")
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                raise ValueError("not an object")
            return obj
        except ValueError as exc:
            raise BronzeCorrupt(f"invalid Bronze manifest {path.name}: {exc}") from None

    def write_once(self, path: Path, manifest: Mapping[str, Any]) -> bool:
        """Publish a manifest unless one exists (returns False; the existing one is kept)."""
        if path.exists():
            self.read_json(path)  # an unreadable existing manifest must surface
            return False
        data = _json(manifest)
        self.check_space(len(data))
        return publish_file(path, data)
