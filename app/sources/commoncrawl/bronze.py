"""Bronze storage: immutable acquisition evidence for Common Crawl records.

Layout (deterministic; derived only from validated identifiers)::

    <bronze_root>/<crawl_id>/<id[0:2]>/<id[2:4]>/<observation_id>.warc.gz   exact fetched byte range
    <bronze_root>/<crawl_id>/<id[0:2]>/<id[2:4]>/<observation_id>.json      manifest (completion marker)

The ``.warc.gz`` file holds the byte-exact compressed WARC record, i.e. the
range ``[warc_offset, warc_offset + warc_length)`` of ``warc_filename``: it can
be re-parsed offline and re-verified against Common Crawl at any time. The
manifest records provenance and the SHA-256 of those bytes.

Write protocol (crash-safe, never overwrites committed evidence)
* write to ``.<name>.tmp-<random>`` in the target directory, flush, fsync;
* publish with a hard link (``os.link``), which fails if the target exists
  ("no clobber"), then remove the temporary name;
* the record is published first, the manifest last: an object is complete only
  when its manifest exists and the record matches the manifest's size and hash.

On a repeat store for the same observation:
* complete with the same bytes: no-op (idempotent);
* complete with different bytes: ``BronzeConflict``, nothing is changed;
* manifest present but the record missing or damaged: the record is restored
  only if the new bytes equal the manifest hash (otherwise ``BronzeConflict``);
* record without manifest (crash between the two writes): the manifest is
  added if the bytes are equal, otherwise ``BronzeConflict``.
"""

from __future__ import annotations

import enum
import hashlib
import json
import os
import re
import shutil
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BRONZE_FORMAT = "kbp-bronze-cc-v1"
_CRAWL_ID = re.compile(r"^CC-MAIN-[0-9]{4}-[0-9]{2}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
MANIFEST_MAX_BYTES = 1024 * 1024
_TEMP_MARK = ".tmp-"


class BronzeError(Exception):
    code = "bronze_error"


class BronzeConflict(BronzeError):
    """Different bytes for an observation whose evidence already exists."""

    code = "bronze_conflict"


class BronzeCorrupt(BronzeError):
    """Unreadable manifest or evidence that fails verification."""

    code = "bronze_corrupt"


class DiskSpaceError(BronzeError):
    code = "disk_space"


class UnsafePath(BronzeError):
    code = "unsafe_path"


class BronzeState(enum.StrEnum):
    MISSING = "missing"
    COMPLETE = "complete"
    RECORD_ONLY = "record_only"          # crash before the manifest was published
    RECORD_DAMAGED = "record_damaged"    # manifest present; record missing or not matching it


@dataclass(frozen=True)
class Inspection:
    state: BronzeState
    record_path: Path
    manifest_path: Path
    manifest: dict[str, Any] | None = None
    record_sha256: str | None = None


@dataclass(frozen=True)
class StoreResult:
    record_path: Path
    manifest_path: Path
    record_sha256: str
    record_bytes: int
    created: bool
    repaired: bool = False


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> tuple[str, int] | None:
    try:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as fh:
            while chunk := fh.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        return digest.hexdigest(), size
    except FileNotFoundError:
        return None


class BronzeStore:
    def __init__(self, root: Path | str, *, min_free_bytes: int = 0,
                 disk_usage: Callable[[str], Any] = shutil.disk_usage) -> None:
        # Absolute and normalized once, lexically. (Path.resolve() of a not-yet-existing
        # directory can differ from its later resolution on Windows, e.g. 8.3 names.)
        self.root = Path(os.path.normpath(os.path.abspath(root)))
        self.min_free_bytes = min_free_bytes
        self._disk_usage = disk_usage

    # -- paths ---------------------------------------------------------------- #

    def paths(self, crawl_id: str, observation_id: uuid.UUID | str) -> tuple[Path, Path]:
        if not isinstance(crawl_id, str) or not _CRAWL_ID.match(crawl_id):
            raise UnsafePath(f"crawl_id must match CC-MAIN-YYYY-WW, got {crawl_id!r}")
        try:
            name = str(uuid.UUID(str(observation_id)))
        except ValueError:
            raise UnsafePath("observation_id must be a UUID") from None
        # Both components are strictly validated (no separators or dots); the
        # lexical containment check is defence in depth.
        directory = Path(os.path.normpath(self.root / crawl_id / name[0:2] / name[2:4]))
        if not directory.is_relative_to(self.root) or directory == self.root:
            raise UnsafePath("resolved Bronze path escapes the Bronze root")
        return directory / f"{name}.warc.gz", directory / f"{name}.json"

    # -- inspection ----------------------------------------------------------- #

    def _read_manifest(self, path: Path, observation_id: str) -> dict[str, Any] | None:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            if len(raw) > MANIFEST_MAX_BYTES:
                raise ValueError("manifest too large")
            manifest = json.loads(raw.decode("utf-8"))
            if (not isinstance(manifest, dict) or manifest.get("format") != BRONZE_FORMAT
                    or manifest.get("observation_id") != observation_id
                    or not isinstance(manifest.get("record_sha256"), str)
                    or not _SHA256.match(manifest["record_sha256"])
                    or isinstance(manifest.get("record_bytes"), bool)
                    or not isinstance(manifest.get("record_bytes"), int)):
                raise ValueError("manifest fields invalid")
        except ValueError as exc:
            raise BronzeCorrupt(f"invalid Bronze manifest {path.name}: {exc}") from None
        return manifest

    def inspect(self, crawl_id: str, observation_id: uuid.UUID | str) -> Inspection:
        record_path, manifest_path = self.paths(crawl_id, observation_id)
        manifest = self._read_manifest(manifest_path, record_path.name[:-len(".warc.gz")])
        found = _file_sha256(record_path)
        if manifest is None:
            if found is None:
                return Inspection(BronzeState.MISSING, record_path, manifest_path)
            return Inspection(BronzeState.RECORD_ONLY, record_path, manifest_path, None, found[0])
        if found is not None and found == (manifest["record_sha256"], manifest["record_bytes"]):
            return Inspection(BronzeState.COMPLETE, record_path, manifest_path, manifest, found[0])
        return Inspection(BronzeState.RECORD_DAMAGED, record_path, manifest_path, manifest,
                          None if found is None else found[0])

    def read_verified(self, crawl_id: str, observation_id: uuid.UUID | str) -> tuple[bytes, dict[str, Any]] | None:
        """The committed record bytes (re-verified against the manifest), or None if not complete."""
        inspection = self.inspect(crawl_id, observation_id)
        if inspection.state is not BronzeState.COMPLETE:
            return None
        data = inspection.record_path.read_bytes()
        if sha256_hex(data) != inspection.manifest["record_sha256"]:  # changed since inspection
            return None
        return data, inspection.manifest

    # -- writing -------------------------------------------------------------- #

    def check_space(self, needed_bytes: int) -> None:
        probe = self.root
        while not probe.exists() and probe.parent != probe:
            probe = probe.parent
        free = self._disk_usage(str(probe)).free
        if free - needed_bytes < self.min_free_bytes:
            raise DiskSpaceError(f"free disk space {free} bytes would fall below the "
                                 f"{self.min_free_bytes}-byte guard")

    def _publish(self, target: Path, data: bytes, *, replace: bool = False) -> bool:
        """Atomically create ``target`` (no clobber unless ``replace``). Returns False if it already existed."""
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.parent / f".{target.name}{_TEMP_MARK}{uuid.uuid4().hex}"
        with temp.open("xb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            if replace:
                os.replace(temp, target)
                return True
            try:
                os.link(temp, target)
            except FileExistsError:
                return False
            return True
        finally:
            temp.unlink(missing_ok=True)
            if hasattr(os, "O_DIRECTORY"):  # POSIX: make the new directory entry durable
                fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)

    def store(self, crawl_id: str, observation_id: uuid.UUID | str, record: bytes,
              manifest: Mapping[str, Any]) -> StoreResult:
        """Store the evidence for one observation idempotently (see the module docstring)."""
        digest, size = sha256_hex(record), len(record)
        inspection = self.inspect(crawl_id, observation_id)
        record_path, manifest_path = inspection.record_path, inspection.manifest_path

        def result(created: bool, repaired: bool = False) -> StoreResult:
            return StoreResult(record_path, manifest_path, digest, size, created, repaired)

        if inspection.state is BronzeState.COMPLETE:
            if inspection.record_sha256 != digest:
                raise BronzeConflict(f"observation {observation_id}: Bronze holds different bytes "
                                     f"(sha256 {inspection.record_sha256[:16]}..., new {digest[:16]}...)")
            return result(False)
        if inspection.state is BronzeState.RECORD_DAMAGED:
            if inspection.manifest["record_sha256"] != digest or inspection.manifest["record_bytes"] != size:
                raise BronzeConflict(f"observation {observation_id}: new bytes differ from the manifest")
            self.check_space(size)
            self._publish(record_path, record, replace=True)  # restore the attested bytes
            return result(False, repaired=True)
        if inspection.state is BronzeState.RECORD_ONLY:
            if inspection.record_sha256 != digest:
                raise BronzeConflict(f"observation {observation_id}: an unconfirmed record with different "
                                     "bytes exists; left for operator review")
        else:
            self.check_space(size + MANIFEST_MAX_BYTES)
            if not self._publish(record_path, record):
                return self.store(crawl_id, observation_id, record, manifest)  # a concurrent writer won
        body = dict(manifest)
        body.update(format=BRONZE_FORMAT, observation_id=str(uuid.UUID(str(observation_id))),
                    record_sha256=digest, record_bytes=size)
        encoded = json.dumps(body, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
                             allow_nan=False).encode("ascii")
        if not self._publish(manifest_path, encoded):
            return self.store(crawl_id, observation_id, record, manifest)  # re-verify the winner
        return result(True)

    # -- maintenance ---------------------------------------------------------- #

    def sweep_temp_files(self, *, older_than_seconds: float = 3600.0, now: float | None = None) -> int:
        """Remove abandoned temporary files (a crash between write and publish). Committed files are untouched."""
        now = time.time() if now is None else now
        removed = 0
        if not self.root.exists():
            return 0
        for path in self.root.rglob(f".*{_TEMP_MARK}*"):
            if path.is_file() and now - path.stat().st_mtime >= older_than_seconds:
                path.unlink(missing_ok=True)
                removed += 1
        return removed
