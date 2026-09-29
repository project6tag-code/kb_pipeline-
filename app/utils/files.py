"""Crash-safe, no-clobber file publication for immutable evidence (Bronze)."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

TEMP_MARK = ".tmp-"


def publish_file(target: Path, data: bytes, *, replace: bool = False) -> bool:
    """Atomically create ``target`` with ``data``.

    Writes a temporary sibling, fsyncs it, then publishes it with ``os.link``
    (fails if ``target`` exists: no clobber) or, only when ``replace`` is set,
    ``os.replace``. Returns False if the target already existed (nothing written).
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.parent / f".{target.name}{TEMP_MARK}{uuid.uuid4().hex}"
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
        if hasattr(os, "O_DIRECTORY"):  # POSIX: make the directory entry durable
            fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
