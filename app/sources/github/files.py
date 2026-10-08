"""Git tree parsing, file qualification, blob integrity and text checks (pure functions).

Qualification happens on tree metadata BEFORE any content is downloaded:
regular files only (symlinks and submodules are never followed), valid
Identity Extension V1 paths without control characters, no excluded
directory segment (vendor, node_modules, build output, ...), no credential
artifact names, no generated/lock/minified files, an allowed extension or
well-known file name, and 1 <= size <= max_file_bytes.

After download: the content must hash to its git blob id
(``SHA1/SHA256("blob <len>\\0" + content)``) and be UTF-8 text without NUL;
otherwise it is never accepted.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from app.sources.github.config import FilePolicy
from app.utils.ids import github_observation_payload

_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
REGULAR_MODES = frozenset({"100644", "100755"})
DOC_EXTENSIONS = frozenset({".md", ".markdown", ".mdx", ".rst", ".txt", ".adoc", ".asciidoc", ".org", ".tex"})

SECRET_NAME = re.compile(
    r"^(\.env(\..*)?|.*\.(pem|key|p12|pfx|jks|keystore|kdbx|ovpn|ppk|asc|gpg)|id_(rsa|dsa|ecdsa|ed25519)(\.pub)?"
    r"|\.npmrc|\.pypirc|\.netrc|\.htpasswd|\.git-credentials|credentials(\..*)?|secrets?(\..*)?"
    r"|.*\.tfstate(\..*)?)$", re.I)
GENERATED_NAME = re.compile(
    r"^(package-lock\.json|yarn\.lock|pnpm-lock\.yaml|cargo\.lock|poetry\.lock|pipfile\.lock|composer\.lock"
    r"|gemfile\.lock|go\.sum|.*\.min\.(js|css)|.*\.map|.*\.bundle\.js|.*\.pb\.go|.*_pb2\.py)$", re.I)


class BlobIntegrityError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class TreeEntry:
    path: str
    mode: str
    type: str
    sha: str
    size: int | None

    @property
    def name(self) -> str:
        return self.path.rsplit("/", 1)[-1]

    @property
    def extension(self) -> str:
        name = self.name.lower()
        return name[name.rfind("."):] if "." in name[1:] else ""


class TreeError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def parse_tree(obj: Any, *, object_length: int, max_entries: int) -> list[TreeEntry]:
    """Validate a recursive git tree response. Raises TreeError if truncated/oversized/malformed."""
    if not isinstance(obj, dict) or not isinstance(obj.get("tree"), list):
        raise TreeError("bad_tree", "tree response has no entry list")
    if obj.get("truncated") is True:
        raise TreeError("tree_truncated", "GitHub truncated the recursive tree (repository too large)")
    if len(obj["tree"]) > max_entries:
        raise TreeError("tree_too_large", f"tree has {len(obj['tree'])} entries (limit {max_entries})")
    entries = []
    for raw in obj["tree"]:
        if not isinstance(raw, dict):
            raise TreeError("bad_tree", "tree entry is not an object")
        path, mode, kind, sha, size = raw.get("path"), raw.get("mode"), raw.get("type"), raw.get("sha"), raw.get("size")
        if (not isinstance(path, str) or not isinstance(mode, str) or kind not in ("blob", "tree", "commit")
                or not isinstance(sha, str) or not _SHA.match(sha) or len(sha) != object_length):
            raise TreeError("bad_tree", "tree entry has invalid fields")
        if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size < 0):
            raise TreeError("bad_tree", "tree entry has an invalid size")
        entries.append(TreeEntry(path, mode, kind, sha, size))
    return entries


def rejection_reason(entry: TreeEntry, policy: FilePolicy, *, commit_sha: str) -> str | None:
    """Why a tree entry is not acquired (None = acquire). Never downloads anything."""
    if entry.type == "commit":
        return "submodule"
    if entry.type != "blob":
        return "not_blob"
    if entry.mode == "120000":
        return "symlink"
    if entry.mode not in REGULAR_MODES:
        return "mode"
    if _CONTROL.search(entry.path) or len(entry.path) > 4096:
        return "unsafe_path"
    try:
        github_observation_payload(1, commit_sha, entry.path, entry.sha)  # Identity Extension V1 path rules
    except (TypeError, ValueError):
        return "invalid_path"
    segments = [s.lower() for s in entry.path.split("/")[:-1]]
    if any(s in policy.excluded_dirs for s in segments):
        return "excluded_dir"
    name = entry.name
    if SECRET_NAME.match(name):
        return "secret_file"
    if GENERATED_NAME.match(name):
        return "generated"
    stem = name.lower().split(".", 1)[0]
    if stem in ("readme", "index"):
        return "readme_or_index"
    if entry.extension not in policy.extensions and not (stem in policy.filenames and entry.extension in ("", ".txt",
                                                                                                         ".md", ".rst")):
        return "extension"
    if not entry.size:
        return "empty"
    if entry.size > policy.max_file_bytes:
        return "too_large"
    return None


def priority(entry: TreeEntry, policy: FilePolicy) -> tuple[int, str]:
    """Deterministic order: documentation first, then everything else, by path."""
    is_doc = entry.extension in DOC_EXTENSIONS or entry.name.lower().split(".", 1)[0] in policy.filenames
    return (0 if is_doc else 1, entry.path)


def git_blob_hash(content: bytes, object_length: int) -> str:
    header = b"blob %d\x00" % len(content)
    digest = hashlib.sha1 if object_length == 40 else hashlib.sha256
    return digest(header + content).hexdigest()


def verify_blob(content: bytes, blob_sha: str, expected_size: int | None) -> None:
    """The content must be exactly the git blob ``blob_sha`` (never accept conflicting content)."""
    if expected_size is not None and len(content) != expected_size:
        raise BlobIntegrityError("blob_size_mismatch", f"content is {len(content)} bytes, tree says {expected_size}")
    if git_blob_hash(content, len(blob_sha)) != blob_sha:
        raise BlobIntegrityError("blob_hash_mismatch", "content does not hash to its git blob id")


def text_rejection(content: bytes) -> str | None:
    """None if the content is UTF-8 text; else why it is binary/unsupported (binary masquerading as text)."""
    if b"\x00" in content:
        return "binary"
    try:
        content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return "not_utf8"
    return None
