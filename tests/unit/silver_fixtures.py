"""Offline fixtures for Bronze -> Silver tests (no network, no database).

* ``store_cc`` / ``store_gh``: write Bronze evidence exactly as the acquisition
  handlers do (same stores, same manifest fields) and return the ``SourceRef``.
* ``InMemorySilverStore``: the ``SilverStore`` protocol with the Schema V1
  constraints that matter here (PK/UNIQUE, hash-binding and status CHECKs,
  extraction and decision CHECKs, FKs); it raises like PostgreSQL would.
* Realistic sample documents.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.processing.dedup import near
from app.processing.silver.model import SourceRef
from app.processing.silver.repository import DocumentRow, lineage_key
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.github.bronze import OBSERVATION_FORMAT, GitHubBronze
from app.sources.github.files import git_blob_hash
from app.utils.ids import cc_observation_id, cc_observation_payload, github_observation_id, github_observation_payload

from tests.unit.cc_fixtures import CRAWL, make_warc_record, warc_filename

ARTICLE_BODY = """
<h1>Photosynthesis</h1>
<p>Photosynthesis is the biological process by which green plants, algae and some bacteria convert light energy
into chemical energy. The process takes place mainly in the chloroplasts of leaf cells, where the pigment
chlorophyll absorbs red and blue light. Oxygen is released as a by-product, and it is the source of almost all of
the oxygen in the atmosphere of the Earth.</p>
<h2>Stages of photosynthesis</h2>
<p>Photosynthesis consists of two linked sets of reactions. The light-dependent reactions occur in the thylakoid
membranes and produce ATP and NADPH. The Calvin cycle, which is also known as the light-independent reactions,
takes place in the stroma and uses that chemical energy to fix carbon dioxide into sugars such as glucose.</p>
<ol><li>The light-dependent reactions split water molecules and release oxygen.</li>
<li>The Calvin cycle fixes about 100 billion tonnes of carbon each year into organic compounds.</li></ol>
<table><caption>Inputs and outputs</caption><tr><th>Input</th><th>Output</th></tr>
<tr><td>Carbon dioxide and water</td><td>Glucose</td></tr><tr><td>Light energy</td><td>Oxygen</td></tr></table>
<h2>Importance</h2>
<p>Photosynthesis is the primary source of organic matter for nearly all ecosystems. The rate of photosynthesis
depends on light intensity, temperature and the concentration of carbon dioxide. Plants that use the C4 pathway,
such as maize, are more efficient at high temperatures than plants that use only the C3 pathway. The overall
reaction is usually summarised as six molecules of carbon dioxide and six molecules of water forming one molecule of
glucose and six molecules of oxygen.</p>
"""

CHROME = """
<header class="site-header"><a href="/">Plant Science Hub</a>
<nav><ul><li><a href="/finance">Finance</a></li><li><a href="/stocks">Stock market news</a></li>
<li><a href="/banking">Banking and loans</a></li><li><a href="/tax">Tax and accounting</a></li></ul></nav></header>
<div id="cookie-consent">We use cookies to improve your experience. <button>Accept all</button></div>
<div class="breadcrumbs"><a href="/">Home</a> &gt; <a href="/biology">Biology</a></div>
"""

FOOTER = """
<aside class="sidebar"><h3>Related</h3><ul><li><a href="/r">Cellular respiration</a></li></ul></aside>
<div class="ad-slot">Buy cheap fertilizer now! 50% off today only.</div>
<footer><p>&copy; 2024 Plant Science Hub. All rights reserved.</p><a href="/privacy">Privacy policy</a></footer>
"""


def article_html(body: str = ARTICLE_BODY, *, title: str = "Photosynthesis - Plant Science Hub") -> bytes:
    return (f"<!doctype html><html><head><meta charset='utf-8'><title>{title}</title>"
            "<style>body{font-family:serif}</style><script>window.track('pageview');</script></head><body>"
            f"{CHROME}<main><article>{body}</article></main>{FOOTER}"
            "<script>var s = document.createElement('script');</script></body></html>").encode("utf-8")


def store_cc(bronze: BronzeStore, url: str, body: bytes, *, offset: int = 1000, crawl: str = CRAWL,
             content_type: str = "text/html; charset=utf-8", status: int = 200, languages: str | None = "eng",
             http_headers: dict[str, str] | None = None) -> SourceRef:
    record = make_warc_record(url, body, content_type=content_type, status=status, http_headers=http_headers)
    filename = warc_filename(crawl=crawl)
    oid = cc_observation_id(crawl, filename, offset, len(record))
    fields = {"url": url, "mime": content_type.split(";")[0], "mime-detected": content_type.split(";")[0],
              "status": str(status), "length": str(len(record)), "offset": str(offset), "filename": filename}
    if languages:
        fields["languages"] = languages
    bronze.store(crawl, oid, record, {
        "source_type": "common_crawl", "crawl_id": crawl, "warc_filename": filename, "warc_offset": offset,
        "warc_length": len(record), "identity": cc_observation_payload(crawl, filename, offset, len(record)),
        "source_url": url, "retrieved_at": datetime(2024, 8, 6, tzinfo=timezone.utc).isoformat(),
        "index": {"cdx_key": "cc-index/collections/x/indexes/cdx-00000.gz", "block_offset": 0, "line": 0,
                  "urlkey": "com,example)/" + url.rsplit("/", 1)[-1], "timestamp": "20240805231234",
                  "filter_version": "f1", "fields": fields},
        "html_metadata": {"version": "ccmeta-v1", "title": None, "canonical_url": None},
    })
    return SourceRef(oid, "common_crawl", url, crawl_id=crawl, warc_filename=filename, warc_offset=offset,
                     warc_length=len(record))


def store_gh(bronze: GitHubBronze, path: str, content: bytes | str, *, repository_id: int = 4242,
             commit_sha: str = "c" * 40, owner: str = "octo", repo: str = "docs") -> SourceRef:
    data = content.encode("utf-8") if isinstance(content, str) else content
    blob = git_blob_hash(data, 40)
    oid = github_observation_id(repository_id, commit_sha, path, blob)
    bronze.store_blob(blob, data)
    url = f"https://github.com/{owner}/{repo}/blob/{commit_sha}/{path}"
    bronze.write_once(bronze.observation_path(repository_id, commit_sha, oid), {
        "format": OBSERVATION_FORMAT, "source_type": "github", "observation_id": str(oid),
        "identity": github_observation_payload(repository_id, commit_sha, path, blob),
        "repository_id": repository_id, "owner_login": owner, "repo_name": repo,
        "html_url": f"https://github.com/{owner}/{repo}", "commit_sha": commit_sha, "blob_sha": blob, "path": path,
        "mode": "100644", "size": len(data), "content_sha256": hashlib.sha256(data).hexdigest(), "source_url": url,
        "api_url": "https://api.github.com/x", "retrieved_at": "2024-08-06T00:00:00+00:00",
        "file_policy_version": "p1"})
    return SourceRef(oid, "github", url, repository_id=repository_id, commit_sha=commit_sha, path=path,
                     blob_sha=blob)


class InMemorySilverStore:
    """SilverStore with Schema V1 constraint semantics.

    ``atomic()`` gives PostgreSQL transaction semantics: an exception inside it
    restores every table to its state at the start (the batch transaction of
    the acquisition handler rolls back as a whole).
    """

    _TABLES = ("observations", "documents", "hashes", "extractions", "decisions", "relationships", "dead_letters")

    @contextlib.contextmanager
    def atomic(self):
        saved = {name: copy.deepcopy(getattr(self, name)) for name in self._TABLES}
        try:
            yield None
        except BaseException:
            for name, value in saved.items():
                setattr(self, name, value)
            raise

    def __init__(self) -> None:
        self.observations: dict[uuid.UUID, str] = {}           # observation_id -> lineage key
        self.documents: dict[uuid.UUID, dict] = {}
        self.hashes: dict[bytes, uuid.UUID] = {}
        self.extractions: dict[tuple[uuid.UUID, str], dict] = {}
        self.decisions: dict[tuple[uuid.UUID, int], dict] = {}
        self.relationships: dict[tuple, dict] = {}
        self.dead_letters: dict[tuple[str, str, str], dict] = {}
        self.methods: dict[tuple[str, str], int] = {(near.ALGORITHM, near.VERSION): 1}
        self.locks = 0
        self.fail_on_extraction_insert: int | None = None     # crash injection: raise on the Nth insert
        self._extraction_inserts = 0

    # test helpers
    def observe(self, *refs: SourceRef) -> None:
        for ref in refs:
            self.observations[ref.observation_id] = ref.lineage_key

    def extraction(self, ref: SourceRef, version: str) -> dict | None:
        return self.extractions.get((ref.observation_id, version))

    def document_for(self, ref: SourceRef, version: str) -> dict | None:
        row = self.extraction(ref, version)
        return self.documents.get(row["document_id"]) if row and row["document_id"] else None

    def snapshot(self) -> tuple:
        return (len(self.documents), len(self.extractions), len(self.decisions), len(self.relationships),
                len(self.dead_letters))

    # protocol
    def lock_registry(self, conn) -> None:
        self.locks += 1

    def existing_extractions(self, conn, observation_ids, version):
        return {oid: (self.extractions[(oid, version)]["outcome"], self.extractions[(oid, version)]["document_id"])
                for oid in observation_ids if (oid, version) in self.extractions}

    def existing_observations(self, conn, observation_ids):
        return {oid for oid in observation_ids if oid in self.observations}

    def existing_documents(self, conn, document_ids):
        return {d for d in document_ids if d in self.documents}

    def candidate_texts(self, conn, document_ids):
        return {d: self.documents[d]["canonical_text"] for d in document_ids
                if d in self.documents and self.documents[d]["status"] == "accepted"
                and self.documents[d]["canonical_text"] is not None}

    def lineage(self, conn, document_ids, limit):
        out: dict[uuid.UUID, set[str]] = {}
        wanted = set(document_ids)
        for (oid, _v), row in self.extractions.items():
            if row["document_id"] in wanted:
                out.setdefault(row["document_id"], set()).add(self.observations[oid])
        return out

    def dedup_method_id(self, conn, algorithm, version):
        return self.methods.get((algorithm, version))

    def insert_document(self, conn, row: DocumentRow) -> bool:
        if row.document_id in self.documents:
            return False
        digest = bytes.fromhex(row.content_hash)
        if len(digest) != 32 or digest in self.hashes:
            raise AssertionError("UNIQUE(content_hash) / octet_length CHECK violated")
        text = row.canonical_text
        if text is not None:
            assert hashlib.sha256(text.encode("utf-8")).digest() == digest, "hash-binding CHECK violated"
            assert len(text) == row.char_length, "char_length CHECK violated"
            assert "\x00" not in text
        assert row.char_length > 0
        assert row.status != "accepted" or (text is not None and row.language == "en"), "accepted CHECK violated"
        assert row.status in ("accepted", "rejected_quality", "rejected_near_duplicate", "rejected_factuality",
                              "rejected_policy", "quarantined")
        assert row.first_observation_id in self.observations, "FK first_observation_id"
        assert row.subject_domain is None or len(row.subject_domain) <= 100
        self.documents[row.document_id] = dict(vars(row))
        self.hashes[digest] = row.document_id
        return True

    def insert_extraction(self, conn, observation_id, version, outcome, document_id, was_new) -> bool:
        self._extraction_inserts += 1
        if self.fail_on_extraction_insert == self._extraction_inserts:
            raise ConnectionError("injected crash")
        assert outcome in ("document", "empty", "rejected_language", "rejected_prefilter", "rejected_nul",
                           "extraction_failed")
        assert (outcome == "document") == (document_id is not None)
        assert (document_id is None) == (was_new is None)
        assert observation_id in self.observations, "FK observation"
        assert document_id is None or document_id in self.documents, "FK document"
        key = (observation_id, version)
        if key in self.extractions:
            return False
        self.extractions[key] = {"outcome": outcome, "document_id": document_id, "document_was_new": was_new}
        return True

    def insert_decision(self, conn, document_id, method_id, classification, canonical_document_id, similarity,
                        run_id) -> None:
        assert (classification == "UNIQUE") == (canonical_document_id is None)
        assert canonical_document_id != document_id
        assert canonical_document_id is None or canonical_document_id in self.documents
        self.decisions.setdefault((document_id, method_id), {
            "classification": classification, "canonical_document_id": canonical_document_id,
            "similarity_score": similarity, "decision_status": "confirmed"})

    def insert_relationship(self, conn, document_id, related_document_id, relationship_type, method_id,
                            similarity) -> None:
        assert document_id != related_document_id
        self.relationships.setdefault((document_id, related_document_id, relationship_type, method_id),
                                      {"similarity_score": similarity})

    def dead_letter(self, conn, *, item_type, item_key, stage, disposition, error_class, error_code, error_message,
                    attempt_count, job_id, run_id, payload_ref) -> None:
        key = (str(item_type), item_key, str(stage))
        row = self.dead_letters.setdefault(key, {"occurrence_count": 0})
        row.update(disposition=str(disposition), error_class=str(error_class), error_code=error_code,
                   error_message=error_message, payload_ref=payload_ref)
        row["occurrence_count"] += 1


class Ctx:
    """Minimal handler context for ``SilverProcessor.prepare`` (transaction yields None)."""

    def __init__(self) -> None:
        self.heartbeats = 0

    def transaction(self):
        import contextlib

        return contextlib.nullcontext(None)

    def heartbeat(self) -> None:
        self.heartbeats += 1


def lineage_of(ref: SourceRef) -> str:
    return lineage_key(ref.channel, ref.source_url, ref.repository_id, ref.path)


def tmpdir(test) -> Path:
    import tempfile

    tmp = tempfile.TemporaryDirectory()
    test.addCleanup(tmp.cleanup)
    return Path(tmp.name)
