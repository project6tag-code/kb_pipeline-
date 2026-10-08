"""DCA_v2 V1 development/test Gold source bridge: the frozen rows and the expected public schema.

This is the bridge contract for ``kb_pipeline_test`` ONLY. Its actual stub
schema is treated as authoritative by explicit project decision. It is not
the historical legacy Gold schema, and nothing here may be applied to
``data_collection_pipeline`` without a separate legacy compatibility review.

Every value has a source; none is invented:

| value | source |
|---|---|
| ``dataset_sources``: name, source_type | project decision; identical to the audited legacy rows (A2) |
| ``dataset_sources``: authentication_required, status | the audited legacy rows of the SAME names (A2): common_crawl ``false`` / ``ACTIVE``, github ``true`` / ``ACTIVE``. All eight audited parents are ``ACTIVE`` |
| ``dataset_sources.id`` | UUIDv5(KB_NAMESPACE, ``dataset-source:v1:<name>``): deterministic, local to this test bridge; consumers look parents up by ``name`` (Identity Extension V1 §26) |
| ``datasets.id`` | the FROZEN Identity Extension V1 bridge ids, UUIDv5(KB_NAMESPACE, ``dataset-bridge:v1:<channel>``) |
| ``datasets.dataset_name`` | project decision: ``Common Crawl``, ``Public GitHub`` |
| ``datasets.status`` | ``KB_REFERENCE``: the DCA_v2 V1 control-plane value (project decision; see the doc) |
| ``datasets.metadata`` | minimal control-plane JSON; no dates, URLs or source facts |
| ``kbp.source_dataset_map`` | Schema V1 §5.x rules: ``dataset_source_name = source_channel``, ``bridge_identity_name = 'dataset-bridge:v1:' || source_channel`` |
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from app.config import target
from app.utils.ids import DATASET_BRIDGE_PREFIX, KB_NAMESPACE, make_dataset_bridge_id

FORBIDDEN_DATABASES = target.FORBIDDEN_DATABASES  # includes data_collection_pipeline


def allowed_databases() -> frozenset[str]:
    """The one authorized bridge target: KB_EXPECTED_DATABASE (default ``kb_pipeline_test``)."""
    return frozenset({target.expected_database()})
EXPECTED_REVISION = "kbp_001e"
STATUS_KB_REFERENCE = "KB_REFERENCE"
LEGACY_WORKFLOW_STATUSES = frozenset({"ACCEPTED", "DISCOVERED", "REJECTED"})  # never used for bridge rows
DATASET_SOURCE_PREFIX = "dataset-source:v1:"

FROZEN_DATASET_IDS = {  # Identity Extension V1 (docs/identity_extension_v1.md), frozen
    "common_crawl": uuid.UUID("a7d8d2e1-8400-5916-9440-b492e8e692ce"),
    "github": uuid.UUID("b487f559-5f8e-56ba-9746-28658b9d09d1"),
}


@dataclass(frozen=True)
class SourceRow:
    id: uuid.UUID
    name: str
    source_type: str
    authentication_required: bool
    status: str


@dataclass(frozen=True)
class DatasetRow:
    id: uuid.UUID
    source_id: uuid.UUID
    dataset_name: str
    status: str
    metadata: dict


@dataclass(frozen=True)
class MapRow:
    source_channel: str
    dataset_source_name: str
    bridge_identity_name: str
    dataset_id: uuid.UUID


@dataclass(frozen=True)
class Channel:
    source: SourceRow
    dataset: DatasetRow
    mapping: MapRow


def dataset_source_id(name: str) -> uuid.UUID:
    return uuid.uuid5(KB_NAMESPACE, DATASET_SOURCE_PREFIX + name)


def _channel(name: str, source_type: str, authentication_required: bool, dataset_name: str) -> Channel:
    dataset_id = make_dataset_bridge_id(name)
    if dataset_id != FROZEN_DATASET_IDS[name]:
        raise AssertionError(f"bridge id for {name} drifted from the frozen Identity Extension V1 value")
    source = SourceRow(dataset_source_id(name), name, source_type, authentication_required, "ACTIVE")
    dataset = DatasetRow(dataset_id, source.id, dataset_name, STATUS_KB_REFERENCE,
                         {"purpose": "dca_v2_gold_bridge", "source_channel": name})
    return Channel(source, dataset, MapRow(name, name, DATASET_BRIDGE_PREFIX + name, dataset_id))


CHANNELS: dict[str, Channel] = {
    "common_crawl": _channel("common_crawl", "document", False, "Common Crawl"),
    "github": _channel("github", "dataset_repository", True, "Public GitHub"),
}


def channel(source_channel: str) -> Channel:
    if source_channel not in CHANNELS:
        raise ValueError(f"unknown source channel {source_channel!r}; the bridge maps only {sorted(CHANNELS)}")
    return CHANNELS[source_channel]


# The ACTUAL kb_pipeline_test public schema (the authoritative V1 contract). Anything else: refuse before writing.
EXPECTED_COLUMNS = {
    "dataset_sources": [("id", "uuid", True), ("name", "character varying(100)", True),
                        ("source_type", "character varying(50)", True), ("authentication_required", "boolean", True),
                        ("status", "character varying(20)", True)],
    "datasets": [("id", "uuid", True), ("source_id", "uuid", True), ("dataset_name", "character varying(255)", True),
                 ("status", "character varying(50)", True), ("metadata", "jsonb", True)],
}
EXPECTED_CONSTRAINTS = {
    "dataset_sources": {("dataset_sources_pkey", "p", "PRIMARY KEY (id)"),
                        ("uq_dataset_sources_name", "u", "UNIQUE (name)")},
    "datasets": {("datasets_pkey", "p", "PRIMARY KEY (id)"),
                 ("datasets_source_id_fkey", "f", "FOREIGN KEY (source_id) REFERENCES dataset_sources(id)")},
}
