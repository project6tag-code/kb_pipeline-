"""kbp Schema V1 — Migration 001A (kbp infrastructure only).

Revision ID: kbp_001a
Revises: (base of the isolated kbp lineage)

Implements the LOCKED docs/database_schema_v1.md §5.1–5.25 (25 tables), the
required indexes of §21.1–21.2, and the runtime grants of
docs/database_privileges_v1.md §5.1.

Deliberately NOT done here:
* no CREATE SCHEMA (kbp is provisioned administratively);
* no roles, no default privileges (provisioning steps, privileges spec §13);
* no DDL or DML on any public object. The only cross-schema dependency is the
  approved FK kbp.source_dataset_map.dataset_id -> public.datasets(id);
* no Gold bridge bootstrap: public.datasets rows are NOT inserted and
  kbp.source_dataset_map stays empty, because datasets.status semantics are
  unresolved (schema §14.3, privileges spec §11);
* no pgvector objects and no ANN index; public.kb is untouched.

All SQL is raw, schema-qualified and executed via op.execute so that it can
be reviewed and checked by app.db.migration_safety.
"""

from __future__ import annotations

revision = "kbp_001a"
down_revision = None
branch_labels = ("kbp",)
depends_on = None

RUNTIME_ROLE = "kb_pipeline_app"

# Creation order respects FK dependencies; downgrade drops in reverse.
KBP_TABLES: tuple[str, ...] = (
    "pipeline_runs",
    "cc_crawls",
    "cc_warc_files",
    "github_repositories",
    "github_commits",
    "source_observations",
    "cc_captures",
    "github_file_observations",
    "document_registry",
    "observation_extractions",
    "dedup_methods",
    "document_dedup_decisions",
    "document_relationships",
    "chunking_versions",
    "chunk_registry",
    "chunk_occurrences",
    "embedding_models",
    "chunk_embeddings",
    "embedding_queue",
    "pipeline_jobs",
    "processing_attempts",
    "dead_letters",
    "source_rate_limits",
    "source_dataset_map",
    "legacy_kb_reconciliation",
)

_ERROR_CLASSES = (
    "'transient', 'rate_limited', 'resource_exhausted', 'permanent_source', "
    "'data_invalid', 'policy_quarantine', 'internal_bug', 'lease_expired'"
)  # schema §17.2

_STAGES = (
    "'DISCOVERED', 'ACQUISITION_PENDING', 'ACQUIRED', 'PARSED', 'NORMALIZED', "
    "'LANGUAGE_VALIDATED', 'QUALITY_VALIDATED', 'EXACT_DEDUP_VALIDATED', "
    "'NEAR_DEDUP_VALIDATED', 'FACTUALITY_VALIDATED', 'SOURCE_CLASSIFIED', "
    "'DOMAIN_CLASSIFIED', 'POLICY_VALIDATED', 'SILVER', 'CHUNKED', "
    "'CHUNK_DEDUPLICATED', 'EMBEDDED', 'STORED'"
)  # schema §16.3

_GIT_OBJECT_ID = "'^[0-9a-f]{40}([0-9a-f]{24})?$'"

SESSION_GUARDS: tuple[str, ...] = (
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '60s'",
)

# Read-only guard, effective in online and offline (--sql) runs alike.
PREFLIGHT_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') THEN
        RAISE EXCEPTION 'kbp preflight: schema kbp is not provisioned; the administrator must provision it before migrating';
    END IF;
    IF (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') <> current_user THEN
        RAISE EXCEPTION 'kbp preflight: the current role must own schema kbp (expected kb_pipeline_migrator)';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_app') THEN
        RAISE EXCEPTION 'kbp preflight: runtime role kb_pipeline_app is not provisioned';
    END IF;
END
$$
"""

CREATE_TABLES: tuple[str, ...] = (
    # §5.1
    """
CREATE TABLE kbp.pipeline_runs (
    run_id uuid PRIMARY KEY,
    run_kind text NOT NULL CHECK (run_kind IN ('ingest', 'reprocess', 'reconcile_legacy', 'reembed', 'maintenance')),
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'aborted')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    code_version text,
    config_sha256 bytea CHECK (octet_length(config_sha256) = 32),
    identity_spec_version text NOT NULL CHECK (identity_spec_version IN ('v1')),
    identity_extension_version text,
    host text,
    notes text,
    CHECK (finished_at IS NULL OR finished_at >= started_at)
)
""",
    # §5.4
    """
CREATE TABLE kbp.cc_crawls (
    crawl_id text PRIMARY KEY CHECK (crawl_id ~ '^CC-MAIN-[0-9]{4}-[0-9]{2}$'),
    index_kind text CHECK (index_kind IN ('cdx', 'columnar')),
    registered_at timestamptz NOT NULL
)
""",
    # §5.5
    """
CREATE TABLE kbp.cc_warc_files (
    warc_file_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawl_id text NOT NULL REFERENCES kbp.cc_crawls (crawl_id),
    warc_filename text NOT NULL UNIQUE,
    registered_at timestamptz NOT NULL,
    CHECK (starts_with(warc_filename, 'crawl-data/' || crawl_id || '/'))
)
""",
    # §5.8
    """
CREATE TABLE kbp.github_repositories (
    repository_id bigint PRIMARY KEY,
    node_id text,
    owner_login text NOT NULL,
    repo_name text NOT NULL,
    html_url text NOT NULL,
    default_branch text,
    visibility text NOT NULL CHECK (visibility IN ('public', 'private', 'internal', 'unknown')),
    license_spdx_id text,
    is_fork boolean,
    is_archived boolean,
    api_etag text,
    last_snapshot_commit_sha text,
    first_seen_at timestamptz NOT NULL,
    metadata_refreshed_at timestamptz
)
""",
    # §5.9
    f"""
CREATE TABLE kbp.github_commits (
    repository_id bigint NOT NULL REFERENCES kbp.github_repositories (repository_id),
    commit_sha text NOT NULL CHECK (commit_sha ~ {_GIT_OBJECT_ID}),
    tree_sha text CHECK (tree_sha ~ {_GIT_OBJECT_ID}),
    ref_name text,
    committed_at timestamptz,
    visibility_at_snapshot text NOT NULL CHECK (visibility_at_snapshot = 'public'),
    previous_snapshot_sha text,
    snapshot_mode text NOT NULL CHECK (snapshot_mode IN ('full', 'incremental')),
    resolved_at timestamptz NOT NULL,
    PRIMARY KEY (repository_id, commit_sha)
)
""",
    # §5.6
    """
CREATE TABLE kbp.source_observations (
    observation_id uuid PRIMARY KEY,
    source_channel text NOT NULL CHECK (source_channel IN ('common_crawl', 'github')),
    source_url text NOT NULL,
    url_host text,
    source_title text,
    source_published_at timestamptz,
    acquired_at timestamptz NOT NULL,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id)
)
""",
    # §5.7
    """
CREATE TABLE kbp.cc_captures (
    observation_id uuid PRIMARY KEY REFERENCES kbp.source_observations (observation_id),
    warc_file_id bigint NOT NULL REFERENCES kbp.cc_warc_files (warc_file_id),
    warc_record_offset bigint NOT NULL CHECK (warc_record_offset >= 0),
    warc_record_length integer NOT NULL CHECK (warc_record_length > 0),
    warc_record_id uuid,
    warc_date timestamptz,
    http_status smallint CHECK (http_status BETWEEN 100 AND 599),
    http_content_type text,
    detected_mime text,
    redirect_location text,
    canonical_url text,
    payload_digest text,
    UNIQUE (warc_file_id, warc_record_offset)
)
""",
    # §5.10
    f"""
CREATE TABLE kbp.github_file_observations (
    observation_id uuid PRIMARY KEY REFERENCES kbp.source_observations (observation_id),
    repository_id bigint NOT NULL,
    commit_sha text NOT NULL,
    file_path text NOT NULL,
    blob_sha text NOT NULL CHECK (blob_sha ~ {_GIT_OBJECT_ID}),
    file_size_bytes bigint CHECK (file_size_bytes >= 0),
    FOREIGN KEY (repository_id, commit_sha) REFERENCES kbp.github_commits (repository_id, commit_sha),
    UNIQUE (repository_id, commit_sha, file_path)
)
""",
    # §5.12
    """
CREATE TABLE kbp.document_registry (
    document_id uuid PRIMARY KEY,
    content_hash bytea NOT NULL UNIQUE CHECK (octet_length(content_hash) = 32),
    canonical_text text,
    char_length integer NOT NULL CHECK (char_length > 0),
    language text,
    language_confidence real CHECK (language_confidence BETWEEN 0 AND 1),
    quality_score real,
    factuality_score real,
    subject_domain text CHECK (char_length(subject_domain) <= 100),
    status text NOT NULL CHECK (status IN ('accepted', 'rejected_quality', 'rejected_near_duplicate', 'rejected_factuality', 'rejected_policy', 'quarantined')),
    status_reason text,
    policy_version text NOT NULL,
    component_versions jsonb NOT NULL,
    first_observation_id uuid NOT NULL REFERENCES kbp.source_observations (observation_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    status_updated_at timestamptz NOT NULL,
    CHECK (canonical_text IS NULL OR sha256(convert_to(canonical_text, 'UTF8')) = content_hash),
    CHECK (canonical_text IS NULL OR char_length(canonical_text) = char_length),
    CHECK (status <> 'accepted' OR (canonical_text IS NOT NULL AND language = 'en'))
)
""",
    # §5.11
    """
CREATE TABLE kbp.observation_extractions (
    observation_id uuid NOT NULL REFERENCES kbp.source_observations (observation_id),
    extraction_version text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('document', 'empty', 'rejected_language', 'rejected_prefilter', 'rejected_nul', 'extraction_failed')),
    document_id uuid REFERENCES kbp.document_registry (document_id),
    document_was_new boolean,
    extracted_at timestamptz NOT NULL,
    PRIMARY KEY (observation_id, extraction_version),
    CHECK ((outcome = 'document') = (document_id IS NOT NULL)),
    CHECK ((document_id IS NULL) = (document_was_new IS NULL))
)
""",
    # §5.23
    """
CREATE TABLE kbp.dedup_methods (
    dedup_method_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    algorithm text NOT NULL,
    algorithm_version text NOT NULL,
    parameters jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (algorithm, algorithm_version)
)
""",
    # §5.24
    """
CREATE TABLE kbp.document_dedup_decisions (
    document_id uuid NOT NULL REFERENCES kbp.document_registry (document_id),
    dedup_method_id smallint NOT NULL REFERENCES kbp.dedup_methods (dedup_method_id),
    classification text NOT NULL CHECK (classification IN ('EXACT_DUPLICATE', 'NEAR_DUPLICATE', 'VERSION', 'RELATED', 'UNIQUE')),
    canonical_document_id uuid REFERENCES kbp.document_registry (document_id),
    similarity_score real CHECK (similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    decided_at timestamptz NOT NULL,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    PRIMARY KEY (document_id, dedup_method_id),
    CHECK ((classification = 'UNIQUE') = (canonical_document_id IS NULL)),
    CHECK (canonical_document_id IS DISTINCT FROM document_id)
)
""",
    # §5.25
    """
CREATE TABLE kbp.document_relationships (
    document_id uuid NOT NULL REFERENCES kbp.document_registry (document_id),
    related_document_id uuid NOT NULL REFERENCES kbp.document_registry (document_id),
    relationship_type text NOT NULL CHECK (relationship_type IN ('EXACT_DUPLICATE', 'NEAR_DUPLICATE', 'VERSION', 'RELATED')),
    dedup_method_id smallint NOT NULL REFERENCES kbp.dedup_methods (dedup_method_id),
    similarity_score real CHECK (similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (document_id, related_document_id, relationship_type, dedup_method_id),
    CHECK (document_id <> related_document_id)
)
""",
    # §5.13
    """
CREATE TABLE kbp.chunking_versions (
    chunking_version_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE,
    tokenizer text,
    parameters jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
)
""",
    # §5.14
    """
CREATE TABLE kbp.chunk_registry (
    chunk_id uuid PRIMARY KEY,
    chunk_hash bytea NOT NULL UNIQUE CHECK (octet_length(chunk_hash) = 32),
    chunk_text text NOT NULL,
    char_length integer NOT NULL,
    token_count integer,
    chunking_version_id smallint NOT NULL REFERENCES kbp.chunking_versions (chunking_version_id),
    first_document_id uuid NOT NULL REFERENCES kbp.document_registry (document_id),
    representative_observation_id uuid NOT NULL REFERENCES kbp.source_observations (observation_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (char_length > 0 AND char_length = char_length(chunk_text)),
    CHECK (sha256(convert_to(chunk_text, 'UTF8')) = chunk_hash)
)
""",
    # §5.15
    """
CREATE TABLE kbp.chunk_occurrences (
    document_id uuid NOT NULL REFERENCES kbp.document_registry (document_id),
    chunking_version_id smallint NOT NULL REFERENCES kbp.chunking_versions (chunking_version_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    char_start integer NOT NULL,
    char_end integer NOT NULL,
    section_path text,
    PRIMARY KEY (document_id, chunking_version_id, ordinal),
    CHECK (0 <= char_start AND char_start < char_end)
)
""",
    # §5.16
    """
CREATE TABLE kbp.embedding_models (
    embedding_model_id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_name text NOT NULL,
    model_revision text NOT NULL,
    embedding_version text NOT NULL,
    embedding_dimension integer NOT NULL CHECK (embedding_dimension > 0),
    vector_type text NOT NULL CHECK (vector_type IN ('vector', 'halfvec')),
    distance_metric text NOT NULL CHECK (distance_metric IN ('cosine', 'l2', 'inner_product')),
    is_normalized boolean NOT NULL,
    max_input_tokens integer,
    config jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('candidate', 'shadow', 'active_gold', 'retired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (model_name, model_revision, embedding_version),
    CHECK (status <> 'active_gold' OR (vector_type = 'vector' AND embedding_dimension = 768))
)
""",
    # §5.17
    """
CREATE TABLE kbp.chunk_embeddings (
    chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    embedding_model_id smallint NOT NULL REFERENCES kbp.embedding_models (embedding_model_id),
    status text NOT NULL CHECK (status IN ('published_gold', 'stored_shadow')),
    attempt_count smallint NOT NULL CHECK (attempt_count > 0),
    embedded_at timestamptz NOT NULL,
    published_at timestamptz,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    PRIMARY KEY (chunk_id, embedding_model_id),
    CHECK ((status = 'published_gold') = (published_at IS NOT NULL))
)
""",
    # §5.18
    f"""
CREATE TABLE kbp.embedding_queue (
    chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    embedding_model_id smallint NOT NULL REFERENCES kbp.embedding_models (embedding_model_id),
    status text NOT NULL CHECK (status IN ('pending', 'running', 'retry_pending')),
    priority smallint NOT NULL DEFAULT 100,
    attempt_count smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    locked_at timestamptz,
    lease_expires_at timestamptz,
    worker_id text,
    lease_token uuid,
    last_error_class text CHECK (last_error_class IN ({_ERROR_CLASSES})),
    last_error_message text,
    enqueued_at timestamptz NOT NULL,
    PRIMARY KEY (chunk_id, embedding_model_id),
    CHECK (max_attempts > 0 AND attempt_count BETWEEN 0 AND max_attempts),
    CHECK ((status = 'running') = (worker_id IS NOT NULL AND lease_token IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL))
)
""",
    # §5.19
    f"""
CREATE TABLE kbp.pipeline_jobs (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_type text NOT NULL CHECK (job_type IN ('cc_index_partition', 'gh_repository_discovery', 'gh_repository_snapshot', 'embedding_enqueue', 'policy_reevaluation', 'legacy_reconciliation')),
    source text NOT NULL CHECK (source IN ('common_crawl', 'github', 'internal')),
    source_identifier text NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'running', 'retry_pending', 'succeeded', 'dead_letter', 'quarantined', 'cancelled')),
    stage text NOT NULL CHECK (stage IN ({_STAGES})),
    checkpoint jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    priority smallint NOT NULL DEFAULT 100,
    attempt_count smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL,
    deferral_count integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    locked_at timestamptz,
    lease_expires_at timestamptz,
    worker_id text,
    lease_token uuid,
    last_error_class text CHECK (last_error_class IN ({_ERROR_CLASSES})),
    last_error_code text,
    last_error_message text,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL,
    UNIQUE (job_type, source_identifier),
    CHECK (max_attempts > 0 AND attempt_count BETWEEN 0 AND max_attempts),
    CHECK ((status IN ('succeeded', 'dead_letter', 'quarantined', 'cancelled')) = (completed_at IS NOT NULL)),
    CHECK ((status = 'running') = (worker_id IS NOT NULL AND lease_token IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL))
)
""",
    # §5.20
    f"""
CREATE TABLE kbp.processing_attempts (
    attempt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES kbp.pipeline_jobs (job_id),
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    worker_id text NOT NULL,
    lease_token uuid NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('succeeded', 'failed', 'deferred_rate_limit', 'lease_expired', 'cancelled')),
    error_class text CHECK (error_class IN ({_ERROR_CLASSES})),
    error_code text,
    error_message text,
    retry_after_seconds integer,
    scheduled_next_attempt_at timestamptz,
    metrics jsonb,
    UNIQUE (job_id, attempt_no)
)
""",
    # §5.21
    f"""
CREATE TABLE kbp.dead_letters (
    dead_letter_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_type text NOT NULL CHECK (item_type IN ('job', 'observation', 'document', 'chunk', 'chunk_embedding', 'legacy_kb_row')),
    item_key text NOT NULL,
    stage text NOT NULL CHECK (stage IN ({_STAGES})),
    disposition text NOT NULL CHECK (disposition IN ('dead_letter', 'quarantined')),
    job_id bigint REFERENCES kbp.pipeline_jobs (job_id),
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    error_class text NOT NULL CHECK (error_class IN ({_ERROR_CLASSES})),
    error_code text,
    error_message text,
    attempt_count integer NOT NULL,
    occurrence_count integer NOT NULL DEFAULT 1,
    payload_ref text,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    resolved_at timestamptz,
    resolution text CHECK (resolution IN ('requeued', 'ignored', 'fixed')),
    UNIQUE (item_type, item_key, stage),
    CHECK ((resolution IS NULL) = (resolved_at IS NULL))
)
""",
    # §5.3
    """
CREATE TABLE kbp.source_rate_limits (
    source_channel text NOT NULL CHECK (source_channel IN ('common_crawl', 'github')),
    limit_scope text NOT NULL,
    credential_fingerprint text NOT NULL,
    limit_total integer,
    remaining integer CHECK (remaining >= 0),
    reset_at timestamptz,
    retry_after_until timestamptz,
    last_http_status smallint,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (source_channel, limit_scope, credential_fingerprint)
)
""",
    # §5.2 — the FK to public.datasets is added separately (NOT VALID, then VALIDATE; schema §23).
    """
CREATE TABLE kbp.source_dataset_map (
    source_channel text PRIMARY KEY CHECK (source_channel IN ('common_crawl', 'github')),
    dataset_source_name text NOT NULL,
    bridge_identity_name text NOT NULL UNIQUE,
    dataset_id uuid NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (dataset_source_name = source_channel),
    CHECK (bridge_identity_name = 'dataset-bridge:v1:' || source_channel)
)
""",
    # §5.22
    """
CREATE TABLE kbp.legacy_kb_reconciliation (
    legacy_kb_id uuid PRIMARY KEY,
    legacy_source_id uuid,
    raw_chunk_sha256 bytea NOT NULL CHECK (octet_length(raw_chunk_sha256) = 32),
    canonical_chunk_hash bytea CHECK (octet_length(canonical_chunk_hash) = 32),
    canonical_chunk_id uuid,
    canonical_differs boolean,
    reconciliation_status text NOT NULL CHECK (reconciliation_status IN ('pending', 'computed', 'empty_after_canonicalization', 'contains_nul', 'unique', 'collides_with_legacy', 'collides_with_new_chunk', 'error')),
    collision_chunk_id uuid,
    collision_legacy_kb_id uuid,
    decision text NOT NULL DEFAULT 'undecided' CHECK (decision IN ('undecided', 'keep_legacy_only', 'alias_new_to_legacy', 'publish_new_mark_legacy_superseded', 'publish_both', 'exclude')),
    decision_reason text,
    review_state text NOT NULL DEFAULT 'not_required' CHECK (review_state IN ('not_required', 'needs_review', 'reviewed')),
    reviewed_by text,
    reviewed_at timestamptz,
    error_message text,
    reconciliation_version text NOT NULL,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    first_reconciled_at timestamptz NOT NULL,
    reconciled_at timestamptz NOT NULL,
    decided_at timestamptz,
    CHECK ((review_state = 'reviewed') = (reviewed_at IS NOT NULL)),
    CHECK ((decision = 'undecided') = (decided_at IS NULL)),
    CHECK (reconciliation_status NOT IN ('collides_with_new_chunk') OR collision_chunk_id IS NOT NULL),
    CHECK (reconciliation_status NOT IN ('collides_with_legacy') OR collision_legacy_kb_id IS NOT NULL),
    CHECK (reconciliation_status NOT IN ('unique', 'collides_with_legacy', 'collides_with_new_chunk') OR (canonical_chunk_hash IS NOT NULL AND canonical_chunk_id IS NOT NULL))
)
""",
)

# The single approved cross-schema dependency (schema §6.2, §23).
BRIDGE_FK_NAME = "source_dataset_map_dataset_id_fkey"
CROSS_SCHEMA_FK: tuple[str, ...] = (
    f"ALTER TABLE kbp.source_dataset_map ADD CONSTRAINT {BRIDGE_FK_NAME} "
    "FOREIGN KEY (dataset_id) REFERENCES public.datasets (id) ON DELETE NO ACTION NOT VALID",
    f"ALTER TABLE kbp.source_dataset_map VALIDATE CONSTRAINT {BRIDGE_FK_NAME}",
)

# Schema §21.1 (non-constraint unique index) and §21.2 (initial operations).
CREATE_INDEXES: tuple[str, ...] = (
    "CREATE UNIQUE INDEX embedding_models_single_active_gold_idx ON kbp.embedding_models (status) WHERE status = 'active_gold'",
    "CREATE INDEX pipeline_jobs_claim_idx ON kbp.pipeline_jobs (job_type, priority, next_attempt_at) WHERE status IN ('pending', 'retry_pending')",
    "CREATE INDEX pipeline_jobs_lease_expiry_idx ON kbp.pipeline_jobs (lease_expires_at) WHERE status = 'running'",
    "CREATE INDEX embedding_queue_claim_idx ON kbp.embedding_queue (embedding_model_id, priority, next_attempt_at) WHERE status IN ('pending', 'retry_pending')",
    "CREATE INDEX embedding_queue_lease_expiry_idx ON kbp.embedding_queue (lease_expires_at) WHERE status = 'running'",
    "CREATE INDEX observation_extractions_document_idx ON kbp.observation_extractions (document_id) WHERE document_id IS NOT NULL",
    "CREATE INDEX dead_letters_open_idx ON kbp.dead_letters (last_seen_at) WHERE resolved_at IS NULL",
    "CREATE INDEX legacy_kb_reconciliation_canonical_hash_idx ON kbp.legacy_kb_reconciliation (canonical_chunk_hash)",
)

# docs/database_privileges_v1.md §5.1 — explicit per-table runtime grants.
# Values: (insert, update) where update is False, "table", or a column tuple.
# SELECT is granted on every table. DELETE only on embedding_queue.
RUNTIME_PRIVILEGES: dict[str, tuple[bool, object]] = {
    "pipeline_runs": (True, ("status", "finished_at", "notes")),
    "pipeline_jobs": (True, "table"),
    "processing_attempts": (True, (
        "finished_at", "outcome", "error_class", "error_code", "error_message",
        "retry_after_seconds", "scheduled_next_attempt_at", "metrics",
    )),
    "dead_letters": (True, (
        "occurrence_count", "last_seen_at", "attempt_count", "error_class", "error_code",
        "error_message", "payload_ref", "disposition", "resolved_at", "resolution",
    )),
    "source_rate_limits": (True, "table"),
    "source_dataset_map": (False, False),
    "cc_crawls": (True, False),
    "cc_warc_files": (True, False),
    "source_observations": (True, False),
    "cc_captures": (True, False),
    "github_repositories": (True, (
        "node_id", "owner_login", "repo_name", "html_url", "default_branch", "visibility",
        "license_spdx_id", "is_fork", "is_archived", "api_etag", "last_snapshot_commit_sha",
        "metadata_refreshed_at",
    )),
    "github_commits": (True, False),
    "github_file_observations": (True, False),
    "observation_extractions": (True, False),
    "document_registry": (True, (
        "status", "status_reason", "policy_version", "component_versions", "language",
        "language_confidence", "quality_score", "factuality_score", "subject_domain",
        "status_updated_at",
    )),
    "dedup_methods": (False, False),
    "document_dedup_decisions": (True, False),
    "document_relationships": (True, False),
    "chunking_versions": (False, False),
    "chunk_registry": (True, False),
    "chunk_occurrences": (True, False),
    "embedding_models": (False, False),
    "chunk_embeddings": (True, False),
    "embedding_queue": (True, "table"),
    "legacy_kb_reconciliation": (True, "table"),
}
RUNTIME_DELETE_TABLES: tuple[str, ...] = ("embedding_queue",)


def _grant_statements() -> tuple[str, ...]:
    statements: list[str] = []
    for table in KBP_TABLES:
        insert, update = RUNTIME_PRIVILEGES[table]
        statements.append(f"GRANT SELECT ON TABLE kbp.{table} TO {RUNTIME_ROLE}")
        if insert:
            statements.append(f"GRANT INSERT ON TABLE kbp.{table} TO {RUNTIME_ROLE}")
        if update == "table":
            statements.append(f"GRANT UPDATE ON TABLE kbp.{table} TO {RUNTIME_ROLE}")
        elif update:
            columns = ", ".join(update)  # type: ignore[arg-type]
            statements.append(f"GRANT UPDATE ({columns}) ON TABLE kbp.{table} TO {RUNTIME_ROLE}")
        if table in RUNTIME_DELETE_TABLES:
            statements.append(f"GRANT DELETE ON TABLE kbp.{table} TO {RUNTIME_ROLE}")
    # kbp.alembic_version is created by Alembic before this revision runs.
    statements.append(f"GRANT SELECT ON TABLE kbp.alembic_version TO {RUNTIME_ROLE}")
    return tuple(statements)


GRANTS: tuple[str, ...] = _grant_statements()

UPGRADE_STATEMENTS: tuple[str, ...] = (
    SESSION_GUARDS
    + (PREFLIGHT_SQL,)
    + CREATE_TABLES
    + CROSS_SCHEMA_FK
    + CREATE_INDEXES
    + GRANTS
)

# Reverse dependency order. Dropping a table drops its indexes, constraints,
# identity sequences and grants; RESTRICT (the default) refuses to drop
# anything else that might depend on it. public.* is never touched, and the
# administratively provisioned kbp schema itself is kept.
DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    SESSION_GUARDS
    + (f"REVOKE SELECT ON TABLE kbp.alembic_version FROM {RUNTIME_ROLE}",)
    + tuple(f"DROP TABLE kbp.{table} RESTRICT" for table in reversed(KBP_TABLES))
)


def upgrade() -> None:
    from alembic import op

    for statement in UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    from alembic import op

    for statement in DOWNGRADE_STATEMENTS:
        op.execute(statement)
