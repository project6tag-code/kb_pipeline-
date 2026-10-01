"""Shadow embedding-vector storage — Migration 001C.

Revision ID: kbp_001c
Revises: kbp_001b

Adds only ``kbp.chunk_embedding_vectors``: the per-(chunk, model) shadow vector
that schema §12.4 step 3 / O-18 left as future work. It does not change
``kbp.chunk_embeddings``, which stays the result-state registry (§5.17), nor
``public.kb``, the job types or any existing data.

**pgvector type resolution.** The column type is the schema-qualified
``public.vector(768)``. The preflight below first verifies, from the catalogs,
that the ``vector`` extension is installed in schema ``public`` and owns the
type ``public.vector``; otherwise it refuses before any DDL. A qualified name
is used rather than an unqualified ``vector(768)`` resolved through
``search_path``: an unqualified name would resolve to whichever ``vector`` type
appears first on the migrator's search path, which is environment-dependent.
The migration safety checker allows exactly this one public type, only as a
column type with a dimension inside ``CREATE TABLE kbp.*``
(``app.db.migration_safety.ALLOWED_PUBLIC_TYPES``).

**Integrity.**

* The composite FK targets the PK of ``chunk_embeddings``, so no vector can
  exist without its result-state row (written first, in the same transaction).
* The PK allows one vector per (chunk, model).
* ``vector(768)`` fixes the dimension; pgvector also rejects NaN and Inf.
* ``vector_sha256`` is exactly 32 bytes.
* The runtime role gets SELECT + INSERT only, so a stored vector can never be
  overwritten. The application inserts with ``ON CONFLICT DO NOTHING`` and
  compares ``vector_sha256``: a different hash for the same (chunk, model) is
  nondeterminism to report, never an overwrite.

No ANN or vector-similarity index is created (§12.4 step 4 is separate work).
"""

from __future__ import annotations

revision = "kbp_001c"
down_revision = "kbp_001b"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "kb_pipeline_app"
EMBEDDING_DIMENSION = 768

KBP_TABLES: tuple[str, ...] = ("chunk_embedding_vectors",)

SESSION_GUARDS: tuple[str, ...] = (
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '60s'",
)

PREFLIGHT_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') THEN
        RAISE EXCEPTION 'kbp preflight: schema kbp is not provisioned';
    END IF;
    IF (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') <> current_user THEN
        RAISE EXCEPTION 'kbp preflight: current role must own schema kbp';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_app') THEN
        RAISE EXCEPTION 'kbp preflight: runtime role kb_pipeline_app is not provisioned';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_extension
        WHERE extname = 'vector'
          AND extnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public')
    ) THEN
        RAISE EXCEPTION 'kbp preflight: extension vector is not installed in schema public';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_depend
        WHERE classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND deptype = 'e'
          AND objid = (SELECT oid FROM pg_catalog.pg_type
                       WHERE typname = 'vector'
                         AND typnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'))
          AND refobjid = (SELECT oid FROM pg_catalog.pg_extension WHERE extname = 'vector')
    ) THEN
        RAISE EXCEPTION 'kbp preflight: type public.vector is not owned by extension vector';
    END IF;
END
$$
"""

CREATE_TABLES: tuple[str, ...] = (
    f"""
CREATE TABLE kbp.chunk_embedding_vectors (
    chunk_id uuid NOT NULL,
    embedding_model_id smallint NOT NULL,
    embedding public.vector({EMBEDDING_DIMENSION}) NOT NULL,
    vector_sha256 bytea NOT NULL,
    CONSTRAINT chunk_embedding_vectors_pkey PRIMARY KEY (chunk_id, embedding_model_id),
    CONSTRAINT chunk_embedding_vectors_result_fkey
        FOREIGN KEY (chunk_id, embedding_model_id)
        REFERENCES kbp.chunk_embeddings (chunk_id, embedding_model_id),
    CONSTRAINT chunk_embedding_vectors_sha256_check
        CHECK (octet_length(vector_sha256) = 32)
)
""",
)

RUNTIME_PRIVILEGES: dict[str, tuple[bool, bool, bool, bool]] = {
    "chunk_embedding_vectors": (True, True, False, False),  # SELECT, INSERT, UPDATE, DELETE
}

GRANTS: tuple[str, ...] = tuple(
    statement
    for table, (select, insert, update, delete) in RUNTIME_PRIVILEGES.items()
    for statement in (
        *((f"GRANT SELECT ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if select else ()),
        *((f"GRANT INSERT ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if insert else ()),
        *((f"GRANT UPDATE ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if update else ()),
        *((f"GRANT DELETE ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if delete else ()),
    )
)

UPGRADE_STATEMENTS: tuple[str, ...] = SESSION_GUARDS + (PREFLIGHT_SQL,) + CREATE_TABLES + GRANTS

DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    SESSION_GUARDS
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
