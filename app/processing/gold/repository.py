"""Psycopg-native SQL for Gold Publisher V1.

No SQLAlchemy imports are used in this package. Every write is scoped to
kb_pipeline_test by the service's identity guard and run in an explicit
transaction.
"""

IDENTITY_SQL = """
SELECT current_database() AS database, current_user AS role,
       (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) AS superuser,
       (SELECT version_num FROM kbp.alembic_version) AS revision
"""

MODEL_SQL = """
SELECT embedding_model_id, model_name, model_revision, embedding_version, embedding_dimension,
       vector_type, distance_metric, is_normalized, max_input_tokens, config, status
FROM kbp.embedding_models
WHERE embedding_model_id = %s
"""

MODELS_ACTIVE_SQL = """
SELECT embedding_model_id FROM kbp.embedding_models WHERE status = 'active_gold' ORDER BY embedding_model_id
"""

PROMOTE_MODEL_SQL = """
UPDATE kbp.embedding_models
SET status = 'active_gold'
WHERE embedding_model_id = %s AND status = 'candidate'
RETURNING embedding_model_id
"""

METHOD_SQL = """
SELECT dedup_method_id FROM kbp.dedup_methods
WHERE algorithm = %s AND algorithm_version = %s
"""

DECISION_PAGE_SQL = """
SELECT d.chunk_id, d.classification, d.representative_chunk_id, d.decision_status
FROM kbp.chunk_dedup_decisions d
WHERE d.dedup_method_id = %s AND d.chunk_id > %s
ORDER BY d.chunk_id
LIMIT %s
"""

CHUNK_COUNT_SQL = "SELECT count(*) AS count FROM kbp.chunk_registry"
DECISION_COUNT_SQL = "SELECT count(*) AS count FROM kbp.chunk_dedup_decisions WHERE dedup_method_id = %s"

SUPPRESSED_ORPHAN_SQL = """
SELECT count(*) AS count
FROM kbp.chunk_dedup_decisions suppressed
WHERE suppressed.dedup_method_id = %s
  AND suppressed.classification = 'NEAR_DUPLICATE'
  AND suppressed.decision_status = 'confirmed'
  AND NOT EXISTS (
      SELECT 1 FROM kbp.chunk_dedup_decisions representative
      WHERE representative.dedup_method_id = suppressed.dedup_method_id
        AND representative.chunk_id = suppressed.representative_chunk_id
        AND representative.decision_status = 'confirmed'
        AND representative.classification <> 'NEAR_DUPLICATE'
  )
"""

SUPPRESSED_EMBEDDED_SQL = """
SELECT count(*) AS count
FROM kbp.chunk_dedup_decisions d
JOIN kbp.chunk_embeddings e ON e.chunk_id = d.chunk_id AND e.embedding_model_id = %s
WHERE d.dedup_method_id = %s AND d.classification = 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
"""

ELIGIBLE_EMBEDDING_AUDIT_SQL = """
SELECT count(*) AS count
FROM kbp.chunk_dedup_decisions d
LEFT JOIN kbp.chunk_embeddings e ON e.chunk_id = d.chunk_id AND e.embedding_model_id = %s
LEFT JOIN kbp.chunk_embedding_vectors v ON v.chunk_id = d.chunk_id AND v.embedding_model_id = %s
WHERE d.dedup_method_id = %s AND d.classification <> 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
  AND (e.chunk_id IS NULL OR e.status <> 'stored_shadow' OR e.published_at IS NOT NULL OR v.chunk_id IS NULL
       OR public.vector_dims(v.embedding) <> 768
       OR v.vector_sha256 <> sha256(substring(public.vector_send(v.embedding) FROM 5)))
"""

BRIDGE_ROWS_SQL = """
SELECT source_channel, dataset_id FROM kbp.source_dataset_map ORDER BY source_channel
"""

PROVENANCE_SQL = """
SELECT c.chunk_id, c.chunk_text, c.chunk_hash, c.first_document_id,
       c.representative_observation_id, d.subject_domain,
       o.source_channel, o.source_url, o.source_title, o.source_published_at
FROM kbp.chunk_registry c
JOIN kbp.document_registry d ON d.document_id = c.first_document_id
JOIN kbp.source_observations o ON o.observation_id = c.representative_observation_id
WHERE c.chunk_id = %s
"""

SHADOW_VECTOR_SQL = """
SELECT e.status AS embedding_status, e.published_at AS embedding_published_at,
       v.embedding::text AS embedding_text, v.vector_sha256,
       public.vector_dims(v.embedding) AS dimensions, public.vector_send(v.embedding) AS vector_wire,
       sha256(substring(public.vector_send(v.embedding) FROM 5)) AS recomputed_sha256,
       public.vector_norm(v.embedding) AS norm
FROM kbp.chunk_embeddings e
JOIN kbp.chunk_embedding_vectors v USING (chunk_id, embedding_model_id)
WHERE e.chunk_id = %s AND e.embedding_model_id = %s
"""

ELIGIBILITY_SQL = """
SELECT classification, representative_chunk_id, decision_status
FROM kbp.chunk_dedup_decisions
WHERE chunk_id = %s AND dedup_method_id = %s
"""

LOCK_CHUNK_SQL = "SELECT pg_catalog.pg_advisory_xact_lock(%s)"

METADATA_GET_SQL = """
SELECT id, source_id, source_url, source_title, source_type, domain, published_at, updated_at
FROM public.metadata WHERE id = %s
"""

KB_GET_SQL = """
SELECT id, chunk, public.vector_send(embedding) AS embedding_wire
FROM public.kb WHERE id = %s
"""

LEDGER_GET_SQL = """
SELECT chunk_id, embedding_model_id, published_at, run_id
FROM kbp.gold_publications WHERE chunk_id = %s AND embedding_model_id = %s
"""

METADATA_INSERT_SQL = """
INSERT INTO public.metadata (id, source_id, source_url, source_title, source_type, domain, published_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING
"""

KB_INSERT_SQL = """
INSERT INTO public.kb (id, chunk, embedding)
VALUES (%s, %s, CAST(%s AS public.vector))
ON CONFLICT (id) DO NOTHING
"""

LEDGER_INSERT_SQL = """
INSERT INTO kbp.gold_publications (chunk_id, embedding_model_id, published_at, run_id)
VALUES (%s, %s, now(), %s)
ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
"""

CREATE_RUN_SQL = """
INSERT INTO kbp.pipeline_runs (run_id, run_kind, status, started_at, code_version,
                               identity_spec_version, identity_extension_version, host, notes)
VALUES (%s, 'maintenance', 'running', now(), 'gold-publisher-v1', 'v1', 'v1', %s, %s)
"""

FINISH_RUN_SQL = """
UPDATE kbp.pipeline_runs SET status = %s, finished_at = now(), notes = %s
WHERE run_id = %s AND status = 'running'
"""

STATE_COUNTS_SQL = """
SELECT (SELECT count(*) FROM kbp.chunk_registry) AS chunks,
       (SELECT count(*) FROM kbp.chunk_occurrences) AS occurrences,
       (SELECT count(*) FROM kbp.document_registry) AS documents,
       (SELECT count(*) FROM kbp.observation_extractions) AS extractions,
       (SELECT count(*) FROM kbp.document_dedup_decisions) AS document_decisions,
       (SELECT count(*) FROM kbp.document_relationships) AS document_relationships,
       (SELECT count(*) FROM kbp.chunk_dedup_decisions) AS chunk_decisions,
       (SELECT count(*) FROM kbp.chunk_relationships) AS chunk_relationships,
       (SELECT count(*) FROM kbp.chunk_embeddings) AS embedding_results,
       (SELECT count(*) FROM kbp.chunk_embedding_vectors) AS vectors,
       (SELECT count(*) FROM kbp.embedding_queue) AS queue,
       (SELECT count(*) FROM kbp.source_dataset_map) AS bridge_rows,
       (SELECT count(*) FROM kbp.gold_publications) AS publications,
       (SELECT count(*) FROM public.kb) AS kb_rows,
       (SELECT count(id) FROM public.metadata) AS metadata_rows
"""

SOURCE_COUNTS_SQL = """
SELECT m.source_type, count(*) AS rows
FROM kbp.gold_publications p
JOIN public.metadata m ON m.id = p.chunk_id
WHERE p.embedding_model_id = %s
GROUP BY m.source_type ORDER BY m.source_type
"""

SHARED_CHUNKS_SQL = """
SELECT o.chunk_id, count(DISTINCT s.source_channel) AS channel_count,
       string_agg(DISTINCT s.source_channel, ',' ORDER BY s.source_channel) AS channels,
       c.representative_observation_id, srep.source_channel AS representative_channel
FROM kbp.chunk_occurrences o
JOIN kbp.document_registry d ON d.document_id = o.document_id
JOIN kbp.observation_extractions e ON e.document_id = d.document_id AND e.outcome = 'document'
JOIN kbp.source_observations s ON s.observation_id = e.observation_id
JOIN kbp.chunk_registry c ON c.chunk_id = o.chunk_id
JOIN kbp.source_observations srep ON srep.observation_id = c.representative_observation_id
GROUP BY o.chunk_id, c.representative_observation_id, srep.source_channel
HAVING count(DISTINCT s.source_channel) > 1
ORDER BY o.chunk_id
"""

SUPPRESSED_SQL = """
SELECT d.chunk_id AS suppressed_chunk_id, d.representative_chunk_id,
       EXISTS (SELECT 1 FROM public.kb WHERE id = d.representative_chunk_id) AS representative_in_kb,
       EXISTS (SELECT 1 FROM public.metadata WHERE id = d.representative_chunk_id) AS representative_in_metadata,
       EXISTS (SELECT 1 FROM kbp.gold_publications p
               WHERE p.chunk_id = d.representative_chunk_id AND p.embedding_model_id = %s) AS representative_in_ledger,
       EXISTS (SELECT 1 FROM public.kb WHERE id = d.chunk_id) AS suppressed_in_kb,
       EXISTS (SELECT 1 FROM public.metadata WHERE id = d.chunk_id) AS suppressed_in_metadata,
       EXISTS (SELECT 1 FROM kbp.gold_publications p
               WHERE p.chunk_id = d.chunk_id AND p.embedding_model_id = %s) AS suppressed_in_ledger
FROM kbp.chunk_dedup_decisions d
WHERE d.dedup_method_id = %s AND d.classification = 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
ORDER BY d.chunk_id
"""

GOLD_AUDIT_SQL = """
SELECT count(*) AS ledger_rows,
  count(*) FILTER (WHERE d.chunk_id IS NULL OR d.decision_status <> 'confirmed'
          OR d.classification = 'NEAR_DUPLICATE') AS ineligible_ledger_rows,
  count(*) FILTER (WHERE c.chunk_id IS NULL OR e.status <> 'stored_shadow' OR e.published_at IS NOT NULL
          OR v.chunk_id IS NULL OR v.vector_sha256 <> sha256(substring(public.vector_send(v.embedding) FROM 5))
          OR public.vector_dims(v.embedding) <> 768) AS invalid_shadow_rows,
  count(*) FILTER (WHERE m.id IS NULL OR k.id IS NULL) AS missing_gold_rows,
  count(*) FILTER (WHERE m.id IS NOT NULL AND
          (m.source_id IS DISTINCT FROM map.dataset_id
           OR m.source_url IS DISTINCT FROM o.source_url
           OR m.source_title IS DISTINCT FROM o.source_title
           OR m.source_type IS DISTINCT FROM o.source_channel
           OR m.domain IS DISTINCT FROM doc.subject_domain
           OR m.published_at IS DISTINCT FROM o.source_published_at
           OR m.updated_at IS NOT NULL)) AS metadata_mismatches,
  count(*) FILTER (WHERE k.id IS NOT NULL AND
          (k.chunk IS DISTINCT FROM c.chunk_text
           OR public.vector_send(k.embedding) IS DISTINCT FROM public.vector_send(v.embedding))) AS kb_mismatches,
  count(*) FILTER (WHERE p.chunk_id IS DISTINCT FROM k.id OR p.chunk_id IS DISTINCT FROM m.id) AS id_mismatches
FROM kbp.gold_publications p
LEFT JOIN kbp.chunk_dedup_decisions d ON d.chunk_id = p.chunk_id AND d.dedup_method_id = %s
LEFT JOIN kbp.chunk_registry c ON c.chunk_id = p.chunk_id
LEFT JOIN kbp.document_registry doc ON doc.document_id = c.first_document_id
LEFT JOIN kbp.source_observations o ON o.observation_id = c.representative_observation_id
LEFT JOIN kbp.source_dataset_map map ON map.source_channel = o.source_channel
LEFT JOIN kbp.chunk_embeddings e ON e.chunk_id = p.chunk_id AND e.embedding_model_id = p.embedding_model_id
LEFT JOIN kbp.chunk_embedding_vectors v ON v.chunk_id = p.chunk_id AND v.embedding_model_id = p.embedding_model_id
LEFT JOIN public.metadata m ON m.id = p.chunk_id
LEFT JOIN public.kb k ON k.id = p.chunk_id
WHERE p.embedding_model_id = %s
"""