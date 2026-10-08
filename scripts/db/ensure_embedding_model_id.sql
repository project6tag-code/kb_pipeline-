-- =============================================================================
-- ensure_embedding_model_id.sql - make the frozen MPNet registration land on embedding_model_id = 2 (clean DB setup)
-- =============================================================================
-- WHY. The runtime pins the production embedding model as id 2 (app.pipeline.cli.MODEL_ID and
-- app.processing.gold.service.MODEL_ID) and the validated database holds exactly one row, id 2
-- (docs/embeddings_v1.md: "registration: 1 row (id 2, candidate)"). kbp.embedding_models.embedding_model_id is
-- GENERATED ALWAYS AS IDENTITY, and identity values are consumed even by a rolled-back or conflicting INSERT; on the
-- validated database one value (1) was consumed before the successful registration, as the gaps in its other
-- registries show (chunking_versions 1,4,5; dedup_methods 1,3,5). A clean database therefore registers the same
-- model as id 1, and the Gold publisher stops with "embedding_model_id 2 is missing".
--
-- WHAT. Reproduces the validated state without inventing a model. The table is empty or holds id 2, or the step stops:
--   * id 2 already present                         -> nothing to do (rerun);
--   * exactly one UNUSED candidate row with id 1   -> an earlier clean initialization registered it as id 1: delete that
--     (no embeddings, vectors, queue rows)            row, so the table is empty;
--   * any other row                                -> refuse; it is in use and needs a human decision.
-- Then, with the table empty, the identity's NEXT value decides:
--   * next = 2                                     -> nothing to do;
--   * next = 1 (a clean database)                  -> consume ONE value with nextval (what the history did);
--   * next > 2 (earlier reruns drew values, because ON CONFLICT DO NOTHING still consumes one) -> RESTART the identity
--     at 2: safe because the table is empty and nothing can reference it.
-- It never changes the identity definition or MODEL_ID, and never touches a row that is in use.
--
-- Run by scripts/production/init_database.ps1 AFTER Alembic and BEFORE `embedding.cli register-model`, as the
-- administrator (or the kbp owner), against the expected database:
--   psql -X -v ON_ERROR_STOP=1 -v expected_database=<database> -h localhost -U postgres -d <database> \
--        -f scripts/db/ensure_embedding_model_id.sql
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

\if :{?expected_database}
\else
    \echo 'refusing: pass -v expected_database=<database> (the database this step is meant for)'
    \quit 3
\endif

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;
SELECT set_config('kbp_ensure.expected_database', :'expected_database', true) AS expected_database;

DO $ensure$
DECLARE
    expected  text := current_setting('kbp_ensure.expected_database');
    frozen_id constant smallint := 2;   -- == MODEL_ID in app/pipeline/cli.py and app/processing/gold/service.py
    seq       regclass;
    seq_last  bigint;
    seq_called boolean;
    next_value bigint;
    n_rows    bigint;
    only_id   smallint;
BEGIN
    IF expected !~ '^[a-z][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'expected_database % is not a valid database name', expected;
    END IF;
    IF expected IN ('data_collection_pipeline', 'postgres', 'template0', 'template1')
       OR current_database() IN ('data_collection_pipeline', 'postgres', 'template0', 'template1') THEN
        RAISE EXCEPTION 'refusing on protected database % (expected %)', current_database(), expected;
    END IF;
    IF current_database() <> expected THEN
        RAISE EXCEPTION 'refusing on database % (expected_database is %)', current_database(), expected;
    END IF;
    IF current_user = 'kb_pipeline_app' THEN
        RAISE EXCEPTION 'run this step as the administrator or the kbp owner, not as the runtime role';
    END IF;
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num = 'kbp_001e') <> 1 THEN
        RAISE EXCEPTION 'kbp must be at kbp_001e (run alembic upgrade head first)';
    END IF;

    seq := pg_get_serial_sequence('kbp.embedding_models', 'embedding_model_id')::regclass;
    IF seq IS NULL THEN
        RAISE EXCEPTION 'kbp.embedding_models.embedding_model_id has no identity sequence';
    END IF;

    IF EXISTS (SELECT 1 FROM kbp.embedding_models WHERE embedding_model_id = frozen_id) THEN
        RAISE NOTICE 'embedding_model_id % already present: nothing to do', frozen_id;
        RETURN;
    END IF;

    SELECT count(*), min(embedding_model_id) INTO n_rows, only_id FROM kbp.embedding_models;
    IF n_rows > 0 THEN
        IF n_rows = 1 AND only_id = frozen_id - 1
           AND (SELECT status FROM kbp.embedding_models) = 'candidate'
           AND NOT EXISTS (SELECT 1 FROM kbp.chunk_embeddings        WHERE embedding_model_id = only_id)
           AND NOT EXISTS (SELECT 1 FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = only_id)
           AND NOT EXISTS (SELECT 1 FROM kbp.embedding_queue         WHERE embedding_model_id = only_id) THEN
            DELETE FROM kbp.embedding_models WHERE embedding_model_id = only_id;
            RAISE NOTICE 'removed the unused candidate registered as id %; the next registration gets id %', only_id, frozen_id;
        ELSE
            RAISE EXCEPTION 'kbp.embedding_models has % row(s) but not id %; refusing to touch registrations that are in use',
                n_rows, frozen_id;
        END IF;
    END IF;

    -- From here on the table is empty (id 2 absent; any other row was in use and already refused above).
    IF (SELECT count(*) FROM kbp.embedding_models) <> 0 THEN
        RAISE EXCEPTION 'kbp.embedding_models is not empty; refusing';
    END IF;
    IF (SELECT seqincrement FROM pg_catalog.pg_sequence WHERE seqrelid = seq) <> 1 THEN
        RAISE EXCEPTION 'the identity of kbp.embedding_models has an unexpected increment';
    END IF;
    EXECUTE format('SELECT last_value, is_called FROM %s', seq) INTO seq_last, seq_called;
    next_value := CASE WHEN seq_called THEN seq_last + 1 ELSE seq_last END;
    IF next_value = frozen_id THEN
        RAISE NOTICE 'identity already at %: the next registration gets id %', next_value, frozen_id;
    ELSIF next_value = frozen_id - 1 THEN
        PERFORM nextval(seq);   -- consume exactly one value, as the validated history did
        RAISE NOTICE 'consumed identity value %; the next registration gets id %', frozen_id - 1, frozen_id;
    ELSE
        -- Reruns of earlier initializations consumed more values (ON CONFLICT DO NOTHING still draws one). The table
        -- is empty and nothing references it, so pointing the identity at the contract's id is safe.
        EXECUTE format('ALTER TABLE kbp.embedding_models ALTER COLUMN embedding_model_id RESTART WITH %s', frozen_id);
        RAISE NOTICE 'identity was at %; restarted at % on the empty table', next_value, frozen_id;
    END IF;
END
$ensure$;

COMMIT;

\echo 'embedding model id: OK (the frozen model will register as id 2)'
