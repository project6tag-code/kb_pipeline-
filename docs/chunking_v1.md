# Chunking V1

Status: implemented and tested offline. Validated on PostgreSQL `kb_pipeline_test` (§11). No embeddings, no Gold publication.

Chunking V1 turns each `accepted` Silver document (`kbp.document_registry.canonical_text`) into deterministic chunks:

- globally unique (`kbp.chunk_registry`);
- with full lineage (`kbp.chunk_occurrences`);
- with exact code-point spans of the Silver text.

It uses Migration 001A as is. There is no schema change and no new job type.

## 1. Where it runs

| Path | When | Transaction |
|---|---|---|
| **Inline** (schema §10.3) | The Silver stage of `cc_index_partition` / `gh_repository_snapshot` units registers a NEW accepted document. `SilverProcessor(chunker=ChunkWriter(...))`, the default in `build_silver_processor`. | The same transaction as the document, its extraction and the job checkpoint (Runtime Engine V1: leases, retries, attempts, checkpoints, dead letters). |
| **Backfill** | Accepted documents with no occurrences for this chunking version: existing Silver data, or a new chunking version. | One transaction per batch. |

An exact-duplicate document is never re-chunked (§9.3).

The backfill is an operator command, not a queued job. Schema V1's job types have no document-range chunking unit (`policy_reevaluation` means something else). The backfill needs no job state: the pending set is derived from the database, so a crash or a rerun simply continues.

## 2. Frozen configuration (`kbp.chunking_versions`)

```
python -m app.processing.chunking.cli version          # name, tokenizer, parameters
```

**Parameters.** `name = kbp-chunker-c1-<sha256(parameters)[:10]>`; V1 default: `kbp-chunker-c1-78f25e8bf5`. The parameters are:

- algorithm and algorithm version (`c1`), tokenizer (`kbp-approx-bpe:1`), canonicalizer (`v1`);
- sizes: `target 256` / `min 64` / `max 400` / `hard_min 12`; `heading_reserve 32`;
- overlap: `none`, 0 tokens;
- boundary order;
- heading, list, table, code and long-block policies;
- `max_document_chars 1,000,000` and `max_chunks_per_document 2,000`.

Any change produces a different name, and therefore a new version. Old occurrences are never rewritten, and identical chunk text is reused (§11 of the schema).

**Registration.** The runtime role has SELECT only on `chunking_versions`, so registration is a migrator step:

```
python -m app.processing.chunking.cli register-version --expect-database <db>
```

It is idempotent and verifies the frozen parameters. It refuses unless the connection is exactly `--expect-database`, as `kb_pipeline_migrator`, at `kbp_001b`. Until it has run, chunking stops with `chunking_version_not_registered`, which is retried and never skipped.

## 3. Tokenizer

`kbp-approx-bpe:1` is a deterministic, dependency-free estimate of subword tokenizers:

- a letter run of length n counts ceil(n/4) tokens;
- a digit run counts ceil(n/3);
- any other non-space character counts 1.

The embedding model is not chosen yet (O-2). `tiktoken` downloads its vocabulary at runtime, and `tokenizers` is a heavy dependency. Chunk ids never depend on the tokenizer. The 400-token maximum leaves head-room below a 512-token model input. The embedding stage re-checks with the real tokenizer (`embedding_models.max_input_tokens`).

## 4. Algorithm

1. **Blocks.** The canonical Silver text is parsed into typed blocks with exact offsets: heading (`#`), paragraph, list, table (`a | b` rows), fenced code (fence-aware, because blank lines survive inside fences), and code groups. GitHub `code`/`config` documents are parsed as code: there, `#` is a comment, not a heading. The Silver kind is read from gate `03_mime` in `status_reason`.
2. **Split oversized blocks** (> `max - heading_reserve`):
   - prose: at sentence ends → clause punctuation → token boundary;
   - lists: between items;
   - tables: between rows;
   - code: between lines, starting new pieces at top-level definitions (`def`, `class`, `func`, …);
   - a giant single run is cut at the token fallback.
3. **Pack in order.**
   - A heading always travels with the content that follows it.
   - A chunk closes at a heading once it holds `min` content tokens.
   - A small section merges forward only into its own subsections. It never merges into a sibling, or across a same- or higher-level heading.
   - A chunk closes before exceeding `target` (once it holds `min`), and always before `max`.
4. **Tail merge.** A small chunk joins the previous one when it continues that section, or opens a subsection or a non-top-level sibling of it, and the result stays ≤ `max`. Top-level sections never merge.
5. **Accept or reject**, judged on the chunk's content (non-heading) lines. Rejected chunks keep their ordinal, since gaps are allowed (§5.15). Rejection reasons:
   - `empty`, `nul`;
   - `heading_only`, `no_alphanumeric`;
   - `table_fragment` (a lone row);
   - `tiny_fragment` (content < `hard_min`, unless it is the document's only chunk);
   - `boilerplate_residue` (chrome phrases).

   This is an integrity/usefulness check only. The document already passed Silver's quality gates.

**Overlap: none.** Boundaries are semantic, and context is carried in `section_path`. Overlap would put duplicated text inside chunk ids, defeating exact chunk dedup and inflating the corpus.

## 5. Identity, offsets, context

- `chunk_text = canonicalize_text(canonical_text[char_start:char_end])`. This is the schema §5.15 invariant. Spans are trimmed at whitespace boundaries, so the span is already canonical.
- `chunk_hash` / `chunk_id` come from the locked Identity V1 (`chunk_content_hash`, `chunk_id_from_hash`: `chunk:v1:` UUIDv5). The same text anywhere, from any document or source, gives the same id.
- **Offsets** are code-point offsets into the stored canonical Silver text. They are always a real contiguous span.
- **Context is never injected into `chunk_text`**: that would break the invariant and chunk dedup. It goes into `chunk_occurrences.section_path` as JSON: `{"headings": [...], "table_header": "..."}`.
  - `headings`: the heading path where the chunk's content starts.
  - `table_header`: only on continuation chunks of a split table.
  - `section_path` is NULL when there is no structure.
- The embedding stage may prepend this context as an `embedding_version` prefix (§5.16).

## 6. Persistence and dedup

| Datum | Column |
|---|---|
| canonical chunk, hash, id | `chunk_registry` (PK chunk_id + UNIQUE chunk_hash + hash CHECK) |
| token count | `chunk_registry.token_count` (the first writer's tokenizer) |
| first document / representative source | `first_document_id` / `representative_observation_id` (the document's first observation) |
| position | `chunk_occurrences` PK (document_id, chunking_version_id, ordinal) |
| span | `char_start` / `char_end` |
| heading and table context | `section_path` |
| document-level failure | `dead_letters` (item `document`, stage `SILVER`, code `chunking_*`) |

- **Inserts** are `INSERT … ON CONFLICT (chunk_id) DO NOTHING` and `… ON CONFLICT (document_id, chunking_version_id, ordinal) DO NOTHING`. There is no UPDATE and no DELETE. The same chunk in 100 documents is 1 registry row and 100 occurrences.
- **Concurrent writers of one chunk** serialize on the unique index. A rolled-back first writer never leaves the chunk missing; this is tested on PostgreSQL. `UNIQUE(chunk_hash)` still raises on a derivation bug.
- **Concurrent writers of one document** serialize on a transaction-scoped advisory lock keyed by the document.
- **Rerun.** If the document already has occurrences for the version, the recomputed chunks must equal them. The rerun is then a no-op. A difference is dead-lettered as `internal_bug` (`chunking_nondeterministic`) and never overwritten.
- **Corrupt Silver rows are refused before chunking**, dead-lettered as `data_invalid`. Refused cases:
  - not accepted;
  - no text;
  - U+0000;
  - `char_length` mismatch;
  - hash or id mismatch.
- **Bounds.** A document over `max_document_chars`, or one that would produce more than `max_chunks_per_document` chunks, is dead-lettered whole: nothing is silently truncated.

## 7. Backfill

```
python -m app.processing.chunking.cli backfill --expect-database <db> [--limit N] [--batch-size N]
python -m app.processing.chunking.cli status --expect-database <db>
```

- Selects accepted documents with no occurrences for this version and no open chunking dead letter, in keyset order.
- `--limit` bounds the run; `--batch-size` sets documents per transaction.
- Transient database errors retry the batch, bounded to 3 attempts with backoff.
- SIGINT/SIGTERM finish the current batch and then stop.
- Refuses any database other than `--expect-database`.
- Acquisition jobs never need resetting.

## 8. Near-duplicate chunks (not in V1)

Only exact chunk dedup is implemented. Document-level near-duplicates are already rejected in Silver (MinHash, J ≥ 0.8), which removes most near-duplicate chunks at the source.

Chunk-level near-dup is the extension point for the next phase. It would need its own benchmarked method, registered in `dedup_methods`. Where its decisions are stored needs design: `document_dedup_decisions` is document-keyed. Document MinHash parameters were not validated for ~250-token texts and are deliberately not reused.

## 9. Known limits

- The token counts are an estimate; the embedding tokenizer is checked at embedding time.
- Code indentation was collapsed by the locked Silver canonicalizer (one leading space marks nesting). Chunk boundaries use it, but chunk text of code is not indentation-exact.
- Section merging is structural, not semantic.
- `token_count` in `chunk_registry` belongs to the chunking version that first produced the chunk.
- Chunking dead letters are per document, not per version. A new chunking version retries a dead-lettered document only after the dead letter is resolved.

## 10. Tests

| Suite | Covers |
|---|---|
| `tests/unit/test_chunking.py` | structure, sizes, merging, splitting, lists, tables, code, bounds, offsets, heading paths |
| `tests/unit/test_chunking_writer.py` | identity, within-document and cross-document/source dedup, rerun, nondeterminism, crash rollback, concurrency, new version, corrupt rows, backfill, inline Silver hook |
| `tests/unit/test_chunking_sql.py` | SQL vs grants and schema |
| `tests/integration/test_chunking_offline_e2e.py` | CC + GitHub through the handlers, sharing one chunk |
| `tests/integration/test_chunking_postgres.py` | opt-in PostgreSQL validation |

## 11. PostgreSQL validation (kb_pipeline_test)

```
set KB_MIGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test
python -m app.processing.chunking.cli register-version --expect-database kb_pipeline_test
set KB_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
python -m app.processing.chunking.cli backfill --expect-database kb_pipeline_test --limit 2 --batch-size 1
set KB_INTEGRATION_DATABASE_URL=%KB_DATABASE_URL%
python -m tests.integration.chunk_pilot run          # inline CC + GitHub pilot, then backfill of the rest
set KB_RUN_PG_INTEGRATION=1
python -m unittest tests.integration.test_chunking_postgres -v
```

**Result.**

- `chunking_version_id` 1, registered once.
- 5 accepted documents, all chunked.
- 9 registry rows, 10 occurrences.
- 1 chunk shared by a Common Crawl page and a GitHub README.
- Token counts: min 80, average 139, max 250.
- 0 duplicate hashes, 0 orphan occurrences, 0 chunking dead letters.
- Offsets, hashes, ids, token counts and recomputed order all verified.

A rerun (pilot, backfill and registration) adds 0 rows and keeps identical ordered chunk ids.
