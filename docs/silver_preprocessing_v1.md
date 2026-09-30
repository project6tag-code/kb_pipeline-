# Bronze → Silver Preprocessing V1

Status: implemented, tested offline, and validated on PostgreSQL `kb_pipeline_test` with a tiny deterministic pilot (§9). No chunking, embeddings or Gold publication.

Silver is the set of `kbp.document_registry` rows with `status = 'accepted'`, together with their provenance and decisions. Quality comes before volume: a questionable document is rejected or held, never silently promoted.

## 1. Where Silver runs (Runtime Engine V1)

Schema V1 fixes the job types, and there is no per-document job (§16.1). Silver therefore runs inside the existing acquisition units, as §9.3 and §16.3 prescribe:

| unit | Silver input per batch |
|---|---|
| `cc_index_partition` | new captures, plus captures already present with complete Bronze |
| `gh_repository_snapshot` | new file observations, plus on a reprocess those already observed at this commit |

The acquisition handler calls the processor twice for each batch:

- **`prepare`**, outside any transaction. It skips observations already extracted under this `extraction_version`. It then runs gates 01–18 and 21 in memory over verified Bronze.
- **`persist`**, in the same transaction as the observation rows and the job checkpoint. Observation, extraction, document, dedup decisions and checkpoint therefore commit or roll back together.

Without a processor attached, acquisition is unchanged and the unit completes at `ACQUIRED`. With a processor attached, the unit completes at `SILVER`.

Retry, attempts, deferral, dead letters, graceful shutdown (the job yields at a batch boundary) and lease recovery all come from Runtime Engine V1 unchanged.

Commands:

```
python -m app.processing.silver.cli versions                   # labels + component versions (no I/O)
python -m app.processing.silver.cli register-methods --expect-database <db>   # MIGRATOR ONLY, once (see §6)
python -m app.processing.silver.cli work --source cc|github    # worker with the Silver stage
python -m app.processing.silver.cli status                     # counts (read-only)
```

## 2. The 22 gates

A failing gate ends the evaluation with an explicit outcome and a reason code.

| # | gate | module | failure → durable record |
|---|---|---|---|
| 01 | Bronze integrity | `silver/bronze_reader.py` | identity mismatch, missing, incomplete, damaged or tampered evidence → dead letter only (no extraction row, so a repair can retry it) |
| 02 | payload extraction | `silver/bronze_reader.py` | non-200, content/transfer coding → `rejected_prefilter`; over a size bound → `extraction_failed` |
| 03 | MIME / binary | `extraction/decoding.py` | magic numbers, NUL/control bytes, MIME not text, JSON in HTML clothing, license/generated/vendored/unknown GitHub files → `rejected_prefilter` |
| 04 | safe decoding | `extraction/decoding.py` | undecodable, unsafe charset, control characters → `extraction_failed` |
| 05 | structural parse | `extraction/html.py` | node or depth bound, unparseable → `extraction_failed` |
| 06 | active/hidden removal | `extraction/html.py` | counts only |
| 07 | boilerplate | `extraction/html.py` | counts only |
| 08 | navigation/chrome | `extraction/html.py` | counts only |
| 09 | ads/promo | `extraction/html.py` | counts only |
| 10 | structure reconstruction | `extraction/html.py`, `markdown.py`, `code.py` | generated/minified/data dump → `rejected_prefilter` |
| 11 | LOCKED canonicalization | `normalization/text.py` (`canonicalize_text`) | empty → `empty` |
| 12 | NUL/corruption | `silver/pipeline.py` | U+0000 → `rejected_nul` (never reaches PostgreSQL); surrogates or U+FFFD → `extraction_failed` |
| 13 | English | `language/detector.py` | NON_ENGLISH / MIXED / UNRESOLVED → `rejected_language` |
| 14 | privacy/PII | `privacy/pii.py` | ALLOW / REDACT → pass; REJECT → `rejected_policy`; QUARANTINE → `quarantined` |
| 15 | secrets | `privacy/secrets.py` | REJECT → `rejected_policy`; QUARANTINE → `quarantined` + dead letter |
| 16 | spam/error/noise | `quality/spam.py` | `rejected_quality` |
| 17 | quality score | `quality/scorer.py` | `rejected_quality` |
| 18 | factual-KB suitability | `factuality/classifier.py` | FAIL → `rejected_factuality`; UNRESOLVED → `quarantined` |
| 19 | global exact dedup | `silver/processor.py` | existing id → `document_was_new = FALSE` |
| 20 | near-dup / version | `dedup/near.py` | NEAR_DUPLICATE → `rejected_near_duplicate` |
| 21 | domain/subdomain | `classification/domain.py` | never fails; UNKNOWN allowed |
| 22 | final Silver gate | `silver/processor.py` (`final_gate`) | one status, strictest first |

Outcomes 01–13 go to `observation_extractions.outcome`; the document is not registered (D13). Gates 14–22 decide `document_registry.status`.

**Wall-clock deadline.** The parser deadline is a safety net, not an outcome. When it fires, the result is a transient dead letter with no extraction row, so extraction outcomes stay deterministic.

**Unexpected exceptions** become `internal_bug` item dead letters. The rest of the unit continues.

## 3. Final-gate precedence

The final gate checks, in order:

1. secret, high confidence → `quarantined` (text NULL, dead letter `policy_quarantine`)
2. secret, medium confidence → `rejected_policy`
3. privacy REJECT → `rejected_policy`
4. privacy QUARANTINE → `quarantined`
5. noise → `rejected_quality`
6. quality → `rejected_quality`
7. suitability FAIL → `rejected_factuality`
8. suitability UNRESOLVED → `quarantined` (non-sensitive text kept, O-14 default)
9. any mandatory gate not PASS → `quarantined`
10. near-duplicate → `rejected_near_duplicate`
11. identity, NUL and English invariants re-verified on the exact stored text
12. otherwise → `accepted`

Near-duplicate evaluation runs only for documents that passed everything else. For the rest it is recorded as `not_required`.

## 4. Persistence (Migration 001A only; no schema change)

| datum | column |
|---|---|
| document_id, content_hash, canonical_text | `document_registry` (hash-binding CHECKs) |
| status + every gate result | `status`, `status_reason` = `<primary>\|<gate>=<OUTCOME>[:score][:reasons];…` (categories and counts only, never source values) |
| language / confidence | `language = 'en'`, `language_confidence` |
| quality / suitability | `quality_score`, `factuality_score` |
| domain | `subject_domain` = `"Domain/Subdomain"` or `"Domain"`; NULL when UNKNOWN |
| versions | `component_versions` (all 13 components + labels), `policy_version` |
| lineage, exact duplicates | `observation_extractions` (`document_was_new`) |
| near-dup / version / related | `document_dedup_decisions` + `document_relationships` (`decision_status = 'confirmed'`, exact-Jaccard verified) |
| corrupt evidence, quarantines, bugs, nondeterminism | `dead_letters` |

Details the schema has no column for are reconstructible deterministically from Bronze plus the recorded versions:

- PII offsets
- per-component quality scores
- the domain runner-up

Rejected documents keep hash and decision; `canonical_text` is NULL (`retain_rejected_text = False`).

**Labels.** `extraction_version = x1-<hash>` covers everything that decides an extraction outcome or the document text:

- extractor
- HTML cleaner
- decoder
- canonicalizer
- language gate
- privacy redaction
- their thresholds and limits

`policy_version = s1-<hash>` covers the whole gate bundle. A changed version adds rows; it never overwrites.

## 5. Identity, idempotency and crash recovery

**Silver text and identity.** The Silver text is `canonicalize_text` of the extracted text. PII redaction is deterministic (`[REDACTED_<CATEGORY>]`) and is followed by re-canonicalization. `content_hash` and `document_id` come from the LOCKED Identity V1 functions on that text.

**Same Bronze + same versions → same result.** The same extraction, canonical text, hash and id; zero new rows. An existing `(observation, extraction_version)` row with a different outcome or document is never overwritten; it becomes an `internal_bug` dead letter (`silver_nondeterministic_extraction`).

**Crash recovery.** Everything for a batch commits in one transaction. After a crash the unit resumes from its checkpoint, and every insert is `ON CONFLICT DO NOTHING` on the schema's keys.

**Concurrency.** A transaction-scoped advisory lock serializes the registry and near-dup section across workers.

## 6. Near-duplicate method (bootstrap)

Method: `minhash_lsh` / `oph-mh128-b32r4-w5-v1`.

- word 5-shingles hashed with BLAKE2b-64
- one-permutation MinHash, 128 bins, rotation densification
- LSH 32 bands × 4 rows
- bucket reads bounded (`max_bucket_rows`)
- at most `max_near_candidates` candidates, verified by exact Jaccard

Thresholds: near-duplicate 0.80; VERSION (same URL or same repository path) 0.50; RELATED 0.50. Topical similarity alone never merges.

**Physical index.** The index is a SQLite side file (`KB_SILVER_NEAR_INDEX`, default `data/silver/near_index.sqlite3`) holding the band keys of accepted documents. It is derived data, as §9.5 allows. It is written before the PostgreSQL commit, and every candidate is re-checked against the registry, so stale entries left by a rolled-back batch are harmless.

**Registration.** `kb_pipeline_app` cannot insert into `kbp.dedup_methods`. Register the method once, as the migrator:

```
python -m app.processing.silver.cli register-methods --expect-database <db>
```

It is idempotent and verifies the frozen parameters. Before writing, it refuses unless the connection is on exactly `--expect-database`, as `kb_pipeline_migrator`, at revision `kbp_001a`. Until it has run, Silver persistence stops with `dedup_method_not_registered` (retried).

## 7. Resource limits (`SilverConfig`)

- Bronze input 8 MiB
- decompressed WARC 32 MiB
- payload 8 MiB
- 200k HTML nodes, depth 256
- 1M extracted characters
- 30 s parser safety deadline
- 20 near-dup candidates, 64 rows per bucket, 4M candidate characters
- 1 GiB free-disk guard for the index

Batch size and concurrency come from the acquisition configuration. Processing is single-threaded per unit, with no multiprocessing.

## 8. Known limits

- The language detector is a function-word model: short or tabular text is UNRESOLVED (rejected), and there is no trained model.
- The locked canonicalizer collapses indentation, so code loses leading whitespace in Silver.
- Site-level (cross-page) boilerplate is not learned. Cleaning is per page.
- Code files need at least 25 words of English comments or docstrings.
- The near-dup index is a local SQLite file. Multi-node use needs O-13.

## 9. PostgreSQL validation (kb_pipeline_test)

**Version labels.** Before the first persistence, the calibrated language detector and suitability classifier were relabelled. Each version is now defined once, in its component module:

| label | before | after |
|---|---|---|
| language detector | `lang1` | `lang2` |
| suitability filter | `suit1` | `suit2` |
| `extraction_version` | `x1-9e4aa54325` | `x1-51c9f32d50` |
| `policy_version` | `s1-c8379dc220` | `s1-65d0ac2798` |

**Commands.** Passwords come from pgpass, never from the URL.

```
set KB_MIGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test
python -m app.processing.silver.cli register-methods --expect-database kb_pipeline_test

set KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
python -m tests.integration.silver_pilot run        # before counts -> pilot -> reprocess -> after counts
set KB_RUN_PG_INTEGRATION=1
python -m unittest tests.integration.test_silver_postgres -v
```

**What the pilot is.** `tests/integration/silver_pilot.py` runs the real Runtime Engine worker, the real acquisition handlers with the Silver stage, and the Postgres stores. Only HTTP is faked (in-memory transports).

It uses fixed synthetic identities, because pilot rows are permanent:
- crawl `CC-MAIN-1999-00`
- repository `900000000001`
- `*.silver-pilot.example` URLs

It refuses any database other than `kb_pipeline_test`.

**Result.**
- 10 observations: 6 Common Crawl, 4 GitHub.
- 10 extractions: 8 documents, 2 `rejected_language`.
- 6 documents: 3 accepted, 1 `rejected_near_duplicate`, 1 `rejected_policy` (PII), 1 `quarantined` (secret).
- 2 exact-duplicate links: the CC mirror, and the GitHub copy of a CC page. All three observations resolve to one document.
- 4 dedup decisions (1 NEAR_DUPLICATE, 3 UNIQUE) and 1 relationship.
- 1 quarantine dead letter.

A rerun of the pilot adds zero rows. A reprocess of all 10 observations with fresh evaluations recomputes the same document IDs and detects no nondeterminism.
