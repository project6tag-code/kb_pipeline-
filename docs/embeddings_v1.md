# Embeddings V1 — Local MPNet Shadow Embeddings

Status: **implemented and validated on `kb_pipeline_test`** at `kbp_001c`.

- The vectors are stored as **shadow** embeddings (`kbp.chunk_embedding_vectors`, Migration 001C).
- Gold is **not** started: nothing is written to `public.kb` or `public.metadata`, and no model is `active_gold`.

Code: `app/processing/embedding/`. Pins: `requirements-embeddings.txt`.

## 1. Model (frozen)

| Item | Value |
|---|---|
| model | `sentence-transformers/paraphrase-multilingual-mpnet-base-v2` (Apache-2.0, not gated) |
| revision | `4328cf26390c98c5e3c738b4460a05b95f4911f5` (exact Hugging Face commit) |
| weights | `model.safetensors`, SHA-256 `b5722100700c48b74c9c199c5f39ff9493c94d1497f719e791d4bc3861d7714a` |
| architecture | `XLMRobertaModel` (12 layers, hidden size 768, multilingual); modules `Transformer` → `Pooling` |
| pooling | the model's own SentenceTransformer `Pooling` (mean of token embeddings, attention-masked); never hand-built; verified on load |
| max sequence length | **128** subword tokens (`sentence_bert_config.json`); longer text is **truncated** at the end |
| input | `chunk_registry.chunk_text` verbatim: no prefix, no lowercasing |
| output | `encode(..., normalize_embeddings=True, convert_to_numpy=True)` → float32, 768 dimensions, L2 norm 1 |
| storage | `public.vector(768)`, distance `cosine` |
| registration | `kbp.embedding_models` id **2**: `embedding_version = st-v1-mean-l2-trunc128`, status **`candidate`** |
| packages | torch 2.11.0+cu128, sentence-transformers 6.1.0, transformers 5.18.0, tokenizers 0.23.2, numpy 2.5.3 (part of the registered config) |

**Why MPNet now.** It is the model already proven in the project notebook, so the end-to-end pipeline can be completed. BGE, E5 and Arctic evaluation is **deferred to Embeddings V2**, which will be a new `embedding_models` row. No V1 row or vector is ever rewritten.

**Local only.** `cli download` is the only online step: it fetches the pinned revision into the Hugging Face cache. `work` and `verify` set `HF_HUB_OFFLINE=1` and load with `local_files_only`. No embedding API is used.

**Config lock.** The full config (above, plus the batching and device policy) is stored in `embedding_models.config`.

- The bootstrap refuses an existing row whose config differs.
- The worker refuses a registered row that differs from the code.
- On load, `verify_loaded` refuses a model whose modules, pooling, dimension, max length or architecture differ.

## 2. Determinism: one text per forward pass

Measured on the 28 pilot texts, with `eval()`, `inference_mode`, TF32 off and deterministic algorithms on.

| Encoding | Bit-identical rerun? |
|---|---|
| same batch size and same set | yes |
| different batch size (1, 4, 32) | **no**: the padding length changes the reduction order (cosine ≥ 0.9999999) |
| **one text per forward pass (V1)** | **yes**: identical across order, reruns and two separate model loads (31/31, CUDA and CPU) |
| CUDA vs CPU | no (cosine 0.99999991): **device-class specific** |

Batched encoding would make a chunk's vector depend on which other chunks were claimed with it, so a recomputed `vector_sha256` could differ. V1 therefore encodes padding-free, one text per forward pass. `encode_batch_size` only groups the call.

The device class is recorded in every run's `pipeline_runs.notes`, and the default is `cuda`. A CPU worker produces valid but bit-different vectors, so recompute-verification must use the device class that wrote them.

## 3. Vector hash

`vector_sha256` = SHA-256 of the 768 normalized float32 components, **big-endian**, in order (the Migration 001C definition).

- PostgreSQL recomputes it as `sha256(substring(public.vector_send(embedding) FROM 5))`.
- Vectors are sent as the shortest round-trip float32 decimal, so the stored bytes equal the computed bytes. Every write proves this in-transaction.

## 4. Eligibility and suppressed near-duplicates

Enqueue takes chunk ids **only** from `app.processing.chunk_dedup.eligibility.eligible_chunks`: confirmed decisions that are not NEAR_DUPLICATE, i.e. representatives.

- **Suppressed chunks** are never enqueued or embedded. `eligibility(...).embed_chunk_id` maps each one to its representative, whose vector serves it.
- **Unchanged:** `chunk_registry`, occurrences, dedup decisions and provenance.

## 5. Queue and write flow

- **`enqueue`.** Keyset-pages the eligible ids and inserts `embedding_queue` rows only for chunks with no `chunk_embeddings` row for this model, no queue row and no open dead letter (`ON CONFLICT DO NOTHING`). It is bounded (`--limit`, `--batch-size`), resumable and rerunnable.
- **`work`.** Each loop:
  1. recover expired leases;
  2. dead-letter exhausted items;
  3. claim up to `--claim-batch` items (`FOR UPDATE SKIP LOCKED`, new `lease_token`, attempt + 1);
  4. read the texts and encode outside any transaction, then validate each vector (768 dimensions, no NaN or Inf, |norm − 1| ≤ 1e-3).
- **Per item, ONE transaction** (the 001C contract):
  1. INSERT `chunk_embeddings` (`stored_shadow`) `ON CONFLICT DO NOTHING`;
  2. INSERT `chunk_embedding_vectors` `ON CONFLICT DO NOTHING`;
  3. verify that both rows exist, that the stored hash equals the computed hash, and that the database's recomputation of the stored bytes equals the computed hash;
  4. lease-fenced DELETE of the queue row; 0 rows means the lease was lost, so ROLLBACK;
  5. COMMIT.
- **Never overwritten.** A different stored hash is **nondeterminism**: the write rolls back, then the item is dead-lettered (`internal_bug`, `embedding_nondeterministic`) and its queue row removed under the lease.

## 6. Retry and crash behaviour

Failures use the Runtime Engine controller (`app.queue.retry.decide`; policy: 5 attempts, base 10 s, cap 600 s, full jitter).

| Failure | Handling |
|---|---|
| `transient` (DB `OperationalError`), `resource_exhausted` (CUDA OOM), `internal_bug` | `retry_pending` with backoff; dead letter after the last attempt |
| `data_invalid` (bad vector, missing text) | dead letter immediately |
| lost lease | nothing written; another worker owns the item |
| crash | expired leases return to `retry_pending`; a partial write is impossible (one transaction) |

Dead letters have `item_type = 'chunk_embedding'`, `stage = 'EMBEDDED'` and key `<chunk_id>:<model_id>`. An open dead letter blocks re-enqueue until it is resolved. SIGINT/SIGTERM stop the loop after the current batch.

## 7. Operations

```
python -m app.processing.embedding.cli download                                                   # online, once
python -m app.processing.embedding.cli register-model --expect-database kb_pipeline_test          # migrator
python -m app.processing.embedding.cli enqueue --expect-database kb_pipeline_test [--limit N] [--batch-size N]
python -m app.processing.embedding.cli work    --expect-database kb_pipeline_test [--limit N] [--claim-batch N] [--device auto|cpu|cuda]
python -m app.processing.embedding.cli status  --expect-database kb_pipeline_test
python -m app.processing.embedding.cli verify  --expect-database kb_pipeline_test [--sample N] [--device cuda]
```

Every database command refuses any database other than `--expect-database`. `register-model` must run as `kb_pipeline_migrator` at `kbp_001c`.

## 8. Pilot results (kb_pipeline_test)

| Check | Result |
|---|---|
| before | 31 chunks: **28 eligible**, **3 suppressed**; embedding tables empty |
| registration | 1 row (id 2, `candidate`); rerun → same row; wrong database or runtime role refused |
| run 1 | enqueued 28; embedded 28 on CUDA; 28 `chunk_embeddings` (`stored_shadow`) = 28 vectors (1:1); queue empty; 0 dead letters |
| audit | 0 hash mismatches, 0 wrong dimensions, 0 norm violations, 0 results without a vector, 0 vectors without a result, 0 suppressed chunks embedded, 0 eligible chunks missing |
| suppressed | 3/3 resolve to a representative that has a vector (two to `7e063eff…`, one to `6130fcd0…`) |
| run 2 | 0 enqueued, 0 claimed; all counts and the 28-hash digest identical |
| recompute (`verify`, CUDA) | **28/28 identical `vector_sha256`** |
| recompute (CPU) | 0/4 identical, cosine 0.999999998 (device-class difference, as documented) |
| upstream unchanged | chunks 31, occurrences 35, dedup decisions 31, representative map hash, documents 18 |
| PostgreSQL tests | 5/5: pilot audit, suppressed resolution, rerun, write contract (same hash, different hash refused without overwrite, lost lease), lost lease on a new chunk leaves no rows; all probes rolled back |

## 9. Resources (pilot measurements; not a scale claim)

| Item | CUDA (RTX 5060 Laptop, 8 GB) | CPU |
|---|---|---|
| model cache | 1.1 GB (`model.safetensors` 1.11 GB) | same |
| load time | 5.3–5.9 s | 2.4 s |
| per-text encoding | 12–15 ms per chunk, 67–81 chunks/s | 125–350 ms per chunk, 2.8–8 chunks/s |
| batched (16), for reference only | 278 chunks/s (not used: not bit-reproducible) | 4.3 chunks/s |
| peak VRAM | ~1.1 GB (no OOM risk at 8 GB) | — |
| peak process RAM | ~2.2 GB | ~2.2 GB |
| pilot run 1 | 28 chunks: 0.39 s encode, 0.51 s wall | — |

At about 80 chunks/s per GPU, 100M chunks would take about 2 weeks of single-GPU time. This is arithmetic, not a measurement, and per-text encoding is the deliberate cost of reproducibility. Scaling means more GPUs or workers (the queue supports concurrent workers), or a V2 decision on batching with a tolerance-based recompute check.

## 10. Limitations

- **Truncation at 128 tokens.** 4 of the 28 pilot chunks exceed 128 subword tokens (lengths 68–197, median 87). Their embedding represents only the first 128 tokens. Chunking V1 targets about 256 estimated tokens, so this will be common at scale. A sliding window or a longer-context model is a V2 decision.
- **Multilingual paraphrase model, not retrieval-tuned.** English factual retrieval quality is not benchmarked here (deferred to V2).
- **Device-class-specific bits.** Exact-hash recompute verification needs the same device class.
- **Throughput.** Per-text encoding is about 3.4× slower than batched encoding on the GPU.
- **No ANN index and no Gold.** `public.kb` is untouched, the model is `candidate`, and Gold publication would copy these shadow vectors.
