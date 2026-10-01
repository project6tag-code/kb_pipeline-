"""Frozen Embeddings V1 model: ``sentence-transformers/paraphrase-multilingual-mpnet-base-v2``.

V1 deliberately locks the model already proven in the project notebook, so the
end-to-end pipeline can be completed. BGE / E5 / Arctic evaluation is deferred
to Embeddings V2, which will be a NEW ``embedding_models`` row; no V1 row or
vector is ever rewritten.

* **Model.** XLM-RoBERTa base (12 layers, hidden size 768, multilingual),
  pinned to an exact Hugging Face commit, loaded with
  ``SentenceTransformer(MODEL_NAME, revision=MODEL_REVISION)`` from the local
  cache only (``local_files_only``).
* **Pooling.** The model's own SentenceTransformer ``Pooling`` module (mean of
  token embeddings, attention-masked). It is never reconstructed by hand;
  ``verify_loaded`` checks it.
* **Input.** ``chunk_registry.chunk_text`` exactly as stored (canonical text):
  no prefix, no lowercasing (``do_lower_case = false``).
* **Max sequence length.** 128 subword tokens (the model's
  ``sentence_bert_config.json``). Longer inputs are TRUNCATED at the end by
  the tokenizer: only the first 128 tokens are embedded (see the V1 doc,
  "Limitations").
* **Output.** ``encode(..., normalize_embeddings=True, convert_to_numpy=True)``,
  forced to float32, 768 dimensions, L2 norm 1. Stored as ``vector(768)``
  with cosine distance (equal to inner product on unit vectors).
* **Determinism.** ``eval()``, ``inference_mode``, TF32 off, deterministic
  algorithms on, and ONE text per forward pass. Measured on this pilot:
  batched encoding changes the low-order bits with the batch's padding length
  (cosine >= 0.9999999), so it would make a recomputed hash depend on the
  queue neighbours. Per-text encoding is bit-identical across order, reruns
  and model reloads. Bits differ between CUDA and CPU (cosine 0.99999991), so
  the device class is recorded per run. See ``embedder.py``.

Every value below is stored in ``kbp.embedding_models.config``. The registration
bootstrap refuses a different stored config, so any change needs a new
``EMBEDDING_VERSION``.
"""

from __future__ import annotations

import hashlib
import json

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
MODEL_REVISION = "4328cf26390c98c5e3c738b4460a05b95f4911f5"  # exact Hugging Face commit (apache-2.0, not gated)
EMBEDDING_VERSION = "st-v1-mean-l2-trunc128"
DIMENSION = 768
MAX_SEQ_LENGTH = 128
VECTOR_TYPE = "vector"
DISTANCE_METRIC = "cosine"
REGISTRATION_STATUS = "candidate"  # schema §12.4 step 1; never active_gold here
MODEL_WEIGHTS_FILE = "model.safetensors"
MODEL_WEIGHTS_SHA256 = "b5722100700c48b74c9c199c5f39ff9493c94d1497f719e791d4bc3861d7714a"
LICENSE = "apache-2.0"

PINNED_PACKAGES = {  # exact versions this config was validated with (requirements-embeddings.txt)
    "torch": "2.11.0+cu128",
    "sentence-transformers": "6.1.0",
    "transformers": "5.18.0",
    "tokenizers": "0.23.2",
    "numpy": "2.5.3",
}

CONFIG: dict = {
    "framework": "sentence-transformers",
    "license": LICENSE,
    "architecture": "XLMRobertaModel",
    "modules": ["Transformer", "Pooling"],  # the model's modules.json; verified on load (embedder.verify_loaded)
    "pooling": "mean_tokens (model-defined SentenceTransformer Pooling)",
    "max_seq_length": MAX_SEQ_LENGTH,
    "truncation": "tokenizer truncation at max_seq_length (end of text dropped); no sliding window",
    "do_lower_case": False,
    "input": "kbp.chunk_registry.chunk_text verbatim; no prefix or instruction",
    "dtype": "float32",
    "normalize": "L2 (encode normalize_embeddings=True)",
    "norm_tolerance": 1e-3,
    "dimension": DIMENSION,
    "vector_sha256": "sha256 of 768 float32 big-endian = sha256(substring(vector_send(embedding) FROM 5))",
    "inference": "model.eval(); torch.inference_mode(); deterministic algorithms; TF32 disabled",
    "batching": "one text per forward pass (padding-free): the vector depends only on the text and device class",
    "device_policy": "cuda when available, else cpu; vector bits are device-class specific (each run records its "
                     "device in pipeline_runs.notes)",
    "weights_file": MODEL_WEIGHTS_FILE,
    "weights_sha256": MODEL_WEIGHTS_SHA256,
    "packages": PINNED_PACKAGES,
}


def config_sha256() -> str:
    return hashlib.sha256(json.dumps(CONFIG, sort_keys=True).encode("utf-8")).hexdigest()


def registration_row() -> dict:
    return {"model_name": MODEL_NAME, "model_revision": MODEL_REVISION, "embedding_version": EMBEDDING_VERSION,
            "embedding_dimension": DIMENSION, "vector_type": VECTOR_TYPE, "distance_metric": DISTANCE_METRIC,
            "is_normalized": True, "max_input_tokens": MAX_SEQ_LENGTH, "config": CONFIG,
            "status": REGISTRATION_STATUS}
