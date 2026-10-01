"""Local SentenceTransformer embedder for Embeddings V1 (no network after download; no embedding API).

The core call is the one proven in the project notebook::

    SentenceTransformer(model_name).encode(texts, batch_size=..., normalize_embeddings=True,
                                           convert_to_numpy=True)  -> float32

It adds:

* an exact revision, loaded from the local cache only;
* verification that the loaded pipeline is the frozen one (modules, mean
  pooling, 768 dimensions, max_seq_length 128);
* deterministic inference (``eval``, ``inference_mode``, TF32 off,
  deterministic algorithms).

**Device policy.** ``auto`` uses CUDA when torch reports a usable device, else
CPU; ``cpu`` / ``cuda`` force a device. A CUDA failure at load time falls back
to CPU, and the fallback is recorded.

Heavy imports (torch, sentence-transformers) happen only when a model is loaded,
so the rest of the pipeline and its tests never need them.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Protocol

import numpy as np

from app.processing.embedding import model as spec


class Embedder(Protocol):
    device: str

    def encode(self, texts: list[str], batch_size: int) -> np.ndarray: ...


class ModelMismatch(RuntimeError):
    """The loaded model is not the frozen V1 pipeline."""


@dataclass
class LoadReport:
    device: str
    requested_device: str
    load_seconds: float
    torch_version: str
    cuda_version: str | None
    gpu: str | None
    fallback_reason: str | None = None
    extra: dict = field(default_factory=dict)


def _deterministic_torch():
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")  # required by deterministic cuBLAS
    import torch

    torch.use_deterministic_algorithms(True, warn_only=False)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_grad_enabled(False)
    return torch


class SentenceTransformerEmbedder:
    def __init__(self, *, device: str = "auto", revision: str = spec.MODEL_REVISION,
                 local_files_only: bool = True) -> None:
        torch = _deterministic_torch()
        from sentence_transformers import SentenceTransformer

        started = time.perf_counter()
        requested = device
        fallback = None
        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        try:
            self.model = SentenceTransformer(spec.MODEL_NAME, revision=revision, device=device,
                                             local_files_only=local_files_only)
            if device == "cuda":  # prove the device actually runs this architecture (e.g. sm_120 kernels)
                self.model.encode(["warm-up"], batch_size=1, convert_to_numpy=True)
        except Exception as exc:  # noqa: BLE001 - CUDA trouble must not block the phase
            if device != "cuda" or requested == "cuda":
                raise
            fallback = f"{type(exc).__name__}: {exc}"[:300]
            device = "cpu"
            self.model = SentenceTransformer(spec.MODEL_NAME, revision=revision, device="cpu",
                                             local_files_only=local_files_only)
        self.model.eval()
        self.device = device
        self.torch = torch
        verify_loaded(self.model)
        self.report = LoadReport(
            device=device, requested_device=requested, load_seconds=round(time.perf_counter() - started, 3),
            torch_version=torch.__version__, cuda_version=torch.version.cuda,
            gpu=torch.cuda.get_device_name(0) if device == "cuda" else None, fallback_reason=fallback)

    def encode(self, texts: list[str], batch_size: int) -> np.ndarray:
        """Encode ``texts``. ``batch_size`` only groups the call: each text is its own forward pass.

        Padding-free encoding makes the vector a function of the text and the
        device class alone. Batched encoding changes low-order bits with the
        padding length of the batch (measured), which would make a recomputed
        ``vector_sha256`` depend on which chunks happened to be claimed together.
        """
        rows = []
        with self.torch.inference_mode():
            for text in texts:
                vector = self.model.encode([text], batch_size=1, normalize_embeddings=True, convert_to_numpy=True,
                                           show_progress_bar=False)
                rows.append(np.asarray(vector, dtype=np.float32)[0])
        return np.stack(rows) if rows else np.zeros((0, spec.DIMENSION), dtype=np.float32)

    def token_lengths(self, texts: list[str]) -> list[int]:
        """Untruncated subword lengths (to measure how many inputs exceed max_seq_length)."""
        tokenizer = self.model.tokenizer
        return [len(tokenizer(t, add_special_tokens=True, truncation=False)["input_ids"]) for t in texts]

    def peak_vram_bytes(self) -> int | None:
        return int(self.torch.cuda.max_memory_allocated()) if self.device == "cuda" else None


def _pooling_mode(pooling) -> str:
    """The pooling mode via the module's public config: sentence-transformers >= 6 stores
    ``pooling_mode`` ('mean', 'cls', ...); older releases store ``pooling_mode_*`` booleans."""
    config = pooling.get_config_dict() if hasattr(pooling, "get_config_dict") else {}
    if "pooling_mode" in config:
        return str(config["pooling_mode"])
    flags = {"mean": "pooling_mode_mean_tokens", "cls": "pooling_mode_cls_token", "max": "pooling_mode_max_tokens",
             "mean_sqrt_len": "pooling_mode_mean_sqrt_len_tokens", "weightedmean": "pooling_mode_weightedmean_tokens",
             "lasttoken": "pooling_mode_lasttoken"}
    active = [name for name, key in flags.items() if config.get(key, getattr(pooling, key, False))]
    return active[0] if len(active) == 1 else "+".join(active) or "none"


def verify_loaded(model) -> None:
    """Refuse anything that is not the frozen V1 SentenceTransformer pipeline."""
    modules = [f"{type(m).__module__.rsplit('.', 1)[0]}.{type(m).__name__}" for m in model]
    problems = []
    if [m.rsplit(".", 1)[-1] for m in modules] != ["Transformer", "Pooling"]:
        problems.append(f"modules {modules}")
    if _pooling_mode(model[1]) != "mean":
        problems.append(f"pooling is {_pooling_mode(model[1])!r}, not mean")
    get_dimension = getattr(model, "get_embedding_dimension", None) or model.get_sentence_embedding_dimension
    dimension = get_dimension()
    if dimension != spec.DIMENSION:
        problems.append(f"dimension {dimension}")
    if model.max_seq_length != spec.MAX_SEQ_LENGTH:
        problems.append(f"max_seq_length {model.max_seq_length}")
    architecture = type(model[0].auto_model).__name__
    if architecture != spec.CONFIG["architecture"]:
        problems.append(f"architecture {architecture}")
    if problems:
        raise ModelMismatch("loaded model differs from the frozen V1 config: " + "; ".join(problems))
