"""Operator commands for Embeddings V1 (``paraphrase-multilingual-mpnet-base-v2``; shadow vectors; no Gold).

    python -m app.processing.embedding.cli model
        the frozen model spec (no I/O)
    python -m app.processing.embedding.cli download
        ONLINE, once: fetch the pinned revision into the local Hugging Face cache and check the weights hash
    python -m app.processing.embedding.cli register-model --expect-database NAME
        MIGRATOR ONLY: register the frozen model in kbp.embedding_models (idempotent, status 'candidate')
    python -m app.processing.embedding.cli enqueue --expect-database NAME [--limit N] [--batch-size N]
        queue eligible representative chunks that have no embedding for this model (idempotent)
    python -m app.processing.embedding.cli work --expect-database NAME [--limit N] [--claim-batch N]
            [--encode-batch-size N] [--device auto|cpu|cuda]
        embed queued chunks (local model only) and persist stored_shadow results + vectors
    python -m app.processing.embedding.cli status --expect-database NAME
        read-only counts and audit (queue, results, vectors, hashes, eligibility, suppressed chunks)
    python -m app.processing.embedding.cli verify --expect-database NAME [--sample N]
        read-only: recompute stored embeddings locally and compare vector_sha256

``register-model`` uses KB_MIGRATION_DATABASE_URL (kb_pipeline_migrator); the
others use KB_DATABASE_URL (kb_pipeline_app). ``work`` and ``verify`` set
HF_HUB_OFFLINE=1: no network, no embedding API.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import threading
import time

from app.processing.embedding import model as spec


def _offline() -> None:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"


def _stop_event() -> threading.Event:
    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            signal.signal(sig, lambda *_: stop.set())  # finish the current batch, then stop
    return stop


def _ids(conn) -> tuple[int, int]:
    from app.processing.chunk_dedup import eligibility
    from app.processing.embedding.repository import PostgresEmbeddingStore
    from app.processing.embedding.service import resolve_model

    return resolve_model(PostgresEmbeddingStore(), conn), eligibility.resolve_method_id(conn)


def cmd_model(_args: argparse.Namespace) -> int:
    print(json.dumps({**spec.registration_row(), "config_sha256": spec.config_sha256()}, indent=2))
    return 0


def cmd_download(_args: argparse.Namespace) -> int:
    import hashlib
    from pathlib import Path

    from huggingface_hub import snapshot_download

    path = Path(snapshot_download(spec.MODEL_NAME, revision=spec.MODEL_REVISION,
                                  allow_patterns=["*.json", "*.safetensors", "sentencepiece.bpe.model", "1_Pooling/*",
                                                  "README.md"]))
    digest = hashlib.sha256()
    with open(path / spec.MODEL_WEIGHTS_FILE, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    size = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
    result = {"path": str(path), "files_bytes": size, "weights_sha256": digest.hexdigest(),
              "weights_sha256_matches_spec": digest.hexdigest() == spec.MODEL_WEIGHTS_SHA256}
    print(json.dumps(result, indent=2))
    return 0 if result["weights_sha256_matches_spec"] or spec.MODEL_WEIGHTS_SHA256 == "PENDING" else 1


def cmd_register_model(args: argparse.Namespace) -> int:
    from app.db.migration_config import create_migration_engine, get_migration_url
    from app.processing.embedding.bootstrap import register_model

    engine = create_migration_engine(get_migration_url())
    try:
        with engine.begin() as conn:
            print(json.dumps(register_model(conn, expect_database=args.expect_database)))
        return 0
    finally:
        engine.dispose()


def cmd_enqueue(args: argparse.Namespace) -> int:
    from app.processing.chunking.cli import _runtime_db
    from app.processing.embedding.repository import PostgresEmbeddingStore
    from app.processing.embedding.service import enqueue_eligible

    db = _runtime_db(args.expect_database)
    try:
        with db.transaction() as conn:
            model_id, method_id = _ids(conn)
        report = enqueue_eligible(db.transaction, PostgresEmbeddingStore(), model_id=model_id,
                                  dedup_method_id=method_id, limit=args.limit, batch_size=args.batch_size,
                                  stop_event=_stop_event())
        print(json.dumps(vars(report), indent=2))
        return 0
    finally:
        db.dispose()


def run_worker(db, *, limit: int, claim_batch: int, encode_batch_size: int, device: str,
               lease_seconds: int = 300, stop_event: threading.Event | None = None, embedder=None) -> dict:
    """One provenance-tracked worker run: open a pipeline_runs row, embed, close it with the metrics."""
    from app.db import repositories
    from app.processing.embedding.embedder import SentenceTransformerEmbedder
    from app.processing.embedding.repository import PostgresEmbeddingStore
    from app.processing.embedding.service import EmbeddingWorker
    from app.queue.states import RunKind, RunStatus

    store = PostgresEmbeddingStore()
    with db.transaction() as conn:
        model_id, _method_id = _ids(conn)
        run = repositories.create_run(conn, RunKind.REPROCESS, code_version=f"embedding:{spec.EMBEDDING_VERSION}",
                                      config_sha256=bytes.fromhex(spec.config_sha256()),
                                      notes="embeddings v1 worker")
    status, summary = RunStatus.FAILED, {}
    try:
        load = None
        if embedder is None:
            _offline()
            embedder = SentenceTransformerEmbedder(device=device)
            load = vars(embedder.report)
        worker = EmbeddingWorker(store, embedder, model_id=model_id, run_id=run.run_id,
                                 worker_id=f"embed-{socket.gethostname()}-{os.getpid()}", lease_seconds=lease_seconds,
                                 encode_batch_size=encode_batch_size)
        started = time.perf_counter()
        report = worker.run(db.transaction, limit=limit, claim_batch=claim_batch, stop_event=stop_event)
        wall = time.perf_counter() - started
        new = report.counts["embedded_new"]
        summary = {"run_id": str(run.run_id), "load": load, **report.summary(), "wall_seconds": round(wall, 3),
                   "chunks_per_second_wall": round(new / wall, 2) if new and wall else None,
                   "peak_vram_bytes": getattr(embedder, "peak_vram_bytes", lambda: None)()}
        status = RunStatus.ABORTED if report.stopped else RunStatus.SUCCEEDED
        return summary
    finally:
        with db.transaction() as conn:
            repositories.finish_run(conn, run.run_id, status, notes=json.dumps(summary, default=str)[:4000])


def cmd_work(args: argparse.Namespace) -> int:
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    try:
        print(json.dumps(run_worker(db, limit=args.limit, claim_batch=args.claim_batch,
                                    encode_batch_size=args.encode_batch_size, device=args.device,
                                    stop_event=_stop_event()), indent=2, default=str))
        return 0
    finally:
        db.dispose()


def status_report(conn) -> dict:
    from app.processing.chunk_dedup import eligibility
    from app.processing.embedding import repository

    model_id, method_id = _ids(conn)
    eligible = eligibility.eligible_chunks(conn, method_id, limit=10_000_000)
    suppressed = eligibility.suppressed_chunks(conn, method_id, limit=10_000_000)
    resolution = eligibility.eligibility(conn, method_id, [s[0] for s in suppressed])
    rep_ids = [r.embed_chunk_id for r in resolution.values()]
    reps_with_vector = {r[0] for r in conn.execute(_t(
        "SELECT chunk_id FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = :m "
        "AND chunk_id = ANY (CAST(:ids AS uuid[]))"), {"m": model_id, "ids": rep_ids})}
    return {"embedding_model_id": model_id, "dedup_method_id": method_id,
            "eligible_representatives": len(eligible), "suppressed_near_duplicates": len(suppressed),
            "suppressed_resolution": [{"chunk_id": str(c), "status": r.status, "embed_chunk_id": str(r.embed_chunk_id),
                                       "representative_has_vector": r.embed_chunk_id in reps_with_vector}
                                      for c, r in sorted(resolution.items())],
            **repository.status(conn, model_id, method_id)}


def _t(sql: str):
    from sqlalchemy import text

    return text(sql)


def cmd_status(args: argparse.Namespace) -> int:
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    try:
        with db.transaction() as conn:
            print(json.dumps(status_report(conn), indent=2, default=str))
        return 0
    finally:
        db.dispose()


def verify_sample(db, *, sample: int, device: str, encode_batch_size: int, embedder=None) -> dict:
    """Recompute stored embeddings with the local model and compare vector_sha256 (read-only)."""
    import numpy as np

    from app.processing.embedding.embedder import SentenceTransformerEmbedder
    from app.processing.embedding.validator import as_float32_matrix, check, from_literal

    with db.transaction() as conn:
        model_id, _m = _ids(conn)
        rows = conn.execute(_t(
            "SELECT v.chunk_id, c.chunk_text, v.vector_sha256, CAST(v.embedding AS text) FROM kbp.chunk_embedding_vectors v "
            "JOIN kbp.chunk_registry c USING (chunk_id) WHERE v.embedding_model_id = :m ORDER BY v.chunk_id LIMIT :n"),
            {"m": model_id, "n": sample}).all()
    if embedder is None:
        _offline()
        embedder = SentenceTransformerEmbedder(device=device)
    matrix = as_float32_matrix(embedder.encode([r[1] for r in rows], encode_batch_size), len(rows))
    same, cosines = 0, []
    for row, vector in zip(rows, matrix):
        checked = check(vector)
        same += checked.sha256 == bytes(row[2])
        cosines.append(float(np.dot(checked.values.astype(np.float64), from_literal(row[3]).astype(np.float64))))
    return {"device": embedder.device, "sampled": len(rows), "identical_sha256": same,
            "min_cosine_to_stored": round(min(cosines), 9) if cosines else None}


def cmd_verify(args: argparse.Namespace) -> int:
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    try:
        result = verify_sample(db, sample=args.sample, device=args.device, encode_batch_size=args.encode_batch_size)
        print(json.dumps(result, indent=2))
        return 0 if result["identical_sha256"] == result["sampled"] else 1
    finally:
        db.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.embedding.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("model")
    sub.add_parser("download")
    for name in ("register-model", "enqueue", "work", "status", "verify"):
        command = sub.add_parser(name)
        command.add_argument("--expect-database", required=True,
                             help="the exact database name the connection must be on (refuses otherwise)")
        if name == "enqueue":
            command.add_argument("--limit", type=int, default=100_000, help="maximum eligible chunks scanned")
            command.add_argument("--batch-size", type=int, default=500, help="chunk ids per transaction")
        if name in ("work", "verify"):
            command.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
            command.add_argument("--encode-batch-size", type=int, default=16)
        if name == "work":
            command.add_argument("--limit", type=int, default=10_000, help="maximum queue items claimed")
            command.add_argument("--claim-batch", type=int, default=32, help="queue items claimed per batch")
        if name == "verify":
            command.add_argument("--sample", type=int, default=8)
    args = parser.parse_args(argv)
    handler = {"model": cmd_model, "download": cmd_download, "register-model": cmd_register_model,
               "enqueue": cmd_enqueue, "work": cmd_work, "status": cmd_status, "verify": cmd_verify}[args.command]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
