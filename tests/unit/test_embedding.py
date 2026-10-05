"""Focused Embeddings V1 tests: frozen spec, vectors/hash, eligibility-driven enqueue, worker, 001C write contract."""

import random
import struct
import unittest
import uuid
from types import SimpleNamespace

import numpy as np

from app.processing.embedding import bootstrap, embedder, model, service
from app.processing.embedding.repository import dead_letter_key
from app.processing.embedding.service import EmbeddingWorker, enqueue_eligible
from app.processing.embedding.validator import (DIMENSION, VectorInvalid, check, from_literal, to_literal,
                                                vector_sha256)
from app.queue.retry import RetryPolicy

from tests.unit.embedding_fixtures import FakeEligibility, FakeEmbedder, InjectedFailure, InMemoryEmbeddingStore

REPS = [uuid.UUID(int=i) for i in range(1, 7)]
SUPPRESSED = {uuid.UUID(int=100 + i): REPS[i] for i in range(3)}
TEXTS = {c: f"Representative chunk {i} about hydropower and glaciers." for i, c in enumerate(REPS)}
TEXTS.update({c: f"Near-duplicate {i} of a representative." for i, c in enumerate(SUPPRESSED)})


def unit(seed: int) -> np.ndarray:
    v = np.random.default_rng(seed).standard_normal(DIMENSION)
    return (v / np.linalg.norm(v)).astype(np.float32)


class SpecTests(unittest.TestCase):
    def test_model_lock(self) -> None:
        row = model.registration_row()
        self.assertEqual(model.MODEL_NAME, "sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
        self.assertEqual((row["embedding_dimension"], row["vector_type"], row["distance_metric"], row["is_normalized"],
                          row["max_input_tokens"], row["status"]), (768, "vector", "cosine", True, 128, "candidate"))
        self.assertEqual(row["config"]["dtype"], "float32")
        self.assertIn("mean_tokens", row["config"]["pooling"])
        self.assertNotIn("PENDING", (model.MODEL_REVISION, model.MODEL_WEIGHTS_SHA256))
        self.assertRegex(model.MODEL_REVISION, r"^[0-9a-f]{40}$")
        self.assertNotIn("PENDING", model.PINNED_PACKAGES.values())

    def test_loaded_model_verification_refuses_a_different_pipeline(self) -> None:
        class Transformer:
            auto_model = type("XLMRobertaModel", (), {})()

        class Pooling:  # sentence-transformers >= 6 form
            def __init__(self):
                self.mode = "mean"

            def get_config_dict(self):
                return {"embedding_dimension": 768, "pooling_mode": self.mode}

        LegacyPooling = type("Pooling", (), {  # sentence-transformers 2.x form (boolean flags)
            "pooling_mode_mean_tokens": True, "pooling_mode_cls_token": False})

        class Fake(list):
            max_seq_length = 128

            def get_embedding_dimension(self):
                return 768

        Transformer.__module__ = Pooling.__module__ = LegacyPooling.__module__ = "sentence_transformers.models.x"
        embedder.verify_loaded(Fake([Transformer(), Pooling()]))
        embedder.verify_loaded(Fake([Transformer(), LegacyPooling()]))
        for change in ("dim", "pool", "legacy-pool", "len"):
            bad = Fake([Transformer(), Pooling()])
            if change == "dim":
                bad.get_embedding_dimension = lambda: 384
            elif change == "pool":
                bad[1].mode = "cls"
            elif change == "legacy-pool":
                bad[1] = LegacyPooling()
                bad[1].pooling_mode_cls_token = True
            else:
                bad.max_seq_length = 512
            with self.subTest(change), self.assertRaises(embedder.ModelMismatch):
                embedder.verify_loaded(bad)


class VectorTests(unittest.TestCase):
    def test_validation_rejects_bad_vectors(self) -> None:
        good = unit(1)
        self.assertAlmostEqual(check(good).norm, 1.0, places=5)
        bad = {"dims": good[:767], "nan": np.where(np.arange(768) == 5, np.nan, good),
               "inf": np.where(np.arange(768) == 5, np.inf, good), "norm": good * 1.5}
        for name, vector in bad.items():
            with self.subTest(name), self.assertRaises(VectorInvalid):
                check(vector.astype(np.float32))

    def test_sha256_is_big_endian_float32_and_the_literal_round_trips_bit_exactly(self) -> None:
        for seed in range(20):
            v = unit(seed)
            self.assertEqual(vector_sha256(v), __import__("hashlib").sha256(struct.pack(">768f", *v.tolist())).digest())
            back = from_literal(to_literal(v))
            self.assertEqual(back.tobytes(), v.tobytes())
            self.assertEqual(vector_sha256(back), vector_sha256(v))


class Harness(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryEmbeddingStore(dict(TEXTS))
        self.embedder = FakeEmbedder()
        self.eligibility = FakeEligibility(REPS, SUPPRESSED)
        self._original = service.eligibility
        service.eligibility = SimpleNamespace(eligible_chunks=self.eligibility.eligible_chunks)
        self.addCleanup(setattr, service, "eligibility", self._original)
        self.policy = RetryPolicy(max_attempts=3, base_delay_seconds=10, max_delay_seconds=60)

    def worker(self, **kwargs) -> EmbeddingWorker:
        return EmbeddingWorker(self.store, self.embedder, model_id=2, run_id=uuid.uuid4(), worker_id="t",
                               policy=self.policy, rng=random.Random(0), **kwargs)

    def enqueue(self, **kwargs):
        return enqueue_eligible(self.store.transaction, self.store, model_id=2, dedup_method_id=3,
                                limit=kwargs.pop("limit", 1000), batch_size=kwargs.pop("batch_size", 4),
                                max_attempts=3, **kwargs)

    def run_worker(self, **kwargs):
        return self.worker().run(self.store.transaction, limit=kwargs.pop("limit", 100),
                                 claim_batch=kwargs.pop("claim_batch", 4), **kwargs)


class EnqueueTests(Harness):
    def test_only_eligible_representatives_are_queued_once(self) -> None:
        report = self.enqueue()
        self.assertEqual((report.scanned_eligible, report.enqueued), (6, 6))
        self.assertEqual(set(self.store.queue), set(REPS))
        self.assertTrue(set(SUPPRESSED).isdisjoint(self.store.queue))  # never enqueued separately
        self.assertEqual(self.enqueue().enqueued, 0)  # rerun: no duplicate queue rows
        self.run_worker()
        self.assertEqual((self.enqueue().enqueued, len(self.store.queue)), (0, 0))  # already embedded

    def test_limit_and_resume(self) -> None:
        self.assertEqual(self.enqueue(limit=4, batch_size=3).enqueued, 4)
        self.assertEqual(self.enqueue().enqueued, 2)


class WorkerTests(Harness):
    def test_embeds_persists_results_and_vectors_one_to_one(self) -> None:
        self.enqueue()
        report = self.run_worker()
        self.assertEqual(report.counts["embedded_new"], 6)
        self.assertEqual(set(self.store.results), set(REPS))
        self.assertEqual(set(self.store.vectors), set(REPS))
        self.assertEqual(self.store.queue, {})
        for chunk_id, row in self.store.vectors.items():
            self.assertEqual(row["sha"], vector_sha256(self.embedder.vector(TEXTS[chunk_id])))
        self.assertTrue(set(SUPPRESSED).isdisjoint(self.store.vectors))

    def test_rerun_is_idempotent(self) -> None:
        self.enqueue()
        self.run_worker()
        before = self.store.state()
        self.enqueue()
        self.assertEqual((self.run_worker().claimed, self.store.state()), (0, before))

    def test_same_hash_rerun_of_an_embedded_chunk_is_a_no_op_success(self) -> None:
        self.enqueue()
        self.run_worker()
        before = self.store.state()
        self.store.enqueue(None, 2, [], max_attempts=3)
        self.store.queue[REPS[0]] = {**{"status": "pending", "attempt_count": 0, "max_attempts": 3,
                                        "next_attempt_at": self.store.now, "lease_token": None,
                                        "lease_expires_at": None, "priority": 1, "last_error_class": None,
                                        "last_error_message": None}}  # e.g. a manual re-enqueue
        report = self.run_worker()
        self.assertEqual(report.counts["already_embedded_same_hash"], 1)
        self.assertEqual(self.store.state(), before)

    def test_different_hash_is_nondeterminism_dead_lettered_never_overwritten(self) -> None:
        self.enqueue()
        self.run_worker()
        stored = dict(self.store.vectors[REPS[0]])
        self.store.queue[REPS[0]] = {"status": "pending", "attempt_count": 0, "max_attempts": 3,
                                     "next_attempt_at": self.store.now, "lease_token": None, "lease_expires_at": None,
                                     "priority": 1, "last_error_class": None, "last_error_message": None}
        self.embedder.corrupt[TEXTS[REPS[0]]] = "shifted"  # one ulp different: a different hash
        report = self.run_worker()
        self.assertEqual(report.counts["nondeterministic_dead_lettered"], 1)
        self.assertEqual(self.store.vectors[REPS[0]]["sha"], stored["sha"])  # untouched
        self.assertEqual(self.store.dead_letters[dead_letter_key(REPS[0], 2)]["error_code"], "embedding_nondeterministic")
        self.assertNotIn(REPS[0], self.store.queue)

    def test_lease_loss_rolls_the_write_back(self) -> None:
        self.enqueue()
        worker = self.worker()
        with self.store.transaction():
            claims = self.store.claim(None, 2, worker_id="t", limit=1, lease_seconds=60)
        self.store.queue[claims[0].chunk_id]["lease_token"] = uuid.uuid4()  # stolen by a newer claimer
        counts = __import__("collections").Counter()
        worker.persist(self.store.transaction, claims[0], check(self.embedder.vector(TEXTS[claims[0].chunk_id])),
                       counts)
        self.assertEqual(counts["lease_lost"], 1)
        self.assertNotIn(claims[0].chunk_id, self.store.results)  # no result row
        self.assertNotIn(claims[0].chunk_id, self.store.vectors)  # no vector

    def test_crash_between_result_and_vector_leaves_neither(self) -> None:
        self.enqueue()
        self.store.fail["insert_vector"] = 2
        report = self.run_worker()
        self.assertEqual(report.counts["retry_scheduled"], 1)
        self.assertEqual(set(self.store.results), set(self.store.vectors))  # never a result without its vector
        self.store.advance(120)
        self.run_worker()
        self.assertEqual(set(self.store.vectors), set(REPS))

    def test_transient_encode_failure_retries_with_backoff_then_dead_letters(self) -> None:
        self.enqueue(limit=1)
        for attempt in range(1, 4):
            self.embedder.fail_next = InjectedFailure("database went away")  # ConnectionError -> internal_bug retry
            self.run_worker()
            if attempt < 3:
                q = self.store.queue[REPS[0]]
                self.assertEqual((q["status"], q["attempt_count"]), ("retry_pending", attempt))
                self.assertGreater(q["next_attempt_at"], self.store.now - __import__("datetime").timedelta(0))
                self.store.advance(3600)
        self.assertNotIn(REPS[0], self.store.queue)
        self.assertEqual(self.store.dead_letters[dead_letter_key(REPS[0], 2)]["attempt_count"], 3)
        self.assertEqual(self.enqueue().enqueued, 5)  # the open dead letter is not re-enqueued

    def test_invalid_vectors_are_dead_lettered_immediately(self) -> None:
        self.enqueue(limit=3)
        for chunk_id, mode in zip(REPS[:3], ("nan", "inf", "unnormalized")):
            self.embedder.corrupt[TEXTS[chunk_id]] = mode
        report = self.run_worker()
        self.assertEqual(report.counts["dead_lettered"], 3)
        self.assertEqual(self.store.vectors, {})
        self.assertTrue(all(d["error_class"] == "data_invalid" for d in self.store.dead_letters.values()))

    def test_expired_lease_is_recovered_after_a_crash(self) -> None:
        self.enqueue(limit=2)
        with self.store.transaction():
            self.store.claim(None, 2, worker_id="crashed", limit=2, lease_seconds=60)
        self.assertEqual(self.run_worker().claimed, 0)  # still leased
        self.store.advance(61)
        report = self.run_worker()
        self.assertEqual((report.counts["lease_recovered"], report.counts["embedded_new"]), (2, 2))

    def test_failure_classification(self) -> None:
        from sqlalchemy.exc import OperationalError

        from app.queue.states import ErrorClass

        class OutOfMemoryError(RuntimeError):  # torch.OutOfMemoryError's name
            pass

        self.assertIs(service._classify(OperationalError("x", {}, Exception("gone"))).error_class, ErrorClass.TRANSIENT)
        self.assertIs(service._classify(OutOfMemoryError("cuda")).error_class, ErrorClass.RESOURCE_EXHAUSTED)
        self.assertIs(service._classify(VectorInvalid("nan")).error_class, ErrorClass.DATA_INVALID)
        self.assertIs(service._classify(ValueError("?")).error_class, ErrorClass.INTERNAL_BUG)

    def test_graceful_stop(self) -> None:
        self.enqueue()
        stop = __import__("threading").Event()
        stop.set()
        self.assertTrue(self.run_worker(stop_event=stop).stopped)
        self.assertEqual(self.store.vectors, {})


class GuardTests(unittest.TestCase):
    class Conn:
        def __init__(self, row):
            self.row = row

        def execute(self, *_a, **_k):
            row = self.row
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: row))

    def test_registration_refuses_wrong_database_role_or_revision(self) -> None:
        from app.processing.silver.bootstrap import BootstrapTargetError

        good = {"database": "kb_pipeline_test", "role": "kb_pipeline_migrator", "has_table": True,
                "revision": "kbp_001d"}
        for change in ({"database": "data_collection_pipeline"}, {"role": "kb_pipeline_app"},
                       {"revision": "kbp_001b"}):
            with self.subTest(change), self.assertRaises(BootstrapTargetError):
                bootstrap.register_model(self.Conn({**good, **change}), expect_database="kb_pipeline_test")


if __name__ == "__main__":
    unittest.main()
