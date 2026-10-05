"""Focused tests for the DCA_v2 V1 test Gold source bridge (offline; a fake connection keyed on the module SQL)."""

import copy
import json
import re
import unittest
import uuid

from app.processing.gold_bridge import bootstrap as b
from app.processing.gold_bridge import spec
from app.utils.ids import KB_NAMESPACE


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def scalar_one(self):
        return self.one()[0]

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class FakeDb:
    """The kb_pipeline_test bridge tables, with their PK / UNIQUE rules. Records every executed statement."""

    def __init__(self, *, database="kb_pipeline_test", role="postgres", superuser=True, revision="kbp_001d") -> None:
        self.database, self.role, self.superuser, self.revision = database, role, superuser, revision
        self.sources: dict[uuid.UUID, tuple] = {}
        self.datasets: dict[uuid.UUID, tuple] = {}
        self.map: dict[str, tuple] = {}
        self.columns = copy.deepcopy(spec.EXPECTED_COLUMNS)
        self.constraints = copy.deepcopy(spec.EXPECTED_CONSTRAINTS)
        self.executed: list[str] = []

    def execute(self, clause, params=None):
        sql = clause.text if hasattr(clause, "text") else str(clause)
        self.executed.append(sql)
        p = params or {}
        if sql == b.IDENTITY_SQL:
            return _Result([{"database": self.database, "role": self.role, "superuser": self.superuser,
                             "has_kbp": True, "datasets_owner": "postgres", "dataset_sources_owner": "postgres"}])
        if sql == b.REVISION_SQL:
            return _Result([(self.revision,)])
        if sql == b.PIN_SEARCH_PATH_SQL:
            return _Result([])
        if sql == b.COLUMNS_SQL:
            return _Result([(n, t, nn, False, "", "") for n, t, nn in self.columns[p["table"].split(".")[1]]])
        if sql == b.CONSTRAINTS_SQL:
            return _Result(list(self.constraints[p["table"].split(".")[1]]))
        if sql == b.TRIGGERS_SQL:
            return _Result([(0,)])
        if sql == b.SOURCE_BY_NAME_SQL:
            return _Result([r for r in self.sources.values() if r[1] == p["name"]])
        if sql == b.SOURCE_BY_ID_SQL:
            return _Result([self.sources[p["id"]]] if p["id"] in self.sources else [])
        if sql == b.INSERT_SOURCE_SQL:
            if p["id"] in self.sources or any(r[1] == p["name"] for r in self.sources.values()):
                return _Result([])  # ON CONFLICT DO NOTHING (PK or uq_dataset_sources_name)
            self.sources[p["id"]] = (p["id"], p["name"], p["source_type"], p["authentication_required"], p["status"])
            return _Result([(p["id"],)])
        if sql == b.DATASET_BY_ID_SQL:
            return _Result([self.datasets[p["id"]]] if p["id"] in self.datasets else [])
        if sql == b.DATASETS_BY_NAME_SQL:
            return _Result([(k,) for k, r in self.datasets.items() if r[2] == p["dataset_name"]])
        if sql == b.INSERT_DATASET_SQL:
            assert p["source_id"] in self.sources, "FK datasets_source_id_fkey"
            if p["id"] in self.datasets:
                return _Result([])
            self.datasets[p["id"]] = (p["id"], p["source_id"], p["dataset_name"], p["status"], json.loads(p["metadata"]))
            return _Result([(p["id"],)])
        if sql == b.MAP_BY_CHANNEL_SQL:
            return _Result([self.map[p["source_channel"]]] if p["source_channel"] in self.map else [])
        if sql == b.MAP_BY_DATASET_SQL:
            return _Result([(r[0],) for r in self.map.values() if r[3] == p["dataset_id"] or r[2] == p["bridge"]])
        if sql == b.INSERT_MAP_SQL:
            assert p["dataset_id"] in self.datasets, "FK source_dataset_map_dataset_id_fkey"
            assert p["dataset_source_name"] == p["source_channel"], "CHECK dataset_source_name = source_channel"
            assert p["bridge_identity_name"] == "dataset-bridge:v1:" + p["source_channel"], "CHECK bridge identity"
            if p["source_channel"] in self.map:
                return _Result([])
            self.map[p["source_channel"]] = (p["source_channel"], p["dataset_source_name"], p["bridge_identity_name"],
                                             p["dataset_id"])
            return _Result([(p["source_channel"],)])
        if sql == b.COUNTS_SQL:
            return _Result([{"dataset_sources": len(self.sources), "datasets": len(self.datasets),
                             "source_dataset_map": len(self.map)}])
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    def as_migrator(self):
        self.role, self.superuser = "kb_pipeline_migrator", False
        return self

    def state(self):
        return copy.deepcopy((self.sources, self.datasets, self.map))


def run_both(db: FakeDb) -> tuple[dict, dict]:
    db.role, db.superuser = "postgres", True
    admin = b.bootstrap_public(db, expect_database="kb_pipeline_test")
    return admin, b.bootstrap_map(db.as_migrator(), expect_database="kb_pipeline_test")


class SpecTests(unittest.TestCase):
    def test_deterministic_identities_and_frozen_dataset_ids(self) -> None:
        cc, gh = spec.CHANNELS["common_crawl"], spec.CHANNELS["github"]
        self.assertEqual(cc.dataset.id, uuid.UUID("a7d8d2e1-8400-5916-9440-b492e8e692ce"))
        self.assertEqual(gh.dataset.id, uuid.UUID("b487f559-5f8e-56ba-9746-28658b9d09d1"))
        self.assertEqual(cc.source.id, uuid.uuid5(KB_NAMESPACE, "dataset-source:v1:common_crawl"))
        self.assertEqual(gh.source.id, uuid.uuid5(KB_NAMESPACE, "dataset-source:v1:github"))
        self.assertEqual((cc.source.source_type, cc.source.authentication_required, cc.source.status),
                         ("document", False, "ACTIVE"))
        self.assertEqual((gh.source.source_type, gh.source.authentication_required, gh.source.status),
                         ("dataset_repository", True, "ACTIVE"))
        self.assertEqual((cc.dataset.dataset_name, gh.dataset.dataset_name), ("Common Crawl", "Public GitHub"))
        self.assertEqual((cc.mapping.bridge_identity_name, gh.mapping.dataset_source_name),
                         ("dataset-bridge:v1:common_crawl", "github"))

    def test_kb_reference_status_and_no_invented_metadata(self) -> None:
        for ch in spec.CHANNELS.values():
            with self.subTest(ch.source.name):
                self.assertEqual(ch.dataset.status, "KB_REFERENCE")
                self.assertNotIn(ch.dataset.status, spec.LEGACY_WORKFLOW_STATUSES)
                self.assertEqual(set(ch.dataset.metadata), {"purpose", "source_channel"})
                self.assertNotRegex(json.dumps(ch.dataset.metadata), r"https?://|\d{4}-\d{2}-\d{2}|_at\b|url|date")

    def test_unknown_source_refused(self) -> None:
        for bad in ("kaggle", "huggingface", "web", "COMMON_CRAWL", ""):
            with self.subTest(bad), self.assertRaises(ValueError):
                spec.channel(bad)


class GuardTests(unittest.TestCase):
    def test_database_name_guard(self) -> None:
        for expect, connected in (("data_collection_pipeline", "data_collection_pipeline"),
                                  ("kb_pipeline_test", "data_collection_pipeline"),
                                  ("other", "other"), ("kb_pipeline_test", "kb_pipeline_prod")):
            with self.subTest(expect=expect, connected=connected), self.assertRaises(b.BridgeRefused):
                b.verify_target(FakeDb(database=connected), expect, stage="admin")

    def test_revision_guard(self) -> None:
        with self.assertRaises(b.BridgeRefused):
            b.verify_target(FakeDb(revision="kbp_001c"), "kb_pipeline_test", stage="admin")

    def test_role_guards(self) -> None:
        for role in ("kb_pipeline_migrator", "kb_pipeline_app", "someone"):
            with self.subTest(role), self.assertRaises(b.BridgeRefused):
                b.verify_target(FakeDb(role=role, superuser=False), "kb_pipeline_test", stage="admin")
        with self.assertRaises(b.BridgeRefused):
            b.verify_target(FakeDb(role="postgres"), "kb_pipeline_test", stage="migrator")

    def test_exact_public_schema_verification(self) -> None:
        b.verify_public_schema(FakeDb())
        mutations = {
            "extra column": lambda db: db.columns["datasets"].append(("description", "text", False)),
            "type change": lambda db: db.columns["datasets"].__setitem__(3, ("status", "character varying(20)", True)),
            "nullable": lambda db: db.columns["dataset_sources"].__setitem__(4, ("status", "character varying(20)", False)),
            "extra constraint": lambda db: db.constraints["datasets"].add(("chk", "c", "CHECK (status <> '')")),
        }
        for name, mutate in mutations.items():
            db = FakeDb()
            mutate(db)
            with self.subTest(name), self.assertRaises(b.BridgeRefused):
                b.verify_public_schema(db)

    def test_stage_b_requires_stage_a(self) -> None:
        with self.assertRaises(b.BridgeRefused):
            b.bootstrap_map(FakeDb().as_migrator(), expect_database="kb_pipeline_test")


class BootstrapTests(unittest.TestCase):
    def test_first_run_creates_exact_rows_and_maps_both_channels(self) -> None:
        db = FakeDb()
        admin, mapped = run_both(db)
        self.assertEqual(set(admin["actions"].values()) | set(mapped["actions"].values()), {"created"})
        self.assertEqual((admin["after"], mapped["after"]), (
            {"dataset_sources": 2, "datasets": 2, "source_dataset_map": 0},
            {"dataset_sources": 2, "datasets": 2, "source_dataset_map": 2}))
        self.assertEqual(b.resolve_dataset_id(db, "common_crawl"), spec.FROZEN_DATASET_IDS["common_crawl"])
        self.assertEqual(b.resolve_dataset_id(db, "github"), spec.FROZEN_DATASET_IDS["github"])
        with self.assertRaises(ValueError):
            b.resolve_dataset_id(db, "kaggle")

    def test_second_run_is_a_no_op(self) -> None:
        db = FakeDb()
        run_both(db)
        before = db.state()
        admin, mapped = run_both(db)
        self.assertEqual(set(admin["actions"].values()) | set(mapped["actions"].values()), {"reused"})
        self.assertEqual(db.state(), before)

    def test_reuses_existing_identical_rows(self) -> None:
        db = FakeDb()
        for ch in spec.CHANNELS.values():  # pre-provisioned identically, e.g. by an earlier admin run
            s = ch.source
            db.sources[s.id] = (s.id, s.name, s.source_type, s.authentication_required, s.status)
        admin = b.bootstrap_public(db, expect_database="kb_pipeline_test")
        self.assertEqual({k: v for k, v in admin["actions"].items() if k.startswith("dataset_sources")},
                         {"dataset_sources.common_crawl": "reused", "dataset_sources.github": "reused"})

    def test_conflicts_are_refused_and_nothing_is_modified(self) -> None:
        cc = spec.CHANNELS["common_crawl"]

        def same_name_other_id(db):
            other = uuid.uuid4()
            db.sources[other] = (other, "common_crawl", "document", False, "ACTIVE")

        def same_id_other_fields(db):
            s = cc.source
            db.sources[s.id] = (s.id, s.name, s.source_type, s.authentication_required, "INACTIVE")

        def dataset_wrong_parent(db):
            run_both(db)
            d = cc.dataset
            db.datasets[d.id] = (d.id, spec.CHANNELS["github"].source.id, d.dataset_name, d.status, d.metadata)

        def dataset_legacy_status(db):
            s, d = cc.source, cc.dataset
            db.sources[s.id] = (s.id, s.name, s.source_type, s.authentication_required, s.status)
            db.datasets[d.id] = (d.id, d.source_id, d.dataset_name, "ACCEPTED", d.metadata)

        def dataset_name_taken(db):
            s = cc.source
            db.sources[s.id] = (s.id, s.name, s.source_type, s.authentication_required, s.status)
            other = uuid.uuid4()
            db.datasets[other] = (other, s.id, "Common Crawl", "KB_REFERENCE", {})

        def map_points_elsewhere(db):
            run_both(db)
            row = db.map["common_crawl"]
            db.map["common_crawl"] = (row[0], row[1], row[2], spec.FROZEN_DATASET_IDS["github"])

        for setup in (same_name_other_id, same_id_other_fields, dataset_wrong_parent, dataset_legacy_status,
                      dataset_name_taken, map_points_elsewhere):
            db = FakeDb()
            setup(db)
            snapshot = db.state()
            with self.subTest(setup.__name__), self.assertRaises(b.BridgeConflict):
                run_both(db)
            self.assertEqual(db.state(), snapshot)

    def test_no_update_delete_ddl_or_grant_path(self) -> None:
        statements = {n: v for n, v in vars(b).items() if n.endswith("_SQL") and isinstance(v, str)}
        for name, sql in statements.items():
            with self.subTest(name):
                self.assertNotRegex(sql.lower(), r"\b(update|delete|truncate|alter|drop|create|grant|revoke)\b")
        inserts = re.findall(r"insert into (\S+)", " ".join(statements.values()).lower())
        self.assertEqual(sorted(inserts), ["kbp.source_dataset_map", "public.dataset_sources", "public.datasets"])
        self.assertTrue(all("on conflict" in v.lower() for k, v in statements.items() if k.startswith("INSERT")))
        db = FakeDb()
        run_both(db)
        self.assertFalse(any(re.search(r"\b(update|delete|grant|revoke|alter)\b", s.lower()) for s in db.executed))


if __name__ == "__main__":
    unittest.main()
