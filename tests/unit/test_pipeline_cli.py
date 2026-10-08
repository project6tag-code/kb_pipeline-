"""Operator orchestration: stage plan, bounded options, and target refusal (no database or network)."""

import os
import unittest
from unittest import mock

from app.config import features, target
from app.pipeline import cli


def parse(*argv):
    return cli.build_parser().parse_args(["run", "--expect-database", "kb_pipeline_test", *argv])


class PlanTests(unittest.TestCase):
    def setUp(self) -> None:
        # Stage planning is tested for both sources; GitHub is opt-in in this release (see ReleaseGuardTests).
        patcher = mock.patch.dict(os.environ, {features.GITHUB_ENV: "1"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def names(self, *argv):
        return [s.name for s in cli.build_stages(parse(*argv))]

    def test_default_plan_is_common_crawl_only_reference_first(self) -> None:
        self.assertEqual(self.names(), [
            "cc_plan_reference", "cc_plan_open", "cc_acquire_silver", "chunking", "chunk_dedup",
            "embedding_enqueue", "embedding_work", "gold_publish"])

    def test_full_plan_with_both_sources_runs_every_stage_in_order(self) -> None:
        self.assertEqual(self.names("--sources", "cc,github"), [
            "cc_plan_reference", "cc_plan_open", "cc_acquire_silver", "gh_plan", "gh_acquire_silver", "chunking",
            "chunk_dedup", "embedding_enqueue", "embedding_work", "gold_publish"])

    def test_cc_profiles(self) -> None:
        stages = {s.name: s for s in cli.build_stages(parse("--cc-open-stride", "7"))}
        self.assertEqual(stages["cc_plan_reference"].env["KB_CC_PROFILE"], "reference")
        self.assertNotIn("KB_CC_PARTITION_STRIDE", stages["cc_plan_reference"].env)
        self.assertEqual((stages["cc_plan_open"].env["KB_CC_PROFILE"], stages["cc_plan_open"].env["KB_CC_PARTITION_STRIDE"]),
                         ("open", "7"))
        self.assertEqual(self.names("--cc-profile", "reference")[:2], ["cc_plan_reference", "cc_acquire_silver"])
        self.assertEqual(self.names("--cc-profile", "open")[:2], ["cc_plan_open", "cc_acquire_silver"])

    def test_source_selection_and_skips(self) -> None:
        self.assertEqual(self.names("--sources", "github")[:2], ["gh_plan", "gh_acquire_silver"])
        self.assertNotIn("gh_plan", self.names("--sources", "cc"))
        self.assertNotIn("cc_plan_reference", self.names("--sources", "github"))
        only_downstream = self.names("--skip-acquisition", "--reprocess", "--skip-gold")
        self.assertEqual(only_downstream[0], "silver_reprocess")
        self.assertNotIn("gold_publish", only_downstream)

    def test_acquisition_stages_always_attach_silver(self) -> None:
        for stage in cli.build_stages(parse()):
            if "acquire" in stage.name:
                self.assertEqual(stage.module, "app.processing.silver.cli")

    def test_bounds_become_acquisition_settings_shared_by_plan_and_work(self) -> None:
        args = parse("--sources", "cc,github", "--cc-crawl", "CC-MAIN-2013-20", "--cc-partitions", "1",
                     "--cc-mime", "application/xhtml+xml", "--gh-windows", "1", "--gh-max-files", "4",
                     "--gh-min-stars", "300")
        stages = {s.name: s for s in cli.build_stages(args)}
        plan_env = {k: v for k, v in stages["cc_plan_reference"].env.items() if k != "KB_CC_PROFILE"}
        self.assertEqual(plan_env, stages["cc_acquire_silver"].env)
        self.assertEqual(stages["cc_plan_reference"].env["KB_CC_CRAWLS"], "CC-MAIN-2013-20")
        self.assertEqual(stages["cc_plan_open"].env["KB_CC_BLOCKS_PER_PARTITION"], "1")
        self.assertEqual(stages["gh_acquire_silver"].env["KB_GH_MAX_FILES_PER_SNAPSHOT"], "4")
        self.assertEqual(stages["gh_plan"].args, ("plan", "--max-windows", "1"))

    def test_every_downstream_stage_carries_the_database_guard(self) -> None:
        for stage in cli.build_stages(parse()):
            if stage.name in ("chunking", "chunk_dedup", "embedding_enqueue", "embedding_work", "gold_publish"):
                self.assertEqual(stage.args[stage.args.index("--expect-database") + 1], "kb_pipeline_test")

    def test_invalid_source_is_refused(self) -> None:
        with self.assertRaises(cli.Refused):
            cli.parse_sources("cc,other")


class TargetTests(unittest.TestCase):
    good = {"database": "kb_pipeline_test", "role": "kb_pipeline_app", "revision": "kbp_001e",
            "model_status": "active_gold"}

    def test_exact_target_is_accepted(self) -> None:
        cli.check_target(self.good, "kb_pipeline_test")

    def test_everything_else_is_refused(self) -> None:
        for change in ({"database": "data_collection_pipeline"}, {"role": "postgres"}, {"revision": "kbp_001c"},
                       {"model_status": "candidate"}):
            with self.subTest(change=change), self.assertRaises(cli.Refused):
                cli.check_target({**self.good, **change}, "kb_pipeline_test")
        with self.assertRaises(cli.Refused):
            cli.check_target({**self.good, "database": "data_collection_pipeline"}, "data_collection_pipeline")


class ConfigurableDatabaseTests(unittest.TestCase):
    good = TargetTests.good

    def with_env(self, value):
        patcher = mock.patch.dict(os.environ, {target.ENV_NAME: value} if value is not None else {}, clear=False)
        if value is None:
            os.environ.pop(target.ENV_NAME, None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_default_is_the_local_test_database(self) -> None:
        self.with_env(None)
        self.assertEqual(target.expected_database(), "kb_pipeline_test")

    def test_a_configured_name_is_enforced_on_both_sides(self) -> None:
        self.with_env("kb_prod")
        cli.check_target({**self.good, "database": "kb_prod"}, "kb_prod")
        with self.assertRaises(cli.Refused):  # --expect-database must equal the configured name
            cli.check_target({**self.good, "database": "kb_prod"}, "kb_pipeline_test")
        with self.assertRaises(cli.Refused):  # and the connection must really reach it
            cli.check_target({**self.good, "database": "kb_pipeline_test"}, "kb_prod")

    def test_protected_or_invalid_names_are_refused(self) -> None:
        for bad in ("data_collection_pipeline", "postgres", "template1", "Kb-Prod", "1db", "a b", "x" * 64):
            with self.subTest(bad=bad):
                self.with_env(bad)
                with self.assertRaises(target.TargetConfigError):
                    target.expected_database()
                with self.assertRaises(cli.Refused):
                    cli.check_target({**self.good, "database": bad}, bad)

    def test_gold_and_bridge_guards_use_the_same_setting(self) -> None:
        from app.processing.gold import service
        from app.processing.gold_bridge import spec

        self.with_env("kb_prod")
        self.assertEqual(spec.allowed_databases(), frozenset({"kb_prod"}))

        class Conn:
            def execute(self, *_a):  # never reached: the name check comes first
                raise AssertionError("connection used before the name check")

        with self.assertRaises(service.GoldPublisherError):
            service.verify_target(Conn(), "kb_pipeline_test", role="kb_pipeline_app")


class ReleaseGuardTests(unittest.TestCase):
    """This release is Common Crawl only: every GitHub entry point refuses unless KB_ENABLE_GITHUB=1."""

    def setUp(self) -> None:
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop(features.GITHUB_ENV, None)

    def test_pipeline_run_refuses_github(self) -> None:
        for value in ("github", "cc,github"):
            with self.subTest(sources=value), self.assertRaises(cli.Refused):
                cli.build_stages(parse("--sources", value))
        self.assertEqual(cli.parse_sources("cc"), ("cc",))
        self.assertEqual([s.name for s in cli.build_stages(parse())][:3],
                         ["cc_plan_reference", "cc_plan_open", "cc_acquire_silver"])

    def test_worker_and_planner_refuse_github(self) -> None:
        from app.sources.github import cli as gh_cli
        from app.workers import acquisition_worker

        with self.assertRaises(features.FeatureDisabled):
            acquisition_worker.run_github_worker(max_iterations=1)
        with self.assertRaises(features.FeatureDisabled):
            gh_cli.cmd_plan(None, None)

    def test_opt_in_values(self) -> None:
        for value, enabled in (("1", True), ("true", True), ("YES", True), ("0", False), ("", False), ("no", False)):
            with self.subTest(value=value):
                self.assertEqual(features.github_enabled({features.GITHUB_ENV: value}), enabled)


if __name__ == "__main__":
    unittest.main()
