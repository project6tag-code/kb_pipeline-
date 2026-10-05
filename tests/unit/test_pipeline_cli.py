"""Operator orchestration: stage plan, bounded options, and target refusal (no database or network)."""

import unittest

from app.pipeline import cli


def parse(*argv):
    return cli.build_parser().parse_args(["run", "--expect-database", "kb_pipeline_test", *argv])


class PlanTests(unittest.TestCase):
    def names(self, *argv):
        return [s.name for s in cli.build_stages(parse(*argv))]

    def test_full_plan_runs_every_stage_in_order(self) -> None:
        self.assertEqual(self.names(), [
            "cc_plan", "cc_acquire_silver", "gh_plan", "gh_acquire_silver", "chunking", "chunk_dedup",
            "embedding_enqueue", "embedding_work", "gold_publish"])

    def test_source_selection_and_skips(self) -> None:
        self.assertEqual(self.names("--sources", "github")[:2], ["gh_plan", "gh_acquire_silver"])
        self.assertNotIn("gh_plan", self.names("--sources", "cc"))
        self.assertNotIn("cc_plan", self.names("--sources", "github"))
        only_downstream = self.names("--skip-acquisition", "--reprocess", "--skip-gold")
        self.assertEqual(only_downstream[0], "silver_reprocess")
        self.assertNotIn("gold_publish", only_downstream)

    def test_acquisition_stages_always_attach_silver(self) -> None:
        for stage in cli.build_stages(parse()):
            if "acquire" in stage.name:
                self.assertEqual(stage.module, "app.processing.silver.cli")

    def test_bounds_become_acquisition_settings_shared_by_plan_and_work(self) -> None:
        args = parse("--cc-crawl", "CC-MAIN-2013-20", "--cc-partitions", "1", "--cc-mime", "application/xhtml+xml",
                     "--gh-windows", "1", "--gh-max-files", "4", "--gh-min-stars", "300")
        stages = {s.name: s for s in cli.build_stages(args)}
        self.assertEqual(stages["cc_plan"].env, stages["cc_acquire_silver"].env)
        self.assertEqual(stages["cc_plan"].env["KB_CC_CRAWLS"], "CC-MAIN-2013-20")
        self.assertEqual(stages["cc_plan"].env["KB_CC_BLOCKS_PER_PARTITION"], "1")
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
    good = {"database": "kb_pipeline_test", "role": "kb_pipeline_app", "revision": "kbp_001d",
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


if __name__ == "__main__":
    unittest.main()
