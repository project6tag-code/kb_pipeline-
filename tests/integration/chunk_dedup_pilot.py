"""Deterministic Global Chunk Near-Dedup V1 pilot against PostgreSQL ``kb_pipeline_test`` ONLY.

    set KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m tests.integration.chunk_dedup_pilot run        # state before -> pilot -> state after
    python -m tests.integration.chunk_dedup_pilot state      # read-only state, audit and expectations
    python -m tests.integration.chunk_dedup_pilot rebuild    # delete + rebuild the side index, replay audit

Prerequisite (once, migrator): python -m app.processing.chunk_dedup.cli register-method --expect-database kb_pipeline_test

Phases, all on PostgreSQL:

A. **Inline acquisition.** A Common Crawl page plus a GitHub repository
   (README + docs) run through the Runtime Engine, the real handlers, Silver
   and inline Chunking V1.
B. **Near-dedup backfill.** One worker, a provenance-tracked run, over every
   chunk without a decision (older pilot chunks included).
C. **Incremental arrival.** A second repository adds new near and fact
   variants, and TWO concurrent workers decide them, sharing the side index.

The cases covered:

* exact: shared text, one chunk_registry row, CC + GitHub occurrences;
* NEAR, cross-source: CC prose vs GitHub markdown bullets;
* related same topic;
* unique;
* mismatches: number, date, entity, negation, and comparative/direction.

Identities are synthetic and fixed, so a rerun is safe. The rows are
permanent. It refuses any database but kb_pipeline_test.
"""

from __future__ import annotations

import json
import re
import sys
import threading
from pathlib import Path
from typing import Any

from tests.integration.chunk_pilot import run_backfill_pilot
from tests.integration.silver_pilot import PILOT_CRAWL, open_database

DATABASE = "kb_pipeline_test"
PILOT_ROOT = Path("data/silver/chunk-dedup-pilot")
PILOT_WARC_LEAF = "chunk-dedup-pilot-v1.warc.gz"
PILOT_CDX = "cdx-99002.gz"
PILOT_CDX_INCREMENTAL = "cdx-99003.gz"
REPO_A = 900_000_000_003
REPO_C = 900_000_000_004
REPO_D = 900_000_000_005  # phase C: one comparative variant per file (see build_incremental_sources)
URL = "https://www.chunk-dedup-pilot.example/kariba"

DAM = ("The Kariba Dam was completed in 1959 on the Zambezi River between Zambia and Zimbabwe. Its hydroelectric "
       "station generates about 1,626 megawatts of electricity for both countries. The reservoir behind the dam is "
       "one of the largest artificial lakes in the world by volume. Engineers monitor the plunge pool below the "
       "spillway because erosion threatens the foundations of the wall.")
GLACIER = ("Glacier mass balance in the Alps decreased sharply after 1980 according to the survey. Most mountain "
           "glaciers lost more ice than they gained in each measured year. The researchers compared stake readings "
           "with satellite altimetry and found higher melt rates at lower elevations. The long records help "
           "hydrologists plan the water supply for the valleys below.")
HYDRO = ("Hydropower converts the energy of flowing water into electricity by driving turbines connected to "
         "generators. Storage schemes hold water in a reservoir and release it through penstocks when demand rises, "
         "while run-of-river schemes use the natural flow with little or no storage. Pumped storage plants move "
         "water uphill when power is cheap and generate when it is expensive.")
WHEAT = ("Durum wheat is a hard species used mainly for pasta and semolina. It grows best in hot and dry climates "
         "with cool nights, such as the plains of southern Italy and the Canadian prairies. Its high protein "
         "content and yellow endosperm give pasta a firm bite and a golden colour, and farmers sow it in spring in "
         "cold regions and in autumn in mild ones.")
BREAD = ("Sourdough bread relies on a culture of wild yeast and lactic acid bacteria that bakers keep alive by "
         "feeding it with flour and water every day. Bakers adjust hydration, temperature and proofing time to "
         "control the open crumb, the thick crust and the sour flavour of the loaf, and many keep the same starter "
         "for decades.")
RAIL = ("The Benguela Railway links the Atlantic port of Lobito with the mining towns of the Copperbelt. The line "
        "was built by a British company between 1903 and 1929 and carries copper, manganese and passengers across "
        "the central plateau. After years of closure during the civil war it was rebuilt by Chinese contractors "
        "and reopened to traffic in 2014.")
FISH = ("Kapenta are small freshwater sardines that were introduced into the lake from Lake Tanganyika in the "
        "1960s. Fishing crews work at night from pontoon rigs with lamps that attract the shoals into lift nets. "
        "The catch is dried in the sun on racks along the shore and sold across the region as a cheap source of "
        "protein.")
TEA = ("Tea estates on the slopes of the Mulanje massif produce most of the leaf exported from Malawi. The bushes "
       "are plucked by hand every seven to ten days during the rainy season, and the leaf is withered, rolled and "
       "fermented in factories on each estate. Auctions in Limbe set the price for the smallholder growers.")
COPPER = ("Copper ore from the Zambian Copperbelt is concentrated near the mines and then smelted into anodes. "
          "Refineries use electrolysis to plate pure copper onto cathodes, which are shipped by rail and road to "
          "ports on the Indian Ocean. The metal is used in cables, motors and plumbing throughout the world.")
SALT = ("The salt pans of the Makgadikgadi basin are the remains of an ancient lake that dried up thousands of years "
        "ago. In the wet season shallow water returns and attracts large flocks of flamingos. Local cooperatives "
        "harvest soda ash and salt from brine pumped out of the ground below the crust.")
RELATED_SAME_TOPIC = ("The Kariba Dam was completed in 1959 on the Zambezi River between Zambia and Zimbabwe. Its "
                      "hydroelectric station generates about 1,626 megawatts of electricity for both countries. The "
                      "dam wall is 128 metres high and 579 metres long, and floods in the wet season sometimes force "
                      "the operators to open several of its gates.")

CASES: dict[str, str] = {
    "cc_dam": DAM,
    "cc_glacier": GLACIER,
    "gh_dam_markdown": None,  # filled below: markdown bullets of DAM (cross-source NEAR)
    "shared_hydro": HYDRO,
    "unique_wheat": WHEAT,
    "unique_bread": BREAD,
    "date_mismatch": DAM.replace("1959", "1961"),
    "entity_mismatch": DAM.replace("Zambezi", "Limpopo"),
    "number_mismatch": DAM.replace("1,626", "1,470"),
    "negation_mismatch": DAM.replace("erosion threatens", "erosion does not threaten"),
    "direction_mismatch": GLACIER.replace("decreased", "increased"),
    "related_same_topic": RELATED_SAME_TOPIC,
    # phase C (incremental, two concurrent workers)
    "late_near_wording": DAM.replace("completed", "finished").replace("monitor", "inspect"),
    "late_comparative_mismatch": GLACIER.replace("higher melt", "lower melt"),
    "late_more_less_mismatch": GLACIER.replace("more ice", "less ice"),
}


def markdown(text: str, heading: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", text)
    first, _, rest = sentences[0].partition(" ")
    return f"## {heading}\n\n" + "\n".join([f"- **{first}** {rest}"] + [f"- {s}" for s in sentences[1:]])


CASES["gh_dam_markdown"] = markdown(DAM, "Zambezi scheme")
NEAR_CLUSTER = ("cc_dam", "gh_dam_markdown", "late_near_wording")  # exactly one representative
UNIQUE_CASES = ("shared_hydro", "unique_wheat", "unique_bread")
# Fact variants and the same-topic chunk: never NEAR_DUPLICATE, and RELATED-linked to their family. Which member
# of a family carries the classification RELATED depends on chunk_hash order (the first one has no earlier
# representative and is UNIQUE); the RELATED edge between them is what must exist.
FAMILIES = {
    "dam": ("cc_dam", "gh_dam_markdown", "late_near_wording", "date_mismatch", "entity_mismatch", "number_mismatch",
            "negation_mismatch", "related_same_topic"),
    "glacier": ("cc_glacier", "direction_mismatch", "late_comparative_mismatch", "late_more_less_mismatch"),
}
FACT_CASES = ("date_mismatch", "entity_mismatch", "number_mismatch", "negation_mismatch", "direction_mismatch",
              "related_same_topic", "late_comparative_mismatch", "late_more_less_mismatch")
# related_same_topic shares only two sentences with the dam chunks (J ~0.31). LSH 10x4 is tuned for the NEAR
# threshold (candidate probability 0.94 at J=0.70, ~0.09 at J=0.31), so its RELATED edge is best-effort:
# only "never merged" is guaranteed.
BEST_EFFORT_RELATED = ("related_same_topic",)


def build_sources():
    from tests.unit.cc_fixtures import FakeCommonCrawl, make_cdx_line, make_warc_record
    from tests.unit.gh_fixtures import FakeGitHub

    page = (f"<html><head><title>Kariba</title></head><body><main><article>"
            f"<h2>Kariba Dam</h2><p>{DAM}</p><h2>Glacier survey</h2><p>{GLACIER}</p>"
            f"<h2>Hydropower</h2><p>{HYDRO}</p><h2>Durum wheat</h2><p>{WHEAT}</p>"
            f"</article></main></body></html>").encode()
    site = FakeCommonCrawl()
    name, spans = site.add_warc([make_warc_record(URL, page)], leaf=PILOT_WARC_LEAF, crawl=PILOT_CRAWL)
    site.add_index([[make_cdx_line(URL, name, *spans[0], urlkey="example,chunk-dedup-pilot)/kariba")]],
                   cdx_file=PILOT_CDX, crawl=PILOT_CRAWL)
    gh = FakeGitHub()
    head = gh.add_repo(REPO_A, "chunk-dedup-pilot", "zambezi-notes", {
        "README.md": f"{CASES['gh_dam_markdown']}\n\n## Hydropower\n\n{HYDRO}\n\n## Baking\n\n{BREAD}\n",
        "docs/history.md": f"## Revised date\n\n{CASES['date_mismatch']}\n\n## Railway\n\n{RAIL}\n\n"
                           f"## Fisheries\n\n{FISH}\n",
        "docs/rivers.md": f"## Another river\n\n{CASES['entity_mismatch']}\n\n## Tea\n\n{TEA}\n\n"
                          f"## Copper\n\n{COPPER}\n",
        "docs/output.md": f"## Station output\n\n{CASES['number_mismatch']}\n\n## Salt pans\n\n{SALT}\n\n"
                          f"## Dam wall\n\n{CASES['related_same_topic']}\n",
        "docs/risk.md": f"## Spillway risk\n\n{CASES['negation_mismatch']}\n\n## Glacier trend\n\n"
                        f"{CASES['direction_mismatch']}\n\n## Railway freight\n\n{RAIL.replace('Lobito', 'Benguela')}\n",
    })
    return site, gh, head


def build_incremental_sources() -> list[tuple[tuple, int, str]]:
    """REPO_C (a late near variant; its docs/glaciers.md holds two near-identical paragraphs and is REJECTED by
    Silver for excessive repetition, kept as a record of that gate) and REPO_D (one comparative variant per file)."""
    from tests.unit.gh_fixtures import FakeGitHub

    gh_d = FakeGitHub()
    head_d = gh_d.add_repo(REPO_D, "chunk-dedup-pilot", "glacier-notes", {
        "docs/melt.md": f"## Melt at altitude\n\n{CASES['late_comparative_mismatch']}\n\n## Durum wheat\n\n{WHEAT}\n",
        "docs/balance.md": f"## Mass loss\n\n{CASES['late_more_less_mismatch']}\n\n## Salt pans\n\n{SALT}\n",
    })
    gh = FakeGitHub()
    head = gh.add_repo(REPO_C, "chunk-dedup-pilot", "late-arrivals", {
        "README.md": f"## Kariba notes\n\n{CASES['late_near_wording']}\n\n## Copper refining\n\n{COPPER}\n",
        "docs/glaciers.md": f"## Melt at altitude\n\n{CASES['late_comparative_mismatch']}\n\n## Tea auctions\n\n"
                            f"{TEA}\n\n## Mass loss\n\n{CASES['late_more_less_mismatch']}\n",
    })
    return [((None, gh, head), REPO_C, "chunk-dedup-pilot-late"),
            ((None, gh_d, head_d), REPO_D, "chunk-dedup-pilot-late-d")]


def run_acquisition(db) -> dict[str, Any]:
    from tests.integration.chunk_pilot import run_inline

    return {"phase_a": run_inline(db, PILOT_ROOT, sources=build_sources(), cdx=PILOT_CDX, repo=REPO_A,
                                  name="chunk-dedup-pilot", iterations=4)}


def run_incremental_acquisition(db) -> dict[str, Any]:
    from tests.integration.chunk_pilot import run_inline

    return {name: run_inline(db, PILOT_ROOT, sources=sources, cdx=PILOT_CDX_INCREMENTAL, repo=repo, name=name,
                             iterations=4) for sources, repo, name in build_incremental_sources()}


def index_path() -> Path:
    from app.processing.chunk_dedup.index import default_index_path

    return default_index_path(DATABASE)


def run_dedup(db, *, batch_size: int = 7) -> dict[str, Any]:
    from app.processing.chunk_dedup.cli import backfill_run

    return backfill_run(db, database=DATABASE, index_path=index_path(), limit=100_000, batch_size=batch_size)


def run_concurrent_dedup(workers: int = 2) -> list[dict[str, Any]]:
    """Two workers, two connection pools, one shared side-index file: they serialize on the method lock."""
    results: list[Any] = [None] * workers

    def work(slot: int) -> None:
        db = open_database()
        try:
            results[slot] = run_dedup(db, batch_size=2)
        except BaseException as exc:  # surfaced to the caller
            results[slot] = exc
        finally:
            db.dispose()

    threads = [threading.Thread(target=work, args=(n,)) for n in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(300)
    errors = [r for r in results if isinstance(r, BaseException)]
    if errors:
        raise errors[0]
    return results


# --------------------------------------------------------------------------- #
# Read-only state
# --------------------------------------------------------------------------- #

def dedup_state(db) -> dict[str, Any]:
    from sqlalchemy import text

    from app.processing.chunk_dedup import eligibility, method, repository

    with db.transaction() as conn:
        one = lambda sql, **p: conn.execute(text(sql), p).scalar_one()  # noqa: E731
        row = conn.execute(text(repository.GET_METHOD_SQL), {"algorithm": method.ALGORITHM,
                                                              "version": method.VERSION}).one_or_none()
        state: dict[str, Any] = {
            "method_rows": one("SELECT count(*) FROM kbp.dedup_methods WHERE algorithm = :a AND algorithm_version = :v",
                               a=method.ALGORITHM, v=method.VERSION),
            "method_parameters_frozen": row is not None and dict(row[1]) == method.PARAMETERS,
            "chunk_registry": one("SELECT count(*) FROM kbp.chunk_registry"),
            "chunk_occurrences": one("SELECT count(*) FROM kbp.chunk_occurrences"),
        }
        if row is None:
            return state
        method_id = int(row[0])
        state["dedup_method_id"] = method_id
        state["audit"] = repository.audit(conn, method_id)
        state["duplicate_decision_keys"] = one(
            "SELECT count(*) - count(DISTINCT (chunk_id, dedup_method_id)) FROM kbp.chunk_dedup_decisions")
        state["duplicate_pairs_any_direction"] = one(
            "SELECT count(*) FROM kbp.chunk_relationships a JOIN kbp.chunk_relationships b "
            "ON a.chunk_id = b.related_chunk_id AND a.related_chunk_id = b.chunk_id "
            "AND a.dedup_method_id = b.dedup_method_id")
        state["decision_runs"] = [dict(r) for r in conn.execute(text(
            "SELECT r.run_kind, r.status, r.code_version, count(*) AS decisions FROM kbp.chunk_dedup_decisions d "
            "JOIN kbp.pipeline_runs r ON r.run_id = d.run_id WHERE d.dedup_method_id = :m "
            "GROUP BY r.run_kind, r.status, r.code_version ORDER BY 1, 2, 3"), {"m": method_id}).mappings()]
        rows = conn.execute(text(
            "SELECT c.chunk_id, c.chunk_text, d.classification, d.representative_chunk_id, d.similarity_score "
            "FROM kbp.chunk_registry c LEFT JOIN kbp.chunk_dedup_decisions d "
            "ON d.chunk_id = c.chunk_id AND d.dedup_method_id = :m ORDER BY c.chunk_hash"), {"m": method_id}).all()
        state["cases"] = {name: [{"chunk_id": str(r[0]), "classification": r[2],
                                  "representative": None if r[3] is None else str(r[3]),
                                  "similarity": None if r[4] is None else round(float(r[4]), 4)}
                                 for r in rows if body in r[1]] for name, body in CASES.items()}
        state["edges"] = sorted([str(r[0]), str(r[1]), r[2]] for r in conn.execute(text(
            "SELECT chunk_id, related_chunk_id, relationship_type FROM kbp.chunk_relationships "
            "WHERE dedup_method_id = :m"), {"m": method_id}))
        state["representative_map"] = {str(r[0]): str(r[1]) for r in conn.execute(text(
            "SELECT chunk_id, representative_chunk_id FROM kbp.chunk_dedup_decisions "
            "WHERE dedup_method_id = :m AND classification = 'NEAR_DUPLICATE' ORDER BY chunk_id"), {"m": method_id})}
        state["eligible_chunks"] = len(eligibility.eligible_chunks(conn, method_id, limit=100_000))
        state["suppressed_chunks"] = len(eligibility.suppressed_chunks(conn, method_id, limit=100_000))
        state["chunk_dedup_dead_letters"] = one(
            "SELECT count(*) FROM kbp.dead_letters WHERE item_type = 'chunk' AND stage = 'CHUNK_DEDUPLICATED'")
    state["expectations"] = check_expectations(state)
    return state


def check_expectations(state: dict[str, Any]) -> list[str]:
    problems = []
    cases = state["cases"]
    if any(len(rows) != 1 for rows in cases.values()):
        return [f"every case must resolve to exactly one chunk: {({n: len(r) for n, r in cases.items()})}"]
    row = {name: rows[0] for name, rows in cases.items()}
    edges = {tuple(sorted(e[:2])): e[2] for e in state["edges"]}

    def edge(a: str, b: str) -> str | None:
        return edges.get(tuple(sorted((row[a]["chunk_id"], row[b]["chunk_id"]))))

    reps = {row[n]["chunk_id"] for n in NEAR_CLUSTER if row[n]["classification"] != "NEAR_DUPLICATE"}
    if len(reps) != 1 or any(row[n]["classification"] == "NEAR_DUPLICATE" and row[n]["representative"] not in reps
                             for n in NEAR_CLUSTER):
        problems.append(f"the near cluster must have exactly one representative: {[row[n] for n in NEAR_CLUSTER]}")
    for name in UNIQUE_CASES:
        if row[name]["classification"] != "UNIQUE":
            problems.append(f"{name}: {row[name]['classification']} != UNIQUE")
    for name in FACT_CASES:
        family = next(members for members in FAMILIES.values() if name in members)
        linked = {edge(name, other) for other in family if other != name}
        if row[name]["classification"] == "NEAR_DUPLICATE" or "NEAR_DUPLICATE" in linked:
            problems.append(f"{name}: merged although its facts differ")
        if "RELATED" not in linked and name not in BEST_EFFORT_RELATED:
            problems.append(f"{name}: no RELATED edge to its family")
    violations = {k: v for k, v in state["audit"].items() if k.endswith("_violations") and v}
    if violations:
        problems.append(f"audit violations {violations}")
    if state["audit"]["chunks_without_decision"]:
        problems.append(f"{state['audit']['chunks_without_decision']} chunks without a decision")
    if state["duplicate_decision_keys"] or state["duplicate_pairs_any_direction"]:
        problems.append("duplicate logical rows")
    return problems


def provenance(db, chunk_id: str) -> list[dict[str, Any]]:
    """chunk -> occurrence -> Silver document -> extraction -> observation -> CC capture / GitHub file."""
    from sqlalchemy import text

    with db.transaction() as conn:
        return [dict(r) for r in conn.execute(text(
            "SELECT o.document_id, o.ordinal, d.status AS document_status, so.source_channel, so.source_url, "
            "w.warc_filename, cc.warc_record_offset, g.repository_id, g.file_path, g.commit_sha "
            "FROM kbp.chunk_occurrences o JOIN kbp.document_registry d ON d.document_id = o.document_id "
            "JOIN kbp.observation_extractions e ON e.document_id = o.document_id "
            "JOIN kbp.source_observations so ON so.observation_id = e.observation_id "
            "LEFT JOIN kbp.cc_captures cc ON cc.observation_id = so.observation_id "
            "LEFT JOIN kbp.cc_warc_files w ON w.warc_file_id = cc.warc_file_id "
            "LEFT JOIN kbp.github_file_observations g ON g.observation_id = so.observation_id "
            "WHERE o.chunk_id = CAST(:c AS uuid) ORDER BY so.source_channel, g.file_path"), {"c": chunk_id}).mappings()]


def counts(state: dict[str, Any]) -> dict[str, Any]:
    audit = state.get("audit", {})
    return {k: audit.get(k) for k in ("decisions", "near_duplicate", "related", "unique", "relationships",
                                      "near_edges", "related_edges")} | {
        "representatives": len(set(state.get("representative_map", {}).values())),
        "method_rows": state["method_rows"], "dead_letters": state.get("chunk_dedup_dead_letters")}


def rebuild_proof(db) -> dict[str, Any]:
    """Fingerprint the side index, delete ONLY that file, rebuild from PostgreSQL, compare, replay every decision."""
    from app.processing.chunk_dedup import repository
    from app.processing.chunk_dedup.index import CandidateIndex, IndexIdentity
    from app.processing.chunk_dedup.service import rebuild_index, replay, resolve_method

    store = repository.PostgresChunkDedupStore()
    with db.transaction() as conn:
        method_id = resolve_method(store, conn)
    path = index_path()
    index = CandidateIndex(path, IndexIdentity(DATABASE, method_id))
    before = {"fingerprint": index.fingerprint(), **index.bucket_stats()}
    index.close()
    decisions_before = counts(dedup_state(db))
    for leftover in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        leftover.unlink(missing_ok=True)
    built = rebuild_index(db.transaction, store, path, database=DATABASE)
    with db.transaction() as conn:
        replayed = replay(repository.decisions_in_order(conn, method_id),
                          repository.stored_relationships(conn, method_id))
    after_rerun = run_dedup(db)
    return {"before": before, "rebuilt": built, "same_fingerprint": before["fingerprint"] == built["fingerprint"],
            "replay": replayed, "backfill_after_rebuild": {"chunks": after_rerun["chunks"]},
            "decisions_unchanged": counts(dedup_state(db)) == decisions_before}


def main(argv: list[str]) -> int:
    command = argv[0] if argv else "state"
    db = open_database()
    try:
        if command == "state":
            print(json.dumps(dedup_state(db), indent=2, default=str))
            return 0
        if command == "rebuild":
            print(json.dumps(rebuild_proof(db), indent=2, default=str))
            return 0
        before = dedup_state(db)
        acquisition = run_acquisition(db)
        backfill_chunking = run_backfill_pilot(db)
        phase_b = run_dedup(db)
        incremental = run_incremental_acquisition(db)
        phase_c = run_concurrent_dedup()
        after = dedup_state(db)
        print(json.dumps({"before_counts": counts(before), "acquisition": acquisition,
                          "chunking_backfill": backfill_chunking, "phase_b": phase_b, "incremental": incremental,
                          "phase_c_workers": phase_c, "after_counts": counts(after), "after": after}, indent=2,
                         default=str))
        return 0 if not after["expectations"] else 1
    finally:
        db.dispose()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
