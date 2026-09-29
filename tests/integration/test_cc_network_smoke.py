"""Opt-in REAL-NETWORK Common Crawl smoke test. Disabled by default; never run in CI.

Enable explicitly (network access to index.commoncrawl.org and data.commoncrawl.org):

    KB_RUN_CC_NETWORK_SMOKE=1
    python -m unittest tests.integration.test_cc_network_smoke -v

Bounded footprint (no database, Bronze in a temporary directory):
* 1 request for collinfo.json;
* 1 streamed request for the latest crawl's cluster.idx, stopped after the first
  line (only the first chunk is transferred);
* 1 byte-range request for ONE CDX block (~hundreds of KiB);
* at most ``KB_CC_SMOKE_MAX_RECORDS`` (default 3, max 10) WARC byte-range requests.
"""

import os
import tempfile
import unittest
from pathlib import Path

RUN = os.environ.get("KB_RUN_CC_NETWORK_SMOKE") == "1"


@unittest.skipUnless(RUN, "set KB_RUN_CC_NETWORK_SMOKE=1 to make real Common Crawl requests")
class CommonCrawlNetworkSmokeTests(unittest.TestCase):
    def test_discover_one_block_and_fetch_a_few_records(self) -> None:
        from app.sources.commoncrawl.bronze import BronzeStore
        from app.sources.commoncrawl.compression import decompress_single_member
        from app.sources.commoncrawl.config import AcquisitionConfig, CCSettings
        from app.sources.commoncrawl.connector import build_client
        from app.sources.commoncrawl.discovery import (
            MAX_CLUSTER_IDX_BYTES,
            MAX_CLUSTER_LINE_BYTES,
            cdx_key,
            cluster_idx_key,
            parse_cluster_line,
            parse_collinfo,
        )
        from app.sources.commoncrawl.index import IndexRowError, parse_cdx_line, rejection_reason, split_block_lines
        from app.sources.commoncrawl.warc import parse_warc_record
        from app.utils.ids import cc_observation_id

        max_records = min(int(os.environ.get("KB_CC_SMOKE_MAX_RECORDS", "3")), 10)
        settings = CCSettings.from_env()
        client = build_client(settings)
        try:
            crawls = parse_collinfo(client.get_bytes(client.endpoints.collinfo_url(), max_bytes=4 * 1024 * 1024))
            crawl = crawls[0]
            lines = client.iter_lines(client.endpoints.data_url(cluster_idx_key(crawl)),
                                      max_line_bytes=MAX_CLUSTER_LINE_BYTES, max_total_bytes=MAX_CLUSTER_IDX_BYTES)
            entry = parse_cluster_line(next(iter(lines)), max_block_bytes=16 * 1024 * 1024)
            lines.close()
            block = client.get_range(client.endpoints.data_url(cdx_key(crawl, entry.cdx_file)), entry.offset,
                                     entry.length)
            index_lines = split_block_lines(decompress_single_member(block, 128 * 1024 * 1024))
            self.assertGreater(len(index_lines), 0)

            with tempfile.TemporaryDirectory() as tmp:
                bronze = BronzeStore(Path(tmp))
                filter_config = AcquisitionConfig().filter
                fetched = 0
                for raw in index_lines:
                    try:
                        record = parse_cdx_line(raw, crawl)
                    except IndexRowError:
                        continue
                    if rejection_reason(record, filter_config) is not None:
                        continue
                    data = client.get_range(client.endpoints.data_url(record.filename), record.offset, record.length)
                    capture = parse_warc_record(data, record, max_decompressed=32 * 1024 * 1024)
                    oid = cc_observation_id(crawl, record.filename, record.offset, record.length)
                    bronze.store(crawl, oid, data, {"source_url": capture.target_uri,
                                                    "retrieved_at": "smoke-test"})
                    self.assertEqual(bronze.read_verified(crawl, oid)[0], data)
                    print(f"{oid} {capture.http_status} {record.url[:80]!r} title={capture.source_title!r:.60}")
                    fetched += 1
                    if fetched >= max_records:
                        break
                self.assertGreater(fetched, 0, "no candidate in the first CDX block")
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
