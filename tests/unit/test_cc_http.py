"""HTTP layer: endpoint allow-list (SSRF), classification, range validation, timeouts, pooling (offline).

The stdlib transport is exercised against a loopback ``http.server`` only.
"""

import http.server
import socket
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

from app.queue.states import ErrorClass
from app.sources.commoncrawl.config import HttpConfig
from app.sources.commoncrawl.http import (
    COMMON_CRAWL,
    DisallowedEndpoint,
    Endpoints,
    FetchError,
    HttpClient,
    StdlibTransport,
    parse_retry_after,
    status_failure,
)

from tests.unit.cc_fixtures import FakeCommonCrawl, FakeTransport

KEY = "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/a.warc.gz"
DATA = bytes(range(256)) * 40  # 10240 bytes


class EndpointTests(unittest.TestCase):
    def test_approved_urls(self) -> None:
        self.assertEqual(COMMON_CRAWL.data_url(KEY), f"https://data.commoncrawl.org/{KEY}")
        self.assertEqual(COMMON_CRAWL.collinfo_url(), "https://index.commoncrawl.org/collinfo.json")
        COMMON_CRAWL.check("https://data.commoncrawl.org:443/cc-index/collections/CC-MAIN-2024-33/indexes/cluster.idx")

    def test_ssrf_attempts_are_refused(self) -> None:
        bad = [
            "http://data.commoncrawl.org/x",                        # not https
            "https://data.commoncrawl.org:8443/x",                   # other port
            "https://evil.example/x",
            "https://data.commoncrawl.org.evil.example/x",           # suffix spoof
            "https://evil.example#@data.commoncrawl.org/x",
            "https://user@data.commoncrawl.org/x",                   # user-info
            "https://data.commoncrawl.org@evil.example/x",
            "https://169.254.169.254/latest/meta-data/",             # cloud metadata
            "https://127.0.0.1/x", "https://[::1]/x",
            "https://data.commoncrawl.org/x?y=1",                    # query
            "https://data.commoncrawl.org/x#frag",
            "https://data.commoncrawl.org/a/../../etc/passwd",       # traversal
            "https://data.commoncrawl.org/a/./b", "https://data.commoncrawl.org//b",
            "https://data.commoncrawl.org/%2e%2e/b",                 # encoded traversal
            "https://data.commoncrawl.org/a\\b",
            "https://data.commoncrawl.org/a b", "https://data.commoncrawl.org/a\r\nHost: evil",
            "file:///etc/passwd", "gopher://data.commoncrawl.org/x", "",
        ]
        for url in bad:
            with self.subTest(url=url), self.assertRaises(DisallowedEndpoint):
                COMMON_CRAWL.check(url)

    def test_data_url_rejects_absolute_or_hostile_keys(self) -> None:
        for key in ("/etc/passwd", "https://evil.example/x", "../x", "a/../../x", "", "a?b"):
            with self.subTest(key=key), self.assertRaises(DisallowedEndpoint):
                COMMON_CRAWL.data_url(key)

    def test_origins_cannot_be_widened(self) -> None:
        for origin in ("https://evil.example", "http://data.commoncrawl.org", "https://data.commoncrawl.org:444"):
            with self.subTest(origin=origin), self.assertRaises(DisallowedEndpoint):
                Endpoints(data_origin=origin)
        with self.assertRaises(DisallowedEndpoint):
            Endpoints.loopback_for_tests("http://10.0.0.1:8080")
        loop = Endpoints.loopback_for_tests("http://127.0.0.1:8080")
        with self.assertRaises(DisallowedEndpoint):
            loop.check("https://data.commoncrawl.org/x")


class ClassificationTests(unittest.TestCase):
    def test_retry_after(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(parse_retry_after("120"), 120)
        self.assertEqual(parse_retry_after("99999999"), 86_400)
        self.assertEqual(parse_retry_after(format_datetime(now + timedelta(seconds=90)), now), 90)
        self.assertEqual(parse_retry_after(format_datetime(now - timedelta(seconds=90)), now), 0)
        for garbage in (None, "", "soon", "-5", "1.5"):
            self.assertIsNone(parse_retry_after(garbage))

    def test_status_classes(self) -> None:
        expected = {429: ErrorClass.RATE_LIMITED, 503: ErrorClass.RATE_LIMITED, 500: ErrorClass.TRANSIENT,
                    502: ErrorClass.TRANSIENT, 504: ErrorClass.TRANSIENT, 408: ErrorClass.TRANSIENT,
                    404: ErrorClass.PERMANENT_SOURCE, 403: ErrorClass.PERMANENT_SOURCE,
                    410: ErrorClass.PERMANENT_SOURCE, 416: ErrorClass.PERMANENT_SOURCE,
                    301: ErrorClass.PERMANENT_SOURCE, 302: ErrorClass.PERMANENT_SOURCE}
        for status, error_class in expected.items():
            with self.subTest(status=status):
                self.assertEqual(status_failure(status).error_class, error_class)
        self.assertEqual(status_failure(429, "30").retry_after_seconds, 30)


def fake_client(**config) -> tuple[HttpClient, FakeTransport]:
    site = FakeCommonCrawl()
    site.put(KEY, DATA)
    transport = FakeTransport(site)
    return HttpClient(HttpConfig(**config), transport=transport), transport


class RangeTests(unittest.TestCase):
    url = COMMON_CRAWL.data_url(KEY)

    def fetch(self, fault=None, offset=100, length=500, **config):
        client, transport = fake_client(**config)
        if fault:
            transport.faults[f"/{KEY}"].append(fault)
        return client.get_range(self.url, offset, length), transport

    def error(self, fault, **kwargs) -> FetchError:
        with self.assertRaises(FetchError) as ctx:
            self.fetch(fault, **kwargs)
        return ctx.exception

    def test_exact_range(self) -> None:
        body, transport = self.fetch()
        self.assertEqual(body, DATA[100:600])
        self.assertEqual(transport.requests, [(f"/{KEY}", "bytes=100-599")])

    def test_invalid_ranges(self) -> None:
        cases = [
            ({"ignore_range": True}, ErrorClass.TRANSIENT, "range_not_honoured"),
            ({"wrong_range": True}, ErrorClass.TRANSIENT, "range_mismatch"),
            ({"short": 7}, ErrorClass.TRANSIENT, "short_read"),
            ({"extra": 3}, ErrorClass.TRANSIENT, "range_mismatch"),
            ({"status": 404}, ErrorClass.PERMANENT_SOURCE, "http_404"),
            ({"status": 500}, ErrorClass.TRANSIENT, "http_500"),
            ({"status": 302, "headers": {"Location": "https://evil.example/"}}, ErrorClass.PERMANENT_SOURCE, "http_302"),
            ({"timeout": True}, ErrorClass.TRANSIENT, "timeout"),
            ({"read_timeout": True}, ErrorClass.TRANSIENT, "timeout"),
            ({"raise": ConnectionResetError("reset")}, ErrorClass.TRANSIENT, "network"),
        ]
        for fault, error_class, code in cases:
            with self.subTest(fault=fault):
                exc = self.error(fault)
                self.assertEqual((exc.error_class, exc.code), (error_class, code))

    def test_rate_limited_carries_retry_after(self) -> None:
        exc = self.error({"status": 429, "headers": {"Retry-After": "120"}})
        self.assertEqual((exc.error_class, exc.retry_after_seconds, exc.status), (ErrorClass.RATE_LIMITED, 120, 429))

    def test_range_beyond_the_object_is_permanent(self) -> None:
        exc = self.error(None, offset=len(DATA) - 10, length=100)
        self.assertEqual((exc.error_class, exc.code), (ErrorClass.PERMANENT_SOURCE, "range_beyond_eof"))

    def test_deadline(self) -> None:
        ticks = iter([0.0] + [1000.0] * 10)
        client, _transport = fake_client(read_timeout_seconds=1, request_deadline_seconds=5)
        client._clock = lambda: next(ticks)
        with self.assertRaises(FetchError) as ctx:
            client.get_range(self.url, 0, 100)
        self.assertEqual(ctx.exception.code, "deadline_exceeded")

    def test_disallowed_url_never_reaches_the_transport(self) -> None:
        client, transport = fake_client()
        with self.assertRaises(DisallowedEndpoint):
            client.get_range("https://evil.example/x", 0, 10)
        self.assertEqual(transport.requests, [])

    def test_connection_is_reused_only_after_a_clean_read(self) -> None:
        client, transport = fake_client()
        responses = []
        original = transport.open
        transport.open = lambda *a, **k: responses.append(original(*a, **k)) or responses[-1]
        client.get_range(self.url, 0, 10)
        transport.faults[f"/{KEY}"].append({"short": 1})
        with self.assertRaises(FetchError):
            client.get_range(self.url, 0, 10)
        self.assertEqual([r.closed_reusable for r in responses], [True, False])


class BodyTests(unittest.TestCase):
    def test_get_bytes_limit(self) -> None:
        client, _ = fake_client()
        self.assertEqual(client.get_bytes(COMMON_CRAWL.data_url(KEY), max_bytes=len(DATA)), DATA)
        with self.assertRaises(FetchError) as ctx:
            client.get_bytes(COMMON_CRAWL.data_url(KEY), max_bytes=100)
        self.assertEqual((ctx.exception.error_class, ctx.exception.code), (ErrorClass.DATA_INVALID, "response_too_large"))

    def test_iter_lines_streams_with_limits(self) -> None:
        site = FakeCommonCrawl()
        site.put("x/lines.txt", b"".join(b"line %d\n" % i for i in range(50_000)) + b"last")
        client = HttpClient(transport=FakeTransport(site))
        lines = list(client.iter_lines(COMMON_CRAWL.data_url("x/lines.txt"), max_line_bytes=100,
                                       max_total_bytes=10 ** 7))
        self.assertEqual((len(lines), lines[0], lines[-1]), (50_001, b"line 0", b"last"))
        with self.assertRaises(FetchError) as ctx:
            list(client.iter_lines(COMMON_CRAWL.data_url("x/lines.txt"), max_line_bytes=3, max_total_bytes=10 ** 7))
        self.assertEqual(ctx.exception.code, "line_too_long")
        with self.assertRaises(FetchError) as ctx:
            list(client.iter_lines(COMMON_CRAWL.data_url("x/lines.txt"), max_line_bytes=100, max_total_bytes=1000))
        self.assertEqual(ctx.exception.code, "response_too_large")


# --------------------------------------------------------------------------- #
# Stdlib transport against a loopback server
# --------------------------------------------------------------------------- #

class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "fake-cc"

    def log_message(self, *args) -> None:  # silence
        pass

    def do_GET(self) -> None:
        state = self.server.state
        with state["lock"]:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["paths"].append(self.path)
        try:
            if self.path == "/slow":
                time.sleep(0.5)
            if self.path == "/hold":
                time.sleep(0.2)
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "http://169.254.169.254/")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            data = DATA
            rng = self.headers.get("Range")
            if rng:
                start, end = (int(x) for x in rng[len("bytes="):].split("-"))
                body = data[start:end + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            else:
                body = data
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        finally:
            with state["lock"]:
                state["active"] -= 1


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.state = {"lock": threading.Lock(), "active": 0, "max_active": 0, "paths": [], "connections": 0}

    def handle_error(self, request, client_address) -> None:
        pass  # the client aborting a timed-out request is expected here

    def get_request(self):
        conn = super().get_request()
        self.state["connections"] += 1
        return conn


class StdlibTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _Server()
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        origin = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.endpoints = Endpoints.loopback_for_tests(origin)
        self.origin = origin

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def client(self, max_connections: int = 2, **config) -> HttpClient:
        return HttpClient(HttpConfig(**config), endpoints=self.endpoints,
                          transport=StdlibTransport(max_connections))

    def test_range_and_keep_alive_reuse(self) -> None:
        client = self.client()
        for n in range(5):
            self.assertEqual(client.get_range(f"{self.origin}/w/a.warc.gz", n * 10, 10), DATA[n * 10:n * 10 + 10])
        self.assertEqual(self.server.state["connections"], 1)
        client.close()

    def test_read_timeout(self) -> None:
        client = self.client(read_timeout_seconds=0.2, connect_timeout_seconds=1, request_deadline_seconds=1)
        with self.assertRaises(FetchError) as ctx:
            client.get_range(f"{self.origin}/slow", 0, 10)
        self.assertEqual((ctx.exception.error_class, ctx.exception.code), (ErrorClass.TRANSIENT, "timeout"))
        client.close()

    def test_redirect_is_not_followed(self) -> None:
        client = self.client()
        with self.assertRaises(FetchError) as ctx:
            client.get_range(f"{self.origin}/redirect", 0, 10)
        self.assertEqual(ctx.exception.code, "http_302")
        self.assertEqual(self.server.state["paths"], ["/redirect"])
        client.close()

    def test_pool_bounds_concurrent_connections(self) -> None:
        client = self.client(max_connections=2)
        errors = []

        def work() -> None:
            try:
                client.get_range(f"{self.origin}/hold", 0, 10)
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=work) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertEqual(errors, [])
        self.assertLessEqual(self.server.state["max_active"], 2)
        self.assertLessEqual(self.server.state["connections"], 2)
        client.close()

    def test_connection_refused_is_transient(self) -> None:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        client = HttpClient(HttpConfig(connect_timeout_seconds=1), transport=StdlibTransport(1),
                            endpoints=Endpoints.loopback_for_tests(f"http://127.0.0.1:{port}"))
        with self.assertRaises(FetchError) as ctx:
            client.get_range(f"http://127.0.0.1:{port}/x", 0, 1)
        self.assertEqual(ctx.exception.error_class, ErrorClass.TRANSIENT)


if __name__ == "__main__":
    unittest.main()
