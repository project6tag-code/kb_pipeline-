"""GitHub Acquisition V1 building blocks (offline): identity, qualification, integrity,
endpoint allow-list, rate-limit classification, token redaction, discovery windows."""

import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

from app.config.settings import SettingsError
from app.queue.states import ErrorClass
from app.sources.commoncrawl.http import DisallowedEndpoint
from app.sources.github.client import GITHUB_API, GitHubEndpoints, redact
from app.sources.github.config import DiscoveryConfig, FilePolicy, GitHubSettings, RepositoryPolicy
from app.sources.github.discovery import qualify_repository, query_id, windows
from app.sources.github.files import (
    BlobIntegrityError,
    TreeEntry,
    TreeError,
    git_blob_hash,
    parse_tree,
    rejection_reason,
    text_rejection,
    verify_blob,
)
from app.sources.github.rate_limit import GitHubFetchError, RateState, RateTracker, classify, parse_rate_headers
from app.utils.ids import github_observation_id

SHA1 = "0123456789abcdef0123456789abcdef01234567"
POLICY = FilePolicy()


def entry(path: str, *, mode: str = "100644", kind: str = "blob", size: int | None = 100, sha: str = SHA1):
    return TreeEntry(path, mode, kind, sha, size)


class IdentityTests(unittest.TestCase):
    def test_frozen_vectors_and_rename_independence(self) -> None:
        self.assertEqual(github_observation_id(123456789, SHA1, "docs/intro.md", "89abcdef0123456789abcdef0123456789abcdef"),
                         uuid.UUID("bf308fc0-77e3-5540-bcf0-5def36127e04"))
        self.assertEqual(github_observation_id(987654321, "a" * 64, "src/lib.rs", "b" * 64),
                         uuid.UUID("4119c85b-bde5-55b9-b978-194249414444"))  # SHA-256 object format
        # owner/name are not identity: a rename/transfer cannot change an observation id
        ids = {github_observation_id(1, SHA1, "a.md", SHA1) for _owner in ("alice", "bob")}
        self.assertEqual(len(ids), 1)
        with self.assertRaises(ValueError):
            github_observation_id(1, SHA1, "a.md", "b" * 64)  # mixed object formats

    def test_git_blob_hash(self) -> None:
        self.assertEqual(git_blob_hash(b"hello\n", 40), "ce013625030ba8dba906f756967f9e9ca394464a")
        self.assertEqual(len(git_blob_hash(b"hello\n", 64)), 64)
        verify_blob(b"hello\n", "ce013625030ba8dba906f756967f9e9ca394464a", 6)
        for content, size, code in ((b"hellO\n", 6, "blob_hash_mismatch"), (b"hello\n", 7, "blob_size_mismatch")):
            with self.subTest(code=code), self.assertRaises(BlobIntegrityError) as ctx:
                verify_blob(content, "ce013625030ba8dba906f756967f9e9ca394464a", size)
            self.assertEqual(ctx.exception.code, code)


class FileQualificationTests(unittest.TestCase):
    def test_accepted(self) -> None:
        for path in ("README.md", "docs/Café Überblick.rst", "src/pkg/mod.py", "LICENSE", "Makefile",
                     "data/table.csv", "a b/c.txt"):
            with self.subTest(path=path):
                self.assertIsNone(rejection_reason(entry(path), POLICY, commit_sha=SHA1))

    def test_rejections(self) -> None:
        cases = {
            "submodule": entry("lib", kind="commit", mode="160000", size=None),
            "symlink": entry("docs/link.md", mode="120000"),
            "mode": entry("x.md", mode="100600"),
            "unsafe_path": entry("docs/evil\nname.md"),
            "invalid_path": entry("../escape.md"),
            "excluded_dir": entry("node_modules/pkg/readme.md"),
            "secret_file": entry("config/.env"),
            "generated": entry("web/app.min.js"),
            "extension": entry("assets/logo.png"),
            "empty": entry("empty.md", size=0),
            "too_large": entry("huge.md", size=POLICY.max_file_bytes + 1),
        }
        for reason, tree_entry in cases.items():
            with self.subTest(reason=reason):
                self.assertEqual(rejection_reason(tree_entry, POLICY, commit_sha=SHA1), reason)
        for secret in ("id_rsa", "server.pem", "prod.key", ".npmrc", "credentials.json", ".env.local"):
            self.assertEqual(rejection_reason(entry(f"x/{secret}"), POLICY, commit_sha=SHA1), "secret_file", secret)
        for traversal in ("a/../b.md", "./a.md", "/abs.md", "a//b.md", "a/"):
            self.assertIsNotNone(rejection_reason(entry(traversal), POLICY, commit_sha=SHA1), traversal)

    def test_binary_masquerading_as_text(self) -> None:
        self.assertIsNone(text_rejection("plain é text\n".encode()))
        self.assertIsNone(text_rejection(b"\xef\xbb\xbfBOM text"))
        self.assertEqual(text_rejection(b"PK\x03\x04\x00\x00"), "binary")
        self.assertEqual(text_rejection(b"\xff\xfe\xfa"), "not_utf8")

    def test_tree_parsing_bounds(self) -> None:
        good = {"tree": [{"path": "a.md", "mode": "100644", "type": "blob", "sha": SHA1, "size": 3}]}
        self.assertEqual(parse_tree(good, object_length=40, max_entries=10)[0].path, "a.md")
        for tree, code in (({"tree": [], "truncated": True}, "tree_truncated"),
                           ({"tree": good["tree"] * 11}, "tree_too_large"),
                           ({"tree": [{"path": "a", "mode": "100644", "type": "blob", "sha": "zz", "size": 1}]}, "bad_tree"),
                           ({"tree": [dict(good["tree"][0], sha="a" * 64)]}, "bad_tree")):
            with self.subTest(code=code), self.assertRaises(TreeError) as ctx:
                parse_tree(tree, object_length=40, max_entries=10)
            self.assertEqual(ctx.exception.code, code)


class RepositoryQualificationTests(unittest.TestCase):
    base = {"id": 7, "node_id": "R_7", "name": "facts", "owner": {"login": "alice"}, "private": False,
            "visibility": "public", "html_url": "https://github.com/alice/facts", "fork": False, "archived": False,
            "size": 10, "stargazers_count": 60, "default_branch": "main", "license": None}

    def test_qualified_row_never_invents_metadata(self) -> None:
        row, reason = qualify_repository(self.base, RepositoryPolicy())
        self.assertEqual((reason, row.repository_id, row.owner_login, row.license_spdx_id), ("qualified", 7, "alice", None))

    def test_rejections(self) -> None:
        cases = {"not_public": [{"private": True}, {"visibility": "internal"}, {"private": None}],
                 "fork": [{"fork": True}], "archived": [{"archived": True}], "disabled": [{"disabled": True}],
                 "too_large": [{"size": 10 ** 9}], "stars": [{"stargazers_count": 3}],
                 "unsupported_default_branch": [{"default_branch": "../x"}, {"default_branch": None}],
                 "invalid_metadata": [{"id": True}, {"owner": {"login": "-bad"}}, {"html_url": "https://evil.example/"}]}
        for reason, variants in cases.items():
            for variant in variants:
                with self.subTest(reason=reason, variant=variant):
                    self.assertEqual(qualify_repository(dict(self.base, **variant), RepositoryPolicy())[1], reason)
        self.assertEqual(qualify_repository(self.base, RepositoryPolicy(licenses=("mit",)))[1], "license")
        self.assertEqual(qualify_repository(dict(self.base, language="Go"), RepositoryPolicy(languages=("rust",)))[1],
                         "language")


class EndpointTests(unittest.TestCase):
    def test_only_allow_listed_api_routes(self) -> None:
        GITHUB_API.check(GITHUB_API.search("is:public stars:>=5", 2))
        GITHUB_API.check(GITHUB_API.git("alice", "facts", "trees", SHA1, recursive=True))
        GITHUB_API.check(GITHUB_API.head("alice", "facts", "release/1.0"))
        bad = ["https://evil.example/repos/a/b/git/blobs/" + SHA1, "http://api.github.com/repositories/1",
               "https://api.github.com:444/repositories/1", "https://user@api.github.com/repositories/1",
               "https://api.github.com/repos/a/b/contents/x", "https://api.github.com/repositories/1?x=1",
               "https://api.github.com/repos/a/../b/git/blobs/" + SHA1, "https://api.github.com/user",
               "https://raw.githubusercontent.com/a/b/main/x", "https://api.github.com/repositories/1#f",
               "https://api.github.com/search/repositories?q=a&q=b"]
        for url in bad:
            with self.subTest(url=url), self.assertRaises(DisallowedEndpoint):
                GITHUB_API.check(url)
        for args in (("-a", "b", "main"), ("a", "..", "main"), ("a", "b", "../x"), ("a", "b", "x..y"), ("a", "b", "/x")):
            with self.subTest(args=args), self.assertRaises(DisallowedEndpoint):
                GITHUB_API.head(*args)
        with self.assertRaises(DisallowedEndpoint):
            GitHubEndpoints("https://evil.example")
        with self.assertRaises(DisallowedEndpoint):
            GitHubEndpoints.loopback_for_tests("http://10.1.2.3:80")


def headers(**values):
    data = {k.replace("_", "-"): v for k, v in values.items()}
    return lambda name: data.get(name.lower())


class RateLimitTests(unittest.TestCase):
    def test_classification(self) -> None:
        reset = int((datetime.now(timezone.utc) + timedelta(minutes=10)).timestamp())
        exhausted = headers(x_ratelimit_remaining="0", x_ratelimit_reset=str(reset), x_ratelimit_resource="core")
        state = parse_rate_headers(exhausted, 403, "core")
        primary = classify(403, exhausted, b"API rate limit exceeded", state=state, what="GET")
        self.assertEqual((primary.error_class, primary.code), (ErrorClass.RATE_LIMITED, "primary_rate_limit"))
        self.assertEqual(int(primary.retry_at.timestamp()), reset)
        secondary = classify(403, headers(retry_after="45", x_ratelimit_remaining="10"), b"",
                             state=parse_rate_headers(headers(x_ratelimit_remaining="10"), 403, "core"), what="GET")
        self.assertEqual((secondary.code, secondary.retry_after_seconds), ("secondary_rate_limit", 45))
        abuse = classify(403, headers(), b'{"message":"You have exceeded a secondary rate limit"}', state=None, what="GET")
        self.assertEqual((abuse.code, abuse.retry_after_seconds), ("secondary_rate_limit", 60))
        self.assertEqual(classify(429, headers(), b"", state=None, what="GET").error_class, ErrorClass.RATE_LIMITED)
        self.assertEqual(classify(403, headers(), b"blocked", state=None, what="GET").error_class,
                         ErrorClass.PERMANENT_SOURCE)
        for status, error_class in ((500, ErrorClass.TRANSIENT), (502, ErrorClass.TRANSIENT), (404, ErrorClass.PERMANENT_SOURCE),
                                    (451, ErrorClass.PERMANENT_SOURCE), (301, ErrorClass.PERMANENT_SOURCE),
                                    (401, ErrorClass.PERMANENT_SOURCE)):
            self.assertEqual(classify(status, headers(), b"", state=None, what="GET").error_class, error_class, status)

    def test_reserve_guard_stops_before_exhaustion(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        tracker = RateTracker(reserve=10, now=lambda: now)
        tracker.update(RateState("core", 5000, 11, now + timedelta(minutes=5), 200))
        tracker.check("core")
        tracker.update(RateState("core", 5000, 10, now + timedelta(minutes=5), 200))
        with self.assertRaises(GitHubFetchError) as ctx:
            tracker.check("core")
        self.assertEqual((ctx.exception.code, ctx.exception.retry_at), ("quota_reserve", now + timedelta(minutes=5)))
        tracker.check("search")  # other resources are independent


class SecretHandlingTests(unittest.TestCase):
    def test_token_is_never_rendered(self) -> None:
        token = "ghp_" + "A1b2C3d4" * 5
        settings = GitHubSettings.from_env({"KB_GH_TOKEN": token})
        self.assertNotIn(token, repr(settings))
        self.assertNotIn(token, repr(settings.credentials))
        self.assertNotIn(token, settings.credentials.fingerprint_identity)
        self.assertEqual(redact(f"failed with Authorization: Bearer {token}", token).count(token), 0)
        self.assertEqual(redact("token=abcdefghijklmnopqrstuvwxyz123456", None), "token <redacted>")
        with self.assertRaises(SettingsError) as ctx:
            GitHubSettings.from_env({"KB_GH_TOKEN": "bad token with spaces"})
        self.assertNotIn("spaces", str(ctx.exception))


class DiscoveryWindowTests(unittest.TestCase):
    def test_windows_are_deterministic_newest_first(self) -> None:
        config = DiscoveryConfig(created_from=date(2024, 1, 1), window_days=10)
        spans = windows(config, date(2024, 1, 25))
        self.assertEqual(spans, [(date(2024, 1, 21), date(2024, 1, 25)), (date(2024, 1, 11), date(2024, 1, 20)),
                                 (date(2024, 1, 1), date(2024, 1, 10))])
        self.assertEqual(windows(DiscoveryConfig(created_from=date(2024, 1, 1), window_days=10, max_windows=1),
                                 date(2024, 1, 25)), spans[:1])
        policy = RepositoryPolicy()
        qid = query_id(policy, "2026w39", *spans[0])
        self.assertEqual(qid, f"ghs1.{policy.version}.2026w39.20240121-20240125")
        self.assertNotEqual(policy.version, RepositoryPolicy(min_stars=10).version)
        self.assertEqual(DiscoveryConfig().resolved_epoch(date(2026, 9, 29)), "2026w40")


if __name__ == "__main__":
    unittest.main()
