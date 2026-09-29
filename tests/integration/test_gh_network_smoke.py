"""Opt-in REAL-NETWORK GitHub smoke test. Disabled by default; never run in CI.

    KB_RUN_GH_NETWORK_SMOKE=1
    python -m unittest tests.integration.test_gh_network_smoke -v

Footprint (no database; Bronze in a temporary directory; works unauthenticated,
KB_GH_TOKEN is optional and never printed):
* 1 request  GET /repositories/{id}          (default 1296269 = octocat/Hello-World;
                                              override KB_GH_SMOKE_REPOSITORY_ID)
* 1 request  GET /repos/{o}/{r}/commits/{default branch}  (HEAD sha)
* 1 request  GET git/commits/{sha}
* 1 request  GET git/trees/{tree}?recursive=1
* <= KB_GH_SMOKE_MAX_FILES (default 3, max 5) GET git/blobs/{sha}
Total <= 9 requests (the unauthenticated limit is 60/hour).
"""

import os
import tempfile
import unittest
from pathlib import Path

RUN = os.environ.get("KB_RUN_GH_NETWORK_SMOKE") == "1"


@unittest.skipUnless(RUN, "set KB_RUN_GH_NETWORK_SMOKE=1 to make real GitHub API requests")
class GitHubNetworkSmokeTests(unittest.TestCase):
    def test_one_small_public_repository(self) -> None:
        from app.sources.github.acquisition import repository_row
        from app.sources.github.bronze import GitHubBronze
        from app.sources.github.client import ACCEPT_RAW, ACCEPT_SHA
        from app.sources.github.config import GitHubSettings
        from app.sources.github.connector import build_client
        from app.sources.github.files import parse_tree, rejection_reason, text_rejection, verify_blob
        from app.utils.ids import github_observation_id

        repository_id = int(os.environ.get("KB_GH_SMOKE_REPOSITORY_ID", "1296269"))
        max_files = min(int(os.environ.get("KB_GH_SMOKE_MAX_FILES", "3")), 5)
        settings = GitHubSettings.from_env()
        client = build_client(settings)
        try:
            meta, _etag = client.get_json(client.endpoints.repository(repository_id), max_bytes=4 * 1024 * 1024)
            repo = repository_row(meta, repository_id)  # must be public
            head = client.get(client.endpoints.head(repo.owner_login, repo.repo_name, repo.default_branch),
                              accept=ACCEPT_SHA, max_bytes=256).body.decode("ascii").strip()
            commit, _ = client.get_json(client.endpoints.git(repo.owner_login, repo.repo_name, "commits", head),
                                        max_bytes=4 * 1024 * 1024)
            self.assertEqual(commit["sha"], head)
            tree, _ = client.get_json(client.endpoints.git(repo.owner_login, repo.repo_name, "trees",
                                                           commit["tree"]["sha"], recursive=True),
                                      max_bytes=64 * 1024 * 1024)
            entries = parse_tree(tree, object_length=len(head), max_entries=100_000)
            policy = settings.acquisition.files
            candidates = [e for e in entries if rejection_reason(e, policy, commit_sha=head) is None][:max_files]
            with tempfile.TemporaryDirectory() as tmp:
                bronze = GitHubBronze(Path(tmp))
                for entry in candidates:
                    content = client.get(client.endpoints.git(repo.owner_login, repo.repo_name, "blobs", entry.sha),
                                         accept=ACCEPT_RAW, max_bytes=policy.max_file_bytes).body
                    verify_blob(content, entry.sha, entry.size)
                    if text_rejection(content) is None:
                        bronze.store_blob(entry.sha, content)
                        self.assertEqual(bronze.read_blob(entry.sha), content)
                    oid = github_observation_id(repository_id, head, entry.path, entry.sha)
                    print(f"{oid} {repo.owner_login}/{repo.repo_name}@{head[:12]} {entry.path} {len(content)} bytes")
            print("rate:", [(s.resource, s.remaining) for s in client.rates.states()],
                  "authenticated:", client.authenticated)
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
