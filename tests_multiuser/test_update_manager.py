from __future__ import annotations

import subprocess

import update_manager as um


def test_parse_repo_slug_supports_common_remote_url_forms():
    cases = {
        "git@github.com:ibarnard/YdxbotV2.git": "ibarnard/YdxbotV2",
        "git@github.com:ibarnard/YdxbotV2": "ibarnard/YdxbotV2",
        "https://github.com/ibarnard/YdxbotV2": "ibarnard/YdxbotV2",
        "https://github.com/ibarnard/YdxbotV2.git": "ibarnard/YdxbotV2",
        "https://token@github.com/ibarnard/YdxbotV2.git": "ibarnard/YdxbotV2",
        "https://github.com/ibarnard/YdxbotV2/": "ibarnard/YdxbotV2",
        "ssh://git@github.com/ibarnard/YdxbotV2.git": "ibarnard/YdxbotV2",
        "git://github.com/ibarnard/YdxbotV2.git": "ibarnard/YdxbotV2",
    }
    for remote_url, expected in cases.items():
        assert um._parse_repo_slug(remote_url) == expected


def test_parse_repo_slug_rejects_non_github_remote():
    assert um._parse_repo_slug("git@gitlab.com:ibarnard/YdxbotV2.git") is None


def test_extract_github_token_from_remote_url():
    url = "https://ibarnard:token_example_1234567890@github.com/ibarnard/YdxbotV2.git"
    assert um._extract_github_token_from_remote(url) == "token_example_1234567890"


def test_resolve_github_token_prefers_env(monkeypatch, tmp_path):
    monkeypatch.setenv("YDXBOT_GITHUB_TOKEN", "token_env_value")
    token = um.resolve_github_token(str(tmp_path), "")
    assert token == "token_env_value"


def test_resolve_github_token_from_shared_global(monkeypatch, tmp_path):
    monkeypatch.delenv("YDXBOT_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)
    (shared_dir / "global.json").write_text(
        """{
  # release auth for private repo
  "update": {
    "github_token": "token_cfg_value"
  }
}
""",
        encoding="utf-8",
    )
    token = um.resolve_github_token(str(tmp_path), "")
    assert token == "token_cfg_value"


def test_get_latest_release_gives_private_repo_hint_on_auth_errors(monkeypatch):
    class DummyResp:
        status_code = 404

        def json(self):
            return {}

    monkeypatch.setattr(um.requests, "get", lambda *args, **kwargs: DummyResp())
    result = um.get_latest_release("ibarnard/YdxbotV2")
    assert result["success"] is False
    assert "私有仓库请配置 GitHub Token" in result["error"]


def test_get_blocking_dirty_paths_ignores_runtime_artifacts(monkeypatch, tmp_path):
    status_stdout = "\n".join(
        [
            "?? .DS_Store",
            "?? tests_multiuser/users/tim/config.json",
            "?? users/shuji/session.session",
            "?? user/legacy_user/config.py",
            " M shared/global.json",
            "?? global.json",
            "?? zq_multiuser.py",
        ]
    )

    def fake_run_cmd(args, cwd, timeout=30):
        if args[:3] == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout=status_stdout, stderr="")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(um, "_run_cmd", fake_run_cmd)
    blocking = um.get_blocking_dirty_paths(str(tmp_path))
    assert blocking == ["zq_multiuser.py"]
