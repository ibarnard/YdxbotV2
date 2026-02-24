"""
更新与发布管理模块

功能：
- 查询当前版本与最新 GitHub Release
- 执行发布版本更新（按 tag）
- 失败自动回滚
- 进程重启
- 周期性发布检查通知
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import base64
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.parse import urlparse

import requests


DEFAULT_RELEASE_CHECK_INTERVAL = 30 * 60  # 30 分钟
RELEASE_STATE_FILE = ".release_state.json"
ROLLBACK_FILE = ".release_rollback.json"
UPDATE_LOCK_FILE = ".update.lock"
GITHUB_TOKEN_ENV_KEYS = ("YDXBOT_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN")
SHARED_GLOBAL_CANDIDATES = ("global.local.json", "global.json", "global.example.json")


def _repo_root(repo_root: Optional[str] = None) -> Path:
    return Path(repo_root or Path(__file__).resolve().parent).resolve()


def _run_cmd(args: List[str], cwd: Path, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _parse_repo_slug(remote_url: str) -> Optional[str]:
    if not remote_url:
        return None

    raw = remote_url.strip()

    # SCP-like SSH URL, e.g. git@github.com:owner/repo.git
    ssh_match = re.match(
        r"(?i)git@github\.com:(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$",
        raw,
    )
    if ssh_match:
        return f"{ssh_match.group('owner')}/{ssh_match.group('repo')}"

    # URL form, e.g.
    # - https://github.com/owner/repo(.git)
    # - https://token@github.com/owner/repo(.git)
    # - ssh://git@github.com/owner/repo.git
    # - git://github.com/owner/repo.git
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if host == "github.com" and parsed.path:
        path = parsed.path.strip("/")
        if path.endswith(".git"):
            path = path[:-4]
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2:
            owner, repo = parts[0], parts[1]
            if owner and repo:
                return f"{owner}/{repo}"

    return None


def _load_json_with_comments(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw_text = path.read_text(encoding="utf-8")
    except Exception:
        return {}

    cleaned_lines: List[str] = []
    for raw_line in raw_text.splitlines():
        stripped = raw_line.lstrip()
        if stripped.startswith("#") or stripped.startswith("//"):
            continue

        in_string = False
        escaped = False
        cleaned: List[str] = []
        i = 0
        while i < len(raw_line):
            ch = raw_line[i]
            if escaped:
                cleaned.append(ch)
                escaped = False
                i += 1
                continue
            if ch == "\\":
                cleaned.append(ch)
                escaped = True
                i += 1
                continue
            if ch == '"':
                in_string = not in_string
                cleaned.append(ch)
                i += 1
                continue
            if not in_string:
                if ch == "#":
                    break
                if ch == "/" and i + 1 < len(raw_line) and raw_line[i + 1] == "/":
                    break
            cleaned.append(ch)
            i += 1

        line = "".join(cleaned).rstrip()
        if line:
            cleaned_lines.append(line)

    if not cleaned_lines:
        return {}

    try:
        return json.loads("\n".join(cleaned_lines))
    except Exception:
        return {}


def _load_shared_global_config(repo_root: Path) -> Dict[str, Any]:
    shared_dir = repo_root / "shared"
    for filename in SHARED_GLOBAL_CANDIDATES:
        path = shared_dir / filename
        if not path.exists():
            continue
        return _load_json_with_comments(path)
    return {}


def _looks_like_github_token(value: str) -> bool:
    token = (value or "").strip()
    if not token:
        return False
    prefixes = ("ghp_", "github_pat_", "gho_", "ghu_", "ghs_", "ghr_")
    return token.startswith(prefixes)


def _extract_github_token_from_remote(remote_url: str) -> str:
    raw = (remote_url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if host != "github.com":
        return ""
    if parsed.password:
        return parsed.password
    if parsed.username and _looks_like_github_token(parsed.username):
        return parsed.username
    return ""


def resolve_github_token(repo_root: Optional[str] = None, remote_url: str = "") -> str:
    for env_key in GITHUB_TOKEN_ENV_KEYS:
        token = (os.getenv(env_key) or "").strip()
        if token:
            return token

    root = _repo_root(repo_root)
    shared_cfg = _load_shared_global_config(root)
    update_cfg = shared_cfg.get("update", {}) if isinstance(shared_cfg.get("update", {}), dict) else {}

    cfg_candidates = [
        update_cfg.get("github_token"),
        update_cfg.get("token"),
        (update_cfg.get("github") or {}).get("token") if isinstance(update_cfg.get("github"), dict) else "",
        (shared_cfg.get("github") or {}).get("token") if isinstance(shared_cfg.get("github"), dict) else "",
    ]
    for item in cfg_candidates:
        token = (item or "").strip()
        if token:
            return token

    return _extract_github_token_from_remote(remote_url)


def _build_git_auth_header(token: str) -> str:
    payload = f"x-access-token:{token}".encode("utf-8")
    encoded = base64.b64encode(payload).decode("ascii")
    return f"AUTHORIZATION: basic {encoded}"


def _git_fetch_tags(root: Path, remote_name: str, github_token: str = "") -> subprocess.CompletedProcess:
    if not remote_name:
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
    cmd = ["git"]
    token = (github_token or "").strip()
    if token:
        cmd += ["-c", f"http.extraheader={_build_git_auth_header(token)}"]
    cmd += ["fetch", "--tags", remote_name]
    return _run_cmd(cmd, root, timeout=120)


def detect_repo_slug(repo_root: Optional[str] = None) -> Optional[str]:
    remote = detect_repo_remote(repo_root)
    return remote.get("slug") or None


def detect_repo_remote(repo_root: Optional[str] = None) -> Dict[str, str]:
    root = _repo_root(repo_root)
    result = _run_cmd(["git", "config", "--get", "remote.origin.url"], root, timeout=10)
    if result.returncode == 0 and result.stdout.strip():
        url = result.stdout.strip()
        return {"name": "origin", "url": url, "slug": _parse_repo_slug(url) or ""}

    remotes_res = _run_cmd(["git", "remote", "-v"], root, timeout=10)
    if remotes_res.returncode != 0:
        return {}

    fetch_remotes: List[Dict[str, str]] = []
    for line in remotes_res.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        name, url, kind = parts[0], parts[1], parts[2]
        if kind != "(fetch)":
            continue
        fetch_remotes.append({"name": name, "url": url, "slug": _parse_repo_slug(url) or ""})

    if not fetch_remotes:
        return {}

    for item in fetch_remotes:
        if item.get("slug"):
            return item
    return fetch_remotes[0]


def get_current_repo_info(repo_root: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)

    commit_res = _run_cmd(["git", "rev-parse", "HEAD"], root)
    commit = commit_res.stdout.strip() if commit_res.returncode == 0 else ""
    short_commit = commit[:8] if commit else ""

    branch_res = _run_cmd(["git", "branch", "--show-current"], root)
    branch = branch_res.stdout.strip() if branch_res.returncode == 0 else ""

    tag_exact_res = _run_cmd(["git", "describe", "--tags", "--exact-match"], root)
    current_tag = tag_exact_res.stdout.strip() if tag_exact_res.returncode == 0 else ""

    nearest_tag_res = _run_cmd(["git", "describe", "--tags", "--abbrev=0"], root)
    nearest_tag = nearest_tag_res.stdout.strip() if nearest_tag_res.returncode == 0 else ""

    display_version = current_tag or (f"{branch}@{short_commit}" if branch else short_commit or "unknown")
    return {
        "commit": commit,
        "short_commit": short_commit,
        "branch": branch,
        "current_tag": current_tag,
        "nearest_tag": nearest_tag,
        "display_version": display_version,
    }


def _list_version_tags(root: Path, limit: int = 0) -> List[str]:
    tags_res = _run_cmd(["git", "tag", "--list", "v*", "--sort=-version:refname"], root, timeout=20)
    if tags_res.returncode != 0:
        return []
    tags = [line.strip() for line in tags_res.stdout.splitlines() if line.strip()]
    if limit and limit > 0:
        return tags[:limit]
    return tags


def _get_tag_date(root: Path, tag: str) -> str:
    date_res = _run_cmd(["git", "log", "-1", "--format=%cs", tag], root, timeout=10)
    if date_res.returncode != 0:
        return ""
    return date_res.stdout.strip()


def _get_tag_summary(root: Path, tag: str) -> str:
    summary_res = _run_cmd(["git", "for-each-ref", f"refs/tags/{tag}", "--format=%(subject)"], root, timeout=10)
    summary = summary_res.stdout.strip() if summary_res.returncode == 0 else ""
    if summary:
        return summary
    commit_subject_res = _run_cmd(["git", "log", "-1", "--format=%s", tag], root, timeout=10)
    if commit_subject_res.returncode != 0:
        return ""
    return commit_subject_res.stdout.strip()


def list_version_catalog(repo_root: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
    """
    版本目录（面向公开仓库）：
    - 历史版本（tag）及摘要
    - 当前版本
    - 待更新版本（相对当前 tag）
    """
    root = _repo_root(repo_root)
    current = get_current_repo_info(root)
    remote = detect_repo_remote(root)
    remote_name = remote.get("name", "")
    github_token = resolve_github_token(root, remote.get("url", ""))

    fetch_warning = ""
    if remote_name:
        fetch_res = _git_fetch_tags(root, remote_name, github_token)
        if fetch_res.returncode != 0:
            fetch_warning = (fetch_res.stderr or fetch_res.stdout).strip()[:200]

    all_tags = _list_version_tags(root)
    if not all_tags:
        return {
            "success": True,
            "current": current,
            "latest_tag": "",
            "current_tag": current.get("current_tag", ""),
            "pending_tags": [],
            "entries": [],
            "fetch_warning": fetch_warning,
        }

    max_entries = max(1, int(limit)) if isinstance(limit, int) else 20
    display_tags = all_tags[:max_entries]

    entries: List[Dict[str, str]] = []
    for tag in display_tags:
        entries.append(
            {
                "tag": tag,
                "date": _get_tag_date(root, tag),
                "summary": _get_tag_summary(root, tag),
            }
        )

    current_tag = current.get("current_tag", "") or current.get("nearest_tag", "")
    pending_tags: List[str] = []
    if current_tag and current_tag in all_tags:
        pending_tags = all_tags[: all_tags.index(current_tag)]
    elif not current_tag:
        pending_tags = list(all_tags)
    else:
        pending_tags = list(all_tags)

    return {
        "success": True,
        "current": current,
        "latest_tag": all_tags[0],
        "current_tag": current.get("current_tag", ""),
        "pending_tags": pending_tags,
        "entries": entries,
        "fetch_warning": fetch_warning,
    }


def update_to_version(repo_root: Optional[str] = None, target: Optional[str] = None) -> Dict[str, Any]:
    """
    更新到指定版本（tag/commit/branch），若未指定则更新到最新 tag。
    公开仓库优先，不依赖 release API。
    """
    target_ref = (target or "").strip()
    if target_ref:
        return update_to_ref(repo_root, target_ref)

    catalog = list_version_catalog(repo_root, limit=1)
    if not catalog.get("success"):
        return {"success": False, "error": catalog.get("error", "获取版本列表失败")}
    latest_tag = catalog.get("latest_tag", "")
    if not latest_tag:
        return {"success": False, "error": "未找到可更新的版本标签"}

    result = update_to_ref(repo_root, latest_tag)
    if result.get("success"):
        result["resolved_target"] = latest_tag
    return result


def reback_to_version(repo_root: Optional[str] = None, target: Optional[str] = None) -> Dict[str, Any]:
    """
    回退到指定版本（tag/commit/branch）。
    """
    target_ref = (target or "").strip()
    if not target_ref:
        return {"success": False, "error": "请提供目标版本号或提交"}
    result = update_to_ref(repo_root, target_ref)
    if result.get("success"):
        result["resolved_target"] = target_ref
    return result


def get_latest_release(repo_slug: str, timeout: int = 10, github_token: str = "") -> Dict[str, Any]:
    url = f"https://api.github.com/repos/{repo_slug}/releases/latest"
    headers = {"Accept": "application/vnd.github+json"}
    token = (github_token or "").strip()
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        return {
            "success": False,
            "error": f"请求 GitHub 失败: {str(e)}",
            "url": url,
        }

    if response.status_code != 200:
        if response.status_code in {401, 403, 404}:
            hint = "（私有仓库请配置 GitHub Token：环境变量 YDXBOT_GITHUB_TOKEN/GITHUB_TOKEN，或 shared/global.json/shared/global.local.json -> update.github_token）"
        else:
            hint = ""
        return {
            "success": False,
            "error": f"GitHub API 返回 {response.status_code}{hint}",
            "url": url,
        }

    data = response.json()
    return {
        "success": True,
        "tag_name": data.get("tag_name", ""),
        "name": data.get("name", ""),
        "html_url": data.get("html_url", ""),
        "published_at": data.get("published_at", ""),
        "body": data.get("body", ""),
    }


def _load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def get_release_state(repo_root: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)
    return _load_json(root / RELEASE_STATE_FILE, default={})


def mark_release_notified(tag_name: str, repo_root: Optional[str] = None) -> None:
    root = _repo_root(repo_root)
    state = get_release_state(root)
    state["last_notified_tag"] = tag_name
    state["updated_at"] = int(time.time())
    _save_json(root / RELEASE_STATE_FILE, state)


def mark_release_applied(tag_name: str, repo_root: Optional[str] = None) -> None:
    root = _repo_root(repo_root)
    state = get_release_state(root)
    state["last_applied_tag"] = tag_name
    state["updated_at"] = int(time.time())
    _save_json(root / RELEASE_STATE_FILE, state)


def check_release_update(repo_root: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)
    info = get_current_repo_info(root)
    remote = detect_repo_remote(root)
    repo_slug = remote.get("slug", "")
    if not repo_slug:
        return {
            "success": False,
            "error": "无法识别 GitHub 仓库地址(remote.origin.url)",
            "current": info,
        }

    github_token = resolve_github_token(root, remote.get("url", ""))
    latest = get_latest_release(repo_slug, github_token=github_token)
    if not latest.get("success"):
        return {
            "success": False,
            "error": latest.get("error", "获取最新 release 失败"),
            "current": info,
            "repo_slug": repo_slug,
        }

    latest_tag = latest.get("tag_name", "")
    current_tag = info.get("current_tag", "")
    has_update = bool(latest_tag and latest_tag != current_tag)

    return {
        "success": True,
        "repo_slug": repo_slug,
        "current": info,
        "latest": latest,
        "has_update": has_update,
    }


def _is_runtime_file(path: str) -> bool:
    if not path:
        return True
    normalized = path.replace("\\", "/")
    filename = Path(normalized).name
    if filename == ".DS_Store":
        return True
    if normalized in {"state.json", "account_funds.json", "MULTIUSER_TEST_RESULTS.json"}:
        return True
    if normalized.endswith(".log"):
        return True
    if ".log." in filename:
        return True
    if normalized.endswith(".session") or normalized.endswith(".session-journal"):
        return True
    if normalized in {"global.json", "shared/global.json", "shared/global.local.json"}:
        return True
    if normalized.startswith("tests_multiuser/users/"):
        return True
    if normalized.startswith("user/"):
        return True
    if normalized.startswith("users/") and not normalized.startswith("users/_template/"):
        return True
    if normalized in {RELEASE_STATE_FILE, ROLLBACK_FILE, UPDATE_LOCK_FILE}:
        return True
    return False


def _parse_status_path(line: str) -> str:
    if len(line) < 4:
        return ""
    path_part = line[3:].strip()
    if " -> " in path_part:
        return path_part.split(" -> ", 1)[1].strip()
    return path_part


def get_blocking_dirty_paths(repo_root: Optional[str] = None) -> List[str]:
    root = _repo_root(repo_root)
    result = _run_cmd(["git", "status", "--porcelain"], root)
    if result.returncode != 0:
        return ["<git status 执行失败>"]

    blocking: List[str] = []
    for line in result.stdout.splitlines():
        path = _parse_status_path(line)
        if path and not _is_runtime_file(path):
            blocking.append(path)
    return blocking


def _acquire_update_lock(repo_root: Path) -> Dict[str, Any]:
    lock_path = repo_root / UPDATE_LOCK_FILE
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return {"success": False, "error": "已有更新任务在执行，请稍后重试", "lock_path": str(lock_path)}

    try:
        payload = {"pid": os.getpid(), "timestamp": int(time.time())}
        os.write(fd, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    finally:
        os.close(fd)
    return {"success": True, "lock_path": str(lock_path)}


def _release_update_lock(repo_root: Path) -> None:
    lock_path = repo_root / UPDATE_LOCK_FILE
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _save_rollback_point(repo_root: Path, info: Dict[str, Any], target_tag: str) -> None:
    payload = {
        "commit": info.get("commit", ""),
        "branch": info.get("branch", ""),
        "current_tag": info.get("current_tag", ""),
        "target_tag": target_tag,
        "timestamp": int(time.time()),
    }
    _save_json(repo_root / ROLLBACK_FILE, payload)


def run_health_check(repo_root: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)
    commands = [
        [sys.executable, "verify_deps.py"],
        [
            sys.executable,
            "-m",
            "py_compile",
            "main.py",
            "main_multiuser.py",
            "zq.py",
            "zq_multiuser.py",
            "user_manager.py",
        ],
    ]

    for cmd in commands:
        result = _run_cmd(cmd, root, timeout=120)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            return {
                "success": False,
                "error": f"健康检查失败: {' '.join(cmd)}",
                "detail": detail[:600],
            }
    return {"success": True}


def _rollback_to_last_release_unlocked(root: Path) -> Dict[str, Any]:
    rollback = _load_json(root / ROLLBACK_FILE, default={})
    commit = rollback.get("commit", "")
    if not commit:
        return {"success": False, "error": "未找到可回滚版本，请先执行一次 upnow"}

    checkout_res = _run_cmd(["git", "checkout", commit], root, timeout=60)
    if checkout_res.returncode != 0:
        return {
            "success": False,
            "error": "回滚 checkout 失败",
            "detail": (checkout_res.stderr or checkout_res.stdout).strip()[:600],
        }

    health = run_health_check(root)
    if not health.get("success"):
        return {"success": False, "error": health.get("error", "回滚后健康检查失败"), "detail": health.get("detail", "")}

    current = get_current_repo_info(root)
    return {"success": True, "current": current, "rollback": rollback}


def rollback_to_last_release(repo_root: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)
    lock = _acquire_update_lock(root)
    if not lock.get("success"):
        return lock

    try:
        blocking = get_blocking_dirty_paths(root)
        if blocking:
            return {
                "success": False,
                "error": "存在未提交代码变更，已阻止回滚",
                "blocking_paths": blocking,
            }
        return _rollback_to_last_release_unlocked(root)
    finally:
        _release_update_lock(root)


def update_to_release(repo_root: Optional[str] = None, target_tag: Optional[str] = None) -> Dict[str, Any]:
    root = _repo_root(repo_root)
    lock = _acquire_update_lock(root)
    if not lock.get("success"):
        return lock

    try:
        blocking = get_blocking_dirty_paths(root)
        if blocking:
            return {
                "success": False,
                "error": "存在未提交代码变更，已阻止更新",
                "blocking_paths": blocking,
            }

        current = get_current_repo_info(root)
        remote = detect_repo_remote(root)
        remote_name = remote.get("name", "")
        repo_slug = remote.get("slug", "")
        github_token = resolve_github_token(root, remote.get("url", ""))

        final_tag = (target_tag or "").strip()
        latest = None
        if not final_tag:
            if not repo_slug:
                return {
                    "success": False,
                    "error": "无法识别 GitHub 仓库地址(remote.origin.url)",
                    "detail": "请检查 git 远程配置，例如：git remote add origin https://github.com/<owner>/<repo>.git",
                }
            if remote_name:
                fetch_res = _git_fetch_tags(root, remote_name, github_token)
                if fetch_res.returncode != 0:
                    return {
                        "success": False,
                        "error": f"git fetch --tags {remote_name} 失败",
                        "detail": (fetch_res.stderr or fetch_res.stdout).strip()[:600],
                    }
            latest = get_latest_release(repo_slug, github_token=github_token)
            if not latest.get("success"):
                return {"success": False, "error": latest.get("error", "获取最新 release 失败")}
            final_tag = latest.get("tag_name", "").strip()
        elif remote_name:
            fetch_res = _git_fetch_tags(root, remote_name, github_token)
            if fetch_res.returncode != 0:
                return {
                    "success": False,
                    "error": f"git fetch --tags {remote_name} 失败",
                    "detail": (fetch_res.stderr or fetch_res.stdout).strip()[:600],
                }

        if not final_tag:
            return {"success": False, "error": "未找到可用的 release tag"}

        if current.get("current_tag") == final_tag:
            return {
                "success": True,
                "no_change": True,
                "current": current,
                "target_tag": final_tag,
                "message": "当前已是最新发布版本",
            }

        _save_rollback_point(root, current, final_tag)

        checkout_res = _run_cmd(["git", "checkout", final_tag], root, timeout=60)
        if checkout_res.returncode != 0:
            return {
                "success": False,
                "error": f"切换到发布版本 {final_tag} 失败",
                "detail": (checkout_res.stderr or checkout_res.stdout).strip()[:600],
            }

        health = run_health_check(root)
        if not health.get("success"):
            rollback_result = _rollback_to_last_release_unlocked(root)
            return {
                "success": False,
                "error": health.get("error", "更新后健康检查失败"),
                "detail": health.get("detail", ""),
                "rollback": rollback_result,
            }

        mark_release_applied(final_tag, root)
        mark_release_notified(final_tag, root)
        after = get_current_repo_info(root)
        return {
            "success": True,
            "current": current,
            "after": after,
            "target_tag": final_tag,
            "latest": latest or {},
        }
    finally:
        _release_update_lock(root)


def update_to_ref(repo_root: Optional[str] = None, target_ref: Optional[str] = None) -> Dict[str, Any]:
    """更新到任意 git 引用（commit/tag/branch）。"""
    root = _repo_root(repo_root)
    lock = _acquire_update_lock(root)
    if not lock.get("success"):
        return lock

    try:
        blocking = get_blocking_dirty_paths(root)
        if blocking:
            return {
                "success": False,
                "error": "存在未提交代码变更，已阻止更新",
                "blocking_paths": blocking,
            }

        final_ref = (target_ref or "").strip()
        if not final_ref:
            return {"success": False, "error": "请提供目标 ref（commit/tag/branch）"}

        current = get_current_repo_info(root)
        remote = detect_repo_remote(root)
        remote_name = remote.get("name", "")
        github_token = resolve_github_token(root, remote.get("url", ""))

        if remote_name:
            fetch_main_res = _run_cmd(["git", "fetch", remote_name], root, timeout=120)
            fetch_tag_res = _git_fetch_tags(root, remote_name, github_token)
            if fetch_main_res.returncode != 0 and fetch_tag_res.returncode != 0:
                verify_local = _run_cmd(["git", "rev-parse", "--verify", f"{final_ref}^{{commit}}"], root, timeout=20)
                if verify_local.returncode != 0:
                    return {
                        "success": False,
                        "error": f"git fetch --tags {remote_name} 失败，且本地不存在目标 ref",
                        "detail": ((fetch_main_res.stderr or fetch_main_res.stdout or fetch_tag_res.stderr or fetch_tag_res.stdout).strip())[:600],
                    }

        resolve_res = _run_cmd(["git", "rev-parse", "--verify", f"{final_ref}^{{commit}}"], root, timeout=20)
        if resolve_res.returncode != 0:
            return {
                "success": False,
                "error": f"目标 ref 不存在: {final_ref}",
                "detail": (resolve_res.stderr or resolve_res.stdout).strip()[:600],
            }
        target_commit = resolve_res.stdout.strip()

        if current.get("commit") == target_commit:
            return {
                "success": True,
                "no_change": True,
                "current": current,
                "target_ref": final_ref,
                "target_commit": target_commit,
                "message": "当前已是目标版本",
            }

        _save_rollback_point(root, current, final_ref)

        checkout_res = _run_cmd(["git", "checkout", final_ref], root, timeout=60)
        if checkout_res.returncode != 0:
            return {
                "success": False,
                "error": f"切换到目标 ref 失败: {final_ref}",
                "detail": (checkout_res.stderr or checkout_res.stdout).strip()[:600],
            }

        health = run_health_check(root)
        if not health.get("success"):
            rollback_result = _rollback_to_last_release_unlocked(root)
            return {
                "success": False,
                "error": health.get("error", "更新后健康检查失败"),
                "detail": health.get("detail", ""),
                "rollback": rollback_result,
            }

        after = get_current_repo_info(root)
        if after.get("current_tag"):
            mark_release_applied(after.get("current_tag"), root)
            mark_release_notified(after.get("current_tag"), root)

        return {
            "success": True,
            "current": current,
            "after": after,
            "target_ref": final_ref,
            "target_commit": target_commit,
        }
    finally:
        _release_update_lock(root)


async def restart_process(delay_seconds: float = 2.0) -> None:
    await asyncio.sleep(delay_seconds)
    os.execv(sys.executable, [sys.executable] + sys.argv)


def build_release_update_message(check_result: Dict[str, Any]) -> str:
    current = check_result.get("current", {})
    latest = check_result.get("latest", {})
    return (
        "🆕 检测到新发布版本\n"
        f"当前版本：{current.get('display_version', 'unknown')}\n"
        f"最新版本：{latest.get('tag_name', 'unknown')}\n"
        f"发布时间：{latest.get('published_at', 'unknown')}\n"
        f"发布链接：{latest.get('html_url', '')}\n"
        "可用命令：`ver` `update` `reback` `restart`"
    )


async def periodic_release_check_loop(
    notify_callback: Callable[[str], Awaitable[None]],
    repo_root: Optional[str] = None,
    interval_seconds: int = DEFAULT_RELEASE_CHECK_INTERVAL,
) -> None:
    root = _repo_root(repo_root)
    while True:
        try:
            result = await asyncio.to_thread(check_release_update, root)
            if result.get("success") and result.get("has_update"):
                latest_tag = result.get("latest", {}).get("tag_name", "")
                state = get_release_state(root)
                if latest_tag and state.get("last_notified_tag") != latest_tag:
                    await notify_callback(build_release_update_message(result))
                    mark_release_notified(latest_tag, root)
        except Exception:
            # 周期任务不抛出异常，避免影响主流程
            pass
        await asyncio.sleep(interval_seconds)
