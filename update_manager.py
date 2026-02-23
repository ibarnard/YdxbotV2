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
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import requests


DEFAULT_RELEASE_CHECK_INTERVAL = 30 * 60  # 30 分钟
RELEASE_STATE_FILE = ".release_state.json"
ROLLBACK_FILE = ".release_rollback.json"
UPDATE_LOCK_FILE = ".update.lock"


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

    ssh_match = re.match(r"git@github\.com:(?P<owner>[^/]+)/(?P<repo>.+?)(?:\.git)?$", remote_url)
    if ssh_match:
        return f"{ssh_match.group('owner')}/{ssh_match.group('repo')}"

    https_match = re.match(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>.+?)(?:\.git)?$", remote_url)
    if https_match:
        return f"{https_match.group('owner')}/{https_match.group('repo')}"

    return None


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


def get_latest_release(repo_slug: str, timeout: int = 10) -> Dict[str, Any]:
    url = f"https://api.github.com/repos/{repo_slug}/releases/latest"
    headers = {"Accept": "application/vnd.github+json"}

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        return {
            "success": False,
            "error": f"请求 GitHub 失败: {str(e)}",
            "url": url,
        }

    if response.status_code != 200:
        return {
            "success": False,
            "error": f"GitHub API 返回 {response.status_code}",
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
    repo_slug = detect_repo_slug(root)
    if not repo_slug:
        return {
            "success": False,
            "error": "无法识别 GitHub 仓库地址(remote.origin.url)",
            "current": info,
        }

    latest = get_latest_release(repo_slug)
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
    if normalized in {"state.json", "account_funds.json", "MULTIUSER_TEST_RESULTS.json"}:
        return True
    if normalized.endswith(".log"):
        return True
    if ".log." in filename:
        return True
    if normalized.endswith(".session") or normalized.endswith(".session-journal"):
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
                fetch_res = _run_cmd(["git", "fetch", "--tags", remote_name], root, timeout=120)
                if fetch_res.returncode != 0:
                    return {
                        "success": False,
                        "error": f"git fetch --tags {remote_name} 失败",
                        "detail": (fetch_res.stderr or fetch_res.stdout).strip()[:600],
                    }
            latest = get_latest_release(repo_slug)
            if not latest.get("success"):
                return {"success": False, "error": latest.get("error", "获取最新 release 失败")}
            final_tag = latest.get("tag_name", "").strip()
        elif remote_name:
            fetch_res = _run_cmd(["git", "fetch", "--tags", remote_name], root, timeout=120)
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

        if remote_name:
            fetch_res = _run_cmd(["git", "fetch", "--tags", remote_name], root, timeout=120)
            if fetch_res.returncode != 0:
                verify_local = _run_cmd(["git", "rev-parse", "--verify", f"{final_ref}^{{commit}}"], root, timeout=20)
                if verify_local.returncode != 0:
                    return {
                        "success": False,
                        "error": f"git fetch --tags {remote_name} 失败，且本地不存在目标 ref",
                        "detail": (fetch_res.stderr or fetch_res.stdout).strip()[:600],
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
        "可用命令：`upcheck` `upnow` `upref` `uprollback` `restart`"
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
