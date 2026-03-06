"""Simple dependency verification for local Windows development."""

from __future__ import annotations

import sys


def check_import(module_name: str, package_name: str | None = None) -> bool:
    if package_name is None:
        package_name = module_name
    try:
        __import__(module_name)
        print(f"OK  {package_name}")
        return True
    except ImportError as exc:
        print(f"ERR {package_name}: {exc}")
        return False


def main() -> int:
    print("Checking installed dependencies...\n")
    all_ok = True
    all_ok &= check_import("telethon")
    all_ok &= check_import("aiohttp")
    all_ok &= check_import("requests")
    all_ok &= check_import("socks", "python-socks")
    all_ok &= check_import("dashscope")

    print("\n" + "=" * 40)
    if all_ok:
        print("All core dependencies are available.")
        return 0
    print("Some dependencies are missing.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
