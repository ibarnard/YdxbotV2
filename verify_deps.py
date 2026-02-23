"""验证项目依赖安装状态。"""

import sys


def check_import(module_name, package_name=None):
    """检查模块是否可以导入。"""
    if package_name is None:
        package_name = module_name
    try:
        __import__(module_name)
        print(f"✅ {package_name}")
        return True
    except ImportError as e:
        print(f"❌ {package_name}: {e}")
        return False


def main():
    """执行依赖检查并返回进程退出码。"""
    print("正在验证依赖安装...\n")
    all_ok = True
    all_ok &= check_import("telethon")
    all_ok &= check_import("aiohttp")
    all_ok &= check_import("requests")
    all_ok &= check_import("socks", "python-socks")
    all_ok &= check_import("dashscope")

    print("\n" + "=" * 40)
    if all_ok:
        print("✅ 所有核心依赖验证通过！")
        return 0
    print("❌ 部分依赖未正确安装")
    return 1


if __name__ == "__main__":
    # 修复：模块导入即退出的问题，原因：旧实现在 import 时直接执行 sys.exit。
    sys.exit(main())
