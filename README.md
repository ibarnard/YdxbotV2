# YdxbotV2 (Multiuser)

Ydxbot 的多账号版本（从主仓库剥离），仅保留多用户运行所需代码。

## 目录
- `main_multiuser.py`: 多用户主入口
- `zq_multiuser.py`: 多用户核心逻辑
- `user_manager.py`: 用户隔离与配置加载
- `users/_template/`: 用户配置模板
- `shared/global.json`: 全局共享配置

## 快速开始
1. 安装依赖
```bash
pip install -r requirements.txt
```
2. 复制模板并创建用户目录（示例 `xu`）
```bash
mkdir -p users/xu
cp users/_template/config.json.template users/xu/config.json
cp users/_template/state.json.default users/xu/state.json
cp users/_template/presets.json.default users/xu/presets.json
```
3. 将 `users/xu/config.json` 中的 Telegram/账号信息改为你自己的配置。
4. 将该账号 session 文件放到 `users/xu/` 下（文件名需与 `session_name` 一致）。
5. 启动
```bash
python3 main_multiuser.py
```

## 说明
- 默认读取 `users/*/config.json` 作为多账号配置。
- 仅保留多用户实现，不包含单用户入口。
