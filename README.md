# YdxbotV2 (Multiuser)

Ydxbot 的多账号版本，面向同一进程管理多个 Telegram 账号的自动下注与通知场景。

## 核心特性
- 多账号隔离：每个账号独立 `state/presets/session`。
- 共享配置：代理、AI、群组、通知统一在 `shared` 下管理。
- 风控能力：连输告警、炸号暂停、盈利暂停、手动暂停/恢复。
- 在线更新：支持 `upcheck / upnow / upref / uprollback / restart`。

## 目录结构
- `main_multiuser.py`：多用户主入口
- `zq_multiuser.py`：下注、结算、通知、命令处理
- `user_manager.py`：用户配置加载、状态持久化
- `update_manager.py`：版本检查、发布更新、回滚逻辑
- `shared/global.example.json`：公开仓库可提交的脱敏共享配置模板
- `users/_template/`：用户配置与状态模板

## 快速开始
1. 安装依赖
```bash
pip install -r requirements.txt
```

2. 准备共享配置（本地私有，不入库）
```bash
cp shared/global.example.json shared/global.json
```
编辑 `shared/global.json`，填写共享配置：
- `proxy`
- `ai`（模型、API Key、降级链）
- `groups`（`zq_group` / `zq_bot` / `admin_chat` / `monitor`）
- `notification`（IYUU / TG Bot）
- `update.github_token`（私有仓库更新可选）

3. 创建账号目录（示例 `xu`）
```bash
mkdir -p users/xu
cp users/_template/config.json.template users/xu/config.json
cp users/_template/state.json.default users/xu/state.json
cp users/_template/presets.json.default users/xu/presets.json
```

4. 填写账号私有配置 `users/xu/config.json`
- `telegram.api_id / telegram.api_hash / telegram.session_name / telegram.user_id`
- `account.name`
- `zhuque.cookie / zhuque.x_csrf`

5. 放置 session 文件  
将 `session_name` 对应的 `.session` 文件放到 `users/xu/` 下。

6. 启动
```bash
python3 main_multiuser.py
```

## 共享配置加载顺序
程序按顺序读取共享配置（命中即停止）：
1. `shared/global.local.json`
2. `shared/global.json`
3. `shared/global.example.json`

说明：`global.example.json` 仅用于模板回退，不建议直接用于生产运行。

## 常用命令
- 基础：`open` `off` `pause` `resume` `status` `users`
- 预设：`st <预设名>` `ys <名> ...` `yss` `yss dl <名>`
- 风控：`set <炸> <赢> <停> <盈停>` `warn <次数>` `gf <金额>`
- 测算：`yc <预设名>` 或 `yc <参数...>`
- 更新：`ver` `upcheck` `upnow [tag]` `upref <ref>` `uprollback` `restart`

## 公开仓库安全说明
- 已默认忽略：`shared/global.json`、`shared/global.local.json`、`*.session`、日志文件。
- 不要提交任何真实密钥、Cookie、Token、会话文件。
- 建议把密钥放在本地配置或环境变量（如 `YDXBOT_GITHUB_TOKEN`）。

## 发布前检查建议
```bash
git status
rg -n "github_pat_|ghp_|sk-[A-Za-z0-9]{20,}|:[A-Za-z0-9_-]{30,}" .
pytest -q tests_multiuser
```

## 免责声明
本项目仅用于技术研究与自动化流程学习，请遵守当地法律法规与平台规则。
