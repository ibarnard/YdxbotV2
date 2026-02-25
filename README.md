# YdxbotV2 (Multiuser)

Ydxbot 的多账号版本，面向同一进程管理多个 Telegram 账号的自动下注与通知场景。

## 核心特性
- 多账号隔离：每个账号独立 `state/presets/session`。
- 共享配置：代理、AI、群组、通知统一在 `shared` 下管理。
- 风控能力：连输告警、炸号暂停、盈利暂停、手动暂停/恢复。
- 在线更新：支持 `ver / update / reback / restart`（兼容旧命令别名）。
- 运维方式：支持 `tmux`（推荐）与 `systemd`（可选）。

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

## 从空白 VPS 部署（推荐：tmux）
以下步骤适用于 Ubuntu/Debian 新机器，目标目录为 `/opt/YdxbotV2`。

1. 一条命令安装（拉代码 + venv + 依赖 + tmux 会话）
```bash
bash -lc 'set -e; apt update; apt install -y git python3 python3-venv python3-pip tmux curl; if [ ! -d /opt/YdxbotV2/.git ]; then git clone https://github.com/ibarnard/YdxbotV2.git /opt/YdxbotV2; fi; cd /opt/YdxbotV2; git fetch origin --tags; git checkout main; git pull --ff-only origin main; [ -d venv ] || python3 -m venv venv; . venv/bin/activate; python -m pip install -U pip; pip install -r requirements.txt; tmux has-session -t ydxbot 2>/dev/null || tmux new-session -d -s ydxbot -c /opt/YdxbotV2'
```

2. 准备共享配置
```bash
cp shared/global.example.json shared/global.json
```
编辑 `shared/global.json`，重点填写：
- `groups`（`zq_group` / `zq_bot` / `admin_chat` / `monitor`）
- `ai`
- `notification`
- `proxy`（如需代理）

3. 创建用户目录（示例：`shuji`）
```bash
mkdir -p users/shuji
cp users/_template/config.json.template users/shuji/config.json
cp users/_template/state.json.default users/shuji/state.json
cp users/_template/presets.json.default users/shuji/presets.json
```
然后编辑 `users/shuji/config.json`，填写 `api_id/api_hash/session_name/user_id/cookie/x_csrf` 等私有信息。

4. 上传 session 文件
- 将 `session_name` 对应的 `.session` 文件放入该用户目录，例如：`users/shuji/<session_name>.session`。

5. 进入 tmux 并手动启动脚本（你要求的方式）
```bash
tmux attach -t ydxbot
cd /opt/YdxbotV2
source venv/bin/activate
python -u main_multiuser.py
```

6. tmux 常用操作
```bash
# 退出 tmux 窗口但保持脚本运行
# 按键: Ctrl+b 然后 d

# 重新进入会话
tmux attach -t ydxbot

# 查看当前会话
tmux ls
```

7. 重启脚本（tmux 模式）
```bash
tmux attach -t ydxbot
# 在会话里按 Ctrl+C 停止
cd /opt/YdxbotV2
source venv/bin/activate
python -u main_multiuser.py
```

8. 开机后启动（tmux）
```bash
tmux has-session -t ydxbot 2>/dev/null || tmux new-session -d -s ydxbot -c /opt/YdxbotV2
tmux send-keys -t ydxbot 'cd /opt/YdxbotV2 && source venv/bin/activate && python -u main_multiuser.py' C-m
```

## 代码更新流程（tmux）
更新到最新 `main`：
```bash
tmux attach -t ydxbot
# 在 tmux 里 Ctrl+C 停止脚本

cd /opt/YdxbotV2
git fetch origin --tags
git checkout main
git pull --ff-only origin main

source venv/bin/activate
pip install -r requirements.txt
python -u main_multiuser.py
```

更新到指定版本/提交：
```bash
tmux attach -t ydxbot
# Ctrl+C

cd /opt/YdxbotV2
git fetch origin --tags
git checkout <tag或commit>

source venv/bin/activate
pip install -r requirements.txt
python -u main_multiuser.py
```

如果更新时提示本地配置文件冲突，建议先备份后恢复：
```bash
cp -f shared/global.json /opt/ydxbot-global.backup.json
# 执行 git 更新后再恢复
cp -f /opt/ydxbot-global.backup.json shared/global.json
```

## 可选运行方式（systemd）
不建议长期混用 `nohup` / `tmux`。生产环境建议只用 `systemd` 托管，避免多开、会话锁冲突和“进程在跑但看不到”。

1. 创建服务文件 `/etc/systemd/system/ydxbot.service`
```ini
[Unit]
Description=YdxbotV2 Multiuser Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/YdxbotV2
# 关闭 stdout 缓冲，日志实时进入 journal
Environment=PYTHONUNBUFFERED=1
# 让 Telegram 的 restart 命令优先走 systemctl restart
Environment=YDXBOT_SYSTEMD_SERVICE=ydxbot.service
ExecStart=/opt/YdxbotV2/venv/bin/python -u /opt/YdxbotV2/main_multiuser.py
Restart=always
RestartSec=3
TimeoutStopSec=20
KillSignal=SIGINT

[Install]
WantedBy=multi-user.target
```

2. 启用并启动服务
```bash
# 先清理旧的手工进程，避免 session 库锁冲突
pkill -9 -f "python.*main_multiuser.py" || true
tmux kill-session -t ydxbot 2>/dev/null || true

systemctl daemon-reload
systemctl enable --now ydxbot
systemctl status ydxbot --no-pager -l
```

3. 常用 systemd 运维命令
```bash
systemctl status ydxbot
systemctl restart ydxbot
systemctl stop ydxbot
systemctl start ydxbot
systemctl disable --now ydxbot
```

## 代码更新流程（systemd/SSH）
```bash
cd /opt/YdxbotV2
git fetch origin --tags
git checkout main
git reset --hard origin/main
systemctl restart ydxbot
systemctl status ydxbot --no-pager -l
```

指定版本/提交更新：
```bash
cd /opt/YdxbotV2
git fetch origin --tags
git checkout main
git reset --hard <tag或commit>
systemctl restart ydxbot
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
- 更新：`ver` `update [版本|提交]` `reback [版本|提交]` `restart`

## 运行日志查看
tmux 模式下：
```bash
# 直接看实时控制台输出
tmux attach -t ydxbot

# 查看业务日志文件
tail -f /opt/YdxbotV2/bot.log
tail -f /opt/YdxbotV2/numai.log
tail -f /opt/YdxbotV2/user_manager.log
```

systemd 托管时，优先看 `journalctl`：
```bash
# 实时日志
journalctl -u ydxbot -f

# 最近 200 行
journalctl -u ydxbot -n 200 --no-pager

# 最近 30 分钟
journalctl -u ydxbot --since "30 min ago" --no-pager

# 只看错误
journalctl -u ydxbot --since "2 hours ago" --no-pager | egrep "ERROR|Traceback|TimeoutError|database is locked"
```

## 常见故障排障
1. `database is locked`
```bash
systemctl stop ydxbot
pkill -9 -f "python.*main_multiuser.py" || true
find /opt/YdxbotV2/users -type f \( -name "*.session-journal" -o -name "*.session-wal" -o -name "*.session-shm" \) -delete
systemctl start ydxbot
```

2. 启动反复 `TimeoutError`（连接 Telegram 超时）
```bash
# 检查网络连通
curl -I --max-time 8 https://api.telegram.org

# 检查共享配置中的 proxy 是否开启
python - <<'PY'
from pathlib import Path
from update_manager import _load_json_with_comments
cfg = _load_json_with_comments(Path("/opt/YdxbotV2/shared/global.local.json")) or _load_json_with_comments(Path("/opt/YdxbotV2/shared/global.json"))
print(cfg.get("proxy"))
PY
```
说明：如果 `proxy.enabled=true`，必须保证代理服务可用；否则改为 `false`。

3. Telegram 里执行 `restart` 没效果
- 检查服务名是否与 `Environment=YDXBOT_SYSTEMD_SERVICE=ydxbot.service` 一致。
- 检查 bot 进程是否由 systemd 启动：`systemctl status ydxbot`。
- 不要混用 `nohup`/`tmux` 与 `systemd`。

4. tmux 会话找不到
```bash
tmux ls
tmux new-session -d -s ydxbot -c /opt/YdxbotV2
tmux attach -t ydxbot
```

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
