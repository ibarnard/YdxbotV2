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
2. 先配置共享项（所有账号共用）
编辑 `shared/global.json`，填写这些共享配置：
- `proxy`
- `ai`（模型、apikey、降级链）
- `groups`（`zq_group` / `zq_bot` / `admin_chat` / `monitor`）
- `notification`（IYUU / TG Bot）

3. 复制模板并创建用户目录（示例 `xu`）
```bash
mkdir -p users/xu
cp users/_template/config.json.template users/xu/config.json
cp users/_template/state.json.default users/xu/state.json
cp users/_template/presets.json.default users/xu/presets.json
```
4. 将 `users/xu/config.json` 中的私有信息改为你自己的配置（最少：
   - `telegram`（api_id / api_hash / session_name / user_id）
   - `account.name`
   - `zhuque.cookie` / `zhuque.x_csrf`）
5. 将该账号 session 文件放到 `users/xu/` 下（文件名需与 `session_name` 一致）。
6. 启动
```bash
python3 main_multiuser.py
```

## 说明
- 默认读取 `shared/global.json` 作为共享配置，再与 `users/*/config.json` 做深度合并（用户配置优先）。
- 仅保留多用户实现，不包含单用户入口。
