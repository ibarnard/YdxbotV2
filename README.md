# YdxbotV2

多账号 Telegram 自动化脚本。

## 使用范围
仅用于个人技术研究与自动化流程学习，请遵守当地法律法规与平台规则。

## 快速安装
```bash
git clone https://github.com/ibarnard/YdxbotV2.git
cd YdxbotV2
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 最小配置
1. 通用配置
```bash
cp config/global_config.example.json config/global_config.json
```
编辑 `config/global_config.json`，按实际环境填写必要字段。

2. 新建账号目录（示例：`xu`）
```bash
mkdir -p users/xu
cp users/_template/example_config.json users/xu/xu_config.json
cp users/_template/state.json.default users/xu/state.json
cp users/_template/presets.json.default users/xu/presets.json
```
编辑 `users/xu/xu_config.json`，填写该账号私有信息。

3. 放置 session
- 将该账号对应的 `.session` 文件放到 `users/xu/` 目录。

## 启动
```bash
python3 main_multiuser.py
```

## 更新
```bash
git fetch --all --tags
git pull --ff-only origin main
source venv/bin/activate
pip install -r requirements.txt
```
然后重启脚本进程。

## 运行建议
- 建议使用 `tmux` 或 `systemd` 托管进程。
- 不要同时混用多种启动方式，避免重复进程。

## 安全说明
- 不要提交任何私密数据（会话文件、密钥、Cookie、Token 等）。
- `users/*` 下账号数据仅保留在本地或服务器。
