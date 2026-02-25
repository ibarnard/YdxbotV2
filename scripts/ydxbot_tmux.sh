#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/YdxbotV2}"
SESSION_NAME="${TMUX_SESSION:-ydxbot}"
ENTRYPOINT="${ENTRYPOINT:-main_multiuser.py}"
PYTHON_BIN="${PYTHON_BIN:-$APP_DIR/venv/bin/python}"

usage() {
  cat <<'EOF'
用法:
  ydxbot_tmux.sh <start|stop|restart|status|attach|logs> [参数]

命令:
  start     启动 bot（tmux 会话）
  stop      停止 bot（关闭 tmux 会话）
  restart   重启 bot（stop + start）
  status    查看会话和进程状态
  attach    进入 tmux 会话
  logs      查看日志（默认 bot，支持 bot|numai|user|all）

环境变量（可选）:
  APP_DIR        项目目录，默认 /opt/YdxbotV2
  TMUX_SESSION   会话名，默认 ydxbot
  ENTRYPOINT     入口脚本，默认 main_multiuser.py
  PYTHON_BIN     Python 路径，默认 $APP_DIR/venv/bin/python
EOF
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "❌ 缺少命令: $1"
    exit 1
  fi
}

has_session() {
  tmux has-session -t "$SESSION_NAME" 2>/dev/null
}

warn_systemd_conflict() {
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet ydxbot 2>/dev/null; then
      echo "⚠️ 检测到 systemd 服务 ydxbot 正在运行。"
      echo "   建议先执行: systemctl stop ydxbot"
      echo "   避免 systemd 与 tmux 同时运行造成会话锁冲突。"
      return 1
    fi
  fi
  return 0
}

start_bot() {
  need_cmd tmux

  if has_session; then
    echo "✅ 已在运行（tmux 会话: $SESSION_NAME）"
    return 0
  fi

  if [[ ! -d "$APP_DIR" ]]; then
    echo "❌ 目录不存在: $APP_DIR"
    exit 1
  fi
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "❌ Python 不存在或不可执行: $PYTHON_BIN"
    echo "   请先创建虚拟环境并安装依赖。"
    exit 1
  fi

  warn_systemd_conflict || exit 1

  local cmd
  cmd="cd \"$APP_DIR\" && unset YDXBOT_SYSTEMD_SERVICE SYSTEMD_SERVICE && exec \"$PYTHON_BIN\" -u \"$ENTRYPOINT\""
  tmux new-session -d -s "$SESSION_NAME" "$cmd"

  if has_session; then
    echo "✅ 已启动: tmux 会话 $SESSION_NAME"
    echo "👉 进入会话: tmux attach -t $SESSION_NAME"
  else
    echo "❌ 启动失败"
    exit 1
  fi
}

stop_bot() {
  need_cmd tmux
  if has_session; then
    tmux kill-session -t "$SESSION_NAME"
    echo "🛑 已停止: tmux 会话 $SESSION_NAME"
  else
    echo "ℹ️ 会话不存在: $SESSION_NAME"
  fi
}

status_bot() {
  need_cmd tmux
  if has_session; then
    echo "✅ tmux 会话运行中: $SESSION_NAME"
    tmux list-sessions | sed 's/^/  /'
  else
    echo "❌ tmux 会话未运行: $SESSION_NAME"
  fi
  echo "----- 进程检查 -----"
  pgrep -af "python.*${ENTRYPOINT}" || true
}

attach_bot() {
  need_cmd tmux
  if ! has_session; then
    echo "❌ 会话不存在: $SESSION_NAME"
    echo "   请先执行: $0 start"
    exit 1
  fi
  tmux attach -t "$SESSION_NAME"
}

show_logs() {
  local target="${1:-bot}"
  case "$target" in
    bot)
      tail -n 120 -F "$APP_DIR/bot.log"
      ;;
    numai)
      tail -n 120 -F "$APP_DIR/numai.log"
      ;;
    user)
      tail -n 120 -F "$APP_DIR/user_manager.log"
      ;;
    all)
      tail -n 120 -F "$APP_DIR/bot.log" "$APP_DIR/numai.log" "$APP_DIR/user_manager.log"
      ;;
    *)
      echo "❌ 不支持的日志类型: $target"
      echo "   可选: bot | numai | user | all"
      exit 1
      ;;
  esac
}

main() {
  local action="${1:-}"
  case "$action" in
    start)
      start_bot
      ;;
    stop)
      stop_bot
      ;;
    restart)
      stop_bot
      start_bot
      ;;
    status)
      status_bot
      ;;
    attach)
      attach_bot
      ;;
    logs)
      show_logs "${2:-bot}"
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      echo "❌ 未知命令: $action"
      usage
      exit 1
      ;;
  esac
}

main "${@:-}"
