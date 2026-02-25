"""
zq_multiuser.py - 多用户版本核心逻辑
版本: 2.4.3
日期: 2026-02-21
功能: 多用户押注、结算、命令处理
"""

import logging
import asyncio
import json
import os
import random
import requests
import aiohttp
import time
import math
from collections import Counter
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from model_manager import model_manager as model_mgr
from user_manager import UserContext
from typing import Dict, Any
import constants
from update_manager import (
    list_version_catalog,
    reback_to_version,
    restart_process,
    update_to_version,
)

# 日志配置
logger = logging.getLogger('zq_multiuser')
logger.setLevel(logging.DEBUG)

file_handler = TimedRotatingFileHandler('bot.log', when='midnight', interval=1, backupCount=7, encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(user_id)s/%(event)s] - %(message)s - [%(data)s]',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)


def log_event(level, module, event, message=None, **kwargs):
    # 兼容旧调用: log_event(level, event, message, user_id, data)
    if message is None:
        message = event
        event = module
        module = 'zq'
    data = ', '.join(f'{k}={v}' for k, v in kwargs.items())
    user_id = kwargs.get('user_id', 0)
    # 使用 'mod' 而不是 'module'，因为 'module' 是 logging 的保留字段
    logger.log(level, message, extra={'user_id': str(user_id), 'mod': module, 'event': event, 'data': data})


# 格式化数字
def format_number(num):
    """与 master 版一致：使用千分位格式。"""
    return f"{int(num):,}"


# 仪表盘格式化 - 与master版本保持一致
def format_dashboard(user_ctx: UserContext) -> str:
    """生成并返回仪表盘信息 - 与master版本format_dashboard一致"""
    state = user_ctx.state
    rt = state.runtime
    
    # 显示近期40次结果（由近及远）
    reversed_data = ["✅" if x == 1 else "❌" for x in state.history[-40:][::-1]]
    mes = f"""📊 **近期 40 次结果**（由近及远）\n✅：大（1）  ❌：小（0）\n{os.linesep.join(
        " ".join(map(str, reversed_data[i:i + 10])) 
        for i in range(0, len(reversed_data), 10)
    )}\n\n———————————————\n🎯 **策略设定**\n"""
    mes += "🔢 **算法版本：V10**\n"
    mes += f"🤖 **模型 API：{rt.get('current_model_id', 'unknown')}**\n"
    mes += f"📋 **当前预设：{rt.get('current_preset_name', 'none')} {rt.get('continuous', 1)} {rt.get('lose_stop', 13)} {rt.get('lose_once', 3.0)} {rt.get('lose_twice', 2.1)} {rt.get('lose_three', 2.05)} {rt.get('lose_four', 2.0)} {rt.get('initial_amount', 500)}**\n"
    mes += f"💰 **初始金额：{rt.get('initial_amount', 500)}**\n⏹ **押注 {rt.get('lose_stop', 13)} 次停止**\n"
    mes += f"💥 **炸 {rt.get('explode', 5)} 次，暂停 {rt.get('stop', 3)} 局**\n📚 **押注倍率：{rt.get('lose_once', 3.0)} / {rt.get('lose_twice', 2.1)} / {rt.get('lose_three', 2.05)} / {rt.get('lose_four', 2.0)}**\n\n"
    
    # 余额显示逻辑 - 与master一致
    balance_status = rt.get('balance_status', 'ok')
    account_balance = rt.get('account_balance', 0)
    
    if balance_status == "auth_failed":
        balance_str = "⚠️ Cookie 失效"
    elif balance_status == "network_error":
        balance_str = "⚠️ 网络错误"
    elif account_balance == 0 and balance_status == "unknown":
        balance_str = "⏳ 获取中..."
    else:
        balance_str = f"{account_balance / 10000:.2f} 万"
        
    mes += f"💰 **账户余额：{balance_str}**\n"
    # 防止资金显示为负数
    display_fund = max(0, rt.get('gambling_fund', 0))
    mes += f"💰 **菠菜余额：{display_fund / 10000:.2f} 万**\n📈 **盈利目标：{rt.get('profit', 1000000) / 10000:.2f} 万，暂停 {rt.get('profit_stop', 5)} 局**\n"
    mes += f"📈 **本轮盈利：{rt.get('period_profit', 0) / 10000:.2f} 万**\n📈 **总盈利：{rt.get('earnings', 0) / 10000:.2f} 万**\n\n"
    
    win_total = rt.get('win_total', 0)
    total = rt.get('total', 0)
    if win_total > 0 or total > 0:
        win_rate = (win_total / total * 100) if total > 0 else 0.00
        mes += f"🎯 **押注次数：{total}**\n🏆 **胜率：{win_rate:.2f}%**\n💰 **收益：{format_number(rt.get('earnings', 0))}**"
    
    stop_count = rt.get('stop_count', 0)
    if stop_count > 1:
        mes += f"\n\n还剩 {stop_count} 局恢复押注"
    
    return mes


def get_bet_status_text(rt: Dict[str, Any]) -> str:
    """统一押注状态展示。"""
    if rt.get("manual_pause", False):
        return "手动暂停"
    if not rt.get("switch", True):
        return "已关闭"
    if rt.get("bet_on", False):
        return "运行中"
    return "已暂停"


# 消息分发规则表（与 master 一致）
MESSAGE_ROUTING_TABLE = {
    "win": {"channels": ["admin", "priority"], "priority": True},
    "explode": {"channels": ["admin", "priority"], "priority": True},
    "lose_streak": {"channels": ["admin", "priority"], "priority": True},
    "lose_end": {"channels": ["admin", "priority"], "priority": True},
    "pause": {"channels": ["admin"], "priority": False},
    "resume": {"channels": ["admin"], "priority": False},
    "settle": {"channels": ["admin"], "priority": False},
    "dashboard": {"channels": ["admin"], "priority": False},
    "info": {"channels": ["admin"], "priority": False},
    "warning": {"channels": ["admin"], "priority": False},
    "error": {"channels": ["admin", "priority"], "priority": True},
}


def _strip_account_prefix(text: str) -> str:
    """管理员消息统一移除账号前缀，与 master 行为一致。"""
    if text is None:
        return ""
    raw = str(text)
    normalized = raw.lstrip()
    if not normalized.startswith("【账号："):
        return raw
    lines = normalized.splitlines()
    if len(lines) <= 1:
        return ""
    return "\n".join(lines[1:]).lstrip("\n")


def _ensure_account_prefix(text: str, account_prefix: str) -> str:
    """重点渠道消息统一补充账号前缀。"""
    content = _strip_account_prefix(text)
    if not content:
        return account_prefix
    return f"{account_prefix}\n{content}"


def _iter_targets(target):
    if isinstance(target, (list, tuple, set)):
        return [item for item in target if item not in (None, "")]
    if target in (None, ""):
        return []
    return [target]


async def send_message_v2(
    client,
    msg_type: str,
    message: str,
    user_ctx: UserContext,
    global_config: dict,
    parse_mode: str = "markdown",
    title=None,
    desp=None
):
    """新版统一消息发送函数（多用户版）- 严格按路由表分发。"""
    routing = MESSAGE_ROUTING_TABLE.get(msg_type)
    if routing is None:
        error = f"未定义消息路由: {msg_type}"
        log_event(logging.ERROR, 'send_msg', '消息路由缺失', user_id=user_ctx.user_id, data=error)
        raise ValueError(error)

    channels = routing.get("channels", [])
    account_name = user_ctx.config.name.strip()
    account_prefix = f"【账号：{account_name}】"
    admin_message = _strip_account_prefix(message)
    # 重点通道（IYUU/TG Bot）统一带账号前缀；管理员通道统一不带前缀。
    priority_message = _ensure_account_prefix(message, account_prefix)
    priority_desp = _ensure_account_prefix(desp if desp is not None else message, account_prefix)

    sent_message = None
    if "admin" in channels or "all" in channels:
        try:
            admin_chat = user_ctx.config.groups.get("admin_chat")
            if admin_chat:
                # 修复：多用户分支 - 返回管理员消息对象，确保仪表盘/统计可被后续刷新删除。
                sent_message = await client.send_message(admin_chat, admin_message, parse_mode=parse_mode)
        except Exception as e:
            log_event(logging.ERROR, 'send_msg', '发送管理员消息失败', user_id=user_ctx.user_id, data=str(e))

    if "priority" in channels or "all" in channels:
        iyuu_cfg = user_ctx.config.notification.get("iyuu", {})
        if iyuu_cfg.get("enable"):
            try:
                final_title = title or f"菠菜机器人 {account_name} 通知"
                payload = {"text": final_title, "desp": priority_desp}
                iyuu_url = iyuu_cfg.get("url")
                if not iyuu_url:
                    token = iyuu_cfg.get("token")
                    iyuu_url = f"https://iyuu.cn/{token}.send" if token else None
                if iyuu_url:
                    requests.post(iyuu_url, data=payload, timeout=5)
            except Exception as e:
                log_event(logging.ERROR, 'send_msg', 'IYUU通知失败', user_id=user_ctx.user_id, data=str(e))

        tg_bot_cfg = user_ctx.config.notification.get("tg_bot", {})
        if tg_bot_cfg.get("enable"):
            try:
                bot_token = tg_bot_cfg.get("bot_token")
                chat_id = tg_bot_cfg.get("chat_id")
                if bot_token and chat_id:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    payload = {"chat_id": chat_id, "text": priority_message}
                    requests.post(url, json=payload, timeout=5)
            except Exception as e:
                log_event(logging.ERROR, 'send_msg', 'TG Bot通知失败', user_id=user_ctx.user_id, data=str(e))

    return sent_message


# 兼容旧接口
async def send_message(
    client,
    to: str,
    message: str,
    user_ctx: UserContext,
    global_config: dict,
    parse_mode: str = "markdown",
    title=None,
    desp=None,
    notify_type: str = "info"
):
    msg_type_map = {
        "profit": "win",
        "explode": "explode",
        "lose_streak": "lose_streak",
        "profit_recovery": "lose_end",
        "info": "info",
    }
    msg_type = msg_type_map.get(notify_type, "info")
    if to not in ("admin", "all", "priority", "iyuu", "tgbot"):
        log_event(logging.WARNING, 'send_msg', '旧接口to参数无效，已按路由表处理', user_id=user_ctx.user_id, data=f"to={to}, type={msg_type}")
        to = "admin"

    if to == "admin":
        return await send_message_v2(client, "info", message, user_ctx, global_config, parse_mode, title, desp)
    if to == "all":
        return await send_message_v2(client, msg_type, message, user_ctx, global_config, parse_mode, title, desp)

    # priority/iyuu/tgbot 兼容：仅走重点渠道
    account_name = user_ctx.config.name.strip()
    account_prefix = f"【账号：{account_name}】"
    priority_message = _ensure_account_prefix(message, account_prefix)
    priority_desp = _ensure_account_prefix(desp if desp is not None else message, account_prefix)
    if to in ("priority", "iyuu"):
        iyuu_cfg = user_ctx.config.notification.get("iyuu", {})
        if iyuu_cfg.get("enable"):
            final_title = title or f"菠菜机器人 {account_name} 通知"
            payload = {"text": final_title, "desp": priority_desp}
            iyuu_url = iyuu_cfg.get("url")
            if not iyuu_url:
                token = iyuu_cfg.get("token")
                iyuu_url = f"https://iyuu.cn/{token}.send" if token else None
            if iyuu_url:
                requests.post(iyuu_url, data=payload, timeout=5)
    if to in ("priority", "tgbot"):
        tg_bot_cfg = user_ctx.config.notification.get("tg_bot", {})
        if tg_bot_cfg.get("enable"):
            bot_token = tg_bot_cfg.get("bot_token")
            chat_id = tg_bot_cfg.get("chat_id")
            if bot_token and chat_id:
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                payload = {"chat_id": chat_id, "text": priority_message}
                requests.post(url, json=payload, timeout=5)
    return None


async def send_to_admin(client, message: str, user_ctx: UserContext, global_config: dict):
    return await send_message_v2(client, "info", message, user_ctx, global_config)


# ==================== V10 M-SMP 核心算法函数 ====================

def calculate_trend_gap(history, window=100):
    """
    计算趋势缺口：最近N期内"大"和"小"偏离50/50均衡线的数值
    返回: {
        'big_ratio': 大占比,
        'small_ratio': 小占比,
        'deviation_score': 标准差/偏离度,
        'gap': 向均值靠拢的缺口(正=缺大, 负=缺小),
        'regression_target': 统计学理论预测目标(0或1)
    }
    """
    if len(history) < window:
        window = len(history)
    
    recent = history[-window:]
    big_count = sum(recent)
    small_count = window - big_count
    
    big_ratio = big_count / window if window > 0 else 0.5
    small_ratio = small_count / window if window > 0 else 0.5
    
    deviation_score = abs(big_ratio - 0.5) * 2
    
    gap = (window / 2) - big_count
    
    regression_target = 1 if big_count < small_count else 0
    
    return {
        'big_ratio': round(big_ratio, 3),
        'small_ratio': round(small_ratio, 3),
        'deviation_score': round(deviation_score, 3),
        'gap': int(gap),
        'regression_target': regression_target,
        'big_count': big_count,
        'small_count': small_count
    }


def extract_pattern_features(history):
    """
    提取形态特征：自动检测单跳、长龙、对称环绕等状态
    返回: {
        'pattern_tag': 形态标签,
        'tail_streak_len': 尾部连龙长度,
        'tail_streak_char': 尾部连龙字符(0/1),
        'is_alternating': 是否单跳模式,
        'is_symmetric': 是否对称环绕
    }
    """
    if not history or len(history) < 3:
        return {
            'pattern_tag': 'INSUFFICIENT_DATA',
            'tail_streak_len': 0,
            'tail_streak_char': None,
            'is_alternating': False,
            'is_symmetric': False
        }
    
    seq_str = ''.join(['1' if x == 1 else '0' for x in history])
    
    tail_char = seq_str[-1]
    tail_streak_len = 1
    for i in range(len(seq_str) - 2, -1, -1):
        if seq_str[i] == tail_char:
            tail_streak_len += 1
        else:
            break
    
    is_alternating = False
    if len(seq_str) >= 6:
        recent_6 = seq_str[-6:]
        if recent_6 in ['010101', '101010']:
            is_alternating = True
    
    is_symmetric = False
    if len(seq_str) >= 5:
        recent_5 = seq_str[-5:]
        if recent_5 == recent_5[::-1]:
            is_symmetric = True
    
    if tail_streak_len >= 5:
        pattern_tag = 'LONG_DRAGON'
    elif tail_streak_len >= 3:
        pattern_tag = 'DRAGON_CANDIDATE'
    elif is_alternating:
        pattern_tag = 'SINGLE_JUMP'
    elif is_symmetric:
        pattern_tag = 'SYMMETRIC_WRAP'
    else:
        pattern_tag = 'CHAOS_SWITCH'
    
    return {
        'pattern_tag': pattern_tag,
        'tail_streak_len': tail_streak_len,
        'tail_streak_char': int(tail_char),
        'is_alternating': is_alternating,
        'is_symmetric': is_symmetric
    }


def fallback_prediction(history):
    """
    天眼兜底机制：如果AI异常，强行维持50:50概率
    缺哪个补哪个，绝不暂停！
    """
    if not history:
        return 1
    
    window = min(40, len(history))
    recent = history[-window:]
    big_count = sum(recent)
    small_count = window - big_count
    
    prediction = 1 if big_count < small_count else 0
    
    log_event(logging.WARNING, 'predict_v10', '天眼兜底触发', 
              user_id=0, data=f'big={big_count}, small={small_count}, fallback={prediction}')
    
    return prediction


def parse_analysis_result_insight(resp_text, default_prediction=1):
    """
    天眼模式：解析AI输出，绝不返回暂停
    只返回0或1，confidence和reason
    """
    try:
        cleaned = str(resp_text).replace('```json', '').replace('```', '').strip()
        if cleaned.lower().startswith('json'):
            cleaned = cleaned[4:].strip()
        start = cleaned.find('{')
        end = cleaned.rfind('}')
        if start >= 0 and end > start:
            cleaned = cleaned[start:end + 1]
        resp_json = json.loads(cleaned)
        
        prediction = resp_json.get('prediction', default_prediction)
        if isinstance(prediction, str):
            prediction = 1 if prediction.upper() in ['1', 'B', 'BIG', '大'] else 0
        prediction = int(prediction)
        if prediction not in [0, 1]:
            prediction = default_prediction
        
        confidence = int(resp_json.get('confidence', 50))
        confidence = max(0, min(100, confidence))
        
        reason = resp_json.get('reason', resp_json.get('logic', '天眼分析'))
        
        return {
            'prediction': prediction,
            'confidence': confidence,
            'reason': reason
        }
    except Exception as e:
        return {
            'prediction': default_prediction,
            'confidence': 50,
            'reason': f'解析兜底:{str(e)[:20]}'
        }


# V10 预测函数 - M-SMP架构
async def predict_next_bet_v10(user_ctx: UserContext, global_config: dict, current_round: int = 1) -> int:
    """
    V10 深度量化博弈版：多策略模拟预测（M-SMP）架构
    核心逻辑：多策略人格模拟博弈，强制输出0或1，绝不暂停！
    """
    state = user_ctx.state
    rt = state.runtime
    history = state.history
    
    try:
        # ========== 第一步：构建三维历史快照（交易员终端感） ==========
        
        # 1.1 短期精确抖动（20局）
        short_term_20 = history[-20:] if len(history) >= 20 else history[:]
        short_str = "".join(['1' if x == 1 else '0' for x in short_term_20])
        
        # 1.2 中期暗趋势（50局）
        medium_term_50 = history[-50:] if len(history) >= 50 else history[:]
        medium_str = "".join(['1' if x == 1 else '0' for x in medium_term_50])
        
        # 1.3 长期大周期回归（100局）
        long_term_100 = history[-100:] if len(history) >= 100 else history[:]
        long_term_gap = round(sum(long_term_100) / len(long_term_100), 3) if long_term_100 else 0.5
        
        # 1.4 趋势缺口计算
        trend_gap = calculate_trend_gap(history, window=100)
        big_cnt = trend_gap['big_count']
        small_cnt = trend_gap['small_count']
        gap = trend_gap['gap']
        
        # 1.5 形态特征
        pattern_features = extract_pattern_features(history)
        pattern_tag = pattern_features['pattern_tag']
        tail_streak_len = pattern_features['tail_streak_len']
        tail_streak_char = pattern_features['tail_streak_char']
        
        # 1.6 模式标记
        lose_count = rt.get('lose_count', 0)
        entropy_tag = "Pattern_Breaking" if lose_count > 2 else "Stability"
        
        # ========== 第二步：构建交易员终端数据负载 ==========
        
        payload = {
            "current_status": {
                "martingale_step": lose_count + 1,
                "total_profit_to_date": rt.get('earnings', 0),
                "entropy_tag": entropy_tag
            },
            "history_views": {
                "short_term_20": short_str,
                "medium_term_50": medium_str,
                "long_term_gap": long_term_gap,
                "big_count_100": big_cnt,
                "small_count_100": small_cnt
            },
            "pattern_analysis": {
                "tag": pattern_tag,
                "tail_streak_len": tail_streak_len,
                "tail_streak_char": tail_streak_char,
                "gap": f"{gap:+d}"
            }
        }
        
        # ========== 第三步：深度博弈推理Prompt（M-SMP架构） ==========
        
        current_model_id = rt.get('current_model_id', 'qwen3-coder-plus')
        
        prompt = f"""[System Instruction]
你是专门破解博弈陷阱的量化交易员。在任何情况下，你的预测严禁输出"NONE"、"SKIP"或建议观望。

[Context Reasoning Flow]
请按顺序执行以下深度推理步骤：

1. 趋势派模型（Trend Following）:
检查当前是否有活跃的长龙（Streak > 5）？如果当前由于随机偏差导致某一数字连出，判定此时逆行阻力大还是顺行阻力大？

2. 数学均衡派模型（Mean Reversion）:
分析过去 100 局。如果大数出的多（>55%），那么在最近的博弈周期内，什么时候是大数由于"均值压力"不得不转向小数点的爆破点？

3. 形态锁定（Sequential Match）:
在历史库中寻找类似的近期片段，识别这种伪随机的惯性分布。

[Data Evidence]
短期20局: {short_str}
中期50局: {medium_str}
长期100局大占比: {long_term_gap}
当前形态: {pattern_tag} (尾部{tail_streak_len}连{'大' if tail_streak_char==1 else '小'})
大数缺口: {gap:+d} (正=缺大, 负=缺小)
倍投压力: 第{lose_count + 1}次 ({entropy_tag})

[Final Choice]
如果短线趋势与长线回归发生冲突（例如长线该回补大，短线一直出小），你必须基于"赌场非对称概率"法则做出一个当前瞬间最理性的抉择。

你必须给出一个自信得分。但无论分值多低，prediction 只能选 0 或 1。

[Response Format]
必须且只能输出如下 JSON：
{{"logic": "50字内分析证据流", "reasoning": "你是顺风追龙还是逆风阻杀龙的原因", "confidence": 1-100, "prediction": 0或1}}

记住：系统已废除暂停机制，你必须给出0或1！"""

        messages = [
            {'role': 'system', 'content': '你是专门破解博弈陷阱的量化交易员，只输出纯JSON，严禁解释性文本，严禁输出NONE或SKIP。'},
            {'role': 'user', 'content': prompt}
        ]
        
        log_event(logging.INFO, 'predict_v10', f'M-SMP模式调用: {current_model_id}', 
                  user_id=user_ctx.user_id, data=f'形态:{pattern_tag} 缺口:{gap:+d} 压力:{lose_count + 1}次')
        
        # ========== 第四步：调用模型与多层兜底 ==========
        
        try:
            result = await model_mgr.call_model(current_model_id, messages, temperature=0.1, max_tokens=500)
            if not result['success']:
                raise Exception(f"Model Error: {result['error']}")
            
            default_pred = trend_gap['regression_target']
            final_result = parse_analysis_result_insight(result['content'], default_prediction=default_pred)
            
        except Exception as model_error:
            log_event(logging.WARNING, 'predict_v10', '模型调用失败，统计兜底', 
                      user_id=user_ctx.user_id, data=str(model_error))
            final_result = {
                'prediction': trend_gap['regression_target'],
                'confidence': 50,
                'reason': '模型异常，统计回归兜底'
            }
        
        # ========== 第五步：结果强制校验与记录 ==========
        
        prediction = final_result['prediction']
        confidence = final_result['confidence']
        reason = final_result.get('reason', final_result.get('logic', '深度分析'))
        
        if prediction not in [0, 1]:
            prediction = trend_gap['regression_target']
            confidence = 50
            reason = '强制校正：统计回归'
        
        # 构建预测信息
        rt["last_predict_info"] = (
            f"M-SMP/{pattern_tag} | {reason} | 信:{confidence}% | "
            f"缺口:{gap:+d} | 回归:{trend_gap['regression_target']}"
        )
        
        # 审计日志
        audit_log = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "round": current_round,
            "mode": "M-SMP",
            "input_payload": payload,
            "output": final_result,
            "model_id": current_model_id,
        }
        rt["last_logic_audit"] = json.dumps(audit_log, ensure_ascii=False, indent=2)
        
        # 写入用户目录下的decisions.log
        user_dir = user_ctx.user_dir
        decisions_log_path = os.path.join(user_dir, "decisions.log")
        try:
            with open(decisions_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(audit_log, ensure_ascii=False) + "\n")
        except Exception as e:
            log_event(logging.WARNING, 'predict_v10', '写入decisions.log失败', 
                      user_id=user_ctx.user_id, data=str(e))
        
        # 记录预测
        state.predictions.append(prediction)
        
        log_event(logging.INFO, 'predict_v10', 'M-SMP预测完成', 
                  user_id=user_ctx.user_id, data=f'pred={prediction}, conf={confidence}, pattern={pattern_tag}')
        
        return prediction
        
    except Exception as e:
        log_event(logging.ERROR, 'predict_v10', 'M-SMP异常，最终保底', 
                  user_id=user_ctx.user_id, data=str(e))
        
        recent_20 = history[-20:] if len(history) >= 20 else history
        recent_sum = sum(recent_20)
        fallback = 0 if recent_sum >= len(recent_20) / 2 else 1
        
        rt["last_predict_info"] = f"M-SMP终极保底 | 强制预测:{fallback}"
        state.predictions.append(fallback)
        return fallback


# 押注处理
async def process_bet_on(client, event, user_ctx: UserContext, global_config: dict):
    state = user_ctx.state
    rt = state.runtime
    
    await asyncio.sleep(5)  # 与 master 一致：延迟等待消息稳定
    text = event.message.message

    if not rt.get("switch", True):
        log_event(logging.INFO, 'bet_on', 'off 命令触发，预测及下注路径已关闭', user_id=user_ctx.user_id)
        if rt.get("bet", False):
            await send_to_admin(client, "押注已关闭，无法执行", user_ctx, global_config)
            rt["bet"] = False
            user_ctx.save_state()
        return

    if rt.get("manual_pause", False):
        if rt.get("bet", False):
            rt["bet"] = False
            user_ctx.save_state()
        log_event(logging.DEBUG, 'bet_on', '手动暂停中，跳过押注', user_id=user_ctx.user_id)
        return

    stop_count = int(rt.get("stop_count", 0))
    if stop_count > 0:
        rt["stop_count"] = stop_count - 1
        if rt["stop_count"] == 0:
            rt["bet"] = True
            rt["bet_on"] = True
            rt["mode_stop"] = True
        else:
            user_ctx.save_state()
            log_event(logging.INFO, 'bet_on', '暂停中跳过押注', user_id=user_ctx.user_id, data=f"stop_count={rt['stop_count']}")
            return

    # 修复：多用户分支 - 更稳健解析历史串（支持换行/多空格），尽量回填更多历史。
    try:
        import re
        history_match = re.search(r"\[0\s*小\s*1\s*大\]([\s\S]*)", text)
        if history_match:
            history_str = history_match.group(1)
            new_history = [int(x) for x in re.findall(r"(?<!\d)[01](?!\d)", history_str)]
            if new_history and len(new_history) >= len(state.history):
                state.history = new_history[-2000:]
    except Exception as e:
        log_event(logging.WARNING, 'bet_on', '解析历史数据失败', user_id=user_ctx.user_id, data=str(e))

    # 修复：对齐master分支 - 历史不足40局也允许继续押注（预测函数已具备短历史兜底）。
    if len(state.history) < 40:
        log_event(logging.INFO, 'bet_on', '历史数据低于40局，继续执行押注', user_id=user_ctx.user_id, data=f'len={len(state.history)}')

    bet_amount = calculate_bet_amount(rt)
    if bet_amount <= 0:
        rt["bet"] = False
        user_ctx.save_state()
        return

    if not is_fund_available(user_ctx, bet_amount):
        if rt.get("bet", False):
            display_fund = max(0, rt.get("gambling_fund", 0))
            mes = f"**菠菜资金不足，已暂停押注**\n当前剩余：{display_fund / 10000:.2f} 万\n请使用 `gf [金额]` 恢复"
            await send_to_admin(client, mes, user_ctx, global_config)
        rt["bet"] = False
        user_ctx.save_state()
        return

    if not (rt.get("bet_on", False) or rt.get("mode_stop", True)):
        log_event(logging.DEBUG, 'bet_on', '押注已暂停', user_id=user_ctx.user_id)
        return

    if not event.reply_markup:
        rt["bet"] = False
        user_ctx.save_state()
        return

    log_event(logging.INFO, 'bet_on', '开始押注', user_id=user_ctx.user_id)
    try:
        rt["last_predict_info"] = "初始化预测"
        prediction = await predict_next_bet_v10(user_ctx, global_config)
        if prediction in (-1, None):
            recent_40 = state.history[-40:] if len(state.history) >= 40 else state.history
            recent_total = sum(recent_40)
            prediction = 1 if recent_total < len(recent_40) / 2 else 0
            rt["last_predict_info"] = f"AI节点闪退 - 触发智能统计回补预测(补{'大' if prediction == 1 else '小'})"

        rt["bet_amount"] = int(bet_amount)
        direction = "大" if prediction == 1 else "小"
        direction_en = "big" if prediction == 1 else "small"
        buttons = constants.BIG_BUTTON if prediction == 1 else constants.SMALL_BUTTON
        combination = constants.find_combination(rt["bet_amount"], buttons)

        if not combination:
            rt["bet"] = False
            log_event(logging.WARNING, 'bet_on', '未找到金额组合', user_id=user_ctx.user_id, data=f"amount={rt['bet_amount']}")
            user_ctx.save_state()
            return

        rt["bet"] = True
        rt["total"] = rt.get("total", 0) + 1
        rt["bet_sequence_count"] = rt.get("bet_sequence_count", 0) + 1

        for amount in combination:
            button_data = buttons.get(amount)
            if button_data is not None:
                await event.click(button_data)
                await asyncio.sleep(1.5)

        rt["bet_type"] = 1 if prediction == 1 else 0
        rt["bet_on"] = True

        bet_id = generate_bet_id(user_ctx)
        state.bet_sequence_log.append({
            "bet_id": bet_id,
            "sequence": rt.get("bet_sequence_count", 0),
            "direction": direction_en,
            "amount": rt["bet_amount"],
            "result": None,
            "profit": 0,
            "lose_stop": rt.get("lose_stop", 13),
            "profit_target": rt.get("profit", 1000000)
        })
        state.bet_sequence_log = state.bet_sequence_log[-5000:]

        bet_report = generate_mobile_bet_report(
            state.history,
            direction,
            rt["bet_amount"],
            rt.get("bet_sequence_count", 1),
            bet_id
        )
        message = await send_to_admin(client, bet_report, user_ctx, global_config)
        asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
        if message:
            asyncio.create_task(delete_later(client, message.chat_id, message.id, 100))

        rt["current_bet_seq"] = int(rt.get("current_bet_seq", 1)) + 1
        user_ctx.save_state()
    except Exception as e:
        log_event(logging.ERROR, 'bet_on', '押注失败', user_id=user_ctx.user_id, data=str(e))
        await send_to_admin(client, f"押注出错: {e}", user_ctx, global_config)


# 结算处理
async def cleanup_message(client, message_ref):
    """安全地删除指定消息对象。"""
    if not message_ref:
        return
    try:
        await message_ref.delete()
        return
    except Exception:
        pass
    try:
        chat_id = getattr(message_ref, "chat_id", None)
        msg_id = getattr(message_ref, "id", None)
        if chat_id is not None and msg_id is not None:
            await client.delete_messages(chat_id, msg_id)
    except Exception:
        pass


async def process_red_packet(client, event, user_ctx: UserContext, global_config: dict):
    """处理红包消息，尝试领取。"""
    sender_id = getattr(event, "sender_id", None)
    zq_bot = user_ctx.config.groups.get("zq_bot")
    zq_bot_targets = {str(item) for item in _iter_targets(zq_bot)}
    if zq_bot_targets and str(sender_id) not in zq_bot_targets:
        return

    text = (getattr(event, "raw_text", None) or getattr(event, "text", None) or "").strip()
    if "灵石" not in text:
        return

    reply_markup = getattr(event, "reply_markup", None)
    rows = getattr(reply_markup, "rows", None) if reply_markup else None
    if not rows:
        return

    first_row = rows[0]
    buttons = getattr(first_row, "buttons", None)
    if not buttons:
        return

    button = buttons[0]
    button_data = getattr(button, "data", None)
    if not button_data:
        log_event(logging.WARNING, "red_packet", "红包按钮无效", user_id=user_ctx.user_id)
        return

    log_event(
        logging.INFO,
        "red_packet",
        "检测到红包按钮消息",
        user_id=user_ctx.user_id,
        msg_id=getattr(event, "id", None),
    )

    from telethon.tl import functions as tl_functions
    import re

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            try:
                await event.click(0, 0)
            except Exception:
                await event.click(button_data)
            await asyncio.sleep(random.uniform(0.5, 1.0))

            response = await client(
                tl_functions.messages.GetBotCallbackAnswerRequest(
                    peer=event.chat_id,
                    msg_id=event.id,
                    data=button_data,
                )
            )
            response_msg = getattr(response, "message", "") or ""

            if "已获得" in response_msg:
                bonus_match = re.search(r"已获得\s*(\d+)\s*灵石", response_msg)
                bonus = bonus_match.group(1) if bonus_match else "未知数量"
                mes = f"🎉 抢到红包 {bonus} 灵石！"
                log_event(
                    logging.INFO,
                    "red_packet",
                    "领取成功",
                    user_id=user_ctx.user_id,
                    bonus=bonus,
                )
                await send_to_admin(client, mes, user_ctx, global_config)
                return

            if any(flag in response_msg for flag in ("不能重复领取", "来晚了", "领过")):
                log_event(
                    logging.INFO,
                    "red_packet",
                    "红包已领取或过期",
                    user_id=user_ctx.user_id,
                    response=response_msg,
                )
                return

            log_event(
                logging.WARNING,
                "red_packet",
                "红包领取回复未知，准备重试",
                user_id=user_ctx.user_id,
                attempt=attempt + 1,
                response=response_msg[:80],
            )
        except Exception as e:
            log_event(
                logging.WARNING,
                "red_packet",
                "尝试领取红包失败",
                user_id=user_ctx.user_id,
                attempt=attempt + 1,
                error=str(e),
            )

        if attempt < max_attempts - 1:
            await asyncio.sleep(random.uniform(1.5, 2.5) * (attempt + 1))

    log_event(
        logging.WARNING,
        "red_packet",
        "多次尝试后未成功领取红包",
        user_id=user_ctx.user_id,
        msg_id=getattr(event, "id", None),
    )


def is_fund_available(user_ctx: UserContext, bet_amount: int = 0) -> bool:
    """检查资金是否充足（与 master 版语义一致：需同时满足余额>0且>=本次下注金额）。"""
    rt = user_ctx.state.runtime
    gambling_fund = rt.get("gambling_fund", 0)
    return gambling_fund > 0 and gambling_fund >= bet_amount


def calculate_bet_amount(rt: dict) -> int:
    """按 master 逻辑计算本局下注金额。"""
    win_count = rt.get("win_count", 0)
    lose_count = rt.get("lose_count", 0)
    initial_amount = int(rt.get("initial_amount", 500))
    lose_stop = int(rt.get("lose_stop", 13))
    lose_once = float(rt.get("lose_once", 3))
    lose_twice = float(rt.get("lose_twice", 2.1))
    lose_three = float(rt.get("lose_three", 2.1))
    lose_four = float(rt.get("lose_four", 2.05))

    if win_count >= 0 and lose_count == 0:
        return constants.closest_multiple_of_500(initial_amount)

    if (lose_count + 1) > lose_stop:
        return 0

    base_amount = int(rt.get("bet_amount", initial_amount))
    if lose_count == 1:
        target = base_amount * lose_once
    elif lose_count == 2:
        target = base_amount * lose_twice
    elif lose_count == 3:
        target = base_amount * lose_three
    else:
        target = base_amount * lose_four

    # 与 master 一致：补 1% 安全边际
    return constants.closest_multiple_of_500(target + target * 0.01)


def count_consecutive(history):
    """统计连续出现次数 - 与master版本一致"""
    result_counts = {"大": {}, "小": {}}
    if not history:
        return result_counts
    
    current_streak = 1
    for i in range(1, len(history)):
        if history[i] == history[i-1]:
            current_streak += 1
        else:
            key = "大" if history[i-1] == 1 else "小"
            result_counts[key][current_streak] = result_counts[key].get(current_streak, 0) + 1
            current_streak = 1
    
    key = "大" if history[-1] == 1 else "小"
    result_counts[key][current_streak] = result_counts[key].get(current_streak, 0) + 1
    
    return result_counts


def count_lose_streaks(bet_sequence_log):
    """统计连输次数 - 与master版本一致"""
    lose_streaks = {}
    current_streak = 0
    
    for entry in bet_sequence_log:
        profit = entry.get("profit", 0)
        if profit < 0:
            current_streak += 1
        else:
            if current_streak > 0:
                lose_streaks[current_streak] = lose_streaks.get(current_streak, 0) + 1
            current_streak = 0
    
    if current_streak > 0:
        lose_streaks[current_streak] = lose_streaks.get(current_streak, 0) + 1
    
    return lose_streaks


def generate_bet_id(user_ctx: UserContext) -> str:
    """生成押注 ID（与 master 逻辑一致：按天重置轮次）。"""
    rt = user_ctx.state.runtime
    current_date = datetime.now().strftime("%Y%m%d")
    if current_date != rt.get("last_reset_date", ""):
        rt["current_round"] = 1
        rt["current_bet_seq"] = 1
        rt["last_reset_date"] = current_date
    return f"{current_date}_{rt.get('current_round', 1)}_{rt.get('current_bet_seq', 1)}"


def format_bet_id(bet_id):
    """将押注 ID 转换为直观格式，如 '3月14日第 1 轮第 12 次'。"""
    try:
        date_str, round_num, seq_num = str(bet_id).split('_')
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        return f"{month}月{day}日第 {round_num} 轮第 {seq_num} 次"
    except Exception:
        return str(bet_id)


def get_settle_position(state, rt):
    """
    获取当前结算对应的轮次与序号。
    优先用当前结算 bet_id，回退到 current_bet_seq - 1。
    """
    settle_round = int(rt.get("current_round", 1))
    settle_seq = max(1, int(rt.get("current_bet_seq", 1)) - 1)
    if state.bet_sequence_log:
        last_bet_id = str(state.bet_sequence_log[-1].get("bet_id", ""))
        import re
        match = re.match(r"^\d{8}_(\d+)_(\d+)$", last_bet_id)
        if match:
            settle_round = int(match.group(1))
            settle_seq = int(match.group(2))
    return settle_round, settle_seq


def _format_recent_binary(history: list, window: int) -> str:
    """
    格式化最近 N 局结果为二进制字符串
    与 master 版本 _format_recent_binary 一致
    """
    if len(history) < window:
        window = len(history)
    if window <= 0:
        return ""
    recent = history[-window:]
    return "".join(str(x) for x in recent)


def _get_current_streak(history: list):
    """返回当前连串长度与方向（与 master 一致）。"""
    if not history:
        return 0, "大"
    tail = history[-1]
    streak = 1
    for value in reversed(history[:-1]):
        if value == tail:
            streak += 1
        else:
            break
    return streak, ("大" if tail == 1 else "小")


def _compact_reason_text(reason: str, max_len: int = 96) -> str:
    """压缩风控原因，避免在通知里输出超长分析（与 master 一致）。"""
    if not reason:
        return "策略风控触发"
    first_line = str(reason).splitlines()[0].strip()
    return first_line if len(first_line) <= max_len else first_line[: max_len - 1] + "…"


def generate_mobile_bet_report(
    history: list,
    direction: str,
    amount: int,
    sequence_count: int,
    bet_id: str = ""
) -> str:
    """生成简短押注执行报告（与 master 一致）。"""
    streak_len, streak_side = _get_current_streak(history)
    return (
        "🎯 押注执行\n"
        f"方向: {direction}\n"
        f"金额: {format_number(amount)}\n"
        f"连押: 第 {sequence_count} 次\n"
        f"当前连{streak_side}: {streak_len}"
    )


def generate_mobile_pause_report(
    history: list,
    pause_reason: str,
    confidence: float = None,
    entropy: float = None
) -> str:
    """生成简短风控暂停报告（与 master 一致）。"""
    streak_len, streak_side = _get_current_streak(history)
    reason_text = _compact_reason_text(pause_reason)
    w5 = _format_recent_binary(history, 5)
    w10 = _format_recent_binary(history, 10)
    w40 = _format_recent_binary(history, 40)

    lines = [
        "⛔ 风控暂停",
        f"原因: {reason_text}",
    ]
    if confidence is not None:
        lines.append(f"置信度: {confidence}%")
    if entropy is not None:
        lines.append(f"熵值: {entropy:.2f}")
    lines.extend(
        [
            f"近5局: {w5}",
            f"近10局: {w10}",
            f"近40局: {w40}",
            f"当前连{streak_side}: {streak_len}",
            "动作: 暂停下注，继续观察",
        ]
    )
    return "\n".join(lines)


async def process_settle(client, event, user_ctx: UserContext, global_config: dict):
    """处理押注结算 - 与master版本zq_settle完全一致，包括连输告警、回补播报、资金安全等"""
    state = user_ctx.state
    rt = state.runtime
    
    text = event.message.message
    
    try:
        import re
        match = re.search(r"已结算: 结果为 (\d+) (大|小)", text)
        if not match:
            log_event(logging.DEBUG, 'settle', '未匹配到结算消息', user_id=user_ctx.user_id, data='action=跳过')
            return
        
        result_num = int(match.group(1))
        result_type = match.group(2)
        is_big = (result_type == "大")
        result = 1 if is_big else 0

        if rt.get("open_ydx", False):
            monitor_targets = _iter_targets(user_ctx.config.groups.get("monitor", []))
            for monitor_target in monitor_targets:
                try:
                    await client.send_message(monitor_target, "/ydx")
                except Exception as e:
                    log_event(
                        logging.WARNING,
                        'settle',
                        '发送/ydx失败',
                        user_id=user_ctx.user_id,
                        data=f'target={monitor_target}, error={str(e)}'
                    )
        
        # 更新历史记录
        state.history.append(result)
        state.history = state.history[-2000:]
        
        log_event(logging.INFO, 'settle', '更新历史记录', 
                  user_id=user_ctx.user_id, data=f'result={result}, history_len={len(state.history)}')
        
        # 实时监控：每10局计算准确率
        if len(state.history) >= 10 and len(state.history) % 10 == 0:
            recent_acc = sum(1 for h, p in zip(state.history[-10:], state.predictions[-10:]) if h == p) / 10 * 100
            log_event(logging.INFO, 'model_monitor', '最近10局准确率', 
                      user_id=user_ctx.user_id, data=f'accuracy={recent_acc:.2f}%')
        
        result_text = None
        direction = None
        profit = 0
        result_amount = 0
        lose_end_payload = None
        
        # 资金安全闸门
        if not is_fund_available(user_ctx):
            if hasattr(user_ctx, 'dashboard_message') and user_ctx.dashboard_message:
                await cleanup_message(client, user_ctx.dashboard_message)
            display_fund = max(0, rt.get("gambling_fund", 0))
            mes = f"**菠菜资金耗尽，已暂停押注**\n当前剩余：{display_fund / 10000:.2f} 万\n请使用 `gf [金额]` 恢复"
            log_event(logging.WARNING, 'settle', '资金耗尽暂停', 
                      user_id=user_ctx.user_id, data=f'fund={rt.get("gambling_fund", 0)}')
            await send_to_admin(client, mes, user_ctx, global_config)
            rt["bet"] = False
            rt["bet_on"] = False
        else:
            if rt.get("bet", False):
                try:
                    prediction = int(rt.get("bet_type", -1))
                    win = (is_big and prediction == 1) or (not is_big and prediction == 0)
                    bet_amount = int(rt.get("bet_amount", 500))
                    profit = int(bet_amount * 0.99) if win else -bet_amount
                    settle_round, settle_seq = get_settle_position(state, rt)
                    
                    # 记录连输状态用于回补播报
                    old_lose_count = rt.get("lose_count", 0)
                    
                    direction = "大" if prediction == 1 else "小"
                    result_text = "赢" if win else "输"
                    state.bet_type_history.append(prediction)
                    rt["gambling_fund"] = rt.get("gambling_fund", 0) + profit
                    rt["earnings"] = rt.get("earnings", 0) + profit
                    rt["period_profit"] = rt.get("period_profit", 0) + profit
                    rt["win_total"] = rt.get("win_total", 0) + (1 if win else 0)
                    rt["win_count"] = rt.get("win_count", 0) + 1 if win else 0
                    rt["lose_count"] = rt.get("lose_count", 0) + 1 if not win else 0
                    rt["status"] = 1 if win else 0
                    
                    # 连输逻辑处理
                    if not win:
                        # 如果连输刚开始（第1次），记录起始信息
                        if rt.get("lose_count", 0) == 1:
                            rt["lose_start_info"] = {
                                "round": settle_round,
                                "seq": settle_seq,
                                "fund": rt.get("gambling_fund", 0) + bet_amount
                            }
                        
                        # 达到告警阈值，标记为待发送状态
                        warning_lose_count = rt.get("warning_lose_count", 3)
                        if rt.get("lose_count", 0) >= warning_lose_count:
                            rt["lose_notify_pending"] = True
                            log_event(logging.INFO, 'settle', '达到连输告警阈值', 
                                      user_id=user_ctx.user_id, data=f'lose_count={rt.get("lose_count", 0)}')
                            
                            # --- 连输实时告警逻辑 (Real-time Lose Streak Warning) ---
                            try:
                                total_losses = bet_amount
                                if rt.get("lose_count", 0) > 1 and state.bet_sequence_log:
                                    start_idx = max(0, len(state.bet_sequence_log) - rt.get("lose_count", 0) + 1)
                                    for entry in state.bet_sequence_log[start_idx:]:
                                        entry_profit = entry.get('profit')
                                        if entry_profit is not None and isinstance(entry_profit, (int, float)) and entry_profit < 0:
                                            total_losses += abs(entry_profit)

                                date_str = datetime.now().strftime("%m月%d日")
                                bet_dir_str = "大" if prediction == 1 else "小"
                                warn_msg = (
                                    f"⚠️ {rt.get('lose_count', 0)} 连输告警 ⚠️\n"
                                    f"🔢 {date_str} 第 {settle_round} 轮第 {settle_seq} 次：\n"
                                    f"😀 连续押注：{rt.get('bet_sequence_count', 0)} 次\n"
                                    f"⚡️ 押注方向：{bet_dir_str}\n"
                                    f"💵 押注本金：{format_number(bet_amount)}\n"
                                    f"💰 累计损失：{format_number(total_losses)}\n"
                                    f"💰 账户余额：{rt.get('account_balance', 0) / 10000:.2f} 万\n"
                                    f"💰 菠菜余额：{rt.get('gambling_fund', 0) / 10000:.2f} 万"
                                )

                                log_event(
                                    logging.WARNING,
                                    'settle',
                                    '触发连输实时告警',
                                    user_id=user_ctx.user_id,
                                    data=f'lose_count={rt.get("lose_count", 0)}, total_loss={total_losses}'
                                )
                                await send_message_v2(
                                    client,
                                    "lose_streak",
                                    warn_msg,
                                    user_ctx,
                                    global_config,
                                    title=f"菠菜机器人 {user_ctx.config.name} 连输告警",
                                    desp=warn_msg
                                )
                            except Exception as e:
                                log_event(logging.ERROR, 'settle', '连输实时告警构建失败', user_id=user_ctx.user_id, data=str(e))
                                fallback_msg = (
                                    f"⚠️ 连输告警(数据异常) ⚠️\n"
                                    f"连输：{rt.get('lose_count', 0)} 次\n"
                                    f"错误：{str(e)[:50]}"
                                )
                                await send_message_v2(client, "lose_streak", fallback_msg, user_ctx, global_config)
                    
                    # 连输终止处理（赢了）
                    if win and rt.get("lose_notify_pending", False):
                        try:
                            lose_start_info = rt.get("lose_start_info", {})
                            total_profit = rt.get("gambling_fund", 0) - lose_start_info.get("fund", rt.get("gambling_fund", 0))
                            
                            start_round = lose_start_info.get("round", "?")
                            start_seq = lose_start_info.get("seq", "?")
                            end_round = settle_round
                            end_seq = settle_seq
                            continuous_count = max(int(rt.get("bet_sequence_count", 0)), old_lose_count + 1)
                            lose_end_payload = {
                                "start_round": start_round,
                                "start_seq": start_seq,
                                "end_round": end_round,
                                "end_seq": end_seq,
                                "lose_count": old_lose_count,
                                "continuous_count": continuous_count,
                                "total_profit": total_profit,
                            }
                        except Exception as e:
                            log_event(logging.ERROR, 'settle', '连输终止通知异常', 
                                      user_id=user_ctx.user_id, data=str(e))
                        
                        # 重置状态
                        rt["lose_notify_pending"] = False
                        rt["lose_start_info"] = {}
                    
                    log_event(logging.INFO, 'settle', '结算结果', 
                              user_id=user_ctx.user_id, data=f'result={result_text}, profit={profit}, fund={rt.get("gambling_fund", 0)}')
                    
                    user_ctx.save_state()
                    
                    # 更新押注日志（存储在 state 中，不是 rt 中）
                    if state.bet_sequence_log:
                        state.bet_sequence_log[-1]["result"] = result_text
                        state.bet_sequence_log[-1]["profit"] = profit
                    
                    result_amount = format_number(int(bet_amount * 0.99) if win else bet_amount)
                    last_bet_id = state.bet_sequence_log[-1].get("bet_id", "") if state.bet_sequence_log else ""
                    bet_id = format_bet_id(last_bet_id) if last_bet_id else f"{datetime.now().strftime('%m月%d日')}第 {rt.get('current_round', 1)} 轮第 {rt.get('current_bet_seq', 1)} 次"
                    
                    mes = f"🔢 **{bet_id}押注结果：**\n"
                    mes += f"😀 连续押注：{rt.get('bet_sequence_count', 0)} 次\n"
                    mes += f"⚡ 押注方向：{direction}\n"
                    mes += f"💵 押注本金：{format_number(bet_amount)}\n"
                    mes += f"📉 输赢结果：{result_text} {result_amount}\n"
                    mes += f"🎲 开奖结果：{result_type}\n"
                    mes += f"🤖 预测依据：{rt.get('last_predict_info', 'N/A')}"
                    
                    log_event(logging.INFO, 'settle', '发送结算通知', 
                              user_id=user_ctx.user_id, data=f'bet_id={bet_id}')
                    await send_to_admin(client, mes, user_ctx, global_config)
                    
                    if win or rt.get("lose_count", 0) >= rt.get("lose_stop", 13):
                        rt["bet_sequence_count"] = 0
                        rt["bet_amount"] = int(rt.get("initial_amount", 500))
                        
                except Exception as e:
                    log_event(logging.ERROR, 'settle', '结算失败', 
                              user_id=user_ctx.user_id, data=str(e))
                    await send_to_admin(client, f"结算出错: {e}", user_ctx, global_config)
        
        # 每5局保存一次状态
        if len(state.history) % 5 == 0:
            user_ctx.save_state()
            log_event(logging.DEBUG, 'settle', '定期保存状态', 
                      user_id=user_ctx.user_id, data=f'history_len={len(state.history)}')
        
        # 炸和盈利通知
        explode_count = rt.get("explode_count", 0)
        explode = rt.get("explode", 5)
        period_profit = rt.get("period_profit", 0)
        profit_target = rt.get("profit", 1000000)
        
        if explode_count >= explode or period_profit >= profit_target:
            if rt.get("flag", True):
                rt["flag"] = False
                notify_type = "explode" if explode_count >= explode else "profit"
                log_event(logging.INFO, 'settle', '触发通知', user_id=user_ctx.user_id, data=f'type={notify_type}')
                if notify_type == "profit":
                    date_str = datetime.now().strftime("%m月%d日")
                    current_round_str = f"{datetime.now().strftime('%Y%m%d')}_{rt.get('current_round', 1)}"
                    round_bet_count = sum(
                        1 for entry in state.bet_sequence_log
                        if str(entry.get("bet_id", "")).startswith(current_round_str)
                    )
                    win_msg = (
                        f"😄📈 {date_str}第 {rt.get('current_round', 1)} 轮 赢了\n"
                        f"收益：{period_profit / 10000:.2f} 万\n"
                        f"共下注：{round_bet_count} 次"
                    )
                    await send_message_v2(client, "win", win_msg, user_ctx, global_config)
                else:
                    mes = f"**💥 本轮炸了**\n收益：{period_profit / 10000:.2f} 万"
                    await send_message_v2(client, "explode", mes, user_ctx, global_config)
                
                rt["stop_count"] = rt.get("stop", 3) if notify_type == "explode" else rt.get("profit_stop", 5)
                rt["bet"] = False
                rt["bet_sequence_count"] = 0
                mes = f"**暂停押注**\n原因：{'被炸' if notify_type == 'explode' else '盈利达成'}\n剩余：{rt['stop_count']} 局"
                log_event(logging.INFO, 'settle', '暂停押注', 
                          user_id=user_ctx.user_id, data=f'type={notify_type}, stop_count={rt["stop_count"]}')
                await send_to_admin(client, mes, user_ctx, global_config)
            
            if rt.get("stop_count", 0) > 1:
                rt["stop_count"] = rt.get("stop_count", 0) - 1
                rt["bet"] = False
                rt["bet_on"] = False
                rt["mode_stop"] = False
                mes = f"**暂停押注**\n剩余：{rt['stop_count']} 局"
                log_event(logging.INFO, 'settle', '暂停中', 
                          user_id=user_ctx.user_id, data=f'stop_count={rt["stop_count"]}')
                await send_to_admin(client, mes, user_ctx, global_config)
            else:
                if period_profit >= profit_target:
                    rt["current_round"] = rt.get("current_round", 1) + 1
                    rt["current_bet_seq"] = 1
                rt["explode_count"] = 0
                rt["period_profit"] = 0
                rt["bet_sequence_count"] = 0
                rt["lose_count"] = 0
                rt["win_count"] = 0
                rt["bet_amount"] = int(rt.get("initial_amount", 500))
                rt["mode_stop"] = True
                rt["flag"] = True
                if rt.get("manual_pause", False):
                    rt["bet_on"] = False
                    rt["bet"] = False
                    mes = "**暂停结束**\n检测到手动暂停，保持暂停状态"
                else:
                    rt["bet_on"] = True
                    rt["bet"] = True
                    mes = "**恢复押注**\n暂停已结束，新轮次开始"
                log_event(logging.INFO, 'settle', '恢复押注', 
                          user_id=user_ctx.user_id, data=f'round={rt.get("current_round", 1)}, bet_amount={rt.get("bet_amount", 500)}')
                await send_to_admin(client, mes, user_ctx, global_config)
        
        # 历史记录统计通知
        if hasattr(user_ctx, 'dashboard_message') and user_ctx.dashboard_message:
            await cleanup_message(client, user_ctx.dashboard_message)
        
        if len(state.history) > 5 and len(state.history) % 10 == 0:
            if hasattr(user_ctx, 'stats_message') and user_ctx.stats_message:
                await cleanup_message(client, user_ctx.stats_message)
            
            windows = [1000, 500, 200, 100]
            stats = {"连大": [], "连小": [], "连输": []}
            all_ns = set()
            
            for window in windows:
                history_window = state.history[-window:]
                bet_types = state.bet_type_history[-len(history_window):] if len(state.bet_type_history) > 0 else []
                result_counts = count_consecutive(history_window)
                bet_sequence_log = state.bet_sequence_log[-window:]
                lose_streaks = count_lose_streaks(bet_sequence_log)
                
                stats["连大"].append(result_counts["大"])
                stats["连小"].append(result_counts["小"])
                stats["连输"].append(lose_streaks)
                
                all_ns.update(result_counts["大"].keys())
                all_ns.update(result_counts["小"].keys())
                all_ns.update(lose_streaks.keys())
            
            mes = "```"
            for category in ["连大", "连小", "连输"]:
                mes += "最近局数“连大、连小、连输”统计\n\n"
                mes += f"{category}\n"
                mes += "================================\n"
                mes += "类别 | 1000|  500  |200 | 100|\n"
                mes += "--------------------------------\n"
                sorted_ns = sorted(all_ns, reverse=True)
                for n in sorted_ns:
                    if any(n in stats[category][i] for i in range(len(windows))):
                        mes += f" {str(n).center(2)}  |"
                        for i in range(len(windows)):
                            count = stats[category][i].get(n, 0)
                            value = str(count) if count > 0 else "-"
                            mes += f" {value.center(3)} |"
                        mes += "\n"
                mes += "\n"
            mes += "```"
            
            log_event(logging.INFO, 'settle', '发送历史记录统计通知', user_id=user_ctx.user_id)
            user_ctx.stats_message = await send_to_admin(client, mes, user_ctx, global_config)
        
        # 获取账户余额
        try:
            balance = await fetch_balance(user_ctx)
            rt["account_balance"] = balance
            rt["balance_status"] = "success"
        except Exception as e:
            log_event(logging.WARNING, 'settle', '获取账户余额失败，使用默认值', 
                      user_id=user_ctx.user_id, data=str(e))
            rt["balance_status"] = "network_error"

        # 连输终止播报延后到结算数据写入后，避免与结算通知时序错位
        if lose_end_payload:
            date_str = datetime.now().strftime("%m月%d日")
            start_round = lose_end_payload.get("start_round", "?")
            start_seq = lose_end_payload.get("start_seq", "?")
            end_round = lose_end_payload.get("end_round", "?")
            end_seq = lose_end_payload.get("end_seq", "?")
            if str(start_round) == str(end_round):
                range_text = f"{date_str} 第 {start_round} 轮第 {start_seq} 次 至 第 {end_seq} 次"
            else:
                range_text = f"{date_str} 第 {start_round} 轮第 {start_seq} 次 至 第 {end_round} 轮第 {end_seq} 次"

            rec_msg = (
                f"✅ 连输已终止！✅\n"
                f"🔢 {range_text}\n"
                f"😀 连续押注：{lose_end_payload.get('continuous_count', lose_end_payload.get('lose_count', 0) + 1)} 次\n"
                f"⚠️本局连输： {lose_end_payload.get('lose_count', 0)} 次\n"
                f"💰 本局盈利： {format_number(lose_end_payload.get('total_profit', 0))}\n"
                f"💰 账户余额：{rt.get('account_balance', 0) / 10000:.2f} 万\n"
                f"💰 菠菜资金剩余：{rt.get('gambling_fund', 0) / 10000:.2f} 万"
            )
            await send_message_v2(client, "lose_end", rec_msg, user_ctx, global_config)
            log_event(
                logging.INFO,
                'settle',
                '触发连输终止通知',
                user_id=user_ctx.user_id,
                data=(
                    f"lose_count={lose_end_payload.get('lose_count', 0)}, "
                    f"start={start_round}-{start_seq}, end={end_round}-{end_seq}, "
                    f"total_profit={lose_end_payload.get('total_profit', 0)}"
                ),
            )
        
        # 发送仪表盘
        dashboard = format_dashboard(user_ctx)
        log_event(logging.INFO, 'settle', '发送仪表盘', user_id=user_ctx.user_id)
        user_ctx.dashboard_message = await send_to_admin(client, dashboard, user_ctx, global_config)
        
        # 保存状态
        user_ctx.save_state()
        
    except Exception as e:
        log_event(logging.ERROR, 'settle', '结算处理失败', 
                  user_id=user_ctx.user_id, data=str(e))


# 用户命令处理
async def delete_later(client, chat_id, message_id, delay=10):
    """延迟指定秒数后删除消息。"""
    await asyncio.sleep(delay)
    try:
        await client.delete_messages(chat_id, message_id)
    except Exception:
        pass


async def handle_model_command_multiuser(event, args, user_ctx: UserContext, global_config: dict):
    """处理 model 命令 - 与master版本handle_model_command一致"""
    rt = user_ctx.state.runtime
    sub_cmd = args[0] if args else "list"
    
    # 兼容 "model id list" 和 "model id XX"
    if sub_cmd == "id":
        if len(args) < 2:
            sub_cmd = "list"
        elif args[1] == "list":
            sub_cmd = "list"
        else:
            sub_cmd = "select"
            args = ["select", args[1]]

    if sub_cmd == "list":
        models = user_ctx.config.ai.get("models", {})
        msg = "**可用模型列表**\n"
        idx = 1
        current_model_id = rt.get("current_model_id", "")
        
        for k, m in models.items():
            if m.get("enabled", True):
                status = "✅"
                current = "👈 当前" if m.get('model_id') == current_model_id else ""
                msg += f"{idx}. `{m.get('model_id', 'unknown')}` {status} {current}\n"
                idx += 1
        await event.reply(msg)
        
    elif sub_cmd in ["select", "use", "switch"]:
        if len(args) < 2:
            await event.reply("请指定模型ID或编号，例如: `model select 1` 或 `model select qwen3-coder-plus`")
            return
            
        target_id = args[1]
        models = user_ctx.config.ai.get("models", {})
        
        # 支持数字编号选择
        if target_id.isdigit():
            idx = int(target_id)
            enabled_models = [m for m in models.values() if m.get("enabled", True)]
            if 1 <= idx <= len(enabled_models):
                target_id = enabled_models[idx-1].get('model_id', '')
            else:
                await event.reply(f"❌ 编号 {idx} 无效")
                return
        
        # 验证模型是否存在
        model_exists = any(m.get('model_id') == target_id for m in models.values() if m.get("enabled"))
        if not model_exists:
            await event.reply(f"❌ 模型 `{target_id}` 不存在或未启用")
            return
            
        await event.reply(f"🔄 正在切换模型 `{target_id}`...")
        
        # 切换模型
        rt["current_model_id"] = target_id
        user_ctx.save_state()
        
        success_msg = (
            f"✅ **模型切换成功**\n"
            f"🤖 **当前模型**: `{target_id}`\n"
            f"🔗 **连接状态**: 🟢 正常\n"
            f"🧠 **算法模式**: V10 (已激活)"
        )
        await event.reply(success_msg)
        log_event(logging.INFO, 'model', '切换模型', user_id=user_ctx.user_id, model=target_id)
            
    elif sub_cmd == "reload":
        await event.reply("🔄 重新加载模型配置...")
        log_event(logging.INFO, 'model', '重新加载模型', user_id=user_ctx.user_id)
        await event.reply("✅ 模型配置已重新加载")
    else:
        await event.reply("未知命令。用法:\n`model list`\n`model select <id>`\n`model reload`")


async def process_user_command(client, event, user_ctx: UserContext, global_config: dict):
    """处理用户命令 - 与master版本完全一致"""
    state = user_ctx.state
    rt = state.runtime
    presets = user_ctx.presets
    
    text = event.raw_text.strip()
    my = text.split(" ")
    cmd = my[0].lower()
    
    log_event(logging.INFO, 'user_cmd', '处理用户命令', user_id=user_ctx.user_id, data=text[:50])
    
    try:
        # ========== help命令 - 与master版本完全一致 ==========
        if cmd == "help":
            mes = """**️ 命令列表 (Commands)**

**基础控制**
- `open` : 开启押注
- `off`  : 停止押注
- `pause` : 仅暂停当前账号押注（不影响其他账号）
- `resume` : 恢复当前账号押注
- `st [预设名]` : 启动预设并自动测算 (例: `st yc`)

**参数设置**
- `gf [金额]` : 设置本金 (例: `gf 1000000`)
- `set [炸] [赢] [停] [盈停]` : 设置风控参数
  (例: `set 5 1000000 3 5` -> 炸5次, 赢100w, 停3局, 盈停5局)
- `warn [次数]` : 设置连输告警阈值 (例: `warn 2`)
- `wlc [次数]` : `warn` 的简写命令

**策略调整**
- `model [list|select|reload]` : 模型管理 (例: `model select 1`)
- `ms [模式]` : 切换模式 (0:反投, 1:预测, 2:追投)

**测算功能**
- `yc [预设名]` : 测算预设策略盈利 (例: `yc yc05`)
- `yc [参数...]` : 自定义参数测算 (例: `yc 1 13 3 2.1 2.1 2.05 500`)

**数据管理**
- `res tj` : 重置统计数据
- `res bet` : 重置押注策略
- `explain` : 查看AI决策解释
- `stats` : 查看连大、连小、连输统计
- `xx` : 清理配置群中“我发送的消息”

**发布更新**
- `ver` : 查看版本概览（最近3个Tag + 最近3个Commit）
- `update [版本|提交]` : 更新到指定版本(留空默认最新)
- `reback [版本|提交]` : 回退到指定版本
- `restart` : 重启当前进程

**预设管理**
- `ys [名] ...` : 保存预设
- `yss` : 查看所有预设
- `yss dl [名]` : 删除预设

**多用户管理**
- `users` : 查看当前用户状态
- `status` : 查看仪表盘
"""
            log_event(logging.INFO, 'user_cmd', '显示帮助', user_id=user_ctx.user_id)
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 60))
            return
        
        # open - 开启押注 - 与master一致
        if cmd == "open":
            rt["switch"] = True
            rt["open_ydx"] = True
            rt["bet"] = False
            rt["bet_on"] = True
            rt["mode_stop"] = True
            rt["manual_pause"] = False
            user_ctx.save_state()
            mes = "押注已启动"
            message = await send_to_admin(client, mes, user_ctx, global_config)
            log_event(logging.INFO, 'user_cmd', '开启押注', user_id=user_ctx.user_id)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return
        
        # off - 停止押注 - 与master一致
        if cmd == "off":
            rt["switch"] = False
            rt["bet"] = False
            rt["open_ydx"] = False
            rt["bet_on"] = False
            rt["manual_pause"] = False
            user_ctx.save_state()
            mes = "押注已停止"
            message = await send_to_admin(client, mes, user_ctx, global_config)
            log_event(logging.INFO, 'user_cmd', '停止押注', user_id=user_ctx.user_id)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return

        if cmd == "xx":
            target_groups = []
            target_groups.extend(_iter_targets(user_ctx.config.groups.get("zq_group", [])))
            target_groups.extend(_iter_targets(user_ctx.config.groups.get("monitor", [])))

            # 去重并保持顺序
            unique_groups = []
            seen = set()
            for gid in target_groups:
                key = str(gid)
                if key in seen:
                    continue
                seen.add(key)
                unique_groups.append(gid)

            if not unique_groups:
                message = await send_to_admin(client, "未配置可清理的群组（zq_group/monitor）", user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
                return

            deleted_total = 0
            failed_groups = []
            scanned_groups = 0

            for gid in unique_groups:
                try:
                    msg_ids = [msg.id async for msg in client.iter_messages(gid, from_user="me", limit=500)]
                    scanned_groups += 1
                    if msg_ids:
                        await client.delete_messages(gid, msg_ids)
                        deleted_total += len(msg_ids)
                except Exception as e:
                    failed_groups.append(f"{gid}: {str(e)[:40]}")

            mes = (
                "群组消息已清理\n"
                f"扫描群组：{scanned_groups}\n"
                f"删除消息：{deleted_total}"
            )
            if failed_groups:
                mes += "\n失败群组：\n" + "\n".join(f"- {item}" for item in failed_groups[:5])

            log_event(
                logging.INFO,
                'user_cmd',
                '执行xx清理',
                user_id=user_ctx.user_id,
                groups=scanned_groups,
                deleted=deleted_total,
                failed=len(failed_groups),
            )
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 3))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            return
        
        # pause/resume - 暂停/恢复押注（新增，master没有但有用）
        if cmd in ("pause", "暂停"):
            if rt.get("manual_pause", False):
                await send_to_admin(client, "⏸ 当前账号已是暂停状态", user_ctx, global_config)
                return
            rt["bet_on"] = False
            rt["bet"] = False
            rt["mode_stop"] = True
            rt["manual_pause"] = True
            user_ctx.save_state()
            mes = "⏸ 已暂停当前账号押注"
            await send_to_admin(client, mes, user_ctx, global_config)
            log_event(logging.INFO, 'user_cmd', '暂停押注', user_id=user_ctx.user_id)
            return
        
        if cmd in ("resume", "恢复"):
            if not rt.get("switch", True):
                await send_to_admin(client, "当前为 off 状态，请先执行 `open`", user_ctx, global_config)
                return
            rt["bet_on"] = True
            rt["mode_stop"] = True
            rt["manual_pause"] = False
            user_ctx.save_state()
            mes = "▶️ 已恢复当前账号押注"
            await send_to_admin(client, mes, user_ctx, global_config)
            log_event(logging.INFO, 'user_cmd', '恢复押注', user_id=user_ctx.user_id)
            return
        
        # st - 启动预设 - 与master一致
        if cmd == "st" and len(my) > 1:
            preset_name = my[1]
            if preset_name in presets:
                preset = presets[preset_name]
                rt["continuous"] = int(preset[0])
                rt["lose_stop"] = int(preset[1])
                rt["lose_once"] = float(preset[2])
                rt["lose_twice"] = float(preset[3])
                rt["lose_three"] = float(preset[4])
                rt["lose_four"] = float(preset[5])
                rt["initial_amount"] = int(preset[6])
                rt["current_preset_name"] = preset_name
                rt["bet_amount"] = int(preset[6])
                rt["bet"] = False  # 修复：st命令不应直接设置bet=True
                user_ctx.save_state()
                
                mes = f"预设启动成功: {preset_name} ({preset[0]} {preset[1]} {preset[2]} {preset[3]} {preset[4]} {preset[5]} {preset[6]})"
                log_event(logging.INFO, 'user_cmd', '启动预设', user_id=user_ctx.user_id, preset=preset_name)
                message = await send_to_admin(client, mes, user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
                await yc_command_handler_multiuser(
                    client,
                    event,
                    [preset_name],
                    user_ctx,
                    global_config,
                    auto_trigger=True,
                )
            else:
                await send_to_admin(client, f"预设不存在: {preset_name}", user_ctx, global_config)
            return
        
        # stats - 查看连大、连小、连输统计
        if cmd == "stats":
            if len(state.history) < 10:
                await send_to_admin(client, "历史数据不足，无法生成统计", user_ctx, global_config)
                return
            
            windows = [1000, 500, 200, 100]
            stats = {"连大": [], "连小": [], "连输": []}
            all_ns = set()
            
            for window in windows:
                history_window = state.history[-window:]
                result_counts = count_consecutive(history_window)
                bet_sequence_log = state.bet_sequence_log[-window:]
                lose_streaks = count_lose_streaks(bet_sequence_log)
                
                stats["连大"].append(result_counts["大"])
                stats["连小"].append(result_counts["小"])
                stats["连输"].append(lose_streaks)
                
                all_ns.update(result_counts["大"].keys())
                all_ns.update(result_counts["小"].keys())
                all_ns.update(lose_streaks.keys())
            
            mes = "```\n最近局数“连大、连小、连输”统计\n\n"
            for category in ["连大", "连小", "连输"]:
                mes += f"{category}\n"
                mes += "================================\n"
                mes += "类别 | 1000|  500  |200 | 100|\n"
                mes += "--------------------------------\n"
                sorted_ns = sorted(all_ns, reverse=True)
                for n in sorted_ns:
                    if any(n in stats[category][i] for i in range(len(windows))):
                        mes += f" {str(n).center(2)}  |"
                        for i in range(len(windows)):
                            count = stats[category][i].get(n, 0)
                            value = str(count) if count > 0 else "-"
                            mes += f" {value.center(3)} |"
                        mes += "\n"
                mes += "\n"
            mes += "```"
            
            log_event(logging.INFO, 'user_cmd', '查看统计', user_id=user_ctx.user_id)
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 30))
            return
        
        # status - 查看仪表盘 - 与master一致
        if cmd == "status":
            dashboard = format_dashboard(user_ctx)
            message = await send_to_admin(client, dashboard, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 60))
            return
        
        # ========== 参数设置命令 ==========
        # gf - 设置资金 - 与master一致
        if cmd == "gf":
            old_fund = rt.get("gambling_fund", 0)
            if len(my) == 1:
                rt["gambling_fund"] = rt.get("gambling_fund", 2000000)
                mes = f"菠菜资金已重置为 {rt['gambling_fund'] / 10000:.2f} 万"
            elif len(my) == 2:
                try:
                    new_fund = int(my[1])
                    if new_fund < 0:
                        mes = "菠菜资金不能设置为负数"
                    else:
                        account_balance = rt.get("account_balance", 0)
                        if new_fund > account_balance:
                            new_fund = account_balance
                            mes = f"设置的资金超过账户余额，已调整为 {new_fund / 10000:.2f} 万"
                        else:
                            mes = f"菠菜资金已设置为 {new_fund / 10000:.2f} 万"
                        rt["gambling_fund"] = new_fund
                except ValueError:
                    mes = "无效的金额格式，请输入整数"
            else:
                mes = "gf 命令格式错误：gf 或 gf [金额]"
            
            log_event(logging.INFO, 'user_cmd', '设置资金', user_id=user_ctx.user_id, mes=mes)
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            
            if rt.get("gambling_fund", 0) != old_fund:
                log_event(logging.INFO, 'user_cmd', '资金变更', user_id=user_ctx.user_id, 
                         old=old_fund, new=rt.get("gambling_fund", 0))
                await check_bet_status(client, user_ctx, global_config)
            return
        
        # set - 设置风控参数 - 与master一致
        if cmd == "set" and len(my) >= 5:
            try:
                rt["explode"] = int(my[1])
                rt["profit"] = int(my[2])
                rt["stop"] = int(my[3])
                rt["profit_stop"] = int(my[4])
                if len(my) > 5:
                    rt["stop_count"] = int(my[5])
                user_ctx.save_state()
                mes = f"设置成功: 炸{rt['explode']}次, 盈利{rt['profit']/10000:.2f}万, 暂停{rt['stop']}局, 盈停{rt['profit_stop']}局"
                log_event(logging.INFO, 'user_cmd', '设置参数', user_id=user_ctx.user_id,
                         explode=rt['explode'], profit=rt['profit'], stop=rt['stop'], profit_stop=rt['profit_stop'])
                message = await send_to_admin(client, mes, user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            except ValueError:
                await send_to_admin(client, "参数格式错误，请输入整数", user_ctx, global_config)
            return

        # warn/wlc - 设置连输告警阈值 - 与master一致
        if cmd in ("warn", "wlc"):
            if len(my) > 1:
                try:
                    warning_count = int(my[1])
                    if warning_count < 1:
                        raise ValueError
                    rt["warning_lose_count"] = warning_count
                    user_ctx.save_state()
                    mes = f"连输告警阈值已设置为: {warning_count} 次"
                    log_event(logging.INFO, 'user_cmd', '设置连输告警阈值', user_id=user_ctx.user_id, warning_lose_count=warning_count)
                except ValueError:
                    mes = "❌ 参数错误：阈值必须是 >= 1 的整数。用法: warn <次数>"
            else:
                mes = (
                    f"当前连输告警阈值: {rt.get('warning_lose_count', 3)} 次\n"
                    "用法: warn <次数> 或 wlc <次数>"
                )
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            return
        
        # model - 模型管理 - 使用与master一致的handle_model_command
        if cmd == "model":
            if len(my) == 2 and my[1].lower().startswith("v"):
                mes = "当前算法固定为 V10，无需切换。请使用 `model select <id>` 切换模型。"
                await event.reply(mes)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                return
            await handle_model_command_multiuser(event, my[1:], user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return

        # ========== 发布更新命令 ==========
        if cmd in ("ver", "version"):
            result = await asyncio.to_thread(list_version_catalog, None, 3)
            if not result.get("success"):
                mes = f"❌ 版本查询失败：{result.get('error', 'unknown')}"
            else:
                current = result.get("current", {})
                current_short = current.get("short_commit", "unknown") or "unknown"
                current_tag_exact = current.get("current_tag", "") or ""
                nearest_tag = current.get("nearest_tag", "") or ""
                if current_tag_exact:
                    current_tag_display = current_tag_exact
                elif nearest_tag:
                    current_tag_display = f"无（最近Tag: {nearest_tag}）"
                else:
                    current_tag_display = "无"

                remote_head = result.get("remote_head", {}) or {}
                remote_head_short = remote_head.get("short_commit", "-") or "-"
                remote_head_tag = result.get("remote_head_tag", "") or ""
                pending_tags = result.get("pending_tags", [])
                recent_tags = result.get("recent_tags", []) or []
                recent_commits = result.get("recent_commits", []) or []

                latest_updatable_tag = pending_tags[0] if pending_tags else "无（已是最新）"
                if remote_head_short in {"", "-", "unknown"}:
                    latest_test_commit = "无"
                elif remote_head_short == current_short:
                    latest_test_commit = f"{remote_head_short}（已是当前）"
                elif remote_head_tag:
                    latest_test_commit = f"{remote_head_short}（Tag: {remote_head_tag}）"
                else:
                    latest_test_commit = f"{remote_head_short}（未打 Tag）"

                lines = [
                    "📦 版本信息概览",
                    f"当前版本（Tag）：{current_tag_display}",
                    f"当前提交（Commit）：{current_short}",
                    f"最新可更新 Tag：{latest_updatable_tag}",
                    f"最新可测试 Commit：{latest_test_commit}",
                    "",
                    "⚠️  操作提示：",
                    "- update <Tag版本号|Commit哈希>：更新到指定版本/提交",
                    "- reback <Tag版本号|Commit哈希>：回滚到指定版本/提交",
                    "- restart：重启应用（版本切换后生效）",
                    "",
                    "🔖 最近 3 个正式版本（Tag，新→旧）",
                ]

                if recent_tags:
                    for idx, item in enumerate(recent_tags[:3], 1):
                        tag = item.get("tag", "")
                        date = item.get("date", "") or "-"
                        summary = item.get("summary", "") or "-"
                        lines.append(f"{idx}. {tag} | {date} | {summary}")
                else:
                    lines.append("1. 无")

                lines.extend(["", "💻 最近 3 个开发提交（Commit，新→旧）"])
                if recent_commits:
                    for idx, item in enumerate(recent_commits[:3], 1):
                        short_commit = item.get("short_commit", "") or "-"
                        date = item.get("date", "") or "-"
                        summary = item.get("summary", "") or "-"
                        suffix = "（当前提交）" if short_commit == current_short else ""
                        lines.append(f"{idx}. {short_commit} | {date} | {summary}{suffix}")
                else:
                    lines.append("1. 无")
                mes = "\n".join(lines)

            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 60))
            return

        if cmd in ("update", "up", "upnow", "upref", "upcommit"):
            target_ref = my[1].strip() if len(my) > 1 else ""
            await send_to_admin(client, f"🔄 开始更新：{target_ref or 'latest'}", user_ctx, global_config)
            result = await asyncio.to_thread(update_to_version, None, target_ref)
            if result.get("success"):
                if result.get("no_change"):
                    await send_to_admin(client, f"✅ {result.get('message', '当前已是目标版本')}", user_ctx, global_config)
                else:
                    after = result.get("after", {})
                    resolved = result.get("resolved_target", "") or result.get("target_ref", target_ref or "latest")
                    mes = (
                        "✅ 更新成功\n"
                        f"目标：{resolved}\n"
                        f"当前：{after.get('display_version', after.get('short_commit', 'unknown'))}\n"
                        "请执行 `restart` 重启脚本使新版本生效"
                    )
                    await send_to_admin(client, mes, user_ctx, global_config)
            else:
                blocking_paths = result.get("blocking_paths", [])
                detail = result.get("detail", "")
                mes_lines = [f"❌ 更新失败：{result.get('error', 'unknown')}"]
                if blocking_paths:
                    mes_lines.append("阻塞文件：")
                    mes_lines.extend([f"- {path}" for path in blocking_paths[:10]])
                if detail:
                    mes_lines.append(f"详情：{detail[:200]}")
                await send_to_admin(client, "\n".join(mes_lines), user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return

        if cmd in ("reback", "rollback", "uprollback"):
            target_ref = my[1].strip() if len(my) > 1 else ""
            if not target_ref:
                await send_to_admin(client, "用法：`reback <版本号|commit|branch>`", user_ctx, global_config)
                return

            await send_to_admin(client, f"↩️ 开始回退到：{target_ref}", user_ctx, global_config)
            result = await asyncio.to_thread(reback_to_version, None, target_ref)
            if result.get("success"):
                after = result.get("after", {})
                resolved = result.get("resolved_target", target_ref)
                mes = (
                    "✅ 回退成功\n"
                    f"目标：{resolved}\n"
                    f"当前：{after.get('display_version', after.get('short_commit', 'unknown'))}\n"
                    "请执行 `restart` 重启脚本使回退生效"
                )
                await send_to_admin(client, mes, user_ctx, global_config)
            else:
                mes = f"❌ 回滚失败：{result.get('error', 'unknown')}"
                if result.get("detail"):
                    mes += f"\n详情：{str(result.get('detail'))[:200]}"
                await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return

        if cmd in ("restart", "reboot"):
            await send_to_admin(client, "♻️ 收到重启指令，2 秒后自动重启进程...", user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 3))
            asyncio.create_task(restart_process())
            return
        
        # ========== 数据管理命令 ==========
        # res - 重置命令 - 与master一致
        if cmd == "res":
            if len(my) > 1:
                if my[1] == "tj":
                    # 重置统计
                    rt["win_total"] = 0
                    rt["total"] = 0
                    rt["earnings"] = 0
                    rt["period_profit"] = 0
                    rt["win_count"] = 0
                    rt["lose_count"] = 0
                    rt["bet_sequence_count"] = 0
                    rt["explode_count"] = 0
                    rt["bet_amount"] = int(rt.get("initial_amount", 500))
                    user_ctx.save_state()
                    mes = "统计数据已重置"
                    log_event(logging.INFO, 'user_cmd', '重置统计数据', user_id=user_ctx.user_id, action='completed')
                elif my[1] == "state":
                    # 重置状态
                    state.history = []
                    state.bet_type_history = []
                    rt["win_total"] = 0
                    rt["total"] = 0
                    rt["earnings"] = 0
                    rt["period_profit"] = 0
                    rt["win_count"] = 0
                    rt["lose_count"] = 0
                    rt["bet_sequence_count"] = 0
                    rt["explode_count"] = 0
                    rt["bet_amount"] = int(rt.get("initial_amount", 500))
                    user_ctx.save_state()
                    mes = "状态文件已重置"
                    log_event(logging.INFO, 'user_cmd', '重置状态文件', user_id=user_ctx.user_id, action='completed')
                elif my[1] == "bet":
                    # 重置押注策略
                    rt["win_count"] = 0
                    rt["lose_count"] = 0
                    rt["bet_sequence_count"] = 0
                    rt["explode_count"] = 0
                    rt["bet_amount"] = int(rt.get("initial_amount", 500))
                    rt["bet"] = False
                    rt["bet_on"] = False
                    rt["stop_count"] = 0
                    rt["mark"] = True
                    rt["flag"] = True
                    rt["mode_stop"] = True
                    rt["manual_pause"] = False
                    rt["pause_count"] = 0
                    rt["current_bet_seq"] = 1
                    user_ctx.save_state()
                    mes = f"押注策略已重置: 初始金额={rt.get('initial_amount', 500)}"
                    log_event(logging.INFO, 'user_cmd', '重置押注策略', user_id=user_ctx.user_id, action='completed')
                else:
                    mes = "无效命令，正确格式：res tj 或 res state 或 res bet"
                    log_event(logging.WARNING, 'user_cmd', '无效重置命令', user_id=user_ctx.user_id, cmd=text)
            else:
                mes = "请指定重置类型：res tj / res state / res bet"
            
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            return
        
        # explain - 查看AI决策解释 - 与master一致
        if cmd == "explain":
            last_logic_audit = rt.get("last_logic_audit", "")
            if last_logic_audit:
                log_event(logging.INFO, 'user_cmd', '查看决策解释', user_id=user_ctx.user_id)
                mes = f"🧠 **AI 深度思考归档：**\n```json\n{last_logic_audit}\n```"
                await send_to_admin(client, mes, user_ctx, global_config)
            else:
                await send_to_admin(client, "⚠️ 暂无 AI 决策记录 (需等待 V10 运行至少一次)", user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return
        
        # balance - 查询余额 - 与master一致
        if cmd == "balance":
            try:
                balance = await fetch_balance(user_ctx)
                rt["account_balance"] = balance
                user_ctx.save_state()
                mes = f"账户余额: {format_number(balance)}"
                await send_to_admin(client, mes, user_ctx, global_config)
                log_event(logging.INFO, 'user_cmd', '查询余额', user_id=user_ctx.user_id, balance=balance)
            except Exception as e:
                await send_to_admin(client, f"查询余额失败: {str(e)}", user_ctx, global_config)
            return
        
        # ========== 预设管理命令 ==========
        # ys - 保存预设 - 与master一致
        if cmd == "ys" and len(my) >= 9:
            try:
                preset_name = my[1]
                ys = [int(my[2]), int(my[3]), float(my[4]), float(my[5]), float(my[6]), float(my[7]), int(my[8])]
                presets[preset_name] = ys
                user_ctx.save_presets()
                rt["current_preset_name"] = preset_name
                user_ctx.save_state()
                mes = f"预设保存成功: {preset_name} ({ys[0]} {ys[1]} {ys[2]} {ys[3]} {ys[4]} {ys[5]} {ys[6]})"
                log_event(logging.INFO, 'user_cmd', '保存预设策略', user_id=user_ctx.user_id, preset=preset_name, params=ys)
                message = await send_to_admin(client, mes, user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            except (ValueError, IndexError) as e:
                await send_to_admin(client, f"预设格式错误: {e}", user_ctx, global_config)
            return
        
        # yss - 查看/删除预设 - 与master一致
        if cmd == "yss":
            if len(my) > 2 and my[1] == "dl":
                # 删除预设
                preset_name = my[2]
                if preset_name in presets:
                    del presets[preset_name]
                    user_ctx.save_presets()
                    mes = f"预设删除成功: {preset_name}"
                    log_event(logging.INFO, 'user_cmd', '删除预设', user_id=user_ctx.user_id, preset=preset_name)
                else:
                    mes = "删除失败：预设不存在或格式错误"
                    log_event(logging.WARNING, 'user_cmd', '删除预设失败', user_id=user_ctx.user_id, cmd=text)
                message = await send_to_admin(client, mes, user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            else:
                # 查看所有预设
                if len(presets) > 0:
                    max_key_length = max(len(str(k)) for k in presets.keys())
                    mes = "\n".join(f"'{k.ljust(max_key_length)}': {v}" for k, v in presets.items())
                    log_event(logging.INFO, 'user_cmd', '查看预设', user_id=user_ctx.user_id)
                else:
                    mes = "暂无预设"
                    log_event(logging.INFO, 'user_cmd', '暂无预设', user_id=user_ctx.user_id)
                message = await send_to_admin(client, mes, user_ctx, global_config)
                asyncio.create_task(delete_later(client, event.chat_id, event.id, 60))
                if message:
                    asyncio.create_task(delete_later(client, message.chat_id, message.id, 60))
            return
        
        # ========== 测算命令 ==========
        if cmd == "yc":
            # 测算命令 - 与master一致
            await yc_command_handler_multiuser(client, event, my[1:], user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            return
        
        # ms - 切换模式 - 与master一致
        if cmd == "ms":
            if len(my) > 1:
                try:
                    mode = int(my[1])
                    mode_names = {0: "反投", 1: "预测", 2: "追投"}
                    if mode in mode_names:
                        rt["bet_mode"] = mode
                        user_ctx.save_state()
                        mes = f"模式已切换: {mode_names[mode]} ({mode})"
                        log_event(logging.INFO, 'user_cmd', '切换模式', user_id=user_ctx.user_id, mode=mode)
                    else:
                        mes = "无效模式: 0=反投, 1=预测, 2=追投"
                except ValueError:
                    mes = "模式必须是数字: 0, 1, 或 2"
            else:
                current_mode = rt.get("bet_mode", 1)
                mode_names = {0: "反投", 1: "预测", 2: "追投"}
                mes = f"当前模式: {mode_names.get(current_mode, '未知')} ({current_mode})\n用法: ms [0|1|2]"
            
            message = await send_to_admin(client, mes, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 10))
            return
        
        # ========== 多用户管理命令 ==========
        # users - 查看所有用户
        if cmd == "users":
            # 获取当前用户信息
            user_info = f"👤 当前用户: {user_ctx.config.name} (ID: {user_ctx.user_id})\n"
            user_info += f"💰 菠菜资金: {format_number(rt.get('gambling_fund', 0))}\n"
            user_info += f"📊 状态: {get_bet_status_text(rt)}\n"
            user_info += f"🎯 预设: {rt.get('current_preset_name', '无')}\n"
            user_info += f"🤖 模型: {rt.get('current_model_id', 'default')}\n"
            user_info += f"📈 胜率: {rt.get('win_total', 0)}/{rt.get('total', 0)}"
            message = await send_to_admin(client, user_info, user_ctx, global_config)
            asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
            if message:
                asyncio.create_task(delete_later(client, message.chat_id, message.id, 30))
            return
        
        # 未知命令
        log_event(logging.DEBUG, 'user_cmd', '未知命令', user_id=user_ctx.user_id, data=text[:50])
        message = await send_to_admin(client, f"未知命令: {cmd}\n输入 help 查看帮助", user_ctx, global_config)
        asyncio.create_task(delete_later(client, event.chat_id, event.id, 10))
        
    except Exception as e:
        log_event(logging.ERROR, 'user_cmd', '命令执行出错', user_id=user_ctx.user_id, error=str(e))
        await send_to_admin(client, f"命令执行出错: {e}", user_ctx, global_config)


async def check_bet_status(client, user_ctx: UserContext, global_config: dict):
    """检查押注状态 - 与master版本一致"""
    rt = user_ctx.state.runtime
    if rt.get("manual_pause", False):
        return
    next_bet_amount = calculate_bet_amount(rt)
    if is_fund_available(user_ctx, next_bet_amount) and not rt.get("bet", False) and rt.get("switch", True) and rt.get("stop_count", 0) == 0:
        rt["bet"] = True
        rt["pause_count"] = 0
        user_ctx.save_state()
        mes = f"**押注已恢复**\n当前资金：{rt.get('gambling_fund', 0) / 10000:.2f} 万\n接续倍投金额：{format_number(next_bet_amount)}"
        await send_to_admin(client, mes, user_ctx, global_config)
    elif not is_fund_available(user_ctx, next_bet_amount):
        rt["bet_on"] = False
        rt["mode_stop"] = True
        user_ctx.save_state()
        await send_to_admin(client, "⚠️ 菠菜资金不足，已自动暂停押注", user_ctx, global_config)


def _parse_yc_params(args, presets):
    if not args:
        return None, None, (
            "📊 **测算功能**\n\n"
            "用法:\n"
            "`yc [预设名]` - 测算已有预设\n"
            "`yc [参数...]` - 自定义参数测算\n\n"
            "例: `yc yc05` 或 `yc 1 13 3 2.1 2.1 2.05 500`"
        )

    if args[0] in presets:
        preset = presets[args[0]]
        params = {
            "continuous": int(preset[0]),
            "lose_stop": int(preset[1]),
            "lose_once": float(preset[2]),
            "lose_twice": float(preset[3]),
            "lose_three": float(preset[4]),
            "lose_four": float(preset[5]),
            "initial_amount": int(preset[6]),
        }
        return params, args[0], None

    if len(args) >= 7:
        try:
            params = {
                "continuous": int(args[0]),
                "lose_stop": int(args[1]),
                "lose_once": float(args[2]),
                "lose_twice": float(args[3]),
                "lose_three": float(args[4]),
                "lose_four": float(args[5]),
                "initial_amount": int(args[6]),
            }
            return params, "自定义", None
        except ValueError:
            return None, None, "❌ 参数格式错误，请确保所有参数都是数字"

    return None, None, f"❌ 预设 `{args[0]}` 不存在，且参数不足7个"


def _calculate_yc_sequence(params):
    initial = max(0, int(params["initial_amount"]))
    lose_stop = max(1, int(params["lose_stop"]))
    multipliers = [
        float(params["lose_once"]),
        float(params["lose_twice"]),
        float(params["lose_three"]),
        float(params["lose_four"]),
    ]
    max_single_bet_limit = 50_000_000
    start_streak = max(1, int(params["continuous"]))

    rows = []
    prev_bet = initial
    cumulative_loss = 0
    capped = False

    for i in range(lose_stop):
        if i == 0:
            multiplier = 1.0
            bet = initial
        else:
            multiplier = multipliers[min(i - 1, 3)]
            bet = int(prev_bet * multiplier)

        if bet > max_single_bet_limit:
            bet = max_single_bet_limit
            capped = True

        cumulative_loss += bet
        profit_if_win = bet - (cumulative_loss - bet)
        rows.append(
            {
                "streak": start_streak + i,
                "multiplier": multiplier,
                "bet": bet,
                "profit_if_win": profit_if_win,
                "cumulative_loss": cumulative_loss,
            }
        )
        prev_bet = bet

        if capped:
            break

    total_investment = rows[-1]["cumulative_loss"] if rows else 0
    max_bet = max((row["bet"] for row in rows), default=0)
    return {
        "rows": rows,
        "total_investment": total_investment,
        "max_bet": max_bet,
        "max_single_bet_limit": max_single_bet_limit,
        "capped": capped,
        "start_streak": start_streak,
    }


def _build_yc_result_message(params, preset_name: str, current_fund: int, auto_trigger: bool) -> str:
    calc = _calculate_yc_sequence(params)
    rows = calc["rows"]
    total_investment = calc["total_investment"]
    max_single_bet_limit = calc["max_single_bet_limit"]
    start_streak = calc["start_streak"]

    def fmt_wan(value: int) -> str:
        return f"{value / 10000:,.1f}"

    def fmt_table_wan(value: int) -> str:
        wan = value / 10000
        if abs(wan) >= 1000:
            return f"{wan:,.0f}"
        return f"{wan:.1f}"

    header_line = "🔮 已根据当前预设自动测算\n" if auto_trigger else ""
    command_text = (
        f"{params['continuous']} {params['lose_stop']} "
        f"{params['lose_once']} {params['lose_twice']} {params['lose_three']} {params['lose_four']} {params['initial_amount']}"
    )

    effective_streak = start_streak + len(rows) - 1 if rows else start_streak
    effective_profit = rows[-1]["profit_if_win"] if rows else 0
    fund_text = f"{format_number(current_fund)} ({fmt_wan(current_fund)}万)" if current_fund > 0 else "未设置"

    lines = []
    if header_line:
        lines.append(header_line.rstrip("\n"))
    lines.append("```")
    lines.extend(
        [
            "🎯 策略参数",
            f"预设名称：{preset_name}",
            f"菠菜资金：{fund_text}",
            f"策略命令: {command_text}",
            f"🏁 起始连数: {params['continuous']}",
            f"🔢 下注次数: {params['lose_stop']}次",
            f"💰 首注金额: {fmt_wan(int(params['initial_amount']))}万",
            f"💰单注上限: {max_single_bet_limit / 10000:,.0f}万",
            "",
            "🎯 策略总结:",
            f"菠菜资金：{fund_text}",
            f"盈利有效连数: {effective_streak}连",
            f"{effective_streak}连所需本金: {fmt_wan(total_investment)}万",
            f"{effective_streak}连可获得盈利: {fmt_wan(effective_profit)}万",
            "",
            "连数|倍率|下注金额| 盈利 |累计损失",
            "---|----|------|------|------",
        ]
    )

    for row in rows:
        multiplier_text = f"{row['multiplier']:.2f}".rstrip("0")
        if multiplier_text.endswith("."):
            multiplier_text += "0"
        row_text = (
            f"{str(row['streak']).center(3)}|"
            f"{multiplier_text.center(4)}|"
            f"{fmt_table_wan(row['bet']).center(6)}|"
            f"{fmt_table_wan(row['profit_if_win']).center(6)}|"
            f"{fmt_table_wan(row['cumulative_loss']).center(6)}"
        )
        lines.append(row_text)

    if calc["capped"]:
        lines.append("")
        lines.append("※ 注意: 后续连数已触发单注上限，测算仅供参考。")

    lines.append("```")
    return "\n".join(lines)


async def yc_command_handler_multiuser(
    client,
    event,
    args,
    user_ctx: UserContext,
    global_config: dict,
    auto_trigger: bool = False,
):
    """处理 yc 测算命令，支持 st 切换预设后自动触发。"""
    presets = user_ctx.presets
    rt = user_ctx.state.runtime

    params, preset_name, error_msg = _parse_yc_params(args, presets)
    if error_msg:
        await send_to_admin(client, error_msg, user_ctx, global_config)
        return

    result_msg = _build_yc_result_message(
        params=params,
        preset_name=preset_name,
        current_fund=int(rt.get("gambling_fund", 0)),
        auto_trigger=auto_trigger,
    )
    await send_to_admin(client, result_msg, user_ctx, global_config)
    log_event(
        logging.INFO,
        'yc',
        '测算完成',
        user_id=user_ctx.user_id,
        preset=preset_name,
        auto_trigger=auto_trigger,
    )


async def fetch_balance(user_ctx: UserContext) -> int:
    zhuque = user_ctx.config.zhuque
    cookie = zhuque.get("cookie", "")
    csrf_token = zhuque.get("csrf_token", "") or zhuque.get("x_csrf", "")
    api_url = zhuque.get("api_url", "https://zhuque.in/api/user/getInfo?")
    
    if not cookie or not csrf_token:
        return 0
    
    headers = {
        "Cookie": cookie,
        "X-Csrf-Token": csrf_token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                api_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 401:
                    user_ctx.set_runtime("balance_status", "auth_failed")
                    log_event(logging.ERROR, 'balance', '认证失败(401)，请更新 Cookie',
                              user_id=user_ctx.user_id)
                    return user_ctx.get_runtime("account_balance", 0)
                
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, dict) and data.get("status", 200) != 200:
                        log_event(logging.WARNING, 'balance', 'API返回错误',
                                  user_id=user_ctx.user_id, message=data.get("message"))
                        return user_ctx.get_runtime("account_balance", 0)
                    
                    balance = int(data.get("data", {}).get("bonus", 0))
                    user_ctx.set_runtime("balance_status", "success")
                    return balance
    except Exception as e:
        user_ctx.set_runtime("balance_status", "network_error")
        log_event(logging.ERROR, 'balance', '获取余额失败',
                  user_id=user_ctx.user_id, data=str(e))
    
    return 0
