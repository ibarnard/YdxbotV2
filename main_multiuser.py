"""
main_multiuser.py - 多用户版本主程序
版本: 2.0.0
日期: 2026-02-20
功能: 支持多用户并发运行的Telegram客户端
"""

import logging
import asyncio
import os
import time
from telethon import TelegramClient, events
from logging.handlers import TimedRotatingFileHandler
from user_manager import UserManager, UserContext
from model_manager import model_manager
from update_manager import periodic_release_check_loop

# 日志配置
logger = logging.getLogger('main_multiuser')
logger.setLevel(logging.DEBUG)

file_handler = TimedRotatingFileHandler('numai.log', when='midnight', interval=1, backupCount=3, encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)s | [%(custom_module)s:%(event)s] | %(message)s | %(data)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s | %(data)s',
    datefmt='%H:%M:%S'
))
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)


def log_event(level, module, event=None, message='', **kwargs):
    # 兼容3参数调用: log_event(level, module, event)
    if event is None:
        event = module
        module = 'main'
        message = ''
    elif not message and not kwargs:
        # log_event(level, module, event) - event作为message
        message = event
        event = module
        module = 'main'
    data = ', '.join(f'{k}={v}' for k, v in kwargs.items())
    logger.log(level, message, extra={'custom_module': module, 'event': event, 'data': data})


async def create_client(user_ctx: UserContext, global_config: dict) -> TelegramClient:
    proxy = None
    proxy_cfg = global_config.get("proxy", {})
    if proxy_cfg.get("enabled"):
        proxy = {
            'proxy_type': proxy_cfg.get("type", "socks5"),
            'addr': proxy_cfg.get("host", "127.0.0.1"),
            'port': proxy_cfg.get("port", 7890),
            'username': proxy_cfg.get("username") or None,
            'password': proxy_cfg.get("password") or None,
            'rdns': True
        }
        log_event(logging.DEBUG, 'create_client', '使用代理', user_id=user_ctx.user_id)
    
    session_path = os.path.join(
        user_ctx.user_dir, 
        user_ctx.config.telegram.get("session_name", "session")
    )
    
    client = TelegramClient(
        session_path,
        user_ctx.config.telegram.get("api_id"),
        user_ctx.config.telegram.get("api_hash"),
        proxy=proxy
    )
    return client


def register_handlers(client: TelegramClient, user_ctx: UserContext, global_config: dict):
    config = user_ctx.config
    state = user_ctx.state
    presets = user_ctx.presets
    button_mapping = global_config.get("button_mapping", {})
    
    @client.on(events.NewMessage(
        chats=config.groups.get("zq_group", []),
        pattern=r"\[近 40 次结果\]\[由近及远\]\[0 小 1 大\].*",
        from_users=config.groups.get("zq_bot")
    ))
    async def bet_on_handler(event):
        log_event(logging.DEBUG, 'bet_on', '收到押注触发消息', 
                  user_id=user_ctx.user_id, msg_id=event.id)
        await zq_bet_on(client, event, user_ctx, global_config)
    
    @client.on(events.NewMessage(
        chats=config.groups.get("zq_group", []),
        # 修复：多用户分支 - 结算正则字符类误写会匹配到 `|`，导致异常消息也被当作结算。
        pattern=r"已结算: 结果为 (\d+) (大|小)",
        from_users=config.groups.get("zq_bot")
    ))
    async def settle_handler(event):
        log_event(logging.DEBUG, 'settle', '收到结算消息',
                  user_id=user_ctx.user_id, msg_id=event.id)
        await zq_settle(client, event, user_ctx, global_config)
    
    @client.on(events.NewMessage(chats=config.groups.get("admin_chat")))
    async def user_handler(event):
        log_event(logging.DEBUG, 'user_cmd', '收到用户命令',
                  user_id=user_ctx.user_id, cmd=event.raw_text[:50])
        await zq_user(client, event, user_ctx, global_config)


async def zq_bet_on(client, event, user_ctx: UserContext, global_config: dict):
    from zq_multiuser import process_bet_on
    await process_bet_on(client, event, user_ctx, global_config)


async def zq_settle(client, event, user_ctx: UserContext, global_config: dict):
    from zq_multiuser import process_settle
    await process_settle(client, event, user_ctx, global_config)


async def zq_user(client, event, user_ctx: UserContext, global_config: dict):
    from zq_multiuser import process_user_command
    await process_user_command(client, event, user_ctx, global_config)


async def check_models_for_user(client, user_ctx: UserContext):
    try:
        model_manager.load_models()
        models = model_manager.list_models()
        
        report = f"🚀 **Bot 启动模型自检报告**\n\n"
        report += f"👤 **用户**: {user_ctx.config.name}\n\n"
        
        total_models = sum(len(ms) for ms in models.values())
        success_count = 0
        
        for provider, ms in models.items():
            report += f"📁 **{provider.upper()}**\n"
            for m in ms:
                mid = m['model_id']
                if not m.get('enabled', True):
                    report += f"⚪ `{mid}`: 已禁用\n"
                    continue
                
                res = await model_manager.validate_model(mid)
                if res['success']:
                    status = "✅ 正常"
                    latency = res.get('latency', 'N/A')
                    success_count += 1
                else:
                    status = f"❌ 失败"
                    latency = "-"
                
                report += f"{status} `{mid}` ({latency}ms)\n"
            report += "\n"
        
        report += f"📊 **汇总**: {success_count}/{total_models} 可用\n"
        report += f"🤖 **当前默认**: `{user_ctx.get_runtime('current_model_id', 'qwen3-coder-plus')}`"
        
        await client.send_message(user_ctx.config.groups.get("admin_chat"), report)
        log_event(logging.INFO, 'model_check', '模型自检完成', user_id=user_ctx.user_id)
        
    except Exception as e:
        log_event(logging.ERROR, 'model_check', '模型自检失败', 
                  user_id=user_ctx.user_id, error=str(e))


async def fetch_account_balance(user_ctx: UserContext) -> int:
    import aiohttp
    
    zhuque = user_ctx.config.zhuque
    cookie = zhuque.get("cookie", "")
    csrf_token = zhuque.get("csrf_token", "") or zhuque.get("x_csrf", "")
    api_url = zhuque.get("api_url", "https://zhuque.in/api/user/getInfo?")
    
    if not cookie or not csrf_token:
        log_event(logging.ERROR, 'balance', '缺少朱雀配置', user_id=user_ctx.user_id)
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
                    log_event(logging.INFO, 'balance', '获取余额成功',
                              user_id=user_ctx.user_id, balance=balance)
                    return balance
                else:
                    user_ctx.set_runtime("balance_status", "network_error")
                    log_event(logging.ERROR, 'balance', '获取余额失败',
                              user_id=user_ctx.user_id, status=response.status)
                    return user_ctx.get_runtime("account_balance", 0)
    except Exception as e:
        user_ctx.set_runtime("balance_status", "network_error")
        log_event(logging.ERROR, 'balance', '获取余额异常',
                  user_id=user_ctx.user_id, error=str(e))
        return user_ctx.get_runtime("account_balance", 0)


async def start_user(user_ctx: UserContext, global_config: dict):
    try:
        client = await create_client(user_ctx, global_config)
        user_ctx.client = client
        
        await client.connect()
        
        if not await client.is_user_authorized():
            log_event(logging.WARNING, 'start', '用户未授权，开始登录流程',
                      user_id=user_ctx.user_id)
            print(f"\n🔐 用户 {user_ctx.config.name} 需要登录 Telegram")
            print(f"   请按照提示输入手机号和验证码...\n")
            try:
                await client.start()
                log_event(logging.INFO, 'start', '登录成功',
                          user_id=user_ctx.user_id)
                print(f"✅ 用户 {user_ctx.config.name} 登录成功！\n")
            except Exception as e:
                log_event(logging.ERROR, 'start', '登录失败',
                          user_id=user_ctx.user_id, error=str(e))
                print(f"❌ 登录失败: {e}")
                return None
        
        register_handlers(client, user_ctx, global_config)
        
        await check_models_for_user(client, user_ctx)
        
        balance = await fetch_account_balance(user_ctx)
        user_ctx.set_runtime("gambling_fund", balance)
        user_ctx.set_runtime("account_balance", balance)
        user_ctx.save_state()
        
        log_event(logging.INFO, 'start', '用户启动成功',
                  user_id=user_ctx.user_id, name=user_ctx.config.name, balance=balance)
        
        return client
        
    except Exception as e:
        log_event(logging.ERROR, 'start', '用户启动失败',
                  user_id=user_ctx.user_id, error=str(e))
        return None


async def main():
    print("=" * 50)
    print("多用户 Telegram Bot 启动中...")
    print("=" * 50)
    
    user_manager = UserManager()
    user_count = user_manager.load_all_users()
    
    if user_count == 0:
        print("❌ 未找到任何用户配置！")
        print("请在 users/ 目录下创建用户配置文件。")
        print("参考 users/_template/ 目录中的模板文件。")
        return
    
    print(f"✅ 已加载 {user_count} 个用户配置")
    log_event(logging.INFO, 'main', '加载用户配置', count=user_count)
    
    clients = []
    tasks = []
    
    for user_id, user_ctx in user_manager.get_all_users().items():
        print(f"🔄 正在启动用户: {user_ctx.config.name} (ID: {user_id})...")
        client = await start_user(user_ctx, user_manager.global_config)
        
        if client:
            clients.append(client)
            tasks.append(client.run_until_disconnected())
            print(f"✅ 用户 {user_ctx.config.name} 启动成功")
        else:
            print(f"❌ 用户 {user_ctx.config.name} 启动失败")
    
    if not clients:
        print("❌ 没有成功启动任何用户，程序退出")
        return
    
    print("=" * 50)
    print(f"🚀 所有用户已启动，共 {len(clients)} 个客户端运行中")
    print("=" * 50)
    log_event(logging.INFO, 'main', '所有用户启动完成', count=len(clients))

    async def notify_release(message: str):
        sent_admins = set()
        for user_ctx in user_manager.get_all_users().values():
            admin_chat = user_ctx.config.groups.get("admin_chat")
            if not admin_chat or admin_chat in sent_admins or not user_ctx.client:
                continue
            try:
                await user_ctx.client.send_message(admin_chat, message)
                sent_admins.add(admin_chat)
            except Exception as e:
                log_event(
                    logging.ERROR,
                    'release_check',
                    '发布通知发送失败',
                    user_id=user_ctx.user_id,
                    error=str(e),
                )

    asyncio.create_task(periodic_release_check_loop(notify_release))
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    for user_ctx in user_manager.get_all_users().values():
        user_ctx.save_state()
    
    log_event(logging.INFO, 'main', '程序正常退出')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 脚本已手动终止")
        log_event(logging.INFO, 'main', 'stop', message='脚本被用户手动终止')
    except Exception as e:
        log_event(logging.ERROR, 'main', 'error', message='启动失败', error=str(e))
