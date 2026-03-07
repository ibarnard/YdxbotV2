# 多账号编排 V1 技术说明书

## 1. 文档目的
本说明书记录 `codex/risk-history-v1` 分支在“多账号编排 V1”阶段完成的能力：

- 多账号总览
- 多账号任务/任务包视图
- 多账号策略灰度视图
- 多账号学习状态观察
- 指定账号策略灰度切换

本阶段不做共享运行态，也不做统一一键实盘启停。

---

## 2. 阶段定位
在上一阶段，系统已经具备：

- 单账号风控、历史资产、复盘、动态押注
- 任务 / 任务包
- 策略版本化与 prompt 回写

但还缺少一个多账号层：

- 无法一眼看到所有账号当前状态
- 无法快速查看哪些账号在跑任务包
- 无法从一个入口切换指定账号的策略灰度模式

本阶段解决的就是这些“观察和调度”问题。

---

## 3. 当前交付范围
本阶段已完成：

1. 多账号注册表
2. 多账号总览命令
3. 多账号任务视图
4. 多账号策略灰度视图
5. 指定账号灰度切到 `baseline/latest`
6. 对应测试
7. 对应说明书

---

## 4. 设计边界
本阶段刻意不做：

- 统一一键启停所有账号实盘
- 账号之间共享运行态
- 账号之间共享投注进度
- 自动多账号同步切版本

本阶段只做：

- 观察
- 汇总
- 单账号定向切换

---

## 5. 核心文件

### 5.1 新增
- `multi_account_orchestrator.py`

### 5.2 修改
- `user_manager.py`
- `zq_multiuser.py`

### 5.3 测试
- `tests_multiuser/test_multi_account_orchestrator_v1.py`

---

## 6. 多账号注册表
新增模块级注册表：

- `register_user_context(user_ctx)`
- `get_registered_user_contexts()`
- `clear_registered_user_contexts()`

用途：

- 不引入共享运行态
- 只维护“当前进程里已经加载的账号上下文”
- 供多账号总览与灰度切换使用

---

## 7. 命令入口

### 7.1 多账号总览
- `fleet`
- `users`

显示每个账号：

- 状态
- 当前预设
- 当前任务/任务包
- 当前策略版本
- 当前学习状态（候选 / 影子 / 灰度 / 转正）
- 总胜率
- 总盈利
- `fk1/fk2/fk3` 开关摘要

### 7.2 多账号任务视图
- `fleet task`

显示每个账号：

- 当前任务包
- 当前任务
- 当前进度
- 最近任务动作

### 7.3 多账号策略灰度视图
- `fleet policy`

显示每个账号：

- 当前策略版本
- 当前模式（基线 / 灰度）
- 上一个版本
- 当前学习状态
- 当前摘要

### 7.4 单账号详情
- `fleet show <账号名|ID>`

### 7.5 单账号灰度切换
- `fleet gray <账号名|ID> baseline`
- `fleet gray <账号名|ID> latest`

含义：

- `baseline`
  - 切回该账号的基线策略版本
- `latest`
  - 切到该账号当前最新策略版本

---

## 8. 编排原则
本阶段坚持 3 条原则：

1. 继续保持每账户独立运行态
2. 多账号层只做“观察和调度”，不接管投注主流程
3. 灰度切换只对指定账号生效

这意味着：

- 多账号错误不会因为共享状态而串号
- 你可以安全地逐账号试版本

---

## 9. 风险控制口径
即使引入多账号视图，本阶段仍然不提供：

- 一键全账号启停
- 一键全账号切到最新灰度策略

原因很明确：

- 这两类动作会把单账号错误放大
- 当前 V1 先保证“可看、可切、可控”

---

## 10. 测试与回归
新增测试：

- `test_build_fleet_overview_and_policy_switch`
- `test_process_user_command_fleet`

阶段回归命令：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py tests_multiuser\test_task_package_engine_v1.py tests_multiuser\test_policy_prompt_v1.py tests_multiuser\test_multi_account_orchestrator_v1.py -q
```

---

## 11. 本阶段结论
到这一阶段，系统已经具备：

- 单账号完整策略链路
- 多账号观察层
- 多账号任务汇总
- 多账号策略灰度切换
- 多账号学习灰度状态观察
