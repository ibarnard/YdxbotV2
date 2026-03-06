# 任务系统 V1 技术说明书

## 1. 文档目的
本说明书沉淀 `codex/risk-history-v1` 分支在“任务系统”阶段完成的设计、代码边界、命令入口、运行态、测试口径与后续计划。

目标是让后续在新电脑、新环境、或换人继续开发时，可以直接基于仓库内文档恢复上下文，而不是依赖聊天记录。

---

## 2. 阶段定位
任务系统 V1 建立在上一阶段“风控-历史资产-动态押注 V1”底座之上。

上一阶段已经解决：
- `fk1 / fk2 / fk3`
- 每账户 `analytics.db`
- `fp 1~6`
- 动态档位决策器

本阶段解决：
- 任务定义与持久化
- 任务状态机
- 任务与盘面/定时触发联动
- 任务与动态押注联动
- 任务日志、任务统计、任务命令入口

当前结论：
- 任务系统 V1 已具备单账户可运行能力。
- 任务已经能接管下注主流程，并把运行事件落入 `analytics.db`。
- 目前仍是 V1，优先解决“能管理、能复盘、能接管”，还不是最终的全自动调度平台。

---

## 3. 当前交付范围
本阶段已完成下面 6 类内容：

1. 任务定义持久化
- 每账户新增 `tasks.json`
- 任务定义不混入 `state.json`

2. 任务状态机
- `idle`
- `running`
- `paused`

3. 任务触发方式
- `manual`
- `schedule`
- `regime`
- `hybrid`

4. 任务接入下注主流程
- 下注前评估是否接管本轮
- 运行中自动应用基准预设
- 与动态档位、`fk1/fk2/fk3` 共存

5. 任务运行记录
- 每次启动、阻断、下注、结算、完成、止损都会写入 `analytics.db.task_runs`

6. 任务命令入口
- `task`
- `task list`
- `task show <id>`
- `task add ...`
- `task run <id>`
- `task pause <id>`
- `task resume <id>`
- `task on <id>`
- `task off <id>`
- `task del <id>`
- `task logs [id]`
- `task stats [id]`

---

## 4. 代码模块

### 4.1 新增模块
- `task_engine.py`

职责：
- 任务定义归一化
- 任务启停
- 任务运行态同步
- 触发判断
- 任务日志/统计文案

### 4.2 已扩展模块
- `zq_multiuser.py`
  - 接入 `task` 命令
  - 下注主流程接入任务接管
  - 结算后回写任务完成情况
- `history_analysis.py`
  - 新增 `task_runs` 表与任务事件写库
- `user_manager.py`
  - 每账户任务列表加载/保存
  - 任务运行态字段默认值

---

## 5. 数据结构设计

### 5.1 每账户任务文件
路径：
- `users/<user>/tasks.json`

格式：
```json
{
  "version": 1,
  "tasks": [
    {
      "task_id": "task_20260306210000_ab12cd",
      "name": "延续巡航",
      "enabled": true,
      "status": "idle",
      "trigger_mode": "regime",
      "interval_minutes": 0,
      "regimes": ["延续盘"],
      "base_preset": "yc20",
      "max_bets": 10,
      "max_loss": 50000,
      "priority": 100
    }
  ]
}
```

### 5.2 任务核心字段
- `task_id`
- `name`
- `enabled`
- `status`
- `trigger_mode`
- `interval_minutes`
- `regimes`
- `base_preset`
- `max_bets`
- `max_loss`
- `priority`
- `current_run_id`
- `progress_bets`
- `progress_profit`
- `progress_loss`
- `total_runs`
- `total_bets`
- `total_profit`
- `last_action`
- `last_reason`

---

## 6. 任务状态机

### 6.1 状态定义
- `idle`
  - 空闲，未运行
- `running`
  - 当前任务正在接管下注链路
- `paused`
  - 任务存在但暂停，不会自动触发

### 6.2 当前状态流转
1. `idle -> running`
- 手动触发
- 盘面触发
- 定时触发
- 混合触发

2. `running -> idle`
- 达到目标笔数
- 达到任务亏损上限
- 手动关闭后重置

3. `running -> paused`
- 手动暂停
- 基准预设不存在

4. `paused -> running`
- 手动恢复

### 6.3 当前约束
- 同一账户同一时刻只允许一个 `running` 任务
- 下注序列仍在进行中时，新任务不会强行接管
- 连输序列进行中时，不切入新任务

---

## 7. 触发方式设计

### 7.1 `manual`
只允许手动运行：
- `task run <id>`

### 7.2 `schedule`
按时间间隔触发：
- `interval_minutes`

### 7.3 `regime`
按盘面标签触发：
- `延续盘`
- `衰竭盘`
- `反转盘`
- `震荡盘`
- `混乱盘`

### 7.4 `hybrid`
同时满足：
- 时间到了
- 当前盘面命中目标标签

---

## 8. 与现有下注主流程的关系

### 8.1 接入顺序
当前下注主流程顺序为：

1. 刷新当前盘面快照
2. 任务系统判断本轮是否接管
3. 模型给方向或观望
4. `fk1`
5. `fk2`
6. 动态档位决策
7. 资金风控
8. 真实下注
9. 结算后 `fk3`

### 8.2 任务接管后做什么
任务接管后只做两件事：
- 应用任务的基准预设
- 维护任务运行进度

任务不会绕开：
- `fk1`
- `fk2`
- `fk3`
- 动态档位
- 资金风控

换句话说：
- 任务系统是调度层
- 不是风控旁路

---

## 9. 与动态押注的关系

### 9.1 当前原则
任务只定义：
- 基准预设
- 目标笔数
- 亏损上限
- 触发条件

真实执行时的实际档位，仍由动态档位引擎决定。

### 9.2 当前链路
例如：
- 任务基准预设 = `yc20`
- 当前盘面为混乱盘
- `fk1` 只允许 `yc1`

则本局最终真实执行为：
- `yc1`

如果当前已经在连输阶段，且首注档位为 `yc20`，则仍保留原有业务约束：
- 连输阶段不能低于本轮首注档位

---

## 10. 任务运行记录

### 10.1 写入位置
- `analytics.db.task_runs`

### 10.2 当前记录的事件类型
- `started`
- `observe`
- `blocked_fk1`
- `blocked_fk2`
- `blocked_fund`
- `bet`
- `settled`
- `pause_fk3`
- `pause_goal`
- `completed`
- `stop_loss`
- `paused`
- `resumed`

### 10.3 设计目的
后续复盘时，不只知道“这轮任务赚没赚钱”，还要能回答：
- 是因为盘面不对没做
- 还是做了但被风控压住
- 还是做了但动态档位下收
- 还是确实执行了高档位后亏掉

---

## 11. 用户命令设计

### 11.1 总览
- `task`
  - 查看任务总览
- `task list`
  - 查看任务列表
- `task show <id>`
  - 查看单个任务详情
- `task logs [id]`
  - 查看任务运行记录
- `task stats [id]`
  - 查看任务统计

### 11.2 管理
- `task add <名称> <预设> <笔数> [manual|schedule|regime|hybrid] ...`
- `task run <id>`
- `task pause <id>`
- `task resume <id>`
- `task on <id>`
- `task off <id>`
- `task del <id>`

### 11.3 当前定位
V1 的命令设计偏工程化，优先保证可用和可回溯；后续如果继续演进，可以再补：
- 向导式创建
- 模板化任务
- 更口语化的参数输入

---

## 12. 运行态字段
当前每账户 runtime 新增：
- `task_current_id`
- `task_current_name`
- `task_current_run_id`
- `task_current_trigger_mode`
- `task_current_base_preset`
- `task_current_progress_bets`
- `task_current_target_bets`
- `task_last_action`
- `task_last_reason`
- `task_last_event_at`

作用：
- 启动提醒直接展示任务重点信息
- 面板与命令统一读同一份运行态
- 不中断现有多账户隔离方式

---

## 13. 本阶段测试

### 13.1 新增测试
- `tests_multiuser/test_task_engine_v1.py`

当前覆盖：
- 任务创建与持久化
- 盘面触发接管
- 有未完成序列时等待接管
- 达到目标笔数后自动结束
- 下注主流程接入任务
- `task list/show/stats` 命令入口

### 13.2 回归结果
本阶段完成后的本地回归：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py -q
```

结果：
- `90 passed`

---

## 14. 当前边界与未做内容
本阶段刻意没有做：
- 多任务并行运行
- 多账号统一任务调度
- 更复杂的 cron / 日历式调度
- 任务模板向导
- 任务自动学习
- 按收益目标自动切换多个任务包

原因：
- 先把单任务、单账户、可回溯链路做稳
- 否则后续复杂调度会建立在不稳定状态之上

---

## 15. 下一阶段建议
任务系统 V1 之后，建议进入下面方向之一：

1. 任务模板化与向导化
- 让任务创建从工程参数，升级成业务模板

2. 动态任务包
- 多个任务按盘面和收益状态切换

3. 更高层执行编排
- 任务组
- 任务优先级
- 任务冲突仲裁

4. 提示词/策略版本化闭环
- 基于复盘结论回写策略版本，而不是手工调整

---

## 16. 换电脑同步流程
换电脑后按下面顺序恢复：

1. 拉取分支：
- `codex/risk-history-v1`

2. 先看：
- `docs/README.md`
- `docs/risk-history-dynamic-v1-tech-spec.md`
- `docs/task-system-v1-tech-spec.md`

3. 本地环境：
- 使用 `venv_win`
- 运行 `verify_deps.py`

4. 回归：
- 跑本说明书中的 `pytest` 命令

5. 再继续下一个阶段

这样可以保证：
- 代码
- 文档
- 测试口径
- 阶段边界

都跟仓库版本保持一致，不依赖聊天记录。
