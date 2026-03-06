# 任务包 V1 技术说明书

## 1. 文档目的
本说明书记录 `codex/risk-history-v1` 分支在“任务包 V1”阶段完成的设计、代码边界、命令入口、运行态、日志与测试口径。

这阶段的核心不是再造一个大平台，而是在现有单任务系统上补一层执行编排。

---

## 2. 阶段定位
在任务系统 V1 之前，脚本已经具备：
- 单任务
- 任务模板
- 风控
- 动态档位

但还不具备：
- 多个任务之间的自动切换
- 根据盘面和近期状态自动切到更保守任务

任务包 V1 解决的就是这层问题。

---

## 3. 当前交付范围
本阶段已完成：

1. 每账户任务包持久化
- `task_packages.json`

2. 任务包运行态
- 当前任务包
- 当前任务包中的当前任务
- 最近切换原因

3. 任务包选择引擎
- 根据盘面标签和近期温度选择更合适的成员任务

4. 任务包命令
- `pkg`
- `pkg tpl`
- `pkg new <模板> [名称]`
- `pkg list`
- `pkg show <id>`
- `pkg run <id>`
- `pkg pause <id>`
- `pkg resume <id>`
- `pkg logs [id]`
- `pkg stats [id]`

5. 任务包日志
- `analytics.db.package_runs`

6. 下注主流程接入
- 下注前任务包先决定当前该由哪个成员任务接管

---

## 4. 代码模块

### 4.1 新增模块
- `task_package_engine.py`

职责：
- 任务包定义归一化
- 任务包模板
- 任务包启停
- 任务包选任务
- 任务包日志与统计文案

### 4.2 已扩展模块
- `user_manager.py`
  - `task_packages`
  - `task_packages.json`
  - 任务包 runtime 字段

- `history_analysis.py`
  - `package_runs`
  - `record_package_event`

- `zq_multiuser.py`
  - `pkg` 命令族
  - 启动提醒和仪表盘显示任务包
  - `process_bet_on` 接入任务包切换
  - `process_settle` 回写任务包收益

- `task_engine.py`
  - 新增 `start_task_if_possible`
  - 新增 `stop_task_run`
  - 让任务包能安全接管/停止成员任务

---

## 5. 数据结构

### 5.1 文件
路径：
- `users/<user>/task_packages.json`

格式：
```json
{
  "version": 1,
  "packages": [
    {
      "package_id": "pkg_20260307010101_ab12cd",
      "name": "主包",
      "enabled": true,
      "status": "running",
      "switch_mode": "adaptive",
      "members": [
        {
          "task_id": "task_xxx",
          "task_name": "主包-趋势跟随",
          "priority": 20
        },
        {
          "task_id": "task_yyy",
          "task_name": "主包-保守巡航",
          "priority": 10
        }
      ]
    }
  ]
}
```

### 5.2 关键字段
- `package_id`
- `name`
- `enabled`
- `status`
- `switch_mode`
- `members`
- `current_run_id`
- `current_task_id`
- `current_task_name`
- `progress_switches`
- `total_runs`
- `total_switches`
- `total_profit`
- `last_action`
- `last_reason`

---

## 6. 任务包模板

### 6.1 当前模板
1. `稳健包`
- 趋势跟随
- 保守巡航

2. `值守包`
- 混合值守
- 保守巡航

3. `全天候包`
- 趋势跟随
- 混合值守
- 保守巡航
- 定时巡航

### 6.2 别名
- `稳健`
- `值守`
- `全天`

### 6.3 模板创建原则
`pkg new` 不直接复用旧任务，而是为当前任务包创建一组独立成员任务。

例如：
- `pkg new 稳健包 主包`

会生成：
- `主包-趋势跟随`
- `主包-保守巡航`

这样可以避免不同任务包共用同一成员任务，导致状态串扰。

---

## 7. 运行逻辑

### 7.1 包与任务的关系
任务包本身不直接下注。

它做的事只有两件：
1. 判断当前该激活哪个成员任务
2. 把该任务交给现有任务系统继续执行

也就是说：
- 任务包 = 编排层
- 单任务 = 执行层

### 7.2 当前选择规则
输入：
- 当前盘面标签
- 当前温度
- 成员任务的盘面范围
- 成员任务基准预设档位

当前规则：
- `延续盘 + normal`
  - 更偏向高档的趋势任务
- `衰竭/反转/震荡/混乱` 或 `cool/cold`
  - 更偏向低档保守任务

### 7.3 当前安全边界
- 单账户同一时刻只允许一个任务包 `running`
- 单账户同一时刻只允许一个真实成员任务运行
- 任务包不会绕过：
  - `fk1`
  - `fk2`
  - `fk3`
  - 动态档位
  - 资金风控

---

## 8. 主流程接入

### 8.1 下注前
顺序：
1. 刷新盘面快照
2. 任务包选择成员任务
3. 单任务系统准备本轮任务
4. 模型判断
5. `fk1`
6. `fk2`
7. 动态档位
8. 资金风控
9. 下注

### 8.2 结算后
顺序：
1. 任务结算
2. 任务包累计收益更新
3. 若当前任务结束，清掉任务包当前任务引用
4. 下一轮由任务包重新选任务

---

## 9. 日志与复盘

### 9.1 表
- `analytics.db.package_runs`

### 9.2 当前事件
- `started`
- `switch_task`
- `waiting`
- `paused`
- `settled`

### 9.3 当前能回答的问题
- 当前任务包什么时候启动
- 当前为什么选了某个任务
- 当前为什么没切到别的任务
- 当前任务包累计收益是多少

---

## 10. 用户命令

### 10.1 查看
- `pkg`
- `pkg tpl`
- `pkg list`
- `pkg show <id>`
- `pkg logs [id]`
- `pkg stats [id]`

### 10.2 操作
- `pkg new <模板> [名称]`
- `pkg run <id>`
- `pkg pause <id>`
- `pkg resume <id>`

### 10.3 当前定位
V1 优先把运行链路打通，不做复杂包编辑器。

---

## 11. 运行态字段
当前每账户 runtime 新增：
- `package_current_id`
- `package_current_name`
- `package_current_status`
- `package_current_task_id`
- `package_current_task_name`
- `package_last_action`
- `package_last_reason`
- `package_last_event_at`

作用：
- 启动提醒展示当前任务包
- 仪表盘展示当前任务包
- 后续命令和日志读取统一状态

---

## 12. 测试

### 12.1 新增测试
- `tests_multiuser/test_task_package_engine_v1.py`

当前覆盖：
- 任务包模板创建
- 任务包选趋势任务
- 任务包收益累计与当前任务清理
- `pkg` 命令
- `process_bet_on` 中任务包接管

### 12.2 回归结果
当前完整回归：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py tests_multiuser\test_task_package_engine_v1.py -q
```

结果：
- `96 passed`

---

## 13. 当前边界
本阶段刻意没有做：
- 包级复杂收益优化
- 包级自动开关多个账号
- 包内共享运行态
- 包内任务图形化编辑
- 包级自学习

原因：
- 当前优先目标是把“包 -> 任务 -> 风控 -> 动态档位”链路打顺
- 先保证包层不会把现有稳定链路破坏

---

## 14. 下一步
任务包 V1 之后，最自然的下一阶段是：

### 模板参数可覆盖 V1
也就是：
- 保留模板创建
- 但允许覆盖预设、目标笔数、止损

再往后才是：
- prompt 回写
- 多账号编排
- 自学习
