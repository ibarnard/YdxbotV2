# 模板参数可覆盖 V1 技术说明书

## 1. 文档目的
本说明书记录 `codex/risk-history-v1` 分支在“模板参数可覆盖 V1”阶段完成的能力：
- `task new` 支持覆盖参数
- `pkg new` 支持覆盖参数

这一步的目标是继续降低创建成本，但不把命令做成新的复杂配置系统。

---

## 2. 阶段定位
在上一阶段，任务模板和任务包模板已经能快速创建：
- `task new <模板> [名称]`
- `pkg new <模板> [名称]`

但还存在一个问题：
- 只能吃模板默认值
- 一旦想改预设、目标笔数、止损，又要退回到更长、更工程化的命令

本阶段解决的就是这个问题。

---

## 3. 当前交付范围
本阶段已完成：

1. `task new` 覆盖参数
2. `pkg new` 覆盖参数
3. 模板参数解析器
4. 对应测试
5. 对应说明书

---

## 4. 支持的覆盖参数
当前只支持 3 类覆盖参数，刻意保持收敛：

- `preset=...`
- `bets=...`
- `loss=...`

### 4.1 示例
```text
task new 保守巡航
task new 保守巡航 巡航A
task new 保守巡航 巡航A preset=yc10 bets=12 loss=30000

pkg new 稳健包
pkg new 稳健包 主包
pkg new 稳健包 主包 preset=yc10 bets=6 loss=18000
```

### 4.2 含义
- `preset`
  - 覆盖模板默认预设
- `bets`
  - 覆盖模板默认目标笔数
- `loss`
  - 覆盖模板默认止损

---

## 5. 代码实现

### 5.1 `task_engine.py`
新增/扩展：
- `parse_template_new_args()`
- `create_task_from_template()` 支持覆盖参数

### 5.2 `task_package_engine.py`
扩展：
- `create_package_from_template()` 支持覆盖参数

当前策略：
- 对任务包，覆盖参数会统一应用到包内每个成员任务

也就是说：
- `pkg new 稳健包 主包 preset=yc10 bets=6 loss=18000`

会让包内自动创建的成员任务全部使用：
- `yc10`
- `6`
- `18000`

---

## 6. 命令入口

### 6.1 任务模板
- `task tpl`
- `task new <模板> [名称] [preset=...] [bets=...] [loss=...]`

### 6.2 任务包模板
- `pkg tpl`
- `pkg new <模板> [名称] [preset=...] [bets=...] [loss=...]`

---

## 7. 设计原则

### 7.1 只做最核心的覆盖项
当前不支持：
- 分钟覆盖
- 盘面覆盖
- 更复杂的多字段组合

原因：
- 当前目标是先把最常用的调整项补齐
- 不把命令重新做成新的长参数地狱

### 7.2 保持模板仍然是主入口
即使支持覆盖参数，也不鼓励退回全手工长命令。

推荐使用方式仍然是：
- 先选模板
- 再只覆盖最关键的 1 到 3 个参数

---

## 8. 参数校验
当前已做：

- `bets` 必须是大于 `0` 的整数
- `loss` 不能为负数
- `preset` 必须是已有预设
- 未知参数直接报错

当前不做：
- 自动纠错
- 模糊参数名猜测

---

## 9. 测试

### 9.1 新增/扩展测试
- `tests_multiuser/test_task_engine_v1.py`
- `tests_multiuser/test_task_package_engine_v1.py`

覆盖：
- 模板参数解析
- `task new` 覆盖参数
- `pkg new` 覆盖参数
- 任务包成员任务统一应用覆盖参数

### 9.2 当前完整回归
```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py tests_multiuser\test_task_package_engine_v1.py -q
```

结果：
- `100 passed`

---

## 10. 当前边界
这一步没有做：
- 交互式创建向导
- 自定义模板编辑
- 每个成员任务单独覆盖不同参数
- 更复杂的包级模板参数

原因：
- 这会把阶段范围拉大
- 当前优先目标是先补“最常用的覆盖项”

---

## 11. 下一步
模板参数可覆盖之后，下一阶段默认进入：

### 策略版本化与 prompt 回写 V1

也就是：
- 把复盘事实回写到策略版本
- 让提示词和策略上下文有版本、有灰度、有回滚
