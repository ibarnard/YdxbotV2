# YdxbotV2 当前执行清单

## 1. 这份文件的定位
这份 `todo.md` 只负责一件事：

- 记录当前分支正在执行的阶段
- 明确每个阶段做到哪里
- 指向对应的技术说明书

它不是长期路线图。

长期路线、预计时间、后续大阶段，请看：
- [docs/refactor-roadmap.md](D:/OneDrive/06Code/YdxbotV2/docs/refactor-roadmap.md)

---

## 2. 当前基线
- 分支：`codex/risk-history-v1`
- 文档索引：[docs/README.md](D:/OneDrive/06Code/YdxbotV2/docs/README.md)
- 当前未纳入版本管理：`analysis_outputs/`

---

## 3. 已完成阶段

### [x] A. 风控-历史资产-动态押注 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/risk-history-dynamic-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/risk-history-dynamic-v1-tech-spec.md)

已交付：
- `fk1 / fk2 / fk3`
- 每账户 `analytics.db`
- `fp 1~6`
- 动态档位决策器
- 动态押注接入主流程

---

### [x] B. 任务系统 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/task-system-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/task-system-v1-tech-spec.md)

已交付：
- `tasks.json`
- 任务状态机
- 任务接管下注主流程
- 任务日志与统计
- `task / task list / task show / task logs / task stats`

---

### [x] C. 任务模板与快速创建 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/task-template-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/task-template-v1-tech-spec.md)

已交付：
- `task tpl`
- `task new <模板> [名称]`
- 内置模板

---

## 4. 当前执行阶段

### [ ] D. 任务包 V1
状态：下一阶段，准备开始

目标：
- 让多个任务组成一个任务包
- 按盘面、风控、近期收益状态切换任务
- 保持单账户同一时刻只运行一个实际任务

本阶段完成标准：

#### D1. 数据结构
- [ ] 新增 `task_packages.json`
- [ ] 定义 `package_id / name / enabled / tasks / switch_mode / status`
- [ ] 定义包内任务优先级和切换条件

#### D2. 运行态
- [ ] 增加当前任务包字段
- [ ] 记录当前包、当前任务、上次切换原因
- [ ] 启动提醒显示任务包摘要

#### D3. 切换逻辑
- [ ] 根据盘面标签选择更合适的任务
- [ ] 根据 `fk1` 和动态档位做降级
- [ ] 根据近期收益/回撤切换保守任务
- [ ] 保证一个账户同一时刻只有一个任务实际运行

#### D4. 命令入口
- [ ] `pkg`
- [ ] `pkg list`
- [ ] `pkg show <id>`
- [ ] `pkg tpl`
- [ ] `pkg new <模板> [名称]`
- [ ] `pkg run <id>`
- [ ] `pkg pause <id>`

#### D5. 记录与复盘
- [ ] 包级运行日志写入 `analytics.db`
- [ ] 能看到“为什么从任务 A 切到任务 B”
- [ ] 能区分单任务收益和任务包收益

#### D6. 测试
- [ ] 单测：包创建、包切换、互斥运行
- [ ] 集成：下注主流程里包切换不破坏现有风控和任务链路
- [ ] 回归：现有主测试继续通过

#### D7. 文档
- [ ] 新增 `docs/task-package-v1-tech-spec.md`
- [ ] 更新 [docs/README.md](D:/OneDrive/06Code/YdxbotV2/docs/README.md)
- [ ] 更新本文件阶段状态

#### D8. 提交
- [ ] 功能代码单独 commit
- [ ] 文档单独 commit
- [ ] 推送到远端分支

---

## 5. 当前之后的顺序
当前默认顺序：

1. `D. 任务包 V1`
2. 模板参数可覆盖 V1
3. 策略版本化与 prompt 回写 V1
4. 多账号编排 V1
5. 自学习 V1

详细拆解、时间预估、风险、确认点，请看：
- [docs/refactor-roadmap.md](D:/OneDrive/06Code/YdxbotV2/docs/refactor-roadmap.md)

---

## 6. 回归标准
每个阶段结束前至少执行：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py -q
```

如果阶段新增新的测试文件，要一并纳入回归。

---

## 7. 更新规则
后续每次推进，按这个顺序更新：

1. 先改代码
2. 再补测试
3. 再补文档
4. 再更新 `todo.md`
5. 最后 commit / push
