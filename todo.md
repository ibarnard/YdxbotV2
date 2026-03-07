# YdxbotV2 当前执行清单

## 1. 文件定位
这份 `todo.md` 只负责一件事：

- 记录当前分支已经完成了哪些阶段
- 说明当前阶段推进到了哪里
- 指向对应的技术说明书

它不是长期路线图。长期规划看：

- [docs/refactor-roadmap.md](./docs/refactor-roadmap.md)

---

## 2. 当前基线
- 分支：`codex/risk-history-v1`
- 文档索引：[docs/README.md](./docs/README.md)
- 当前不纳入版本管理：`analysis_outputs/`

---

## 3. 已完成阶段

### [x] A. 风控-历史资产-动态押注 V1
- 说明书：[docs/risk-history-dynamic-v1-tech-spec.md](./docs/risk-history-dynamic-v1-tech-spec.md)
- 交付：`fk1 / fk2 / fk3`、每账号 `analytics.db`、`fp 1~6`、动态档位决策器、动态押注接入主流程

### [x] B. 任务系统 V1
- 说明书：[docs/task-system-v1-tech-spec.md](./docs/task-system-v1-tech-spec.md)
- 交付：`tasks.json`、任务状态机、任务接管下注主流程、任务日志/统计、`task` 命令族

### [x] C. 任务模板与快速创建 V1
- 说明书：[docs/task-template-v1-tech-spec.md](./docs/task-template-v1-tech-spec.md)
- 交付：`task tpl`、`task new <模板> [名称]`、内置模板

### [x] D. 任务包 V1
- 说明书：[docs/task-package-v1-tech-spec.md](./docs/task-package-v1-tech-spec.md)
- 交付：`task_packages.json`、任务包状态机、`pkg` 命令、包级运行/日志/统计、主流程接入

### [x] E. 模板参数可覆盖 V1
- 说明书：[docs/task-template-overrides-v1-tech-spec.md](./docs/task-template-overrides-v1-tech-spec.md)
- 交付：`task new/pkg new` 参数覆盖、参数解析与校验、对应测试

### [x] F. 策略版本化与 Prompt 回写 V1
- 说明书：[docs/policy-prompt-v1-tech-spec.md](./docs/policy-prompt-v1-tech-spec.md)
- 交付：`policy_versions.json`、`policy_versions / policy_events`、结构化证据包、prompt 回写片段、`policy` 命令、单账号灰度/回滚

### [x] G. 多账号编排 V1
- 说明书：[docs/multi-account-orchestration-v1-tech-spec.md](./docs/multi-account-orchestration-v1-tech-spec.md)
- 交付：多账号注册表、`fleet / users`、`fleet task`、`fleet policy`、`fleet show`、`fleet gray`

### [x] H. 受控自学习 V1
- 说明书：[docs/controlled-self-learning-v1-tech-spec.md](./docs/controlled-self-learning-v1-tech-spec.md)
- 当前实现：H1/H2/H3/H4/H5 全部完成
- 交付：
  - H1 候选中心：`learning_center.json`、`learn / learn list / learn show`
  - H2 候选生成：`learn gen`
  - H3 离线评估：`learn eval`
  - H4 影子验证：`learn shadow`
  - H5 灰度转正回滚：`learn gray / learn promote / learn rollback`

### [x] I. Telegram 值守播报 V1
- 说明书：[docs/tg-watch-v1-tech-spec.md](./docs/tg-watch-v1-tech-spec.md)
- 当前实现：I1/I2/I3/I4/I5 全部完成
- 交付：
  - I1 指定值守目标：`notification.watch`、旧字段归一化、专用 `send_to_watch`
  - I2 值守摘要命令：`watch / watch fleet / watch learn`
  - I3 主动播报：任务切换、任务接管、资金暂停/恢复、模型超时、学习阶段变更
  - I4 告警摘要：`watch alerts`、当前风险 + 最近播报
  - I5 文档、测试、回归

### [x] J. 执行稳定性与启动自检 V1
- 说明书：[docs/runtime-stability-v1-tech-spec.md](./docs/runtime-stability-v1-tech-spec.md)
- 当前实现：J1/J2/J3/J4/J5 全部完成
- 交付：
  - J1 配置自检：`doctor / doctor fleet`
  - J2 启动阻断：启动前阻断明显错配，避免静默带病运行
  - J3 运行态稳态修复：清理非法值守缓存、过期暂停/影子状态、失效挂单标记
  - J4 异常快照：统一 `runtime_faults / last_runtime_fault`
  - J5 文档、测试、回归

---

## 4. 当前状态
当前既定阶段 A 到 J 均已完成。

下一步不是继续补旧计划，而是等待新的阶段目标。

如需继续推进，先在 [docs/refactor-roadmap.md](./docs/refactor-roadmap.md) 里补一个新的阶段定义，再进入实现。

---

## 5. 回归标准
每个阶段收尾前至少执行：

```powershell
python -m pytest `
  tests_multiuser\test_multiuser_branch.py `
  tests_multiuser\test_risk_history_v1.py `
  tests_multiuser\test_dynamic_betting.py `
  tests_multiuser\test_task_engine_v1.py `
  tests_multiuser\test_task_package_engine_v1.py `
  tests_multiuser\test_policy_prompt_v1.py `
  tests_multiuser\test_multi_account_orchestrator_v1.py `
  tests_multiuser\test_self_learning_v1.py `
  tests_multiuser\test_tg_watch_v1.py `
  tests_multiuser\test_runtime_stability_v1.py `
  -q
```

如果后续阶段再新增测试文件，要一起纳入这条回归命令。

---

## 6. 更新规则
后续每次推进，按这个顺序更新：

1. 先改代码
2. 再补测试
3. 再补文档
4. 再更新 `todo.md`
5. 最后 `commit / push`
