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

### [x] D. 任务包 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/task-package-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/task-package-v1-tech-spec.md)

已交付：
- `task_packages.json`
- 任务包运行态
- 任务包模板与 `pkg` 命令
- 包级选任务逻辑
- `analytics.db.package_runs`
- `process_bet_on` / `process_settle` 接入任务包
- 包级测试与回归

---

## 5. 当前执行阶段

### [x] E. 模板参数可覆盖 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/task-template-overrides-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/task-template-overrides-v1-tech-spec.md)

已交付：
- `task new <模板> [名称] [preset=...] [bets=...] [loss=...]`
- `pkg new <模板> [名称] [preset=...] [bets=...] [loss=...]`
- 模板参数解析器
- 参数校验
- 对应测试与说明书

---

## 6. 当前执行阶段

### [x] F. 策略版本化与 prompt 回写 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/policy-prompt-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/policy-prompt-v1-tech-spec.md)

已交付：
- `policy_versions.json`
- `policy_versions / policy_events`
- 结构化证据包
- prompt 回写片段
- `policy / pol` 命令
- 单账户灰度与回滚
- 决策落库 `policy_*`
- 对应测试与说明书

---

## 7. 当前执行阶段

### [x] G. 多账号编排 V1
状态：已完成，已提交，已推送

对应文档：
- [docs/multi-account-orchestration-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/multi-account-orchestration-v1-tech-spec.md)

已交付：
- 多账号注册表
- `fleet` / `users`
- `fleet task`
- `fleet policy`
- `fleet show <账号名|ID>`
- `fleet gray <账号名|ID> baseline|latest`
- 对应测试与说明书

---

## 8. 当前之后的顺序
当前默认顺序：

### [ ] H. 受控自学习 V1
状态：进行中（H1/H2/H3 已完成，H4/H5 待实现）

对应文档：
- [docs/controlled-self-learning-v1-tech-spec.md](D:/OneDrive/06Code/YdxbotV2/docs/controlled-self-learning-v1-tech-spec.md)

执行子阶段：
- [x] H1 候选中心
  - `learning_center.json`
  - `learning_candidates / learning_evaluations / learning_shadows / learning_promotions`
  - `learn` / `learn list` / `learn show`
- [x] H2 候选生成器
  - 基于复盘事实生成候选策略
  - `learn gen`
- [x] H3 离线评估器
  - 样本覆盖、收益、回撤、稳定性评分
  - `learn eval`
- [ ] H4 影子验证器
  - 在线候选影子对比
  - `learn shadow`
- [ ] H5 灰度与转正
  - `learn gray`
  - `learn promote`
  - `learn rollback`

设计结论：
- 不允许自动转正
- 不允许覆盖硬风控
- 只允许单账户灰度

---

## 9. 当前之后的顺序
当前默认顺序：

1. H4 影子验证器
2. H5 灰度与转正

详细拆解、时间预估、风险、确认点，请看：
- [docs/refactor-roadmap.md](D:/OneDrive/06Code/YdxbotV2/docs/refactor-roadmap.md)

---

## 10. 回归标准
每个阶段结束前至少执行：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py tests_multiuser\test_task_package_engine_v1.py tests_multiuser\test_policy_prompt_v1.py tests_multiuser\test_multi_account_orchestrator_v1.py tests_multiuser\test_self_learning_v1.py -q
```

如果阶段新增新的测试文件，要一并纳入回归。

---

## 11. 更新规则
后续每次推进，按这个顺序更新：

1. 先改代码
2. 再补测试
3. 再补文档
4. 再更新 `todo.md`
5. 最后 commit / push
