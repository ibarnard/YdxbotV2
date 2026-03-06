# 策略版本化与 Prompt 回写 V1 技术说明书

## 1. 文档目的
本说明书记录 `codex/risk-history-v1` 分支在“策略版本化与 prompt 回写 V1”阶段完成的能力：

- 每账户独立策略版本中心
- 结构化证据包
- prompt 回写片段
- 单账户灰度激活
- 策略版本回滚
- 决策链路落库 `policy_*`

这一步的目标不是“让模型自己乱改策略”，而是把复盘事实变成可版本化、可灰度、可回滚的策略上下文。

---

## 2. 阶段定位
在上一阶段，系统已经具备：

- `fk1 / fk2 / fk3`
- 历史资产化与 `analytics.db`
- `fp 1~6`
- 动态押注
- 单任务 / 任务包 / 模板覆盖

但仍然缺一块关键闭环：

- 复盘事实虽然能看见
- 但没有正式进入“策略版本”
- 也没有办法比较、灰度、回滚

本阶段解决的就是这件事。

---

## 3. 当前交付范围
本阶段已完成：

1. `policy_versions.json`
2. `policy_versions` / `policy_events` analytics 表
3. 结构化证据包生成
4. prompt 回写片段生成
5. `policy` / `pol` 命令
6. 决策链路落库 `policy_id / policy_version / policy_mode / policy_summary`
7. 单账户灰度激活与回滚
8. 对应测试
9. 对应说明书

---

## 4. 设计边界
本阶段明确不做：

- 自动替换硬风控
- 自动全账号同步上线
- 自动转正候选策略
- 自动修改底层赔率/倍投参数

本阶段只做：

- 把复盘事实整理成小证据包
- 挂到策略版本上
- 作为 prompt 的策略覆盖层参与决策

---

## 5. 核心文件

### 5.1 新增
- `policy_engine.py`

### 5.2 修改
- `history_analysis.py`
- `zq_multiuser.py`
- `user_manager.py`

### 5.3 测试
- `tests_multiuser/test_policy_prompt_v1.py`

---

## 6. 每账户策略存储
每个账户新增：

- `users/<user>/policy_versions.json`

结构为：

```json
{
  "version": 1,
  "policy_id": "pol_<user>_main",
  "active_version": "v2",
  "previous_version": "v1",
  "last_synced_at": "2026-03-07 10:00:00",
  "policies": [
    {
      "policy_id": "pol_<user>_main",
      "policy_version": "v1",
      "source": "baseline",
      "activation_mode": "baseline",
      "summary": "基线策略：顺势优先，证据不足可观望，高档位谨慎使用"
    },
    {
      "policy_id": "pol_<user>_main",
      "policy_version": "v2",
      "source": "writeback",
      "activation_mode": "gray",
      "summary": "延续盘 | strong | 限档 yc5 | 24h 12 笔 | 温度 正常"
    }
  ]
}
```

---

## 7. 结构化证据包
策略回写不是把全部历史直接塞给模型，而是生成一个小证据包。

当前证据包包括：

- 当前盘面
- 趋势分 / 混乱分 / 反转分
- 相似样本数量
- 证据强度
- 历史建议档位上限
- 近期实盘温度
- 最近 24 小时：
  - 已结算样本数
  - 胜率
  - 总盈亏
  - 最大回撤
  - 观望次数
  - 阻断次数
- 最近 24 小时低/中/高档表现

这一层由 `history_analysis.build_policy_evidence_package()` 负责。

---

## 8. Prompt 回写机制
`policy_engine.sync_policy_from_evidence()` 会基于证据包生成：

1. 策略摘要
2. 写回规则
3. prompt 片段
4. 新策略版本

写回规则的来源包括：

- 当前盘面类型
- 相似历史是否整体偏弱
- 高档位是否历史回撤偏大
- 近期实盘是否偏冷/很冷
- 24h 回撤与盈亏状态

典型写回句子例如：

- 混乱盘时证据一般优先观望
- 反转盘只在证据足够强时逆势
- 高档位历史均收益为负时优先低档
- 近期实盘偏冷时提高保守权重

---

## 9. 决策链路接入
`predict_next_bet_v10()` 已接入：

1. 读取当前策略版本
2. 构建当前证据包
3. 把策略摘要 + 写回规则 + 证据包摘要注入 prompt
4. 把 `policy_*` 信息写入审计日志与 analytics 决策表

决策表新增字段：

- `policy_id`
- `policy_version`
- `policy_mode`
- `policy_summary`

---

## 10. 命令入口

### 10.1 查看当前策略
- `policy`
- `pol`

### 10.2 查看版本列表
- `policy list`

### 10.3 查看版本详情
- `policy show`
- `policy show v2`

### 10.4 生成并激活新版本
- `policy sync`

行为：
- 读取当前复盘事实
- 生成新版本
- 当前账户直接以灰度方式激活

### 10.5 切换指定版本
- `policy use v1`

### 10.6 回滚
- `policy rollback`

---

## 11. 灰度与回滚口径
本阶段的灰度策略固定为：

- 只影响当前账号
- 新生成版本默认 `activation_mode = gray`
- 不自动影响其他账号
- `policy rollback` 随时可回到上一版本

这保证了：

- 策略写回可以尽快试
- 出问题能立刻退
- 不会把风险一下放大到全账号

---

## 12. Analytics 落库
新增表：

### 12.1 `policy_versions`
记录每个版本的：

- 来源
- 模式
- 摘要
- prompt 片段
- 写回规则
- 证据包

### 12.2 `policy_events`
记录：

- 同步生成
- 手动切换
- 回滚

---

## 13. 风险边界
这一阶段坚持 4 条硬边界：

1. 策略版本只影响 prompt 上下文，不改硬风控
2. 默认只做单账户灰度
3. 必须可回滚
4. 每次决策都要能追溯到具体 `policy_version`

---

## 14. 测试与回归
新增测试：

- `test_policy_sync_and_rollback`
- `test_record_decision_audit_persists_policy_fields`
- `test_predict_next_bet_v10_includes_policy_context`
- `test_process_user_command_policy_sync`

阶段回归命令：

```powershell
.\venv_win\Scripts\python.exe -m pytest tests_multiuser\test_multiuser_branch.py tests_multiuser\test_risk_history_v1.py tests_multiuser\test_dynamic_betting.py tests_multiuser\test_task_engine_v1.py tests_multiuser\test_task_package_engine_v1.py tests_multiuser\test_policy_prompt_v1.py -q
```

本阶段结果：

- `104 passed`

---

## 15. 本阶段结论
到这一阶段，系统已经形成：

- 复盘事实
- 证据包
- 策略版本
- prompt 回写
- 灰度激活
- 回滚

这意味着“复盘 -> 策略 -> 决策”的链路已经打通。

下一阶段默认进入：

- 多账号编排 V1
