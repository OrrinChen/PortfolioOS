# Eligibility Gate 值班手册（运行与告警处置）

Version: 1.0  
Date: 2026-03-25  
Owner: Ops On-call + Research A/B Owner

## 1. 目的与边界

本手册将 `comparison_eligibility` 相关门禁运维化，覆盖值班检查、告警分级、处置与升级流程。

适用范围：

- `outputs/**/comparison_eligibility.json`
- `outputs/**/go_nogo_status.md`
- `outputs/**/pilot_dashboard.csv`
- `outputs/**/incident_register.csv`

不在本手册范围：

- 风险模型数学实现、优化器逻辑、脚本判定逻辑改动
- 新增状态码/新增状态机规则

本手册严格复用现有契约：

- 状态机：`ELIGIBLE` / `INELIGIBLE` / `INVALID` / `NOT_AVAILABLE`
- 错误码：`21` / `22` / `23`
- C11 窗口边界：仅 `NOT_AVAILABLE` 时为 `WAIVE`（非 `PASS`）

## 2. 契约与产物字段映射

| 产物路径 | 关键字段 | 用途 |
|---|---|---|
| `outputs/**/comparison_eligibility.json` | `eligible` (bool), `reasons` (string[]), `baseline_quality_flags` (string[]) | 单次 run 的 eligibility 原始机读信号 |
| `outputs/**/pilot_dashboard.csv` | `run_root`, `comparison_eligibility_status`, `comparison_eligibility_reason_count`, `release_status`, `release_gate_passed`, `date` | 多 run 聚合面板，告警入口与窗口统计来源 |
| `outputs/**/go_nogo_status.md` | `C11_comparison_eligibility_gate` 行的 `actual`、`status`、`evidence` | 管理层可读的门禁窗口结论（PASS/FAIL/WAIVE） |
| `outputs/**/incident_register.csv` | `incident_id`, `date`, `severity`, `category`, `description`, `root_cause`, `resolution` | 事件留痕与复盘 |

状态与退出码映射（来自既有策略，不可改）：

| 状态 | 触发语义 | 退出码 |
|---|---|---:|
| `ELIGIBLE` | JSON 存在且 `eligible=true` | 0 |
| `INELIGIBLE` | JSON 存在且 `eligible=false` | 21 |
| `INVALID` | JSON malformed 或 contract-invalid | 22 |
| `NOT_AVAILABLE` | JSON 缺失（A/B required gate 场景下阻断） | 23（仅 required gate） |

## 3. 值班标准流程（每次告警统一执行）

### 3.1 T+0 到 T+5 分钟：定位与定级

1. 从 `pilot_dashboard.csv` 读取最新告警行。  
2. 记录 `run_root`、`comparison_eligibility_status`、`comparison_eligibility_reason_count`。  
3. 关联到 run 目录中的 `evaluation/comparison_eligibility.json`，确认是 `INELIGIBLE` / `INVALID` / `NOT_AVAILABLE` 哪一类。

操作映射：

| 步骤 | 产物路径 | 字段 | 通过标准 |
|---|---|---|---|
| 读取最新 run | `outputs/**/pilot_dashboard.csv` | `date`, `run_root` | 能唯一定位本次 run 路径 |
| 读取状态 | `outputs/**/pilot_dashboard.csv` | `comparison_eligibility_status` | 状态属于 4 态之一 |
| 读取原因计数 | `outputs/**/pilot_dashboard.csv` | `comparison_eligibility_reason_count` | 与 JSON/缺失场景可解释 |

### 3.2 T+5 到 T+15 分钟：证据核对

1. 若 JSON 存在，校验字段完整性：`eligible/reasons/baseline_quality_flags`。  
2. 对照 `go_nogo_status.md` 中 C11 行，确认 `actual` 计数与 `status` 一致。  
3. 确认当前窗口是否出现 `FAIL` 或 `WAIVE`，并注明“`WAIVE` 非通过态”。

操作映射：

| 步骤 | 产物路径 | 字段 | 通过标准 |
|---|---|---|---|
| JSON 契约检查 | `outputs/**/comparison_eligibility.json` | `eligible`, `reasons`, `baseline_quality_flags` | 字段类型合法且可解析 |
| C11 计数核对 | `outputs/**/go_nogo_status.md` | C11 行 `actual` | 与 CSV 窗口聚合一致 |
| C11 状态核对 | `outputs/**/go_nogo_status.md` | C11 行 `status` | 符合既有规则（FAIL/WAIVE/PASS） |

### 3.3 T+15 到 T+30 分钟：处置、升级、留痕

1. 按告警矩阵执行分流（见第 4 节）。  
2. 在 `incident_register.csv` 记录事件条目。  
3. 对外同步固定格式摘要：`状态 + 退出码 + 影响窗口 + 下一步 + 负责人 + ETA`。  
4. 若影响 release 决策，明确引用 `risk_model_release_decision.md`：保持 `risk_model=off`，不得因本次告警改变默认开关逻辑。

## 4. 告警矩阵（21/22/23）

| 退出码 | 状态 | 触发条件（严格按现有契约） | 严重级别 | 第一责任人 | 处置步骤 | 升级路径 |
|---:|---|---|---|---|---|---|
| 21 | `INELIGIBLE` | `comparison_eligibility.json` 存在且 `eligible=false`；通常 `reasons` 非空 | P2 | Ops On-call（Research 配合） | 1) 读取 JSON 中 `reasons` 与 `baseline_quality_flags`；2) 在 `pilot_dashboard.csv` 核对 `comparison_eligibility_reason_count`；3) 在 `go_nogo_status.md` 核对 C11 为 `FAIL`；4) 标记“阻断 release 决策”，等待数据/实验重跑 | 15 分钟内通知 Research A/B Owner；若连续 2 个决策窗口出现同类原因，升级到 Ops Governance |
| 22 | `INVALID` | `comparison_eligibility.json` malformed 或 contract-invalid payload | P1 | Ops On-call + Data Pipeline Owner | 1) 先确认为 JSON 解析/契约问题；2) 核对 `pilot_dashboard.csv` 为 `INVALID`；3) 核对 C11 为 `FAIL`；4) 发起修复任务（仅修复数据/产物格式，不改模型逻辑）；5) 修复后重跑同窗口并复核 | 10 分钟内拉起 Data Pipeline Owner；30 分钟未恢复则升级到 Tech Lead + Ops Governance |
| 23 | `NOT_AVAILABLE`（required gate） | A/B flow + required gate 场景下，缺失 `comparison_eligibility.json` | P1 | Ops On-call + Workflow Owner | 1) 在 `run_root/evaluation/` 确认文件缺失；2) 在 `pilot_dashboard.csv` 核对 `NOT_AVAILABLE`；3) 核对 C11（可能 `FAIL`，若窗口仅 NOT_AVAILABLE 则 `WAIVE`）；4) 补齐产物链后重跑并复核 | 10 分钟内通知 Workflow Owner；若跨 1 个交易日仍缺失，升级到 Decision Owner（Research + Ops Governance） |

补充说明：

- `WAIVE` 仅表示“窗口内只有 `NOT_AVAILABLE`”，不是 release 通过态。  
- 出现 `21/22/23` 时，不得下发“默认开启 risk_model”结论。

## 5. 日检清单模板（JSON/CSV/MD 一致性）

### 5.1 MD 模板（每日值班记录）

```md
# Eligibility Gate Daily Check - YYYY-MM-DD

- operator:
- window:
- dashboard_path: outputs/<run_group>/pilot_dashboard.csv
- go_nogo_path: outputs/<run_group>/go_nogo_status.md

## A. JSON contract checks
- [ ] A1 `outputs/**/comparison_eligibility.json` 存在（required gate 场景）
- [ ] A2 字段存在：`eligible`, `reasons`, `baseline_quality_flags`
- [ ] A3 字段类型正确：bool / string[] / string[]

## B. CSV aggregation checks
- [ ] B1 `comparison_eligibility_status` 仅出现 `ELIGIBLE/INELIGIBLE/INVALID/NOT_AVAILABLE`
- [ ] B2 `comparison_eligibility_reason_count` 与 JSON/缺失场景一致
- [ ] B3 `run_root` 可定位到对应 run 目录

## C. MD gate checks
- [ ] C1 `go_nogo_status.md` 存在 C11 行
- [ ] C2 C11 `actual` 与 CSV 聚合一致
- [ ] C3 C11 `status` 与既有语义一致（FAIL/WAIVE/PASS）

## D. Decision guardrail
- [ ] D1 若 C11 非 PASS，结论保持 `NO-GO` / `risk_model=off`
- [ ] D2 已记录 incident_register.csv（如有告警）
```

### 5.2 JSON 模板（结构化留痕）

```json
{
  "date": "YYYY-MM-DD",
  "operator": "oncall_name",
  "window": "rolling_20|pilot_to_date",
  "artifacts": {
    "dashboard_csv": "outputs/<run_group>/pilot_dashboard.csv",
    "go_nogo_md": "outputs/<run_group>/go_nogo_status.md"
  },
  "checks": [
    {
      "id": "A2_json_contract_fields",
      "result": "PASS|FAIL",
      "evidence_path": "outputs/<run>/evaluation/comparison_eligibility.json",
      "evidence_field": "eligible,reasons,baseline_quality_flags",
      "note": ""
    },
    {
      "id": "B2_csv_reason_count_consistency",
      "result": "PASS|FAIL",
      "evidence_path": "outputs/<run_group>/pilot_dashboard.csv",
      "evidence_field": "comparison_eligibility_reason_count",
      "note": ""
    },
    {
      "id": "C2_md_c11_actual_consistency",
      "result": "PASS|FAIL",
      "evidence_path": "outputs/<run_group>/go_nogo_status.md",
      "evidence_field": "C11 actual/status",
      "note": ""
    }
  ],
  "overall": "PASS|FAIL"
}
```

### 5.3 CSV 模板（批量审计）

```csv
date,operator,window,run_root,status_from_csv,reason_count_from_csv,json_path,json_eligible,json_reason_count,md_path,c11_actual,c11_status,consistency_result,remark
YYYY-MM-DD,oncall_name,rolling_20,outputs/<run>,INELIGIBLE,2,outputs/<run>/evaluation/comparison_eligibility.json,false,2,outputs/<run_group>/go_nogo_status.md,"eligible=1/4,ineligible=1/4,invalid=1/4,not_available=1/4,reasons_total=4",FAIL,PASS,
```

## 6. 30 分钟演练脚本（值班可执行）

演练目标：在 30 分钟内完成 `21/22/23` 任一场景的识别、核对、处置与升级动作。  
建议演练数据：`outputs/eligibility_gate_ops_validation_20260325/`（包含 `ELIGIBLE/INELIGIBLE/INVALID/NOT_AVAILABLE` 四种样例）。

### 0-5 分钟：场景抽签与定位

1. 指定场景：
   - 21 场景：`run_20260323_ineligible`
   - 22 场景：`run_20260324_invalid`
   - 23 场景：`run_20260325_missing`
2. 在 `pilot_dashboard.csv` 定位对应 `run_root` 与状态列。

### 5-12 分钟：证据采集

1. 打开 `run_root/evaluation/comparison_eligibility.json`（23 场景应确认缺失）。  
2. 记录 `eligible`、`reasons`、`baseline_quality_flags`（若存在）。  
3. 打开 `go_nogo_status.md`，定位 C11 行并抄录 `actual/status`。

### 12-20 分钟：执行处置

1. 按第 4 节矩阵执行对应步骤。  
2. 输出固定格式通报：
   - `incident_type=<21|22|23>`
   - `status=<INELIGIBLE|INVALID|NOT_AVAILABLE>`
   - `impact_window=<rolling_20|pilot_to_date>`
   - `action=<data_fix|contract_fix|artifact_rebuild>`
   - `owner=<name>`
   - `eta=<time>`

### 20-26 分钟：留痕与升级

1. 在 `incident_register.csv` 新增演练记录（可标记 `category=drill`）。  
2. 按矩阵执行升级路径（模拟通知 Research/Data Pipeline/Workflow Owner）。

### 26-30 分钟：复盘结论

1. 输出 3 条复盘：
   - 根因是否可从产物字段直接定位
   - JSON/CSV/MD 是否一致
   - 是否保持 `risk_model_release_decision` 的 `NO-GO` guardrail
2. 演练通过标准：
   - 30 分钟内完成状态识别与证据链闭环
   - 所有处置步骤均可映射到“路径 + 字段”
   - 未引入新状态码/新规则

## 7. 与发布决策条款的一致性核对

与 `docs/standards/risk_model_release_decision.md` 一致点：

- 当前默认结论保持 `NO-GO`，`risk_model` 默认 `off`。  
- 只有当 `comparison_eligibility.json.eligible=true` 且 C11 为 `PASS` 时，才具备进入默认开启讨论的前提。  
- `WAIVE` 明确为非通过态，不可视作 release-ready。  

与 `docs/standards/ab_experiment_policy.md` 一致点：

- 状态机与退出码完全一致（`ELIGIBLE/INELIGIBLE/INVALID/NOT_AVAILABLE`，`21/22/23`）。  
- `INELIGIBLE`/`INVALID`/required-gate `NOT_AVAILABLE` 均阻断门禁结论。  
- 本手册不新增判定规则，不变更脚本逻辑，仅定义运维处置动作。
