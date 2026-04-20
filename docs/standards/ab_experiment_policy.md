# A/B Experiment Policy (Risk Comparison)

Version: 1.0  
Effective date: 2026-03-25  
Status: active

This policy defines how baseline and risk-enabled replay results must be compared so that conclusions are comparable, reproducible, auditable, and gateable by automation.

## 1. Scope

Applies to:

- `scripts/run_risk_ab_comparison.py`
- `tests/test_risk_ab_comparison.py`
- generated report `risk_ab_report.md`
- generated report `risk_ab_v2_report.md`
- generated machine-readable eligibility artifact `comparison_eligibility.json`
- generated reproducibility artifact `experiment_manifest.json`

Out of scope:

- risk model math implementation (`risk/model.py`, objective math, solver internals)
- production default configuration semantics

## 2. Comparison Unit (Koujing)

All comparisons use the same business-day unit under one date window:

- baseline: nightly replay dashboard with risk disabled
- variant: nightly replay dashboard with risk enabled under a tested `w5` value
- report compares baseline and variant on aligned dates within `[start_date, end_date]`

If date coverage is inconsistent, report must keep running and mark mismatch under data quality warnings.
Comparison eligibility must be evaluated before directional conclusions are produced.
Risk inputs must satisfy temporal alignment: `returns_end_date <= replay_start_date_previous_trading_day`.

## 3. Data Cleaning Rules

Dashboard row filtering:

- accept `as_of_date` first, fallback to `date`
- parse business date with `pandas.to_datetime(..., errors="coerce")`
- drop rows with invalid business date
- keep nightly rows only
- when `notes` exists and contains historical replay marks, keep `historical_replay_*` rows

Deduplication:

- duplicate business dates are deduplicated by deterministic keep-last
- sorting basis is `(_business_date ASC, _source_row_number ASC)` where `_source_row_number` is the CSV row order
- duplicate detection must be recorded as data quality warning

## 4.1 Comparison Eligibility

The report must include a `Comparison Eligibility` section.

Eligibility output:

- `eligible` or `ineligible`
- reason list
- baseline quality flag list

Minimum ineligible triggers:

- baseline duplicate dates detected
- baseline vs variant date mismatch
- insufficient overlap between baseline and variant dates

When ineligible:

- conclusion section must not output directional statements such as improved/worsened/unchanged
- use fixed wording: result is not suitable for strategy decision-making
- `--require-eligible` mode must return non-zero and emit a fixed gate failure message

Machine-readable contract (`comparison_eligibility.json`):

- `eligible`: boolean
- `reasons`: string array
- `baseline_quality_flags`: string array

Report consistency:

- `Comparison Eligibility` section status and reasons must match JSON values
- baseline quality flags listed in report must match JSON values

## 4.2 Upper Gate Integration

Upper orchestration/dashboard gate (pilot operations layer) must consume `comparison_eligibility.json` as machine-readable signal.

Dashboard aggregation requirements:

- aggregate output must include `comparison_eligibility_status`
- aggregate output must include `comparison_eligibility_reason_count`

Eligibility state machine:

- `ELIGIBLE`: JSON present, `eligible=true`
- `INELIGIBLE`: JSON present, `eligible=false`
- `INVALID`: JSON malformed or contract-invalid payload
- `NOT_AVAILABLE`: file not found in current run context

Decision-block rule:

- when `eligible=false`, upper gate marks run state as `INELIGIBLE`
- decision flow must be blocked (non-zero exit in gate path)
- release-style gate status must be treated as not passed
- A/B orchestration (`run_risk_ab_comparison.py`) must auto-pass `--ab-flow` and `--require-eligibility-gate` through replay -> ops chain by default

Boundary semantics:

- missing file in non-A/B flow: do not block, status=`NOT_AVAILABLE`
- missing file in A/B flow with required gate: block with fixed missing reason
- malformed JSON: status=`INVALID` with traceable non-zero reason count
- C11 boundary for go/no-go windows:
  - if window has only `NOT_AVAILABLE` (no `ELIGIBLE/INELIGIBLE/INVALID`), status must be `WAIVE` (not `FAIL`)

Exit-code and log semantics:

- `INELIGIBLE` -> dedicated ineligible exit code and `INELIGIBLE` gate log
- `INVALID` -> dedicated invalid exit code and `INVALID` gate log
- `NOT_AVAILABLE` with required A/B gate -> dedicated missing exit code and `MISSING` gate log

Error code table:

| Exit code | Eligibility state | Trigger |
|---|---|---|
| 21 | INELIGIBLE | `eligible=false` |
| 22 | INVALID | malformed JSON or contract-invalid payload |
| 23 | NOT_AVAILABLE (required gate) | A/B flow with required gate and missing `comparison_eligibility.json` |

## 5. Metric Mapping And Missing Fields

Metric families:

- success: `pipeline_success` or `nightly_status`
- override: `override_used_count` or `override_count`
- cost: `cost_better_ratio` or `cost_better_ratio_static`
- turnover: `turnover` or `turnover_ratio` or `avg_turnover`
- solver time: `solver_time` or `solver_time_seconds` or `solver_runtime_seconds`
- solver name: `solver_name` or `solver_primary` (for CLARABEL convergence summary)

Missing-field behavior:

- missing metric columns must not crash report generation
- missing metric values are rendered as `N/A`
- missing metric families must be listed in `Data Quality Warnings`

## 6. Data Quality Warning Policy

The report must include a dedicated `Data Quality Warnings` section.

Required warning classes:

1. baseline duplicate business dates
2. baseline/variant date coverage mismatch
3. missing metric families rendered as `N/A`

Warnings are informational for report consumers and do not change risk mathematics.

## 7. Reproducibility And Auditability

To keep runs reproducible and auditable:

- persist tested `w5` values and date range in report header
- preserve per-variant tracking output folders
- preserve variant daily-detail table with date-level baseline-vs-variant values
- keep deterministic formatting for missing values (`N/A`)
- preserve comparison eligibility status and reasons in report text
- preserve machine-readable eligibility payload for automated gating
- persist `experiment_manifest.json` with parameters, input paths, window, and experiment version
- keep V2 report sections for decision traceability:
  - `Eligibility & Data Quality Gate`
  - `Paired Daily Delta`
  - `Statistical Confidence`
  - `Decision Readiness`

## 8. Test Requirements

At minimum `tests/test_risk_ab_comparison.py` must cover:

- baseline repeated date handling (dedupe + warning)
- baseline vs variant date-range inconsistency warning
- missing metric fields rendered as `N/A` + warning section retained
- ineligible suppression of directional conclusion text
- `--require-eligible` non-zero gate behavior when ineligible
- machine-readable eligibility schema and report-value consistency
