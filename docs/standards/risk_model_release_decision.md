# Risk Model Release Decision (A/B V2)

Version: 1.0  
Date: 2026-03-25  
Decision owner: Research + Ops Governance

## 1) Decision Summary

Current release decision: **NO-GO (do not enable by default)**.

- `risk_model` default recommendation: `off`
- `w5` default recommendation: **暂不建议固定默认值**
- Rationale: available A/B V2 evidence is `ineligible` and upper-gate signal includes `C11 FAIL/WAIVE`; this does not satisfy decision-ready criteria in `ab_experiment_policy`.

## 2) Evidence Table

| Evidence file | Field / metric | Observed value | Decision implication |
|---|---|---|---|
| `outputs/risk_ab_comparison/sample_v2_decision_validation/risk_ab_v2_report.md` | `Decision Readiness` | `Ineligible` | Not decision-ready; block default-on decision |
| `outputs/risk_ab_comparison/sample_v2_decision_validation/risk_ab_v2_report.md` | `sample_scope_check` | `FAIL` | Baseline/risk sample scope mismatch; comparability broken |
| `outputs/risk_ab_comparison/sample_v2_decision_validation/risk_ab_v2_report.md` | `time_alignment_check` | `PASS` (`returns_end_date=2026-03-18 <= required_max_end_date=2026-03-18`) | Temporal alignment requirement is met, but not sufficient for release |
| `outputs/risk_ab_comparison/sample_v2_decision_validation/comparison_eligibility.json` | `eligible` | `false` | Per policy, ineligible result cannot support directional production decision |
| `outputs/risk_ab_comparison/sample_v2_decision_validation/comparison_eligibility.json` | `reasons` | `baseline duplicate dates detected`; `w5=0.2: date mismatch`; `insufficient overlap (1/2, ratio=0.50)` | Baseline contamination + overlap insufficiency invalidate A/B comparability |
| `outputs/risk_ab_comparison/sample_v2_decision_validation/risk_ab_v2_report.md` | `Statistical Confidence` | `turnover_ci95 = N/A`, `cost_ci95` inconclusive | No statistically reliable support for default-on/w5 fixation |
| `outputs/eligibility_gate_ops_validation_20260325/go_nogo_status.md` | `C11_comparison_eligibility_gate` | `FAIL` (`eligible=1/4, ineligible=1/4, invalid=1/4, not_available=1/4`) | Upper gate is not pass state; release should be blocked |
| `outputs/eligibility_gate_semantics_c11/go_nogo_status.md` | `C11_comparison_eligibility_gate` | `WAIVE` (NOT_AVAILABLE-only window) | WAIVE is non-pass for release readiness; indicates missing A/B evidence window |
| `docs/standards/ab_experiment_policy.md` | Policy clauses | Ineligible => no directional conclusion; C11 is mandatory gate signal | Confirms conservative default-off decision is policy-compliant |

## 3) Default Config Recommendation (on/off + w5)

- Default `risk_model`: **off**
- Default `w5`: **do not pin a production default yet**
- Conditional enabling rule:
  - enable candidate only after **eligible=true** and **C11 PASS** in release window, with paired-delta confidence not inconclusive for primary metrics.

## 4) Rollout Plan (10% -> 30% -> 100%)

This is a **conditional plan**. Current state remains at 0% until entry criteria pass.

Entry criteria before 10%:

- A/B V2 `Decision Readiness = Eligible`
- `comparison_eligibility.json.eligible = true`
- `go_nogo_status.md` C11 status = `PASS` (no `FAIL`, no `WAIVE` in decision window)
- no baseline duplicate-date warning in active experiment window
- paired-delta confidence available for key metrics (no `N/A` on primary decision metrics)

Stage rollout:

1. 10% canary (>=5 trading days)
- Monitor: eligibility reasons count, C11 status, release pass rate, incident counts.

2. 30% controlled rollout (next >=10 trading days)
- Promote only if no rollback trigger fired in 10% stage.

3. 100% rollout (after cumulative >=20 trading days)
- Promote only if all gate checks remain stable and decision metrics remain non-degraded.

## 5) Rollback Triggers (explicit thresholds)

Immediate rollback to `risk_model=off` if any of the following occurs:

- `comparison_eligibility.json.eligible = false` in release evaluation run.
- `go_nogo_status.md` C11 status = `FAIL` in active decision window.
- `go_nogo_status.md` C11 status = `WAIVE` for >=2 consecutive decision windows.
- time alignment violation (`returns_end_date > replay_start_date_previous_trading_day`) in manifest/check output.
- release gate pass rate (`C02_release_pass_rate`) < 95% in active rollout window.

## 6) Open Risks And Follow-ups

Open risks:

- Baseline data quality risk: duplicate business dates detected in current evidence set.
- Sample comparability risk: variant date coverage mismatch (`insufficient overlap`).
- Statistical power risk: key paired-delta confidence currently inconclusive or missing (`N/A`).

Follow-ups:

1. Re-run A/B V2 with clean baseline window (dedup-confirmed) and full date overlap for all tested `w5`.
2. Ensure each candidate `w5` has sufficient paired days for CI interpretation on primary metrics.
3. Re-evaluate release decision only after fresh artifacts show `Eligible + C11 PASS`.

