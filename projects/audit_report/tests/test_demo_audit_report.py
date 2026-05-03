from __future__ import annotations

from pathlib import Path

from audit_report import build_demo_audit_report, load_demo_audit_manifest, write_demo_audit_report


REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "projects" / "audit_report" / "examples" / "demo_audit_manifest.yaml"
FORBIDDEN_OUTPUT_TERMS = {
    "broker_output",
    "live_performance",
    "trading_instruction",
    "realized_return",
    "alpha_performance",
}


def test_demo_audit_report_contains_required_sections_and_boundaries() -> None:
    report = build_demo_audit_report(REPO_ROOT, load_demo_audit_manifest(MANIFEST_PATH))

    assert report.promoted_case_ran_q2 is True
    assert report.rejected_case_ran_q2 is False
    assert report.markdown.startswith("# PortfolioOS Demo Audit Report\n")
    for section in (
        "## 1. Hypothesis",
        "## 2. Signal Contract",
        "## 3. Point-in-Time Safety",
        "## 4. Leakage Checks",
        "## 5. Evaluation Plan",
        "## 6. Promotion Decision",
        "## 7. Execution-Aware Evaluation",
        "## 8. Cost Sensitivity",
        "## 9. Constraint Diagnostics",
        "## 10. Final Decision",
        "## 11. Reproducibility Manifest",
    ):
        assert section in report.markdown
    assert "Rejected leakage case" in report.markdown
    assert "Q2 execution evaluation: skipped because promotion decision is `reject`." in report.markdown
    assert "sidecar_path: `reports/demo_run_manifest.json`" in report.markdown
    assert "Not available" in report.markdown
    assert _has_no_forbidden_outputs(report.markdown)


def test_demo_audit_report_is_deterministic() -> None:
    manifest = load_demo_audit_manifest(MANIFEST_PATH)

    first = build_demo_audit_report(REPO_ROOT, manifest)
    second = build_demo_audit_report(REPO_ROOT, manifest)

    assert first.markdown == second.markdown


def test_demo_audit_report_matches_golden_output() -> None:
    report = build_demo_audit_report(REPO_ROOT, load_demo_audit_manifest(MANIFEST_PATH))

    assert report.markdown == EXPECTED_DEMO_AUDIT_REPORT


def test_write_demo_audit_report_writes_requested_path(tmp_path: Path) -> None:
    output_path = tmp_path / "demo_audit_report.md"

    written_path = write_demo_audit_report(
        output_path,
        repo_root=REPO_ROOT,
        manifest=load_demo_audit_manifest(MANIFEST_PATH),
    )

    assert written_path == output_path
    assert output_path.read_text(encoding="utf-8").startswith("# PortfolioOS Demo Audit Report\n")


def _has_no_forbidden_outputs(markdown: str) -> bool:
    lowered = markdown.lower()
    return all(term not in lowered for term in FORBIDDEN_OUTPUT_TERMS)


EXPECTED_DEMO_AUDIT_REPORT = """# PortfolioOS Demo Audit Report

## 1. Hypothesis

### Promoted-like case

- hypothesis_id: `H-SEC-GUIDANCE-RAISE-001`
- title: `Guidance Raise Post-Event Drift`
- expected_horizon: `5-20 trading days`

### Rejected leakage case

- evidence_bundle: `rejected_bundle_forward_leakage`
- status: `reject`

## 2. Signal Contract

- signal_name: `guidance_raise_drift`
- output_column: `alpha_score`
- timestamp_column: `event_available_timestamp`
- no_future_data_required: `True`

## 3. Point-in-Time Safety

- event_available_timestamp_present: `True`
- signal_timestamp_present: `True`
- anchor_trade_date_present: `True`
- anchor_not_before_event_available: `True`
- anchor_not_before_signal_timestamp: `True`
- point_in_time_inputs_only: `True`

## 4. Leakage Checks

| check_name | passed | severity | details |
|---|---:|---|---|
| no_forward_return_features | True | critical | Required signal inputs exclude realized or forward return labels. |
| timestamp_alignment | True | critical | Event availability and signal timestamps are no later than the anchor trade date. |

Rejected leakage case explanation:

| decision | primary_reason | severity | human_readable | fix_hint |
|---|---|---|---|---|
| reject | forward_return_feature_leakage | critical | The candidate uses future return information as an input, so it is not a point-in-time signal. | Remove future-return fields from the signal contract and rebuild the evidence. |

## 5. Evaluation Plan

- entry_rule: `Enter at the next open after event_available_timestamp when the symbol is tradable and in universe.`
- holding_windows: `['1d', '5d', '20d']`
- benchmark: `sector_neutral_equal_weight_event_cohort`
- cost_assumptions: `{'commission_bps': 1.0, 'half_spread_bps': 5.0, 'slippage_bps': 5.0}`

## 6. Promotion Decision

### Promoted-like case

- decision: `promote_to_execution_eval`
- reasons: `['all promotion preconditions satisfied']`
- q2_allowed_columns: `['date', 'symbol', 'alpha_score', 'alpha_source', 'alpha_confidence']`

### Rejected leakage case

- decision: `reject`
- reasons: `['forward-return leakage in required_columns: realized_forward_return_5d']`
- Q2 execution evaluation: skipped because promotion decision is `reject`.

## 7. Execution-Aware Evaluation

| total_scenarios | total_rows | observed_rows | unavailable_rows |
|---:|---:|---:|---:|
| 1 | 1 | 0 | 1 |

| scenario_id | layer | status | net_return | explanation |
|---|---|---|---:|---|
| cost_5bps__participation_0p001__liquidity_high__constraint_full_execution_aware__execution_impact_aware | full_execution_aware_cost_adjusted | unavailable | Not available | q2_adapter_unavailable |

## 8. Cost Sensitivity

- configured_cost_bps: `[5]`
- result: `Not available until explicit Q2 execution is enabled.`

## 9. Constraint Diagnostics

- binding_constraints: `[]`
- rejected_symbols: `[]`
- infeasible_rebalance_dates: `[]`
- todos: `['PortfolioOS optimizer dual values / shadow prices are not exposed yet; report slack/usage metrics instead.', 'Add explicit liquidity constraint usage once PortfolioOS exports per-name participation slack.', 'Add risk exposure attribution once PortfolioOS exports rebalance-level factor exposures in a stable schema.']`

## 10. Final Decision

- promoted_like_case: `Promoted to execution-evaluation contract, but default Q2 execution remains unavailable.`
- rejected_leakage_case: `Rejected before Q2 execution evaluation.`
- no_fabricated_results: `True`

## 11. Reproducibility Manifest

- manifest_status: `available_as_sidecar_after_script_run`
- sidecar_path: `reports/demo_run_manifest.json`
- recorded_fields: `git, command, config, inputs, outputs, environment, random_seed, schema_version`
- command: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md`
- live_services: `not used`
"""
