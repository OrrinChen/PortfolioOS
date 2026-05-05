from __future__ import annotations

import json
from pathlib import Path

from execution_aware_optimizer.sue_execution_survival_attribution import (
    build_sue_execution_survival_attribution,
    render_sue_execution_survival_attribution_report,
    write_sue_execution_survival_attribution_artifacts,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import (
    SueTypedQ2SurvivalResult,
    SueTypedQ2SurvivalRow,
)


def _row(
    *,
    layer: str,
    status: str,
    gross_return: float | None = None,
    net_return: float | None = None,
    turnover: float | None = None,
    cost_drag: float | None = None,
    gross_to_net_retention: float | None = None,
    unavailable_reason: str | None = None,
) -> SueTypedQ2SurvivalRow:
    return SueTypedQ2SurvivalRow(
        scenario_id=f"scenario_{layer}_{status}",
        layer=layer,
        date="2026-02-27",
        status=status,
        active_rebalance_count=1,
        active_name_count=2,
        expected_return_used_share=2 / 3,
        gross_return=gross_return,
        net_return=net_return,
        turnover=turnover,
        cost_drag=cost_drag,
        gross_to_net_retention=gross_to_net_retention,
        unavailable_reason=unavailable_reason,
        source_config_hash="row-hash",
    )


def _partial_observed_result() -> SueTypedQ2SurvivalResult:
    return SueTypedQ2SurvivalResult(
        run_id="sue-survival-attribution-fixture",
        survival_status="partially_observed",
        injection_status="injected",
        expected_return_reached_optimizer_input=True,
        optimizer_rebalance_date="2026-02-27",
        original_projection_dates=["2025-02-10"],
        local_rebalance_date="2026-02-27",
        active_rebalance_count=1,
        active_name_count=2,
        expected_return_used_share=2 / 3,
        q2_observed_rows=2,
        q2_unavailable_rows=1,
        matrix_rows=[
            _row(
                layer="raw_top_alpha_equal_weight",
                status="observed",
                gross_return=-0.01,
                net_return=-0.011,
                turnover=0.4,
                cost_drag=0.001,
                gross_to_net_retention=1.1,
            ),
            _row(
                layer="full_execution_aware_cost_adjusted",
                status="observed",
                gross_return=0.012,
                net_return=0.011,
                turnover=0.05,
                cost_drag=0.001,
                gross_to_net_retention=0.92,
            ),
            _row(
                layer="risk_controlled",
                status="unavailable",
                unavailable_reason="No stable PortfolioOS adapter exists yet for this intermediate layer.",
            ),
        ],
        source_config_hash="source-hash",
    )


def _observed_result() -> SueTypedQ2SurvivalResult:
    return SueTypedQ2SurvivalResult(
        run_id="sue-survival-attribution-observed-fixture",
        survival_status="observed",
        injection_status="injected",
        expected_return_reached_optimizer_input=True,
        optimizer_rebalance_date="2026-02-27",
        original_projection_dates=["2025-02-10"],
        local_rebalance_date="2026-02-27",
        active_rebalance_count=1,
        active_name_count=2,
        expected_return_used_share=2 / 3,
        q2_observed_rows=3,
        q2_unavailable_rows=0,
        matrix_rows=[
            _row(
                layer="raw_top_alpha_equal_weight",
                status="observed",
                gross_return=0.01,
                net_return=0.009,
                turnover=0.4,
                cost_drag=0.001,
                gross_to_net_retention=0.9,
            ),
            _row(
                layer="risk_controlled",
                status="observed",
                gross_return=0.011,
                net_return=0.010,
                turnover=0.3,
                cost_drag=0.001,
                gross_to_net_retention=0.91,
            ),
            _row(
                layer="full_execution_aware_cost_adjusted",
                status="observed",
                gross_return=0.012,
                net_return=0.011,
                turnover=0.05,
                cost_drag=0.001,
                gross_to_net_retention=0.92,
            ),
        ],
        source_config_hash="source-hash",
    )


def test_attribution_explains_partial_observed_sue_without_overclaiming() -> None:
    attribution = build_sue_execution_survival_attribution(_partial_observed_result())

    assert attribution.decision_label == "sue_q2_inconclusive"
    assert attribution.primary_stop_layer == "unavailable_local_fixture_hook"
    assert attribution.phase52_revision_marginal_value_should_proceed is True
    assert attribution.alpha_failure_detected is False
    assert attribution.execution_failure_detected is False
    assert attribution.projection_sparsity_detected is False
    assert attribution.optimizer_failure_detected is False
    assert attribution.production_approval_claimed is False

    layers = {layer.layer_name: layer for layer in attribution.layer_attribution}
    assert layers["evidence"].status == "passed"
    assert layers["projection"].status == "observed"
    assert layers["injection"].status == "observed"
    assert layers["optimizer_response"].status == "observed"
    assert layers["unavailable_local_fixture_hook"].status == "unavailable"
    assert "risk_controlled" in layers["unavailable_local_fixture_hook"].details


def test_attribution_labels_all_observed_local_sue_without_production_approval() -> None:
    attribution = build_sue_execution_survival_attribution(_observed_result())

    assert attribution.decision_label == "sue_q2_observed_survives"
    assert attribution.primary_stop_layer == "none"
    assert attribution.phase52_revision_marginal_value_should_proceed is True
    assert attribution.alpha_failure_detected is False
    assert attribution.execution_failure_detected is False
    assert attribution.production_approval_claimed is False

    layers = {layer.layer_name: layer for layer in attribution.layer_attribution}
    assert "unavailable_local_fixture_hook" not in layers
    assert layers["constraint_repair"].status == "observed"
    assert layers["cost"].status == "observed"
    assert layers["turnover"].status == "observed"


def test_attribution_blocks_revision_when_injection_is_unavailable() -> None:
    result = _partial_observed_result().model_copy(
        update={
            "survival_status": "unavailable",
            "injection_status": "unavailable",
            "expected_return_reached_optimizer_input": False,
            "q2_observed_rows": 0,
            "q2_unavailable_rows": 0,
            "matrix_rows": [],
            "unavailable_reason": "typed expected-return injection unavailable",
        }
    )

    attribution = build_sue_execution_survival_attribution(result)

    assert attribution.decision_label == "sue_q2_injection_unavailable"
    assert attribution.primary_stop_layer == "injection"
    assert attribution.phase52_revision_marginal_value_should_proceed is False
    assert attribution.optimizer_failure_detected is False
    assert attribution.projection_sparsity_detected is False


def test_attribution_writer_outputs_json_and_markdown_without_production_approval_claim(tmp_path: Path) -> None:
    attribution = build_sue_execution_survival_attribution(_partial_observed_result())

    artifacts = write_sue_execution_survival_attribution_artifacts(
        attribution,
        output_dir=tmp_path / "outputs",
        report_path=tmp_path / "reports" / "sue_typed_q2_survival_attribution.md",
    )

    assert artifacts["json"].name == "failure_attribution.json"
    assert artifacts["report"].name == "sue_typed_q2_survival_attribution.md"
    payload = json.loads(artifacts["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "sue_execution_survival_attribution.v1"
    assert payload["decision_label"] == "sue_q2_inconclusive"
    assert payload["phase52_revision_marginal_value_should_proceed"] is True

    report = artifacts["report"].read_text(encoding="utf-8")
    assert "What This Proves" in report
    assert "What This Does Not Prove" in report
    assert "Alpha Failure vs Execution Failure" in report
    assert "Projection Sparsity vs Optimizer Response" in report
    assert "Phase 52 Recommendation" in report
    assert "production approval: not claimed" in report
    assert "production approval: approved" not in report
    assert "broker_output" not in report
    assert "recommended_trade" not in report
    assert "production_alpha_approved" not in report


def test_report_renderer_states_phase52_recommendation() -> None:
    attribution = build_sue_execution_survival_attribution(_partial_observed_result())

    report = render_sue_execution_survival_attribution_report(attribution)

    assert "Proceed to Phase 52" in report
    assert "marginal-value diagnostic" in report
