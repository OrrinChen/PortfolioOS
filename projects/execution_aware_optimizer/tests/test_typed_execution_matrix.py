from __future__ import annotations

from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.typed_execution_matrix import (
    render_typed_execution_matrix_report,
    run_typed_alpha_execution_matrix,
    summarize_typed_execution_matrix,
)


def _q2_input_contract_v2() -> dict[str, object]:
    return {
        "alpha_abstain_report_artifact": "alpha_abstain_report.json",
        "alpha_projection_diagnostics_artifact": "alpha_projection_diagnostics.json",
        "alpha_view_id": "AV-US-SUE-PEAD-001",
        "allowed_consumer": "projects/execution_aware_optimizer",
        "bundle_id": "EB-GUIDANCE-RAISE-DRIFT-001",
        "direct_q2_execution_allowed": False,
        "expected_return_panel_artifact": "expected_return_panel.csv",
        "input_type": "projected_expected_return_panel",
        "projection_manifest_hash": "projection-hash-fixture",
    }


def _projection_artifacts() -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    projection_manifest = {
        "abstain_row_count": 1,
        "alpha_view_ids": ["AV-US-SUE-PEAD-001"],
        "content_hash": "projection-hash-fixture",
        "diagnostic_row_count": 1,
        "panel_row_count": 2,
        "rebalance_dates": ["2025-02-10"],
        "risk_horizon_days": 21,
        "schema_version": "alpha_projection.v2",
        "universe_symbols": ["AAPL", "MSFT", "TSLA"],
    }
    expected_return_panel = [
        {
            "active_alpha_views": "AV-US-SUE-PEAD-001",
            "date": "2025-02-10",
            "expected_return": 0.005,
            "symbol": "AAPL",
        },
        {
            "active_alpha_views": "AV-US-SUE-PEAD-001",
            "date": "2025-02-10",
            "expected_return": 0.002,
            "symbol": "MSFT",
        },
    ]
    diagnostics = [
        {
            "active_views": ["AV-US-SUE-PEAD-001"],
            "abstained_views": [],
            "coverage_count": 2,
            "date": "2025-02-10",
            "final_expected_return_scale": {"AAPL": 0.005, "MSFT": 0.002},
        }
    ]
    abstain_report = [
        {
            "alpha_view_id": "AV-US-SUE-PEAD-001",
            "date": "2025-02-10",
            "family_id": "US_EVENT_SUE",
            "reason": "coverage_missing",
            "symbol": "TSLA",
        }
    ]
    return projection_manifest, expected_return_panel, diagnostics, abstain_report


def test_typed_execution_matrix_consumes_q2_input_contract_v2_without_fabricating_returns() -> None:
    projection_manifest, expected_return_panel, diagnostics, abstain_report = _projection_artifacts()
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [5],
            "execution_matrix": {
                "participation_rates": [0.001],
                "liquidity_buckets": ["high"],
                "constraint_levels": ["full_execution_aware"],
                "execution_modes": ["impact_aware"],
            },
        }
    )

    rows = run_typed_alpha_execution_matrix(
        config=config,
        q2_input_contract_v2=_q2_input_contract_v2(),
        projection_manifest=projection_manifest,
        expected_return_panel=expected_return_panel,
        projection_diagnostics=diagnostics,
        alpha_abstain_report=abstain_report,
        projection_policies=["event_window_decay"],
        abstain_policies=["explicit_abstain"],
        alpha_families=["SUE"],
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.q2_input_contract_version == "v2"
    assert row.status == "unavailable"
    assert row.gross_to_net_retention is None
    assert row.turnover is None
    assert row.cost_drag is None
    assert row.active_rebalance_count == 1
    assert row.active_name_count == 2
    assert row.abstain_count == 1
    assert row.expected_return_used_share == 2 / 3
    assert row.source_config_hash
    assert "typed Q2 execution adapter is not implemented" in row.unavailable_reason


def test_typed_execution_matrix_hash_includes_projection_dimensions() -> None:
    projection_manifest, expected_return_panel, diagnostics, abstain_report = _projection_artifacts()
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [5],
            "execution_matrix": {
                "participation_rates": [0.001],
                "liquidity_buckets": ["medium"],
                "constraint_levels": ["raw"],
                "execution_modes": ["impact_aware"],
            },
        }
    )

    rows = run_typed_alpha_execution_matrix(
        config=config,
        q2_input_contract_v2=_q2_input_contract_v2(),
        projection_manifest=projection_manifest,
        expected_return_panel=expected_return_panel,
        projection_diagnostics=diagnostics,
        alpha_abstain_report=abstain_report,
        projection_policies=["event_window_only", "event_window_decay"],
        abstain_policies=["explicit_abstain"],
        alpha_families=["SUE"],
    )

    assert len(rows) == 2
    assert rows[0].projection_policy != rows[1].projection_policy
    assert rows[0].source_config_hash != rows[1].source_config_hash


def test_typed_execution_report_explains_cost_constraint_coverage_and_abstain_consumption() -> None:
    projection_manifest, expected_return_panel, diagnostics, abstain_report = _projection_artifacts()
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [25],
            "execution_matrix": {
                "participation_rates": [0.005],
                "liquidity_buckets": ["low"],
                "constraint_levels": ["full_execution_aware"],
                "execution_modes": ["participation_twap"],
            },
        }
    )
    rows = run_typed_alpha_execution_matrix(
        config=config,
        q2_input_contract_v2=_q2_input_contract_v2(),
        projection_manifest=projection_manifest,
        expected_return_panel=expected_return_panel,
        projection_diagnostics=diagnostics,
        alpha_abstain_report=abstain_report,
        projection_policies=["event_window_decay"],
        abstain_policies=["explicit_abstain"],
        alpha_families=["SUE"],
    )
    summary = summarize_typed_execution_matrix(rows)
    report = render_typed_execution_matrix_report(rows, summary=summary)

    assert summary.total_rows == 1
    assert summary.unavailable_rows == 1
    assert "cost assumptions" in report
    assert "constraint level" in report
    assert "expected-return used share" in report
    assert "abstain count" in report
    assert "Not available" in report
