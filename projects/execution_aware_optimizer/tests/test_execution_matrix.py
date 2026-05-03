from __future__ import annotations

import json

from execution_aware_optimizer.execution_matrix import run_execution_matrix
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.robustness_summary import (
    render_execution_matrix_report,
    summarize_execution_matrix,
)
from execution_aware_optimizer.scenario_grid import build_scenario_grid


def test_scenario_grid_records_stable_source_config_hashes() -> None:
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [5, 25],
            "execution_matrix": {
                "participation_rates": [0.001],
                "liquidity_buckets": ["high"],
                "constraint_levels": ["raw"],
                "execution_modes": ["impact_aware"],
            },
        }
    )

    first = build_scenario_grid(config)
    second = build_scenario_grid(config)

    assert [scenario.scenario_id for scenario in first] == [
        "cost_5bps__participation_0p001__liquidity_high__constraint_raw__execution_impact_aware",
        "cost_25bps__participation_0p001__liquidity_high__constraint_raw__execution_impact_aware",
    ]
    assert [scenario.source_config_hash for scenario in first] == [
        scenario.source_config_hash for scenario in second
    ]
    assert all(len(scenario.source_config_hash) == 64 for scenario in first)


def test_execution_matrix_preserves_unavailable_rows_and_scenario_metadata() -> None:
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [5],
            "layers": [{"layer_name": "full_execution_aware_cost_adjusted"}],
            "execution_matrix": {
                "participation_rates": [0.005],
                "liquidity_buckets": ["medium"],
                "constraint_levels": ["full_execution_aware"],
                "execution_modes": ["participation_twap"],
            },
        }
    )

    rows = run_execution_matrix(config)

    assert len(rows) == 1
    row = rows[0]
    assert row.cost_bps == 5
    assert row.participation_rate == 0.005
    assert row.liquidity_bucket == "medium"
    assert row.constraint_level == "full_execution_aware"
    assert row.execution_mode == "participation_twap"
    assert row.layer_name == "full_execution_aware_cost_adjusted"
    assert row.status == "unavailable"
    assert row.unavailable_reason == row.infeasibility_reason
    assert "PortfolioOS run disabled by config" in row.unavailable_reason
    assert row.explanation is not None
    assert row.explanation["primary_reason"] == "q2_adapter_unavailable"
    assert row.explanation["decision"] == "unavailable"
    assert row.net_return is None


def test_robustness_summary_counts_unavailable_reasons_and_serializes() -> None:
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [1],
            "layers": [{"layer_name": "raw_top_alpha_equal_weight"}],
            "execution_matrix": {
                "participation_rates": [0.001],
                "liquidity_buckets": ["low"],
                "constraint_levels": ["raw"],
                "execution_modes": ["impact_aware"],
            },
        }
    )
    rows = run_execution_matrix(config)

    summary = summarize_execution_matrix(rows)
    payload = summary.model_dump(mode="json")

    assert summary.total_scenarios == 1
    assert summary.total_rows == 1
    assert summary.observed_rows == 0
    assert summary.unavailable_rows == 1
    assert summary.unique_source_config_hashes == 1
    expected_reason = (
        "PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true "
        "to execute the configured backtest adapter explicitly."
    )
    assert payload["unavailable_reason_counts"] == {expected_reason: 1}
    assert json.loads(summary.to_deterministic_json()) == payload


def test_execution_matrix_report_sections_do_not_fabricate_results() -> None:
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [5],
            "layers": [{"layer_name": "full_execution_aware_cost_adjusted"}],
            "execution_matrix": {
                "participation_rates": [0.001],
                "liquidity_buckets": ["high"],
                "constraint_levels": ["full_execution_aware"],
                "execution_modes": ["impact_aware"],
            },
        }
    )
    rows = run_execution_matrix(config)
    summary = summarize_execution_matrix(rows)

    report = render_execution_matrix_report(rows, summary=summary)

    assert "## Scenario Coverage" in report
    assert "| total_scenarios | total_rows | observed_rows | unavailable_rows |" in report
    assert "| 1 | 1 | 0 | 1 |" in report
    assert "Not available" in report
    assert "PortfolioOS run disabled by config" in report
