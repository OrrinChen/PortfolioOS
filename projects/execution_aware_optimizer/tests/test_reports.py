from __future__ import annotations

from datetime import date

from execution_aware_optimizer.cost_sensitivity import CostSensitivityResultRow
from execution_aware_optimizer.diagnostics import ConstraintDiagnostics
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import LadderResultRow
from execution_aware_optimizer.reports import render_execution_aware_optimizer_report


def test_report_renders_gross_net_and_alpha_decay_summary_tables() -> None:
    config = ExperimentConfig.model_validate({})
    rows = [
        LadderResultRow(
            layer_name="raw_top_alpha_equal_weight",
            date=date(2026, 2, 28),
            gross_return=0.030,
            net_return=0.025,
            turnover=0.40,
            estimated_transaction_cost=0.005,
        ),
        LadderResultRow(
            layer_name="raw_top_alpha_equal_weight",
            date=date(2026, 3, 31),
            gross_return=0.020,
            net_return=0.018,
            turnover=0.20,
            estimated_transaction_cost=0.002,
        ),
        LadderResultRow(
            layer_name="full_execution_aware_cost_adjusted",
            date=date(2026, 2, 28),
            gross_return=0.022,
            net_return=0.020,
            turnover=0.21,
            estimated_transaction_cost=0.002,
        ),
        LadderResultRow(
            layer_name="risk_controlled",
            infeasibility_reason="No stable PortfolioOS adapter exists yet.",
        ),
    ]

    report = render_execution_aware_optimizer_report(
        config=config,
        alpha_report=None,
        ladder_rows=rows,
        diagnostics=ConstraintDiagnostics(),
    )

    assert "| layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |" in report
    assert "| raw_top_alpha_equal_weight | 2 | 0.025000 | 0.021500 | 0.003500 | 0.300000 | 0 |" in report
    assert "| full_execution_aware_cost_adjusted | 1 | 0.022000 | 0.020000 | 0.002000 | 0.210000 | 0 |" in report
    assert "| risk_controlled | 0 | Not available | Not available | Not available | Not available | 1 |" in report
    assert "| full_execution_aware_cost_adjusted | 0.020000 | 0.001500 | 0.002000 |" in report


def test_report_does_not_fabricate_summary_values_for_unavailable_rows() -> None:
    config = ExperimentConfig.model_validate({})
    rows = [
        LadderResultRow(
            layer_name="turnover_constrained",
            infeasibility_reason="No stable PortfolioOS adapter exists yet.",
        )
    ]

    report = render_execution_aware_optimizer_report(
        config=config,
        alpha_report=None,
        ladder_rows=rows,
        diagnostics=ConstraintDiagnostics(),
    )

    assert "| turnover_constrained | 0 | Not available | Not available | Not available | Not available | 1 |" in report
    assert "Alpha decay cannot be summarized until the raw layer has net return observations." in report


def test_report_renders_cost_sensitivity_summary_for_executed_rows() -> None:
    config = ExperimentConfig.model_validate({})
    cost_rows = [
        CostSensitivityResultRow(
            cost_bps=5,
            transaction_cost_objective_mode="nav_fraction",
            layer_name="full_execution_aware_cost_adjusted",
            date=date(2026, 2, 28),
            gross_return=0.020,
            net_return=0.018,
            turnover=0.21,
            estimated_transaction_cost=0.002,
        ),
        CostSensitivityResultRow(
            cost_bps=25,
            transaction_cost_objective_mode="nav_fraction",
            layer_name="full_execution_aware_cost_adjusted",
            date=date(2026, 2, 28),
            gross_return=0.015,
            net_return=0.010,
            turnover=0.18,
            estimated_transaction_cost=0.005,
        ),
        CostSensitivityResultRow(
            cost_bps=50,
            transaction_cost_objective_mode="nav_fraction",
            layer_name="full_execution_aware_cost_adjusted",
            infeasibility_reason="Configured but not executed.",
        ),
    ]

    report = render_execution_aware_optimizer_report(
        config=config,
        alpha_report=None,
        ladder_rows=[],
        diagnostics=ConstraintDiagnostics(),
        cost_sensitivity_rows=cost_rows,
    )

    assert "| cost_bps | layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |" in report
    assert "| 5 | full_execution_aware_cost_adjusted | 1 | 0.020000 | 0.018000 | 0.002000 | 0.210000 | 0 |" in report
    assert "| 25 | full_execution_aware_cost_adjusted | 1 | 0.015000 | 0.010000 | 0.005000 | 0.180000 | 0 |" in report
    assert "| 50 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |" in report


def test_report_renders_adapter_status_and_layer_coverage_for_executed_fixture_rows() -> None:
    config = ExperimentConfig.model_validate(
        {
            "portfolioos": {
                "allow_portfolioos_run": True,
                "backtest_manifest": "data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml",
            }
        }
    )
    rows = [
        LadderResultRow(
            layer_name="raw_top_alpha_equal_weight",
            date=date(2026, 2, 28),
            gross_return=0.030,
            net_return=0.025,
            turnover=0.40,
            estimated_transaction_cost=0.005,
        ),
        LadderResultRow(
            layer_name="risk_controlled",
            infeasibility_reason="No stable PortfolioOS adapter exists yet.",
        ),
        LadderResultRow(
            layer_name="full_execution_aware_cost_adjusted",
            date=date(2026, 2, 28),
            gross_return=0.022,
            net_return=0.020,
            turnover=0.21,
            estimated_transaction_cost=0.002,
        ),
    ]

    report = render_execution_aware_optimizer_report(
        config=config,
        alpha_report=None,
        ladder_rows=rows,
        diagnostics=ConstraintDiagnostics(),
    )

    assert "- PortfolioOS adapter execution: `enabled`" in report
    assert "| layer | observed_rows | unavailable_rows | coverage_status |" in report
    assert "| raw_top_alpha_equal_weight | 1 | 0 | observed |" in report
    assert "| risk_controlled | 0 | 1 | unavailable |" in report
    assert "| full_execution_aware_cost_adjusted | 1 | 0 | observed |" in report
