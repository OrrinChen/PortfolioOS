from __future__ import annotations

from pathlib import Path

import pytest

from execution_aware_optimizer.cost_sensitivity import build_cost_sensitivity_scenarios, load_cost_sensitivity_results
from execution_aware_optimizer.experiment_config import ExperimentConfig


def test_cost_sensitivity_scenarios_clone_config_without_mutating_base() -> None:
    config = ExperimentConfig.model_validate(
        {
            "cost_sensitivity_bps": [0, 5, 10],
            "portfolioos": {
                "transaction_cost_objective_mode": "nav_fraction",
                "output_dir": "reports/base",
            },
        }
    )

    scenarios = build_cost_sensitivity_scenarios(config)

    assert [scenario.cost_bps for scenario in scenarios] == [0, 5, 10]
    assert [scenario.config.portfolioos.cost_assumption_bps for scenario in scenarios] == [0.0, 5.0, 10.0]
    assert scenarios[1].portfolioos_overrides == {
        "fees.commission_rate": pytest.approx(0.0005),
        "execution.backtest_fixed_half_spread_bps": pytest.approx(5.0),
        "objective_weights.transaction_cost_objective_mode": "nav_fraction",
    }
    assert config.portfolioos.cost_assumption_bps is None
    assert config.portfolioos.output_dir == "reports/base"
    assert scenarios[1].config.portfolioos.output_dir.endswith("cost_5bps")


def test_load_cost_sensitivity_results_parses_executed_rows(tmp_path: Path) -> None:
    results_path = tmp_path / "cost_sensitivity_results.csv"
    results_path.write_text(
        "\n".join(
            [
                "layer_name,date,gross_return,net_return,turnover,estimated_transaction_cost,infeasibility_reason,cost_bps,transaction_cost_objective_mode,portfolioos_overrides",
                "full_execution_aware_cost_adjusted,2026-02-28,0.020,0.018,0.21,0.002,,5,nav_fraction,\"{'fees.commission_rate': 0.0005}\"",
                "full_execution_aware_cost_adjusted,2026-02-28,0.015,0.010,0.18,0.005,,25,nav_fraction,\"{'fees.commission_rate': 0.0025}\"",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_cost_sensitivity_results(results_path)

    assert [row.cost_bps for row in rows] == [5, 25]
    assert rows[0].date.isoformat() == "2026-02-28"
    assert rows[0].net_return == pytest.approx(0.018)
    assert rows[0].portfolioos_overrides == {"fees.commission_rate": pytest.approx(0.0005)}


def test_load_cost_sensitivity_results_preserves_default_unavailable_rows() -> None:
    rows = load_cost_sensitivity_results("projects/execution_aware_optimizer/reports/cost_sensitivity_results.csv")

    assert sorted(row.cost_bps for row in rows) == [0, 5, 10, 25, 50]
    assert all(row.layer_name == "full_execution_aware_cost_adjusted" for row in rows)
    assert all(row.net_return is None for row in rows)
    assert all(row.infeasibility_reason for row in rows)
