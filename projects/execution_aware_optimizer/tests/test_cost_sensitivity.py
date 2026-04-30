from __future__ import annotations

import pytest

from execution_aware_optimizer.cost_sensitivity import build_cost_sensitivity_scenarios
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
