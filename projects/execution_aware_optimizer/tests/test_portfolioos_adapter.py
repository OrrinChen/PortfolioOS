from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import run_alpha_decay_ladder


@dataclass(frozen=True)
class FakeBacktestResult:
    period_attribution: pd.DataFrame


def _fake_backtest_result() -> FakeBacktestResult:
    return FakeBacktestResult(
        period_attribution=pd.DataFrame(
            [
                {
                    "strategy": "alpha_only_top_quintile",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 2.0,
                    "active_trading_pnl": 1.0,
                    "trading_cost_pnl": -0.5,
                    "period_return": 0.025,
                    "turnover": 0.40,
                    "commission_cost": 0.2,
                    "spread_cost": 0.3,
                },
                {
                    "strategy": "optimizer",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 1.5,
                    "active_trading_pnl": 0.7,
                    "trading_cost_pnl": -0.2,
                    "period_return": 0.020,
                    "turnover": 0.21,
                    "commission_cost": 0.1,
                    "spread_cost": 0.1,
                },
            ]
        )
    )


def test_portfolioos_backtest_adapter_maps_available_strategies_to_ladder_rows() -> None:
    config = ExperimentConfig.model_validate(
        {
            "portfolioos": {
                "allow_portfolioos_run": True,
                "backtest_manifest": "fake_manifest.yaml",
            },
            "layers": [
                {"layer_name": "raw_top_alpha_equal_weight"},
                {"layer_name": "risk_controlled"},
                {"layer_name": "full_execution_aware_cost_adjusted"},
            ],
        }
    )

    rows = run_alpha_decay_ladder(config, backtest_runner=lambda _manifest: _fake_backtest_result())

    raw_row = next(row for row in rows if row.layer_name == "raw_top_alpha_equal_weight")
    full_row = next(row for row in rows if row.layer_name == "full_execution_aware_cost_adjusted")
    unavailable_row = next(row for row in rows if row.layer_name == "risk_controlled")

    assert raw_row.date.isoformat() == "2026-02-28"
    assert raw_row.gross_return == pytest.approx(0.03)
    assert raw_row.net_return == pytest.approx(0.025)
    assert raw_row.estimated_transaction_cost == pytest.approx(0.005)
    assert raw_row.turnover == pytest.approx(0.40)
    assert raw_row.infeasibility_reason is None
    assert full_row.gross_return == pytest.approx(0.022)
    assert full_row.net_return == pytest.approx(0.020)
    assert full_row.estimated_transaction_cost == pytest.approx(0.002)
    assert unavailable_row.infeasibility_reason is not None
    assert "No stable PortfolioOS adapter" in unavailable_row.infeasibility_reason
