from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.experiment_config import ExperimentConfig, load_experiment_config
from execution_aware_optimizer.ladder import run_alpha_decay_ladder
from portfolio_os.backtest.engine import run_backtest


REPO_ROOT = Path(__file__).resolve().parents[3]
Q2_CONFIG_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "configs"


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
                    "strategy": "naive_pro_rata",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 1.7,
                    "active_trading_pnl": 0.5,
                    "trading_cost_pnl": -0.3,
                    "period_return": 0.019,
                    "turnover": 0.30,
                    "commission_cost": 0.1,
                    "spread_cost": 0.2,
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
    risk_row = next(row for row in rows if row.layer_name == "risk_controlled")
    full_row = next(row for row in rows if row.layer_name == "full_execution_aware_cost_adjusted")

    assert raw_row.date.isoformat() == "2026-02-28"
    assert raw_row.gross_return == pytest.approx(0.03)
    assert raw_row.net_return == pytest.approx(0.025)
    assert raw_row.estimated_transaction_cost == pytest.approx(0.005)
    assert raw_row.turnover == pytest.approx(0.40)
    assert raw_row.infeasibility_reason is None
    assert risk_row.date.isoformat() == "2026-02-28"
    assert risk_row.gross_return == pytest.approx(0.022)
    assert risk_row.net_return == pytest.approx(0.019)
    assert risk_row.estimated_transaction_cost == pytest.approx(0.003)
    assert risk_row.turnover == pytest.approx(0.30)
    assert risk_row.infeasibility_reason is None
    assert full_row.gross_return == pytest.approx(0.022)
    assert full_row.net_return == pytest.approx(0.020)
    assert full_row.estimated_transaction_cost == pytest.approx(0.002)


def test_q2_default_configs_do_not_enable_portfolioos_runs() -> None:
    for config_name in ("base.yaml", "alpha_decay_ladder.yaml", "cost_sensitivity.yaml"):
        config = load_experiment_config(Q2_CONFIG_DIR / config_name)

        assert config.portfolioos.allow_portfolioos_run is False


def test_local_portfolioos_fixture_maps_executed_rows_without_report_writes(tmp_path: Path) -> None:
    config = ExperimentConfig.model_validate(
        {
            "portfolioos": {
                "allow_portfolioos_run": True,
                "backtest_manifest": "data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml",
                "output_dir": str(tmp_path),
            },
            "layers": [
                {"layer_name": "raw_top_alpha_equal_weight"},
                {"layer_name": "risk_controlled"},
                {"layer_name": "full_execution_aware_cost_adjusted"},
            ],
        }
    )

    rows = run_alpha_decay_ladder(config, backtest_runner=run_backtest)

    raw_rows = [row for row in rows if row.layer_name == "raw_top_alpha_equal_weight"]
    risk_rows = [row for row in rows if row.layer_name == "risk_controlled"]
    full_rows = [row for row in rows if row.layer_name == "full_execution_aware_cost_adjusted"]

    assert raw_rows
    assert risk_rows
    assert full_rows
    assert any(row.net_return is not None for row in raw_rows)
    assert any(row.net_return is not None for row in risk_rows)
    assert any(row.net_return is not None for row in full_rows)
    assert all(row.infeasibility_reason is None for row in raw_rows + risk_rows + full_rows)
    assert list(tmp_path.iterdir()) == []
