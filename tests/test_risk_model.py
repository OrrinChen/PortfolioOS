from __future__ import annotations

from datetime import date, timedelta
import math
from pathlib import Path

import numpy as np
import pytest

from portfolio_os.domain.errors import InputValidationError
from portfolio_os.explain.summary import build_summary
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.optimizer.solver import solve_rebalance_problem
from portfolio_os.risk.model import build_risk_model_context
from portfolio_os.utils.config import FactorBoundConfig


def _write_returns_long(path: Path, tickers: list[str], *, days: int = 130) -> None:
    lines = ["date,ticker,return"]
    start = date(2025, 1, 1)
    for offset in range(days):
        current_date = start + timedelta(days=offset)
        for idx, ticker in enumerate(tickers):
            base = 0.00035 * (idx + 1)
            seasonal = 0.00025 * math.sin((offset + 1) * (idx + 2) / 9.0)
            lines.append(f"{current_date.isoformat()},{ticker},{base + seasonal:.8f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_factor_exposure(path: Path, tickers: list[str]) -> None:
    lines = ["ticker,factor,exposure"]
    for idx, ticker in enumerate(tickers):
        lines.append(f"{ticker},market,{1.0 + 0.05 * (idx - 3):.6f}")
        lines.append(f"{ticker},value,{0.4 - 0.08 * idx:.6f}")
        lines.append(f"{ticker},momentum,{-0.3 + 0.07 * idx:.6f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _risk_enabled_config(sample_context: dict, tmp_path: Path):
    tickers = sample_context["universe"]["ticker"].astype(str).tolist()
    returns_path = tmp_path / "returns_long.csv"
    factor_path = tmp_path / "factor_exposure.csv"
    _write_returns_long(returns_path, tickers, days=140)
    _write_factor_exposure(factor_path, tickers)

    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.estimator = "ledoit_wolf"
    config.risk_model.returns_path = str(returns_path)
    config.risk_model.factor_exposure_path = str(factor_path)
    config.risk_model.lookback_days = 130
    config.risk_model.min_history_days = 120
    config.constraints.factor_bounds = {}
    return config


def test_ledoit_wolf_context_is_psd(sample_context: dict, tmp_path: Path) -> None:
    universe = sample_context["universe"]
    config = _risk_enabled_config(sample_context, tmp_path)
    context = build_risk_model_context(universe, config)
    assert context is not None
    assert context.sigma.shape == (len(universe), len(universe))
    eigvals = np.linalg.eigvalsh(context.sigma)
    assert float(np.min(eigvals)) >= -1e-10
    assert context.factor_matrix.shape[0] == len(universe)


def test_risk_input_requires_all_tickers(sample_context: dict, tmp_path: Path) -> None:
    universe = sample_context["universe"]
    tickers = universe["ticker"].astype(str).tolist()
    returns_path = tmp_path / "returns_long.csv"
    factor_path = tmp_path / "factor_exposure.csv"
    _write_returns_long(returns_path, tickers[:-1], days=140)
    _write_factor_exposure(factor_path, tickers)
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.returns_path = str(returns_path)
    config.risk_model.factor_exposure_path = str(factor_path)
    config.risk_model.lookback_days = 130
    config.risk_model.min_history_days = 120

    with pytest.raises(InputValidationError):
        _ = build_risk_model_context(universe, config)


def test_factor_bounds_support_absolute_and_active_limits(sample_context: dict, tmp_path: Path) -> None:
    universe = sample_context["universe"]
    config = _risk_enabled_config(sample_context, tmp_path)
    config.constraints.factor_bounds = {
        "market": FactorBoundConfig(abs_min=-2.0, abs_max=2.0, active_min=-0.10, active_max=0.10),
        "value": FactorBoundConfig(abs_min=-1.0, abs_max=1.0, active_min=-0.20, active_max=0.20),
    }
    config.solver.eps = 0.07
    context = build_risk_model_context(universe, config)
    assert context is not None
    result = solve_rebalance_problem(universe, config, risk_context=context)

    post_weights = np.array(
        [float(result.post_trade_weights[str(ticker)]) for ticker in universe["ticker"].astype(str)],
        dtype=float,
    )
    post_factor = np.matmul(context.factor_matrix.T, post_weights)
    active_factor = post_factor - context.target_factor_exposure

    market_idx = context.factor_names.index("market")
    value_idx = context.factor_names.index("value")
    tolerance = 5e-4
    assert -2.0 - 1e-6 <= post_factor[market_idx] <= 2.0 + 1e-6
    assert -1.0 - 1e-6 <= post_factor[value_idx] <= 1.0 + 1e-6
    assert -0.10 - tolerance <= active_factor[market_idx] <= 0.10 + tolerance
    assert -0.20 - tolerance <= active_factor[value_idx] <= 0.20 + tolerance


def test_no_trade_zone_forces_zero_trades(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.constraints.no_trade_zone.enabled = True
    config.constraints.no_trade_zone.weight_threshold = 1.0
    config.constraints.single_name_max_weight = 1.0
    config.constraints.industry_bounds = {}
    config.constraints.max_turnover = 1.0
    config.constraints.double_ten.enabled = False
    config.constraints.single_name_guardrail.enabled = False
    config.solver.eps = 0.05
    result = solve_rebalance_problem(universe, config)
    assert all(abs(instruction.quantity) <= 1e-6 for instruction in result.instructions)


def test_risk_metrics_are_populated_in_summary(sample_context: dict, tmp_path: Path) -> None:
    universe = sample_context["universe"]
    config = _risk_enabled_config(sample_context, tmp_path)
    run = run_rebalance(universe, config)
    summary = build_summary(
        run.universe,
        run.basket,
        run.findings,
        config,
        cash_before=run.cash_before,
        cash_after=run.cash_after,
        pre_trade_nav=run.pre_trade_nav,
        post_trade_quantities=run.post_trade_quantities,
        risk_context=run.risk_context,
    )
    assert summary["portfolio_variance_before"] >= 0.0
    assert summary["portfolio_variance_after"] >= 0.0
    assert summary["tracking_error_variance_before"] >= 0.0
    assert summary["tracking_error_variance_after"] >= 0.0
