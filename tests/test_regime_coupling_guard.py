from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from portfolio_os.optimizer.rebalancer import run_rebalance


def _write_returns_long(path: Path, tickers: list[str], *, days: int = 140) -> None:
    lines = ["date,ticker,return"]
    start = date(2025, 1, 1)
    for offset in range(days):
        current_date = start + timedelta(days=offset)
        for idx, ticker in enumerate(tickers):
            base = 0.0002 * (idx + 1)
            seasonal = 0.0001 * ((offset % 10) - 5)
            lines.append(f"{current_date.isoformat()},{ticker},{base + seasonal:.8f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_factor_exposure(path: Path, tickers: list[str]) -> None:
    lines = ["ticker,factor,exposure"]
    for idx, ticker in enumerate(tickers):
        lines.append(f"{ticker},market,{1.0 + idx * 0.01:.6f}")
        lines.append(f"{ticker},value,{0.2 - idx * 0.02:.6f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _signed_order_quantities(orders) -> dict[str, int]:
    signed: dict[str, int] = {}
    for order in orders:
        qty = int(order.quantity)
        if str(order.side).lower() == "sell":
            qty = -qty
        signed[str(order.ticker)] = signed.get(str(order.ticker), 0) + qty
    return signed


def _risk_mode_config(base_config, *, integration_mode: str):
    config = base_config.model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = integration_mode
    config.risk_model.estimator = "ledoit_wolf"
    config.risk_model.lookback_days = 130
    config.risk_model.min_history_days = 120
    config.constraints.factor_bounds = {}
    config.objective_weights.risk_term = 0.0
    config.objective_weights.tracking_error = 0.0
    config.objective_weights.transaction_cost = 0.0
    return config


def test_augment_mode_with_zero_risk_terms_matches_legacy_behavior(sample_context: dict, tmp_path: Path) -> None:
    universe = sample_context["universe"]
    tickers = universe["ticker"].astype(str).tolist()
    returns_path = tmp_path / "returns_long.csv"
    factor_path = tmp_path / "factor_exposure.csv"
    _write_returns_long(returns_path, tickers)
    _write_factor_exposure(factor_path, tickers)

    baseline_config = sample_context["config"].model_copy(deep=True)
    replace_config = _risk_mode_config(sample_context["config"], integration_mode="replace")
    augment_config = _risk_mode_config(sample_context["config"], integration_mode="augment")
    replace_config.risk_model.returns_path = str(returns_path)
    replace_config.risk_model.factor_exposure_path = str(factor_path)
    augment_config.risk_model.returns_path = str(returns_path)
    augment_config.risk_model.factor_exposure_path = str(factor_path)

    baseline_run = run_rebalance(universe, baseline_config)
    replace_run = run_rebalance(universe, replace_config)
    augment_run = run_rebalance(universe, augment_config)

    assert len(baseline_run.orders) > 0
    assert replace_run is not None

    assert len(augment_run.orders) == len(baseline_run.orders)
    assert augment_run.basket.gross_traded_notional == pytest.approx(
        baseline_run.basket.gross_traded_notional,
        abs=1e-6,
    )
    assert augment_run.basket.total_cost == pytest.approx(
        baseline_run.basket.total_cost,
        abs=1e-6,
    )

    baseline_signed = _signed_order_quantities(baseline_run.orders)
    augment_signed = _signed_order_quantities(augment_run.orders)
    assert baseline_signed.keys() == augment_signed.keys()
    for ticker in baseline_signed:
        assert augment_signed[ticker] == baseline_signed[ticker]
