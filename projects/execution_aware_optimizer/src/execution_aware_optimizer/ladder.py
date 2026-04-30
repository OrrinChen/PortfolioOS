"""Alpha-decay ladder result schema and PortfolioOS adapters."""

from __future__ import annotations

from datetime import date as Date
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from pydantic import BaseModel, Field

from execution_aware_optimizer.experiment_config import ExperimentConfig


class LadderResultRow(BaseModel):
    """Standard result row for one ladder layer and rebalance date."""

    layer_name: str
    date: Date | None = None
    gross_return: float | None = None
    net_return: float | None = None
    turnover: float | None = None
    estimated_transaction_cost: float | None = None
    realized_transaction_cost: float | None = None
    num_positions: int | None = None
    max_position_weight: float | None = None
    cash_weight: float | None = None
    sector_exposure: dict[str, float] | None = None
    risk_exposure: dict[str, float] | None = None
    rejected_symbols: list[str] = Field(default_factory=list)
    binding_constraints: list[str] = Field(default_factory=list)
    infeasibility_reason: str | None = None


BacktestRunner = Callable[[str | Path], Any]


def build_unavailable_ladder_rows(
    config: ExperimentConfig,
    *,
    reason: str,
    result_date: Date | str | None = None,
) -> list[LadderResultRow]:
    """Build explicit unavailable rows for configured layers."""

    return [
        LadderResultRow(
            layer_name=layer.layer_name,
            date=result_date,
            infeasibility_reason=reason,
        )
        for layer in config.layers
        if layer.enabled
    ]


def _map_period_attribution_row(layer_name: str, row: pd.Series) -> LadderResultRow:
    """Convert an existing PortfolioOS period-attribution row into the Q2 schema."""

    start_nav = float(row.get("start_nav") or 0.0)
    commission_cost = float(row.get("commission_cost") or 0.0)
    spread_cost = float(row.get("spread_cost") or 0.0)
    estimated_cost = commission_cost + spread_cost
    holding_pnl = float(row.get("holding_pnl") or 0.0)
    active_trading_pnl = float(row.get("active_trading_pnl") or 0.0)
    gross_return = (holding_pnl + active_trading_pnl) / start_nav if start_nav else None
    cost_fraction = estimated_cost / start_nav if start_nav else None
    return LadderResultRow(
        layer_name=layer_name,
        date=str(row.get("end_date")) if row.get("end_date") else None,
        gross_return=gross_return,
        net_return=float(row.get("period_return")) if pd.notna(row.get("period_return")) else None,
        turnover=float(row.get("turnover")) if pd.notna(row.get("turnover")) else None,
        estimated_transaction_cost=cost_fraction,
        realized_transaction_cost=None,
    )


def _run_portfolioos_backtest_layers(
    config: ExperimentConfig,
    *,
    backtest_runner: BacktestRunner,
) -> list[LadderResultRow]:
    """Map available existing backtest strategies into ladder rows."""

    if not config.portfolioos.backtest_manifest:
        return build_unavailable_ladder_rows(
            config,
            reason="No PortfolioOS backtest manifest configured for this project.",
        )

    result = backtest_runner(config.portfolioos.backtest_manifest)
    period_attribution = getattr(result, "period_attribution", pd.DataFrame())
    if period_attribution.empty:
        return build_unavailable_ladder_rows(
            config,
            reason="PortfolioOS backtest returned no period attribution rows.",
        )

    rows: list[LadderResultRow] = []
    layer_map = {
        "raw_top_alpha_equal_weight": "alpha_only_top_quintile",
        "full_execution_aware_cost_adjusted": "optimizer",
    }
    for layer in config.layers:
        if not layer.enabled:
            continue
        strategy_name = layer_map.get(layer.layer_name)
        if strategy_name is None:
            rows.append(
                LadderResultRow(
                    layer_name=layer.layer_name,
                    infeasibility_reason=(
                        "No stable PortfolioOS adapter exists yet for this intermediate layer. "
                        "The project records this as unavailable rather than fabricating a result."
                    ),
                )
            )
            continue
        strategy_rows = period_attribution.loc[period_attribution["strategy"] == strategy_name]
        if strategy_rows.empty:
            rows.append(
                LadderResultRow(
                    layer_name=layer.layer_name,
                    infeasibility_reason=f"PortfolioOS strategy {strategy_name!r} was not present in the backtest output.",
                )
            )
            continue
        rows.extend(_map_period_attribution_row(layer.layer_name, row) for _, row in strategy_rows.iterrows())
    return rows


def run_alpha_decay_ladder(
    config: ExperimentConfig,
    *,
    alpha_panel: pd.DataFrame | None = None,
    backtest_runner: BacktestRunner | None = None,
) -> list[LadderResultRow]:
    """Run or plan the alpha-decay ladder.

    Existing PortfolioOS workflows are only invoked when
    `portfolioos.allow_portfolioos_run` is true. Otherwise the function returns
    explicit unavailable rows so reports remain honest and reproducible.
    """

    _ = alpha_panel
    if not config.portfolioos.allow_portfolioos_run:
        return build_unavailable_ladder_rows(
            config,
            reason=(
                "PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true "
                "to execute the configured backtest adapter explicitly."
            ),
        )

    if backtest_runner is None:
        from portfolio_os.backtest.engine import run_backtest

        backtest_runner = run_backtest
    return _run_portfolioos_backtest_layers(config, backtest_runner=backtest_runner)


def ladder_rows_to_frame(rows: list[LadderResultRow]) -> pd.DataFrame:
    """Serialize ladder rows to a DataFrame."""

    return pd.DataFrame([row.model_dump(mode="json") for row in rows])
