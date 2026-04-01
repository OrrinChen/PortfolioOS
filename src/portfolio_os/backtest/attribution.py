"""Attribution and summary helpers for the historical backtest."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


_PERIOD_ATTRIBUTION_COLUMNS = [
    "strategy",
    "period_index",
    "start_date",
    "fill_date",
    "end_date",
    "start_nav",
    "end_nav",
    "filled_notional",
    "gross_traded_notional",
    "turnover",
    "commission_cost",
    "spread_cost",
    "holding_pnl",
    "active_trading_pnl",
    "trading_cost_pnl",
    "period_pnl",
    "period_return",
    "optimizer_vs_naive_period_pnl_delta",
]


def build_period_attribution_frame(period_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize period attribution rows into a stable export frame."""

    if not period_rows:
        return pd.DataFrame(columns=_PERIOD_ATTRIBUTION_COLUMNS)

    frame = pd.DataFrame(period_rows)
    if "optimizer_vs_naive_period_pnl_delta" not in frame.columns:
        frame["optimizer_vs_naive_period_pnl_delta"] = np.nan
    naive_period_pnl = (
        frame.loc[frame["strategy"] == "naive_pro_rata", ["period_index", "period_pnl"]]
        .drop_duplicates(subset=["period_index"])
        .set_index("period_index")["period_pnl"]
        if "naive_pro_rata" in set(frame["strategy"])
        else pd.Series(dtype=float)
    )
    if not naive_period_pnl.empty:
        optimizer_mask = frame["strategy"] == "optimizer"
        frame.loc[optimizer_mask, "optimizer_vs_naive_period_pnl_delta"] = (
            frame.loc[optimizer_mask, "period_pnl"]
            - frame.loc[optimizer_mask, "period_index"].map(naive_period_pnl)
        ).astype(float)
    frame = frame.reindex(columns=_PERIOD_ATTRIBUTION_COLUMNS)
    return frame.sort_values(["period_index", "strategy"]).reset_index(drop=True)


def _annualized_return(nav: pd.Series) -> float:
    """Compute annualized return from a daily NAV series."""

    if len(nav) < 2:
        return 0.0
    start = float(nav.iloc[0])
    end = float(nav.iloc[-1])
    if start <= 0.0 or end <= 0.0:
        return 0.0
    trading_days = len(nav) - 1
    return float((end / start) ** (252.0 / trading_days) - 1.0)


def _sharpe_ratio(nav: pd.Series) -> float:
    """Compute a zero-rate daily Sharpe ratio from NAV."""

    daily_returns = nav.astype(float).pct_change().dropna()
    if daily_returns.empty:
        return 0.0
    volatility = float(daily_returns.std(ddof=0))
    if volatility <= 0.0:
        return 0.0
    return float(np.sqrt(252.0) * daily_returns.mean() / volatility)


def _max_drawdown(nav: pd.Series) -> float:
    """Compute the maximum drawdown over the NAV path."""

    nav = nav.astype(float)
    if nav.empty:
        return 0.0
    running_peak = nav.cummax()
    drawdown = nav / running_peak - 1.0
    return float(drawdown.min())


def build_backtest_summary(
    *,
    schedule: list[pd.Timestamp],
    nav_series: pd.DataFrame,
    period_attribution: pd.DataFrame,
    strategy_state_summary: dict[str, dict[str, float | int]],
) -> dict[str, Any]:
    """Build summary stats and optimizer-vs-naive comparison fields."""

    summary: dict[str, Any] = {
        "start_date": str(nav_series["date"].min()),
        "end_date": str(nav_series["date"].max()),
        "rebalance_count": len(schedule),
        "rebalance_schedule": [item.strftime("%Y-%m-%d") for item in schedule],
        "strategies": {},
        "comparison": {},
    }
    ending_nav_by_strategy: dict[str, float] = {}
    total_return_by_strategy: dict[str, float] = {}
    annualized_by_strategy: dict[str, float] = {}
    for strategy_name, state_metrics in strategy_state_summary.items():
        strategy_nav = nav_series.loc[nav_series["strategy"] == strategy_name, "nav"].astype(float).reset_index(drop=True)
        starting_nav = float(strategy_nav.iloc[0]) if not strategy_nav.empty else 0.0
        ending_nav = float(strategy_nav.iloc[-1]) if not strategy_nav.empty else 0.0
        total_return = float((ending_nav / starting_nav) - 1.0) if starting_nav else 0.0
        annualized_return = _annualized_return(strategy_nav)
        sharpe = _sharpe_ratio(strategy_nav)
        max_drawdown = _max_drawdown(strategy_nav)
        strategy_rows = period_attribution.loc[period_attribution["strategy"] == strategy_name]
        summary["strategies"][strategy_name] = {
            "starting_nav": starting_nav,
            "ending_nav": ending_nav,
            "total_return": total_return,
            "annualized_return": annualized_return,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
            "rebalance_count": int(state_metrics["rebalance_count"]),
            "period_count": int(len(strategy_rows)),
            "total_turnover": float(state_metrics["total_turnover"]),
            "total_filled_notional": float(state_metrics["total_filled_notional"]),
            "total_commission": float(state_metrics["total_commission"]),
            "total_spread_cost": float(state_metrics["total_spread_cost"]),
            "total_transaction_cost": float(state_metrics["total_transaction_cost"]),
        }
        ending_nav_by_strategy[strategy_name] = ending_nav
        total_return_by_strategy[strategy_name] = total_return
        annualized_by_strategy[strategy_name] = annualized_return

    if "optimizer" in ending_nav_by_strategy and "naive_pro_rata" in ending_nav_by_strategy:
        summary["comparison"] = {
            "optimizer_vs_naive_ending_nav_delta": float(
                ending_nav_by_strategy["optimizer"] - ending_nav_by_strategy["naive_pro_rata"]
            ),
            "optimizer_vs_naive_total_return_delta": float(
                total_return_by_strategy["optimizer"] - total_return_by_strategy["naive_pro_rata"]
            ),
            "optimizer_vs_naive_annualized_return_delta": float(
                annualized_by_strategy["optimizer"] - annualized_by_strategy["naive_pro_rata"]
            ),
            "optimizer_vs_naive_total_cost_delta": float(
                summary["strategies"]["optimizer"]["total_transaction_cost"]
                - summary["strategies"]["naive_pro_rata"]["total_transaction_cost"]
            ),
            "optimizer_vs_naive_total_turnover_delta": float(
                summary["strategies"]["optimizer"]["total_turnover"]
                - summary["strategies"]["naive_pro_rata"]["total_turnover"]
            ),
        }
    return summary
