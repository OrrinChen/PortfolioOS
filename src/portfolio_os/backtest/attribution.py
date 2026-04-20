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
    "optimizer_vs_alpha_only_period_pnl_delta",
    "alpha_only_vs_naive_period_pnl_delta",
]


def _period_pnl_by_strategy(frame: pd.DataFrame, strategy_name: str) -> pd.Series:
    """Return one period-indexed PnL lookup for one strategy."""

    if strategy_name not in set(frame["strategy"]):
        return pd.Series(dtype=float)
    return (
        frame.loc[frame["strategy"] == strategy_name, ["period_index", "period_pnl"]]
        .drop_duplicates(subset=["period_index"])
        .set_index("period_index")["period_pnl"]
    )


def _attach_period_delta(
    frame: pd.DataFrame,
    *,
    lhs_strategy: str,
    rhs_strategy: str,
    output_column: str,
) -> None:
    """Attach one pairwise period PnL delta column in-place."""

    rhs_period_pnl = _period_pnl_by_strategy(frame, rhs_strategy)
    if rhs_period_pnl.empty or lhs_strategy not in set(frame["strategy"]):
        return
    lhs_mask = frame["strategy"] == lhs_strategy
    frame.loc[lhs_mask, output_column] = (
        frame.loc[lhs_mask, "period_pnl"]
        - frame.loc[lhs_mask, "period_index"].map(rhs_period_pnl)
    ).astype(float)


def build_period_attribution_frame(period_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize period attribution rows into a stable export frame."""

    if not period_rows:
        return pd.DataFrame(columns=_PERIOD_ATTRIBUTION_COLUMNS)

    frame = pd.DataFrame(period_rows)
    if "optimizer_vs_naive_period_pnl_delta" not in frame.columns:
        frame["optimizer_vs_naive_period_pnl_delta"] = np.nan
    if "optimizer_vs_alpha_only_period_pnl_delta" not in frame.columns:
        frame["optimizer_vs_alpha_only_period_pnl_delta"] = np.nan
    if "alpha_only_vs_naive_period_pnl_delta" not in frame.columns:
        frame["alpha_only_vs_naive_period_pnl_delta"] = np.nan
    _attach_period_delta(
        frame,
        lhs_strategy="optimizer",
        rhs_strategy="naive_pro_rata",
        output_column="optimizer_vs_naive_period_pnl_delta",
    )
    _attach_period_delta(
        frame,
        lhs_strategy="optimizer",
        rhs_strategy="alpha_only_top_quintile",
        output_column="optimizer_vs_alpha_only_period_pnl_delta",
    )
    _attach_period_delta(
        frame,
        lhs_strategy="alpha_only_top_quintile",
        rhs_strategy="naive_pro_rata",
        output_column="alpha_only_vs_naive_period_pnl_delta",
    )
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


def _update_pairwise_comparison(
    summary: dict[str, Any],
    *,
    left_name: str,
    right_name: str,
    prefix: str,
    ending_nav_by_strategy: dict[str, float],
    total_return_by_strategy: dict[str, float],
    annualized_by_strategy: dict[str, float],
    sharpe_by_strategy: dict[str, float],
) -> None:
    """Attach one pairwise strategy comparison block when both strategies exist."""

    if left_name not in ending_nav_by_strategy or right_name not in ending_nav_by_strategy:
        return
    summary["comparison"].update(
        {
            f"{prefix}_ending_nav_delta": float(
                ending_nav_by_strategy[left_name] - ending_nav_by_strategy[right_name]
            ),
            f"{prefix}_total_return_delta": float(
                total_return_by_strategy[left_name] - total_return_by_strategy[right_name]
            ),
            f"{prefix}_annualized_return_delta": float(
                annualized_by_strategy[left_name] - annualized_by_strategy[right_name]
            ),
            f"{prefix}_sharpe_delta": float(
                sharpe_by_strategy[left_name] - sharpe_by_strategy[right_name]
            ),
            f"{prefix}_total_cost_delta": float(
                summary["strategies"][left_name]["total_transaction_cost"]
                - summary["strategies"][right_name]["total_transaction_cost"]
            ),
            f"{prefix}_total_turnover_delta": float(
                summary["strategies"][left_name]["total_turnover"]
                - summary["strategies"][right_name]["total_turnover"]
            ),
        }
    )


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
    sharpe_by_strategy: dict[str, float] = {}
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
        sharpe_by_strategy[strategy_name] = sharpe

    _update_pairwise_comparison(
        summary,
        left_name="optimizer",
        right_name="naive_pro_rata",
        prefix="optimizer_vs_naive",
        ending_nav_by_strategy=ending_nav_by_strategy,
        total_return_by_strategy=total_return_by_strategy,
        annualized_by_strategy=annualized_by_strategy,
        sharpe_by_strategy=sharpe_by_strategy,
    )
    _update_pairwise_comparison(
        summary,
        left_name="optimizer",
        right_name="alpha_only_top_quintile",
        prefix="optimizer_vs_alpha_only",
        ending_nav_by_strategy=ending_nav_by_strategy,
        total_return_by_strategy=total_return_by_strategy,
        annualized_by_strategy=annualized_by_strategy,
        sharpe_by_strategy=sharpe_by_strategy,
    )
    _update_pairwise_comparison(
        summary,
        left_name="alpha_only_top_quintile",
        right_name="naive_pro_rata",
        prefix="alpha_only_vs_naive",
        ending_nav_by_strategy=ending_nav_by_strategy,
        total_return_by_strategy=total_return_by_strategy,
        annualized_by_strategy=annualized_by_strategy,
        sharpe_by_strategy=sharpe_by_strategy,
    )
    return summary
