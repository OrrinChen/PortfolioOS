"""Rolling out-of-sample ICIR weighting for Factor Discovery Sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .teaching_baseline import (
    _annualized_return,
    _build_factor_panel,
    _build_qqq_returns,
    _build_teaching_price_fixture,
    _compute_ic_table,
    _compute_icir_weights,
    _max_drawdown,
    _sharpe,
    _total_return,
)


@dataclass(frozen=True)
class RollingOOSResult:
    """Artifacts and summary for rolling OOS weighting."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_rolling_oos(output_dir: str | Path, min_history_months: int = 12) -> RollingOOSResult:
    """Run rolling ICIR weighting with no full-sample ICIR estimates."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    prices = _build_teaching_price_fixture()
    returns = prices.pct_change().fillna(0.0)
    next_returns = returns.shift(-1)
    qqq_returns = _build_qqq_returns(returns)
    factor_panel = _build_factor_panel(prices, returns)
    dates = sorted(factor_panel["date"].unique())

    weight_rows = []
    score_rows = []
    portfolio_rows = []
    for index in range(min_history_months, len(dates) - 1):
        rebalance_date = dates[index]
        estimation_window_end = dates[index - 1]
        tradable_date = dates[index + 1]
        history_panel = factor_panel[factor_panel["date"] < rebalance_date]
        current_panel = factor_panel[factor_panel["date"] == rebalance_date]
        weights = _compute_icir_weights(_compute_ic_table(history_panel, next_returns))
        weights = weights.assign(
            rebalance_date=rebalance_date,
            estimation_window_end=estimation_window_end,
            uses_full_sample_icir=False,
        )
        weight_rows.extend(
            weights[
                [
                    "rebalance_date",
                    "estimation_window_end",
                    "factor",
                    "ic_mean",
                    "ic_std",
                    "icir",
                    "weight",
                    "uses_full_sample_icir",
                ]
            ]
            .rename(columns={"icir": "rolling_icir"})
            .to_dict("records")
        )

        score = _score_current_panel(current_panel, weights)
        forward_returns = next_returns.loc[pd.Timestamp(rebalance_date)]
        top = score.sort_values("score", ascending=False).head(4)
        portfolio_rows.append(
            {
                "date": rebalance_date,
                "return": float(forward_returns[top["ticker"]].mean()),
                "benchmark_return": float(qqq_returns.loc[pd.Timestamp(rebalance_date)]),
            }
        )
        for row in score.itertuples(index=False):
            score_rows.append(
                {
                    "date": rebalance_date,
                    "ticker": row.ticker,
                    "score": row.score,
                    "coverage_state": "active_view",
                    "signal_timestamp": rebalance_date,
                    "visibility_timestamp": rebalance_date,
                    "tradable_timestamp": tradable_date,
                }
            )

    weights_df = pd.DataFrame(weight_rows)
    scores_df = pd.DataFrame(score_rows)
    returns_df = pd.DataFrame(portfolio_rows)
    report = _render_oos_report(returns_df, dates[min_history_months - 1], dates[min_history_months])

    artifacts = {
        "rolling_icir_weights": output_path / "rolling_icir_weights.csv",
        "oos_factor_score_panel": output_path / "oos_factor_score_panel.csv",
        "oos_backtest_report": output_path / "oos_backtest_report.md",
    }
    weights_df.to_csv(artifacts["rolling_icir_weights"], index=False)
    scores_df.to_csv(artifacts["oos_factor_score_panel"], index=False)
    artifacts["oos_backtest_report"].write_text(report, encoding="utf-8")

    summary = {
        "mode": "research_mode_oos",
        "uses_full_sample_icir": False,
        "trade_timing": "score_at_t_trade_at_t_plus_1",
        "train_boundary": dates[min_history_months - 1],
        "test_start": dates[min_history_months],
        "production_approval_claimed": False,
    }
    return RollingOOSResult(summary=summary, artifacts=artifacts)


def _score_current_panel(current_panel: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    weight_map = weights.set_index("factor")["weight"].to_dict()
    scored = current_panel.copy()
    scored["weighted_value"] = scored["value"] * scored["factor"].map(weight_map)
    return scored.groupby("ticker", as_index=False)["weighted_value"].sum().rename(
        columns={"weighted_value": "score"}
    )


def _render_oos_report(returns_df: pd.DataFrame, train_boundary: str, test_start: str) -> str:
    returns = pd.Series(returns_df["return"].to_numpy(), index=pd.to_datetime(returns_df["date"]))
    benchmark = pd.Series(returns_df["benchmark_return"].to_numpy(), index=pd.to_datetime(returns_df["date"]))
    excess = _annualized_return(returns) - _annualized_return(benchmark)
    return "\n".join(
        [
            "# Rolling ICIR OOS Backtest",
            "",
            "mode: research_mode_oos",
            "full-sample ICIR: forbidden",
            "teaching-mode result: separate",
            f"train_boundary: {train_boundary}",
            f"test_start: {test_start}",
            "trade_timing: score_at_t_trade_at_t_plus_1",
            "production approval: not claimed",
            "",
            "## OOS Metrics",
            f"- total_return: {_total_return(returns):.6f}",
            f"- annualized_return: {_annualized_return(returns):.6f}",
            f"- sharpe: {_sharpe(returns):.6f}",
            f"- max_drawdown: {_max_drawdown(returns):.6f}",
            f"- excess_annualized_return_vs_QQQ: {excess:.6f}",
            "",
        ]
    )
