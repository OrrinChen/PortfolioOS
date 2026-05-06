"""Deterministic teaching-mode NASDAQ100 factor baseline.

The teaching baseline intentionally uses a small local fixture that represents a
current-constituent style universe. It is educational only and is not alpha
evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


FACTOR_NAMES = [
    "momentum_1m",
    "momentum_2m",
    "momentum_3m",
    "momentum_6m",
    "momentum_9m",
    "momentum_12m",
    "reversal_1m",
    "reversal_2m",
    "volatility_1m",
    "volatility_2m",
    "volatility_3m",
    "volatility_6m",
    "liquidity_dollar_volume_1m",
    "liquidity_dollar_volume_3m",
    "turnover_1m",
    "turnover_3m",
    "volume_momentum_1m",
    "volume_momentum_3m",
    "price_to_high_3m",
    "price_to_high_12m",
    "drawdown_3m",
    "drawdown_12m",
    "trend_slope_3m",
    "trend_slope_6m",
    "ema_gap_1m",
    "ema_gap_3m",
    "range_1m",
    "range_3m",
    "residual_momentum_6m",
]


@dataclass(frozen=True)
class TeachingBaselineResult:
    """Result returned by the teaching baseline runner."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_teaching_baseline(output_dir: str | Path) -> TeachingBaselineResult:
    """Write deterministic teaching-mode factor artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    prices = _build_teaching_price_fixture()
    returns = prices.pct_change().fillna(0.0)
    qqq_returns = _build_qqq_returns(returns)
    factor_panel = _build_factor_panel(prices, returns)
    next_returns = returns.shift(-1)

    ic_table = _compute_ic_table(factor_panel, next_returns)
    corr_matrix = _compute_factor_correlation(factor_panel)
    weights = _compute_icir_weights(ic_table)
    portfolio_returns = _compute_teaching_portfolio_returns(factor_panel, next_returns, weights)
    benchmark_report = _build_benchmark_report(portfolio_returns, qqq_returns)
    report = _render_teaching_report(benchmark_report)

    artifacts = {
        "factor_table": output_path / "nasdaq100_factor_table.csv",
        "benchmark_report": output_path / "qqq_benchmark_report.csv",
        "ic_table": output_path / "factor_ic_table.csv",
        "correlation_matrix": output_path / "factor_correlation_matrix.csv",
        "icir_weight_table": output_path / "icir_weight_table.csv",
        "report": output_path / "teaching_backtest_report.md",
    }

    factor_panel.to_csv(artifacts["factor_table"], index=False)
    benchmark_report.to_csv(artifacts["benchmark_report"], index=False)
    ic_table.to_csv(artifacts["ic_table"], index=False)
    corr_matrix.to_csv(artifacts["correlation_matrix"])
    weights.to_csv(artifacts["icir_weight_table"], index=False)
    artifacts["report"].write_text(report, encoding="utf-8")

    summary = {
        "mode": "teaching_mode",
        "benchmark": "QQQ",
        "factor_count": len(FACTOR_NAMES),
        "survivorship_biased": True,
        "educational_only": True,
        "not_alpha_evidence": True,
        "same_close_trading_allowed": False,
        "network_used": False,
        "production_approval_claimed": False,
    }
    return TeachingBaselineResult(summary=summary, artifacts=artifacts)


def _build_teaching_price_fixture() -> pd.DataFrame:
    tickers = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "GOOGL",
        "AVGO",
        "COST",
        "TSLA",
        "ADBE",
        "PEP",
        "CSCO",
    ]
    dates = pd.date_range("2021-01-31", periods=30, freq="ME")
    rows: dict[str, list[float]] = {}
    for ticker_index, ticker in enumerate(tickers):
        base = 80.0 + ticker_index * 7.0
        monthly_values = []
        price = base
        for month_index, _date in enumerate(dates):
            trend = 0.006 + 0.0012 * (ticker_index % 5)
            cycle = 0.018 * np.sin((month_index + ticker_index) / 3.2)
            shock = 0.004 * np.cos((month_index * (ticker_index + 2)) / 5.5)
            price *= 1.0 + trend + cycle + shock
            monthly_values.append(round(price, 6))
        rows[ticker] = monthly_values
    return pd.DataFrame(rows, index=dates)


def _build_qqq_returns(returns: pd.DataFrame) -> pd.Series:
    equal_weight = returns.mean(axis=1)
    dampener = pd.Series(
        [0.0015 * np.cos(index / 4.0) for index in range(len(equal_weight))],
        index=equal_weight.index,
    )
    return (equal_weight * 0.92 + dampener).rename("QQQ")


def _build_factor_panel(prices: pd.DataFrame, returns: pd.DataFrame) -> pd.DataFrame:
    volume = _build_volume_fixture(prices)
    frames: list[pd.DataFrame] = []
    for factor in FACTOR_NAMES:
        values = _factor_values(factor, prices, returns, volume)
        long = values.stack().rename("value").reset_index()
        long.columns = ["date", "ticker", "value"]
        long["factor"] = factor
        long["coverage_state"] = "active_view"
        frames.append(long[["date", "ticker", "factor", "value", "coverage_state"]])
    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = panel["date"].dt.strftime("%Y-%m-%d")
    return panel


def _build_volume_fixture(prices: pd.DataFrame) -> pd.DataFrame:
    values = {}
    for ticker_index, ticker in enumerate(prices.columns):
        base = 5_000_000 + ticker_index * 450_000
        path = []
        for month_index in range(len(prices.index)):
            path.append(base * (1.0 + 0.08 * np.sin((month_index + ticker_index) / 4.5)))
        values[ticker] = path
    return pd.DataFrame(values, index=prices.index)


def _factor_values(
    factor: str,
    prices: pd.DataFrame,
    returns: pd.DataFrame,
    volume: pd.DataFrame,
) -> pd.DataFrame:
    if factor.startswith("momentum_"):
        window = _window_from_name(factor)
        return prices.pct_change(window).fillna(0.0)
    if factor.startswith("reversal_"):
        window = _window_from_name(factor)
        return -prices.pct_change(window).fillna(0.0)
    if factor.startswith("volatility_"):
        window = _window_from_name(factor)
        return -returns.rolling(window, min_periods=1).std().fillna(0.0)
    if factor.startswith("liquidity_dollar_volume_"):
        window = _window_from_name(factor)
        return (prices * volume).rolling(window, min_periods=1).mean().rank(axis=1, pct=True)
    if factor.startswith("turnover_"):
        window = _window_from_name(factor)
        turnover = volume.div(volume.mean(axis=1), axis=0)
        return turnover.rolling(window, min_periods=1).mean()
    if factor.startswith("volume_momentum_"):
        window = _window_from_name(factor)
        return volume.pct_change(window).fillna(0.0)
    if factor.startswith("price_to_high_"):
        window = _window_from_name(factor)
        return prices.div(prices.rolling(window, min_periods=1).max()) - 1.0
    if factor.startswith("drawdown_"):
        window = _window_from_name(factor)
        return prices.div(prices.rolling(window, min_periods=1).max()) - 1.0
    if factor.startswith("trend_slope_"):
        window = _window_from_name(factor)
        return prices.pct_change(window).fillna(0.0) / max(window, 1)
    if factor.startswith("ema_gap_"):
        window = _window_from_name(factor)
        ema = prices.ewm(span=max(window, 2), adjust=False).mean()
        return prices.div(ema) - 1.0
    if factor.startswith("range_"):
        window = _window_from_name(factor)
        high = prices.rolling(window, min_periods=1).max()
        low = prices.rolling(window, min_periods=1).min()
        return (high - low).div(prices).fillna(0.0)
    if factor == "residual_momentum_6m":
        momentum = prices.pct_change(6).fillna(0.0)
        market = returns.mean(axis=1).rolling(6, min_periods=1).sum()
        return momentum.sub(market, axis=0)
    raise ValueError(f"unsupported factor: {factor}")


def _window_from_name(name: str) -> int:
    raw = name.rsplit("_", maxsplit=1)[-1]
    if raw.endswith("m"):
        return int(raw[:-1])
    return int(raw)


def _compute_ic_table(factor_panel: pd.DataFrame, next_returns: pd.DataFrame) -> pd.DataFrame:
    returns_long = next_returns.stack().rename("next_return").reset_index()
    returns_long.columns = ["date", "ticker", "next_return"]
    returns_long["date"] = returns_long["date"].dt.strftime("%Y-%m-%d")
    merged = factor_panel.merge(returns_long, on=["date", "ticker"], how="left").dropna()
    rows = []
    for factor, group in merged.groupby("factor", sort=True):
        monthly_ic = []
        for _date, date_group in group.groupby("date", sort=True):
            if date_group["value"].nunique() > 1 and date_group["next_return"].nunique() > 1:
                monthly_ic.append(date_group["value"].corr(date_group["next_return"], method="spearman"))
        series = pd.Series(monthly_ic, dtype="float64").dropna()
        ic_mean = float(series.mean()) if not series.empty else 0.0
        ic_std = float(series.std(ddof=0)) if len(series) > 1 else 0.0
        rows.append(
            {
                "factor": factor,
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "icir": ic_mean / ic_std if ic_std else 0.0,
                "observations": int(len(series)),
            }
        )
    return pd.DataFrame(rows).sort_values("factor").reset_index(drop=True)


def _compute_factor_correlation(factor_panel: pd.DataFrame) -> pd.DataFrame:
    wide = factor_panel.pivot_table(index=["date", "ticker"], columns="factor", values="value")
    return wide.corr().fillna(0.0)


def _compute_icir_weights(ic_table: pd.DataFrame) -> pd.DataFrame:
    weights = ic_table.copy()
    denominator = weights["icir"].abs().sum()
    if denominator == 0:
        weights["weight"] = 1.0 / len(weights)
    else:
        weights["weight"] = weights["icir"] / denominator
    return weights[["factor", "ic_mean", "ic_std", "icir", "weight", "observations"]]


def _compute_teaching_portfolio_returns(
    factor_panel: pd.DataFrame,
    next_returns: pd.DataFrame,
    weights: pd.DataFrame,
) -> pd.Series:
    weight_map = weights.set_index("factor")["weight"].to_dict()
    scored = factor_panel.copy()
    scored["weighted_value"] = scored["value"] * scored["factor"].map(weight_map)
    score = scored.groupby(["date", "ticker"], as_index=False)["weighted_value"].sum()
    score["rank"] = score.groupby("date")["weighted_value"].rank(ascending=False, method="first")

    returns_long = next_returns.stack().rename("next_return").reset_index()
    returns_long.columns = ["date", "ticker", "next_return"]
    returns_long["date"] = returns_long["date"].dt.strftime("%Y-%m-%d")
    selected = score[score["rank"] <= 4].merge(returns_long, on=["date", "ticker"], how="left").dropna()
    portfolio_returns = selected.groupby("date")["next_return"].mean()
    portfolio_returns.index = pd.to_datetime(portfolio_returns.index)
    return portfolio_returns.rename("teaching_factor_rotation")


def _build_benchmark_report(portfolio_returns: pd.Series, qqq_returns: pd.Series) -> pd.DataFrame:
    aligned = pd.concat([portfolio_returns, qqq_returns], axis=1).dropna()
    aligned.columns = ["teaching_factor_rotation", "QQQ"]
    metrics = []
    beta = _beta(aligned["teaching_factor_rotation"], aligned["QQQ"])
    annualized_strategy = _annualized_return(aligned["teaching_factor_rotation"])
    annualized_qqq = _annualized_return(aligned["QQQ"])
    alpha = annualized_strategy - beta * annualized_qqq
    values = {
        "teaching_factor_rotation": {
            "total_return": _total_return(aligned["teaching_factor_rotation"]),
            "annualized_return": annualized_strategy,
            "sharpe": _sharpe(aligned["teaching_factor_rotation"]),
            "max_drawdown": _max_drawdown(aligned["teaching_factor_rotation"]),
            "alpha": alpha,
            "beta": beta,
            "excess_annualized_return": annualized_strategy - annualized_qqq,
        },
        "QQQ": {
            "total_return": _total_return(aligned["QQQ"]),
            "annualized_return": annualized_qqq,
            "sharpe": _sharpe(aligned["QQQ"]),
            "max_drawdown": _max_drawdown(aligned["QQQ"]),
            "alpha": 0.0,
            "beta": 1.0,
            "excess_annualized_return": 0.0,
        },
    }
    for series, series_metrics in values.items():
        for metric, value in series_metrics.items():
            metrics.append({"series": series, "metric": metric, "value": value})
    return pd.DataFrame(metrics)


def _total_return(returns: pd.Series) -> float:
    return float((1.0 + returns).prod() - 1.0)


def _annualized_return(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float((1.0 + returns).prod() ** (12.0 / len(returns)) - 1.0)


def _sharpe(returns: pd.Series) -> float:
    std = float(returns.std(ddof=0))
    if std == 0:
        return 0.0
    return float(returns.mean() / std * np.sqrt(12.0))


def _max_drawdown(returns: pd.Series) -> float:
    wealth = (1.0 + returns).cumprod()
    drawdown = wealth / wealth.cummax() - 1.0
    return float(drawdown.min())


def _beta(strategy: pd.Series, benchmark: pd.Series) -> float:
    variance = float(benchmark.var(ddof=0))
    if variance == 0:
        return 0.0
    covariance = float(np.cov(strategy, benchmark, ddof=0)[0, 1])
    return covariance / variance


def _render_teaching_report(benchmark_report: pd.DataFrame) -> str:
    metric_lines = []
    for row in benchmark_report.itertuples(index=False):
        metric_lines.append(f"- {row.series} {row.metric}: {row.value:.6f}")
    return "\n".join(
        [
            "# Factor Discovery Sandbox Teaching Baseline",
            "",
            "mode: teaching_mode",
            "benchmark: QQQ",
            "survivorship_biased: true",
            "educational_only: true",
            "not_alpha_evidence: true",
            "production approval: not claimed",
            "",
            "This baseline intentionally uses a current-constituent survivorship bias fixture.",
            "It demonstrates the mechanics of a factor rotation workflow only.",
            "Signals are formed from month-end information and are evaluated on the next month return.",
            "",
            "## Metrics",
            *metric_lines,
            "",
        ]
    )
