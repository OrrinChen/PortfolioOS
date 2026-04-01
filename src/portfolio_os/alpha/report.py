"""Markdown rendering for alpha research artifacts."""

from __future__ import annotations

import pandas as pd


def _fmt_pct(value: float) -> str:
    """Format one decimal value as a percentage string."""

    return f"{value * 100.0:.2f}%"


def _fmt_float(value: float) -> str:
    """Format one float with four decimals."""

    return f"{value:.4f}"


def render_alpha_research_report(
    summary_payload: dict[str, object],
    *,
    ic_frame: pd.DataFrame,
    signal_summary_frame: pd.DataFrame,
) -> str:
    """Render one compact markdown report for baseline alpha diagnostics."""

    params = summary_payload["parameters"]
    primary_signal_name = str(summary_payload["primary_signal_name"])
    primary_ic_frame = (
        ic_frame.loc[ic_frame["signal_name"] == primary_signal_name].copy()
        if "signal_name" in ic_frame.columns
        else ic_frame.copy()
    )
    lines = [
        "# Alpha Research Report",
        "",
        f"- Returns file: `{summary_payload['returns_file']}`",
        f"- Date range: {summary_payload['date_range']['start']} to {summary_payload['date_range']['end']}",
        f"- Ticker count: {summary_payload['ticker_count']}",
        f"- Evaluation dates: {summary_payload['evaluation_date_count']}",
        f"- Primary signal: `{primary_signal_name}`",
        "",
        "## Parameters",
        f"- Reversal lookback days: {params['reversal_lookback_days']}",
        f"- Momentum lookback days: {params['momentum_lookback_days']}",
        f"- Momentum skip days: {params['momentum_skip_days']}",
        f"- Forward horizon days: {params['forward_horizon_days']}",
        f"- Reversal weight: {_fmt_float(float(params['reversal_weight']))}",
        f"- Momentum weight: {_fmt_float(float(params['momentum_weight']))}",
        f"- Min assets per date: {params['min_assets_per_date']}",
        f"- Quantiles: {params['quantiles']}",
        "",
        "## Summary",
        f"- Mean IC: {_fmt_float(float(summary_payload['mean_ic']))}",
        f"- Mean Rank IC: {_fmt_float(float(summary_payload['mean_rank_ic']))}",
        f"- Positive Rank IC Ratio: {_fmt_pct(float(summary_payload['positive_rank_ic_ratio']))}",
        f"- Mean Top-Bottom Spread: {_fmt_pct(float(summary_payload['mean_top_bottom_spread']))}",
        f"- Best signal by mean Rank IC: {summary_payload['best_signal_name']} ({_fmt_float(float(summary_payload['best_signal_mean_rank_ic']))})",
        f"- Best Rank IC date: {summary_payload['best_rank_ic_date']} ({_fmt_float(float(summary_payload['best_rank_ic']))})",
        f"- Worst Rank IC date: {summary_payload['worst_rank_ic_date']} ({_fmt_float(float(summary_payload['worst_rank_ic']))})",
        "",
        "## Signal Leaderboard",
        "",
        "| Signal | Eval Dates | Mean IC | Mean Rank IC | Positive Rank IC Ratio | Mean Top-Bottom Spread |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in signal_summary_frame.to_dict(orient="records"):
        lines.append(
            f"| {row['signal_name']} | {int(row['evaluation_date_count'])} | {_fmt_float(float(row['mean_ic']))} | "
            f"{_fmt_float(float(row['mean_rank_ic']))} | {_fmt_pct(float(row['positive_rank_ic_ratio']))} | "
            f"{_fmt_pct(float(row['mean_top_bottom_spread']))} |"
        )
    lines.extend(
        [
            "",
            f"## IC By Date ({primary_signal_name})",
            "",
            "| Date | Obs | IC | Rank IC | Top-Bottom Spread |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in primary_ic_frame.to_dict(orient="records"):
        lines.append(
            f"| {row['date']} | {int(row['observation_count'])} | {_fmt_float(float(row['ic']))} | "
            f"{_fmt_float(float(row['rank_ic']))} | {_fmt_pct(float(row['top_bottom_spread']))} |"
        )
    lines.append("")
    return "\n".join(lines)
