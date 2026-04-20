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


def render_alpha_acceptance_note(
    decision_payload: dict[str, object],
    *,
    summary_frame: pd.DataFrame,
) -> str:
    """Render one markdown closeout note for the Phase 1 alpha acceptance gate."""

    holdout_frame = (
        summary_frame.loc[summary_frame["slice_name"] == "holdout"].copy()
        if not summary_frame.empty and "slice_name" in summary_frame.columns
        else pd.DataFrame()
    )
    lines = [
        "# Phase 1 Alpha Acceptance Note",
        "",
        f"- Final status: `{decision_payload['status']}`",
        f"- Acceptance mode: `{decision_payload.get('acceptance_mode')}`",
        f"- Accepted recipe: `{decision_payload.get('accepted_recipe_name')}`",
        f"- Baseline recipe: `{decision_payload.get('baseline_recipe_name')}`",
        f"- Stop reason: `{decision_payload.get('stop_reason')}`",
        f"- Next recommended action: `{decision_payload.get('next_recommended_action')}`",
        "",
    ]
    accepted_metrics = decision_payload.get("accepted_holdout_metrics")
    if isinstance(accepted_metrics, dict):
        lines.extend(
            [
                "## Accepted Holdout Metrics",
                f"- Mean IC: {_fmt_float(float(accepted_metrics['mean_ic']))}",
                f"- Mean Rank IC: {_fmt_float(float(accepted_metrics['mean_rank_ic']))}",
                f"- Positive Rank IC Ratio: {_fmt_pct(float(accepted_metrics['positive_rank_ic_ratio']))}",
                f"- Mean Top-Bottom Spread: {_fmt_pct(float(accepted_metrics['mean_top_bottom_spread']))}",
                f"- Evaluation Dates: {int(accepted_metrics['evaluation_date_count'])}",
                f"- Mean Monthly Factor Turnover: {_fmt_float(float(accepted_metrics['mean_monthly_factor_turnover']))}",
                "",
            ]
        )
    if not holdout_frame.empty:
        lines.extend(
            [
                "## Holdout Leaderboard",
                "",
                "| Round | Recipe | Baseline | Mean Rank IC | Positive Rank IC Ratio | Mean Top-Bottom Spread | Turnover |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        ordered = holdout_frame.sort_values(
            by=["round_number", "mean_rank_ic", "positive_rank_ic_ratio", "mean_top_bottom_spread", "recipe_name"],
            ascending=[True, False, False, False, True],
        )
        for row in ordered.to_dict(orient="records"):
            turnover = float(row["mean_monthly_factor_turnover"])
            turnover_text = "n/a" if pd.isna(turnover) else _fmt_float(turnover)
            lines.append(
                f"| {int(row['round_number'])} | {row['recipe_name']} | {str(bool(row['is_baseline'])).lower()} | "
                f"{_fmt_float(float(row['mean_rank_ic']))} | {_fmt_pct(float(row['positive_rank_ic_ratio']))} | "
                f"{_fmt_pct(float(row['mean_top_bottom_spread']))} | {turnover_text} |"
            )
        lines.append("")
    return "\n".join(lines)
