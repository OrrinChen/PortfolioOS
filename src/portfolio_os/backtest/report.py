"""Markdown reporting for the historical backtest CLI."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from portfolio_os.backtest.engine import BacktestResult


def _fmt_pct(value: float) -> str:
    """Format one decimal percentage point string."""

    return f"{value * 100.0:.2f}%"


def _fmt_float(value: float) -> str:
    """Format one float with two decimals."""

    return f"{value:.2f}"


def _append_pairwise_comparison(
    lines: list[str],
    *,
    title: str,
    comparison: dict[str, float],
    prefix: str,
) -> None:
    """Append one pairwise comparison section when the required keys exist."""

    required_key = f"{prefix}_ending_nav_delta"
    if required_key not in comparison:
        return
    lines.extend(
        [
            "",
            f"## {title}",
            f"- Ending NAV delta: {_fmt_float(float(comparison[required_key]))}",
            f"- Total return delta: {_fmt_pct(float(comparison[f'{prefix}_total_return_delta']))}",
            f"- Annualized return delta: {_fmt_pct(float(comparison[f'{prefix}_annualized_return_delta']))}",
            f"- Sharpe delta: {_fmt_float(float(comparison[f'{prefix}_sharpe_delta']))}",
            f"- Total cost delta: {_fmt_float(float(comparison[f'{prefix}_total_cost_delta']))}",
            f"- Total turnover delta: {_fmt_float(float(comparison[f'{prefix}_total_turnover_delta']))}",
        ]
    )


def render_backtest_report(result: "BacktestResult") -> str:
    """Render a compact markdown report for the backtest outputs."""

    summary = result.summary
    lines = [
        "# Backtest Report",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Overview",
        f"- Manifest: `{result.manifest_path}`",
        f"- Date range: {summary['start_date']} to {summary['end_date']}",
        f"- Rebalance count: {summary['rebalance_count']}",
        "",
        "## Strategy Summary",
        "",
        "| Strategy | Ending NAV | Total Return | Annualized Return | Sharpe | Max Drawdown | Total Turnover | Total Cost |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for strategy_name, metrics in summary["strategies"].items():
        lines.append(
            f"| {strategy_name} | {_fmt_float(metrics['ending_nav'])} | {_fmt_pct(metrics['total_return'])} | "
            f"{_fmt_pct(metrics['annualized_return'])} | {_fmt_float(metrics['sharpe'])} | "
            f"{_fmt_pct(metrics['max_drawdown'])} | {_fmt_float(metrics['total_turnover'])} | "
            f"{_fmt_float(metrics['total_transaction_cost'])} |"
        )

    comparison = summary.get("comparison", {})
    alpha_model = summary.get("alpha_model", {})
    if alpha_model.get("enabled"):
        lines.extend(
            [
                "",
                "## Alpha Model",
                f"- Recipe: `{alpha_model.get('recipe_name')}`",
                f"- Alpha weight: {_fmt_float(float(alpha_model.get('alpha_weight', 0.0)))}",
                f"- Alpha panel rows: {int(alpha_model.get('panel_row_count', 0))}",
                (
                    "- Signal-ready rebalances: "
                    f"{int(alpha_model.get('rebalance_dates_with_alpha_signal', 0))} / {summary['rebalance_count']}"
                ),
                (
                    "- Cold-start rebalances without enough history: "
                    f"{int(alpha_model.get('rebalance_dates_without_alpha_signal', 0))}"
                ),
                (
                    "- Alpha-only baseline enabled: "
                    f"{'yes' if alpha_model.get('add_alpha_only_baseline') else 'no'}"
                ),
            ]
        )

    _append_pairwise_comparison(
        lines,
        title="Optimizer Vs Naive",
        comparison=comparison,
        prefix="optimizer_vs_naive",
    )
    _append_pairwise_comparison(
        lines,
        title="Optimizer Vs Alpha-Only",
        comparison=comparison,
        prefix="optimizer_vs_alpha_only",
    )
    _append_pairwise_comparison(
        lines,
        title="Alpha-Only Vs Naive",
        comparison=comparison,
        prefix="alpha_only_vs_naive",
    )

    optimizer_rows = result.period_attribution.loc[result.period_attribution["strategy"] == "optimizer"].copy()
    if not optimizer_rows.empty:
        lines.extend(
            [
                "",
                "## Optimizer Period Attribution",
                "",
                "| Period | Start | End | Holding PnL | Active Trading PnL | Trading Cost PnL | Period PnL | Vs Naive Delta |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in optimizer_rows.to_dict(orient="records"):
            delta = row.get("optimizer_vs_naive_period_pnl_delta")
            delta_text = _fmt_float(float(delta)) if pd.notna(delta) else "n/a"
            lines.append(
                f"| {int(row['period_index'])} | {row['start_date']} | {row['end_date']} | "
                f"{_fmt_float(float(row['holding_pnl']))} | {_fmt_float(float(row['active_trading_pnl']))} | "
                f"{_fmt_float(float(row['trading_cost_pnl']))} | {_fmt_float(float(row['period_pnl']))} | {delta_text} |"
            )

    lines.extend(
        [
            "",
            "## Conclusion",
        ]
    )
    if comparison:
        delta = float(comparison["optimizer_vs_naive_ending_nav_delta"])
        if delta > 0:
            lines.append(
                f"Optimizer finished ahead of naive pro-rata by {_fmt_float(delta)} of ending NAV after explicit transaction costs."
            )
        elif delta < 0:
            lines.append(
                f"Optimizer finished behind naive pro-rata by {_fmt_float(abs(delta))} of ending NAV after explicit transaction costs."
            )
        else:
            lines.append("Optimizer and naive pro-rata finished at the same ending NAV after explicit transaction costs.")
        if "optimizer_vs_alpha_only_sharpe_delta" in comparison:
            alpha_only_sharpe_delta = float(comparison["optimizer_vs_alpha_only_sharpe_delta"])
            if alpha_only_sharpe_delta >= 0.0:
                lines.append(
                    f"Optimizer matched or exceeded alpha-only on Sharpe by {_fmt_float(alpha_only_sharpe_delta)}."
                )
            else:
                lines.append(
                    f"Optimizer lagged alpha-only on Sharpe by {_fmt_float(abs(alpha_only_sharpe_delta))}."
                )
    else:
        lines.append("No optimizer-vs-naive comparison was available for this backtest run.")
    lines.append("")
    return "\n".join(lines)


def render_cost_sweep_report(summary_frame: pd.DataFrame, *, base_manifest_path) -> str:
    """Render a compact markdown report for the cost bundle sweep."""

    lines = [
        "# Cost Bundle Sweep Report",
        "",
        f"- Base manifest: `{base_manifest_path}`",
        "",
        "| Multiplier | Ending NAV | Annualized Return | Sharpe | Max Drawdown | Total Turnover | Total Cost | Vs Naive Ending NAV Delta |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_frame.to_dict(orient="records"):
        lines.append(
            f"| {row['cost_bundle_multiplier']:.3f} | {row['ending_nav']:.2f} | {row['annualized_return'] * 100.0:.2f}% | "
            f"{row['sharpe']:.2f} | {row['max_drawdown'] * 100.0:.2f}% | {row['total_turnover']:.4f} | "
            f"{row['total_transaction_cost']:.2f} | {row['optimizer_vs_naive_ending_nav_delta']:.2f} |"
        )
    if not summary_frame.empty:
        best_nav = summary_frame.loc[summary_frame["ending_nav"].idxmax()]
        best_sharpe = summary_frame.loc[summary_frame["sharpe"].idxmax()]
        lowest_cost = summary_frame.loc[summary_frame["total_transaction_cost"].idxmin()]
        lines.extend(
            [
                "",
                "## Highlights",
                f"- Best ending NAV multiplier: {best_nav['cost_bundle_multiplier']:.3f}",
                f"- Best Sharpe multiplier: {best_sharpe['cost_bundle_multiplier']:.3f}",
                f"- Lowest explicit cost multiplier: {lowest_cost['cost_bundle_multiplier']:.3f}",
                "",
            ]
        )
    return "\n".join(lines)


def render_risk_sweep_report(summary_frame: pd.DataFrame, *, base_manifest_path) -> str:
    """Render a compact markdown report for the risk aversion sweep."""

    lines = [
        "# Risk Aversion Sweep Report",
        "",
        f"- Base manifest: `{base_manifest_path}`",
        "",
        "| Multiplier | risk_term Weight | Ending NAV | Ann. Return | Ann. Vol | Sharpe | Max DD | Turnover | Cost | Vs Naive Delta |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary_frame.to_dict(orient="records"):
        lines.append(
            f"| {_fmt_float(float(row['risk_aversion_multiplier']))} | {_fmt_float(float(row['risk_term_weight']))} | "
            f"{_fmt_float(float(row['ending_nav']))} | {_fmt_pct(float(row['annualized_return']))} | "
            f"{_fmt_pct(float(row['annualized_volatility']))} | {_fmt_float(float(row['sharpe']))} | "
            f"{_fmt_pct(float(row['max_drawdown']))} | {_fmt_float(float(row['total_turnover']))} | "
            f"{_fmt_float(float(row['total_transaction_cost']))} | "
            f"{_fmt_float(float(row['optimizer_vs_naive_ending_nav_delta']))} |"
        )
    if not summary_frame.empty:
        best_sharpe = summary_frame.loc[summary_frame["sharpe"].idxmax()]
        lowest_volatility = summary_frame.loc[summary_frame["annualized_volatility"].idxmin()]
        best_nav = summary_frame.loc[summary_frame["ending_nav"].idxmax()]
        lines.extend(
            [
                "",
                "## Highlights",
                f"- Best Sharpe multiplier: {_fmt_float(float(best_sharpe['risk_aversion_multiplier']))}",
                f"- Lowest volatility multiplier: {_fmt_float(float(lowest_volatility['risk_aversion_multiplier']))}",
                f"- Best ending NAV multiplier: {_fmt_float(float(best_nav['risk_aversion_multiplier']))}",
                "",
            ]
        )
    return "\n".join(lines)
