"""Reporting helpers for execution simulation outputs."""

from __future__ import annotations

import pandas as pd

from portfolio_os.execution.simulator import (
    ExecutionOrderResult,
    ExecutionPortfolioSummary,
    ExecutionSimulationResult,
)


def build_execution_fills_frame(result: ExecutionSimulationResult) -> pd.DataFrame:
    """Flatten per-order execution results into a CSV-friendly frame."""

    rows = [
        {
            "ticker": order.ticker,
            "side": order.side,
            "ordered_quantity": order.ordered_quantity,
            "filled_quantity": order.filled_quantity,
            "unfilled_quantity": order.unfilled_quantity,
            "fill_ratio": order.fill_ratio,
            "average_fill_price": order.average_fill_price,
            "estimated_fee": order.estimated_fee,
            "estimated_slippage": order.estimated_slippage,
            "estimated_total_cost": order.estimated_total_cost,
            "evaluated_average_fill_price": order.evaluated_average_fill_price,
            "evaluated_fee": order.evaluated_fee,
            "evaluated_slippage": order.evaluated_slippage,
            "evaluated_total_cost": order.evaluated_total_cost,
            "status": order.status,
        }
        for order in result.per_order_results
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "ticker",
            "side",
            "ordered_quantity",
            "filled_quantity",
            "unfilled_quantity",
            "fill_ratio",
            "average_fill_price",
            "estimated_fee",
            "estimated_slippage",
            "estimated_total_cost",
            "evaluated_average_fill_price",
            "evaluated_fee",
            "evaluated_slippage",
            "evaluated_total_cost",
            "status",
        ],
    )


def build_execution_child_orders_frame(result: ExecutionSimulationResult) -> pd.DataFrame:
    """Flatten bucket-level child order details into a CSV-friendly frame."""

    rows: list[dict[str, object]] = []
    for order in result.per_order_results:
        for bucket in order.bucket_results:
            rows.append(
                {
                    "ticker": order.ticker,
                    "side": order.side,
                    "bucket_index": bucket.bucket_index,
                    "bucket_label": bucket.bucket_label,
                    "bucket_status": bucket.status,
                    "requested_quantity": bucket.requested_quantity,
                    "filled_quantity": bucket.filled_quantity,
                    "remaining_quantity": bucket.remaining_quantity,
                    "bucket_available_volume": bucket.bucket_available_volume,
                    "bucket_participation_limit": bucket.bucket_participation_limit,
                    "bucket_fill_cap": bucket.bucket_fill_cap,
                    "fill_price": bucket.fill_price,
                    "estimated_fee": bucket.estimated_fee,
                    "estimated_slippage": bucket.estimated_slippage,
                    "estimated_total_cost": bucket.estimated_total_cost,
                    "evaluated_fill_price": bucket.evaluated_fill_price,
                    "evaluated_fee": bucket.evaluated_fee,
                    "evaluated_slippage": bucket.evaluated_slippage,
                    "evaluated_total_cost": bucket.evaluated_total_cost,
                    "slippage_multiplier": bucket.slippage_multiplier,
                    "forced_completion": bucket.forced_completion,
                    "liquidity_constrained": bucket.liquidity_constrained,
                }
            )
    return pd.DataFrame(
        rows,
        columns=[
            "ticker",
            "side",
            "bucket_index",
            "bucket_label",
            "bucket_status",
            "requested_quantity",
            "filled_quantity",
            "remaining_quantity",
            "bucket_available_volume",
            "bucket_participation_limit",
            "bucket_fill_cap",
            "fill_price",
            "estimated_fee",
            "estimated_slippage",
            "estimated_total_cost",
            "evaluated_fill_price",
            "evaluated_fee",
            "evaluated_slippage",
            "evaluated_total_cost",
            "slippage_multiplier",
            "forced_completion",
            "liquidity_constrained",
        ],
    )


def _build_summary_delta(
    baseline: ExecutionPortfolioSummary,
    stress: ExecutionPortfolioSummary,
) -> dict[str, float]:
    """Build compact stress-vs-baseline summary deltas."""

    return {
        "fill_rate_delta": float(stress.fill_rate - baseline.fill_rate),
        "partial_fill_count_delta": float(stress.partial_fill_count - baseline.partial_fill_count),
        "unfilled_order_count_delta": float(stress.unfilled_order_count - baseline.unfilled_order_count),
        "inactive_bucket_count_delta": float(stress.inactive_bucket_count - baseline.inactive_bucket_count),
        "total_cost_delta": float(stress.total_cost - baseline.total_cost),
        "total_unfilled_notional_delta": float(
            stress.total_unfilled_notional - baseline.total_unfilled_notional
        ),
    }


def _extract_residual_risk_markers(
    result: ExecutionSimulationResult,
) -> dict[str, object]:
    """Extract concise residual-risk markers from simulation outputs."""

    constrained_tickers = sorted(
        {
            order.ticker
            for order in result.per_order_results
            if order.status in {"partial_fill", "unfilled"}
        }
    )
    constrained_buckets = sorted(
        {
            f"{order.ticker}:{bucket.bucket_label}"
            for order in result.per_order_results
            for bucket in order.bucket_results
            if bucket.status == "partial_fill" or (bucket.status == "unfilled" and bucket.requested_quantity > 0)
        }
    )
    return {
        "constrained_tickers": constrained_tickers,
        "constrained_buckets": constrained_buckets,
    }


def build_execution_report_payload(
    result: ExecutionSimulationResult,
    *,
    stress_result: ExecutionSimulationResult | None = None,
) -> dict:
    """Build the JSON execution report payload."""

    stress_payload = {
        "enabled": False,
        "profile": None,
        "portfolio_summary": None,
        "delta_vs_baseline": None,
        "conclusion": None,
    }
    if stress_result is not None:
        baseline_risk = _extract_residual_risk_markers(result)
        stress_risk = _extract_residual_risk_markers(stress_result)
        stress_payload = {
            "enabled": True,
            "profile": stress_result.resolved_calibration.get("selected_profile"),
            "portfolio_summary": stress_result.portfolio_summary.model_dump(mode="json"),
            "delta_vs_baseline": _build_summary_delta(
                result.portfolio_summary,
                stress_result.portfolio_summary,
            ),
            "residual_risk": {
                "baseline": baseline_risk,
                "stress": stress_risk,
            },
            "conclusion": stress_result.conclusion,
        }
    return {
        "run_id": result.run_id,
        "created_at": result.created_at,
        "disclaimer": result.disclaimer,
        "request": result.request_metadata,
        "request_path": result.request_path,
        "cost_comparison": {
            "planned_cost": result.portfolio_summary.total_cost,
            "evaluated_cost": result.portfolio_summary.evaluated_total_cost,
            "planned_slippage": result.portfolio_summary.total_slippage,
            "evaluated_slippage": result.portfolio_summary.evaluated_total_slippage,
            "planned_fee": result.portfolio_summary.total_fee,
            "evaluated_fee": result.portfolio_summary.evaluated_total_fee,
        },
        "resolved_calibration": result.resolved_calibration,
        "bucket_curve": result.bucket_curve,
        "per_order_results": [
            order.model_dump(mode="json") for order in result.per_order_results
        ],
        "portfolio_summary": result.portfolio_summary.model_dump(mode="json"),
        "stress_test": stress_payload,
        "source_artifacts": result.source_artifacts,
        "conclusion": result.conclusion,
    }


def _format_money(value: float) -> str:
    """Format a money-like value for Markdown."""

    return f"{value:,.2f}"


def _format_percent(value: float) -> str:
    """Format a ratio as a percentage."""

    return f"{value:.1%}"


def _worst_orders(order_results: list[ExecutionOrderResult], *, limit: int = 3) -> list[ExecutionOrderResult]:
    """Return the worst execution outcomes for the report."""

    return sorted(
        order_results,
        key=lambda order: (
            order.fill_ratio,
            -(order.estimated_total_cost / order.ordered_notional if order.ordered_notional > 0 else 0.0),
            -order.unfilled_notional,
        ),
    )[:limit]


def render_execution_report_markdown(
    result: ExecutionSimulationResult,
    *,
    stress_result: ExecutionSimulationResult | None = None,
) -> str:
    """Render a PM / trader / risk-facing execution simulation report."""

    summary = result.portfolio_summary
    worst_orders = _worst_orders(result.per_order_results)
    lines = [
        "# Execution Simulation Report",
        "",
        f"> {result.disclaimer}",
        "",
        "## Request",
        f"- name: {result.request_metadata['name']}",
        f"- artifact_dir: {result.request_metadata['artifact_dir']}",
        f"- input_orders: {result.request_metadata['input_orders']}",
        f"- mode: {result.request_metadata['simulation']['mode']}",
        "",
        "## Calibration",
        f"- selected_profile: {result.resolved_calibration['selected_profile']['name']}",
        f"- selected_profile_source: {result.resolved_calibration['selected_profile']['source']}",
        f"- selected_profile_path: {result.resolved_calibration['selected_profile']['path']}",
        f"- request_overrides: {', '.join(result.resolved_calibration['overridden_fields']) or 'none'}",
        "",
        "## Execution Summary",
        f"- fill_rate: {_format_percent(summary.fill_rate)}",
        f"- filled_order_count: {summary.filled_order_count}",
        f"- partial_fill_count: {summary.partial_fill_count}",
        f"- unfilled_order_count: {summary.unfilled_order_count}",
        f"- inactive_bucket_count: {summary.inactive_bucket_count}",
        f"- total_filled_notional: {_format_money(summary.total_filled_notional)}",
        f"- total_unfilled_notional: {_format_money(summary.total_unfilled_notional)}",
        f"- planned_total_cost: {_format_money(summary.total_cost)}",
        f"- evaluated_total_cost: {_format_money(summary.evaluated_total_cost)}",
        "",
        "## Worst 3 Orders",
        "| ticker | side | status | fill_ratio | unfilled_quantity | total_cost |",
        "| --- | --- | --- | --- | ---: | ---: |",
    ]
    for order in worst_orders:
        lines.append(
            "| "
            f"{order.ticker} | "
            f"{order.side} | "
            f"{order.status} | "
            f"{_format_percent(order.fill_ratio)} | "
            f"{order.unfilled_quantity} | "
            f"{_format_money(order.estimated_total_cost)} |"
        )
    if not worst_orders:
        lines.append("| none | - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Conclusion",
            result.conclusion,
            "",
        ]
    )
    if stress_result is not None:
        stress_summary = stress_result.portfolio_summary
        lines.extend(
            [
                "## Stress Comparison",
                f"- stress_profile: {stress_result.resolved_calibration['selected_profile']['name']}",
                f"- stress_fill_rate: {_format_percent(stress_summary.fill_rate)}",
                f"- stress_partial_fill_count: {stress_summary.partial_fill_count}",
                f"- stress_unfilled_order_count: {stress_summary.unfilled_order_count}",
                f"- stress_inactive_bucket_count: {stress_summary.inactive_bucket_count}",
                f"- stress_total_cost: {_format_money(stress_summary.total_cost)}",
                f"- stress_total_unfilled_notional: {_format_money(stress_summary.total_unfilled_notional)}",
                f"- delta_fill_rate: {_format_percent(stress_summary.fill_rate - summary.fill_rate)}",
                f"- delta_unfilled_notional: {_format_money(stress_summary.total_unfilled_notional - summary.total_unfilled_notional)}",
                f"- baseline_residual_tickers: {', '.join(_extract_residual_risk_markers(result)['constrained_tickers']) or 'none'}",
                f"- stress_residual_tickers: {', '.join(_extract_residual_risk_markers(stress_result)['constrained_tickers']) or 'none'}",
                "",
                f"- stress_conclusion: {stress_result.conclusion}",
                "",
            ]
        )
    return "\n".join(lines)
