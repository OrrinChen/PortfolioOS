"""Paper-calibration payload and report helpers."""

from __future__ import annotations

from typing import Any

from portfolio_os.execution.models import ExecutionResult


def build_paper_calibration_payload(
    *,
    strategy_name: str,
    target_manifest: dict[str, Any],
    execution_result: ExecutionResult,
    expected_assumptions: dict[str, Any],
    reference_snapshot_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact simulator-vs-paper calibration payload."""

    submitted_count = int(execution_result.submitted_count)
    realized_denominator = max(1, submitted_count)
    realized_order_count = int(execution_result.filled_count + execution_result.partial_count)
    fill_rate = float(realized_order_count) / float(realized_denominator)
    partial_fill_frequency = float(execution_result.partial_count) / float(realized_denominator)

    avg_fill_price = None
    prices = [order.avg_fill_price for order in execution_result.orders if order.avg_fill_price is not None]
    if prices:
        avg_fill_price = float(sum(prices) / len(prices))

    return {
        "strategy_name": str(strategy_name),
        "target_manifest": target_manifest,
        "expected_assumptions": expected_assumptions,
        "reference_snapshot_summary": dict(reference_snapshot_summary or {}),
        "realized_summary": {
            "submitted_count": submitted_count,
            "filled_count": int(execution_result.filled_count),
            "partial_count": int(execution_result.partial_count),
            "unfilled_count": int(execution_result.unfilled_count),
            "rejected_count": int(execution_result.rejected_count),
            "timeout_cancelled_count": int(execution_result.timeout_cancelled_count),
            "fill_rate": fill_rate,
            "partial_fill_frequency": partial_fill_frequency,
            "average_fill_price": avg_fill_price,
        },
        "deviation_summary": {
            "fill_rate_vs_full_completion": float(fill_rate - 1.0),
            "partial_fill_frequency": partial_fill_frequency,
            "timeout_cancelled_count": int(execution_result.timeout_cancelled_count),
            "rejected_count": int(execution_result.rejected_count),
        },
    }


def render_paper_calibration_report_markdown(payload: dict[str, Any]) -> str:
    """Render a Markdown calibration report."""

    realized = payload["realized_summary"]
    reference_summary = payload.get("reference_snapshot_summary", {})
    return "\n".join(
        [
            "# Paper Calibration Report",
            "",
            "## Strategy",
            f"- Name: {payload['strategy_name']}",
            f"- Selected tickers: {payload['target_manifest'].get('selected_tickers', [])}",
            "",
            "## Reference Snapshot",
            f"- Captured ticker count: {reference_summary.get('captured_ticker_count', 0)}",
            f"- With dedicated reference price: {reference_summary.get('with_reference_price_count', 0)}",
            f"- With mid-price quote: {reference_summary.get('with_mid_price_count', 0)}",
            f"- Fallback reference count: {reference_summary.get('fallback_reference_count', 0)}",
            "",
            "## Realized Execution",
            f"- Fill rate: {realized['fill_rate']:.1%}",
            f"- Partial fill frequency: {realized['partial_fill_frequency']:.1%}",
            f"- Average fill price: {realized['average_fill_price']}",
            "",
            "## Slippage / Deviation",
            f"- Expected assumptions: {payload['expected_assumptions']}",
            f"- Deviation summary: {payload['deviation_summary']}",
        ]
    )
