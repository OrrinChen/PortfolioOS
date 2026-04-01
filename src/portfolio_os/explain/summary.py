"""Portfolio-level summary generation."""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.compliance.findings import aggregate_findings_for_reporting, summarize_findings
from portfolio_os.constraints.base import compute_weights_from_quantities, target_deviation_value
from portfolio_os.domain.enums import FindingSeverity, OrderSide
from portfolio_os.domain.models import Basket, ComplianceFinding
from portfolio_os.explain.trade_reason import build_benchmark_explanation, describe_reason_label, summarize_reason_counts
from portfolio_os.risk.model import RiskModelContext, portfolio_variance, tracking_error_variance
from portfolio_os.utils.config import AppConfig


def build_summary(
    universe: pd.DataFrame,
    basket: Basket,
    findings: list[ComplianceFinding],
    config: AppConfig,
    *,
    cash_before: float,
    cash_after: float,
    pre_trade_nav: float,
    post_trade_quantities: np.ndarray,
    benchmark_summary: dict[str, Any] | None = None,
    risk_context: RiskModelContext | None = None,
) -> dict[str, Any]:
    """Build portfolio-level summary metrics."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    current_weights = compute_weights_from_quantities(current_quantities, prices, pre_trade_nav)
    post_trade_nav = float(np.sum(post_trade_quantities * prices) + cash_after)
    post_trade_weights = compute_weights_from_quantities(post_trade_quantities, prices, post_trade_nav)

    buy_notional = float(
        sum(order.estimated_notional for order in basket.orders if order.side == OrderSide.BUY)
    )
    sell_notional = float(
        sum(order.estimated_notional for order in basket.orders if order.side == OrderSide.SELL)
    )
    buy_order_count = int(sum(1 for order in basket.orders if order.side == OrderSide.BUY))
    sell_order_count = int(sum(1 for order in basket.orders if order.side == OrderSide.SELL))
    severity_counts = Counter(finding.severity.value for finding in findings)
    rule_counts = Counter(finding.rule_code for finding in findings)
    finding_summary = summarize_findings(findings)
    target_deviation_before = target_deviation_value(current_quantities, prices, target_weights, pre_trade_nav)
    target_deviation_after = target_deviation_value(post_trade_quantities, prices, target_weights, pre_trade_nav)
    portfolio_variance_before = 0.0
    portfolio_variance_after = 0.0
    tracking_error_variance_before = 0.0
    tracking_error_variance_after = 0.0
    if risk_context is not None:
        portfolio_variance_before = portfolio_variance(
            current_quantities,
            prices,
            pre_trade_nav,
            risk_context.sigma,
        )
        portfolio_variance_after = portfolio_variance(
            post_trade_quantities,
            prices,
            pre_trade_nav,
            risk_context.sigma,
        )
        tracking_error_variance_before = tracking_error_variance(
            current_quantities,
            prices,
            pre_trade_nav,
            target_weights,
            risk_context.sigma,
        )
        tracking_error_variance_after = tracking_error_variance(
            post_trade_quantities,
            prices,
            pre_trade_nav,
            target_weights,
            risk_context.sigma,
        )

    top_weight_changes: list[dict[str, Any]] = []
    for ticker, before, after in zip(universe["ticker"], current_weights, post_trade_weights, strict=True):
        top_weight_changes.append(
            {
                "ticker": str(ticker),
                "weight_before": float(before),
                "weight_after": float(after),
                "delta": float(after - before),
            }
        )
    top_weight_changes.sort(key=lambda item: abs(item["delta"]), reverse=True)
    top_weight_changes = top_weight_changes[: config.reporting.top_weight_changes]

    blocked_summary = [
        {
            "ticker": finding.ticker,
            "message": finding.message,
            "reason": describe_reason_label(str(finding.details.get("reason_label", finding.code))),
        }
        for finding in findings
        if finding.rule_code in {"trade_blocked", "no_order_due_to_constraint", "cash_repair_failed", "small_order_removed"}
    ]
    blocked_reason_counts = summarize_reason_counts(findings, blocked_only=True)
    repair_reason_counts = summarize_reason_counts(findings, repaired_only=True)
    return {
        "account_id": config.portfolio_state.account_id,
        "cash_before": float(cash_before),
        "cash_after": float(cash_after),
        "pre_trade_nav": float(pre_trade_nav),
        "post_trade_nav": float(post_trade_nav),
        "total_buy_notional": buy_notional,
        "total_sell_notional": sell_notional,
        "gross_traded_notional": float(basket.gross_traded_notional),
        "turnover": float(basket.gross_traded_notional / pre_trade_nav) if pre_trade_nav else 0.0,
        "estimated_total_fee": float(basket.total_fee),
        "estimated_total_slippage": float(basket.total_slippage),
        "estimated_total_cost": float(basket.total_cost),
        "buy_order_count": buy_order_count,
        "sell_order_count": sell_order_count,
        "blocked_trade_count": int(finding_summary["blocked_trade_count"]),
        "compliance_finding_count": len(findings),
        "target_deviation_before": float(target_deviation_before),
        "target_deviation_after": float(target_deviation_after),
        "target_deviation_improvement": float(target_deviation_before - target_deviation_after),
        "portfolio_variance_before": float(portfolio_variance_before),
        "portfolio_variance_after": float(portfolio_variance_after),
        "tracking_error_variance_before": float(tracking_error_variance_before),
        "tracking_error_variance_after": float(tracking_error_variance_after),
        "finding_severity_counts": dict(severity_counts),
        "finding_category_counts": dict(finding_summary["category_counts"]),
        "finding_repair_status_counts": dict(finding_summary["repair_status_counts"]),
        "finding_rule_counts": dict(rule_counts),
        "blocked_or_filtered": blocked_summary,
        "blocked_reason_counts": blocked_reason_counts,
        "repair_reason_counts": repair_reason_counts,
        "top_weight_changes": top_weight_changes,
        "disclaimer": config.project.disclaimer,
        "finding_count": len(findings),
        "warning_count": severity_counts.get(FindingSeverity.WARNING.value, 0),
        "breach_count": severity_counts.get(FindingSeverity.BREACH.value, 0),
        "blocking_finding_count": int(finding_summary["blocking_count"]),
        "unresolved_blocking_count": int(finding_summary["unresolved_blocking_count"]),
        "benchmark_summary": benchmark_summary,
        "benchmark_explanation": build_benchmark_explanation(benchmark_summary) if benchmark_summary else [],
        "report_labels": config.constraints.report_labels.model_dump(mode="json"),
    }


def render_summary_markdown(summary: dict[str, Any], findings: list[ComplianceFinding]) -> str:
    """Render the summary payload to Markdown."""

    lines = [
        "# PortfolioOS Summary",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Portfolio",
        f"- account_id: {summary['account_id']}",
        f"- cash_before: {summary['cash_before']:.2f}",
        f"- cash_after: {summary['cash_after']:.2f}",
        f"- pre_trade_nav: {summary['pre_trade_nav']:.2f}",
        f"- post_trade_nav: {summary['post_trade_nav']:.2f}",
        "",
        "## Trading Metrics",
        f"- total_buy_notional: {summary['total_buy_notional']:.2f}",
        f"- total_sell_notional: {summary['total_sell_notional']:.2f}",
        f"- gross_traded_notional: {summary['gross_traded_notional']:.2f}",
        f"- turnover: {summary['turnover']:.4f}",
        f"- estimated_total_fee: {summary['estimated_total_fee']:.2f}",
        f"- estimated_total_slippage: {summary['estimated_total_slippage']:.2f}",
        f"- estimated_total_cost: {summary['estimated_total_cost']:.2f}",
        f"- buy_order_count: {summary['buy_order_count']}",
        f"- sell_order_count: {summary['sell_order_count']}",
        f"- blocked_trade_count: {summary['blocked_trade_count']}",
        f"- compliance_finding_count: {summary['compliance_finding_count']}",
        "",
        "## Target Deviation",
        f"- target_deviation_before: {summary['target_deviation_before']:.6f}",
        f"- target_deviation_after: {summary['target_deviation_after']:.6f}",
        f"- target_deviation_improvement: {summary['target_deviation_improvement']:.6f}",
        "",
        "## Risk Metrics",
        f"- portfolio_variance_before: {summary['portfolio_variance_before']:.8f}",
        f"- portfolio_variance_after: {summary['portfolio_variance_after']:.8f}",
        f"- tracking_error_variance_before: {summary['tracking_error_variance_before']:.8f}",
        f"- tracking_error_variance_after: {summary['tracking_error_variance_after']:.8f}",
        "",
        "## Findings",
        f"- total_findings: {summary['finding_count']}",
        f"- warnings: {summary['warning_count']}",
        f"- breaches: {summary['breach_count']}",
        f"- blocking_findings: {summary['blocking_finding_count']}",
        f"- unresolved_blocking_findings: {summary['unresolved_blocking_count']}",
        "",
        "## Findings By Category",
    ]
    for category, count in sorted(summary["finding_category_counts"].items()):
        lines.append(f"- {category}: {count}")
    lines.extend(["", "## Findings By Severity"])
    for severity, count in sorted(summary["finding_severity_counts"].items()):
        lines.append(f"- {severity}: {count}")
    lines.extend(["", "## Findings By Repair Status"])
    for repair_status, count in sorted(summary["finding_repair_status_counts"].items()):
        lines.append(f"- {repair_status}: {count}")
    if summary["blocked_or_filtered"]:
        lines.extend(["", "## Blocked Or Filtered Trades"])
        for item in summary["blocked_or_filtered"]:
            lines.append(f"- {item['ticker']}: {item['message']} ({item['reason']})")
    if summary["blocked_reason_counts"]:
        lines.extend(["", "## Blocked Reason Summary"])
        for reason, count in sorted(summary["blocked_reason_counts"].items()):
            lines.append(f"- {reason}: {count}")
    if summary["repair_reason_counts"]:
        lines.extend(["", "## Repair Reason Summary"])
        for reason, count in sorted(summary["repair_reason_counts"].items()):
            lines.append(f"- {reason}: {count}")
    lines.extend(["", "## Top Weight Changes"])
    for item in summary["top_weight_changes"]:
        lines.append(
            f"- {item['ticker']}: {item['weight_before']:.4f} -> {item['weight_after']:.4f} "
            f"(delta {item['delta']:+.4f})"
        )
    if findings:
        lines.extend(["", "## Top Findings"])
        aggregated = aggregate_findings_for_reporting(findings)
        for finding in aggregated[:10]:
            ticker_text = f" [{finding['ticker']}]" if finding["ticker"] else ""
            repeat_text = f" x{finding['count']}" if int(finding["count"]) > 1 else ""
            action_text = (
                f" | action={finding['suggested_action']}"
                if bool(finding["blocking"])
                else ""
            )
            lines.append(
                f"- {finding['severity']}{ticker_text}{repeat_text} "
                f"[{finding['category']} | {finding['repair_status']} | "
                f"{'blocking' if finding['blocking'] else 'non_blocking'}{action_text}]: "
                f"{finding['message']}"
            )
    if summary.get("benchmark_summary"):
        benchmark_summary = summary["benchmark_summary"]
        lines.extend(
            [
                "",
                "## Benchmark Highlights",
                f"- cost_savings_vs_naive: {benchmark_summary['cost_savings_vs_naive']:.2f}",
                f"- cost_savings_vs_cost_unaware: {benchmark_summary['cost_savings_vs_cost_unaware']:.2f}",
                f"- turnover_reduction_vs_naive: {benchmark_summary['turnover_reduction_vs_naive']:.4f}",
                f"- blocked_trade_reduction_vs_naive: {benchmark_summary['blocked_trade_reduction_vs_naive']}",
                f"- conclusion: {benchmark_summary['conclusion']}",
            ]
        )
        if summary.get("benchmark_explanation"):
            lines.extend(["", "## Benchmark Explanation"])
            for bullet in summary["benchmark_explanation"]:
                lines.append(f"- {bullet}")
    return "\n".join(lines) + "\n"
