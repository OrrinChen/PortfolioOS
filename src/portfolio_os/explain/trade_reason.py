"""Human-readable explanations for trades, blocked actions, and repair steps."""

from __future__ import annotations

from collections import Counter

import pandas as pd

from portfolio_os.domain.models import ComplianceFinding


REASON_LABEL_TEXT = {
    "suspended": "suspended",
    "upper_limit_hit": "upper limit hit",
    "lower_limit_hit": "lower limit hit",
    "buy_blacklist": "buy blacklist",
    "sell_blacklist": "sell blacklist",
    "position_availability": "clipped to position availability",
    "participation_cap_binding": "participation cap binding",
    "rounded_to_board_lot": "rounded to board lot",
    "dust_order_below_min_notional": "removed as dust order",
    "insufficient_cash_after_repair": "insufficient cash after repair",
    "target_weight_sum_near_zero": "target weight sum near zero",
    "benchmark_weight_total_anomaly": "benchmark weight total anomaly",
    "target_weight_extreme_concentration": "extreme target concentration",
    "no_tradable_securities_in_snapshot": "no tradable securities in snapshot",
}


def describe_reason_label(reason_label: str | None) -> str:
    """Return a short human-readable label for a reason code."""

    if reason_label is None:
        return "unspecified"
    return REASON_LABEL_TEXT.get(reason_label, reason_label.replace("_", " "))


def reason_label_from_finding(finding: ComplianceFinding) -> str | None:
    """Extract a compact reason label from a finding."""

    reason_label = finding.details.get("reason_label")
    if isinstance(reason_label, str):
        return reason_label
    return finding.code


def summarize_reason_counts(
    findings: list[ComplianceFinding],
    *,
    blocked_only: bool = False,
    repaired_only: bool = False,
) -> dict[str, int]:
    """Count reason labels for blocked or repaired findings."""

    counter: Counter[str] = Counter()
    for finding in findings:
        if blocked_only and finding.code not in {"trade_blocked", "no_order_due_to_constraint"}:
            continue
        if repaired_only and finding.repair_status.value not in {"repaired", "partially_repaired"}:
            continue
        label = reason_label_from_finding(finding)
        if label is None:
            continue
        counter[describe_reason_label(label)] += 1
    return dict(counter)


def build_trade_reason(
    *,
    row: pd.Series,
    signed_quantity: float,
    estimated_fee: float,
    estimated_slippage: float,
    effective_single_name_limit: float,
) -> str:
    """Generate a short human-readable reason for one live order."""

    ticker = str(row["ticker"])
    quantity = int(abs(signed_quantity))
    notional = abs(signed_quantity) * float(row["estimated_price"])
    cost_bps = ((estimated_fee + estimated_slippage) / notional * 10000.0) if notional else 0.0
    current_weight = float(row["current_weight"])
    target_weight = float(row["target_weight"])
    if signed_quantity < 0:
        reasons = ["current weight is above target"]
        if current_weight > effective_single_name_limit:
            reasons.append("single-name cap pressure is present")
        return (
            f"Sell {ticker} {quantity} shares because "
            f"{', '.join(reasons)}; estimated trading cost is {cost_bps:.1f} bps."
        )

    reasons = ["target weight is above current weight"]
    if float(row["adv_shares"]) > 0:
        reasons.append("liquidity is acceptable for the snapshot")
    return (
        f"Buy {ticker} {quantity} shares because "
        f"{', '.join(reasons)}; estimated trading cost is {cost_bps:.1f} bps."
    )


def build_benchmark_explanation(comparison_summary: dict[str, float]) -> list[str]:
    """Return a few heuristic benchmark explanation bullets."""

    bullets: list[str] = []
    if comparison_summary.get("turnover_reduction_vs_naive", 0.0) > 0:
        bullets.append("PortfolioOS spent less by trading less gross notional than the naive basket.")
    if comparison_summary.get("blocked_trade_reduction_vs_naive", 0) > 0:
        bullets.append("PortfolioOS avoided tickets that would have been blocked by tradability or blacklist constraints.")
    if comparison_summary.get("cost_savings_vs_cost_unaware", 0.0) > 0:
        bullets.append("The cost-aware objective suppressed higher-friction trades relative to the cost-unaware optimizer.")
    if not bullets:
        bullets.append("PortfolioOS and the baselines were close on this snapshot, so the advantage came from more modest execution choices.")
    return bullets
