"""Pre-trade compliance checks."""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio_os.compliance.findings import build_finding
from portfolio_os.constraints.base import compute_post_trade_quantities, gross_traded_notional
from portfolio_os.constraints.regulatory import manager_aggregate_ratio
from portfolio_os.constraints.risk import industry_breaches, single_name_breaches
from portfolio_os.constraints.trading import blocked_trade_reason_details, exceeds_participation_limit
from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.data.portfolio import build_portfolio_data_quality_findings
from portfolio_os.data.reference import build_reference_data_quality_findings
from portfolio_os.domain.enums import FindingCategory, FindingSeverity, OrderSide, RepairStatus
from portfolio_os.domain.models import ComplianceFinding, Order
from portfolio_os.utils.config import AppConfig


def _severity_from_policy(value: str) -> FindingSeverity:
    """Convert a template severity string into the enum."""

    return FindingSeverity(str(value).upper())


def collect_data_quality_findings(
    universe: pd.DataFrame,
    config: AppConfig,
) -> list[ComplianceFinding]:
    """Build non-blocking data-quality findings from validated inputs."""

    findings: list[ComplianceFinding] = []
    findings.extend(build_portfolio_data_quality_findings(universe, config))
    findings.extend(build_reference_data_quality_findings(universe))
    tradable_mask = universe["tradable"].astype(bool) & ~universe["upper_limit_hit"].astype(bool) & ~universe["lower_limit_hit"].astype(bool)
    if int(tradable_mask.sum()) == 0:
        findings.append(
            build_finding(
                "no_tradable_securities_in_snapshot",
                FindingCategory.DATA_QUALITY,
                FindingSeverity.WARNING,
                "No securities are currently tradable in the provided snapshot, so optimization outputs will be highly constrained.",
                rule_source="market.csv",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={"security_count": int(len(universe))},
            )
        )
    return findings


def run_pretrade_checks(
    universe: pd.DataFrame,
    orders: list[Order],
    config: AppConfig,
    *,
    pre_trade_nav: float,
    extra_findings: list[ComplianceFinding] | None = None,
) -> list[ComplianceFinding]:
    """Evaluate repaired orders against post-trade mandate checks."""

    findings: list[ComplianceFinding] = list(extra_findings or [])
    quantity_map = {order.ticker: order.quantity if order.side == OrderSide.BUY else -order.quantity for order in orders}
    signed_quantities = np.array([float(quantity_map.get(ticker, 0.0)) for ticker in universe["ticker"]], dtype=float)
    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    post_trade_quantities = compute_post_trade_quantities(universe, signed_quantities)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(signed_quantities, prices, adv_shares, config.slippage)
    cash_after = (
        config.portfolio_state.available_cash
        - float(np.sum(prices * signed_quantities))
        - float(np.sum(fees))
        - float(np.sum(slippage))
    )
    if cash_after < config.portfolio_state.min_cash_buffer - 1e-6:
        findings.append(
            build_finding(
                "cash_buffer",
                FindingCategory.CASH,
                FindingSeverity.BREACH,
                "Post-trade cash falls below the configured minimum cash buffer.",
                rule_source="constraints.cash_non_negative",
                blocking=True,
                repair_status=RepairStatus.UNRESOLVED,
                details={
                    "cash_after": cash_after,
                    "min_cash_buffer": config.portfolio_state.min_cash_buffer,
                    "reason_label": "insufficient_cash_after_repair",
                },
            )
        )

    turnover = gross_traded_notional(signed_quantities, prices) / pre_trade_nav if pre_trade_nav else 0.0
    if turnover > config.constraints.max_turnover + 1e-9:
        findings.append(
            build_finding(
                "turnover_limit",
                FindingCategory.RISK,
                FindingSeverity.BREACH,
                "Post-trade basket exceeds the configured turnover limit.",
                rule_source="constraints.max_turnover",
                blocking=True,
                repair_status=RepairStatus.UNRESOLVED,
                details={
                    "turnover": turnover,
                    "max_turnover": config.constraints.max_turnover,
                    "reason_label": "turnover_limit_binding",
                },
            )
        )

    limit = config.effective_single_name_limit
    universe_by_ticker = universe.set_index("ticker")
    for ticker, weight in single_name_breaches(universe, post_trade_quantities, pre_trade_nav, limit).items():
        row = universe_by_ticker.loc[ticker]
        sell_blocked_details = blocked_trade_reason_details(
            pd.Series(row),
            -1.0,
            market=config.trading.normalized_market,
        )
        is_untradeable = sell_blocked_details is not None
        disposition = "blocked_untradeable" if is_untradeable else "blocking"
        untradeable_reason = sell_blocked_details[0] if sell_blocked_details is not None else None
        findings.append(
            build_finding(
                "single_name_limit",
                FindingCategory.RISK,
                _severity_from_policy(config.constraints.severity_policy.unresolved_risk),
                "Post-trade single-name weight breaches the effective cap.",
                ticker=ticker,
                rule_source="constraints.single_name_max_weight",
                blocking=not is_untradeable,
                repair_status=RepairStatus.UNRESOLVED,
                details={
                    "weight": weight,
                    "effective_limit": limit,
                    "breach_amount": max(0.0, float(weight - limit)),
                    "disposition": disposition,
                    "untradeable_reason": untradeable_reason,
                    "recovery_condition": (
                        "Re-evaluate on next tradable session or when restriction is lifted."
                        if is_untradeable
                        else "Reduce exposure via executable sell orders."
                    ),
                    "reason_label": "single_name_limit_breach",
                },
            )
        )

    for industry, details in industry_breaches(
        universe, post_trade_quantities, pre_trade_nav, config.constraints.industry_bounds
    ).items():
        findings.append(
            build_finding(
                "industry_bounds",
                FindingCategory.RISK,
                _severity_from_policy(config.constraints.severity_policy.unresolved_risk),
                "Post-trade industry exposure falls outside configured bounds.",
                ticker=None,
                rule_source="constraints.industry_bounds",
                blocking=True,
                repair_status=RepairStatus.UNRESOLVED,
                details={"industry": industry, "reason_label": "industry_bound_breach", **details},
            )
        )

    for order in orders:
        row = universe.loc[universe["ticker"] == order.ticker].iloc[0]
        if exceeds_participation_limit(
            order.quantity if order.side == OrderSide.BUY else -order.quantity,
            float(row["adv_shares"]),
            config.constraints.participation_limit,
        ):
            findings.append(
                build_finding(
                    "participation_limit",
                    FindingCategory.TRADABILITY,
                    FindingSeverity.BREACH,
                    "Order breaches the configured participation limit.",
                    ticker=order.ticker,
                    rule_source="constraints.participation_limit",
                    blocking=True,
                    repair_status=RepairStatus.UNRESOLVED,
                    details={
                        "quantity": order.quantity,
                        "adv_shares": float(row["adv_shares"]),
                        "reason_label": "participation_cap_binding",
                    },
                )
            )

    ordered_tickers = {order.ticker for order in orders}
    for _, row in universe.iterrows():
        ticker = str(row["ticker"])
        if ticker in ordered_tickers:
            continue
        current_weight = float(row["current_weight"])
        target_weight = float(row["target_weight"])
        desired_quantity_sign = 0.0
        if target_weight > current_weight + 1e-6:
            desired_quantity_sign = 1.0
        elif current_weight > target_weight + 1e-6:
            desired_quantity_sign = -1.0
        if desired_quantity_sign == 0.0:
            continue
        blocked_details = blocked_trade_reason_details(
            pd.Series(row),
            desired_quantity_sign,
            market=config.trading.normalized_market,
        )
        if blocked_details is None:
            continue
        blocked_reason_label, blocked_reason_message = blocked_details
        findings.append(
            build_finding(
                "no_order_due_to_constraint",
                FindingCategory.TRADABILITY,
                _severity_from_policy(config.constraints.severity_policy.blocked_trade),
                f"No active order was generated because {blocked_reason_message}.",
                ticker=ticker,
                rule_source="constraints.blocked_trade_policy",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={
                    "reason_label": blocked_reason_label,
                    "current_weight": current_weight,
                    "target_weight": target_weight,
                    "desired_side": "BUY" if desired_quantity_sign > 0 else "SELL",
                },
            )
        )

    double_ten_limit = config.constraints.double_ten.manager_aggregate_limit
    double_ten_enabled = bool(config.constraints.double_ten.enabled) and (not config.trading.is_us_market)
    if double_ten_enabled and double_ten_limit is not None:
        for row, post_trade_quantity in zip(universe.to_dict(orient="records"), post_trade_quantities, strict=True):
            ratio = manager_aggregate_ratio(pd.Series(row), float(post_trade_quantity))
            if ratio is not None and ratio > double_ten_limit + 1e-9:
                findings.append(
                    build_finding(
                        "manager_aggregate_limit",
                        FindingCategory.REGULATORY,
                        _severity_from_policy(config.constraints.severity_policy.manager_aggregate),
                        "Manager aggregate ownership ratio breaches the configured warning threshold.",
                        ticker=str(row["ticker"]),
                        rule_source="constraints.double_ten.manager_aggregate_limit",
                        blocking=False,
                        repair_status=RepairStatus.UNRESOLVED,
                        details={
                            "ratio": ratio,
                            "limit": double_ten_limit,
                            "manager_aggregate_qty": float(row.get("manager_aggregate_qty", 0.0) or 0.0),
                            "post_trade_quantity": float(post_trade_quantity),
                            "issuer_total_shares": float(row.get("issuer_total_shares", 0.0) or 0.0),
                            "reason_label": "manager_aggregate_warning",
                        },
                    )
                )

    remediation_days = config.constraints.double_ten.remediation_days
    if double_ten_enabled and any(
        f.rule_code in {"single_name_limit", "manager_aggregate_limit"} for f in findings
    ):
        findings.append(
            build_finding(
                "double_ten_remediation",
                FindingCategory.REGULATORY,
                _severity_from_policy(config.constraints.severity_policy.remediation_note),
                "Double-ten related items require manual remediation tracking in the MVP.",
                rule_source="constraints.double_ten.remediation_days",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={"remediation_days": remediation_days, "reason_label": "remediation_note"},
            )
        )

    return findings
