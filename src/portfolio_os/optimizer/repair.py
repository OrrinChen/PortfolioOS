"""Repair continuous optimizer output into executable board-lot orders."""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio_os.compliance.findings import build_finding
from portfolio_os.constraints.trading import blocked_trade_reason_details
from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.domain.enums import FindingCategory, FindingSeverity, RepairStatus
from portfolio_os.domain.models import ComplianceFinding, TradeInstruction
from portfolio_os.utils.config import AppConfig

NEAR_ZERO_TRADE_TOLERANCE = 1e-3


def _round_toward_zero(quantity: float, lot_size: int) -> int:
    """Round a quantity toward zero to the nearest board-lot multiple."""

    return int(quantity / lot_size) * lot_size


def _cash_after_trades(
    universe: pd.DataFrame,
    signed_quantities: np.ndarray,
    config: AppConfig,
) -> float:
    """Compute post-trade cash using the canonical cost formulas."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(signed_quantities, prices, adv_shares, config.slippage)
    return float(
        config.portfolio_state.available_cash
        - np.sum(prices * signed_quantities)
        - np.sum(fees)
        - np.sum(slippage)
    )


def _deviation_improvement(row: pd.Series, quantity: float, pre_trade_nav: float) -> float:
    """Estimate how much a trade improves target deviation."""

    current_weight = float(row["current_weight"])
    target_weight = float(row["target_weight"])
    delta_weight = float(row["estimated_price"]) * quantity / pre_trade_nav
    before = abs(target_weight - current_weight)
    after = abs(target_weight - (current_weight + delta_weight))
    return before - after


def _round_away_from_zero_shares(quantity: float) -> int:
    """Round a signed share count away from zero to the nearest integer."""

    if quantity >= 0:
        return int(np.ceil(quantity))
    return int(np.floor(quantity))


def _can_sell_for_compliance(row: pd.Series, *, market: str) -> tuple[bool, str | None]:
    """Return whether a ticker can be sold in the current snapshot."""

    blocked = blocked_trade_reason_details(row, -1.0, market=market)
    if blocked is None:
        return True, None
    return False, blocked[0]


def repair_instructions(
    instructions: list[TradeInstruction],
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    pre_trade_nav: float,
) -> tuple[list[TradeInstruction], list[ComplianceFinding]]:
    """Repair optimizer output into board-lot orders that pass cash checks."""

    findings: list[ComplianceFinding] = []
    if not instructions:
        return [], findings

    lot_size = 1 if config.trading.is_us_market else int(config.trading.lot_size)
    instruction_map = {instruction.ticker: float(instruction.quantity) for instruction in instructions}
    quantities = np.array(
        [instruction_map.get(str(ticker), 0.0) for ticker in universe["ticker"]],
        dtype=float,
    )
    quantities[np.abs(quantities) < NEAR_ZERO_TRADE_TOLERANCE] = 0.0
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    prices = universe["estimated_price"].to_numpy(dtype=float)
    participation_limit = config.constraints.participation_limit

    for idx, row in universe.iterrows():
        if abs(quantities[idx]) < NEAR_ZERO_TRADE_TOLERANCE:
            quantities[idx] = 0.0
            continue
        blocked_reason_details = blocked_trade_reason_details(
            row,
            quantities[idx],
            market=config.trading.normalized_market,
        )
        if blocked_reason_details is not None:
            blocked_reason_label, blocked_reason_message = blocked_reason_details
            findings.append(
                build_finding(
                    "trade_blocked",
                    FindingCategory.TRADABILITY,
                    FindingSeverity(str(config.constraints.severity_policy.blocked_trade).upper()),
                    f"Desired trade was removed because {blocked_reason_message}.",
                    ticker=str(row["ticker"]),
                    rule_source="constraints.blocked_trade_policy",
                    blocking=bool(config.constraints.blocked_trade_policy.treat_as_blocking),
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "requested_quantity": float(quantities[idx]),
                        "reason_label": blocked_reason_label,
                        "repair_action": "removed_blocked_trade",
                    },
                )
            )
            quantities[idx] = 0.0

    for idx, row in universe.iterrows():
        min_quantity = -current_quantities[idx]
        if quantities[idx] < min_quantity:
            findings.append(
                build_finding(
                    "sell_clipped",
                    FindingCategory.RISK,
                    FindingSeverity.WARNING,
                    "Sell quantity exceeded current holdings and was clipped.",
                    ticker=str(row["ticker"]),
                    rule_source="constraints.no_negative_holdings",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "requested_quantity": float(quantities[idx]),
                        "max_sell": float(-min_quantity),
                        "reason_label": "position_availability",
                        "repair_action": "clipped_quantity",
                    },
                )
            )
            quantities[idx] = min_quantity

        max_participation = participation_limit * float(row["adv_shares"])
        if max_participation < lot_size:
            max_participation = 0.0
        if quantities[idx] > max_participation:
            findings.append(
                build_finding(
                    "participation_clipped",
                    FindingCategory.TRADABILITY,
                    FindingSeverity.WARNING,
                    "Buy quantity exceeded the participation limit and was clipped.",
                    ticker=str(row["ticker"]),
                    rule_source="constraints.participation_limit",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "requested_quantity": float(quantities[idx]),
                        "max_quantity": max_participation,
                        "reason_label": "participation_cap_binding",
                        "repair_action": "clipped_quantity",
                    },
                )
            )
            quantities[idx] = max_participation
        if quantities[idx] < -max_participation:
            findings.append(
                build_finding(
                    "participation_clipped",
                    FindingCategory.TRADABILITY,
                    FindingSeverity.WARNING,
                    "Sell quantity exceeded the participation limit and was clipped.",
                    ticker=str(row["ticker"]),
                    rule_source="constraints.participation_limit",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "requested_quantity": float(quantities[idx]),
                        "max_quantity": max_participation,
                        "reason_label": "participation_cap_binding",
                        "repair_action": "clipped_quantity",
                    },
                )
            )
            quantities[idx] = -max_participation

    rounded = np.array([_round_toward_zero(quantity, lot_size) for quantity in quantities], dtype=float)
    for idx, row in universe.iterrows():
        max_participation = participation_limit * float(row["adv_shares"])
        board_lot_cap = float(np.floor(max_participation / lot_size) * lot_size) if max_participation >= lot_size else 0.0
        if rounded[idx] > board_lot_cap:
            rounded[idx] = board_lot_cap
        if rounded[idx] < -board_lot_cap:
            rounded[idx] = -board_lot_cap
    for idx, (original, repaired) in enumerate(zip(quantities, rounded, strict=True)):
        if abs(original - repaired) > 1e-9:
            findings.append(
                build_finding(
                    "lot_rounding",
                    FindingCategory.TRADABILITY,
                    FindingSeverity.INFO,
                    "Trade was rounded toward zero to the board-lot requirement.",
                    ticker=str(universe.iloc[idx]["ticker"]),
                    rule_source="trading.lot_size",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "requested_quantity": float(original),
                        "rounded_quantity": float(repaired),
                        "reason_label": "rounded_to_board_lot",
                        "repair_action": "rounded_quantity",
                    },
                )
            )
    quantities = rounded

    for idx, row in universe.iterrows():
        notional = abs(quantities[idx]) * float(row["estimated_price"])
        if 0 < notional < config.constraints.min_order_notional:
            findings.append(
                build_finding(
                    "small_order_removed",
                    FindingCategory.TRADABILITY,
                    FindingSeverity.INFO,
                    "Residual order was removed because it fell below the minimum notional threshold.",
                    ticker=str(row["ticker"]),
                    rule_source="constraints.min_order_notional",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "quantity": float(quantities[idx]),
                        "notional": notional,
                        "reason_label": "dust_order_below_min_notional",
                        "repair_action": "removed_dust_order",
                    },
                )
            )
            quantities[idx] = 0.0

    buy_indices = [idx for idx, quantity in enumerate(quantities) if quantity > 0]
    cost_per_notional = np.zeros(len(quantities), dtype=float)
    if buy_indices:
        fees = estimate_fee_array(quantities, prices, config.fees)
        slippage = estimate_slippage_array(
            quantities,
            prices,
            universe["adv_shares"].to_numpy(dtype=float),
            config.slippage,
        )
        notionals = np.abs(quantities) * prices
        cost_per_notional = (fees + slippage) / np.maximum(notionals, 1.0)

    buy_indices.sort(
        key=lambda idx: (
            _deviation_improvement(universe.iloc[idx], quantities[idx], pre_trade_nav),
            -cost_per_notional[idx],
        )
    )

    cash_after = _cash_after_trades(universe, quantities, config)
    while cash_after < config.portfolio_state.min_cash_buffer - 1e-6 and any(quantities[idx] > 0 for idx in buy_indices):
        changed = False
        for idx in buy_indices:
            if quantities[idx] < lot_size:
                continue
            previous_quantity = float(quantities[idx])
            quantities[idx] -= lot_size
            changed = True
            notional = abs(quantities[idx]) * prices[idx]
            if 0 < notional < config.constraints.min_order_notional:
                quantities[idx] = 0.0
            findings.append(
                build_finding(
                    "cash_buy_reduced",
                    FindingCategory.CASH,
                    FindingSeverity.INFO,
                    "Buy order was reduced during cash repair to restore the minimum cash buffer.",
                    ticker=str(universe.iloc[idx]["ticker"]),
                    rule_source="constraints.cash_non_negative",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "previous_quantity": previous_quantity,
                        "new_quantity": float(quantities[idx]),
                        "reason_label": "insufficient_cash_after_repair",
                        "repair_action": "reduced_buy_order",
                    },
                )
            )
            cash_after = _cash_after_trades(universe, quantities, config)
            if cash_after >= config.portfolio_state.min_cash_buffer - 1e-6:
                break
        if not changed:
            break

    if cash_after < config.portfolio_state.min_cash_buffer - 1e-6:
        findings.append(
            build_finding(
                "cash_repair_failed",
                FindingCategory.CASH,
                FindingSeverity.BREACH,
                "Cash repair could not fully restore the minimum cash buffer.",
                rule_source="constraints.cash_non_negative",
                blocking=True,
                repair_status=RepairStatus.UNRESOLVED,
                details={
                    "cash_after": cash_after,
                    "min_cash_buffer": config.portfolio_state.min_cash_buffer,
                    "reason_label": "insufficient_cash_after_repair",
                    "repair_action": "cash_repair_failed",
                },
            )
        )

    # Final single-name safeguard after lot/cash repairs. This prioritizes
    # mandate compliance on tradable names and leaves explicit traces.
    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    post_trade_quantities = current_quantities + quantities
    limit = float(config.effective_single_name_limit)
    for idx, row in universe.iterrows():
        if pre_trade_nav <= 0:
            continue
        ticker = str(row["ticker"])
        post_weight = float(prices[idx] * post_trade_quantities[idx] / pre_trade_nav)
        if post_weight <= limit + 1e-9:
            continue
        can_sell, blocked_reason_label = _can_sell_for_compliance(
            pd.Series(row),
            market=config.trading.normalized_market,
        )
        if not can_sell:
            findings.append(
                build_finding(
                    "single_name_guardrail_not_actionable",
                    FindingCategory.RISK,
                    FindingSeverity.INFO,
                    "Post-repair single-name breach could not be auto-reduced because the ticker is not sellable in this snapshot.",
                    ticker=ticker,
                    rule_source="constraints.single_name_max_weight",
                    blocking=False,
                    repair_status=RepairStatus.NOT_NEEDED,
                    details={
                        "weight_before_clip": post_weight,
                        "effective_limit": limit,
                        "breach_amount": max(0.0, post_weight - limit),
                        "reason_label": blocked_reason_label or "sell_not_actionable",
                        "repair_action": "none_not_sellable",
                    },
                )
            )
            continue

        required_shares = (post_weight - limit) * pre_trade_nav / float(prices[idx])
        required_lot_shares = float(np.ceil(required_shares / float(lot_size)) * lot_size)
        if required_lot_shares <= 0:
            continue
        max_participation = float(participation_limit * float(row["adv_shares"]))
        max_position_sell = float(post_trade_quantities[idx])
        max_sell_allowed = min(max_position_sell, max_participation)
        max_sell_lot = float(np.floor(max_sell_allowed / float(lot_size)) * lot_size)
        sell_lot_shares = min(required_lot_shares, max_sell_lot)
        if sell_lot_shares > 0:
            quantities[idx] -= sell_lot_shares
            findings.append(
                build_finding(
                    "single_name_guardrail_clip",
                    FindingCategory.RISK,
                    FindingSeverity.INFO,
                    "Sell quantity was increased to reduce single-name limit breach after lot/cash repairs.",
                    ticker=ticker,
                    rule_source="constraints.single_name_max_weight",
                    blocking=False,
                    repair_status=RepairStatus.REPAIRED,
                    details={
                        "extra_sell_quantity": sell_lot_shares,
                        "effective_limit": limit,
                        "reason_label": "single_name_limit_guardrail_post_repair",
                        "repair_action": "increased_sell_order",
                    },
                )
            )
            post_trade_quantities[idx] = current_quantities[idx] + quantities[idx]

        post_weight = float(prices[idx] * post_trade_quantities[idx] / pre_trade_nav)
        if post_weight <= limit + 1e-9:
            continue

        # Odd-lot clean-out: if a residual odd lot is left, allow a one-shot
        # odd-lot sell to reduce residual compliance friction.
        residual_position = float(post_trade_quantities[idx])
        odd_lot_remainder = residual_position % float(lot_size)
        if odd_lot_remainder > 1e-9:
            remaining_shares_needed = (post_weight - limit) * pre_trade_nav / float(prices[idx])
            odd_sell = min(float(_round_away_from_zero_shares(remaining_shares_needed)), odd_lot_remainder)
            odd_sell = max(0.0, odd_sell)
            if odd_sell > 0:
                quantities[idx] -= odd_sell
                post_trade_quantities[idx] = current_quantities[idx] + quantities[idx]
                findings.append(
                    build_finding(
                        "odd_lot_clear_for_compliance",
                        FindingCategory.RISK,
                        FindingSeverity.INFO,
                        "An odd-lot sell was applied to reduce residual single-name breach.",
                        ticker=ticker,
                        rule_source="constraints.single_name_max_weight",
                        blocking=False,
                        repair_status=RepairStatus.REPAIRED,
                        details={
                            "odd_lot_sell_quantity": odd_sell,
                            "effective_limit": limit,
                            "reason_label": "odd_lot_compliance_clear",
                            "repair_action": "odd_lot_sell_for_limit",
                        },
                    )
                )

    repaired_instructions = [
        TradeInstruction(
            ticker=str(ticker),
            quantity=float(quantity),
            estimated_price=float(price),
            current_weight=float(current_weight),
            target_weight=float(target_weight),
            reason_tags=["repaired"],
        )
        for ticker, quantity, price, current_weight, target_weight in zip(
            universe["ticker"],
            quantities,
            prices,
            universe["current_weight"],
            universe["target_weight"],
            strict=True,
        )
        if abs(quantity) > 0
    ]
    return repaired_instructions, findings
