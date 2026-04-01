from __future__ import annotations

import numpy as np

from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.domain.models import TradeInstruction
from portfolio_os.optimizer.repair import repair_instructions


def test_repair_makes_board_lots_and_preserves_cash(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"]
    pre_trade_nav = float(config.portfolio_state.available_cash + universe["current_notional"].sum())
    instructions = [
        TradeInstruction(ticker="601318", quantity=-2500.0, estimated_price=50.2, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="300750", quantity=5555.0, estimated_price=219.0, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="000333", quantity=33333.0, estimated_price=59.8, current_weight=0.0, target_weight=0.0),
    ]
    subset = universe.loc[universe["ticker"].isin(["601318", "300750", "000333"])].reset_index(drop=True)
    repaired, _ = repair_instructions(instructions, subset, config, pre_trade_nav=pre_trade_nav)

    quantity_map = {instruction.ticker: instruction.quantity for instruction in repaired}
    signed_quantities = np.array([float(quantity_map.get(ticker, 0.0)) for ticker in subset["ticker"]], dtype=float)
    prices = subset["estimated_price"].to_numpy(dtype=float)
    adv = subset["adv_shares"].to_numpy(dtype=float)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(signed_quantities, prices, adv, config.slippage)
    cash_after = float(config.portfolio_state.available_cash - np.sum(prices * signed_quantities) - np.sum(fees) - np.sum(slippage))
    post_trade_quantities = subset["quantity"].to_numpy(dtype=float) + signed_quantities

    assert all(int(abs(quantity)) % 100 == 0 for quantity in signed_quantities if quantity != 0)
    assert np.all(post_trade_quantities >= 0)
    assert cash_after >= config.portfolio_state.min_cash_buffer - 1e-6
    assert all(abs(quantity) * price >= config.constraints.min_order_notional for quantity, price in zip(signed_quantities, prices, strict=True) if quantity != 0)


def test_repair_reduces_buys_until_cash_is_restored(sample_context: dict) -> None:
    config = sample_context["config"].model_copy(deep=True)
    config.portfolio_state.available_cash = 80000.0
    config.portfolio_state.min_cash_buffer = 20000.0
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["300750", "000333"])
    ].reset_index(drop=True)
    pre_trade_nav = float(
        config.portfolio_state.available_cash
        + float(subset.iloc[0]["estimated_price"]) * float(subset.iloc[0]["quantity"])
    )
    instructions = [
        TradeInstruction(ticker="300750", quantity=800.0, estimated_price=219.0, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="000333", quantity=1200.0, estimated_price=59.8, current_weight=0.0, target_weight=0.0),
    ]
    repaired, findings = repair_instructions(instructions, subset, config, pre_trade_nav=pre_trade_nav)

    quantity_map = {instruction.ticker: instruction.quantity for instruction in repaired}
    signed_quantities = np.array([float(quantity_map.get(ticker, 0.0)) for ticker in subset["ticker"]], dtype=float)
    prices = subset["estimated_price"].to_numpy(dtype=float)
    adv = subset["adv_shares"].to_numpy(dtype=float)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(signed_quantities, prices, adv, config.slippage)
    cash_after = float(config.portfolio_state.available_cash - np.sum(prices * signed_quantities) - np.sum(fees) - np.sum(slippage))

    assert any(finding.rule_code == "cash_buy_reduced" for finding in findings)
    assert cash_after >= config.portfolio_state.min_cash_buffer - 1e-6
    assert sum(abs(instruction.quantity) for instruction in repaired) < 2000.0


def test_participation_clip_remains_valid_after_board_lot_rounding(sample_context: dict) -> None:
    config = sample_context["config"].model_copy(deep=True)
    config.constraints.participation_limit = 0.000025
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["300750"])
    ].reset_index(drop=True)
    pre_trade_nav = float(config.portfolio_state.available_cash + subset["current_notional"].sum())
    instructions = [
        TradeInstruction(ticker="300750", quantity=1000.0, estimated_price=219.0, current_weight=0.0, target_weight=0.0),
    ]
    repaired, findings = repair_instructions(instructions, subset, config, pre_trade_nav=pre_trade_nav)

    repaired_quantity = repaired[0].quantity if repaired else 0.0
    assert repaired_quantity == 0.0
    assert any(finding.rule_code == "participation_clipped" for finding in findings)


def test_small_order_removal_changes_order_count_and_blocked_stats(sample_context: dict) -> None:
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["601012", "300750"])
    ].reset_index(drop=True)
    pre_trade_nav = float(sample_context["config"].portfolio_state.available_cash + subset["current_notional"].sum())
    instructions = [
        TradeInstruction(ticker="601012", quantity=1000.0, estimated_price=25.1, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="300750", quantity=100.0, estimated_price=219.0, current_weight=0.0, target_weight=0.0),
    ]
    repaired, findings = repair_instructions(instructions, subset, sample_context["config"], pre_trade_nav=pre_trade_nav)

    assert len(repaired) == 1
    assert repaired[0].ticker == "300750"
    assert sum(1 for finding in findings if finding.rule_code == "trade_blocked") == 1


def test_single_name_guardrail_clip_reduces_over_limit_position(sample_context: dict) -> None:
    config = sample_context["config"].model_copy(deep=True)
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["600519"])
    ].reset_index(drop=True)
    subset.loc[:, "quantity"] = 250.0
    subset.loc[:, "tradable"] = True
    subset.loc[:, "upper_limit_hit"] = False
    subset.loc[:, "lower_limit_hit"] = False
    subset.loc[:, "blacklist_sell"] = False
    subset.loc[:, "adv_shares"] = 1_000_000.0
    pre_trade_nav = float(config.portfolio_state.available_cash + subset["current_notional"].sum())
    instruction = TradeInstruction(
        ticker="600519",
        quantity=0.0,
        estimated_price=1675.0,
        current_weight=float(subset.iloc[0]["current_weight"]),
        target_weight=float(subset.iloc[0]["target_weight"]),
    )

    repaired, findings = repair_instructions([instruction], subset, config, pre_trade_nav=pre_trade_nav)

    quantity_map = {item.ticker: item.quantity for item in repaired}
    post_quantity = float(subset.iloc[0]["quantity"]) + float(quantity_map.get("600519", 0.0))
    post_weight = float(subset.iloc[0]["estimated_price"]) * post_quantity / pre_trade_nav

    assert post_weight <= config.effective_single_name_limit + 1e-6
    assert any(item.rule_code == "single_name_guardrail_clip" for item in findings)
