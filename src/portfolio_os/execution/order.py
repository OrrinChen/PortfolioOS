"""Order construction helpers."""

from __future__ import annotations

import pandas as pd

from portfolio_os.cost.fee import estimate_fee
from portfolio_os.cost.slippage import estimate_slippage
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.models import Order, TradeInstruction
from portfolio_os.explain.trade_reason import build_trade_reason
from portfolio_os.utils.config import AppConfig


def build_orders(
    instructions: list[TradeInstruction],
    universe: pd.DataFrame,
    config: AppConfig,
) -> list[Order]:
    """Convert repaired instructions into exportable orders."""

    orders: list[Order] = []
    for instruction in instructions:
        if instruction.quantity == 0:
            continue
        row = universe.loc[universe["ticker"] == instruction.ticker].iloc[0]
        side = OrderSide.BUY if instruction.quantity > 0 else OrderSide.SELL
        quantity = int(abs(instruction.quantity))
        estimated_price = float(row["estimated_price"])
        fee = estimate_fee(instruction.quantity, estimated_price, config.fees)
        slippage = estimate_slippage(
            instruction.quantity,
            estimated_price,
            float(row["adv_shares"]),
            config.slippage,
        )
        reason = build_trade_reason(
            row=row,
            signed_quantity=instruction.quantity,
            estimated_fee=fee,
            estimated_slippage=slippage,
            effective_single_name_limit=config.effective_single_name_limit,
        )
        orders.append(
            Order(
                ticker=instruction.ticker,
                side=side,
                quantity=quantity,
                estimated_price=estimated_price,
                estimated_notional=float(quantity * estimated_price),
                estimated_fee=fee,
                estimated_slippage=slippage,
                urgency=config.execution.urgency,
                reason=reason,
            )
        )
    return orders

