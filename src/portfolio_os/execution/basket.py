"""Order basket aggregation."""

from __future__ import annotations

import pandas as pd

from portfolio_os.domain.models import Basket, Order


def build_basket(orders: list[Order]) -> Basket:
    """Aggregate orders into a basket."""

    gross_traded_notional = float(sum(order.estimated_notional for order in orders))
    total_fee = float(sum(order.estimated_fee for order in orders))
    total_slippage = float(sum(order.estimated_slippage for order in orders))
    return Basket(
        orders=orders,
        gross_traded_notional=gross_traded_notional,
        total_fee=total_fee,
        total_slippage=total_slippage,
        total_cost=total_fee + total_slippage,
    )


def basket_to_frame(basket: Basket) -> pd.DataFrame:
    """Convert an order basket into a DataFrame for CSV export."""

    columns = [
        "ticker",
        "side",
        "quantity",
        "estimated_price",
        "estimated_notional",
        "estimated_fee",
        "estimated_slippage",
        "urgency",
        "reason",
    ]
    frame = pd.DataFrame([order.model_dump(mode="json") for order in basket.orders])
    if frame.empty:
        return pd.DataFrame(columns=columns)
    return frame[columns]
