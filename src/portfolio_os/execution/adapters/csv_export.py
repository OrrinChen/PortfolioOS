"""CSV order exporters for analysis and OMS-style workflows."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.compliance.posttrade import build_order_release_status
from portfolio_os.domain.models import Basket, ComplianceFinding
from portfolio_os.execution.basket import basket_to_frame
from portfolio_os.utils.config import AppConfig


def export_basket_csv(basket: Basket, path: str | Path) -> None:
    """Write the analysis-oriented order basket to CSV."""

    frame = basket_to_frame(basket)
    frame.to_csv(Path(path), index=False)


def build_oms_frame(
    *,
    basket: Basket,
    findings: list[ComplianceFinding],
    config: AppConfig,
    basket_id: str,
) -> pd.DataFrame:
    """Build an OMS-friendly order export frame."""

    release_status = build_order_release_status(basket.orders, findings)
    strategy_tag = config.constraints.report_labels.strategy_tag
    rows = []
    for order in basket.orders:
        rows.append(
            {
                "account_id": config.portfolio_state.account_id,
                "ticker": order.ticker,
                "side": order.side.value,
                "quantity": order.quantity,
                "price_type": "VWAP_REF",
                "limit_price": "",
                "estimated_price": order.estimated_price,
                "estimated_notional": order.estimated_notional,
                "urgency": order.urgency,
                "strategy_tag": strategy_tag,
                "basket_id": basket_id,
                "reason": order.reason,
                "blocking_checks_cleared": bool(release_status.get(order.ticker, True)),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "account_id",
            "ticker",
            "side",
            "quantity",
            "price_type",
            "limit_price",
            "estimated_price",
            "estimated_notional",
            "urgency",
            "strategy_tag",
            "basket_id",
            "reason",
            "blocking_checks_cleared",
        ],
    )


def export_basket_oms_csv(
    *,
    basket: Basket,
    findings: list[ComplianceFinding],
    config: AppConfig,
    basket_id: str,
    path: str | Path,
) -> None:
    """Write the OMS-style order basket to CSV."""

    frame = build_oms_frame(
        basket=basket,
        findings=findings,
        config=config,
        basket_id=basket_id,
    )
    frame.to_csv(Path(path), index=False)
