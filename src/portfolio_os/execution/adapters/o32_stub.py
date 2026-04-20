"""O32-style export preview helpers."""

from __future__ import annotations

from portfolio_os.domain.models import Basket, ComplianceFinding
from portfolio_os.execution.adapters.csv_export import build_oms_frame
from portfolio_os.utils.config import AppConfig


def build_o32_preview(
    *,
    basket: Basket,
    findings: list[ComplianceFinding],
    config: AppConfig,
    basket_id: str,
) -> list[dict[str, object]]:
    """Build a lightweight O32 mapping preview for future adapter work.

    The MVP does not connect to O32. This preview shows how the OMS-style
    export fields would be mapped into a likely O32 import payload.
    """

    oms_frame = build_oms_frame(
        basket=basket,
        findings=findings,
        config=config,
        basket_id=basket_id,
    )
    preview: list[dict[str, object]] = []
    for row in oms_frame.to_dict(orient="records"):
        preview.append(
            {
                "fund_account": row["account_id"],
                "stock_code": row["ticker"],
                "entrust_bs": row["side"],
                "entrust_amount": row["quantity"],
                "price_mode": row["price_type"],
                "entrust_price": row["limit_price"] or row["estimated_price"],
                "basket_no": row["basket_id"],
                "strategy_name": row["strategy_tag"],
                "memo": row["reason"],
                "risk_checks_passed": row["blocking_checks_cleared"],
            }
        )
    return preview


def export_to_o32_stub() -> str:
    """Return a stub adapter status message."""

    return (
        "O32 live connectivity is not implemented. Use build_o32_preview() to inspect "
        "the future field mapping from PortfolioOS OMS-ready orders."
    )
