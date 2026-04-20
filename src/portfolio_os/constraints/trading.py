"""Trading-rule checks and helpers."""

from __future__ import annotations

import pandas as pd

from portfolio_os.cost.liquidity import participation_ratio


def is_effectively_tradable(row: pd.Series) -> bool:
    """Return whether a security can be traded in the current snapshot."""

    return bool(row["tradable"]) and not bool(row["upper_limit_hit"]) and not bool(row["lower_limit_hit"])


def blocked_trade_reason_details(
    row: pd.Series,
    quantity: float,
    *,
    market: str = "cn",
) -> tuple[str, str] | None:
    """Return a stable reason label and human-readable message for a blocked trade."""

    if quantity == 0:
        return None
    market_normalized = str(market).strip().lower()
    is_us_market = market_normalized in {"us", "usa", "us_equity", "us_stock"}
    if not bool(row["tradable"]):
        return ("suspended", "security is suspended or otherwise not tradable")
    if (not is_us_market) and bool(row["upper_limit_hit"]):
        return ("upper_limit_hit", "security is at the upper limit and cannot trade")
    if (not is_us_market) and bool(row["lower_limit_hit"]):
        return ("lower_limit_hit", "security is at the lower limit and cannot trade")
    if quantity > 0 and bool(row["blacklist_buy"]):
        return ("buy_blacklist", "security is in the buy blacklist")
    if quantity < 0 and bool(row["blacklist_sell"]):
        return ("sell_blacklist", "security is in the sell blacklist")
    return None


def blocked_trade_reason(row: pd.Series, quantity: float, *, market: str = "cn") -> str | None:
    """Explain why a proposed trade is blocked."""

    details = blocked_trade_reason_details(row, quantity, market=market)
    if details is None:
        return None
    return details[1]


def exceeds_participation_limit(quantity: float, adv_shares: float, limit: float) -> bool:
    """Check whether the trade quantity breaches ADV participation."""

    return participation_ratio(quantity, adv_shares) > limit + 1e-12
