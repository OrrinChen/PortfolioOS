"""Liquidity helpers."""

from __future__ import annotations


def participation_ratio(quantity: float, adv_shares: float) -> float:
    """Return order size divided by ADV shares."""

    if adv_shares <= 0:
        return 0.0
    return abs(quantity) / adv_shares


def within_participation_limit(quantity: float, adv_shares: float, limit: float) -> bool:
    """Check whether an order stays within the configured participation limit."""

    return participation_ratio(quantity, adv_shares) <= limit + 1e-12

