"""Regulatory helper checks."""

from __future__ import annotations

import pandas as pd


def manager_aggregate_ratio(row: pd.Series, post_trade_quantity: float) -> float | None:
    """Compute the simplified manager aggregate ownership ratio."""

    issuer_total_shares = float(row.get("issuer_total_shares", 0.0) or 0.0)
    if issuer_total_shares <= 0:
        return None
    manager_aggregate_qty = float(row.get("manager_aggregate_qty", 0.0) or 0.0)
    return (manager_aggregate_qty + post_trade_quantity) / issuer_total_shares

