"""Soft-constraint and penalty helpers."""

from __future__ import annotations

import cvxpy as cp
import numpy as np


def turnover_penalty_expression(trades: cp.Expression, prices: np.ndarray, pre_trade_nav: float) -> cp.Expression:
    """Return the normalized turnover penalty term."""

    gross_notional = cp.sum(cp.multiply(prices, cp.abs(trades)))
    return gross_notional / pre_trade_nav

