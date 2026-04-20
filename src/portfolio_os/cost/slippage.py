"""Simple slippage estimators."""

from __future__ import annotations

import cvxpy as cp
import numpy as np

from portfolio_os.utils.config import SlippageConfig


def estimate_slippage(
    quantity: float,
    price: float,
    adv_shares: float,
    slippage_config: SlippageConfig,
) -> float:
    """Estimate convex power-law slippage for one signed order."""

    absolute_quantity = abs(quantity)
    if absolute_quantity <= 0 or adv_shares <= 0:
        return 0.0
    return float(
        price
        * slippage_config.k
        * absolute_quantity
        * np.power(absolute_quantity / adv_shares, slippage_config.alpha)
    )


def estimate_slippage_array(
    quantities: np.ndarray,
    prices: np.ndarray,
    adv_shares: np.ndarray,
    slippage_config: SlippageConfig,
) -> np.ndarray:
    """Vectorized power-law slippage estimation."""

    absolute_quantities = np.abs(quantities)
    safe_adv = np.maximum(adv_shares, 1.0)
    return (
        prices
        * slippage_config.k
        * absolute_quantities
        * np.power(absolute_quantities / safe_adv, slippage_config.alpha)
    )


def slippage_expression(
    quantities: cp.Expression,
    prices: np.ndarray,
    adv_shares: np.ndarray,
    slippage_config: SlippageConfig,
) -> cp.Expression:
    """Convex slippage expression for the optimizer."""

    safe_adv = np.maximum(adv_shares, 1.0)
    coefficient = prices * slippage_config.k / np.power(safe_adv, slippage_config.alpha)
    return cp.sum(cp.multiply(coefficient, cp.power(cp.abs(quantities), 1.0 + slippage_config.alpha)))
