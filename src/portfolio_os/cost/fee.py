"""Fee estimation functions shared across optimizer, repair, and reporting."""

from __future__ import annotations

import cvxpy as cp
import numpy as np

from portfolio_os.utils.config import FeeConfig


def estimate_fee(quantity: float, price: float, fee_config: FeeConfig) -> float:
    """Estimate fees for a signed share quantity."""

    notional = abs(quantity) * price
    sell_notional = max(-quantity, 0.0) * price
    commission = fee_config.commission_rate * notional
    transfer_fee = fee_config.transfer_fee_rate * notional
    stamp_duty = fee_config.stamp_duty_rate * sell_notional
    return float(commission + transfer_fee + stamp_duty)


def estimate_fee_array(
    quantities: np.ndarray,
    prices: np.ndarray,
    fee_config: FeeConfig,
) -> np.ndarray:
    """Vectorized fee estimation."""

    absolute_notionals = np.abs(quantities) * prices
    sell_notionals = np.maximum(-quantities, 0.0) * prices
    return (
        fee_config.commission_rate * absolute_notionals
        + fee_config.transfer_fee_rate * absolute_notionals
        + fee_config.stamp_duty_rate * sell_notionals
    )


def fee_expression(
    quantities: cp.Expression,
    prices: np.ndarray,
    fee_config: FeeConfig,
) -> cp.Expression:
    """Convex fee expression for the optimizer."""

    absolute_notionals = cp.multiply(prices, cp.abs(quantities))
    sell_notionals = cp.multiply(prices, cp.pos(-quantities))
    return (
        fee_config.commission_rate * cp.sum(absolute_notionals)
        + fee_config.transfer_fee_rate * cp.sum(absolute_notionals)
        + fee_config.stamp_duty_rate * cp.sum(sell_notionals)
    )

