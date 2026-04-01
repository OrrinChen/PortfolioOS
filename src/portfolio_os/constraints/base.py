"""Common constraint helpers."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from portfolio_os.utils.config import AppConfig


def effective_single_name_limit(config: AppConfig) -> float:
    """Return the strictest single-name limit."""

    return config.effective_single_name_limit


def gross_traded_notional(quantities: np.ndarray, prices: np.ndarray) -> float:
    """Compute the project-wide turnover numerator."""

    return float(np.sum(np.abs(quantities) * prices))


def compute_post_trade_quantities(frame: pd.DataFrame, signed_quantities: Iterable[float]) -> np.ndarray:
    """Apply trade deltas to the current portfolio."""

    current_quantities = frame["quantity"].to_numpy(dtype=float)
    return current_quantities + np.asarray(list(signed_quantities), dtype=float)


def compute_weights_from_quantities(
    quantities: np.ndarray,
    prices: np.ndarray,
    nav: float,
) -> np.ndarray:
    """Convert share quantities into portfolio weights."""

    if nav <= 0:
        return np.zeros_like(quantities, dtype=float)
    return quantities * prices / nav


def target_deviation_value(
    quantities: np.ndarray,
    prices: np.ndarray,
    target_weights: np.ndarray,
    nav: float,
) -> float:
    """Compute the squared target-deviation metric used by the optimizer."""

    weights = compute_weights_from_quantities(quantities, prices, nav)
    return float(np.sum(np.square(weights - target_weights)))


def compute_industry_exposures(
    frame: pd.DataFrame,
    post_trade_quantities: np.ndarray,
    nav: float,
) -> dict[str, float]:
    """Compute post-trade industry exposures."""

    prices = frame["estimated_price"].to_numpy(dtype=float)
    exposures: dict[str, float] = {}
    for industry in sorted(frame["industry"].unique()):
        mask = frame["industry"] == industry
        exposures[industry] = float(np.sum(post_trade_quantities[mask] * prices[mask]) / nav) if nav else 0.0
    return exposures
