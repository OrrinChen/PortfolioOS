"""Risk and exposure checks."""

from __future__ import annotations

import pandas as pd

from portfolio_os.constraints.base import compute_industry_exposures, compute_weights_from_quantities


def single_name_breaches(
    frame: pd.DataFrame,
    post_trade_quantities,
    nav: float,
    limit: float,
) -> dict[str, float]:
    """Return post-trade single-name weights that breach a limit."""

    prices = frame["estimated_price"].to_numpy(dtype=float)
    weights = compute_weights_from_quantities(post_trade_quantities, prices, nav)
    breaches: dict[str, float] = {}
    for ticker, weight in zip(frame["ticker"], weights, strict=True):
        if weight > limit + 1e-9:
            breaches[str(ticker)] = float(weight)
    return breaches


def industry_breaches(frame: pd.DataFrame, post_trade_quantities, nav: float, bounds) -> dict[str, dict[str, float]]:
    """Return post-trade industry exposures outside configured bounds."""

    exposures = compute_industry_exposures(frame, post_trade_quantities, nav)
    breaches: dict[str, dict[str, float]] = {}
    for industry, exposure in exposures.items():
        bound = bounds.get(industry)
        if bound is None:
            continue
        lower = bound.min
        upper = bound.max
        if lower is not None and exposure < lower - 1e-9:
            breaches[industry] = {"exposure": float(exposure), "min": float(lower)}
        if upper is not None and exposure > upper + 1e-9:
            breaches[industry] = {"exposure": float(exposure), "max": float(upper)}
    return breaches

