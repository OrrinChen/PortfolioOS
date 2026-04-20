"""Build the unified optimization universe."""

from __future__ import annotations

import pandas as pd

from portfolio_os.domain.models import PortfolioState


def build_universe_frame(
    portfolio_frame: pd.DataFrame,
    market_frame: pd.DataFrame,
    reference_frame: pd.DataFrame,
    portfolio_state: PortfolioState,
) -> pd.DataFrame:
    """Merge dynamic and static inputs into one working frame."""

    frame = portfolio_frame.merge(market_frame, on="ticker", how="left")
    frame = frame.merge(reference_frame, on="ticker", how="left")
    frame["estimated_price"] = frame["vwap"].fillna(frame["close"]).astype(float)
    frame["quantity"] = frame["quantity"].astype(int)
    frame["target_weight"] = frame["target_weight"].astype(float)
    frame["current_notional"] = frame["quantity"] * frame["estimated_price"]
    pre_trade_nav = float(frame["current_notional"].sum() + portfolio_state.available_cash)
    frame["current_weight"] = frame["current_notional"] / pre_trade_nav if pre_trade_nav else 0.0
    frame["manager_aggregate_qty"] = frame["manager_aggregate_qty"].fillna(0.0)
    frame["issuer_total_shares"] = frame["issuer_total_shares"].fillna(0.0)
    return frame.sort_values("ticker").reset_index(drop=True)
