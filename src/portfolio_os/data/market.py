"""Market data loaders."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from portfolio_os.data.import_profiles import ImportProfile
from portfolio_os.data.loaders import (
    ensure_columns,
    ensure_optional_positive,
    ensure_positive,
    ensure_unique_tickers,
    normalize_ticker,
    parse_bool,
    read_input_frame,
)
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import MarketRow, MarketSnapshot


def load_market_snapshot(
    path: str | Path,
    required_tickers: Iterable[str],
    *,
    import_profile: ImportProfile | None = None,
) -> MarketSnapshot:
    """Load market snapshot and validate required ticker coverage."""

    frame = read_input_frame(path, input_type="market", import_profile=import_profile)
    ensure_columns(
        frame,
        [
            "ticker",
            "close",
            "adv_shares",
            "tradable",
            "upper_limit_hit",
            "lower_limit_hit",
        ],
        str(path),
    )
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, str(path))
    required = sorted(set(required_tickers))
    available = set(frame["ticker"].tolist())
    missing = sorted(set(required) - available)
    if missing:
        raise InputValidationError(
            f"market.csv is missing required ticker(s): {', '.join(missing)}"
        )
    frame = frame[frame["ticker"].isin(required)].copy()
    for column in ("tradable", "upper_limit_hit", "lower_limit_hit"):
        frame[column] = frame[column].map(lambda value, field=column: parse_bool(value, field))
    ensure_positive(frame, "close", "market.csv")
    ensure_positive(frame, "adv_shares", "market.csv")
    ensure_optional_positive(frame, "vwap", "market.csv")
    frame["close"] = frame["close"].astype(float)
    frame["adv_shares"] = frame["adv_shares"].astype(float)
    frame["vwap"] = frame["vwap"].astype(float) if "vwap" in frame.columns else frame["close"]
    frame["vwap"] = frame["vwap"].fillna(frame["close"])
    rows = [
        MarketRow(
            ticker=row["ticker"],
            close=float(row["close"]),
            vwap=float(row["vwap"]),
            adv_shares=float(row["adv_shares"]),
            tradable=bool(row["tradable"]),
            upper_limit_hit=bool(row["upper_limit_hit"]),
            lower_limit_hit=bool(row["lower_limit_hit"]),
        )
        for row in frame.to_dict(orient="records")
    ]
    return MarketSnapshot(rows=rows)


def market_to_frame(snapshot: MarketSnapshot) -> pd.DataFrame:
    """Convert a market snapshot to a DataFrame."""

    frame = pd.DataFrame([row.model_dump(mode="json") for row in snapshot.rows])
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "close",
                "vwap",
                "adv_shares",
                "tradable",
                "upper_limit_hit",
                "lower_limit_hit",
            ]
        )
    return frame.sort_values("ticker").reset_index(drop=True)
