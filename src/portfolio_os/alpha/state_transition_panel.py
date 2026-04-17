"""Daily panel contract for A-share state-transition pilot expressions."""

from __future__ import annotations

import pandas as pd

from portfolio_os.domain.errors import InputValidationError


REQUIRED_STATE_TRANSITION_COLUMNS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "upper_limit_price",
    "lower_limit_price",
    "tradable",
]

_PRICE_TOLERANCE = 1e-6


def build_state_transition_daily_panel(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize one daily-bar frame and derive first-slice state tags."""

    work = frame.copy()
    missing = sorted(set(REQUIRED_STATE_TRANSITION_COLUMNS) - set(work.columns))
    if missing:
        raise InputValidationError(
            "state-transition daily panel is missing required columns: "
            + ", ".join(missing)
        )

    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["ticker"] = work["ticker"].astype(str).str.strip()
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "upper_limit_price",
        "lower_limit_price",
    ]
    for column in numeric_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    if work["date"].isna().any():
        raise InputValidationError("state-transition daily panel contains invalid dates.")
    if work["ticker"].eq("").any():
        raise InputValidationError("state-transition daily panel contains blank tickers.")
    if work[numeric_columns].isna().any().any():
        raise InputValidationError("state-transition daily panel contains invalid numeric values.")
    if work.duplicated(subset=["date", "ticker"]).any():
        raise InputValidationError(
            "state-transition daily panel contains duplicate (date, ticker) rows."
        )

    work["date"] = work["date"].dt.normalize()
    work["tradable"] = work["tradable"].fillna(False).astype(bool)
    work = work.sort_values(["ticker", "date"]).reset_index(drop=True)

    work["upper_limit_touched"] = work["high"] >= (
        work["upper_limit_price"] - _PRICE_TOLERANCE
    )
    work["lower_limit_touched"] = work["low"] <= (
        work["lower_limit_price"] + _PRICE_TOLERANCE
    )
    work["sealed_upper_limit"] = work["upper_limit_touched"] & (
        work["close"] >= (work["upper_limit_price"] - _PRICE_TOLERANCE)
    )
    work["failed_upper_limit"] = work["upper_limit_touched"] & ~work["sealed_upper_limit"]

    grouped = work.groupby("ticker", sort=False)
    work["next_open"] = grouped["open"].shift(-1)
    work["next_close"] = grouped["close"].shift(-1)
    work["next_open_return"] = work["next_open"] / work["close"] - 1.0
    work["next_close_return"] = work["next_close"] / work["close"] - 1.0
    work["date"] = work["date"].dt.strftime("%Y-%m-%d")
    return work


def extract_upper_limit_daily_state_slice(panel: pd.DataFrame) -> pd.DataFrame:
    """Return the first D2 pilot subset keyed on upper-limit touches."""

    required = {"upper_limit_touched", "sealed_upper_limit", "failed_upper_limit"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise InputValidationError(
            "upper-limit pilot extraction requires derived state columns: "
            + ", ".join(missing)
        )
    work = panel.copy()
    work = work.loc[work["upper_limit_touched"]].copy()
    return work.sort_values(["date", "ticker"]).reset_index(drop=True)
