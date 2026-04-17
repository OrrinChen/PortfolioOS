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
_UPPER_LIMIT_PILOT_REQUIRED_COLUMNS = {
    "date",
    "ticker",
    "sealed_upper_limit",
    "failed_upper_limit",
    "next_open_return",
    "next_intraday_return",
    "next_close_return",
}
_UPPER_LIMIT_MATCHING_REQUIRED_COLUMNS = {
    "date",
    "ticker",
    "tradable",
    "upper_limit_touched",
    "sealed_upper_limit",
    "failed_upper_limit",
    "industry",
    "size_tercile",
    "liquidity_tercile",
    "recent_realized_volatility",
    "recent_return_state",
}
_UPPER_LIMIT_PILOT_SPECS = (
    ("P1_SEALED_UPPER_LIMIT", "M1", "SEALED_UPPER_LIMIT", "sealed_upper_limit", 1.0, "next_close_return"),
    ("P2_FAILED_UPPER_LIMIT", "M2", "FAILED_UPPER_LIMIT", "failed_upper_limit", -1.0, "next_close_return"),
    ("P3_NEXT_DAY_AFTER_SEALED", "M5", "SEALED_UPPER_LIMIT", "sealed_upper_limit", 1.0, "next_intraday_return"),
    ("P4_NEXT_DAY_AFTER_FAILED", "M5", "FAILED_UPPER_LIMIT", "failed_upper_limit", -1.0, "next_intraday_return"),
)


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
    work["next_intraday_return"] = work["next_close"] / work["next_open"] - 1.0
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


def build_upper_limit_pilot_expression_frame(panel: pd.DataFrame) -> pd.DataFrame:
    """Expand one upper-limit daily-state slice into the first-wave pilot expressions."""

    missing = sorted(_UPPER_LIMIT_PILOT_REQUIRED_COLUMNS - set(panel.columns))
    if missing:
        raise InputValidationError(
            "upper-limit pilot expression frame requires derived columns: "
            + ", ".join(missing)
        )

    rows: list[pd.DataFrame] = []
    for expression_id, mechanism_id, state_anchor, state_column, expected_sign, forward_column in _UPPER_LIMIT_PILOT_SPECS:
        subset = panel.loc[panel[state_column]].copy()
        if subset.empty:
            continue
        subset["expression_id"] = expression_id
        subset["mechanism_id"] = mechanism_id
        subset["state_anchor"] = state_anchor
        subset["signal_value"] = float(expected_sign)
        subset["expected_sign"] = float(expected_sign)
        subset["forward_return"] = pd.to_numeric(subset[forward_column], errors="coerce")
        rows.append(
            subset.loc[
                :,
                [
                    "date",
                    "ticker",
                    "expression_id",
                    "mechanism_id",
                    "state_anchor",
                    "signal_value",
                    "expected_sign",
                    "forward_return",
                    "next_open_return",
                    "next_intraday_return",
                    "next_close_return",
                ],
            ]
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "signal_value",
                "expected_sign",
                "forward_return",
                "next_open_return",
                "next_intraday_return",
                "next_close_return",
            ]
        )
    return (
        pd.concat(rows, ignore_index=True)
        .sort_values(["date", "ticker", "expression_id"])
        .reset_index(drop=True)
    )


def build_upper_limit_matched_non_event_control_frame(panel: pd.DataFrame) -> pd.DataFrame:
    """Select same-day matched non-event controls for upper-limit events."""

    missing = sorted(_UPPER_LIMIT_MATCHING_REQUIRED_COLUMNS - set(panel.columns))
    if missing:
        raise InputValidationError(
            "upper-limit matched control frame requires matching columns: "
            + ", ".join(missing)
        )

    work = panel.copy()
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["industry"] = work["industry"].astype(str).str.strip()
    work["tradable"] = work["tradable"].fillna(False).astype(bool)
    numeric_columns = [
        "size_tercile",
        "liquidity_tercile",
        "recent_realized_volatility",
        "recent_return_state",
    ]
    for column in numeric_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    events = work.loc[work["tradable"] & work["upper_limit_touched"]].copy()
    candidates = work.loc[work["tradable"] & ~work["upper_limit_touched"]].copy()

    rows: list[dict[str, object]] = []
    for _, event_row in events.sort_values(["date", "ticker"]).iterrows():
        bucket = candidates.loc[
            (candidates["date"] == event_row["date"])
            & (candidates["industry"] == event_row["industry"])
            & (candidates["size_tercile"] == event_row["size_tercile"])
            & (candidates["liquidity_tercile"] == event_row["liquidity_tercile"])
        ].copy()
        if bucket.empty:
            continue
        bucket["match_distance"] = (
            (bucket["recent_realized_volatility"] - float(event_row["recent_realized_volatility"])).abs()
            + (bucket["recent_return_state"] - float(event_row["recent_return_state"])).abs()
        )
        best = bucket.sort_values(["match_distance", "ticker"]).iloc[0]
        rows.append(
            {
                "date": event_row["date"],
                "event_ticker": event_row["ticker"],
                "control_ticker": best["ticker"],
                "event_state_anchor": (
                    "SEALED_UPPER_LIMIT"
                    if bool(event_row["sealed_upper_limit"])
                    else "FAILED_UPPER_LIMIT"
                ),
                "industry": event_row["industry"],
                "size_tercile": int(event_row["size_tercile"]),
                "liquidity_tercile": int(event_row["liquidity_tercile"]),
                "match_distance": float(best["match_distance"]),
                "event_recent_realized_volatility": float(event_row["recent_realized_volatility"]),
                "control_recent_realized_volatility": float(best["recent_realized_volatility"]),
                "event_recent_return_state": float(event_row["recent_return_state"]),
                "control_recent_return_state": float(best["recent_return_state"]),
            }
        )

    return (
        pd.DataFrame(
            rows,
            columns=[
                "date",
                "event_ticker",
                "control_ticker",
                "event_state_anchor",
                "industry",
                "size_tercile",
                "liquidity_tercile",
                "match_distance",
                "event_recent_realized_volatility",
                "control_recent_realized_volatility",
                "event_recent_return_state",
                "control_recent_return_state",
            ],
        )
        .sort_values(["date", "event_ticker"])
        .reset_index(drop=True)
    )
