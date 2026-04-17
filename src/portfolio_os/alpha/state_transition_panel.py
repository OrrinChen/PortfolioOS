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
_MATCHING_REFERENCE_REQUIRED_COLUMNS = {
    "ticker",
    "industry",
    "issuer_total_shares",
}
_UPPER_LIMIT_PILOT_SPECS = (
    ("P1_SEALED_UPPER_LIMIT", "M1", "SEALED_UPPER_LIMIT", "sealed_upper_limit", 1.0, "next_close_return"),
    ("P2_FAILED_UPPER_LIMIT", "M2", "FAILED_UPPER_LIMIT", "failed_upper_limit", -1.0, "next_close_return"),
    ("P3_NEXT_DAY_AFTER_SEALED", "M5", "SEALED_UPPER_LIMIT", "sealed_upper_limit", 1.0, "next_intraday_return"),
    ("P4_NEXT_DAY_AFTER_FAILED", "M5", "FAILED_UPPER_LIMIT", "failed_upper_limit", -1.0, "next_intraday_return"),
)
_UPPER_LIMIT_CONTROL_FORWARD_COLUMN_BY_EXPRESSION = {
    "P1_SEALED_UPPER_LIMIT": "next_close_return",
    "P2_FAILED_UPPER_LIMIT": "next_close_return",
    "P3_NEXT_DAY_AFTER_SEALED": "next_intraday_return",
    "P4_NEXT_DAY_AFTER_FAILED": "next_intraday_return",
}


def _cross_sectional_terciles(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    result = pd.Series(pd.NA, index=values.index, dtype="Float64")
    valid = numeric.dropna()
    if valid.empty:
        return result
    terciles = (
        (valid.rank(method="first", pct=True) * 3.0)
        .apply(lambda value: min(3, max(1, int(value if float(value).is_integer() else value + 0.999999999)))))
    result.loc[valid.index] = terciles.astype(float)
    return result


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


def build_state_transition_matching_covariates(
    panel: pd.DataFrame,
    reference_frame: pd.DataFrame,
    *,
    lookback_days: int = 20,
) -> pd.DataFrame:
    """Enrich the state-transition panel with NC-1 matching covariates."""

    if lookback_days <= 0:
        raise InputValidationError("lookback_days must be positive.")

    missing_reference = sorted(_MATCHING_REFERENCE_REQUIRED_COLUMNS - set(reference_frame.columns))
    if missing_reference:
        raise InputValidationError(
            "state-transition matching covariates requires reference columns: "
            + ", ".join(missing_reference)
        )

    work = panel.copy()
    reference = reference_frame.copy()
    reference["ticker"] = reference["ticker"].astype(str).str.strip()
    reference["industry"] = reference["industry"].astype(str).str.strip()
    reference["issuer_total_shares"] = pd.to_numeric(reference["issuer_total_shares"], errors="coerce")

    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    work = work.merge(
        reference.loc[:, ["ticker", "industry", "issuer_total_shares"]],
        on="ticker",
        how="left",
    )
    work["float_market_cap"] = pd.to_numeric(work["close"], errors="coerce") * work["issuer_total_shares"]
    work = work.sort_values(["ticker", "date"]).reset_index(drop=True)

    grouped = work.groupby("ticker", sort=False)
    work["daily_return"] = grouped["close"].pct_change()
    work["recent_realized_volatility"] = (
        grouped["daily_return"].rolling(window=int(lookback_days), min_periods=int(lookback_days)).std().reset_index(level=0, drop=True)
    )
    work["recent_return_state"] = grouped["close"].pct_change(periods=int(lookback_days))
    work["recent_liquidity_amount"] = (
        grouped["amount"].rolling(window=int(lookback_days), min_periods=int(lookback_days)).mean().reset_index(level=0, drop=True)
    )

    work["size_tercile"] = (
        work.groupby("date", sort=False)["float_market_cap"]
        .transform(_cross_sectional_terciles)
    )
    work["liquidity_tercile"] = (
        work.groupby("date", sort=False)["recent_liquidity_amount"]
        .transform(_cross_sectional_terciles)
    )
    return work


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
    work = work.dropna(
        subset=[
            "industry",
            "size_tercile",
            "liquidity_tercile",
            "recent_realized_volatility",
            "recent_return_state",
        ]
    ).copy()

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


def build_upper_limit_matched_control_comparison_frame(
    expression_frame: pd.DataFrame,
    matched_control_frame: pd.DataFrame,
    control_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Join event expressions with matched control forward returns on the same horizon."""

    required_expression = {
        "date",
        "ticker",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "signal_value",
        "expected_sign",
        "forward_return",
    }
    missing_expression = sorted(required_expression - set(expression_frame.columns))
    if missing_expression:
        raise InputValidationError(
            "upper-limit matched control comparison requires expression columns: "
            + ", ".join(missing_expression)
        )

    required_match = {"date", "event_ticker", "control_ticker"}
    missing_match = sorted(required_match - set(matched_control_frame.columns))
    if missing_match:
        raise InputValidationError(
            "upper-limit matched control comparison requires matched-control columns: "
            + ", ".join(missing_match)
        )

    required_control_panel = {
        "date",
        "ticker",
        "next_intraday_return",
        "next_close_return",
    }
    missing_control_panel = sorted(required_control_panel - set(control_panel.columns))
    if missing_control_panel:
        raise InputValidationError(
            "upper-limit matched control comparison requires control-panel columns: "
            + ", ".join(missing_control_panel)
        )

    work = expression_frame.rename(columns={"ticker": "event_ticker", "forward_return": "event_forward_return"}).copy()
    matches = matched_control_frame.loc[:, ["date", "event_ticker", "control_ticker"]].copy()
    controls = control_panel.rename(columns={"ticker": "control_ticker"}).copy()

    merged = work.merge(matches, on=["date", "event_ticker"], how="inner")
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "control_ticker",
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "control_forward_return",
                "excess_forward_return",
            ]
        )

    control_rows: list[pd.DataFrame] = []
    for expression_id, date_frame in merged.groupby("expression_id", sort=False):
        forward_column = _UPPER_LIMIT_CONTROL_FORWARD_COLUMN_BY_EXPRESSION.get(str(expression_id))
        if forward_column is None:
            continue
        control_forward_column = "__control_forward_return_source"
        joined = date_frame.merge(
            controls.loc[:, ["date", "control_ticker", forward_column]].rename(
                columns={forward_column: control_forward_column}
            ),
            on=["date", "control_ticker"],
            how="left",
        )
        joined["control_forward_return"] = pd.to_numeric(
            joined[control_forward_column], errors="coerce"
        )
        control_rows.append(joined)

    if not control_rows:
        return pd.DataFrame(
            columns=[
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "control_ticker",
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "control_forward_return",
                "excess_forward_return",
            ]
        )

    result = pd.concat(control_rows, ignore_index=True)
    result["excess_forward_return"] = (
        pd.to_numeric(result["event_forward_return"], errors="coerce")
        - pd.to_numeric(result["control_forward_return"], errors="coerce")
    )
    return (
        result.loc[
            :,
            [
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "control_ticker",
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "control_forward_return",
                "excess_forward_return",
            ],
        ]
        .sort_values(["date", "expression_id", "event_ticker"])
        .reset_index(drop=True)
    )
