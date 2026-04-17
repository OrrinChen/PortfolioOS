"""Daily panel contract for A-share state-transition pilot expressions."""

from __future__ import annotations

import numpy as np
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
_UPPER_LIMIT_PLACEBO_FORWARD_COLUMN_BY_EXPRESSION = {
    "P1_SEALED_UPPER_LIMIT": "__prior_next_close_return",
    "P2_FAILED_UPPER_LIMIT": "__prior_next_close_return",
    "P3_NEXT_DAY_AFTER_SEALED": "__prior_intraday_return",
    "P4_NEXT_DAY_AFTER_FAILED": "__prior_intraday_return",
}
_UPPER_LIMIT_HORIZON_BUCKET_BY_EXPRESSION = {
    "P1_SEALED_UPPER_LIMIT": "NEXT_CLOSE",
    "P2_FAILED_UPPER_LIMIT": "NEXT_CLOSE",
    "P3_NEXT_DAY_AFTER_SEALED": "NEXT_INTRADAY",
    "P4_NEXT_DAY_AFTER_FAILED": "NEXT_INTRADAY",
}


def _empirical_percentile(sample: pd.Series, value: float) -> float:
    clean = pd.to_numeric(sample, errors="coerce").dropna().astype(float)
    if clean.empty or not np.isfinite(value):
        return float("nan")
    return float((clean <= float(value)).mean())


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


def build_upper_limit_pre_event_placebo_comparison_frame(
    expression_frame: pd.DataFrame,
    placebo_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Join event expressions with prior-window placebo returns on the same names."""

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
            "upper-limit pre-event placebo comparison requires expression columns: "
            + ", ".join(missing_expression)
        )

    required_placebo_panel = {
        "date",
        "ticker",
        "open",
        "close",
        "next_close_return",
    }
    missing_placebo_panel = sorted(required_placebo_panel - set(placebo_panel.columns))
    if missing_placebo_panel:
        raise InputValidationError(
            "upper-limit pre-event placebo comparison requires placebo-panel columns: "
            + ", ".join(missing_placebo_panel)
        )

    work = expression_frame.rename(
        columns={"ticker": "event_ticker", "forward_return": "event_forward_return"}
    ).copy()

    source = placebo_panel.copy()
    source["date"] = pd.to_datetime(source["date"], errors="coerce")
    if source["date"].isna().any():
        raise InputValidationError(
            "upper-limit pre-event placebo comparison contains invalid placebo-panel dates."
        )
    source = source.sort_values(["ticker", "date"]).reset_index(drop=True)
    source["__intraday_return"] = pd.to_numeric(source["close"], errors="coerce") / pd.to_numeric(
        source["open"], errors="coerce"
    ) - 1.0
    source["__event_date"] = source.groupby("ticker", sort=False)["date"].shift(-1)
    placebo_source = (
        source.loc[
            :,
            [
                "__event_date",
                "ticker",
                "next_close_return",
                "__intraday_return",
            ],
        ]
        .rename(
            columns={
                "__event_date": "date",
                "ticker": "event_ticker",
                "next_close_return": "__prior_next_close_return",
                "__intraday_return": "__prior_intraday_return",
            }
        )
        .dropna(subset=["date"])
    )
    placebo_source["date"] = placebo_source["date"].dt.strftime("%Y-%m-%d")

    merged = work.merge(placebo_source, on=["date", "event_ticker"], how="left")
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "placebo_forward_return",
                "placebo_excess_return",
            ]
        )

    placebo_rows: list[pd.DataFrame] = []
    for expression_id, date_frame in merged.groupby("expression_id", sort=False):
        placebo_column = _UPPER_LIMIT_PLACEBO_FORWARD_COLUMN_BY_EXPRESSION.get(str(expression_id))
        if placebo_column is None:
            continue
        joined = date_frame.copy()
        joined["placebo_forward_return"] = pd.to_numeric(joined[placebo_column], errors="coerce")
        placebo_rows.append(joined)

    if not placebo_rows:
        return pd.DataFrame(
            columns=[
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "placebo_forward_return",
                "placebo_excess_return",
            ]
        )

    result = pd.concat(placebo_rows, ignore_index=True)
    result["placebo_excess_return"] = (
        pd.to_numeric(result["event_forward_return"], errors="coerce")
        - pd.to_numeric(result["placebo_forward_return"], errors="coerce")
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
                "signal_value",
                "expected_sign",
                "event_forward_return",
                "placebo_forward_return",
                "placebo_excess_return",
            ],
        ]
        .sort_values(["date", "expression_id", "event_ticker"])
        .reset_index(drop=True)
    )


def build_upper_limit_event_conditioned_null_pool(
    expression_frame: pd.DataFrame,
    conditioning_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Attach P-001 event-conditioned strata to live upper-limit pilot expressions."""

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
            "upper-limit event-conditioned null pool requires expression columns: "
            + ", ".join(missing_expression)
        )

    required_conditioning = {
        "date",
        "ticker",
        "size_tercile",
        "liquidity_tercile",
    }
    missing_conditioning = sorted(required_conditioning - set(conditioning_panel.columns))
    if missing_conditioning:
        raise InputValidationError(
            "upper-limit event-conditioned null pool requires conditioning columns: "
            + ", ".join(missing_conditioning)
        )

    work = expression_frame.rename(columns={"ticker": "event_ticker"}).copy()
    conditioning = conditioning_panel.loc[:, ["date", "ticker", "size_tercile", "liquidity_tercile"]].rename(
        columns={"ticker": "event_ticker"}
    )
    merged = work.merge(conditioning, on=["date", "event_ticker"], how="left")

    merged["size_tercile"] = pd.to_numeric(merged["size_tercile"], errors="coerce")
    merged["liquidity_tercile"] = pd.to_numeric(merged["liquidity_tercile"], errors="coerce")
    if merged[["size_tercile", "liquidity_tercile"]].isna().any().any():
        raise InputValidationError(
            "upper-limit event-conditioned null pool requires complete conditioning rows."
        )

    merged["event_type_bucket"] = merged["state_anchor"].astype(str)
    merged["horizon_bucket"] = merged["expression_id"].map(_UPPER_LIMIT_HORIZON_BUCKET_BY_EXPRESSION)
    if merged["horizon_bucket"].isna().any():
        raise InputValidationError(
            "upper-limit event-conditioned null pool encountered an unknown expression horizon."
        )
    merged["conditioning_bucket_key"] = merged.apply(
        lambda row: (
            f"{row['event_type_bucket']}|{row['horizon_bucket']}|"
            f"{int(row['size_tercile'])}|{int(row['liquidity_tercile'])}"
        ),
        axis=1,
    )
    return (
        merged.loc[
            :,
            [
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "signal_value",
                "expected_sign",
                "forward_return",
                "event_type_bucket",
                "horizon_bucket",
                "size_tercile",
                "liquidity_tercile",
                "conditioning_bucket_key",
            ],
        ]
        .sort_values(["date", "expression_id", "event_ticker"])
        .reset_index(drop=True)
    )


def build_upper_limit_event_conditioned_null_draw(
    null_pool: pd.DataFrame,
    *,
    random_seed: int,
) -> pd.DataFrame:
    """Generate one seed-based P-001 null draw by shuffling within event-conditioned buckets."""

    required_null_pool = {
        "date",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "event_ticker",
        "signal_value",
        "expected_sign",
        "forward_return",
        "event_type_bucket",
        "horizon_bucket",
        "size_tercile",
        "liquidity_tercile",
        "conditioning_bucket_key",
    }
    missing_null_pool = sorted(required_null_pool - set(null_pool.columns))
    if missing_null_pool:
        raise InputValidationError(
            "upper-limit event-conditioned null draw requires null-pool columns: "
            + ", ".join(missing_null_pool)
        )

    work = null_pool.copy().sort_values(["date", "conditioning_bucket_key", "event_ticker"]).reset_index(drop=True)
    shuffled_rows: list[pd.DataFrame] = []
    grouped = list(work.groupby(["date", "conditioning_bucket_key"], sort=True))
    for offset, (_, bucket_frame) in enumerate(grouped):
        rng = np.random.default_rng(int(random_seed) + int(offset))
        shuffled = bucket_frame.copy()
        shuffled["null_forward_return"] = rng.permutation(
            pd.to_numeric(bucket_frame["forward_return"], errors="coerce").to_numpy()
        )
        shuffled["null_seed"] = int(random_seed)
        shuffled_rows.append(shuffled)

    if not shuffled_rows:
        return pd.DataFrame(
            columns=[
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "signal_value",
                "expected_sign",
                "forward_return",
                "event_type_bucket",
                "horizon_bucket",
                "size_tercile",
                "liquidity_tercile",
                "conditioning_bucket_key",
                "null_forward_return",
                "null_seed",
            ]
        )

    result = pd.concat(shuffled_rows, ignore_index=True)
    return (
        result.loc[
            :,
            [
                "date",
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "event_ticker",
                "signal_value",
                "expected_sign",
                "forward_return",
                "event_type_bucket",
                "horizon_bucket",
                "size_tercile",
                "liquidity_tercile",
                "conditioning_bucket_key",
                "null_forward_return",
                "null_seed",
            ],
        ]
        .sort_values(["date", "expression_id", "event_ticker"])
        .reset_index(drop=True)
    )


def build_upper_limit_event_conditioned_null_summary(
    null_pool: pd.DataFrame,
    *,
    random_seeds: list[int],
) -> pd.DataFrame:
    """Summarize observed expression means against repeated P-001 null draws."""

    if not random_seeds:
        raise InputValidationError("random_seeds must not be empty.")

    required_null_pool = {
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "forward_return",
    }
    missing_null_pool = sorted(required_null_pool - set(null_pool.columns))
    if missing_null_pool:
        raise InputValidationError(
            "upper-limit event-conditioned null summary requires null-pool columns: "
            + ", ".join(missing_null_pool)
        )

    observed = (
        null_pool.groupby(["expression_id", "mechanism_id", "state_anchor"], sort=True)
        .agg(
            observation_count=("event_ticker", "count"),
            observed_mean_forward_return=("forward_return", lambda values: float(pd.to_numeric(values, errors="coerce").mean())),
        )
        .reset_index()
    )

    draw_summaries: list[pd.DataFrame] = []
    for seed in random_seeds:
        draw = build_upper_limit_event_conditioned_null_draw(null_pool, random_seed=int(seed))
        grouped = (
            draw.groupby(["expression_id", "mechanism_id", "state_anchor"], sort=True)
            .agg(
                null_mean_forward_return=("null_forward_return", lambda values: float(pd.to_numeric(values, errors="coerce").mean()))
            )
            .reset_index()
        )
        grouped["null_seed"] = int(seed)
        draw_summaries.append(grouped)

    null_summary_frame = (
        pd.concat(draw_summaries, ignore_index=True)
        if draw_summaries
        else pd.DataFrame(columns=["expression_id", "mechanism_id", "state_anchor", "null_mean_forward_return", "null_seed"])
    )

    rows: list[dict[str, object]] = []
    for observed_row in observed.itertuples(index=False):
        sample = null_summary_frame.loc[
            null_summary_frame["expression_id"] == observed_row.expression_id,
            "null_mean_forward_return",
        ]
        unique_count = int(pd.to_numeric(sample, errors="coerce").dropna().nunique())
        is_degenerate = unique_count <= 1
        rows.append(
            {
                "expression_id": observed_row.expression_id,
                "mechanism_id": observed_row.mechanism_id,
                "state_anchor": observed_row.state_anchor,
                "observation_count": int(observed_row.observation_count),
                "null_seed_count": int(len(random_seeds)),
                "observed_mean_forward_return": float(observed_row.observed_mean_forward_return),
                "null_mean_forward_return_median": float(pd.to_numeric(sample, errors="coerce").median())
                if not sample.empty
                else float("nan"),
                "null_mean_forward_return_std": float(pd.to_numeric(sample, errors="coerce").std(ddof=1))
                if len(pd.to_numeric(sample, errors="coerce").dropna()) >= 2
                else float("nan"),
                "null_mean_forward_return_unique_count": unique_count,
                "null_is_degenerate": bool(is_degenerate),
                "observed_mean_forward_return_null_percentile": (
                    float("nan")
                    if is_degenerate
                    else _empirical_percentile(sample, float(observed_row.observed_mean_forward_return))
                ),
            }
        )

    return pd.DataFrame(rows).sort_values(["expression_id"]).reset_index(drop=True)


def build_upper_limit_pilot_read_frame(
    expression_frame: pd.DataFrame,
    control_comparison_frame: pd.DataFrame,
    placebo_comparison_frame: pd.DataFrame,
    null_summary_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Join live, control, placebo, and P-001 null reads into one per-expression pilot frame."""

    required_expression = {
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "forward_return",
    }
    missing_expression = sorted(required_expression - set(expression_frame.columns))
    if missing_expression:
        raise InputValidationError(
            "upper-limit pilot read frame requires expression columns: "
            + ", ".join(missing_expression)
        )

    required_control = {"expression_id", "excess_forward_return"}
    missing_control = sorted(required_control - set(control_comparison_frame.columns))
    if missing_control:
        raise InputValidationError(
            "upper-limit pilot read frame requires control-comparison columns: "
            + ", ".join(missing_control)
        )

    required_placebo = {"expression_id", "placebo_excess_return"}
    missing_placebo = sorted(required_placebo - set(placebo_comparison_frame.columns))
    if missing_placebo:
        raise InputValidationError(
            "upper-limit pilot read frame requires placebo-comparison columns: "
            + ", ".join(missing_placebo)
        )

    required_null_summary = {
        "expression_id",
        "null_seed_count",
        "null_mean_forward_return_median",
        "null_mean_forward_return_std",
        "null_mean_forward_return_unique_count",
        "null_is_degenerate",
        "observed_mean_forward_return_null_percentile",
    }
    missing_null_summary = sorted(required_null_summary - set(null_summary_frame.columns))
    if missing_null_summary:
        raise InputValidationError(
            "upper-limit pilot read frame requires null-summary columns: "
            + ", ".join(missing_null_summary)
        )

    observed = (
        expression_frame.groupby(["expression_id", "mechanism_id", "state_anchor"], sort=True)
        .agg(
            observation_count=("ticker", "count"),
            observed_mean_forward_return=("forward_return", lambda values: float(pd.to_numeric(values, errors="coerce").mean())),
        )
        .reset_index()
    )
    control = (
        control_comparison_frame.groupby("expression_id", sort=True)
        .agg(mean_excess_vs_control=("excess_forward_return", lambda values: float(pd.to_numeric(values, errors="coerce").mean())))
        .reset_index()
    )
    placebo = (
        placebo_comparison_frame.groupby("expression_id", sort=True)
        .agg(mean_excess_vs_placebo=("placebo_excess_return", lambda values: float(pd.to_numeric(values, errors="coerce").mean())))
        .reset_index()
    )

    result = observed.merge(control, on="expression_id", how="left")
    result = result.merge(placebo, on="expression_id", how="left")
    result = result.merge(
        null_summary_frame.loc[
            :,
            [
                "expression_id",
                "null_seed_count",
                "null_mean_forward_return_median",
                "null_mean_forward_return_std",
                "null_mean_forward_return_unique_count",
                "null_is_degenerate",
                "observed_mean_forward_return_null_percentile",
            ],
        ],
        on="expression_id",
        how="left",
    )
    return (
        result.loc[
            :,
            [
                "expression_id",
                "mechanism_id",
                "state_anchor",
                "observation_count",
                "observed_mean_forward_return",
                "mean_excess_vs_control",
                "mean_excess_vs_placebo",
                "null_seed_count",
                "null_mean_forward_return_median",
                "null_mean_forward_return_std",
                "null_mean_forward_return_unique_count",
                "null_is_degenerate",
                "observed_mean_forward_return_null_percentile",
            ],
        ]
        .sort_values(["expression_id"])
        .reset_index(drop=True)
    )
