"""Rolling weighting estimators for the Factor Discovery reliability gate."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


GUARD_VALUES = {
    "uses_full_sample_icir": False,
    "future_universe_used": False,
    "future_normalization_used": False,
    "post_period_factor_selection_used": False,
    "not_alpha_evidence": True,
    "direct_q2_entry_allowed": False,
}


@dataclass(frozen=True)
class EstimatorConfig:
    """Estimator metadata for output rows."""

    estimator_name: str
    estimator_family: str
    shrink_lambda: float | None = None
    ridge_alpha: float | None = None


def estimate_equal_weight_all(factor_ids: list[str] | tuple[str, ...] | pd.Series) -> pd.DataFrame:
    """Return equal positive weights across every factor id."""

    factors = sorted(pd.Series(list(factor_ids)).dropna().astype(str).unique())
    weight = 1.0 / len(factors) if factors else 0.0
    rows = [
        {
            "factor_id": factor_id,
            "mechanism_family": "",
            "weight": weight,
            "raw_weight": 1.0,
            "raw_shrunk_weight": 1.0,
            "history_observation_count": 0,
            "rolling_icir": 0.0,
            "weight_status": "active" if factors else "no_factors",
            **GUARD_VALUES,
        }
        for factor_id in factors
    ]
    return pd.DataFrame(rows)


def estimate_family_equal_weight(metadata: pd.DataFrame) -> pd.DataFrame:
    """Return equal family weights and equal within-family factor weights."""

    frame = _metadata_frame(metadata)
    if frame.empty:
        return pd.DataFrame()
    family_count = frame["mechanism_family"].nunique()
    counts = frame.groupby("mechanism_family")["factor_id"].transform("count")
    frame["weight"] = 1.0 / family_count / counts
    frame["raw_weight"] = 1.0
    frame["raw_shrunk_weight"] = 1.0
    frame["history_observation_count"] = 0
    frame["rolling_icir"] = 0.0
    frame["weight_status"] = "active"
    for key, value in GUARD_VALUES.items():
        frame[key] = value
    return frame[
        [
            "factor_id",
            "mechanism_family",
            "weight",
            "raw_weight",
            "raw_shrunk_weight",
            "history_observation_count",
            "rolling_icir",
            "weight_status",
            *GUARD_VALUES.keys(),
        ]
    ]


def estimate_shrunk_rolling_icir(current_icir: pd.DataFrame, shrink_lambda: float, signed: bool) -> pd.DataFrame:
    """Shrink rolling ICIR weights toward zero using n_eff / (n_eff + lambda)."""

    frame = current_icir.copy()
    frame["factor_id"] = frame["factor_id"].astype(str)
    frame["rolling_icir"] = pd.to_numeric(frame.get("rolling_icir", 0.0), errors="coerce").fillna(0.0)
    frame["history_observation_count"] = pd.to_numeric(
        frame.get("history_observation_count", 0),
        errors="coerce",
    ).fillna(0.0)
    n_eff = frame["history_observation_count"].clip(lower=0.0)
    shrink = n_eff / (n_eff + float(shrink_lambda))
    raw = frame["rolling_icir"] * shrink
    if not signed:
        raw = raw.clip(lower=0.0)
    frame["raw_weight"] = frame["rolling_icir"]
    frame["raw_shrunk_weight"] = raw
    frame["weight"] = _normalize_weight(raw, signed=signed)
    frame["weight_status"] = np.where(frame["history_observation_count"] > 0, "active", "insufficient_visible_history")
    if "mechanism_family" not in frame.columns:
        frame["mechanism_family"] = ""
    for key, value in GUARD_VALUES.items():
        frame[key] = value
    return frame[
        [
            "factor_id",
            "mechanism_family",
            "weight",
            "raw_weight",
            "raw_shrunk_weight",
            "history_observation_count",
            "rolling_icir",
            "weight_status",
            *GUARD_VALUES.keys(),
        ]
    ]


def estimate_ridge_weighting(
    factor_panel: pd.DataFrame,
    targets: pd.DataFrame,
    rebalance_date: pd.Timestamp,
    horizon_months: int,
    factor_ids: list[str],
    train_window_months: int,
    ridge_alpha: float,
) -> pd.DataFrame:
    """Estimate rolling ridge weights using only past visible targets."""

    factors = sorted(pd.Series(factor_ids).dropna().astype(str).unique())
    rebalance = pd.Timestamp(rebalance_date)
    panel = _normalize_factor_panel(factor_panel)
    target = _normalize_targets(targets)
    history_target = target[
        (target["horizon_months"] == int(horizon_months))
        & (target["rebalance_date"] < rebalance)
        & (target["target_return_visible_timestamp"] < rebalance)
        & (target["forward_return_available"])
    ].copy()
    history_dates = sorted(history_target["rebalance_date"].dropna().unique())[-int(train_window_months) :]
    history_target = history_target[history_target["rebalance_date"].isin(history_dates)]
    if history_target.empty:
        return _zero_ridge_rows(factors, "insufficient_visible_history")

    active = panel[
        (panel["coverage_status"] == "active_view")
        & (panel["rebalance_date"].isin(history_dates))
        & (panel["factor_id"].isin(factors))
    ][["rebalance_date", "asset_id", "factor_id", "normalized_value"]]
    wide = active.pivot_table(
        index=["rebalance_date", "asset_id"],
        columns="factor_id",
        values="normalized_value",
        aggfunc="last",
    ).reindex(columns=factors)
    y = history_target[["rebalance_date", "asset_id", "forward_excess_return"]].copy()
    y["target_rank"] = y.groupby("rebalance_date")["forward_excess_return"].rank(pct=True) - 0.5
    training = wide.reset_index().merge(
        y[["rebalance_date", "asset_id", "target_rank"]],
        on=["rebalance_date", "asset_id"],
        how="inner",
    )
    if len(training) < max(3, len(factors)):
        return _zero_ridge_rows(factors, "insufficient_training_rows", history_target)

    x = training[factors].fillna(0.0).to_numpy(dtype="float64")
    response = training["target_rank"].fillna(0.0).to_numpy(dtype="float64")
    xtx = x.T @ x
    penalty = float(ridge_alpha) * np.eye(len(factors))
    try:
        raw_weights = np.linalg.solve(xtx + penalty, x.T @ response)
    except np.linalg.LinAlgError:
        raw_weights = np.linalg.pinv(xtx + penalty) @ x.T @ response
    weights = _normalize_weight(pd.Series(raw_weights, index=factors), signed=True)
    rows = []
    for factor_id in factors:
        rows.append(
            {
                "factor_id": factor_id,
                "mechanism_family": "",
                "weight": float(weights.loc[factor_id]),
                "raw_weight": float(raw_weights[factors.index(factor_id)]),
                "raw_shrunk_weight": float(raw_weights[factors.index(factor_id)]),
                "history_observation_count": int(history_target["rebalance_date"].nunique()),
                "rolling_icir": 0.0,
                "weight_status": "active",
                "estimation_window_start": _date_str(history_target["rebalance_date"].min()),
                "estimation_window_end": _date_str(history_target["rebalance_date"].max()),
                "return_visibility_cutoff": _date_str(history_target["target_return_visible_timestamp"].max()),
                **GUARD_VALUES,
            }
        )
    return pd.DataFrame(rows)


def _normalize_weight(values: pd.Series, signed: bool) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").fillna(0.0)
    denominator = float(series.abs().sum() if signed else series.sum())
    if denominator <= 0.0 or not np.isfinite(denominator):
        return series * 0.0
    return series / denominator


def _metadata_frame(metadata: pd.DataFrame) -> pd.DataFrame:
    frame = metadata.copy()
    if "mechanism_family" not in frame.columns:
        frame["mechanism_family"] = frame.get("known_correlation_family", "unknown")
    frame["mechanism_family"] = frame["mechanism_family"].fillna("unknown").astype(str)
    frame["factor_id"] = frame["factor_id"].astype(str)
    return frame[["factor_id", "mechanism_family"]].drop_duplicates("factor_id").sort_values("factor_id")


def _normalize_factor_panel(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    normalized["normalized_value"] = pd.to_numeric(normalized["normalized_value"], errors="coerce")
    return normalized


def _normalize_targets(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["horizon_months"] = normalized["horizon_months"].astype(int)
    normalized["forward_excess_return"] = pd.to_numeric(normalized["forward_excess_return"], errors="coerce")
    normalized["forward_return_available"] = normalized["forward_return_available"].astype(str).str.lower().isin(
        {"true", "1", "yes"}
    )
    if "target_return_visible_timestamp" not in normalized.columns:
        normalized["target_return_visible_timestamp"] = normalized["rebalance_date"] + pd.offsets.MonthEnd(1)
    normalized["target_return_visible_timestamp"] = pd.to_datetime(
        normalized["target_return_visible_timestamp"],
        errors="coerce",
    )
    return normalized


def _zero_ridge_rows(
    factors: list[str],
    status: str,
    history_target: pd.DataFrame | None = None,
) -> pd.DataFrame:
    history_target = history_target if history_target is not None else pd.DataFrame()
    rows = []
    for factor_id in factors:
        rows.append(
            {
                "factor_id": factor_id,
                "mechanism_family": "",
                "weight": 0.0,
                "raw_weight": 0.0,
                "raw_shrunk_weight": 0.0,
                "history_observation_count": int(history_target["rebalance_date"].nunique()) if not history_target.empty else 0,
                "rolling_icir": 0.0,
                "weight_status": status,
                "estimation_window_start": _date_str(history_target["rebalance_date"].min()) if not history_target.empty else "",
                "estimation_window_end": _date_str(history_target["rebalance_date"].max()) if not history_target.empty else "",
                "return_visibility_cutoff": _date_str(history_target["target_return_visible_timestamp"].max())
                if not history_target.empty
                else "",
                **GUARD_VALUES,
            }
        )
    return pd.DataFrame(rows)


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()
