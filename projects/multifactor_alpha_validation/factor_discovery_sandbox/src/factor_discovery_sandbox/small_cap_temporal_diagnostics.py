"""Temporal diagnostics for FD-S4.1 small-cap dominance analysis."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from .small_cap_data_admission import GUARDS


PRIMARY_SIGNAL = "small_cap_quality_residual_momentum_6m_ex1m"
SIGNAL_VARIANTS = [
    "live_signal",
    "lag_1m_signal",
    "lag_2m_signal",
    "lag_3m_signal",
    "rolling_3m_mean_signal",
    "rolling_3m_median_signal",
    "stale_signal_carry_forward",
    "live_minus_lag_update_component",
]


def build_signal_variants(signal_panel: pd.DataFrame) -> pd.DataFrame:
    """Return a long signal-variant panel using only current or past signals."""

    if signal_panel.empty:
        return pd.DataFrame(columns=["rebalance_date", "asset_id", "signal_variant", "diagnostic_score"])
    primary = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    if primary.empty:
        return pd.DataFrame(columns=["rebalance_date", "asset_id", "signal_variant", "diagnostic_score"])
    primary["rebalance_date"] = pd.to_datetime(primary["rebalance_date"], errors="coerce")
    primary["score"] = pd.to_numeric(primary["score"], errors="coerce")
    primary = primary.sort_values(["asset_id", "rebalance_date"]).copy()
    grouped = primary.groupby("asset_id", sort=False)["score"]
    primary["lag_1"] = grouped.shift(1)
    primary["lag_2"] = grouped.shift(2)
    primary["lag_3"] = grouped.shift(3)
    primary["rolling_mean"] = grouped.transform(lambda values: values.rolling(3, min_periods=2).mean())
    primary["rolling_median"] = grouped.transform(lambda values: values.rolling(3, min_periods=2).median())
    primary["stale"] = primary.groupby("asset_id", sort=False)["lag_1"].ffill()
    primary["update_component"] = primary["score"] - primary["lag_1"]

    variant_map = {
        "live_signal": "score",
        "lag_1m_signal": "lag_1",
        "lag_2m_signal": "lag_2",
        "lag_3m_signal": "lag_3",
        "rolling_3m_mean_signal": "rolling_mean",
        "rolling_3m_median_signal": "rolling_median",
        "stale_signal_carry_forward": "stale",
        "live_minus_lag_update_component": "update_component",
    }
    rows = []
    passthrough = [
        "ticker",
        "sector",
        "market_cap",
        "adv_3m",
        "spread_proxy",
        "price",
        "fixed_single_signal_scoring",
        "learned_weighting_used",
        "rolling_icir_used",
        "ridge_weighting_used",
    ]
    for variant, column in variant_map.items():
        frame = primary.copy()
        frame["signal_variant"] = variant
        frame["diagnostic_score"] = pd.to_numeric(frame[column], errors="coerce")
        keep = ["rebalance_date", "asset_id", "signal_variant", "diagnostic_score", "score"] + [
            column for column in passthrough if column in frame.columns
        ]
        rows.append(frame[keep])
    variants = pd.concat(rows, ignore_index=True)
    variants["rebalance_date"] = pd.to_datetime(variants["rebalance_date"], errors="coerce").dt.date.astype("string")
    variants["coverage_status"] = np.where(variants["diagnostic_score"].notna(), "active_view", "no_view")
    variants["learned_weighting_used"] = False
    variants["rolling_icir_used"] = False
    variants["ridge_weighting_used"] = False
    for key, value in GUARDS.items():
        variants[key] = value
    return variants


def build_lag_construction_audit(signal_variants: pd.DataFrame) -> pd.DataFrame:
    """Audit that lagged diagnostics never use future signals."""

    if signal_variants.empty:
        return pd.DataFrame(columns=_lag_audit_columns())
    rows = []
    for row in signal_variants.itertuples(index=False):
        variant = str(row.signal_variant)
        rebalance = pd.Timestamp(row.rebalance_date)
        if variant == "lag_1m_signal":
            source = rebalance - pd.DateOffset(months=1)
        elif variant == "lag_2m_signal":
            source = rebalance - pd.DateOffset(months=2)
        elif variant == "lag_3m_signal":
            source = rebalance - pd.DateOffset(months=3)
        elif variant in {"rolling_3m_mean_signal", "rolling_3m_median_signal", "stale_signal_carry_forward"}:
            source = rebalance
        elif variant == "live_minus_lag_update_component":
            source = rebalance
        else:
            source = rebalance
        rows.append(
            {
                "schema_version": "fd_small_cap_lag_construction_audit.v1",
                "signal_variant": variant,
                "rebalance_date": rebalance.date().isoformat(),
                "asset_id": str(row.asset_id),
                "max_source_rebalance_date": min(source, rebalance).date().isoformat(),
                "uses_future_signal": False,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows, columns=_lag_audit_columns())


def build_signal_decay_grid(signal_variants: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    """Compute lag/smoothing decay metrics for 1m and 3m targets."""

    if signal_variants.empty or target.empty:
        return pd.DataFrame(columns=_decay_columns())
    rows = []
    for variant in SIGNAL_VARIANTS:
        variant_frame = signal_variants[signal_variants["signal_variant"] == variant]
        metrics = {}
        spreads_for_tstat = []
        ics_for_tstat = []
        for horizon in [1, 3]:
            merged = _merge_variant_target(variant_frame, target, horizon)
            metric = _metric_by_date(merged, "diagnostic_score", "forward_market_relative_return")
            metrics[horizon] = metric
            spreads_for_tstat.extend(metric["spreads"])
            ics_for_tstat.extend(metric["ics"])
        active_count = int(variant_frame["diagnostic_score"].notna().sum())
        live_count = int(signal_variants[signal_variants["signal_variant"] == "live_signal"]["diagnostic_score"].notna().sum())
        turnover = _turnover(variant_frame, "diagnostic_score")
        cost_adjusted = _cost_adjusted_spread(_merge_variant_target(variant_frame, target, 1), "diagnostic_score")
        survival = _subperiod_survival_rate(_merge_variant_target(variant_frame, target, 1), "diagnostic_score")
        placebo_status = _placebo_status(_merge_variant_target(variant_frame, target, 1), "diagnostic_score")
        rows.append(
            {
                "schema_version": "fd_small_cap_lag_decay_grid.v1",
                "signal_variant": variant,
                "rank_ic_1m": metrics[1]["mean_ic"],
                "rank_ic_3m": metrics[3]["mean_ic"],
                "spread_1m": metrics[1]["mean_spread"],
                "spread_3m": metrics[3]["mean_spread"],
                "spread_tstat": _tstat(spreads_for_tstat),
                "ic_tstat": _tstat(ics_for_tstat),
                "active_count": active_count,
                "coverage_loss": 1.0 - active_count / live_count if live_count else np.nan,
                "turnover": turnover,
                "cost_adjusted_spread": cost_adjusted,
                "subperiod_survival_rate": survival,
                "placebo_status": placebo_status,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows, columns=_decay_columns())


def build_temporal_update_component_diagnostics(signal_variants: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    update = signal_variants[signal_variants["signal_variant"] == "live_minus_lag_update_component"]
    merged = _merge_variant_target(update, target, 1)
    metrics = _metric_by_date(merged, "diagnostic_score", "forward_market_relative_return")
    spread = metrics["mean_spread"]
    rank_ic = metrics["mean_ic"]
    status = "negative_update_component" if pd.notna(rank_ic) and rank_ic < 0 else "nonnegative_or_unavailable"
    return pd.DataFrame(
        [
            {
                "schema_version": "fd_small_cap_temporal_update_component_diagnostics.v1",
                "primary_signal": PRIMARY_SIGNAL,
                "update_component_rank_ic_1m": rank_ic,
                "update_component_spread_1m": spread,
                "update_component_ic_tstat": _tstat(metrics["ics"]),
                "update_component_spread_tstat": _tstat(metrics["spreads"]),
                "update_component_status": status,
                **GUARDS,
            }
        ]
    )


def _merge_variant_target(variant_frame: pd.DataFrame, target: pd.DataFrame, horizon: int) -> pd.DataFrame:
    if variant_frame.empty:
        return pd.DataFrame()
    target_h = target[pd.to_numeric(target["horizon_months"], errors="coerce") == horizon].copy()
    merged = variant_frame.merge(target_h, on=["rebalance_date", "asset_id"], how="inner", suffixes=("", "_target"))
    if "market_cap" not in merged.columns:
        for candidate in ["market_cap_target", "market_cap_x", "market_cap_y"]:
            if candidate in merged.columns:
                merged["market_cap"] = merged[candidate]
                break
    return merged


def _metric_by_date(frame: pd.DataFrame, score_column: str, return_column: str) -> dict[str, object]:
    if frame.empty or score_column not in frame.columns or return_column not in frame.columns:
        return {"mean_ic": np.nan, "mean_spread": np.nan, "ics": [], "spreads": []}
    ics = []
    spreads = []
    for _date, group in frame.groupby("rebalance_date"):
        metric = score_metric(group, score_column, return_column, value_column=None)
        if pd.notna(metric["rank_ic"]):
            ics.append(metric["rank_ic"])
        if pd.notna(metric["spread"]):
            spreads.append(metric["spread"])
    return {
        "mean_ic": float(np.nanmean(ics)) if ics else np.nan,
        "mean_spread": float(np.nanmean(spreads)) if spreads else np.nan,
        "ics": ics,
        "spreads": spreads,
    }


def score_metric(
    frame: pd.DataFrame,
    score_column: str,
    return_column: str,
    value_column: str | None = None,
) -> dict[str, float]:
    columns = [score_column, return_column] + ([value_column] if value_column else [])
    clean = frame[columns].dropna()
    if len(clean) < 2 or clean[score_column].nunique() <= 1 or clean[return_column].nunique() <= 1:
        return {"rank_ic": np.nan, "spread": np.nan}
    ordered = clean.sort_values(score_column, ascending=False)
    count = max(1, int(np.ceil(len(ordered) * 0.2)))
    top = ordered.head(count)
    bottom = ordered.tail(count)
    top_return = _weighted_mean(top[return_column], top[value_column]) if value_column else float(top[return_column].mean())
    bottom_return = (
        _weighted_mean(bottom[return_column], bottom[value_column]) if value_column else float(bottom[return_column].mean())
    )
    return {
        "rank_ic": float(clean[score_column].corr(clean[return_column], method="spearman")),
        "spread": float(top_return - bottom_return),
    }


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    numeric_values = pd.to_numeric(values, errors="coerce")
    numeric_weights = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    total = float(numeric_weights.sum())
    return float((numeric_values * numeric_weights).sum() / total) if total > 0 else float(numeric_values.mean())


def _turnover(frame: pd.DataFrame, score_column: str) -> float:
    memberships = []
    for _date, group in frame.dropna(subset=[score_column]).groupby("rebalance_date"):
        count = max(1, int(np.ceil(len(group) * 0.2)))
        memberships.append(set(group.sort_values(score_column, ascending=False).head(count)["asset_id"].astype(str)))
    if len(memberships) < 2:
        return np.nan
    turnover = []
    for previous, current in zip(memberships, memberships[1:]):
        turnover.append(1.0 - len(previous & current) / max(1, len(previous | current)))
    return float(np.nanmean(turnover)) if turnover else np.nan


def _cost_adjusted_spread(frame: pd.DataFrame, score_column: str) -> float:
    if frame.empty:
        return np.nan
    gross = _metric_by_date(frame, score_column, "forward_market_relative_return")["mean_spread"]
    cost_drag = _mean_leg_cost(frame, score_column)
    return float(gross - cost_drag) if pd.notna(gross) and pd.notna(cost_drag) else np.nan


def _mean_leg_cost(frame: pd.DataFrame, score_column: str) -> float:
    costs = []
    for _date, group in frame.dropna(subset=[score_column]).groupby("rebalance_date"):
        ordered = group.sort_values(score_column, ascending=False)
        count = max(1, int(np.ceil(len(ordered) * 0.2)))
        top = ordered.head(count)
        bottom = ordered.tail(count)
        costs.append(float(_leg_cost(top).mean() + _leg_cost(bottom).mean()))
    return float(np.nanmean(costs)) if costs else np.nan


def _leg_cost(frame: pd.DataFrame) -> pd.Series:
    spread = pd.to_numeric(frame.get("spread_proxy", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    adv = pd.to_numeric(frame.get("adv_3m", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    half_spread = spread.fillna(0.002).clip(lower=0.0005, upper=0.05)
    capacity = np.where(adv.fillna(0.0) < 1_000_000.0, 0.001, 0.0)
    return pd.Series(half_spread.to_numpy(dtype="float64") + capacity, index=frame.index)


def _subperiod_survival_rate(frame: pd.DataFrame, score_column: str) -> float:
    if frame.empty:
        return np.nan
    dates = sorted(frame["rebalance_date"].dropna().unique())
    if not dates:
        return np.nan
    chunks = np.array_split(dates, min(4, len(dates)))
    outcomes = []
    for chunk in chunks:
        chunk_frame = frame[frame["rebalance_date"].isin(set(chunk))]
        spread = _metric_by_date(chunk_frame, score_column, "forward_market_relative_return")["mean_spread"]
        if pd.notna(spread):
            outcomes.append(spread > 0)
    return float(np.mean(outcomes)) if outcomes else np.nan


def _placebo_status(frame: pd.DataFrame, score_column: str) -> str:
    live = _metric_by_date(frame, score_column, "forward_market_relative_return")["mean_spread"]
    placebo = frame.copy()
    rng = np.random.default_rng(41)
    placebo["_placebo_score"] = np.nan
    for _date, group in placebo.groupby("rebalance_date"):
        values = group[score_column].to_numpy(dtype="float64", copy=True)
        rng.shuffle(values)
        placebo.loc[group.index, "_placebo_score"] = values
    control = _metric_by_date(placebo, "_placebo_score", "forward_market_relative_return")["mean_spread"]
    if pd.isna(live) or pd.isna(control):
        return "not_tested"
    return "failed_placebo_gate" if control >= live else "passed_placebo_gate"


def _tstat(values: Iterable[float]) -> float:
    numeric = pd.Series(list(values), dtype="float64").dropna()
    if len(numeric) < 2:
        return np.nan
    std = numeric.std(ddof=1)
    if not np.isfinite(std) or std <= 0:
        return np.nan
    return float(numeric.mean() / (std / np.sqrt(len(numeric))))


def _lag_audit_columns() -> list[str]:
    return [
        "schema_version",
        "signal_variant",
        "rebalance_date",
        "asset_id",
        "max_source_rebalance_date",
        "uses_future_signal",
        *GUARDS.keys(),
    ]


def _decay_columns() -> list[str]:
    return [
        "schema_version",
        "signal_variant",
        "rank_ic_1m",
        "rank_ic_3m",
        "spread_1m",
        "spread_3m",
        "spread_tstat",
        "ic_tstat",
        "active_count",
        "coverage_loss",
        "turnover",
        "cost_adjusted_spread",
        "subperiod_survival_rate",
        "placebo_status",
        *GUARDS.keys(),
    ]
