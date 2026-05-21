"""Capacity and fixed-weight diagnostics for FD-S4.1."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .small_cap_data_admission import GUARDS
from .small_cap_temporal_diagnostics import PRIMARY_SIGNAL, _leg_cost, _metric_by_date, _tstat, score_metric


FIXED_WEIGHTING_SCHEMES = [
    "equal_weight",
    "value_weight",
    "sqrt_market_cap_weight",
    "adv_weight",
    "capacity_capped_equal_weight",
    "capacity_capped_value_weight",
]


def primary_merge(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    primary = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    if primary.empty or target.empty:
        return pd.DataFrame()
    merged = primary.merge(target, on=["rebalance_date", "asset_id"], how="inner", suffixes=("", "_target"))
    if "market_cap" not in merged.columns:
        for candidate in ["market_cap_target", "market_cap_x", "market_cap_y"]:
            if candidate in merged.columns:
                merged["market_cap"] = merged[candidate]
                break
    return merged


def build_holding_period_sensitivity(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    merged = primary_merge(signal_panel, target)
    rows = []
    for rebalance_frequency in ["monthly", "quarterly"]:
        frame = _quarterly_only(merged) if rebalance_frequency == "quarterly" else merged
        for horizon in [1, 3, 6]:
            horizon_frame = frame[pd.to_numeric(frame["horizon_months"], errors="coerce") == horizon].copy()
            gross = _metric_by_date(horizon_frame, "score", "forward_market_relative_return")["mean_spread"]
            rank_ic = _metric_by_date(horizon_frame, "score", "forward_market_relative_return")["mean_ic"]
            turnover = _turnover(horizon_frame, "score")
            cost_drag = _mean_cost_drag(horizon_frame, "score")
            capacity_penalty = _mean_capacity_penalty(horizon_frame)
            rows.append(
                {
                    "schema_version": "fd_small_cap_holding_period_sensitivity.v1",
                    "rebalance_frequency": rebalance_frequency,
                    "holding_period_months": horizon,
                    "gross_spread": gross,
                    "net_spread": gross - cost_drag if pd.notna(gross) and pd.notna(cost_drag) else np.nan,
                    "turnover": turnover,
                    "cost_drag": cost_drag,
                    "capacity_penalty": capacity_penalty,
                    "rank_ic": rank_ic,
                    "subperiod_survival_rate": _subperiod_survival(horizon_frame),
                    **GUARDS,
                }
            )
    return pd.DataFrame(rows)


def build_capacity_bucket_diagnostics(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    merged = primary_merge(signal_panel, target)
    merged = merged[pd.to_numeric(merged.get("horizon_months", np.nan), errors="coerce") == 1].copy()
    rows = []
    bucket_specs = {
        "market_cap": "market_cap",
        "adv": "adv_3m",
        "spread": "spread_proxy",
        "price": "price",
    }
    for bucket_type, column in bucket_specs.items():
        bucketed = _assign_bucket(merged, column, bucket_type)
        for bucket_label, group in bucketed.groupby("bucket_label", dropna=False, observed=False):
            equal_spread = _metric_by_date(group, "score", "forward_market_relative_return")["mean_spread"]
            cost_drag = _mean_cost_drag(group, "score")
            rows.append(
                {
                    "schema_version": "fd_small_cap_capacity_bucket_diagnostics.v1",
                    "bucket_type": bucket_type,
                    "bucket_label": str(bucket_label),
                    "equal_weight_spread": equal_spread,
                    "value_weight_spread": _weighted_spread(group, "market_cap"),
                    "adv_weight_spread": _weighted_spread(group, "adv_3m"),
                    "capacity_weight_spread": _weighted_spread(group, "capacity_weight"),
                    "gross_spread": equal_spread,
                    "net_spread": equal_spread - cost_drag if pd.notna(equal_spread) and pd.notna(cost_drag) else np.nan,
                    "cost_drag": cost_drag,
                    "turnover": _turnover(group, "score"),
                    "active_count": int(group["score"].notna().sum()),
                    **GUARDS,
                }
            )
    return pd.DataFrame(rows)


def build_weighting_scheme_comparison(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    merged = primary_merge(signal_panel, target)
    merged = merged[pd.to_numeric(merged.get("horizon_months", np.nan), errors="coerce") == 1].copy()
    rows = []
    for scheme in FIXED_WEIGHTING_SCHEMES:
        frame = merged.copy()
        frame["_scheme_weight"] = _scheme_weights(frame, scheme)
        gross = _weighted_spread(frame, "_scheme_weight")
        cost_drag = _mean_cost_drag(frame, "score")
        rows.append(
            {
                "schema_version": "fd_small_cap_weighting_scheme_comparison.v1",
                "weighting_scheme": scheme,
                "gross_spread": gross,
                "net_spread": gross - cost_drag if pd.notna(gross) and pd.notna(cost_drag) else np.nan,
                "rank_ic": _metric_by_date(frame, "score", "forward_market_relative_return")["mean_ic"],
                "turnover": _turnover(frame, "score"),
                "learned_weighting_used": False,
                "rolling_icir_used": False,
                "ridge_weighting_used": False,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows)


def build_cost_drag_decomposition(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    merged = primary_merge(signal_panel, target)
    merged = merged[pd.to_numeric(merged.get("horizon_months", np.nan), errors="coerce") == 1].copy()
    gross = _metric_by_date(merged, "score", "forward_market_relative_return")["mean_spread"]
    half_spread = _mean_half_spread_cost(merged)
    turnover_cost = _turnover(merged, "score") * 0.001 if not merged.empty else np.nan
    capacity_penalty = _mean_capacity_penalty(merged)
    impact = _mean_impact_cost(merged)
    total_cost = np.nansum([half_spread, turnover_cost, capacity_penalty, impact])
    return pd.DataFrame(
        [
            {
                "schema_version": "fd_small_cap_cost_drag_decomposition.v1",
                "gross_spread": gross,
                "half_spread_cost": half_spread,
                "turnover_cost": turnover_cost,
                "ADV_capacity_penalty": capacity_penalty,
                "estimated_impact_cost": impact,
                "borrow_or_short_constraint_proxy": np.nan,
                "net_spread": gross - total_cost if pd.notna(gross) else np.nan,
                "spread_proxy_used": True,
                "cost_evidence_quality": "spread_adv_proxy",
                "shortability_unknown": True,
                **GUARDS,
            }
        ]
    )


def _assign_bucket(frame: pd.DataFrame, column: str, bucket_type: str) -> pd.DataFrame:
    output = frame.copy()
    if column not in output.columns or output[column].notna().sum() < 3:
        output["bucket_label"] = f"{bucket_type}_unavailable"
    else:
        output["bucket_label"] = pd.qcut(output[column], 3, labels=["low", "mid", "high"], duplicates="drop")
    output["capacity_weight"] = pd.to_numeric(output.get("adv_3m", 0.0), errors="coerce").clip(upper=2_000_000.0)
    return output


def _scheme_weights(frame: pd.DataFrame, scheme: str) -> pd.Series:
    if scheme == "equal_weight":
        return pd.Series(1.0, index=frame.index)
    if scheme == "value_weight":
        return pd.to_numeric(frame.get("market_cap"), errors="coerce").clip(lower=0.0)
    if scheme == "sqrt_market_cap_weight":
        return np.sqrt(pd.to_numeric(frame.get("market_cap"), errors="coerce").clip(lower=0.0))
    if scheme == "adv_weight":
        return pd.to_numeric(frame.get("adv_3m"), errors="coerce").clip(lower=0.0)
    if scheme == "capacity_capped_equal_weight":
        cap = pd.to_numeric(frame.get("adv_3m"), errors="coerce").clip(lower=0.0, upper=2_000_000.0)
        return pd.Series(1.0, index=frame.index).where(cap > 0.0, 0.0)
    if scheme == "capacity_capped_value_weight":
        cap = pd.to_numeric(frame.get("adv_3m"), errors="coerce").clip(lower=0.0, upper=2_000_000.0)
        value = pd.to_numeric(frame.get("market_cap"), errors="coerce").clip(lower=0.0)
        return np.minimum(value, cap * 100.0)
    return pd.Series(1.0, index=frame.index)


def _weighted_spread(frame: pd.DataFrame, weight_column: str) -> float:
    spreads = []
    for _date, group in frame.dropna(subset=["score", "forward_market_relative_return"]).groupby("rebalance_date"):
        clean = group.copy()
        if weight_column not in clean.columns:
            clean[weight_column] = pd.to_numeric(clean.get(weight_column), errors="coerce")
        metric = score_metric(clean, "score", "forward_market_relative_return", value_column=weight_column)
        if pd.notna(metric["spread"]):
            spreads.append(metric["spread"])
    return float(np.nanmean(spreads)) if spreads else np.nan


def _quarterly_only(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    dates = sorted(frame["rebalance_date"].dropna().unique())
    quarterly_dates = set(dates[::3])
    return frame[frame["rebalance_date"].isin(quarterly_dates)].copy()


def _turnover(frame: pd.DataFrame, score_column: str) -> float:
    memberships = []
    for _date, group in frame.dropna(subset=[score_column]).groupby("rebalance_date"):
        count = max(1, int(np.ceil(len(group) * 0.2)))
        memberships.append(set(group.sort_values(score_column, ascending=False).head(count)["asset_id"].astype(str)))
    if len(memberships) < 2:
        return np.nan
    values = [1.0 - len(prev & cur) / max(1, len(prev | cur)) for prev, cur in zip(memberships, memberships[1:])]
    return float(np.nanmean(values)) if values else np.nan


def _mean_cost_drag(frame: pd.DataFrame, score_column: str) -> float:
    costs = []
    for _date, group in frame.dropna(subset=[score_column]).groupby("rebalance_date"):
        ordered = group.sort_values(score_column, ascending=False)
        count = max(1, int(np.ceil(len(ordered) * 0.2)))
        costs.append(float(_leg_cost(ordered.head(count)).mean() + _leg_cost(ordered.tail(count)).mean()))
    return float(np.nanmean(costs)) if costs else np.nan


def _mean_half_spread_cost(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    spread = pd.to_numeric(frame.get("spread_proxy", pd.Series(dtype=float)), errors="coerce").clip(lower=0.0005)
    return float(spread.mean()) if spread.notna().any() else np.nan


def _mean_capacity_penalty(frame: pd.DataFrame) -> float:
    if frame.empty or "adv_3m" not in frame.columns:
        return np.nan
    adv = pd.to_numeric(frame["adv_3m"], errors="coerce")
    return float((adv < 1_000_000.0).mean() * 0.001) if adv.notna().any() else np.nan


def _mean_impact_cost(frame: pd.DataFrame) -> float:
    if frame.empty or "adv_3m" not in frame.columns:
        return np.nan
    adv = pd.to_numeric(frame["adv_3m"], errors="coerce")
    impact = np.where(adv.fillna(0.0) > 0, np.minimum(0.002, 50_000.0 / adv.fillna(np.nan) * 0.0005), np.nan)
    return float(np.nanmean(impact)) if np.isfinite(impact).any() else np.nan


def _subperiod_survival(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    dates = sorted(frame["rebalance_date"].dropna().unique())
    chunks = np.array_split(dates, min(4, len(dates)))
    outcomes = []
    for chunk in chunks:
        chunk_frame = frame[frame["rebalance_date"].isin(set(chunk))]
        spread = _metric_by_date(chunk_frame, "score", "forward_market_relative_return")["mean_spread"]
        if pd.notna(spread):
            outcomes.append(spread > 0)
    return float(np.mean(outcomes)) if outcomes else np.nan
