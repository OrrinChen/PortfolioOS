"""FD-S4.2 slow / capacity-filtered small-cap diagnostic rerun."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

from .small_cap_data_admission import GUARDS
from .small_cap_quality_family import PRIMARY_SIGNAL
from .small_cap_target_cache import (
    REQUIRED_TARGET_HORIZONS,
    build_forward_target_cache_from_manifest,
    write_forward_target_cache_from_panel,
)


S4_2_SIGNAL_VARIANTS = [
    "live_signal",
    "lag_1m_signal",
    "rolling_3m_mean_signal",
    "rolling_3m_median_signal",
]
S4_2_WEIGHTING_SCHEMES = [
    "equal_weight_within_bucket",
    "value_weight_within_bucket",
    "adv_weight_within_bucket",
    "capacity_capped_equal_weight",
    "capacity_capped_value_weight",
]
S4_2_FILTER = {
    "allowed_universe_tiers": ["small_cap_investable"],
    "adv_percentile_min": 0.67,
    "spread_percentile_max": 0.33,
    "min_price": 5.0,
}


@dataclass(frozen=True)
class FDSmallCapS42Result:
    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_cap_s4_2_diagnostic(
    source_signal_panel_path: str | Path,
    source_target_panel_path: str | Path,
    target_cache_output_dir: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    manifest_path: str | Path | None = None,
) -> FDSmallCapS42Result:
    """Run the preregistered S4.2 slow/capacity diagnostic."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path, report_file, Path(target_cache_output_dir))

    pre_registered = _write_pre_registered_filter_manifest(artifacts["pre_registered_filter_manifest"])
    source_target = _read_csv(Path(source_target_panel_path))
    if _has_required_horizons(source_target):
        target_artifacts = write_forward_target_cache_from_panel(source_target, target_cache_output_dir)
    elif manifest_path is not None and Path(manifest_path).exists():
        target_artifacts = build_forward_target_cache_from_manifest(manifest_path, target_cache_output_dir)
    else:
        target_artifacts = write_forward_target_cache_from_panel(source_target, target_cache_output_dir)
    artifacts["forward_returns_1m_3m_6m"] = target_artifacts["forward_returns"]
    artifacts["target_cache_audit"] = target_artifacts["audit"]

    signal_panel = _read_csv(Path(source_signal_panel_path))
    target = _read_csv(target_artifacts["forward_returns"])
    target_audit = json.loads(target_artifacts["audit"].read_text(encoding="utf-8"))
    variants = _slow_signal_variants(signal_panel)
    merged = _merge_variants_targets(variants, target)
    grid = _build_validation_grid(merged, pre_registered)
    oos = grid[grid["period"] == "test"].copy()
    placebo = _placebo_comparison(grid)
    cost_survival = _cost_adjusted_survival(grid)
    subperiod = _subperiod_survival(grid)
    decision = _decision(grid, target_audit, pre_registered, artifacts["pre_registered_filter_manifest"])

    grid.to_csv(artifacts["slow_signal_validation_grid"], index=False)
    oos.to_csv(artifacts["capacity_filtered_oos"], index=False)
    placebo.to_csv(artifacts["placebo_comparison"], index=False)
    cost_survival.to_csv(artifacts["cost_adjusted_survival"], index=False)
    subperiod.to_csv(artifacts["subperiod_survival"], index=False)
    artifacts["s4_2_decision"].write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["s4_2_report"].write_text(_render_report(decision, grid), encoding="utf-8")

    summary = {
        "schema_version": "fd_small_cap_s4_2_summary.v1",
        "stage": "FD-S4.2",
        "decision_label": decision["decision_label"],
        "six_month_target_available": decision["six_month_target_available"],
        **GUARDS,
    }
    return FDSmallCapS42Result(summary=summary, artifacts=artifacts)


def _write_pre_registered_filter_manifest(path: Path) -> dict[str, object]:
    payload = {
        "schema_version": "fd_small_cap_s4_2_pre_registered_filter_manifest.v1",
        "stage": "FD-S4.2",
        "filter_id": "small_cap_high_capacity_filter_v1",
        "filter_locked_before_evaluation": True,
        "manifest_written_before_evaluation": True,
        "allowed_universe_tiers": S4_2_FILTER["allowed_universe_tiers"],
        "adv_percentile_min": S4_2_FILTER["adv_percentile_min"],
        "spread_percentile_max": S4_2_FILTER["spread_percentile_max"],
        "min_price": S4_2_FILTER["min_price"],
        "excludes_microcap_quarantine": True,
        "excludes_low_adv_bucket": True,
        "excludes_wide_spread_bucket": True,
        "excludes_low_price_bucket": True,
        "changed_after_evaluation": False,
        **GUARDS,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _slow_signal_variants(signal_panel: pd.DataFrame) -> pd.DataFrame:
    if signal_panel.empty:
        return pd.DataFrame()
    primary = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    if primary.empty:
        return pd.DataFrame()
    primary["asset_id"] = primary["asset_id"].astype(str)
    primary["rebalance_date"] = pd.to_datetime(primary["rebalance_date"], errors="coerce")
    primary["score"] = pd.to_numeric(primary["score"], errors="coerce")
    primary = primary.sort_values(["asset_id", "rebalance_date"]).copy()
    grouped = primary.groupby("asset_id", sort=False)["score"]
    primary["lag_1m_signal"] = grouped.shift(1)
    primary["rolling_3m_mean_signal"] = grouped.transform(lambda values: values.rolling(3, min_periods=2).mean())
    primary["rolling_3m_median_signal"] = grouped.transform(lambda values: values.rolling(3, min_periods=2).median())
    primary["live_signal"] = primary["score"]
    rows = []
    passthrough = [
        "asset_id",
        "rebalance_date",
        "ticker",
        "sector",
        "universe_tier",
        "market_cap",
        "adv_3m",
        "spread_proxy",
        "beta_6m",
        "fixed_single_signal_scoring",
        "learned_weighting_used",
        "rolling_icir_used",
        "ridge_weighting_used",
    ]
    for variant in S4_2_SIGNAL_VARIANTS:
        frame = primary[[column for column in passthrough if column in primary.columns]].copy()
        frame["signal_variant"] = variant
        frame["diagnostic_score"] = pd.to_numeric(primary[variant], errors="coerce")
        rows.append(frame)
    variants = pd.concat(rows, ignore_index=True)
    variants["rebalance_date"] = pd.to_datetime(variants["rebalance_date"], errors="coerce").dt.date.astype("string")
    variants["coverage_status"] = np.where(variants["diagnostic_score"].notna(), "active_view", "no_view")
    variants["learned_weighting_used"] = False
    variants["rolling_icir_used"] = False
    variants["ridge_weighting_used"] = False
    for key, value in GUARDS.items():
        variants[key] = value
    return variants


def _merge_variants_targets(variants: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    if variants.empty or target.empty:
        return pd.DataFrame()
    left = variants.copy()
    right = target.copy()
    left["asset_id"] = left["asset_id"].astype(str)
    right["asset_id"] = right["asset_id"].astype(str)
    left["rebalance_date"] = pd.to_datetime(left["rebalance_date"], errors="coerce").dt.date.astype("string")
    right["rebalance_date"] = pd.to_datetime(right["rebalance_date"], errors="coerce").dt.date.astype("string")
    merged = left.merge(right, on=["rebalance_date", "asset_id"], how="inner", suffixes=("", "_target"))
    for column in ["market_cap", "price"]:
        if column not in merged.columns:
            for candidate in [f"{column}_target", f"{column}_x", f"{column}_y"]:
                if candidate in merged.columns:
                    merged[column] = merged[candidate]
                    break
    return merged


def _build_validation_grid(merged: pd.DataFrame, filter_manifest: Mapping[str, object]) -> pd.DataFrame:
    rows = []
    for variant in S4_2_SIGNAL_VARIANTS:
        for horizon in REQUIRED_TARGET_HORIZONS:
            for rebalance_frequency in ["monthly", "quarterly"]:
                for weighting_scheme in S4_2_WEIGHTING_SCHEMES:
                    base = _slice_grid_frame(merged, variant, horizon, rebalance_frequency)
                    filtered, exclusion_counts = _apply_capacity_filter(base, filter_manifest)
                    for period in ["test"]:
                        period_frame = filtered[filtered["period"].astype(str) == period]
                        base_period = base[base["period"].astype(str) == period]
                        metrics = _metrics(period_frame, base_period, weighting_scheme)
                        rows.append(
                            {
                                "schema_version": "fd_small_cap_s4_2_validation_grid.v1",
                                "stage": "FD-S4.2",
                                "signal_variant": variant,
                                "horizon_months": horizon,
                                "rebalance_frequency": rebalance_frequency,
                                "weighting_scheme": weighting_scheme,
                                "period": period,
                                **metrics,
                                **exclusion_counts,
                                "learned_weighting_used": False,
                                "rolling_icir_used": False,
                                "ridge_weighting_used": False,
                                **GUARDS,
                            }
                        )
    return pd.DataFrame(rows)


def _slice_grid_frame(merged: pd.DataFrame, variant: str, horizon: int, rebalance_frequency: str) -> pd.DataFrame:
    frame = merged[
        (merged["signal_variant"] == variant)
        & (pd.to_numeric(merged["horizon_months"], errors="coerce") == horizon)
        & (merged["diagnostic_score"].notna())
    ].copy()
    if rebalance_frequency == "quarterly" and not frame.empty:
        dates = sorted(frame["rebalance_date"].dropna().unique())
        quarterly_dates = set(dates[::3])
        frame = frame[frame["rebalance_date"].isin(quarterly_dates)].copy()
    return frame


def _apply_capacity_filter(frame: pd.DataFrame, filter_manifest: Mapping[str, object]) -> tuple[pd.DataFrame, dict[str, int]]:
    if frame.empty:
        return frame.copy(), _exclusion_counts(frame)
    output = frame.copy()
    allowed_tiers = set(filter_manifest.get("allowed_universe_tiers", ["small_cap_investable"]))
    universe_tier = output.get("universe_tier", pd.Series("", index=output.index))
    output["_passes_universe"] = universe_tier.astype(str).isin(allowed_tiers)
    output["_adv_threshold"] = output.groupby("rebalance_date")["adv_3m"].transform(
        lambda values: pd.to_numeric(values, errors="coerce").quantile(float(filter_manifest["adv_percentile_min"]))
    )
    output["_spread_threshold"] = output.groupby("rebalance_date")["spread_proxy"].transform(
        lambda values: pd.to_numeric(values, errors="coerce").quantile(float(filter_manifest["spread_percentile_max"]))
    )
    output["_passes_adv"] = pd.to_numeric(output["adv_3m"], errors="coerce") >= output["_adv_threshold"]
    output["_passes_spread"] = pd.to_numeric(output["spread_proxy"], errors="coerce") <= output["_spread_threshold"]
    price = output.get("price", pd.Series(np.nan, index=output.index))
    output["_passes_price"] = pd.to_numeric(price, errors="coerce") >= float(filter_manifest["min_price"])
    filtered = output[output["_passes_universe"] & output["_passes_adv"] & output["_passes_spread"] & output["_passes_price"]].copy()
    return filtered, _exclusion_counts(filtered)


def _exclusion_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {
            "microcap_rows_after_filter": 0,
            "low_adv_rows_after_filter": 0,
            "wide_spread_rows_after_filter": 0,
            "low_price_rows_after_filter": 0,
        }
    return {
        "microcap_rows_after_filter": int(
            (frame.get("universe_tier", pd.Series("", index=frame.index)).astype(str) == "microcap_quarantine").sum()
        ),
        "low_adv_rows_after_filter": int((frame.get("_passes_adv", True) == False).sum()),  # noqa: E712
        "wide_spread_rows_after_filter": int((frame.get("_passes_spread", True) == False).sum()),  # noqa: E712
        "low_price_rows_after_filter": int((frame.get("_passes_price", True) == False).sum()),  # noqa: E712
    }


def _metrics(frame: pd.DataFrame, base_frame: pd.DataFrame, weighting_scheme: str) -> dict[str, object]:
    date_metrics = []
    for _date, group in frame.groupby("rebalance_date"):
        date_metrics.append(_date_metric(group, weighting_scheme))
    spreads = [metric["gross_spread"] for metric in date_metrics if pd.notna(metric["gross_spread"])]
    rank_ics = [metric["rank_ic"] for metric in date_metrics if pd.notna(metric["rank_ic"])]
    cost_drags = [metric["cost_drag"] for metric in date_metrics if pd.notna(metric["cost_drag"])]
    capacity_penalties = [metric["capacity_penalty"] for metric in date_metrics if pd.notna(metric["capacity_penalty"])]
    gross = float(np.nanmean(spreads)) if spreads else np.nan
    cost_drag = float(np.nanmean(cost_drags)) if cost_drags else np.nan
    active_count = int(frame["diagnostic_score"].notna().sum()) if "diagnostic_score" in frame.columns else 0
    base_count = int(base_frame["diagnostic_score"].notna().sum()) if "diagnostic_score" in base_frame.columns else 0
    placebo_spread = _placebo_spread(frame, weighting_scheme)
    cost_adjusted = gross - cost_drag if pd.notna(gross) and pd.notna(cost_drag) else np.nan
    return {
        "gross_spread": gross,
        "cost_adjusted_spread": cost_adjusted,
        "rank_ic": float(np.nanmean(rank_ics)) if rank_ics else np.nan,
        "rank_ic_tstat": _tstat(rank_ics),
        "spread_tstat": _tstat(spreads),
        "turnover": _turnover(frame),
        "cost_drag": cost_drag,
        "capacity_penalty": float(np.nanmean(capacity_penalties)) if capacity_penalties else np.nan,
        "active_count": active_count,
        "coverage_loss": 1.0 - active_count / base_count if base_count else np.nan,
        "subperiod_survival_rate": _subperiod_survival_rate(frame, weighting_scheme),
        "placebo_status": "passed_placebo_gate" if pd.notna(placebo_spread) and pd.notna(gross) and gross > placebo_spread else "failed_or_unavailable_placebo_gate",
        "placebo_spread": placebo_spread,
        "exposure_to_size": _mean_corr(frame, "diagnostic_score", "market_cap"),
        "exposure_to_liquidity": _mean_corr(frame, "diagnostic_score", "adv_3m"),
        "market_beta": _mean_corr(frame, "diagnostic_score", "beta_6m"),
        "smb_beta": np.nan,
        "shortability_unknown": True,
    }


def _date_metric(group: pd.DataFrame, weighting_scheme: str) -> dict[str, float]:
    score = pd.to_numeric(group["diagnostic_score"], errors="coerce").to_numpy(dtype="float64")
    returns = pd.to_numeric(group["forward_market_relative_return"], errors="coerce").to_numpy(dtype="float64")
    valid = np.isfinite(score) & np.isfinite(returns)
    if valid.sum() < 2:
        return {"gross_spread": np.nan, "rank_ic": np.nan, "cost_drag": np.nan, "capacity_penalty": np.nan}
    score = score[valid]
    returns = returns[valid]
    if np.unique(score).size <= 1 or np.unique(returns).size <= 1:
        return {"gross_spread": np.nan, "rank_ic": np.nan, "cost_drag": np.nan, "capacity_penalty": np.nan}
    market_cap = _numeric_array(group, "market_cap")[valid]
    adv = _numeric_array(group, "adv_3m")[valid]
    spread = _numeric_array(group, "spread_proxy")[valid]
    order = np.argsort(score)[::-1]
    count = max(1, int(np.ceil(len(order) * 0.2)))
    top_index = order[:count]
    bottom_index = order[-count:]
    weights = _weights_array(market_cap, adv, weighting_scheme)
    costs = np.nan_to_num(spread, nan=0.002)
    costs = np.clip(costs, 0.0005, 0.05) + np.where(np.nan_to_num(adv, nan=0.0) < 1_000_000.0, 0.001, 0.0)
    top_return = _weighted_mean_array(returns[top_index], weights[top_index])
    bottom_return = _weighted_mean_array(returns[bottom_index], weights[bottom_index])
    top_cost = _weighted_mean_array(costs[top_index], weights[top_index])
    bottom_cost = _weighted_mean_array(costs[bottom_index], weights[bottom_index])
    capacity = np.where(np.nan_to_num(adv, nan=0.0) < 1_000_000.0, 0.001, 0.0)
    return {
        "gross_spread": float(top_return - bottom_return),
        "rank_ic": _rank_corr(score, returns),
        "cost_drag": float(top_cost + bottom_cost),
        "capacity_penalty": float(np.nanmean(capacity[top_index]) + np.nanmean(capacity[bottom_index])),
    }


def _weights_array(market_cap: np.ndarray, adv: np.ndarray, scheme: str) -> np.ndarray:
    if scheme == "equal_weight_within_bucket":
        return np.ones_like(market_cap, dtype="float64")
    if scheme == "value_weight_within_bucket":
        return np.clip(np.nan_to_num(market_cap, nan=0.0), 0.0, None)
    if scheme == "adv_weight_within_bucket":
        return np.clip(np.nan_to_num(adv, nan=0.0), 0.0, None)
    if scheme == "capacity_capped_equal_weight":
        return np.where(np.nan_to_num(adv, nan=0.0) > 0.0, 1.0, 0.0)
    if scheme == "capacity_capped_value_weight":
        value = np.clip(np.nan_to_num(market_cap, nan=0.0), 0.0, None)
        adv_value = np.clip(np.nan_to_num(adv, nan=0.0), 0.0, None)
        return np.minimum(value, adv_value * 100.0)
    return np.ones_like(market_cap, dtype="float64")


def _weighted_mean_array(values: np.ndarray, weights: np.ndarray) -> float:
    valid = np.isfinite(values)
    if not valid.any():
        return np.nan
    numeric_values = values[valid]
    numeric_weights = np.nan_to_num(weights[valid], nan=0.0)
    total = float(numeric_weights.sum())
    return float((numeric_values * numeric_weights).sum() / total) if total > 0.0 else float(np.nanmean(numeric_values))


def _placebo_spread(frame: pd.DataFrame, weighting_scheme: str) -> float:
    if frame.empty:
        return np.nan
    placebo = frame.copy()
    rng = np.random.default_rng(42)
    placebo["_placebo_score"] = np.nan
    for _date, group in placebo.groupby("rebalance_date"):
        values = group["diagnostic_score"].to_numpy(dtype="float64", copy=True)
        rng.shuffle(values)
        placebo.loc[group.index, "_placebo_score"] = values
    renamed = placebo.copy()
    renamed["diagnostic_score"] = renamed["_placebo_score"]
    values = [_date_metric(group, weighting_scheme)["gross_spread"] for _date, group in renamed.groupby("rebalance_date")]
    clean = [value for value in values if pd.notna(value)]
    return float(np.nanmean(clean)) if clean else np.nan


def _turnover(frame: pd.DataFrame) -> float:
    memberships = []
    for _date, group in frame.dropna(subset=["diagnostic_score"]).groupby("rebalance_date"):
        count = max(1, int(np.ceil(len(group) * 0.2)))
        memberships.append(set(group.sort_values("diagnostic_score", ascending=False).head(count)["asset_id"].astype(str)))
    if len(memberships) < 2:
        return np.nan
    values = [1.0 - len(prev & cur) / max(1, len(prev | cur)) for prev, cur in zip(memberships, memberships[1:])]
    return float(np.nanmean(values)) if values else np.nan


def _subperiod_survival_rate(frame: pd.DataFrame, weighting_scheme: str) -> float:
    if frame.empty:
        return np.nan
    dates = sorted(frame["rebalance_date"].dropna().unique())
    chunks = np.array_split(dates, min(4, len(dates)))
    outcomes = []
    for chunk in chunks:
        chunk_frame = frame[frame["rebalance_date"].isin(set(chunk))]
        values = [_date_metric(group, weighting_scheme) for _date, group in chunk_frame.groupby("rebalance_date")]
        gross = np.nanmean([value["gross_spread"] for value in values]) if values else np.nan
        cost = np.nanmean([value["cost_drag"] for value in values]) if values else np.nan
        if pd.notna(gross) and pd.notna(cost):
            outcomes.append(gross - cost > 0.0)
    return float(np.mean(outcomes)) if outcomes else np.nan


def _mean_corr(frame: pd.DataFrame, left: str, right: str) -> float:
    if frame.empty or right not in frame.columns:
        return np.nan
    values = []
    for _date, group in frame.groupby("rebalance_date"):
        left_values = pd.to_numeric(group[left], errors="coerce").to_numpy(dtype="float64")
        right_values = pd.to_numeric(group[right], errors="coerce").to_numpy(dtype="float64")
        valid = np.isfinite(left_values) & np.isfinite(right_values)
        if valid.sum() >= 3 and np.unique(left_values[valid]).size > 1 and np.unique(right_values[valid]).size > 1:
            values.append(_rank_corr(left_values[valid], right_values[valid]))
    return float(np.nanmean(values)) if values else np.nan


def _numeric_array(frame: pd.DataFrame, column: str) -> np.ndarray:
    if column not in frame.columns:
        return np.full(len(frame), np.nan, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype="float64")


def _rank_corr(left: np.ndarray, right: np.ndarray) -> float:
    left_rank = _ordinal_rank(left)
    right_rank = _ordinal_rank(right)
    left_std = float(left_rank.std())
    right_std = float(right_rank.std())
    if left_std <= 0.0 or right_std <= 0.0:
        return np.nan
    return float(np.corrcoef(left_rank, right_rank)[0, 1])


def _ordinal_rank(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(len(values), dtype="float64")
    ranks[order] = np.arange(len(values), dtype="float64")
    return ranks


def _tstat(values: list[float]) -> float:
    numeric = pd.Series(values, dtype="float64").dropna()
    if len(numeric) < 2:
        return np.nan
    std = numeric.std(ddof=1)
    return float(numeric.mean() / (std / np.sqrt(len(numeric)))) if np.isfinite(std) and std > 0.0 else np.nan


def _placebo_comparison(grid: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "signal_variant",
        "horizon_months",
        "rebalance_frequency",
        "weighting_scheme",
        "period",
        "gross_spread",
        "placebo_spread",
        "placebo_status",
        *GUARDS.keys(),
    ]
    return grid[columns].copy() if not grid.empty else pd.DataFrame(columns=columns)


def _cost_adjusted_survival(grid: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "signal_variant",
        "horizon_months",
        "rebalance_frequency",
        "weighting_scheme",
        "period",
        "gross_spread",
        "cost_adjusted_spread",
        "cost_drag",
        "capacity_penalty",
        "subperiod_survival_rate",
        "placebo_status",
        *GUARDS.keys(),
    ]
    return grid[columns].copy() if not grid.empty else pd.DataFrame(columns=columns)


def _subperiod_survival(grid: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "signal_variant",
        "horizon_months",
        "rebalance_frequency",
        "weighting_scheme",
        "period",
        "subperiod_survival_rate",
        *GUARDS.keys(),
    ]
    return grid[columns].copy() if not grid.empty else pd.DataFrame(columns=columns)


def _decision(
    grid: pd.DataFrame,
    target_audit: Mapping[str, object],
    filter_manifest: Mapping[str, object],
    filter_manifest_path: Path,
) -> dict[str, object]:
    six_available = _six_month_target_available(target_audit)
    test = grid[grid["period"] == "test"].copy() if not grid.empty else pd.DataFrame()
    best = _best_row(test)
    live_best = _best_row(test[test["signal_variant"] == "live_signal"]) if not test.empty else {}
    slow_best = _best_row(test[test["signal_variant"] != "live_signal"]) if not test.empty else {}
    lag_or_smoothed_beats_live = _value(slow_best, "cost_adjusted_spread") > _value(live_best, "cost_adjusted_spread")
    horizon_improves = _best_horizon(test, [3, 6]) > _best_horizon(test, [1])
    placebo_pass = str(best.get("placebo_status", "")).startswith("passed")
    cost_survives = _value(best, "cost_adjusted_spread") > 0.0
    subperiod_pass = _value(best, "subperiod_survival_rate") >= 0.5
    exposure_blocked = any(abs(_value(best, column)) > 0.5 for column in ["exposure_to_size", "exposure_to_liquidity", "market_beta"])
    shortability_unknown = bool(best.get("shortability_unknown", True))
    gross_survives = pd.to_numeric(test.get("gross_spread", pd.Series(dtype=float)), errors="coerce").max() > 0 if not test.empty else False
    all_cost_adjusted_fail = pd.to_numeric(test.get("cost_adjusted_spread", pd.Series(dtype=float)), errors="coerce").max() <= 0 if not test.empty else False

    if not six_available:
        label = "reject_temporal_noise_confirmed"
    elif gross_survives and all_cost_adjusted_fail:
        label = "diagnostic_only_cost_blocked"
    elif cost_survives and subperiod_pass and placebo_pass and exposure_blocked:
        label = "reject_capacity_filter_not_enough"
    elif (
        lag_or_smoothed_beats_live
        and horizon_improves
        and placebo_pass
        and cost_survives
        and subperiod_pass
        and not exposure_blocked
        and shortability_unknown
    ):
        label = "diagnostic_only_shortability_blocked"
    elif (
        lag_or_smoothed_beats_live
        and horizon_improves
        and placebo_pass
        and cost_survives
        and subperiod_pass
        and not exposure_blocked
        and not shortability_unknown
    ):
        label = "revise_to_pre_registered_v2_candidate"
    elif not lag_or_smoothed_beats_live:
        label = "reject_temporal_noise_confirmed"
    elif not placebo_pass or exposure_blocked:
        label = "reject_capacity_filter_not_enough"
    else:
        label = "close_family"

    return {
        "schema_version": "fd_small_cap_s4_2_decision.v1",
        "stage": "FD-S4.2",
        "decision_label": label,
        "six_month_target_available": six_available,
        "lag_or_smoothed_beats_live": lag_or_smoothed_beats_live,
        "holding_horizon_improves_cost_adjusted": horizon_improves,
        "best_cost_adjusted_spread": _value(best, "cost_adjusted_spread"),
        "best_subperiod_survival_rate": _value(best, "subperiod_survival_rate"),
        "best_placebo_status": best.get("placebo_status"),
        "exposure_blocked": exposure_blocked,
        "shortability_unknown": shortability_unknown,
        "pre_registered_filter_manifest": str(filter_manifest_path),
        "filter_locked_before_evaluation": bool(filter_manifest["filter_locked_before_evaluation"]),
        "learned_weighting_used": False,
        "rolling_icir_used": False,
        "ridge_weighting_used": False,
        **GUARDS,
    }


def _six_month_target_available(target_audit: Mapping[str, object]) -> bool:
    for row in target_audit.get("horizon_audit", []):
        if int(row.get("target_horizon", 0)) == 6:
            return int(row.get("available_row_count", 0)) > 0
    return False


def _best_row(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty or "cost_adjusted_spread" not in frame.columns:
        return {}
    values = pd.to_numeric(frame["cost_adjusted_spread"], errors="coerce")
    if values.dropna().empty:
        return {}
    return frame.loc[values.idxmax()].to_dict()


def _best_horizon(frame: pd.DataFrame, horizons: list[int]) -> float:
    if frame.empty:
        return np.nan
    selected = frame[frame["horizon_months"].isin(horizons)]
    return float(pd.to_numeric(selected["cost_adjusted_spread"], errors="coerce").max()) if not selected.empty else np.nan


def _value(row: Mapping[str, object], column: str) -> float:
    value = row.get(column, np.nan)
    return float(value) if pd.notna(value) else np.nan


def _has_required_horizons(target: pd.DataFrame) -> bool:
    if target.empty or "horizon_months" not in target.columns:
        return False
    available = set(pd.to_numeric(target["horizon_months"], errors="coerce").dropna().astype(int))
    return set(REQUIRED_TARGET_HORIZONS).issubset(available)


def _render_report(decision: Mapping[str, object], grid: pd.DataFrame) -> str:
    lines = [
        "# FD-S4.2 Small-Cap Slow / Capacity-Filtered Diagnostic",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- decision: {decision['decision_label']}",
        f"- six month target available: {str(decision['six_month_target_available']).lower()}",
        f"- learned weighting used: {str(decision['learned_weighting_used']).lower()}",
        "",
        "## Best Test Rows",
    ]
    test = grid[grid["period"] == "test"].copy() if not grid.empty else pd.DataFrame()
    if test.empty:
        lines.append("- unavailable")
    else:
        ranked = test.sort_values("cost_adjusted_spread", ascending=False).head(8)
        for row in ranked.itertuples(index=False):
            lines.append(
                f"- {row.signal_variant} {int(row.horizon_months)}m {row.rebalance_frequency} "
                f"{row.weighting_scheme}: gross={_fmt(row.gross_spread)}, net={_fmt(row.cost_adjusted_spread)}, "
                f"placebo={row.placebo_status}"
            )
    lines.append("")
    return "\n".join(lines)


def _fmt(value: object) -> str:
    return f"{float(value):.6f}" if pd.notna(value) else "unavailable"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _artifact_paths(output_dir: Path, report_file: Path, target_cache_dir: Path) -> dict[str, Path]:
    return {
        "pre_registered_filter_manifest": output_dir / "pre_registered_filter_manifest.json",
        "slow_signal_validation_grid": output_dir / "slow_signal_validation_grid.csv",
        "capacity_filtered_oos": output_dir / "capacity_filtered_oos.csv",
        "placebo_comparison": output_dir / "placebo_comparison.csv",
        "cost_adjusted_survival": output_dir / "cost_adjusted_survival.csv",
        "subperiod_survival": output_dir / "subperiod_survival.csv",
        "s4_2_decision": output_dir / "s4_2_decision.json",
        "s4_2_report": report_file,
        "forward_returns_1m_3m_6m": target_cache_dir / "forward_returns_1m_3m_6m.csv",
        "target_cache_audit": target_cache_dir / "target_cache_audit.json",
    }
