"""FD-S4.3 capacity-filtered live-signal preregistration gate."""

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
from .small_cap_s4_2_diagnostic import (
    S4_2_FILTER,
    _apply_capacity_filter,
    _date_metric,
    _mean_corr,
    _rank_corr,
    _slice_grid_frame,
    _subperiod_survival_rate,
    _tstat,
    _turnover,
)


S4_3_SIGNAL_VARIANT = "live_signal"
S4_3_HORIZON_MONTHS = 3
S4_3_REBALANCE_FREQUENCY = "quarterly"
S4_3_WEIGHTING_SCHEMES = ["adv_weight_within_bucket", "capacity_capped_equal_weight"]
S4_3_FORBIDDEN_SIGNAL_VARIANTS = {
    "lag_1m_signal",
    "lag_2m_signal",
    "lag_3m_signal",
    "rolling_3m_mean_signal",
    "rolling_3m_median_signal",
    "stale_signal_carry_forward",
}


@dataclass(frozen=True)
class FDSmallCapS43Result:
    """Artifacts and summary for FD-S4.3."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_cap_s4_3_preregistration(
    source_signal_panel_path: str | Path,
    source_target_cache_path: str | Path,
    s4_2_decision_path: str | Path,
    s4_2_grid_path: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
) -> FDSmallCapS43Result:
    """Run the locked S4.3 live-signal capacity-filter confirmation."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path, report_file)

    s4_2_decision = _read_json(Path(s4_2_decision_path))
    s4_2_grid = _read_csv(Path(s4_2_grid_path))
    slow_closeout = _write_slow_closeout(artifacts["slow_signal_closeout"], s4_2_decision, s4_2_grid)
    prereg = _write_capacity_filter_manifest(artifacts["capacity_filter_preregistration_manifest"])

    signal_panel = _read_csv(Path(source_signal_panel_path))
    target = _read_csv(Path(source_target_cache_path))
    live_variants = _live_signal_variant(signal_panel)
    merged = _merge_live_signal_targets(live_variants, target)
    base = _slice_grid_frame(merged, S4_3_SIGNAL_VARIANT, S4_3_HORIZON_MONTHS, S4_3_REBALANCE_FREQUENCY)
    filtered, exclusion_counts = _apply_capacity_filter(base, prereg)
    split_manifest = _build_confirmation_split_manifest(filtered)
    split_manifest.update(GUARDS)
    artifacts["confirmation_split_manifest"].write_text(
        json.dumps(split_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    confirmation_grid, placebo = _build_confirmation_outputs(base, filtered, split_manifest, exclusion_counts)
    oos = confirmation_grid[confirmation_grid["split_role"] == "confirmation"].copy()
    cost_survival = _cost_adjusted_survival(confirmation_grid)
    subperiod = _subperiod_survival(confirmation_grid)
    exposure = _exposure_attribution(confirmation_grid)
    shortability = _write_shortability_boundary(artifacts["shortability_boundary_report"])
    decision = _decision(confirmation_grid, placebo, split_manifest, shortability)

    oos.to_csv(artifacts["capacity_filtered_live_signal_oos"], index=False)
    confirmation_grid.to_csv(artifacts["fixed_weighting_confirmation"], index=False)
    placebo.to_csv(artifacts["placebo_comparison"], index=False)
    cost_survival.to_csv(artifacts["cost_adjusted_survival"], index=False)
    subperiod.to_csv(artifacts["subperiod_survival"], index=False)
    exposure.to_csv(artifacts["exposure_attribution"], index=False)
    artifacts["s4_3_decision"].write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["s4_3_report"].write_text(
        _render_report(decision, confirmation_grid, placebo, split_manifest, slow_closeout),
        encoding="utf-8",
    )

    summary = {
        "schema_version": "fd_small_cap_s4_3_summary.v1",
        "stage": "FD-S4.3",
        "decision_label": decision["decision_label"],
        "confirmation_available": split_manifest["confirmation_available"],
        **GUARDS,
    }
    return FDSmallCapS43Result(summary=summary, artifacts=artifacts)


def _write_slow_closeout(path: Path, s4_2_decision: Mapping[str, object], s4_2_grid: pd.DataFrame) -> dict[str, object]:
    live_best = _best_s4_2_value(s4_2_grid, "live_signal")
    lagged_best = _best_s4_2_value(s4_2_grid, "lag_1m_signal")
    smoothed_best = max(
        _best_s4_2_value(s4_2_grid, "rolling_3m_mean_signal"),
        _best_s4_2_value(s4_2_grid, "rolling_3m_median_signal"),
    )
    payload = {
        "schema_version": "fd_small_cap_s4_3_slow_signal_closeout.v1",
        "stage": "FD-S4.3",
        "s4_2_decision": str(s4_2_decision.get("decision_label", "reject_temporal_noise_confirmed")),
        "lagged_signal_beats_live_after_capacity_filter": bool(lagged_best > live_best),
        "smoothed_signal_beats_live_after_capacity_filter": bool(smoothed_best > live_best),
        "slow_signal_branch_closed": True,
        **GUARDS,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _best_s4_2_value(grid: pd.DataFrame, signal_variant: str) -> float:
    if grid.empty or "signal_variant" not in grid.columns or "cost_adjusted_spread" not in grid.columns:
        return -np.inf
    selected = grid[grid["signal_variant"].astype(str) == signal_variant]
    if selected.empty:
        return -np.inf
    values = pd.to_numeric(selected["cost_adjusted_spread"], errors="coerce").dropna()
    return float(values.max()) if not values.empty else -np.inf


def _write_capacity_filter_manifest(path: Path) -> dict[str, object]:
    payload = {
        "schema_version": "fd_small_cap_s4_3_capacity_filter_preregistration_manifest.v1",
        "stage": "FD-S4.3",
        "filter_id": "small_cap_capacity_filtered_live_signal_v1",
        "filter_locked_before_evaluation": True,
        "manifest_written_before_evaluation": True,
        "signal_variant": S4_3_SIGNAL_VARIANT,
        "target_horizon": "3m",
        "target_horizon_months": S4_3_HORIZON_MONTHS,
        "rebalance_frequency": S4_3_REBALANCE_FREQUENCY,
        "primary_weighting_scheme": "adv_weight_within_bucket",
        "control_weighting_scheme": "capacity_capped_equal_weight",
        "allowed_universe_tiers": S4_2_FILTER["allowed_universe_tiers"],
        "require_small_cap_investable": True,
        "adv_percentile_min": S4_2_FILTER["adv_percentile_min"],
        "spread_percentile_max": S4_2_FILTER["spread_percentile_max"],
        "min_price": S4_2_FILTER["min_price"],
        "threshold_rules": {
            "adv_3m": f">= datewise {S4_2_FILTER['adv_percentile_min']:.0%} percentile",
            "spread_proxy": f"<= datewise {S4_2_FILTER['spread_percentile_max']:.0%} percentile",
            "price": f">= {S4_2_FILTER['min_price']}",
        },
        "excludes_microcap_quarantine": True,
        "excludes_low_adv_bucket": True,
        "excludes_wide_spread_bucket": True,
        "excludes_low_price_bucket": True,
        "chosen_from_s4_2_diagnostic": True,
        "changed_after_evaluation": False,
        **GUARDS,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _live_signal_variant(signal_panel: pd.DataFrame) -> pd.DataFrame:
    if signal_panel.empty:
        return pd.DataFrame()
    primary = signal_panel[
        (signal_panel["signal_id"].astype(str) == PRIMARY_SIGNAL)
        & (signal_panel["coverage_status"].astype(str) == "active_view")
    ].copy()
    if primary.empty:
        return pd.DataFrame()
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
        "price",
        "fixed_single_signal_scoring",
        "learned_weighting_used",
        "rolling_icir_used",
        "ridge_weighting_used",
    ]
    output = primary[[column for column in passthrough if column in primary.columns]].copy()
    output["asset_id"] = output["asset_id"].astype(str)
    output["rebalance_date"] = pd.to_datetime(output["rebalance_date"], errors="coerce").dt.date.astype("string")
    output["signal_variant"] = S4_3_SIGNAL_VARIANT
    output["diagnostic_score"] = pd.to_numeric(primary["score"], errors="coerce")
    output["coverage_status"] = np.where(output["diagnostic_score"].notna(), "active_view", "no_view")
    output["learned_weighting_used"] = False
    output["rolling_icir_used"] = False
    output["ridge_weighting_used"] = False
    output["shrunk_icir_used"] = False
    for key, value in GUARDS.items():
        output[key] = value
    return output


def _merge_live_signal_targets(live_signal: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    if live_signal.empty or target.empty:
        return pd.DataFrame()
    left = live_signal.copy()
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


def _build_confirmation_split_manifest(filtered: pd.DataFrame) -> dict[str, object]:
    dates = sorted(pd.Series(filtered.get("rebalance_date", pd.Series(dtype=str))).dropna().astype(str).unique())
    if len(dates) < 4:
        return {
            "schema_version": "fd_small_cap_s4_3_confirmation_split_manifest.v1",
            "stage": "FD-S4.3",
            "confirmation_available": False,
            "split_method": "unavailable_insufficient_quarterly_dates",
            "hypothesis_generation_date_count": len(dates),
            "confirmation_date_count": 0,
            "hypothesis_generation_start": dates[0] if dates else None,
            "hypothesis_generation_end": dates[-1] if dates else None,
            "confirmation_start": None,
            "confirmation_end": None,
            "pre_register_capacity_filtered_v2_allowed": False,
        }
    split_index = max(1, len(dates) // 2)
    hypothesis_dates = dates[:split_index]
    confirmation_dates = dates[split_index:]
    return {
        "schema_version": "fd_small_cap_s4_3_confirmation_split_manifest.v1",
        "stage": "FD-S4.3",
        "confirmation_available": True,
        "split_method": "nested_time_split",
        "out_of_time_holdout_available": False,
        "hypothesis_generation_period": "S4.2 diagnostic and first nested S4.3 dates",
        "confirmation_period": "second nested S4.3 date block",
        "hypothesis_generation_dates": hypothesis_dates,
        "confirmation_dates": confirmation_dates,
        "hypothesis_generation_date_count": len(hypothesis_dates),
        "confirmation_date_count": len(confirmation_dates),
        "hypothesis_generation_start": hypothesis_dates[0],
        "hypothesis_generation_end": hypothesis_dates[-1],
        "confirmation_start": confirmation_dates[0],
        "confirmation_end": confirmation_dates[-1],
        "pre_register_capacity_filtered_v2_allowed": True,
    }


def _build_confirmation_outputs(
    base: pd.DataFrame,
    filtered: pd.DataFrame,
    split_manifest: Mapping[str, object],
    exclusion_counts: Mapping[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    placebo_rows: list[dict[str, object]] = []
    split_dates = {
        "hypothesis_generation": set(split_manifest.get("hypothesis_generation_dates", [])),
        "confirmation": set(split_manifest.get("confirmation_dates", [])),
    }
    if not split_dates["confirmation"]:
        split_dates["hypothesis_generation"] = set(pd.Series(filtered.get("rebalance_date", pd.Series(dtype=str))).astype(str))
    for split_role, dates in split_dates.items():
        if not dates:
            continue
        frame = filtered[filtered["rebalance_date"].astype(str).isin(dates)].copy()
        base_frame = base[base["rebalance_date"].astype(str).isin(dates)].copy()
        for weighting_scheme in S4_3_WEIGHTING_SCHEMES:
            placebos = _placebo_metrics(frame, weighting_scheme)
            metrics = _metrics(frame, base_frame, weighting_scheme, placebos)
            row = {
                "schema_version": "fd_small_cap_s4_3_confirmation.v1",
                "stage": "FD-S4.3",
                "signal_variant": S4_3_SIGNAL_VARIANT,
                "horizon_months": S4_3_HORIZON_MONTHS,
                "rebalance_frequency": S4_3_REBALANCE_FREQUENCY,
                "weighting_scheme": weighting_scheme,
                "split_role": split_role,
                "rolling_icir_used": False,
                "ridge_weighting_used": False,
                "shrunk_icir_used": False,
                "learned_weighting_used": False,
                **metrics,
                **exclusion_counts,
                **GUARDS,
            }
            rows.append(row)
            for placebo_type, placebo_metric in placebos.items():
                placebo_rows.append(
                    {
                        "schema_version": "fd_small_cap_s4_3_placebo_comparison.v1",
                        "stage": "FD-S4.3",
                        "signal_variant": S4_3_SIGNAL_VARIANT,
                        "horizon_months": S4_3_HORIZON_MONTHS,
                        "rebalance_frequency": S4_3_REBALANCE_FREQUENCY,
                        "weighting_scheme": weighting_scheme,
                        "split_role": split_role,
                        "placebo_type": placebo_type,
                        "live_gross_spread": metrics["gross_spread"],
                        "live_cost_adjusted_spread": metrics["cost_adjusted_spread"],
                        "placebo_gross_spread": placebo_metric["gross_spread"],
                        "placebo_cost_adjusted_spread": placebo_metric["cost_adjusted_spread"],
                        "placebo_status": placebo_metric["status"],
                        **GUARDS,
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(placebo_rows)


def _metrics(
    frame: pd.DataFrame,
    base_frame: pd.DataFrame,
    weighting_scheme: str,
    placebos: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    date_metrics = [_date_metric(group, weighting_scheme) for _date, group in frame.groupby("rebalance_date")]
    spreads = [metric["gross_spread"] for metric in date_metrics if pd.notna(metric["gross_spread"])]
    rank_ics = [metric["rank_ic"] for metric in date_metrics if pd.notna(metric["rank_ic"])]
    cost_drags = [metric["cost_drag"] for metric in date_metrics if pd.notna(metric["cost_drag"])]
    capacity_penalties = [metric["capacity_penalty"] for metric in date_metrics if pd.notna(metric["capacity_penalty"])]
    gross = float(np.nanmean(spreads)) if spreads else np.nan
    cost_drag = float(np.nanmean(cost_drags)) if cost_drags else np.nan
    cost_adjusted = gross - cost_drag if pd.notna(gross) and pd.notna(cost_drag) else np.nan
    active_count = int(frame["diagnostic_score"].notna().sum()) if "diagnostic_score" in frame.columns else 0
    base_count = int(base_frame["diagnostic_score"].notna().sum()) if "diagnostic_score" in base_frame.columns else 0
    sector_exposure = _sector_exposure(frame)
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
        "same_coverage_placebo_status": str(placebos["same_coverage_placebo"]["status"]),
        "capacity_matched_placebo_status": str(placebos["capacity_matched_placebo"]["status"]),
        "rebalance_shifted_placebo_status": str(placebos["rebalance_shifted_placebo"]["status"]),
        "market_beta": _mean_corr(frame, "diagnostic_score", "beta_6m"),
        "size_exposure": _mean_corr(frame, "diagnostic_score", "market_cap"),
        "liquidity_exposure": _mean_corr(frame, "diagnostic_score", "adv_3m"),
        "sector_exposure": sector_exposure,
        "shortability_unknown": True,
    }


def _placebo_metrics(frame: pd.DataFrame, weighting_scheme: str) -> dict[str, dict[str, object]]:
    live = _spread_metrics(frame, weighting_scheme)
    outputs = {}
    for placebo_type, placebo_frame in {
        "same_coverage_placebo": _same_coverage_placebo(frame),
        "capacity_matched_placebo": _capacity_matched_placebo(frame),
        "rebalance_shifted_placebo": _rebalance_shifted_placebo(frame),
    }.items():
        spread = _spread_metrics(placebo_frame, weighting_scheme)
        live_value = live["cost_adjusted_spread"]
        placebo_value = spread["cost_adjusted_spread"]
        status = (
            "passed_placebo_gate"
            if pd.notna(live_value) and pd.notna(placebo_value) and float(live_value) > float(placebo_value)
            else "failed_or_unavailable_placebo_gate"
        )
        outputs[placebo_type] = {
            "gross_spread": spread["gross_spread"],
            "cost_adjusted_spread": placebo_value,
            "status": status,
        }
    return outputs


def _spread_metrics(frame: pd.DataFrame, weighting_scheme: str) -> dict[str, float]:
    if frame.empty:
        return {"gross_spread": np.nan, "cost_adjusted_spread": np.nan}
    date_metrics = [_date_metric(group, weighting_scheme) for _date, group in frame.groupby("rebalance_date")]
    gross_values = [value["gross_spread"] for value in date_metrics if pd.notna(value["gross_spread"])]
    cost_values = [value["cost_drag"] for value in date_metrics if pd.notna(value["cost_drag"])]
    gross = float(np.nanmean(gross_values)) if gross_values else np.nan
    cost = float(np.nanmean(cost_values)) if cost_values else np.nan
    return {
        "gross_spread": gross,
        "cost_adjusted_spread": gross - cost if pd.notna(gross) and pd.notna(cost) else np.nan,
    }


def _same_coverage_placebo(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    placebo = frame.copy()
    rng = np.random.default_rng(43)
    for _date, group in placebo.groupby("rebalance_date"):
        values = group["diagnostic_score"].to_numpy(dtype="float64", copy=True)
        rng.shuffle(values)
        placebo.loc[group.index, "diagnostic_score"] = values
    return placebo


def _capacity_matched_placebo(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    placebo = frame.copy()
    rng = np.random.default_rng(44)
    for _date, date_group in placebo.groupby("rebalance_date"):
        bucket = _capacity_bucket(date_group)
        for _bucket, group in date_group.groupby(bucket):
            values = group["diagnostic_score"].to_numpy(dtype="float64", copy=True)
            rng.shuffle(values)
            placebo.loc[group.index, "diagnostic_score"] = values
    return placebo


def _capacity_bucket(group: pd.DataFrame) -> pd.Series:
    adv = pd.to_numeric(group.get("adv_3m", pd.Series(np.nan, index=group.index)), errors="coerce")
    if adv.nunique(dropna=True) < 3:
        return pd.Series("single_capacity_bucket", index=group.index)
    try:
        return pd.qcut(adv.rank(method="first"), q=3, labels=False, duplicates="drop").astype(str)
    except ValueError:
        return pd.Series("single_capacity_bucket", index=group.index)


def _rebalance_shifted_placebo(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    placebo = frame.copy().sort_values(["asset_id", "rebalance_date"])
    placebo["diagnostic_score"] = placebo.groupby("asset_id", sort=False)["diagnostic_score"].shift(1)
    return placebo.dropna(subset=["diagnostic_score"]).copy()


def _sector_exposure(frame: pd.DataFrame) -> float:
    if frame.empty or "sector" not in frame.columns:
        return np.nan
    exposures = []
    for _date, group in frame.dropna(subset=["diagnostic_score"]).groupby("rebalance_date"):
        count = max(1, int(np.ceil(len(group) * 0.2)))
        top = group.sort_values("diagnostic_score", ascending=False).head(count)
        sector_share = top["sector"].astype(str).value_counts(normalize=True)
        if not sector_share.empty:
            exposures.append(float(sector_share.iloc[0]))
    return float(np.nanmean(exposures)) if exposures else np.nan


def _cost_adjusted_survival(grid: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "signal_variant",
        "horizon_months",
        "rebalance_frequency",
        "weighting_scheme",
        "split_role",
        "gross_spread",
        "cost_adjusted_spread",
        "cost_drag",
        "capacity_penalty",
        "subperiod_survival_rate",
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
        "split_role",
        "subperiod_survival_rate",
        *GUARDS.keys(),
    ]
    return grid[columns].copy() if not grid.empty else pd.DataFrame(columns=columns)


def _exposure_attribution(grid: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "signal_variant",
        "horizon_months",
        "rebalance_frequency",
        "weighting_scheme",
        "split_role",
        "market_beta",
        "size_exposure",
        "liquidity_exposure",
        "sector_exposure",
        "shortability_unknown",
        *GUARDS.keys(),
    ]
    return grid[columns].copy() if not grid.empty else pd.DataFrame(columns=columns)


def _write_shortability_boundary(path: Path) -> dict[str, object]:
    payload = {
        "schema_version": "fd_small_cap_s4_3_shortability_boundary.v1",
        "stage": "FD-S4.3",
        "shortability_unknown": True,
        "borrow_data_available": False,
        "borrow_cost_available": False,
        "long_short_tradability_claimed": False,
        "diagnostic_only": True,
        **GUARDS,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _decision(
    grid: pd.DataFrame,
    placebo: pd.DataFrame,
    split_manifest: Mapping[str, object],
    shortability: Mapping[str, object],
) -> dict[str, object]:
    confirmation = grid[grid["split_role"] == "confirmation"].copy() if not grid.empty else pd.DataFrame()
    primary = _row_for(confirmation, "adv_weight_within_bucket")
    control = _row_for(confirmation, "capacity_capped_equal_weight")
    confirmation_available = bool(split_manifest.get("confirmation_available", False))
    cost_survives = _value(primary, "cost_adjusted_spread") > 0.0
    gross_survives = _value(primary, "gross_spread") > 0.0
    control_survives = _value(control, "cost_adjusted_spread") > 0.0
    same_coverage_pass = str(primary.get("same_coverage_placebo_status", "")).startswith("passed")
    capacity_matched_pass = str(primary.get("capacity_matched_placebo_status", "")).startswith("passed")
    rebalance_shifted_pass = str(primary.get("rebalance_shifted_placebo_status", "")).startswith("passed")
    subperiod_pass = _value(primary, "subperiod_survival_rate") >= 0.5
    exposure_blocked = _exposure_blocked(primary)
    shortability_unknown = bool(shortability.get("shortability_unknown", True))
    placebo_statuses = {
        "same_coverage_placebo_status": primary.get("same_coverage_placebo_status"),
        "capacity_matched_placebo_status": primary.get("capacity_matched_placebo_status"),
        "rebalance_shifted_placebo_status": primary.get("rebalance_shifted_placebo_status"),
    }

    if not confirmation_available:
        label = "diagnostic_only_replicated" if gross_survives or cost_survives else "close_family"
    elif not capacity_matched_pass or exposure_blocked:
        label = "reject_capacity_filter_hypothesis"
    elif gross_survives and not cost_survives:
        label = "diagnostic_only_cost_blocked"
    elif (
        cost_survives
        and control_survives
        and same_coverage_pass
        and capacity_matched_pass
        and rebalance_shifted_pass
        and subperiod_pass
        and shortability_unknown
    ):
        label = "diagnostic_only_shortability_blocked"
    elif (
        cost_survives
        and control_survives
        and same_coverage_pass
        and capacity_matched_pass
        and rebalance_shifted_pass
        and subperiod_pass
        and not shortability_unknown
    ):
        label = "pre_register_capacity_filtered_v2"
    else:
        label = "close_family"

    return {
        "schema_version": "fd_small_cap_s4_3_decision.v1",
        "stage": "FD-S4.3",
        "decision_label": label,
        "confirmation_available": confirmation_available,
        "signal_variant": S4_3_SIGNAL_VARIANT,
        "target_horizon": "3m",
        "rebalance_frequency": S4_3_REBALANCE_FREQUENCY,
        "primary_weighting_scheme": "adv_weight_within_bucket",
        "control_weighting_scheme": "capacity_capped_equal_weight",
        "primary_cost_adjusted_spread": _value(primary, "cost_adjusted_spread"),
        "control_cost_adjusted_spread": _value(control, "cost_adjusted_spread"),
        "gross_spread": _value(primary, "gross_spread"),
        "subperiod_survival_rate": _value(primary, "subperiod_survival_rate"),
        "same_coverage_placebo_passed": same_coverage_pass,
        "capacity_matched_placebo_passed": capacity_matched_pass,
        "rebalance_shifted_placebo_passed": rebalance_shifted_pass,
        "placebo_statuses": placebo_statuses,
        "placebo_row_count": int(len(placebo)),
        "exposure_blocked": exposure_blocked,
        "shortability_unknown": shortability_unknown,
        "learned_weighting_used": False,
        "rolling_icir_used": False,
        "ridge_weighting_used": False,
        "shrunk_icir_used": False,
        **GUARDS,
    }


def _row_for(frame: pd.DataFrame, weighting_scheme: str) -> dict[str, object]:
    if frame.empty or "weighting_scheme" not in frame.columns:
        return {}
    selected = frame[frame["weighting_scheme"].astype(str) == weighting_scheme]
    if selected.empty:
        return {}
    return selected.iloc[0].to_dict()


def _exposure_blocked(row: Mapping[str, object]) -> bool:
    if not row:
        return False
    numeric_block = any(abs(_value(row, column)) > 0.5 for column in ["market_beta", "size_exposure", "liquidity_exposure"])
    sector_value = _value(row, "sector_exposure")
    sector_block = pd.notna(sector_value) and sector_value > 0.85
    return bool(numeric_block or sector_block)


def _value(row: Mapping[str, object], column: str) -> float:
    value = row.get(column, np.nan)
    return float(value) if pd.notna(value) else np.nan


def _render_report(
    decision: Mapping[str, object],
    grid: pd.DataFrame,
    placebo: pd.DataFrame,
    split_manifest: Mapping[str, object],
    slow_closeout: Mapping[str, object],
) -> str:
    lines = [
        "# FD-S4.3 Small-Cap Capacity-Filtered Live Signal Preregistration Gate",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- decision: {decision['decision_label']}",
        f"- slow signal branch closed: {str(slow_closeout['slow_signal_branch_closed']).lower()}",
        f"- confirmation available: {str(split_manifest['confirmation_available']).lower()}",
        f"- split method: {split_manifest['split_method']}",
        f"- signal: {decision['signal_variant']} / {decision['target_horizon']} / {decision['rebalance_frequency']}",
        f"- shortability unknown: {str(decision['shortability_unknown']).lower()}",
        "",
        "## Confirmation Rows",
    ]
    if grid.empty:
        lines.append("- unavailable")
    else:
        for row in grid.sort_values(["split_role", "weighting_scheme"]).itertuples(index=False):
            lines.append(
                f"- {row.split_role} {row.weighting_scheme}: gross={_fmt(row.gross_spread)}, "
                f"net={_fmt(row.cost_adjusted_spread)}, subperiod={_fmt(row.subperiod_survival_rate)}, "
                f"placebos={row.same_coverage_placebo_status}/{row.capacity_matched_placebo_status}/"
                f"{row.rebalance_shifted_placebo_status}"
            )
    lines.extend(["", "## Placebos"])
    if placebo.empty:
        lines.append("- unavailable")
    else:
        for row in placebo[placebo["split_role"] == "confirmation"].itertuples(index=False):
            lines.append(
                f"- {row.weighting_scheme} {row.placebo_type}: live={_fmt(row.live_cost_adjusted_spread)}, "
                f"placebo={_fmt(row.placebo_cost_adjusted_spread)}, status={row.placebo_status}"
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


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_paths(output_dir: Path, report_file: Path) -> dict[str, Path]:
    return {
        "slow_signal_closeout": output_dir / "slow_signal_closeout.json",
        "capacity_filter_preregistration_manifest": output_dir / "capacity_filter_preregistration_manifest.json",
        "confirmation_split_manifest": output_dir / "confirmation_split_manifest.json",
        "capacity_filtered_live_signal_oos": output_dir / "capacity_filtered_live_signal_oos.csv",
        "fixed_weighting_confirmation": output_dir / "fixed_weighting_confirmation.csv",
        "placebo_comparison": output_dir / "placebo_comparison.csv",
        "cost_adjusted_survival": output_dir / "cost_adjusted_survival.csv",
        "subperiod_survival": output_dir / "subperiod_survival.csv",
        "exposure_attribution": output_dir / "exposure_attribution.csv",
        "shortability_boundary_report": output_dir / "shortability_boundary_report.json",
        "s4_3_decision": output_dir / "s4_3_decision.json",
        "s4_3_report": report_file,
    }
