"""Locked validation for the E1 full-market small-emotion top pocket."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS
from .small_emotion_full_market_overfit_lab import _apply_candidate_event_filters, _leaf_search


STAGE = "SMALL-EMOTION-FREEZE-02"
ALLOWED_DECISIONS = {
    "promote_to_q2_candidate",
    "locked_oos_pass_cost_pending",
    "cost_liquidity_failed",
    "selection_bias_failed",
    "stale_or_bad_print_failed",
    "locked_oos_failed",
}


@dataclass(frozen=True)
class SmallEmotionFreezeValidationResult:
    """Artifacts and summary for SMALL-EMOTION-FREEZE-02."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_freeze_validation(
    *,
    top_pockets_path: str | Path,
    search_grid_path: str | Path,
    feature_cache_dir: str | Path,
    prior_measurement_spec_path: str | Path,
    prior_measurement_spec_hash: str,
    output_dir: str | Path,
    random_seed: int = 20260517,
    min_events: int = 50,
    min_event_months: int = 6,
    excluded_predicates: tuple[str, ...] | list[str] = (),
    exclude_stale_price_events: bool = False,
) -> SmallEmotionFreezeValidationResult:
    """Freeze the E1 full-market top pocket and run locked validation audits."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    excluded_predicates_list = sorted({str(value).strip() for value in excluded_predicates if str(value).strip()})

    top = pd.read_csv(top_pockets_path)
    grid = pd.read_csv(search_grid_path) if Path(search_grid_path).exists() else pd.DataFrame()
    pocket = _selected_pocket(top)
    pocket["candidate_id"] = _candidate_id(pocket)
    event_labels_raw = _read_event_labels(Path(feature_cache_dir), str(pocket["window"]))
    event_labels, candidate_filter_report = _apply_candidate_event_filters(
        event_labels_raw,
        exclude_stale_price_events=exclude_stale_price_events,
    )
    selected = _select_frame(event_labels, pocket).copy()
    selected["directional_return"] = _directional_return(selected, str(pocket["mechanism"]))

    charter = _d3_charter(pocket)
    artifacts["d3_candidate_charter"].write_text(
        yaml.safe_dump(charter, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    spec = _measurement_spec(charter)
    artifacts["measurement_spec"].write_text(
        yaml.safe_dump(spec, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    new_spec_hash = _file_hash(artifacts["measurement_spec"])
    prior_hash_observed = _file_hash(Path(prior_measurement_spec_path)) if Path(prior_measurement_spec_path).exists() else ""
    prior_identical = bool(new_spec_hash == prior_measurement_spec_hash)
    reconciliation = {
        "schema_version": "small_emotion_freeze_02_spec_reconciliation.v1",
        "stage": STAGE,
        "prior_measurement_spec_hash_required": prior_measurement_spec_hash,
        "prior_measurement_spec_hash_observed": prior_hash_observed,
        "new_measurement_spec_hash": new_spec_hash,
        "prior_spec_identical": prior_identical,
        "new_d3_charter_written": not prior_identical,
        "new_measurement_spec_written": not prior_identical,
        "candidate_id": pocket["candidate_id"],
        "frozen_mechanism": pocket["mechanism"],
        "frozen_window": pocket["window"],
        "frozen_path_predicates": pocket["path_predicates"],
        "excluded_predicates_for_placebo_selection": excluded_predicates_list,
        **candidate_filter_report,
        **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }
    artifacts["spec_reconciliation"].write_text(
        json.dumps(reconciliation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    selected.to_csv(artifacts["locked_event_panel"], index=False)
    split_metrics = _temporal_split_metrics(selected)
    split_metrics.to_csv(artifacts["temporal_split_metrics"], index=False)

    placebo_top = _placebo_top_pockets(
        event_labels=event_labels,
        selected=selected,
        grid=grid,
        random_seed=random_seed,
        min_events=min_events,
        min_event_months=min_event_months,
        excluded_predicates=excluded_predicates_list,
    )
    placebo_top.to_csv(artifacts["placebo_top_pockets"], index=False)
    sweep = _sweep_adjusted_selection_audit(
        grid=grid,
        selected=selected,
        placebo_top=placebo_top,
        pocket=pocket,
    )
    artifacts["sweep_adjusted_selection_audit"].write_text(
        json.dumps(sweep, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    cost = _cost_liquidity_gate(selected)
    cost.to_csv(artifacts["cost_liquidity_gate"], index=False)
    capacity = _capacity_frontier(selected)
    capacity.to_csv(artifacts["capacity_frontier"], index=False)
    anomaly = _data_anomaly_audit(selected)
    anomaly.to_csv(artifacts["data_anomaly_audit"], index=False)

    decision, reason = _decision(split_metrics, sweep, cost, capacity, anomaly)
    summary = _summary(
        reconciliation=reconciliation,
        selected=selected,
        split_metrics=split_metrics,
        sweep=sweep,
        cost=cost,
        capacity=capacity,
        anomaly=anomaly,
        decision=decision,
        reason=reason,
    )
    artifacts["decision_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["freeze_report"].write_text(_report(summary), encoding="utf-8")
    return SmallEmotionFreezeValidationResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "spec_reconciliation": output_path / "spec_reconciliation.json",
        "d3_candidate_charter": output_path / "d3_candidate_charter.yaml",
        "measurement_spec": output_path / "measurement_spec.yaml",
        "locked_event_panel": output_path / "locked_event_panel.csv",
        "temporal_split_metrics": output_path / "temporal_split_metrics.csv",
        "sweep_adjusted_selection_audit": output_path / "sweep_adjusted_selection_audit.json",
        "placebo_top_pockets": output_path / "placebo_top_pockets.csv",
        "cost_liquidity_gate": output_path / "cost_liquidity_gate.csv",
        "capacity_frontier": output_path / "capacity_frontier.csv",
        "data_anomaly_audit": output_path / "data_anomaly_audit.csv",
        "decision_summary": output_path / "freeze_decision_summary.json",
        "freeze_report": output_path / "small_emotion_freeze_02_report.md",
    }


def _selected_pocket(top: pd.DataFrame) -> dict[str, object]:
    if top.empty:
        raise ValueError("top pocket input is empty")
    row = top.sort_values("pocket_rank" if "pocket_rank" in top.columns else top.columns[0]).iloc[0].to_dict()
    required = {"mechanism", "window", "path_predicates", "shock_threshold", "volume_spike_threshold", "adv_min_dollars"}
    missing = required - set(row)
    if missing:
        raise ValueError(f"top pocket missing required fields: {sorted(missing)}")
    return row


def _read_event_labels(cache_dir: Path, window: str) -> pd.DataFrame:
    path = cache_dir / f"event_labels_{window}.csv"
    if not path.exists():
        raise FileNotFoundError(f"missing cached event labels: {path}")
    return pd.read_csv(path)


def _select_frame(event_labels: pd.DataFrame, pocket: dict[str, object]) -> pd.DataFrame:
    frame = event_labels[
        event_labels["window"].astype(str).eq(str(pocket["window"]))
        & event_labels["label_status"].astype(str).eq("observed")
        & pd.to_numeric(event_labels["abnormal_volume"], errors="coerce").ge(float(pocket["volume_spike_threshold"]))
        & pd.to_numeric(event_labels["adv20"], errors="coerce").ge(float(pocket["adv_min_dollars"]))
    ].copy()
    shock_threshold = _frozen_shock_threshold(pocket)
    mechanism = str(pocket["mechanism"])
    if mechanism.startswith("up_"):
        frame = frame[pd.to_numeric(frame["shock_return"], errors="coerce").ge(shock_threshold)].copy()
    elif mechanism.startswith("down_"):
        frame = frame[pd.to_numeric(frame["shock_return"], errors="coerce").le(-shock_threshold)].copy()
    for predicate in _path_parts(pocket.get("path_predicates", "")):
        frame = _apply_path_predicate(frame, predicate)
    return frame.reset_index(drop=True)


def _path_parts(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split("&") if part.strip()]


def _frozen_shock_threshold(pocket: dict[str, object]) -> float:
    shock = float(pocket.get("shock_threshold", 0.0) or 0.0)
    for predicate in _path_parts(pocket.get("path_predicates", "")):
        if predicate.startswith("shock_ge_") and predicate.endswith("pct"):
            shock = max(shock, float(predicate.removeprefix("shock_ge_").removesuffix("pct")) / 100.0)
    return shock


def _apply_path_predicate(frame: pd.DataFrame, predicate: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    if predicate.startswith("shock_ge_") and predicate.endswith("pct"):
        value = float(predicate.removeprefix("shock_ge_").removesuffix("pct")) / 100.0
        return frame[pd.to_numeric(frame["abs_shock_return"], errors="coerce").ge(value)].copy()
    if predicate.startswith("volume_ge_") and predicate.endswith("x"):
        value = float(predicate.removeprefix("volume_ge_").removesuffix("x"))
        return frame[pd.to_numeric(frame["abnormal_volume"], errors="coerce").ge(value)].copy()
    if predicate == "prior5_ge_0":
        return frame[pd.to_numeric(frame["prior_5d_return"], errors="coerce").ge(0.0)].copy()
    if predicate == "prior5_le_0":
        return frame[pd.to_numeric(frame["prior_5d_return"], errors="coerce").le(0.0)].copy()
    if predicate.startswith("prior5_ge_") and predicate.endswith("pct"):
        value = float(predicate.removeprefix("prior5_ge_").removesuffix("pct")) / 100.0
        return frame[pd.to_numeric(frame["prior_5d_return"], errors="coerce").ge(value)].copy()
    if predicate == "prior20_ge_0":
        return frame[pd.to_numeric(frame["prior_20d_return"], errors="coerce").ge(0.0)].copy()
    if predicate == "prior20_le_0":
        return frame[pd.to_numeric(frame["prior_20d_return"], errors="coerce").le(0.0)].copy()
    if predicate.startswith("prior20_ge_") and predicate.endswith("pct"):
        value = float(predicate.removeprefix("prior20_ge_").removesuffix("pct")) / 100.0
        return frame[pd.to_numeric(frame["prior_20d_return"], errors="coerce").ge(value)].copy()
    if predicate == "spread_wide":
        return frame[frame["spread_bucket"].astype(str).eq("wide")].copy()
    if predicate == "spread_tight":
        return frame[frame["spread_bucket"].astype(str).eq("tight")].copy()
    if predicate.startswith("size_"):
        return frame[frame["full_market_size_bucket"].astype(str).eq(predicate.removeprefix("size_"))].copy()
    if predicate.startswith("liquidity_"):
        return frame[frame["liquidity_bucket"].astype(str).eq(predicate.removeprefix("liquidity_"))].copy()
    if predicate == "weak_liquidity":
        return frame[frame["weak_liquidity"].astype(bool)].copy()
    if predicate == "regime_market_up":
        return frame[frame["market_regime"].astype(str).eq("market_up_20d")].copy()
    if predicate == "regime_market_down":
        return frame[frame["market_regime"].astype(str).eq("market_down_20d")].copy()
    if predicate == "regime_high_vol":
        return frame[frame["market_regime"].astype(str).eq("market_high_vol")].copy()
    if predicate == "price_under_5":
        return frame[frame["low_price_bucket"].astype(str).eq("under_5")].copy()
    if predicate == "price_under_10":
        return frame[frame["low_price_bucket"].astype(str).isin({"under_5", "under_10"})].copy()
    if predicate == "price_under_20":
        return frame[frame["low_price_bucket"].astype(str).isin({"under_5", "under_10", "under_20"})].copy()
    if predicate == "open_to_close_le_minus_5pct":
        return frame[pd.to_numeric(frame["open_to_close_return"], errors="coerce").le(-0.05)].copy()
    if predicate == "open_to_close_ge_5pct":
        return frame[pd.to_numeric(frame["open_to_close_return"], errors="coerce").ge(0.05)].copy()
    if predicate == "close_top_quartile":
        return frame[pd.to_numeric(frame["close_location"], errors="coerce").ge(0.75)].copy()
    if predicate == "close_lower_half":
        return frame[pd.to_numeric(frame["close_location"], errors="coerce").le(0.50)].copy()
    return frame


def _directional_return(frame: pd.DataFrame, mechanism: str) -> pd.Series:
    abnormal = pd.to_numeric(frame["abnormal_return"], errors="coerce")
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return -abnormal
    return abnormal


def _d3_charter(pocket: dict[str, object]) -> dict[str, object]:
    candidate_id = str(pocket["candidate_id"])
    return {
        "schema_version": "small_emotion_d3_candidate_charter.v1",
        "stage": STAGE,
        "candidate_id": candidate_id,
        "candidate_family": "small_cap_shock_conditioned_emotion_liquidity",
        "source_stage": "E1-SMALL-EMOTION-FULL-MARKET-OVERFIT",
        "thesis": "Full-market wide-spread extreme up-shock events may reverse after attention/liquidity overshoot; this charter freezes the exact selected pocket before locked validation.",
        "candidate": {
            "mechanism": pocket["mechanism"],
            "expected_direction": "negative_post_shock_abnormal_return",
            "shock_threshold": _frozen_shock_threshold(pocket),
            "volume_spike_threshold": float(pocket["volume_spike_threshold"]),
            "adv_min_dollars": float(pocket["adv_min_dollars"]),
            "primary_window": pocket["window"],
            "path_predicates": pocket["path_predicates"],
            "spread_filter": "wide" if "spread_wide" in _path_parts(pocket["path_predicates"]) else "all",
            "market_cap_bucket": "all_full_market",
            "signal_state": "locked_validation_only_no_signal_panel",
        },
        "coverage_policy": {
            "missing_coverage": "no_view_not_zero_alpha",
            "no_view_rows_ranked": False,
            "coverage_as_alpha_allowed": False,
        },
        "hard_falsifiers": [
            "locked_temporal_validation",
            "sweep_adjusted_placebo_selection",
            "cost_liquidity_implementability",
            "data_anomaly_top_contributor_audit",
        ],
        "downstream_boundaries": _boundary_flags(measurement_spec_written=False, d3_charter_written=True),
    }


def _measurement_spec(charter: dict[str, object]) -> dict[str, object]:
    candidate = dict(charter["candidate"])  # type: ignore[index]
    candidate_id = str(charter["candidate_id"])
    return {
        "schema_version": "small_emotion_measurement_spec.v1",
        "stage": STAGE,
        "measurement_spec_id": candidate_id,
        "candidate_id": candidate_id,
        "status": "frozen_locked_validation_not_alpha_evidence",
        "signal_definition": {
            "mechanism": candidate["mechanism"],
            "expected_direction": candidate["expected_direction"],
            "signal_anchor": "shock_trading_date_close",
            "tradable_timestamp_policy": "next_trading_day_after_shock_close",
            "filters": {
                "shock_threshold": float(candidate["shock_threshold"]),
                "volume_spike_threshold": float(candidate["volume_spike_threshold"]),
                "adv_min_dollars": float(candidate["adv_min_dollars"]),
                "spread_filter": candidate["spread_filter"],
                "market_cap_bucket": candidate["market_cap_bucket"],
                "path_predicates": candidate["path_predicates"],
            },
            "signal_value": {
                "active_signal": -1.0,
                "missing_or_unqualified": "no_view",
            },
        },
        "label_contract": {
            "primary_window": candidate["primary_window"],
            "label_start_after_signal_anchor": True,
            "label_fields_forbidden_in_features": True,
        },
        "coverage_policy": {
            "missing_signal_policy": "no_view_not_zero_alpha",
            "unqualified_events_policy": "no_view",
            "no_view_rows_ranked": False,
        },
        "hard_falsifiers": list(charter.get("hard_falsifiers", [])),
        "downstream_boundaries": _boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }


def _temporal_split_metrics(selected: pd.DataFrame) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame(columns=_split_columns())
    frame = selected.copy()
    frame["date_ts"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values(["date_ts", "asset_id"]).reset_index(drop=True)
    n = len(frame)
    train_end = max(1, int(np.floor(n * 0.60)))
    validation_end = max(train_end + 1, int(np.floor(n * 0.80))) if n >= 3 else train_end
    splits = [
        ("train", frame.iloc[:train_end]),
        ("validation", frame.iloc[train_end:validation_end]),
        ("test", frame.iloc[validation_end:]),
    ]
    rows = [_metrics_row(split, data) for split, data in splits]
    return pd.DataFrame(rows, columns=_split_columns())


def _metrics_row(split: str, frame: pd.DataFrame) -> dict[str, object]:
    directional = pd.to_numeric(frame.get("directional_return", pd.Series(dtype=float)), errors="coerce").dropna()
    std = directional.std(ddof=1) if len(directional) > 1 else np.nan
    mean = directional.mean() if not directional.empty else np.nan
    return {
        "schema_version": "small_emotion_freeze_02_temporal_split_metrics.v1",
        "stage": STAGE,
        "split": split,
        "event_count": int(len(directional)),
        "event_month_count": int(frame["event_month"].nunique()) if not frame.empty else 0,
        "issuer_count": int(frame["asset_id"].nunique()) if not frame.empty else 0,
        "sector_count": int(frame["sector"].nunique()) if "sector" in frame and not frame.empty else 0,
        "mean_directional_return": float(mean) if pd.notna(mean) else np.nan,
        "hit_rate": float((directional > 0.0).mean()) if not directional.empty else np.nan,
        "t_stat": float(mean / (std / np.sqrt(len(directional)))) if len(directional) > 1 and pd.notna(std) and std > 0 else np.nan,
        "top5_abs_directional_return_share": _top_abs_share(directional),
        **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }


def _placebo_top_pockets(
    *,
    event_labels: pd.DataFrame,
    selected: pd.DataFrame,
    grid: pd.DataFrame,
    random_seed: int,
    min_events: int,
    min_event_months: int,
    excluded_predicates: list[str],
) -> pd.DataFrame:
    mechanisms = sorted(grid["mechanism"].dropna().astype(str).unique().tolist()) if "mechanism" in grid else ["up_shock_reversal"]
    windows = sorted(grid["window"].dropna().astype(str).unique().tolist()) if "window" in grid else ["post_1_22"]
    shocks = sorted(pd.to_numeric(grid.get("shock_threshold", pd.Series([0.08])), errors="coerce").dropna().unique().tolist())
    volumes = sorted(pd.to_numeric(grid.get("volume_spike_threshold", pd.Series([1.0])), errors="coerce").dropna().unique().tolist())
    advs = sorted(pd.to_numeric(grid.get("adv_min_dollars", pd.Series([250000.0])), errors="coerce").dropna().unique().tolist())
    if not shocks:
        shocks = [0.08]
    if not volumes:
        volumes = [1.0]
    if not advs:
        advs = [250000.0]

    frames = {
        "same_coverage_random": _same_coverage_random(event_labels, random_seed),
        "shifted_date": _shifted_date_proxy(event_labels),
        "large_cap_matched": event_labels[event_labels["full_market_size_bucket"].astype(str).isin({"large", "mega"})].copy(),
        "stale_price_matched": event_labels[
            pd.to_numeric(event_labels.get("stale_roll_5", pd.Series(dtype=float)), errors="coerce").ge(1)
            | event_labels.get("zero_volume", pd.Series(False, index=event_labels.index)).astype(str).str.lower().eq("true")
        ].copy(),
        "adv_capacity_matched": event_labels[
            pd.to_numeric(event_labels["adv20"], errors="coerce").le(pd.to_numeric(selected["adv20"], errors="coerce").median())
        ].copy(),
    }
    rows = []
    for name, frame in frames.items():
        if frame.empty:
            rows.append(_empty_placebo_row(name))
            continue
        grid_out, _lookup, search_count = _leaf_search(
            event_labels=frame,
            mechanisms=mechanisms,
            windows=windows,
            shock_thresholds=[float(value) for value in shocks],
            volume_spike_thresholds=[float(value) for value in volumes],
            adv_min_dollars=[float(value) for value in advs],
            max_depth=int(pd.to_numeric(grid.get("depth", pd.Series([2])), errors="coerce").max() or 2),
            beam_width=16,
            min_events=min_events,
            min_event_months=min_event_months,
            excluded_predicates=excluded_predicates,
        )
        best = grid_out[grid_out["eligible_for_overfit_review"].astype(bool)].head(1)
        if best.empty:
            rows.append({**_empty_placebo_row(name), "search_burden_row_count": int(search_count)})
        else:
            row = best.iloc[0]
            rows.append(
                {
                    "schema_version": "small_emotion_freeze_02_placebo_top_pocket.v1",
                    "stage": STAGE,
                    "placebo_name": name,
                    "search_burden_row_count": int(search_count),
                    "active_event_count": int(row["active_event_count"]),
                    "event_month_count": int(row["event_month_count"]),
                    "mean_directional_return": float(row["mean_directional_return"]),
                    "t_stat": float(row["t_stat"]) if pd.notna(row["t_stat"]) else np.nan,
                    "hit_rate": float(row["hit_rate"]) if pd.notna(row["hit_rate"]) else np.nan,
                    "path_predicates": row["path_predicates"],
                    **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
                }
            )
    result = pd.DataFrame(rows)
    result.attrs["excluded_predicates"] = list(excluded_predicates)
    return result


def _same_coverage_random(event_labels: pd.DataFrame, seed: int) -> pd.DataFrame:
    out = event_labels.copy()
    rng = np.random.default_rng(seed)
    values = pd.to_numeric(out["abnormal_return"], errors="coerce").to_numpy()
    rng.shuffle(values)
    out["abnormal_return"] = values
    return out


def _shifted_date_proxy(event_labels: pd.DataFrame) -> pd.DataFrame:
    out = event_labels.sort_values(["asset_id", "window", "date"]).copy()
    out["abnormal_return"] = out.groupby(["asset_id", "window"], sort=False)["abnormal_return"].shift(-1)
    return out.dropna(subset=["abnormal_return"]).copy()


def _empty_placebo_row(name: str) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_freeze_02_placebo_top_pocket.v1",
        "stage": STAGE,
        "placebo_name": name,
        "search_burden_row_count": 0,
        "active_event_count": 0,
        "event_month_count": 0,
        "mean_directional_return": np.nan,
        "t_stat": np.nan,
        "hit_rate": np.nan,
        "path_predicates": "",
        **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }


def _sweep_adjusted_selection_audit(
    *,
    grid: pd.DataFrame,
    selected: pd.DataFrame,
    placebo_top: pd.DataFrame,
    pocket: dict[str, object],
) -> dict[str, object]:
    live_profile = _selected_profile(selected)
    placebo_mean = pd.to_numeric(placebo_top["mean_directional_return"], errors="coerce")
    best_placebo = float(placebo_mean.max()) if placebo_mean.notna().any() else np.nan
    placebo_profiles = placebo_top.copy()
    if placebo_profiles.empty:
        placebo_profiles["profile_score"] = pd.Series(dtype="float64")
    else:
        placebo_profiles["profile_score"] = placebo_profiles.apply(_placebo_profile_score, axis=1)
    best_profile_score = (
        float(pd.to_numeric(placebo_profiles["profile_score"], errors="coerce").max())
        if "profile_score" in placebo_profiles and pd.to_numeric(placebo_profiles["profile_score"], errors="coerce").notna().any()
        else np.nan
    )
    best_profile_idx = (
        pd.to_numeric(placebo_profiles["profile_score"], errors="coerce").idxmax()
        if "profile_score" in placebo_profiles and pd.to_numeric(placebo_profiles["profile_score"], errors="coerce").notna().any()
        else None
    )
    return {
        "schema_version": "small_emotion_freeze_02_sweep_adjusted_selection_audit.v1",
        "stage": STAGE,
        "searched_grid_row_count": int(len(grid)),
        "selected_mechanism": pocket["mechanism"],
        "selected_window": pocket["window"],
        "selected_path_predicates": pocket["path_predicates"],
        "excluded_predicates_for_placebo_selection": list(placebo_top.attrs.get("excluded_predicates", [])),
        "placebo_profile_gate_version": "mean_tstat_hit_breadth_v1",
        "placebo_profile_gate_definition": "profile_score=max(mean,0)*max(t_stat,0)*max(hit_rate-0.5,0)*sqrt(event_month_count)",
        "live_mean_directional_return": live_profile["mean_directional_return"],
        "live_t_stat": live_profile["t_stat"],
        "live_hit_rate": live_profile["hit_rate"],
        "live_event_month_count": live_profile["event_month_count"],
        "live_profile_score": live_profile["profile_score"],
        "best_placebo_mean_directional_return": best_placebo,
        "best_placebo_name": str(placebo_top.loc[placebo_mean.idxmax(), "placebo_name"]) if placebo_mean.notna().any() else "",
        "selected_mean_beats_best_placebo_mean": bool(
            pd.notna(live_profile["mean_directional_return"])
            and (pd.isna(best_placebo) or live_profile["mean_directional_return"] > best_placebo)
        ),
        "best_placebo_profile_score": best_profile_score,
        "best_placebo_profile_name": str(placebo_profiles.loc[best_profile_idx, "placebo_name"]) if best_profile_idx is not None else "",
        "selected_beats_best_placebo": bool(
            pd.notna(live_profile["profile_score"])
            and (pd.isna(best_profile_score) or live_profile["profile_score"] > best_profile_score)
        ),
        "placebo_selection_scope": "same_search_space_on_placebo_label_panels_profile_gate",
        **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }


def _selected_profile(selected: pd.DataFrame) -> dict[str, float | int]:
    directional = pd.to_numeric(selected.get("directional_return", pd.Series(dtype="float64")), errors="coerce").dropna()
    mean = float(directional.mean()) if not directional.empty else np.nan
    std = float(directional.std(ddof=1)) if len(directional) > 1 else np.nan
    t_stat = float(mean / (std / np.sqrt(len(directional)))) if len(directional) > 1 and pd.notna(std) and std > 0 else np.nan
    hit_rate = float((directional > 0.0).mean()) if not directional.empty else np.nan
    months = int(selected.loc[directional.index, "event_month"].nunique()) if "event_month" in selected.columns and not directional.empty else 0
    return {
        "mean_directional_return": mean,
        "t_stat": t_stat,
        "hit_rate": hit_rate,
        "event_month_count": months,
        "profile_score": _profile_score(mean=mean, t_stat=t_stat, hit_rate=hit_rate, event_month_count=months),
    }


def _placebo_profile_score(row: pd.Series) -> float:
    return _profile_score(
        mean=float(row.get("mean_directional_return", np.nan)),
        t_stat=float(row.get("t_stat", np.nan)),
        hit_rate=float(row.get("hit_rate", np.nan)),
        event_month_count=int(row.get("event_month_count", 0) or 0),
    )


def _profile_score(*, mean: float, t_stat: float, hit_rate: float, event_month_count: int) -> float:
    if pd.isna(mean) or pd.isna(t_stat) or pd.isna(hit_rate) or event_month_count <= 0:
        return np.nan
    return float(max(float(mean), 0.0) * max(float(t_stat), 0.0) * max(float(hit_rate) - 0.5, 0.0) * np.sqrt(event_month_count))


def _cost_liquidity_gate(selected: pd.DataFrame) -> pd.DataFrame:
    gross = pd.to_numeric(selected["directional_return"], errors="coerce")
    spread = pd.to_numeric(selected.get("bid_ask_spread", pd.Series(dtype=float)), errors="coerce")
    adv = pd.to_numeric(selected.get("adv20", pd.Series(dtype=float)), errors="coerce").replace(0.0, np.nan)
    participation = 25_000.0 / adv
    timing_stress = pd.to_numeric(selected.get("open_to_close_return", pd.Series(dtype=float)), errors="coerce").abs() * 0.25
    rows = []
    for name, timing in {"next_close_entry": 0.0, "next_open_entry_proxy": timing_stress}.items():
        conservative_cost = (2.0 * spread.fillna(spread.median())) + (0.10 * participation.fillna(participation.median())) + timing
        post_cost = gross - conservative_cost
        rows.append(
            {
                "schema_version": "small_emotion_freeze_02_cost_liquidity_gate.v1",
                "stage": STAGE,
                "entry_assumption": name,
                "gross_mean_directional_return": float(gross.mean()) if gross.notna().any() else np.nan,
                "mean_conservative_cost": float(conservative_cost.mean()) if conservative_cost.notna().any() else np.nan,
                "post_cost_mean_directional_return": float(post_cost.mean()) if post_cost.notna().any() else np.nan,
                "spread_proxy_p95": float(spread.quantile(0.95)) if spread.notna().any() else np.nan,
                "slippage_stress_p95": float((conservative_cost - timing).quantile(0.95)) if conservative_cost.notna().any() else np.nan,
                "cost_liquidity_status": "fail" if post_cost.mean() <= 0.0 or spread.quantile(0.95) > 0.20 else "pass",
                **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
            }
        )
    return pd.DataFrame(rows)


def _capacity_frontier(selected: pd.DataFrame) -> pd.DataFrame:
    gross = pd.to_numeric(selected["directional_return"], errors="coerce")
    spread = pd.to_numeric(selected.get("bid_ask_spread", pd.Series(dtype=float)), errors="coerce")
    adv = pd.to_numeric(selected.get("adv20", pd.Series(dtype=float)), errors="coerce").replace(0.0, np.nan)
    rows = []
    for notional in [10_000, 25_000, 50_000, 100_000]:
        participation = float((float(notional) / adv).quantile(0.95)) if adv.notna().any() else np.nan
        cost = (2.0 * spread.fillna(spread.median())) + (0.10 * (float(notional) / adv).fillna(0.0))
        post_cost = gross - cost
        rows.append(
            {
                "schema_version": "small_emotion_freeze_02_capacity_frontier.v1",
                "stage": STAGE,
                "notional_usd": int(notional),
                "adv_participation_p95": participation,
                "post_cost_mean_directional_return": float(post_cost.mean()) if post_cost.notna().any() else np.nan,
                "capacity_status": "fail" if post_cost.mean() <= 0.0 or (pd.notna(participation) and participation > 0.35) else "pass",
                **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
            }
        )
    return pd.DataFrame(rows)


def _data_anomaly_audit(selected: pd.DataFrame) -> pd.DataFrame:
    frame = selected.copy()
    directional = pd.to_numeric(frame["directional_return"], errors="coerce")
    top = frame.loc[directional.abs().sort_values(ascending=False).head(25).index].copy() if not frame.empty else frame
    rows = []
    for row in top.itertuples(index=False):
        stale = float(getattr(row, "stale_roll_5", 0.0) or 0.0) >= 4
        zero_volume = str(getattr(row, "zero_volume", False)).lower() == "true"
        shock = abs(float(getattr(row, "shock_return", 0.0) or 0.0))
        asset_return = abs(float(getattr(row, "asset_return", 0.0) or 0.0))
        rows.append(
            {
                "schema_version": "small_emotion_freeze_02_data_anomaly_audit.v1",
                "stage": STAGE,
                "asset_id": getattr(row, "asset_id", ""),
                "date": getattr(row, "date", ""),
                "directional_return": float(getattr(row, "directional_return", np.nan)),
                "stale_price_flag": bool(stale),
                "zero_volume_flag": bool(zero_volume),
                "bad_print_proxy_flag": bool(shock > 3.0 or asset_return > 5.0),
                "delisting_audit_status": "unavailable" if not hasattr(row, "delisting_within_label_window") else "available",
                "split_or_corporate_action_audit_status": "unavailable" if not hasattr(row, "split_factor") else "available",
                "halt_suspension_audit_status": "unavailable" if not hasattr(row, "halted") else "available",
                **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
            }
        )
    return pd.DataFrame(rows)


def _decision(
    split_metrics: pd.DataFrame,
    sweep: dict[str, object],
    cost: pd.DataFrame,
    capacity: pd.DataFrame,
    anomaly: pd.DataFrame,
) -> tuple[str, str]:
    if not anomaly.empty and (
        anomaly["stale_price_flag"].astype(bool).any()
        or anomaly["zero_volume_flag"].astype(bool).any()
        or anomaly["bad_print_proxy_flag"].astype(bool).any()
    ):
        return "stale_or_bad_print_failed", "top_contributor_data_anomaly"
    test = split_metrics[split_metrics["split"].eq("test")]
    validation = split_metrics[split_metrics["split"].eq("validation")]
    if test.empty or validation.empty or float(test["mean_directional_return"].iloc[0]) <= 0.0 or float(validation["mean_directional_return"].iloc[0]) <= 0.0:
        return "locked_oos_failed", "validation_or_test_direction_failed"
    if not bool(sweep.get("selected_beats_best_placebo", False)):
        return "selection_bias_failed", "best_placebo_selected_pocket_dominated"
    if (not cost.empty and cost["cost_liquidity_status"].astype(str).eq("fail").any()) or (
        not capacity.empty and capacity["capacity_status"].astype(str).eq("fail").any()
    ):
        return "cost_liquidity_failed", "post_cost_or_capacity_frontier_failed"
    return "promote_to_q2_candidate", "locked_validation_passed_no_downstream_opened"


def _summary(
    *,
    reconciliation: dict[str, object],
    selected: pd.DataFrame,
    split_metrics: pd.DataFrame,
    sweep: dict[str, object],
    cost: pd.DataFrame,
    capacity: pd.DataFrame,
    anomaly: pd.DataFrame,
    decision: str,
    reason: str,
) -> dict[str, object]:
    if decision not in ALLOWED_DECISIONS:
        raise ValueError(f"unsupported SMALL-EMOTION-FREEZE-02 decision: {decision}")
    directional = pd.to_numeric(selected.get("directional_return", pd.Series(dtype=float)), errors="coerce")
    return {
        "schema_version": "small_emotion_freeze_02_summary.v1",
        "stage": STAGE,
        "candidate_id": reconciliation["candidate_id"],
        "prior_spec_identical": bool(reconciliation["prior_spec_identical"]),
        "new_measurement_spec_hash": reconciliation["new_measurement_spec_hash"],
        "exclude_stale_price_events": bool(reconciliation.get("exclude_stale_price_events", False)),
        "candidate_event_row_count_before_filter": int(reconciliation.get("candidate_event_row_count_before_filter", len(selected))),
        "candidate_event_row_count_after_filter": int(reconciliation.get("candidate_event_row_count_after_filter", len(selected))),
        "candidate_event_row_count_removed_by_stale_price_filter": int(
            reconciliation.get("candidate_event_row_count_removed_by_stale_price_filter", 0)
        ),
        "locked_event_count": int(len(selected)),
        "locked_event_month_count": int(selected["event_month"].nunique()) if not selected.empty else 0,
        "locked_issuer_count": int(selected["asset_id"].nunique()) if not selected.empty else 0,
        "locked_mean_directional_return": float(directional.mean()) if directional.notna().any() else np.nan,
        "temporal_split_count": int(len(split_metrics)),
        "placebo_profile_gate_version": str(sweep.get("placebo_profile_gate_version", "")),
        "live_profile_score": float(sweep.get("live_profile_score", np.nan)),
        "best_placebo_profile_score": float(sweep.get("best_placebo_profile_score", np.nan)),
        "best_placebo_profile_name": str(sweep.get("best_placebo_profile_name", "")),
        "selected_mean_beats_best_placebo_mean": bool(sweep.get("selected_mean_beats_best_placebo_mean", False)),
        "selected_beats_best_placebo": bool(sweep.get("selected_beats_best_placebo", False)),
        "cost_gate_failed": bool(not cost.empty and cost["cost_liquidity_status"].astype(str).eq("fail").any()),
        "capacity_gate_failed": bool(not capacity.empty and capacity["capacity_status"].astype(str).eq("fail").any()),
        "anomaly_count": int(anomaly[["stale_price_flag", "zero_volume_flag", "bad_print_proxy_flag"]].astype(bool).any(axis=1).sum()) if not anomaly.empty else 0,
        "decision": decision,
        "decision_reason": reason,
        **_boundary_flags(measurement_spec_written=True, d3_charter_written=True),
    }


def _candidate_id(pocket: dict[str, object]) -> str:
    path_tokens = [
        _slug(part)
        for part in _path_parts(pocket.get("path_predicates", ""))
        if not (part.startswith("shock_ge_") and part.endswith("pct"))
    ]
    shock_token = f"shock_ge{int(round(_frozen_shock_threshold(pocket) * 100.0))}"
    mechanism_token = {
        "up_shock_reversal": "up_reversal",
        "up_shock_continuation": "up_continuation",
        "down_shock_reversal": "down_reversal",
        "down_shock_continuation": "down_continuation",
    }.get(str(pocket.get("mechanism", "")), _slug(pocket.get("mechanism", "unknown")))
    window_token = _slug(pocket.get("window", "unknown_window"))
    tokens = [token for token in [*path_tokens, shock_token, mechanism_token, window_token] if token]
    return "small_emotion_full_market_" + "_".join(tokens) + "_v0"


def _slug(value: object) -> str:
    text = str(value or "").strip().lower()
    return "".join(char if char.isalnum() else "_" for char in text).strip("_")


def _report(summary: dict[str, object]) -> str:
    return "\n".join(
        [
            "# SMALL-EMOTION-FREEZE-02 Locked Validation",
            "",
            "This is locked validation for one frozen full-market small-emotion pocket. It does not open downstream execution, allocation, registry, paper, live, broker, order, or production paths.",
            "",
            f"- candidate_id: {summary['candidate_id']}",
            f"- prior_spec_identical: {summary['prior_spec_identical']}",
            f"- locked_event_count: {summary['locked_event_count']}",
            f"- locked_event_month_count: {summary['locked_event_month_count']}",
            f"- locked_mean_directional_return: {summary['locked_mean_directional_return']}",
            f"- placebo_profile_gate_version: {summary['placebo_profile_gate_version']}",
            f"- live_profile_score: {summary['live_profile_score']}",
            f"- best_placebo_profile_score: {summary['best_placebo_profile_score']}",
            f"- selected_beats_best_placebo: {summary['selected_beats_best_placebo']}",
            f"- cost_gate_failed: {summary['cost_gate_failed']}",
            f"- decision: {summary['decision']}",
            f"- q2_entry_allowed: {summary['q2_entry_allowed']}",
            "",
        ]
    )


def _split_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "split",
        "event_count",
        "event_month_count",
        "issuer_count",
        "sector_count",
        "mean_directional_return",
        "hit_rate",
        "t_stat",
        "top5_abs_directional_return_share",
        *_boundary_flags(measurement_spec_written=True, d3_charter_written=True).keys(),
    ]


def _top_abs_share(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().abs()
    total = clean.sum()
    return float(clean.nlargest(min(5, len(clean))).sum() / total) if total else np.nan


def _boundary_flags(*, measurement_spec_written: bool, d3_charter_written: bool) -> dict[str, object]:
    return {
        **EXPLORATORY_GUARDS,
        "d3_charter_written": bool(d3_charter_written),
        "measurement_spec_written": bool(measurement_spec_written),
        "formula_score_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
    }


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()
