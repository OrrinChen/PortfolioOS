"""FD-S3/S4 small-cap quality-controlled residual momentum family."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
import yaml

from .factor_design import write_candidate_design_manifest
from .small_cap_data_admission import GUARDS, run_small_cap_data_admission
from .small_cap_universe import build_small_cap_universe_tiers


FAMILY_ID = "small_cap_quality_residual_momentum_v1"
PRIMARY_SIGNAL = "small_cap_quality_residual_momentum_6m_ex1m"
RAW_BASELINE = "raw_momentum_6m_ex1m"
SECTOR_BASELINE = "sector_neutral_momentum_6m_ex1m"
SIZE_BASELINE = "small_cap_size_only_baseline"
CONTROL_NAMES = [
    "random_same_mcap_adv_coverage",
    "size_bucket_shuffled_signal",
    "sector_shuffled_signal",
    "rebalance_date_shifted_signal",
    "delisting_return_removed_sensitivity",
    "equal_weight_vs_value_weight_comparison",
]


@dataclass(frozen=True)
class FDSmallCapFamilyResult:
    """Artifacts and summary for the small-cap residual momentum family."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_cap_quality_residual_momentum(
    manifest_path: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
) -> FDSmallCapFamilyResult:
    """Run a fixed single-signal small-cap residual momentum family diagnostic."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)

    artifacts = _artifact_paths(output_path, report_file)
    manifest = _load_manifest(manifest_file)
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    delistings = _normalize_delistings(_load_section_csv(manifest, manifest_file, "delisting"))
    quality_scores = _normalize_quality_scores(_load_section_csv(manifest, manifest_file, "quality"))

    admission_root = output_path.parents[1] if len(output_path.parents) > 1 else output_path
    admission = run_small_cap_data_admission(manifest_file, admission_root)
    admission_payload = json.loads(admission.artifacts["data_admission_report"].read_text(encoding="utf-8"))

    tiers = build_small_cap_universe_tiers(prices=prices, universe=universe)
    tiers.to_csv(artifacts["universe_tiering_report"], index=False)

    design_manifest = write_candidate_design_manifest(
        artifacts["candidate_design_manifest"],
        candidate_id=PRIMARY_SIGNAL,
        family_id=FAMILY_ID,
        mechanism_family="small_cap_quality_residual_momentum",
    )
    family_manifest = _family_manifest(manifest_file, admission_payload, design_manifest)
    artifacts["family_manifest"].write_text(json.dumps(family_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    cache_key = _panel_cache_key(manifest, manifest_file)

    if not admission_payload["candidate_family_run_allowed"]:
        signal_panel = _empty_signal_panel()
        target = pd.DataFrame()
        oos = _empty_oos_validation()
        placebo = _empty_placebo_comparison()
        dominance = _empty_placebo_dominance_diagnosis()
        exposure = _empty_exposure_attribution()
        cost_capacity = _empty_cost_capacity_pre_gate()
        cache_manifest = _write_panel_cache_manifest(artifacts, cache_key, "unavailable", "unavailable")
        decision = _decision("reject_data_admission", admission_payload, signal_panel, oos, placebo, exposure, cost_capacity)
    else:
        cached = _read_panel_cache(artifacts, cache_key)
        if cached is None:
            signal_panel = _build_signal_panel(prices, benchmark, tiers, quality_scores)
            target = _build_forward_target_panel(prices, benchmark, tiers, delistings)
            _write_panel_cache(artifacts, signal_panel, target)
            cache_manifest = _write_panel_cache_manifest(artifacts, cache_key, "miss", "miss")
        else:
            signal_panel, target = cached
            cache_manifest = _write_panel_cache_manifest(artifacts, cache_key, "hit", "hit")
        oos = _build_oos_validation(signal_panel, target)
        placebo = _build_placebo_comparison(signal_panel, target)
        dominance = _build_placebo_dominance_diagnosis(signal_panel, target, placebo)
        exposure = _build_exposure_attribution(signal_panel)
        cost_capacity = _build_cost_capacity_pre_gate(signal_panel, oos)
        decision = _decision(
            _select_decision(signal_panel, oos, placebo, exposure, cost_capacity),
            admission_payload,
            signal_panel,
            oos,
            placebo,
            exposure,
            cost_capacity,
        )

    signal_panel.to_csv(artifacts["signal_panel"], index=False)
    oos.to_csv(artifacts["oos_validation"], index=False)
    placebo.to_csv(artifacts["placebo_comparison"], index=False)
    dominance.to_csv(artifacts["placebo_dominance_diagnosis"], index=False)
    exposure.to_csv(artifacts["exposure_attribution"], index=False)
    cost_capacity.to_csv(artifacts["cost_capacity_pre_gate"], index=False)
    artifacts["family_decision"].write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["family_report"].write_text(
        _render_report(decision, oos, placebo, dominance, exposure, cost_capacity), encoding="utf-8"
    )

    summary = {
        "schema_version": "fd_small_cap_family_summary.v1",
        "family_id": FAMILY_ID,
        "primary_signal": PRIMARY_SIGNAL,
        "decision_label": decision["decision_label"],
        "candidate_family_run_allowed": admission_payload["candidate_family_run_allowed"],
        "signal_panel_cache_status": cache_manifest["signal_panel_cache_status"],
        "target_panel_cache_status": cache_manifest["target_panel_cache_status"],
        "design_contract_valid": design_manifest["design_contract_valid"],
        "design_layer_required_before_formula": design_manifest["design_layer_required_before_formula"],
        "formula_is_measurement_not_thesis": design_manifest["formula_is_measurement_not_thesis"],
        **GUARDS,
    }
    return FDSmallCapFamilyResult(summary=summary, artifacts=artifacts)


def _build_signal_panel(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    tiers: pd.DataFrame,
    quality_scores: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if prices.empty or tiers.empty:
        return _empty_signal_panel()
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    volume = prices.pivot_table(index="date", columns="asset_id", values="volume", aggfunc="last").reindex(close.index)
    market_cap = _value_panel(prices, "market_cap", close)
    if market_cap.empty:
        shares = _value_panel(prices, "shares_float", close)
        if shares.empty:
            shares = _value_panel(prices, "shares_outstanding", close)
        market_cap = close * shares
    quality = _quality_panel(prices, quality_scores if quality_scores is not None else pd.DataFrame(), close)
    spread = _value_panel(prices, "bid_ask_spread", close)
    returns = np.log(close / close.shift(1))
    benchmark_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    benchmark_return = np.log(benchmark_close / benchmark_close.shift(1)).reindex(close.index)
    dollar_volume = close * volume
    adv_3m = dollar_volume.rolling(63, min_periods=20).mean()
    vol_6m = returns.rolling(126, min_periods=60).std() * np.sqrt(126)
    signal_dates = [pd.Timestamp(value) for value in sorted(tiers["rebalance_date"].dropna().unique())]

    rows: list[dict[str, object]] = []
    for signal_date in signal_dates:
        if signal_date not in close.index:
            continue
        candidate_tiers = tiers[tiers["rebalance_date"] == signal_date.date().isoformat()].copy()
        if candidate_tiers.empty:
            continue
        raw = _raw_signal_frame(signal_date, close, returns, benchmark_return, market_cap, adv_3m, vol_6m, quality, spread, candidate_tiers)
        if raw.empty:
            continue
        residualized = _residualize(raw)
        for row in residualized.itertuples(index=False):
            for signal_id, score, raw_value in _signal_variants(row):
                coverage = "active_view" if pd.notna(score) else "no_view"
                rows.append(
                    {
                        "schema_version": "fd_small_cap_signal_panel.v1",
                        "family_id": FAMILY_ID,
                        "signal_id": signal_id,
                        "rebalance_date": signal_date.date().isoformat(),
                        "asset_id": row.asset_id,
                        "ticker": row.ticker,
                        "sector": row.sector,
                        "universe_tier": row.universe_tier,
                        "quality_control_status": row.quality_control_status,
                        "evidence_quality": row.evidence_quality if coverage == "active_view" else "no_view",
                        "coverage_status": coverage,
                        "abstain_reason": "" if coverage == "active_view" else "missing_required_signal_inputs",
                        "raw_momentum_6m_ex1m": row.raw_momentum_6m_ex1m,
                        "raw_value": raw_value if coverage == "active_view" else np.nan,
                        "score": score if coverage == "active_view" else np.nan,
                        "realized_vol_6m": row.realized_vol_6m,
                        "beta_6m": row.beta_6m,
                        "market_cap": row.market_cap,
                        "log_market_cap": row.log_market_cap,
                        "adv_3m": row.adv_3m,
                        "log_adv_3m": row.log_adv_3m,
                        "quality_score": row.quality_score,
                        "spread_proxy": row.spread_proxy,
                        "residualization_controls": row.residualization_controls,
                        "fixed_single_signal_scoring": True,
                        "learned_weighting_used": False,
                        "rolling_icir_used": False,
                        "ridge_weighting_used": False,
                        "no_view_is_not_zero_alpha": True,
                        **GUARDS,
                    }
                )
    return pd.DataFrame(rows, columns=_signal_columns())


def _raw_signal_frame(
    signal_date: pd.Timestamp,
    close: pd.DataFrame,
    returns: pd.DataFrame,
    benchmark_return: pd.Series,
    market_cap: pd.DataFrame,
    adv_3m: pd.DataFrame,
    vol_6m: pd.DataFrame,
    quality: pd.DataFrame,
    spread: pd.DataFrame,
    tiers: pd.DataFrame,
) -> pd.DataFrame:
    position = close.index.get_loc(signal_date)
    if not isinstance(position, int) or position < 147:
        return pd.DataFrame()
    end = close.index[position - 21]
    start = close.index[position - 147]
    candidate_pool = tiers[tiers["universe_tier"].isin(["small_cap_investable", "large_cap_control"])].copy()
    rows = []
    for tier in candidate_pool.itertuples(index=False):
        asset = str(tier.asset_id)
        if asset not in close.columns:
            continue
        start_price = close.at[start, asset] if start in close.index else np.nan
        end_price = close.at[end, asset] if end in close.index else np.nan
        raw_momentum = np.log(end_price / start_price) if pd.notna(start_price) and pd.notna(end_price) and start_price > 0 else np.nan
        beta = _rolling_beta(returns[asset].loc[:signal_date].tail(126), benchmark_return.loc[:signal_date].tail(126))
        mcap = market_cap.at[signal_date, asset] if asset in market_cap.columns and signal_date in market_cap.index else np.nan
        adv = adv_3m.at[signal_date, asset] if asset in adv_3m.columns and signal_date in adv_3m.index else np.nan
        vol = vol_6m.at[signal_date, asset] if asset in vol_6m.columns and signal_date in vol_6m.index else np.nan
        q = quality.at[signal_date, asset] if not quality.empty and asset in quality.columns and signal_date in quality.index else np.nan
        spr = spread.at[signal_date, asset] if not spread.empty and asset in spread.columns and signal_date in spread.index else np.nan
        rows.append(
            {
                "rebalance_date": signal_date,
                "asset_id": asset,
                "ticker": str(tier.ticker),
                "sector": str(tier.sector),
                "universe_tier": str(tier.universe_tier),
                "candidate_decision_allowed": bool(tier.candidate_decision_allowed),
                "raw_momentum_6m_ex1m": raw_momentum,
                "beta_6m": beta,
                "market_cap": mcap,
                "log_market_cap": np.log(mcap) if pd.notna(mcap) and mcap > 0 else np.nan,
                "adv_3m": adv,
                "log_adv_3m": np.log(adv) if pd.notna(adv) and adv > 0 else np.nan,
                "realized_vol_6m": vol,
                "quality_score": q,
                "spread_proxy": spr,
            }
        )
    return pd.DataFrame(rows)


def _residualize(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    has_quality = output["quality_score"].notna().any() if "quality_score" in output.columns else False
    controls = ["sector", "beta", "log_market_cap", "log_adv_3m"] + (["quality_score"] if has_quality else [])
    valid_columns = ["raw_momentum_6m_ex1m", "beta_6m", "log_market_cap", "log_adv_3m", "realized_vol_6m"]
    if has_quality:
        valid_columns.append("quality_score")
    valid = output.dropna(subset=valid_columns).copy()
    output["residual"] = np.nan
    if not valid.empty:
        x = pd.DataFrame({"intercept": 1.0}, index=valid.index)
        sectors = pd.get_dummies(valid["sector"].astype(str), prefix="sector", drop_first=True, dtype=float)
        x = pd.concat(
            [
                x,
                sectors,
                valid[["beta_6m", "log_market_cap", "log_adv_3m"]].astype(float),
            ],
            axis=1,
        )
        if has_quality:
            x["quality_score"] = valid["quality_score"].astype(float)
        y = valid["raw_momentum_6m_ex1m"].astype(float)
        if len(valid) > x.shape[1]:
            beta = np.linalg.pinv(x.to_numpy(dtype="float64")) @ y.to_numpy(dtype="float64")
            fitted = x.to_numpy(dtype="float64") @ beta
            output.loc[valid.index, "residual"] = y.to_numpy(dtype="float64") - fitted
    output["score"] = output["residual"] / output["realized_vol_6m"]
    output.loc[output["realized_vol_6m"] <= 0.0, "score"] = np.nan
    output["sector_median_momentum"] = output.groupby("sector")["raw_momentum_6m_ex1m"].transform("median")
    output["sector_neutral_momentum"] = output["raw_momentum_6m_ex1m"] - output["sector_median_momentum"]
    output["size_only_score"] = -output["log_market_cap"]
    output["quality_control_status"] = "quality_controlled" if has_quality else "no_quality_variant"
    output["evidence_quality"] = "standard" if has_quality else "degraded"
    output["residualization_controls"] = ",".join(controls)
    return output[output["universe_tier"] == "small_cap_investable"].copy()


def _signal_variants(row: object) -> list[tuple[str, float, float]]:
    return [
        (PRIMARY_SIGNAL, row.score, row.residual),
        (RAW_BASELINE, row.raw_momentum_6m_ex1m, row.raw_momentum_6m_ex1m),
        (SECTOR_BASELINE, row.sector_neutral_momentum, row.sector_neutral_momentum),
        (SIZE_BASELINE, row.size_only_score, row.size_only_score),
    ]


def _build_oos_validation(
    signal_panel: pd.DataFrame,
    target: pd.DataFrame,
) -> pd.DataFrame:
    if signal_panel.empty or target.empty:
        return _empty_oos_validation()
    rows = []
    for signal_id, signal_group in signal_panel[signal_panel["coverage_status"] == "active_view"].groupby("signal_id"):
        merged = signal_group.merge(target, on=["rebalance_date", "asset_id"], how="inner")
        if "market_cap" not in merged.columns:
            for candidate in ("market_cap_x", "market_cap_y"):
                if candidate in merged.columns:
                    merged["market_cap"] = merged[candidate]
                    break
        for adjustment, return_column in [
            ("market_relative", "forward_market_relative_return"),
            ("small_cap_benchmark_relative", "forward_small_cap_relative_return"),
            ("smb_adjusted", "forward_smb_adjusted_return"),
            ("sector_adjusted", "forward_sector_adjusted_return"),
            ("liquidity_adjusted", "forward_liquidity_adjusted_return"),
            ("cost_adjusted", "forward_cost_adjusted_return"),
        ]:
            for (period, horizon), group in merged.groupby(["period", "horizon_months"]):
                if adjustment == "cost_adjusted":
                    metric = _score_metrics_cost_adjusted(group)
                else:
                    metric = _score_metrics(group, return_column, value_weighted=False)
                rows.append(
                    {
                        "schema_version": "fd_small_cap_oos_validation.v1",
                        "family_id": FAMILY_ID,
                        "signal_id": signal_id,
                        "period": period,
                        "horizon_months": int(horizon),
                        "return_adjustment": adjustment,
                        **metric,
                        "fixed_single_signal_scoring": True,
                        "learned_weighting_used": False,
                        "rolling_icir_used": False,
                        "ridge_weighting_used": False,
                        **GUARDS,
                    }
                )
    return pd.DataFrame(rows, columns=_oos_columns())


def _build_placebo_comparison(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    merged = _primary_target_merge(signal_panel, target)
    if merged.empty:
        return _empty_placebo_comparison()
    live = _placebo_metric_summary(merged, score_column="score", return_column="forward_market_relative_return")
    control_frames = {
        "random_same_mcap_adv_coverage": (
            "deterministic_permutation",
            _with_permuted_score(merged, ["rebalance_date", "horizon_months"], seed=17),
            "diagnostic_score",
            "forward_market_relative_return",
            False,
        ),
        "size_bucket_shuffled_signal": (
            "within_size_bucket_permutation",
            _with_size_bucket_permuted_score(merged, seed=23),
            "diagnostic_score",
            "forward_market_relative_return",
            False,
        ),
        "sector_shuffled_signal": (
            "within_sector_permutation",
            _with_permuted_score(merged, ["rebalance_date", "horizon_months", "sector"], seed=31),
            "diagnostic_score",
            "forward_market_relative_return",
            False,
        ),
        "rebalance_date_shifted_signal": (
            "asset_rebalance_lag",
            _with_lagged_score(merged),
            "diagnostic_score",
            "forward_market_relative_return",
            False,
        ),
        "delisting_return_removed_sensitivity": (
            "without_delisting_adjustment",
            merged,
            "score",
            "forward_market_relative_return_no_delisting",
            False,
        ),
        "equal_weight_vs_value_weight_comparison": (
            "value_weighted_live",
            merged,
            "score",
            "forward_market_relative_return",
            True,
        ),
    }
    rows = []
    for control, (method, frame, score_column, return_column, value_weighted) in control_frames.items():
        metric = _placebo_metric_summary(frame, score_column=score_column, return_column=return_column, value_weighted=value_weighted)
        rows.append(
            {
                "schema_version": "fd_small_cap_placebo_comparison.v1",
                "family_id": FAMILY_ID,
                "control_name": control,
                "control_method": method,
                "uses_realized_forward_returns": True,
                "live_mean_rank_ic": live["mean_rank_ic"],
                "control_mean_rank_ic": metric["mean_rank_ic"],
                "live_mean_spread": live["mean_spread"],
                "control_mean_spread": metric["mean_spread"],
                "control_beats_live": bool(
                    pd.notna(metric["mean_spread"])
                    and pd.notna(live["mean_spread"])
                    and float(metric["mean_spread"]) >= float(live["mean_spread"])
                ),
                **GUARDS,
            }
        )
    return pd.DataFrame(rows, columns=_placebo_columns())


def _build_placebo_dominance_diagnosis(
    signal_panel: pd.DataFrame,
    target: pd.DataFrame,
    placebo: pd.DataFrame,
) -> pd.DataFrame:
    merged = _primary_target_merge(signal_panel, target)
    if merged.empty:
        return _empty_placebo_dominance_diagnosis()
    lagged = _with_lagged_score(merged)
    lag_placebo = _placebo_control_row(placebo, "rebalance_date_shifted_signal")
    value_placebo = _placebo_control_row(placebo, "equal_weight_vs_value_weight_comparison")
    rows = [
        {
            "schema_version": "fd_small_cap_placebo_dominance_diagnosis.v1",
            "family_id": FAMILY_ID,
            "primary_signal": PRIMARY_SIGNAL,
            "control_name": "rebalance_date_shifted_signal",
            "dominance_detected": bool(lag_placebo.get("control_beats_live", False)),
            "live_mean_spread": lag_placebo.get("live_mean_spread", np.nan),
            "control_mean_spread": lag_placebo.get("control_mean_spread", np.nan),
            "likely_driver": "signal_persistence_or_monthly_update_noise"
            if bool(lag_placebo.get("control_beats_live", False))
            else "lag_control_not_dominant",
            "evidence_metric_1_name": "score_lag_rank_corr",
            "evidence_metric_1_value": _mean_group_corr(lagged, "score", "diagnostic_score"),
            "evidence_metric_2_name": "top_quintile_lag_overlap",
            "evidence_metric_2_value": _mean_top_quintile_overlap(lagged, "score", "diagnostic_score"),
            "recommended_action": "diagnose_signal_decay_and_test_smoothing_or_slower_holding_period_before_any_promotion",
            **GUARDS,
        },
        {
            "schema_version": "fd_small_cap_placebo_dominance_diagnosis.v1",
            "family_id": FAMILY_ID,
            "primary_signal": PRIMARY_SIGNAL,
            "control_name": "equal_weight_vs_value_weight_comparison",
            "dominance_detected": bool(value_placebo.get("control_beats_live", False)),
            "live_mean_spread": value_placebo.get("live_mean_spread", np.nan),
            "control_mean_spread": value_placebo.get("control_mean_spread", np.nan),
            "likely_driver": "payoff_concentrated_in_larger_more_capacity_names"
            if bool(value_placebo.get("control_beats_live", False))
            else "value_weight_control_not_dominant",
            "evidence_metric_1_name": "score_market_cap_rank_corr",
            "evidence_metric_1_value": _mean_group_corr(merged, "score", "market_cap"),
            "evidence_metric_2_name": "top_bottom_market_cap_ratio",
            "evidence_metric_2_value": _mean_top_bottom_market_cap_ratio(merged),
            "recommended_action": "split_large_small_and_capacity_weighted_diagnostics_before_rewriting_candidate_family",
            **GUARDS,
        },
    ]
    return pd.DataFrame(rows, columns=_dominance_columns())


def _build_exposure_attribution(signal_panel: pd.DataFrame) -> pd.DataFrame:
    primary = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    exposures = {
        "market_beta": _mean_corr(primary, "score", "beta_6m"),
        "smb_beta": _mean_corr(primary, "score", "log_market_cap") * -1.0,
        "sector_exposure": _sector_exposure(primary),
        "liquidity_exposure": _mean_corr(primary, "score", "log_adv_3m"),
        "microcap_exposure": 0.0 if not primary.empty else np.nan,
        "quality_exposure": _mean_corr(primary, "score", "quality_score"),
    }
    rows = [
        {
            "schema_version": "fd_small_cap_exposure_attribution.v1",
            "family_id": FAMILY_ID,
            "exposure_name": name,
            "exposure_value": value,
            "exposure_status": "measured" if pd.notna(value) else "unavailable",
            **GUARDS,
        }
        for name, value in exposures.items()
    ]
    return pd.DataFrame(rows, columns=_exposure_columns())


def _build_cost_capacity_pre_gate(signal_panel: pd.DataFrame, oos: pd.DataFrame) -> pd.DataFrame:
    primary = oos[
        (oos["signal_id"] == PRIMARY_SIGNAL)
        & (oos["period"] == "test")
        & (oos["return_adjustment"].isin(["market_relative", "cost_adjusted"]))
    ]
    gross = primary.loc[primary["return_adjustment"] == "market_relative", "top_bottom_spread"].mean()
    cost_adjusted = primary.loc[primary["return_adjustment"] == "cost_adjusted", "top_bottom_spread"].mean()
    active = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    adv = pd.to_numeric(active.get("adv_3m", pd.Series(dtype=float)), errors="coerce").replace([np.inf, -np.inf], np.nan)
    spread = pd.to_numeric(active.get("spread_proxy", pd.Series(dtype=float)), errors="coerce").replace([np.inf, -np.inf], np.nan)
    capacity = float(adv.dropna().median() * 0.01) if adv.notna().any() else np.nan
    median_spread = float(spread.dropna().median()) if spread.notna().any() else np.nan
    if pd.isna(gross) or pd.isna(cost_adjusted):
        status = "insufficient_oos_evidence"
    elif float(cost_adjusted) <= 0.0:
        status = "fail_cost_adjusted_spread"
    elif pd.isna(capacity) or capacity <= 0.0:
        status = "fail_capacity_unavailable"
    else:
        status = "pass_pre_gate"
    return pd.DataFrame(
        [
            {
                "schema_version": "fd_small_cap_cost_capacity_pre_gate.v1",
                "family_id": FAMILY_ID,
                "primary_signal": PRIMARY_SIGNAL,
                "pre_gate_status": status,
                "gross_mean_spread": gross,
                "cost_adjusted_mean_spread": cost_adjusted,
                "estimated_cost_drag": gross - cost_adjusted if pd.notna(gross) and pd.notna(cost_adjusted) else np.nan,
                "capacity_usd_1pct_adv": capacity,
                "median_spread_proxy": median_spread,
                "allocator_entry_allowed": False,
                "q1_entry_allowed": False,
                "q2_entry_allowed": False,
                "alpha_registry_update_allowed": False,
                "production_approval_claimed": False,
                **GUARDS,
            }
        ],
        columns=_cost_capacity_columns(),
    )


def _decision(
    decision_label: str,
    admission_payload: Mapping[str, Any],
    signal_panel: pd.DataFrame,
    oos: pd.DataFrame,
    placebo: pd.DataFrame,
    exposure: pd.DataFrame,
    cost_capacity: pd.DataFrame,
) -> dict[str, object]:
    primary = oos[
        (oos["signal_id"] == PRIMARY_SIGNAL)
        & (oos["period"] == "test")
        & (oos["return_adjustment"] == "market_relative")
    ]
    return {
        "schema_version": "fd_small_cap_family_decision.v1",
        "family_id": FAMILY_ID,
        "primary_signal": PRIMARY_SIGNAL,
        "decision_label": decision_label,
        "small_cap_research_admitted": bool(admission_payload.get("small_cap_research_admitted", False)),
        "candidate_family_run_allowed": bool(admission_payload.get("candidate_family_run_allowed", False)),
        "active_signal_rows": int((signal_panel.get("coverage_status", pd.Series(dtype=str)) == "active_view").sum())
        if not signal_panel.empty
        else 0,
        "test_mean_rank_ic": _json_float(primary["rank_ic"].mean()) if not primary.empty else None,
        "test_mean_spread": _json_float(primary["top_bottom_spread"].mean()) if not primary.empty else None,
        "placebo_failure": bool(placebo["control_beats_live"].fillna(False).any()) if not placebo.empty else False,
        "cost_capacity_pre_gate_status": _pre_gate_status(cost_capacity),
        "cost_capacity_failure": _pre_gate_failed(cost_capacity),
        "fixed_single_signal_scoring": True,
        "learned_weighting_used": False,
        "rolling_icir_used": False,
        "ridge_weighting_used": False,
        "recommended_next_action": "phase64_import_review_only" if decision_label == "candidate_for_phase64_review" else "do_not_enter_allocator",
        **GUARDS,
    }


def _json_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _select_decision(
    signal_panel: pd.DataFrame,
    oos: pd.DataFrame,
    placebo: pd.DataFrame,
    exposure: pd.DataFrame,
    cost_capacity: pd.DataFrame | None = None,
) -> str:
    primary = oos[
        (oos["signal_id"] == PRIMARY_SIGNAL)
        & (oos["period"] == "test")
        & (oos["return_adjustment"] == "market_relative")
    ]
    if signal_panel.empty or primary.empty:
        return "reject_no_signal"
    if float(primary["rank_ic"].mean()) <= 0.0 or float(primary["top_bottom_spread"].mean()) <= 0.0:
        return "reject_no_signal"
    liquidity = exposure.loc[exposure["exposure_name"] == "liquidity_exposure", "exposure_value"]
    if not liquidity.empty and pd.notna(liquidity.iloc[0]) and abs(float(liquidity.iloc[0])) > 0.80:
        return "reject_liquidity_exposure"
    if not placebo.empty and placebo["control_beats_live"].fillna(False).any():
        return "reject_placebo_failure"
    if cost_capacity is not None and _pre_gate_failed(cost_capacity):
        return "reject_cost_capacity"
    primary_signal = signal_panel[signal_panel["signal_id"] == PRIMARY_SIGNAL]
    active_evidence_quality = set(
        primary_signal.loc[primary_signal["coverage_status"] == "active_view", "evidence_quality"].astype(str)
    )
    if "degraded" in active_evidence_quality and "standard" not in active_evidence_quality:
        return "calibration_only"
    return "candidate_for_phase64_review"


def _pre_gate_status(cost_capacity: pd.DataFrame) -> str:
    if cost_capacity.empty or "pre_gate_status" not in cost_capacity.columns:
        return "unavailable"
    return str(cost_capacity["pre_gate_status"].iloc[0])


def _pre_gate_failed(cost_capacity: pd.DataFrame) -> bool:
    status = _pre_gate_status(cost_capacity)
    return status.startswith("fail_")


def _forward_targets(
    close: pd.DataFrame,
    benchmark_close: pd.Series,
    signal_dates: list[pd.Timestamp],
    next_dates: Mapping[pd.Timestamp, pd.Timestamp],
    tiers: pd.DataFrame,
    delistings: pd.DataFrame | None = None,
    horizons: tuple[int, ...] = (1, 3),
) -> pd.DataFrame:
    rows = []
    benchmark = benchmark_close.reindex(close.index).ffill()
    delisting_lookup = _delisting_lookup(delistings if delistings is not None else pd.DataFrame())
    for position, signal_date in enumerate(signal_dates):
        if signal_date not in next_dates:
            continue
        entry = next_dates[signal_date]
        date_tiers = tiers[tiers["rebalance_date"] == signal_date.date().isoformat()]
        for horizon in horizons:
            exit_position = position + horizon
            if exit_position >= len(signal_dates):
                continue
            exit_signal = signal_dates[exit_position]
            if exit_signal not in next_dates:
                continue
            exit_date = next_dates[exit_signal]
            if entry not in close.index or exit_date not in close.index:
                continue
            asset_returns_no_delisting = close.loc[exit_date] / close.loc[entry] - 1.0
            asset_returns = asset_returns_no_delisting.copy()
            delisting_applied = {}
            for asset in asset_returns.index.astype(str):
                dlret = _lookup_delisting_return(delisting_lookup, asset, entry, exit_date)
                delisting_applied[asset] = dlret
                if pd.notna(dlret) and pd.notna(asset_returns.get(asset, np.nan)):
                    asset_returns.loc[asset] = (1.0 + float(asset_returns.loc[asset])) * (1.0 + float(dlret)) - 1.0
            market_return = float(benchmark.loc[exit_date] / benchmark.loc[entry] - 1.0) if entry in benchmark.index and exit_date in benchmark.index else np.nan
            small_assets = date_tiers.loc[date_tiers["universe_tier"] == "small_cap_investable", "asset_id"].astype(str)
            large_assets = date_tiers.loc[date_tiers["universe_tier"] == "large_cap_control", "asset_id"].astype(str)
            small_benchmark = float(asset_returns.reindex(small_assets).dropna().mean()) if len(small_assets) else np.nan
            large_benchmark = float(asset_returns.reindex(large_assets).dropna().mean()) if len(large_assets) else np.nan
            sector_returns = _group_returns(asset_returns, date_tiers, "sector")
            liquidity_returns = _group_returns(asset_returns, date_tiers.assign(liquidity_bucket=pd.qcut(date_tiers["adv_3m"], 3, labels=False, duplicates="drop")), "liquidity_bucket")
            period = "validation" if position < 12 else "test"
            for tier_row in date_tiers.itertuples(index=False):
                asset = str(tier_row.asset_id)
                asset_return = float(asset_returns[asset]) if asset in asset_returns.index and pd.notna(asset_returns[asset]) else np.nan
                signal_price = (
                    float(close.at[signal_date, asset])
                    if signal_date in close.index and asset in close.columns and pd.notna(close.at[signal_date, asset])
                    else np.nan
                )
                asset_return_no_delisting = (
                    float(asset_returns_no_delisting[asset])
                    if asset in asset_returns_no_delisting.index and pd.notna(asset_returns_no_delisting[asset])
                    else np.nan
                )
                market_relative_no_delisting = (
                    asset_return_no_delisting - market_return
                    if pd.notna(asset_return_no_delisting) and pd.notna(market_return)
                    else np.nan
                )
                sector_adj = asset_return - sector_returns.get(str(tier_row.sector), np.nan) if pd.notna(asset_return) else np.nan
                liquidity_adj = asset_return - liquidity_returns.get(str(getattr(tier_row, "liquidity_bucket", "")), np.nan) if pd.notna(asset_return) else np.nan
                rows.append(
                    {
                        "rebalance_date": signal_date.date().isoformat(),
                        "asset_id": asset,
                        "period": period,
                        "horizon_months": horizon,
                        "target_timestamp": exit_date.date().isoformat(),
                        "target_visibility_rule": "forward_return_visible_only_after_exit_timestamp",
                        "forward_asset_return": asset_return,
                        "forward_asset_return_no_delisting": asset_return_no_delisting,
                        "delisting_return_applied": delisting_applied.get(asset, np.nan),
                        "forward_market_relative_return": asset_return - market_return if pd.notna(asset_return) and pd.notna(market_return) else np.nan,
                        "forward_market_relative_return_no_delisting": market_relative_no_delisting,
                        "forward_small_cap_relative_return": asset_return - small_benchmark if pd.notna(asset_return) and pd.notna(small_benchmark) else np.nan,
                        "forward_smb_adjusted_return": asset_return - (small_benchmark - large_benchmark) if pd.notna(asset_return) and pd.notna(small_benchmark) and pd.notna(large_benchmark) else np.nan,
                        "forward_sector_adjusted_return": sector_adj,
                        "forward_liquidity_adjusted_return": liquidity_adj,
                        "forward_cost_adjusted_return": asset_return - market_return - 0.002 if pd.notna(asset_return) and pd.notna(market_return) else np.nan,
                        "market_cap": getattr(tier_row, "market_cap"),
                        "price": signal_price,
                    }
                )
    return pd.DataFrame(rows)


def _build_forward_target_panel(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    tiers: pd.DataFrame,
    delistings: pd.DataFrame,
) -> pd.DataFrame:
    if prices.empty or benchmark.empty or tiers.empty:
        return pd.DataFrame()
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    benchmark_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    signal_dates = [pd.Timestamp(value) for value in sorted(tiers["rebalance_date"].dropna().unique())]
    next_dates = _next_trading_dates(close.index, signal_dates)
    return _forward_targets(close, benchmark_close, signal_dates, next_dates, tiers, delistings)


def _score_metrics(group: pd.DataFrame, return_column: str, value_weighted: bool) -> dict[str, object]:
    clean = group[["score", return_column, "market_cap"]].dropna()
    if len(clean) < 2 or clean["score"].nunique() <= 1 or clean[return_column].nunique() <= 1:
        return {"eligible_name_count": int(len(clean)), "rank_ic": np.nan, "top_bottom_spread": np.nan}
    ordered = clean.sort_values("score", ascending=False)
    count = max(1, int(np.ceil(len(ordered) * 0.2)))
    top = ordered.head(count)
    bottom = ordered.tail(count)
    top_return = _weighted_mean(top[return_column], top["market_cap"]) if value_weighted else float(top[return_column].mean())
    bottom_return = _weighted_mean(bottom[return_column], bottom["market_cap"]) if value_weighted else float(bottom[return_column].mean())
    return {
        "eligible_name_count": int(len(clean)),
        "rank_ic": float(clean["score"].corr(clean[return_column], method="spearman")),
        "top_bottom_spread": float(top_return - bottom_return),
    }


def _score_metrics_cost_adjusted(group: pd.DataFrame) -> dict[str, object]:
    clean = group[["score", "forward_market_relative_return", "market_cap", "spread_proxy", "adv_3m"]].dropna(
        subset=["score", "forward_market_relative_return"]
    )
    if len(clean) < 2 or clean["score"].nunique() <= 1 or clean["forward_market_relative_return"].nunique() <= 1:
        return {"eligible_name_count": int(len(clean)), "rank_ic": np.nan, "top_bottom_spread": np.nan}
    ordered = clean.sort_values("score", ascending=False)
    count = max(1, int(np.ceil(len(ordered) * 0.2)))
    top = ordered.head(count)
    bottom = ordered.tail(count)
    gross_spread = float(top["forward_market_relative_return"].mean() - bottom["forward_market_relative_return"].mean())
    cost_drag = float(_leg_cost(top).mean() + _leg_cost(bottom).mean())
    return {
        "eligible_name_count": int(len(clean)),
        "rank_ic": float(clean["score"].corr(clean["forward_market_relative_return"], method="spearman")),
        "top_bottom_spread": gross_spread - cost_drag,
    }


def _leg_cost(frame: pd.DataFrame) -> pd.Series:
    spread = pd.to_numeric(frame.get("spread_proxy", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    adv = pd.to_numeric(frame.get("adv_3m", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    spread_cost = spread.fillna(0.002).clip(lower=0.0005, upper=0.05)
    capacity_penalty = np.where(adv.fillna(0.0) < 1_000_000.0, 0.001, 0.0)
    return pd.Series(spread_cost.to_numpy(dtype="float64") + capacity_penalty, index=frame.index)


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    weights = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    values = pd.to_numeric(values, errors="coerce")
    total = float(weights.sum())
    return float((values * weights).sum() / total) if total > 0 else float(values.mean())


def _primary_target_merge(signal_panel: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    primary = signal_panel[
        (signal_panel["signal_id"] == PRIMARY_SIGNAL) & (signal_panel["coverage_status"] == "active_view")
    ].copy()
    if primary.empty or target.empty:
        return pd.DataFrame()
    merged = primary.merge(target, on=["rebalance_date", "asset_id"], how="inner", suffixes=("", "_target"))
    if "market_cap" not in merged.columns:
        for candidate in ("market_cap_x", "market_cap_y", "market_cap_target"):
            if candidate in merged.columns:
                merged["market_cap"] = merged[candidate]
                break
    if "sector" not in merged.columns:
        for candidate in ("sector_x", "sector_y", "sector_target"):
            if candidate in merged.columns:
                merged["sector"] = merged[candidate]
                break
    return merged


def _placebo_metric_summary(
    frame: pd.DataFrame,
    *,
    score_column: str,
    return_column: str,
    value_weighted: bool = False,
) -> dict[str, float]:
    if frame.empty or score_column not in frame.columns or return_column not in frame.columns:
        return {"mean_rank_ic": np.nan, "mean_spread": np.nan}
    metrics = []
    metric_frame = frame.copy()
    metric_frame["_diagnostic_score"] = pd.to_numeric(metric_frame[score_column], errors="coerce")
    for (_date, _horizon), group in metric_frame.groupby(["rebalance_date", "horizon_months"]):
        renamed = group.copy()
        renamed["score"] = renamed["_diagnostic_score"]
        metrics.append(_score_metrics(renamed, return_column, value_weighted=value_weighted))
    if not metrics:
        return {"mean_rank_ic": np.nan, "mean_spread": np.nan}
    return {
        "mean_rank_ic": float(np.nanmean([metric["rank_ic"] for metric in metrics])),
        "mean_spread": float(np.nanmean([metric["top_bottom_spread"] for metric in metrics])),
    }


def _with_permuted_score(frame: pd.DataFrame, group_columns: list[str], seed: int) -> pd.DataFrame:
    output = frame.copy()
    output["diagnostic_score"] = np.nan
    rng = np.random.default_rng(seed)
    for _key, group in output.groupby(group_columns, dropna=False):
        values = group["score"].to_numpy(dtype="float64", copy=True)
        rng.shuffle(values)
        output.loc[group.index, "diagnostic_score"] = values
    return output


def _with_size_bucket_permuted_score(frame: pd.DataFrame, seed: int) -> pd.DataFrame:
    output = frame.copy()
    output["size_bucket"] = np.nan
    for (_date, _horizon), group in output.groupby(["rebalance_date", "horizon_months"], dropna=False):
        if group["market_cap"].notna().sum() >= 3:
            output.loc[group.index, "size_bucket"] = pd.qcut(
                group["market_cap"],
                3,
                labels=False,
                duplicates="drop",
            )
    return _with_permuted_score(output, ["rebalance_date", "horizon_months", "size_bucket"], seed=seed)


def _with_lagged_score(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.sort_values(["asset_id", "horizon_months", "rebalance_date"]).copy()
    output["diagnostic_score"] = output.groupby(["asset_id", "horizon_months"])["score"].shift(1)
    return output


def _placebo_control_row(placebo: pd.DataFrame, control_name: str) -> dict[str, object]:
    if placebo.empty or "control_name" not in placebo.columns:
        return {}
    row = placebo[placebo["control_name"] == control_name]
    return row.iloc[0].to_dict() if not row.empty else {}


def _mean_group_corr(frame: pd.DataFrame, left: str, right: str) -> float:
    if frame.empty or left not in frame.columns or right not in frame.columns:
        return np.nan
    values = []
    for (_date, _horizon), group in frame.groupby(["rebalance_date", "horizon_months"]):
        clean = group[[left, right]].dropna()
        if len(clean) >= 3 and clean[left].nunique() > 1 and clean[right].nunique() > 1:
            values.append(float(clean[left].corr(clean[right], method="spearman")))
    return float(np.nanmean(values)) if values else np.nan


def _mean_top_quintile_overlap(frame: pd.DataFrame, left: str, right: str) -> float:
    if frame.empty or left not in frame.columns or right not in frame.columns:
        return np.nan
    overlaps = []
    for (_date, _horizon), group in frame.groupby(["rebalance_date", "horizon_months"]):
        clean = group[["asset_id", left, right]].dropna()
        if len(clean) < 2:
            continue
        count = max(1, int(np.ceil(len(clean) * 0.2)))
        left_top = set(clean.sort_values(left, ascending=False).head(count)["asset_id"].astype(str))
        right_top = set(clean.sort_values(right, ascending=False).head(count)["asset_id"].astype(str))
        overlaps.append(len(left_top & right_top) / count)
    return float(np.nanmean(overlaps)) if overlaps else np.nan


def _mean_top_bottom_market_cap_ratio(frame: pd.DataFrame) -> float:
    if frame.empty or "market_cap" not in frame.columns:
        return np.nan
    ratios = []
    for (_date, _horizon), group in frame.groupby(["rebalance_date", "horizon_months"]):
        clean = group[["score", "market_cap"]].dropna()
        if len(clean) < 2:
            continue
        ordered = clean.sort_values("score", ascending=False)
        count = max(1, int(np.ceil(len(ordered) * 0.2)))
        top = ordered.head(count)["market_cap"].median()
        bottom = ordered.tail(count)["market_cap"].median()
        if pd.notna(top) and pd.notna(bottom) and bottom > 0:
            ratios.append(float(top / bottom))
    return float(np.nanmean(ratios)) if ratios else np.nan


def _group_returns(asset_returns: pd.Series, tiers: pd.DataFrame, column: str) -> dict[str, float]:
    frame = tiers[["asset_id", column]].copy()
    frame["asset_return"] = frame["asset_id"].astype(str).map(asset_returns)
    return frame.groupby(column)["asset_return"].mean().dropna().rename(index=str).to_dict()


def _delisting_lookup(delistings: pd.DataFrame) -> dict[str, list[tuple[pd.Timestamp, float]]]:
    if delistings.empty or "asset_id" not in delistings.columns:
        return {}
    frame = delistings.copy()
    date_column = "delisting_date" if "delisting_date" in frame.columns else "date"
    if date_column not in frame.columns or "delisting_return" not in frame.columns:
        return {}
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce")
    frame["delisting_return"] = pd.to_numeric(frame["delisting_return"], errors="coerce")
    lookup: dict[str, list[tuple[pd.Timestamp, float]]] = {}
    for row in frame.dropna(subset=[date_column, "delisting_return"]).itertuples(index=False):
        asset = str(getattr(row, "asset_id"))
        date = pd.Timestamp(getattr(row, date_column))
        value = float(getattr(row, "delisting_return"))
        lookup.setdefault(asset, []).append((date, value))
    return lookup


def _lookup_delisting_return(
    lookup: Mapping[str, list[tuple[pd.Timestamp, float]]],
    asset_id: str,
    entry: pd.Timestamp,
    exit_date: pd.Timestamp,
) -> float:
    for date, value in lookup.get(asset_id, []):
        if entry < date <= exit_date:
            return value
    return np.nan


def _rolling_beta(asset_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    aligned = pd.concat([asset_returns, benchmark_returns], axis=1).dropna()
    if len(aligned) < 40:
        return np.nan
    x = aligned.iloc[:, 1].astype(float)
    y = aligned.iloc[:, 0].astype(float)
    variance = float(x.var(ddof=1))
    if variance <= 0.0 or not np.isfinite(variance):
        return np.nan
    return float(y.cov(x) / variance)


def _mean_corr(frame: pd.DataFrame, left: str, right: str) -> float:
    if frame.empty or right not in frame.columns:
        return np.nan
    values = []
    for _date, group in frame.groupby("rebalance_date"):
        clean = group[[left, right]].dropna()
        if len(clean) >= 3 and clean[left].nunique() > 1 and clean[right].nunique() > 1:
            values.append(float(clean[left].corr(clean[right], method="spearman")))
    return float(np.nanmean(values)) if values else np.nan


def _sector_exposure(frame: pd.DataFrame) -> float:
    if frame.empty:
        return np.nan
    values = []
    for _date, group in frame.groupby("rebalance_date"):
        if group.empty:
            continue
        values.append(float(group["sector"].value_counts(normalize=True).max()))
    return float(np.nanmean(values)) if values else np.nan


def _next_trading_dates(index: pd.Index, signal_dates: list[pd.Timestamp]) -> dict[pd.Timestamp, pd.Timestamp]:
    dates = list(pd.DatetimeIndex(index).sort_values())
    mapping = {}
    for date in signal_dates:
        future = [candidate for candidate in dates if candidate > date]
        if future:
            mapping[date] = future[0]
    return mapping


def _value_panel(prices: pd.DataFrame, column: str, close: pd.DataFrame) -> pd.DataFrame:
    if column not in prices.columns:
        return pd.DataFrame(index=close.index)
    return prices.pivot_table(index="date", columns="asset_id", values=column, aggfunc="last").reindex(close.index)


def _quality_panel(prices: pd.DataFrame, quality_scores: pd.DataFrame, close: pd.DataFrame) -> pd.DataFrame:
    price_quality = _value_panel(prices, "quality_score", close)
    if quality_scores.empty:
        return price_quality
    quality = quality_scores.copy()
    quality["date"] = pd.to_datetime(quality["date"], errors="coerce")
    quality["asset_id"] = quality["asset_id"].astype(str)
    quality["quality_score"] = pd.to_numeric(quality["quality_score"], errors="coerce")
    external = quality.pivot_table(index="date", columns="asset_id", values="quality_score", aggfunc="last").sort_index()
    external = external.reindex(close.index).ffill()
    if price_quality.empty:
        return external
    return external.combine_first(price_quality).reindex(close.index)


def _panel_cache_key(manifest: Mapping[str, Any], manifest_path: Path) -> str:
    payload = {
        "schema_version": "fd_small_cap_panel_cache_key.v1",
        "family_id": FAMILY_ID,
        "inputs": {
            section: _section_file_fingerprint(manifest, manifest_path, section)
            for section in ["prices", "universe", "benchmark", "delisting", "quality"]
        },
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _section_file_fingerprint(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> dict[str, object]:
    payload = manifest.get(section)
    if not isinstance(payload, Mapping) or not payload.get("path"):
        return {"path": None, "exists": False}
    path = Path(str(payload["path"]))
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "exists": True,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _read_panel_cache(artifacts: Mapping[str, Path], cache_key: str) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    manifest_path = artifacts["panel_cache_manifest"]
    signal_path = artifacts["monthly_signal_panel_cache"]
    target_path = artifacts["forward_target_panel_cache"]
    if not manifest_path.exists() or not signal_path.exists() or not target_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if manifest.get("cache_key") != cache_key:
        return None
    try:
        signal = pd.read_csv(signal_path)
        target = pd.read_csv(target_path)
    except EmptyDataError:
        return None
    return signal, target


def _write_panel_cache(artifacts: Mapping[str, Path], signal_panel: pd.DataFrame, target: pd.DataFrame) -> None:
    signal_panel.to_csv(artifacts["monthly_signal_panel_cache"], index=False)
    target.to_csv(artifacts["forward_target_panel_cache"], index=False)


def _write_panel_cache_manifest(
    artifacts: Mapping[str, Path],
    cache_key: str,
    signal_status: str,
    target_status: str,
) -> dict[str, object]:
    payload = {
        "schema_version": "fd_small_cap_panel_cache_manifest.v1",
        "family_id": FAMILY_ID,
        "cache_key": cache_key,
        "signal_panel_cache_status": signal_status,
        "target_panel_cache_status": target_status,
        "monthly_signal_panel_cache": str(artifacts["monthly_signal_panel_cache"]),
        "forward_target_panel_cache": str(artifacts["forward_target_panel_cache"]),
        **GUARDS,
    }
    artifacts["panel_cache_manifest"].write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _artifact_paths(output_path: Path, report_file: Path) -> dict[str, Path]:
    return {
        "universe_tiering_report": output_path.parent.parent / "universe_tiering_report.csv",
        "candidate_design_manifest": output_path / "candidate_design_manifest.json",
        "family_manifest": output_path / "family_manifest.json",
        "monthly_signal_panel_cache": output_path / "monthly_signal_panel_cache.csv",
        "forward_target_panel_cache": output_path / "forward_target_panel_cache.csv",
        "panel_cache_manifest": output_path / "panel_cache_manifest.json",
        "signal_panel": output_path / "signal_panel.csv",
        "oos_validation": output_path / "oos_validation.csv",
        "placebo_comparison": output_path / "placebo_comparison.csv",
        "placebo_dominance_diagnosis": output_path / "placebo_dominance_diagnosis.csv",
        "exposure_attribution": output_path / "exposure_attribution.csv",
        "cost_capacity_pre_gate": output_path / "cost_capacity_pre_gate.csv",
        "family_decision": output_path / "family_decision.json",
        "family_report": report_file,
    }


def _family_manifest(
    manifest_path: Path,
    admission_payload: Mapping[str, Any],
    design_manifest: Mapping[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "fd_small_cap_family_manifest.v1",
        "family_id": FAMILY_ID,
        "primary_signal": PRIMARY_SIGNAL,
        "source_manifest": str(manifest_path),
        "design_contract": design_manifest["design_contract"],
        "design_contract_valid": design_manifest["design_contract_valid"],
        "design_layer_required_before_formula": design_manifest["design_layer_required_before_formula"],
        "formula_is_measurement_not_thesis": design_manifest["formula_is_measurement_not_thesis"],
        "candidate_validation_allowed_by_design": design_manifest["candidate_validation_allowed"],
        "small_cap_research_admitted": bool(admission_payload.get("small_cap_research_admitted", False)),
        "fixed_single_signal_scoring": True,
        "rolling_icir_used": False,
        "ridge_weighting_used": False,
        "learned_weighting_used": False,
        "baselines": [RAW_BASELINE, SECTOR_BASELINE, SIZE_BASELINE],
        "negative_controls": CONTROL_NAMES,
        **GUARDS,
    }


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("small-cap family manifest must be a mapping")
    return payload


def _load_section_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    payload = manifest.get(section)
    if not isinstance(payload, Mapping) or not payload.get("path"):
        return pd.DataFrame()
    path = Path(str(payload["path"]))
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _normalize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in normalized.columns:
        if column not in {"asset_id", "permno", "ticker", "date", "sector", "industry", "adjusted_price_convention", "exchange_code"}:
            converted = pd.to_numeric(normalized[column], errors="coerce")
            if converted.notna().any():
                normalized[column] = converted
    return normalized


def _normalize_universe(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    return normalized


def _normalize_benchmark(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    return normalized


def _normalize_delistings(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    if "asset_id" in normalized.columns:
        normalized["asset_id"] = normalized["asset_id"].astype(str)
    if "delisting_date" in normalized.columns:
        normalized["delisting_date"] = pd.to_datetime(normalized["delisting_date"], errors="coerce")
    if "delisting_return" in normalized.columns:
        normalized["delisting_return"] = pd.to_numeric(normalized["delisting_return"], errors="coerce")
    return normalized


def _normalize_quality_scores(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return normalized
    if "asset_id" in normalized.columns:
        normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ["date", "visibility_timestamp", "tradable_timestamp"]:
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    for column in normalized.columns:
        if column not in {"schema_version", "asset_id", "date", "visibility_timestamp", "tradable_timestamp", "source"}:
            converted = pd.to_numeric(normalized[column], errors="coerce")
            if converted.notna().any():
                normalized[column] = converted
    return normalized


def _render_report(
    decision: Mapping[str, object],
    oos: pd.DataFrame,
    placebo: pd.DataFrame,
    dominance: pd.DataFrame,
    exposure: pd.DataFrame,
    cost_capacity: pd.DataFrame,
) -> str:
    lines = [
        "# FD Small-Cap Quality Residual Momentum",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- family: {FAMILY_ID}",
        f"- primary signal: {PRIMARY_SIGNAL}",
        f"- decision: {decision['decision_label']}",
        f"- learned weighting used: {str(decision['learned_weighting_used']).lower()}",
        "",
    ]
    if not oos.empty:
        rows = oos[(oos["signal_id"] == PRIMARY_SIGNAL) & (oos["return_adjustment"] == "market_relative")]
        for row in rows.itertuples(index=False):
            lines.append(
                f"- {row.period} {int(row.horizon_months)}m: "
                f"rank_ic={row.rank_ic:.6f}, spread={row.top_bottom_spread:.6f}"
            )
    if not placebo.empty:
        lines.append("")
        lines.append("## Negative Controls")
        for row in placebo.itertuples(index=False):
            lines.append(f"- {row.control_name}: control_beats_live={str(row.control_beats_live).lower()}")
    if not dominance.empty:
        lines.append("")
        lines.append("## Dominance Diagnosis")
        for row in dominance.itertuples(index=False):
            value = row.evidence_metric_1_value
            formatted = f"{value:.6f}" if pd.notna(value) else "unavailable"
            lines.append(
                f"- {row.control_name}: driver={row.likely_driver}, "
                f"{row.evidence_metric_1_name}={formatted}"
            )
    if not exposure.empty:
        lines.append("")
        lines.append("## Exposure Attribution")
        for row in exposure.itertuples(index=False):
            value = row.exposure_value
            formatted = f"{value:.6f}" if pd.notna(value) else "unavailable"
            lines.append(f"- {row.exposure_name}: {formatted}")
    if not cost_capacity.empty:
        lines.append("")
        lines.append("## Cost / Capacity Pre-Gate")
        row = cost_capacity.iloc[0]
        gross = row.get("gross_mean_spread", np.nan)
        cost_adjusted = row.get("cost_adjusted_mean_spread", np.nan)
        capacity = row.get("capacity_usd_1pct_adv", np.nan)
        lines.append(f"- status: {row.get('pre_gate_status', 'unavailable')}")
        lines.append(f"- gross mean spread: {_format_metric(gross)}")
        lines.append(f"- cost-adjusted mean spread: {_format_metric(cost_adjusted)}")
        lines.append(f"- capacity USD at 1% ADV: {_format_metric(capacity)}")
    lines.append("")
    return "\n".join(lines)


def _format_metric(value: object) -> str:
    return f"{float(value):.6f}" if pd.notna(value) else "unavailable"


def _empty_signal_panel() -> pd.DataFrame:
    return pd.DataFrame(columns=_signal_columns())


def _empty_oos_validation() -> pd.DataFrame:
    return pd.DataFrame(columns=_oos_columns())


def _empty_placebo_comparison() -> pd.DataFrame:
    rows = [
        {
            "schema_version": "fd_small_cap_placebo_comparison.v1",
            "family_id": FAMILY_ID,
            "control_name": control,
            "control_method": "unavailable",
            "uses_realized_forward_returns": True,
            "live_mean_rank_ic": np.nan,
            "control_mean_rank_ic": np.nan,
            "live_mean_spread": np.nan,
            "control_mean_spread": np.nan,
            "control_beats_live": False,
            **GUARDS,
        }
        for control in CONTROL_NAMES
    ]
    return pd.DataFrame(rows, columns=_placebo_columns())


def _empty_placebo_dominance_diagnosis() -> pd.DataFrame:
    rows = [
        {
            "schema_version": "fd_small_cap_placebo_dominance_diagnosis.v1",
            "family_id": FAMILY_ID,
            "primary_signal": PRIMARY_SIGNAL,
            "control_name": control,
            "dominance_detected": False,
            "live_mean_spread": np.nan,
            "control_mean_spread": np.nan,
            "likely_driver": "unavailable",
            "evidence_metric_1_name": "unavailable",
            "evidence_metric_1_value": np.nan,
            "evidence_metric_2_name": "unavailable",
            "evidence_metric_2_value": np.nan,
            "recommended_action": "do_not_enter_allocator",
            **GUARDS,
        }
        for control in ["rebalance_date_shifted_signal", "equal_weight_vs_value_weight_comparison"]
    ]
    return pd.DataFrame(rows, columns=_dominance_columns())


def _empty_exposure_attribution() -> pd.DataFrame:
    rows = [
        {
            "schema_version": "fd_small_cap_exposure_attribution.v1",
            "family_id": FAMILY_ID,
            "exposure_name": name,
            "exposure_value": np.nan,
            "exposure_status": "unavailable",
            **GUARDS,
        }
        for name in ["market_beta", "smb_beta", "sector_exposure", "liquidity_exposure", "microcap_exposure", "quality_exposure"]
    ]
    return pd.DataFrame(rows, columns=_exposure_columns())


def _empty_cost_capacity_pre_gate() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "fd_small_cap_cost_capacity_pre_gate.v1",
                "family_id": FAMILY_ID,
                "primary_signal": PRIMARY_SIGNAL,
                "pre_gate_status": "unavailable",
                "gross_mean_spread": np.nan,
                "cost_adjusted_mean_spread": np.nan,
                "estimated_cost_drag": np.nan,
                "capacity_usd_1pct_adv": np.nan,
                "median_spread_proxy": np.nan,
                "allocator_entry_allowed": False,
                "q1_entry_allowed": False,
                "q2_entry_allowed": False,
                "alpha_registry_update_allowed": False,
                "production_approval_claimed": False,
                **GUARDS,
            }
        ],
        columns=_cost_capacity_columns(),
    )


def _signal_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "signal_id",
        "rebalance_date",
        "asset_id",
        "ticker",
        "sector",
        "universe_tier",
        "quality_control_status",
        "evidence_quality",
        "coverage_status",
        "abstain_reason",
        "raw_momentum_6m_ex1m",
        "raw_value",
        "score",
        "realized_vol_6m",
        "beta_6m",
        "market_cap",
        "log_market_cap",
        "adv_3m",
        "log_adv_3m",
        "quality_score",
        "spread_proxy",
        "residualization_controls",
        "fixed_single_signal_scoring",
        "learned_weighting_used",
        "rolling_icir_used",
        "ridge_weighting_used",
        "no_view_is_not_zero_alpha",
        *GUARDS.keys(),
    ]


def _oos_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "signal_id",
        "period",
        "horizon_months",
        "return_adjustment",
        "eligible_name_count",
        "rank_ic",
        "top_bottom_spread",
        "fixed_single_signal_scoring",
        "learned_weighting_used",
        "rolling_icir_used",
        "ridge_weighting_used",
        *GUARDS.keys(),
    ]


def _placebo_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "control_name",
        "control_method",
        "uses_realized_forward_returns",
        "live_mean_rank_ic",
        "control_mean_rank_ic",
        "live_mean_spread",
        "control_mean_spread",
        "control_beats_live",
        *GUARDS.keys(),
    ]


def _dominance_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "primary_signal",
        "control_name",
        "dominance_detected",
        "live_mean_spread",
        "control_mean_spread",
        "likely_driver",
        "evidence_metric_1_name",
        "evidence_metric_1_value",
        "evidence_metric_2_name",
        "evidence_metric_2_value",
        "recommended_action",
        *GUARDS.keys(),
    ]


def _exposure_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "exposure_name",
        "exposure_value",
        "exposure_status",
        *GUARDS.keys(),
    ]


def _cost_capacity_columns() -> list[str]:
    return [
        "schema_version",
        "family_id",
        "primary_signal",
        "pre_gate_status",
        "gross_mean_spread",
        "cost_adjusted_mean_spread",
        "estimated_cost_drag",
        "capacity_usd_1pct_adv",
        "median_spread_proxy",
        *GUARDS.keys(),
    ]
