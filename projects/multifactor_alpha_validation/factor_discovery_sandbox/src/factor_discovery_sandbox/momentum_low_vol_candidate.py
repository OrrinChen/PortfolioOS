"""Standalone FD diagnostic for a 12-1 momentum minus volatility candidate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from .factor_design import write_candidate_design_manifest
from .oos_splitter import build_rolling_oos_splits
from .real_factor_replay import (
    _active_members_by_signal_date,
    _date_str,
    _detect_frequency,
    _load_manifest,
    _load_section_csv,
    _month_end_signal_dates,
    _next_trading_date_map,
    _normalize_benchmark,
    _normalize_prices,
    _normalize_universe,
    _priced_assets_by_date,
)
from .real_rolling_oos import _build_forward_targets


CANDIDATE_ID = "momentum_12m_ex1m_low_vol_3m"
FORMULA_TEXT = "rank(ts_sum(returns,252)-ts_sum(returns,21))-0.5*rank(ts_std_dev(returns,63))"
MOMENTUM_DAYS = 252
RECENT_EXCLUSION_DAYS = 21
VOLATILITY_DAYS = 63
CAPACITY_WINDOW_DAYS = 63
CAPACITY_FILTER_MIN_RANK = 0.30
CANDIDATE_VARIANT_COLUMNS = {
    "raw_candidate_score": "candidate_score",
    "industry_neutral_score": "industry_neutral_score",
    "capacity_filtered_score": "capacity_filtered_score",
}

SIGNAL_COLUMNS = [
    "schema_version",
    "candidate_id",
    "formula",
    "formula_source",
    "rebalance_date",
    "asset_id",
    "ticker",
    "sector",
    "industry",
    "momentum_12m_ex1m",
    "volatility_3m",
    "momentum_rank",
    "volatility_rank",
    "candidate_score",
    "industry_neutral_score",
    "dollar_volume_63d",
    "capacity_rank",
    "capacity_filter_passed",
    "capacity_filtered_score",
    "candidate_rank",
    "coverage_status",
    "abstain_reason",
    "signal_timestamp",
    "visibility_timestamp",
    "tradable_timestamp",
    "lookback_start",
    "lookback_end",
    "momentum_observations",
    "volatility_observations",
    "no_view_is_not_zero_alpha",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]

VALIDATION_COLUMNS = [
    "schema_version",
    "candidate_id",
    "candidate_variant",
    "rebalance_date",
    "period",
    "horizon_months",
    "eligible_name_count",
    "top_decile_count",
    "bottom_decile_count",
    "top_decile_excess_return",
    "bottom_decile_excess_return",
    "top_bottom_spread",
    "rank_ic",
    "no_view_is_not_zero_alpha",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]

PLACEBO_COLUMNS = [
    "schema_version",
    "candidate_id",
    "candidate_variant",
    "test_name",
    "period",
    "horizon_months",
    "rebalance_count",
    "mean_rank_ic",
    "mean_top_bottom_spread",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
]


@dataclass(frozen=True)
class FDMomentumLowVolCandidateResult:
    """Artifacts and summary for the standalone candidate diagnostic."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_momentum_low_vol_candidate_validation(
    manifest_path: str | Path,
    output_dir: str | Path,
    horizons: Iterable[int] = (1, 3),
    train_window_months: int = 36,
    validation_window_months: int = 12,
    min_cross_section: int = 5,
) -> FDMomentumLowVolCandidateResult:
    """Validate the fixed user-supplied candidate inside FD diagnostic boundaries."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "candidate_design_manifest": output_path / "candidate_design_manifest.json",
        "candidate_signal_panel": output_path / "candidate_signal_panel.csv",
        "candidate_validation_by_rebalance": output_path / "candidate_validation_by_rebalance.csv",
        "candidate_placebo_report": output_path / "candidate_placebo_report.csv",
        "candidate_summary": output_path / "candidate_summary.json",
        "candidate_report": output_path / "candidate_report.md",
    }
    design_manifest = write_candidate_design_manifest(
        artifacts["candidate_design_manifest"],
        candidate_id=CANDIDATE_ID,
        family_id="momentum_low_vol_candidate",
        mechanism_family="momentum_low_vol",
    )
    if not design_manifest["candidate_validation_allowed"]:
        raise ValueError("momentum low-vol candidate design contract is invalid")

    horizons_tuple = tuple(int(horizon) for horizon in horizons)
    manifest = _load_manifest(manifest_file)
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    frequency = _detect_frequency(prices)
    if frequency != "daily":
        raise ValueError("momentum low-vol candidate validation requires daily price-volume data")

    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    volume = prices.pivot_table(index="date", columns="asset_id", values="volume", aggfunc="last").reindex(close.index)
    qqq_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    signal_dates = _month_end_signal_dates(close.index, qqq_close.index)
    if not signal_dates:
        raise ValueError("momentum low-vol candidate validation requires at least one signal date")

    next_trading_date = _next_trading_date_map(pd.Index(close.index), signal_dates)
    active_members_by_date = _active_members_by_signal_date(universe, signal_dates)
    priced_assets_by_date = _priced_assets_by_date(close, signal_dates)

    signal_panel = _build_signal_panel(
        close=close,
        volume=volume,
        signal_dates=signal_dates,
        next_trading_date=next_trading_date,
        active_members_by_date=active_members_by_date,
        priced_assets_by_date=priced_assets_by_date,
    )
    targets = _build_forward_targets(
        signal_panel.assign(rebalance_date=pd.to_datetime(signal_panel["rebalance_date"])),
        close,
        qqq_close,
        signal_dates,
        next_trading_date,
        horizons_tuple,
    )
    splits = build_rolling_oos_splits(
        signal_dates,
        train_window_months=train_window_months,
        validation_window_months=validation_window_months,
        max_horizon_months=max(horizons_tuple),
    )
    scored = _attach_targets_and_periods(signal_panel, targets, splits)
    validation = _build_validation_by_rebalance(scored, min_cross_section=min_cross_section)
    placebo_report = _build_placebo_report(scored, min_cross_section=min_cross_section)
    summary = _build_summary(
        manifest_file=manifest_file,
        frequency=frequency,
        signal_panel=signal_panel,
        validation=validation,
        placebo_report=placebo_report,
        horizons=horizons_tuple,
        train_window_months=train_window_months,
        validation_window_months=validation_window_months,
        design_manifest=design_manifest,
    )

    signal_panel.to_csv(artifacts["candidate_signal_panel"], index=False)
    validation.to_csv(artifacts["candidate_validation_by_rebalance"], index=False)
    placebo_report.to_csv(artifacts["candidate_placebo_report"], index=False)
    artifacts["candidate_summary"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifacts["candidate_report"].write_text(_render_report(summary, placebo_report), encoding="utf-8")
    return FDMomentumLowVolCandidateResult(summary=summary, artifacts=artifacts)


def _build_signal_panel(
    close: pd.DataFrame,
    volume: pd.DataFrame,
    signal_dates: list[pd.Timestamp],
    next_trading_date: Mapping[pd.Timestamp, pd.Timestamp],
    active_members_by_date: Mapping[pd.Timestamp, pd.DataFrame],
    priced_assets_by_date: Mapping[pd.Timestamp, set[str]],
) -> pd.DataFrame:
    returns = close.pct_change(fill_method=None)
    momentum = returns.rolling(MOMENTUM_DAYS, min_periods=MOMENTUM_DAYS).sum() - returns.rolling(
        RECENT_EXCLUSION_DAYS,
        min_periods=RECENT_EXCLUSION_DAYS,
    ).sum()
    volatility = returns.rolling(VOLATILITY_DAYS, min_periods=VOLATILITY_DAYS).std()
    dollar_volume = close * volume
    trailing_dollar_volume = dollar_volume.rolling(CAPACITY_WINDOW_DAYS, min_periods=CAPACITY_WINDOW_DAYS).mean()
    momentum_observations = returns.rolling(MOMENTUM_DAYS, min_periods=1).count()
    volatility_observations = returns.rolling(VOLATILITY_DAYS, min_periods=1).count()

    rows: list[dict[str, object]] = []
    for signal_date in signal_dates:
        tradable_date = next_trading_date.get(signal_date)
        if tradable_date is None:
            continue
        members = active_members_by_date[signal_date]
        priced_assets = priced_assets_by_date[signal_date]
        date_momentum = momentum.loc[signal_date] if signal_date in momentum.index else pd.Series(dtype="float64")
        date_volatility = volatility.loc[signal_date] if signal_date in volatility.index else pd.Series(dtype="float64")
        date_capacity = (
            trailing_dollar_volume.loc[signal_date]
            if signal_date in trailing_dollar_volume.index
            else pd.Series(dtype="float64")
        )
        active_assets = []
        for member in members.itertuples(index=False):
            asset_id = str(member.asset_id)
            mom_value = date_momentum.get(asset_id, np.nan)
            vol_value = date_volatility.get(asset_id, np.nan)
            mom_obs = momentum_observations.at[signal_date, asset_id] if asset_id in momentum_observations.columns else 0
            vol_obs = volatility_observations.at[signal_date, asset_id] if asset_id in volatility_observations.columns else 0
            if (
                asset_id in priced_assets
                and int(mom_obs) >= MOMENTUM_DAYS
                and int(vol_obs) >= VOLATILITY_DAYS
                and pd.notna(mom_value)
                and pd.notna(vol_value)
                and np.isfinite(float(mom_value))
                and np.isfinite(float(vol_value))
            ):
                active_assets.append(asset_id)

        momentum_ranks = date_momentum.reindex(active_assets).rank(method="average", pct=True)
        volatility_ranks = date_volatility.reindex(active_assets).rank(method="average", pct=True)
        candidate_scores = momentum_ranks - 0.5 * volatility_ranks
        industry_neutral_scores = _neutralize_by_industry(candidate_scores, members, active_assets)
        capacity_values = date_capacity.reindex(active_assets)
        capacity_ranks = capacity_values.rank(method="average", pct=True)
        capacity_filter_passed = capacity_ranks >= CAPACITY_FILTER_MIN_RANK
        capacity_filtered_scores = candidate_scores.where(capacity_filter_passed)
        candidate_ranks = candidate_scores.rank(method="average", pct=True)
        lookback_start = _lookback_date(close.index, signal_date, MOMENTUM_DAYS)
        lookback_end = signal_date

        for member in members.itertuples(index=False):
            asset_id = str(member.asset_id)
            ticker = str(member.ticker) if hasattr(member, "ticker") and pd.notna(member.ticker) else ""
            mom_obs = momentum_observations.at[signal_date, asset_id] if asset_id in momentum_observations.columns else 0
            vol_obs = volatility_observations.at[signal_date, asset_id] if asset_id in volatility_observations.columns else 0
            coverage_status, abstain_reason = _coverage_status(
                asset_id=asset_id,
                priced_assets=priced_assets,
                momentum_observations=int(mom_obs) if pd.notna(mom_obs) else 0,
                volatility_observations=int(vol_obs) if pd.notna(vol_obs) else 0,
                score=candidate_scores.get(asset_id, np.nan),
            )
            active = coverage_status == "active_view"
            rows.append(
                {
                    "schema_version": "fd_momentum_low_vol_candidate_signal.v1",
                    "candidate_id": CANDIDATE_ID,
                    "formula": FORMULA_TEXT,
                    "formula_source": "user_supplied_fixed_formula",
                    "rebalance_date": _date_str(signal_date),
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "sector": _member_field(member, "sector"),
                    "industry": _member_field(member, "industry"),
                    "momentum_12m_ex1m": date_momentum.get(asset_id, np.nan) if active else np.nan,
                    "volatility_3m": date_volatility.get(asset_id, np.nan) if active else np.nan,
                    "momentum_rank": momentum_ranks.get(asset_id, np.nan) if active else np.nan,
                    "volatility_rank": volatility_ranks.get(asset_id, np.nan) if active else np.nan,
                    "candidate_score": candidate_scores.get(asset_id, np.nan) if active else np.nan,
                    "industry_neutral_score": industry_neutral_scores.get(asset_id, np.nan) if active else np.nan,
                    "dollar_volume_63d": capacity_values.get(asset_id, np.nan) if active else np.nan,
                    "capacity_rank": capacity_ranks.get(asset_id, np.nan) if active else np.nan,
                    "capacity_filter_passed": (
                        bool(capacity_filter_passed.get(asset_id, False)) if active else False
                    ),
                    "capacity_filtered_score": capacity_filtered_scores.get(asset_id, np.nan) if active else np.nan,
                    "candidate_rank": candidate_ranks.get(asset_id, np.nan) if active else np.nan,
                    "coverage_status": coverage_status,
                    "abstain_reason": abstain_reason,
                    "signal_timestamp": f"{_date_str(signal_date)}T16:00:00",
                    "visibility_timestamp": f"{_date_str(signal_date + pd.Timedelta(days=1))}T00:00:00",
                    "tradable_timestamp": f"{_date_str(tradable_date)}T16:00:00",
                    "lookback_start": _date_str(lookback_start),
                    "lookback_end": _date_str(lookback_end),
                    "momentum_observations": int(mom_obs) if pd.notna(mom_obs) else 0,
                    "volatility_observations": int(vol_obs) if pd.notna(vol_obs) else 0,
                    "no_view_is_not_zero_alpha": True,
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                }
            )
    return pd.DataFrame(rows, columns=SIGNAL_COLUMNS)


def _coverage_status(
    asset_id: str,
    priced_assets: set[str],
    momentum_observations: int,
    volatility_observations: int,
    score: object,
) -> tuple[str, str]:
    if asset_id not in priced_assets:
        return "explicit_abstain", "not_tradable_on_signal_date"
    if momentum_observations < MOMENTUM_DAYS:
        return "explicit_abstain", "insufficient_12m_return_history"
    if volatility_observations < VOLATILITY_DAYS:
        return "explicit_abstain", "insufficient_3m_volatility_history"
    if score is None or pd.isna(score) or not np.isfinite(float(score)):
        return "explicit_abstain", "candidate_score_unavailable"
    return "active_view", ""


def _attach_targets_and_periods(signal_panel: pd.DataFrame, targets: pd.DataFrame, splits: pd.DataFrame) -> pd.DataFrame:
    if splits.empty or targets.empty:
        return _empty_scored_frame()
    period_map = {
        pd.Timestamp(row.rebalance_date): str(row.period)
        for row in splits.itertuples(index=False)
    }
    scored = signal_panel.copy()
    scored["rebalance_date"] = pd.to_datetime(scored["rebalance_date"], errors="coerce")
    targets = targets.copy()
    targets["rebalance_date"] = pd.to_datetime(targets["rebalance_date"], errors="coerce")
    scored = scored.merge(targets, on=["rebalance_date", "asset_id"], how="inner")
    scored["period"] = scored["rebalance_date"].map(period_map)
    scored = scored[scored["period"].notna()].copy()
    if scored.empty:
        return _empty_scored_frame()
    scored["forward_return_available"] = scored["forward_return_available"].astype(bool)
    return scored


def _build_validation_by_rebalance(scored: pd.DataFrame, min_cross_section: int) -> pd.DataFrame:
    rows = []
    if scored.empty:
        return pd.DataFrame(columns=VALIDATION_COLUMNS)
    for candidate_variant, score_column in CANDIDATE_VARIANT_COLUMNS.items():
        for (rebalance_date, period, horizon), group in scored.groupby(["rebalance_date", "period", "horizon_months"]):
            rows.append(
                _diagnostic_row(
                    group=group,
                    candidate_variant=candidate_variant,
                    score_column=score_column,
                    rebalance_date=pd.Timestamp(rebalance_date),
                    period=str(period),
                    horizon=int(horizon),
                    min_cross_section=min_cross_section,
                )
            )
    return pd.DataFrame(rows, columns=VALIDATION_COLUMNS)


def _build_placebo_report(scored: pd.DataFrame, min_cross_section: int) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame(columns=PLACEBO_COLUMNS)
    base = scored.copy().sort_values(["horizon_months", "asset_id", "rebalance_date"])
    rows = []
    for candidate_variant, score_column in CANDIDATE_VARIANT_COLUMNS.items():
        active_mask = (base["coverage_status"] == "active_view") & pd.to_numeric(
            base[score_column],
            errors="coerce",
        ).notna()
        variants = {
            "live_candidate_score": base[score_column],
            "sign_flipped_negative_control": -base[score_column],
            "lagged_signal_placebo": base.groupby(["asset_id", "horizon_months"])[score_column].shift(1),
            "rebalance_date_shifted_placebo": base.groupby(["asset_id", "horizon_months"])[score_column].shift(2),
            "random_same_coverage_placebo": base.apply(_deterministic_random_score, axis=1),
            "future_return_leakage_negative_control": base["forward_excess_return"],
        }
        for test_name, score_series in variants.items():
            diagnostic = base.copy()
            diagnostic["diagnostic_score"] = score_series.where(active_mask)
            per_rebalance = []
            for (rebalance_date, period, horizon), group in diagnostic.groupby(
                ["rebalance_date", "period", "horizon_months"]
            ):
                row = _metric_values(group, "diagnostic_score", min_cross_section=min_cross_section)
                row.update(
                    {
                        "rebalance_date": pd.Timestamp(rebalance_date),
                        "period": str(period),
                        "horizon_months": int(horizon),
                    }
                )
                per_rebalance.append(row)
            frame = pd.DataFrame(per_rebalance)
            if frame.empty:
                continue
            for (period, horizon), group in frame.groupby(["period", "horizon_months"]):
                rows.append(
                    {
                        "schema_version": "fd_momentum_low_vol_candidate_placebo.v1",
                        "candidate_id": CANDIDATE_ID,
                        "candidate_variant": candidate_variant,
                        "test_name": test_name,
                        "period": str(period),
                        "horizon_months": int(horizon),
                        "rebalance_count": int(group["top_bottom_spread"].notna().sum()),
                        "mean_rank_ic": _safe_mean(group["rank_ic"]),
                        "mean_top_bottom_spread": _safe_mean(group["top_bottom_spread"]),
                        "not_alpha_evidence": True,
                        "direct_q2_entry_allowed": False,
                    }
                )
    return pd.DataFrame(rows, columns=PLACEBO_COLUMNS)


def _diagnostic_row(
    group: pd.DataFrame,
    candidate_variant: str,
    score_column: str,
    rebalance_date: pd.Timestamp,
    period: str,
    horizon: int,
    min_cross_section: int,
) -> dict[str, object]:
    metrics = _metric_values(group, score_column, min_cross_section=min_cross_section)
    return {
        "schema_version": "fd_momentum_low_vol_candidate_validation.v1",
        "candidate_id": CANDIDATE_ID,
        "candidate_variant": candidate_variant,
        "rebalance_date": _date_str(rebalance_date),
        "period": period,
        "horizon_months": horizon,
        **metrics,
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def _metric_values(group: pd.DataFrame, score_column: str, min_cross_section: int) -> dict[str, object]:
    eligible = group[
        (group["coverage_status"] == "active_view")
        & (group["forward_return_available"])
        & pd.to_numeric(group[score_column], errors="coerce").notna()
        & pd.to_numeric(group["forward_excess_return"], errors="coerce").notna()
    ].copy()
    eligible[score_column] = pd.to_numeric(eligible[score_column], errors="coerce")
    eligible["forward_excess_return"] = pd.to_numeric(eligible["forward_excess_return"], errors="coerce")
    row = {
        "eligible_name_count": int(len(eligible)),
        "top_decile_count": 0,
        "bottom_decile_count": 0,
        "top_decile_excess_return": np.nan,
        "bottom_decile_excess_return": np.nan,
        "top_bottom_spread": np.nan,
        "rank_ic": np.nan,
    }
    if len(eligible) < min_cross_section:
        return row
    ordered = eligible.sort_values(score_column, ascending=False)
    count = max(1, int(np.ceil(len(ordered) * 0.1)))
    top = ordered.head(count)
    bottom = ordered.tail(count)
    rank_ic = np.nan
    if ordered[score_column].nunique(dropna=True) > 1 and ordered["forward_excess_return"].nunique(dropna=True) > 1:
        rank_ic = ordered[score_column].corr(ordered["forward_excess_return"], method="spearman")
    row.update(
        {
            "top_decile_count": int(len(top)),
            "bottom_decile_count": int(len(bottom)),
            "top_decile_excess_return": float(top["forward_excess_return"].mean()),
            "bottom_decile_excess_return": float(bottom["forward_excess_return"].mean()),
            "top_bottom_spread": float(top["forward_excess_return"].mean() - bottom["forward_excess_return"].mean()),
            "rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan,
        }
    )
    return row


def _build_summary(
    manifest_file: Path,
    frequency: str,
    signal_panel: pd.DataFrame,
    validation: pd.DataFrame,
    placebo_report: pd.DataFrame,
    horizons: tuple[int, ...],
    train_window_months: int,
    validation_window_months: int,
    design_manifest: Mapping[str, object],
) -> dict[str, object]:
    variant_validation_status = {
        candidate_variant: _candidate_status(placebo_report, candidate_variant=candidate_variant)
        for candidate_variant in CANDIDATE_VARIANT_COLUMNS
    }
    status = variant_validation_status["raw_candidate_score"]
    test_live = placebo_report[
        (placebo_report["candidate_variant"] == "raw_candidate_score")
        & (placebo_report["test_name"] == "live_candidate_score")
        & (placebo_report["period"] == "test")
    ]
    if test_live.empty:
        test_live = placebo_report[
            (placebo_report["candidate_variant"] == "raw_candidate_score")
            & (placebo_report["test_name"] == "live_candidate_score")
        ]
    return {
        "schema_version": "fd_momentum_low_vol_candidate_summary.v1",
        "stage": "FD-CANDIDATE-DIAGNOSTIC",
        "candidate_id": CANDIDATE_ID,
        "formula": FORMULA_TEXT,
        "formula_source": "user_supplied_fixed_formula",
        "design_contract_valid": bool(design_manifest["design_contract_valid"]),
        "design_layer_required_before_formula": bool(design_manifest["design_layer_required_before_formula"]),
        "formula_is_measurement_not_thesis": bool(design_manifest["formula_is_measurement_not_thesis"]),
        "candidate_validation_allowed_by_design": bool(design_manifest["candidate_validation_allowed"]),
        "manifest_path": str(manifest_file),
        "dataset_frequency": frequency,
        "horizons_months": list(horizons),
        "train_window_months": train_window_months,
        "validation_window_months": validation_window_months,
        "signal_row_count": int(len(signal_panel)),
        "signal_date_count": int(signal_panel["rebalance_date"].nunique()) if not signal_panel.empty else 0,
        "active_signal_rows": int((signal_panel["coverage_status"] == "active_view").sum()) if not signal_panel.empty else 0,
        "explicit_abstain_rows": int((signal_panel["coverage_status"] == "explicit_abstain").sum()) if not signal_panel.empty else 0,
        "validation_row_count": int(len(validation)),
        "placebo_test_count": int(placebo_report["test_name"].nunique()) if not placebo_report.empty else 0,
        "candidate_variants": list(CANDIDATE_VARIANT_COLUMNS),
        "variant_validation_status": variant_validation_status,
        "test_mean_rank_ic": _safe_mean(test_live["mean_rank_ic"]) if not test_live.empty else np.nan,
        "test_mean_top_bottom_spread": _safe_mean(test_live["mean_top_bottom_spread"]) if not test_live.empty else np.nan,
        "candidate_validation_status": status,
        "allocator_ran": False,
        "alpha_success_claimed": False,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
    }


def _candidate_status(placebo_report: pd.DataFrame, candidate_variant: str) -> str:
    if placebo_report.empty:
        return "insufficient_diagnostic_evidence"
    rows = placebo_report[
        (placebo_report["candidate_variant"] == candidate_variant) & (placebo_report["period"] == "test")
    ]
    if rows.empty:
        rows = placebo_report[placebo_report["candidate_variant"] == candidate_variant]
    live = rows[rows["test_name"] == "live_candidate_score"].copy()
    if live.empty:
        return "insufficient_diagnostic_evidence"
    horizon_results = []
    for live_row in live.itertuples(index=False):
        comparators = rows[
            (rows["horizon_months"] == int(live_row.horizon_months))
            & rows["test_name"].isin(
                {
                    "lagged_signal_placebo",
                    "rebalance_date_shifted_placebo",
                    "random_same_coverage_placebo",
                }
            )
        ]
        if comparators.empty:
            continue
        live_spread = float(live_row.mean_top_bottom_spread)
        live_rank_ic = float(live_row.mean_rank_ic)
        comparator_spreads = pd.to_numeric(comparators["mean_top_bottom_spread"], errors="coerce").dropna()
        comparator_spread = float(comparator_spreads.max()) if len(comparator_spreads) else np.nan
        if not np.isfinite(live_spread) or not np.isfinite(live_rank_ic):
            continue
        horizon_results.append(live_spread > max(0.0, comparator_spread) and live_rank_ic > 0.0)
    if not horizon_results:
        return "insufficient_diagnostic_evidence"
    if all(horizon_results):
        return "passed_initial_diagnostic_gate"
    if any(horizon_results):
        return "mixed_initial_diagnostic_gate"
    return "failed_initial_diagnostic_gate"


def _render_report(summary: Mapping[str, object], placebo_report: pd.DataFrame) -> str:
    lines = [
        "# Momentum 12-1 Low-Vol Candidate Diagnostic",
        "",
        "not alpha evidence",
        "direct Q2 entry: not allowed",
        "allocator: not run",
        "production approval: not claimed",
        "",
        f"- candidate_id: {summary['candidate_id']}",
        f"- formula: `{summary['formula']}`",
        f"- dataset frequency: {summary['dataset_frequency']}",
        f"- signal dates: {summary['signal_date_count']}",
        f"- active signal rows: {summary['active_signal_rows']}",
        f"- explicit abstain rows: {summary['explicit_abstain_rows']}",
        f"- status: {summary['candidate_validation_status']}",
        "",
        "## Variant Status",
        *[
            f"- {variant}: {status}"
            for variant, status in dict(summary.get("variant_validation_status", {})).items()
        ],
        "",
        "## Placebo Summary",
    ]
    if placebo_report.empty:
        lines.append("- no placebo diagnostics were available")
    else:
        for row in placebo_report.sort_values(
            ["candidate_variant", "period", "horizon_months", "test_name"]
        ).itertuples(index=False):
            lines.append(
                f"- {row.candidate_variant} {row.period} {int(row.horizon_months)}m {row.test_name}: "
                f"mean_rank_ic={_format_float(row.mean_rank_ic)}, "
                f"mean_top_bottom_spread={_format_float(row.mean_top_bottom_spread)}"
            )
    lines.append("")
    return "\n".join(lines)


def _empty_scored_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *SIGNAL_COLUMNS,
            "horizon_months",
            "forward_asset_return",
            "forward_benchmark_return",
            "forward_excess_return",
            "forward_return_available",
            "period",
        ]
    )


def _lookback_date(dates: pd.Index, signal_date: pd.Timestamp, lookback_days: int) -> pd.Timestamp | None:
    position = dates.get_loc(signal_date)
    if isinstance(position, slice) or isinstance(position, np.ndarray):
        raise ValueError("price dates must be unique")
    start = int(position) - lookback_days + 1
    if start < 0:
        return None
    return pd.Timestamp(dates[start])


def _member_field(member: object, field: str) -> str:
    if not hasattr(member, field):
        return ""
    value = getattr(member, field)
    if pd.isna(value):
        return ""
    return str(value)


def _neutralize_by_industry(scores: pd.Series, members: pd.DataFrame, active_assets: list[str]) -> pd.Series:
    if not active_assets:
        return pd.Series(dtype="float64")
    group_by_asset = {}
    for member in members.itertuples(index=False):
        asset_id = str(member.asset_id)
        if asset_id not in active_assets:
            continue
        industry = _member_field(member, "industry").strip()
        sector = _member_field(member, "sector").strip()
        group_by_asset[asset_id] = industry or sector or "__unknown__"
    groups = pd.Series(group_by_asset).reindex(scores.index)
    group_means = scores.groupby(groups).transform("mean")
    return scores - group_means


def _deterministic_random_score(row: pd.Series) -> float:
    if row.get("coverage_status") != "active_view":
        return np.nan
    key = f"{row.get('rebalance_date')}|{row.get('asset_id')}|{row.get('horizon_months')}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12 - 1) - 0.5


def _safe_mean(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if len(values) else np.nan


def _format_float(value: object) -> str:
    if value is None or pd.isna(value) or not np.isfinite(float(value)):
        return "nan"
    return f"{float(value):.6f}"
