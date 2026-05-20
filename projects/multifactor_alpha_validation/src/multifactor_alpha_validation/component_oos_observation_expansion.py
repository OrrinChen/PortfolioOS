from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml

from multifactor_alpha_validation.real_rolling_oos import (
    _COST_DRAG,
    _adjusted_metrics,
    _benchmark_return,
    _date_position,
    _date_str,
    _exposures_for_date,
    _forward_returns,
    _latest_market_cap,
    _month_end_trading_dates,
    _normalize_benchmark,
    _normalize_prices,
    _normalize_universe,
    _round_optional,
    _top_bottom_spread,
)


@dataclass(frozen=True)
class ComponentOOSObservationExpansionResult:
    observation_path: str
    summary_path: str
    enablement_report_path: str
    generated_factor_ids: tuple[str, ...]
    observed_factor_count_after_expansion: int
    unavailable_factor_ids_after_expansion: tuple[str, ...]
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


_GENERATABLE_FACTORS = (
    "liquidity_turnover",
    "value_bm",
    "profitability_quality",
    "investment_asset_growth",
    "accruals",
)
_FUNDAMENTAL_FACTORS = {
    "value_bm",
    "profitability_quality",
    "investment_asset_growth",
    "accruals",
}
_MIN_PRICE_HISTORY_DAYS = 252
_MIN_LIQUIDITY_HISTORY_DAYS = 60
_MIN_ASSETS = 3


def run_component_oos_observation_expansion(
    source_observation_path: Path,
    component_pool_path: Path,
    daily_manifest_path: Path,
    fundamentals_manifest_path: Path,
    output_dir: Path,
) -> ComponentOOSObservationExpansionResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_observations = _read_csv(source_observation_path)
    component_pool = _read_csv(component_pool_path)
    daily_manifest = _read_yaml(daily_manifest_path)
    fundamentals_manifest = _read_yaml(fundamentals_manifest_path) if fundamentals_manifest_path.exists() else {}

    prices = _normalize_prices(_load_manifest_csv(daily_manifest, daily_manifest_path, "prices"))
    universe = _normalize_universe(_load_manifest_csv(daily_manifest, daily_manifest_path, "universe"))
    benchmark = _normalize_benchmark(_load_manifest_csv(daily_manifest, daily_manifest_path, "benchmark"))
    annual_fundamentals = _load_annual_fundamentals(fundamentals_manifest, fundamentals_manifest_path)

    pool_ids = _pool_factor_ids(component_pool)
    observed_ids_before = _observed_factor_ids(source_observations)
    target_ids = [factor_id for factor_id in _GENERATABLE_FACTORS if factor_id in pool_ids and factor_id not in observed_ids_before]

    generated_rows: list[dict[str, Any]] = []
    enablement_rows: list[dict[str, Any]] = []
    if target_ids:
        generated_by_factor = _build_generated_observations(
            target_ids=target_ids,
            source_observations=source_observations,
            prices=prices,
            universe=universe,
            benchmark=benchmark,
            annual_fundamentals=annual_fundamentals,
        )
        for factor_id in target_ids:
            rows = generated_by_factor.get(factor_id, [])
            generated_rows.extend(rows)
            enablement_rows.append(_enablement_row(factor_id, rows, fundamentals_manifest_exists=bool(fundamentals_manifest)))

    for factor_id in sorted(pool_ids - observed_ids_before - set(target_ids)):
        enablement_rows.append(_unavailable_enablement_row(factor_id))

    generated = pd.DataFrame(generated_rows)
    expanded = _merge_observations(source_observations, generated)
    observed_after = sorted(set(expanded["factor_id"].astype(str))) if not expanded.empty and "factor_id" in expanded.columns else []
    unavailable_after = sorted(pool_ids - set(observed_after))
    generated_ids = sorted(set(generated["factor_id"].astype(str))) if not generated.empty else []
    enablement = _finalize_enablement_report(enablement_rows, observed_after, unavailable_after)
    summary = _summary(
        source_observation_path=source_observation_path,
        component_pool_path=component_pool_path,
        daily_manifest_path=daily_manifest_path,
        fundamentals_manifest_path=fundamentals_manifest_path,
        source_observed_count=len(observed_ids_before),
        generated_ids=generated_ids,
        observed_after=observed_after,
        unavailable_after=unavailable_after,
        generated_row_count=len(generated),
    )

    observation_path = output_dir / "real_oos_observations.csv"
    summary_path = output_dir / "component_oos_observation_expansion_summary.json"
    enablement_path = output_dir / "component_oos_observation_enablement_report.csv"
    expanded.to_csv(observation_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    enablement.to_csv(enablement_path, index=False)

    return ComponentOOSObservationExpansionResult(
        observation_path=str(observation_path),
        summary_path=str(summary_path),
        enablement_report_path=str(enablement_path),
        generated_factor_ids=tuple(generated_ids),
        observed_factor_count_after_expansion=len(observed_after),
        unavailable_factor_ids_after_expansion=tuple(unavailable_after),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _load_manifest_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    section_payload = manifest.get(section)
    if not isinstance(section_payload, Mapping):
        raise ValueError(f"{section} section is required in daily manifest")
    path = _resolve_manifest_path(Path(str(section_payload.get("path", ""))), manifest_path)
    return pd.read_csv(path)


def _load_annual_fundamentals(manifest: Mapping[str, Any], manifest_path: Path) -> pd.DataFrame:
    paths = manifest.get("paths", {})
    if not isinstance(paths, Mapping) or "annual_fundamentals_panel" not in paths:
        return pd.DataFrame()
    path = _resolve_manifest_path(Path(str(paths["annual_fundamentals_panel"])), manifest_path)
    if not path.exists():
        return pd.DataFrame()
    fundamentals = pd.read_csv(path)
    if fundamentals.empty:
        return fundamentals
    fundamentals = fundamentals.copy()
    fundamentals["gvkey"] = fundamentals["gvkey"].map(_normalize_identifier)
    fundamentals["datadate"] = pd.to_datetime(fundamentals["datadate"], errors="coerce")
    fundamentals["visibility_timestamp"] = pd.to_datetime(fundamentals["visibility_timestamp"], errors="coerce")
    fundamentals["tradable_timestamp"] = pd.to_datetime(fundamentals.get("tradable_timestamp"), errors="coerce")
    for column in ["ceq", "at", "oibdp", "ib", "oancf", "mkvalt"]:
        if column in fundamentals.columns:
            fundamentals[column] = pd.to_numeric(fundamentals[column], errors="coerce")
    return fundamentals.dropna(subset=["gvkey", "datadate", "visibility_timestamp"]).sort_values(["gvkey", "datadate"])


def _resolve_manifest_path(raw_path: Path, manifest_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path
    if raw_path.exists():
        return raw_path
    return manifest_path.parent / raw_path


def _pool_factor_ids(component_pool: pd.DataFrame) -> set[str]:
    if component_pool.empty or "factor_id" not in component_pool.columns:
        return set()
    eligible = component_pool.copy()
    if "component_pool_eligible" in eligible.columns:
        eligible = eligible[eligible["component_pool_eligible"].map(_as_bool)]
    if "filter_class" in eligible.columns:
        eligible = eligible[~eligible["filter_class"].astype(str).eq("hard_excluded")]
    return set(eligible["factor_id"].astype(str))


def _observed_factor_ids(observations: pd.DataFrame) -> set[str]:
    if observations.empty or "factor_id" not in observations.columns:
        return set()
    return set(observations["factor_id"].astype(str))


def _build_generated_observations(
    target_ids: list[str],
    source_observations: pd.DataFrame,
    prices: pd.DataFrame,
    universe: pd.DataFrame,
    benchmark: pd.DataFrame,
    annual_fundamentals: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    dates = sorted(pd.Timestamp(value) for value in prices["date"].dropna().unique())
    month_ends = _month_end_trading_dates(dates)
    source_dates = _source_rebalance_dates(source_observations, month_ends)
    benchmark_lookup = benchmark.set_index("date")["adjusted_close"].to_dict()
    generated: dict[str, list[dict[str, Any]]] = {factor_id: [] for factor_id in target_ids}
    for signal_date in source_dates:
        signal_position = _date_position(dates, signal_date)
        if signal_position is None or signal_position < _MIN_PRICE_HISTORY_DAYS:
            continue
        next_month = _next_month_end(month_ends, signal_date)
        if next_month is None:
            continue
        tradable_position = signal_position + 1
        if tradable_position >= len(dates):
            continue
        tradable_date = dates[tradable_position]
        if next_month <= tradable_date:
            continue
        history_cutoff = dates[signal_position - 1]
        base = _base_panel_for_date(prices, universe, signal_date)
        forward = _forward_returns(prices, tradable_date, next_month)
        exposure = _exposures_for_date(universe, base, signal_date)
        merged = base.merge(exposure, on="asset_id", how="left").merge(forward, on="asset_id", how="inner")
        if len(merged) < _MIN_ASSETS:
            continue
        merged = _attach_fundamental_signals(merged, annual_fundamentals, signal_date)
        qqq_return = _benchmark_return(benchmark_lookup, tradable_date, next_month)
        for factor_id in target_ids:
            signal_column = f"{factor_id}_signal"
            if signal_column not in merged.columns:
                continue
            row = _observation_row(
                factor_id=factor_id,
                signal_column=signal_column,
                frame=merged,
                signal_date=signal_date,
                history_cutoff=history_cutoff,
                tradable_date=tradable_date,
                horizon_end=next_month,
                qqq_return=qqq_return,
            )
            if row is not None:
                generated[factor_id].append(row)
    return generated


def _source_rebalance_dates(source_observations: pd.DataFrame, fallback_dates: list[pd.Timestamp]) -> list[pd.Timestamp]:
    if not source_observations.empty and "rebalance_date" in source_observations.columns:
        return sorted(pd.to_datetime(source_observations["rebalance_date"], errors="coerce").dropna().unique())
    return fallback_dates[:-1]


def _next_month_end(month_ends: list[pd.Timestamp], signal_date: pd.Timestamp) -> pd.Timestamp | None:
    try:
        index = month_ends.index(signal_date)
    except ValueError:
        return None
    next_index = index + 1
    if next_index >= len(month_ends):
        return None
    return month_ends[next_index]


def _base_panel_for_date(prices: pd.DataFrame, universe: pd.DataFrame, signal_date: pd.Timestamp) -> pd.DataFrame:
    active = universe[(universe["membership_start"] <= signal_date) & (universe["membership_end"] >= signal_date)].copy()
    if active.empty:
        active = universe.copy()
    active = active.sort_values(["asset_id", "membership_start"]).drop_duplicates("asset_id", keep="last")
    rows: list[dict[str, Any]] = []
    for asset_id, history in prices[prices["date"] <= signal_date].groupby("asset_id"):
        history = history.sort_values("date")
        if len(history) < _MIN_LIQUIDITY_HISTORY_DAYS:
            continue
        close = pd.to_numeric(history["adjusted_close"], errors="coerce").reset_index(drop=True)
        volume = pd.to_numeric(history["volume"], errors="coerce").reset_index(drop=True)
        returns = pd.to_numeric(history["daily_return"], errors="coerce").reset_index(drop=True)
        shrout = pd.to_numeric(history.get("shrout"), errors="coerce").reset_index(drop=True) if "shrout" in history.columns else pd.Series(dtype=float)
        dollar_volume = close * volume
        turnover = volume / shrout.replace(0.0, np.nan) if not shrout.empty else pd.Series(np.nan, index=volume.index)
        market_cap = _latest_market_cap(history)
        rows.append(
            {
                "asset_id": str(asset_id),
                "market_cap": market_cap,
                "liquidity_score_60d": float(np.log1p(dollar_volume.iloc[-60:].mean())),
                "turnover_score_60d": float(turnover.iloc[-60:].mean()) if turnover.iloc[-60:].notna().any() else np.nan,
                "volatility_score_60d": float(returns.iloc[-60:].std()),
            }
        )
    base = pd.DataFrame(rows)
    if base.empty:
        return base
    base = base.merge(active[["asset_id", "gvkey"]], on="asset_id", how="left")
    base["gvkey"] = base["gvkey"].map(_normalize_identifier)
    base["liquidity_turnover_signal"] = _zscore(base["liquidity_score_60d"]) - _zscore(base["turnover_score_60d"])
    return base


def _attach_fundamental_signals(
    frame: pd.DataFrame,
    fundamentals: pd.DataFrame,
    signal_date: pd.Timestamp,
) -> pd.DataFrame:
    if fundamentals.empty or "gvkey" not in frame.columns:
        return frame
    visible = fundamentals[fundamentals["visibility_timestamp"] <= signal_date].copy()
    if visible.empty:
        return frame
    visible = visible.sort_values(["gvkey", "datadate"])
    latest = visible.groupby("gvkey", as_index=False).tail(1).copy()
    previous = visible.groupby("gvkey", as_index=False).nth(-2).reset_index(drop=True)
    previous = previous[["gvkey", "at"]].rename(columns={"at": "previous_at"})
    latest = latest.merge(previous, on="gvkey", how="left")
    latest = latest[
        [
            "gvkey",
            "ceq",
            "at",
            "oibdp",
            "ib",
            "oancf",
            "mkvalt",
            "previous_at",
            "visibility_timestamp",
            "tradable_timestamp",
        ]
    ].copy()
    merged = frame.merge(latest, on="gvkey", how="left")
    market_cap = pd.to_numeric(merged["market_cap"], errors="coerce").where(
        pd.to_numeric(merged["market_cap"], errors="coerce").gt(0.0),
        pd.to_numeric(merged["mkvalt"], errors="coerce"),
    )
    at = pd.to_numeric(merged["at"], errors="coerce")
    previous_at = pd.to_numeric(merged["previous_at"], errors="coerce")
    merged["value_bm_signal"] = pd.to_numeric(merged["ceq"], errors="coerce") / market_cap.replace(0.0, np.nan)
    merged["profitability_quality_signal"] = pd.to_numeric(merged["oibdp"], errors="coerce") / at.replace(0.0, np.nan)
    merged["investment_asset_growth_signal"] = -((at / previous_at.replace(0.0, np.nan)) - 1.0)
    merged["accruals_signal"] = -(
        (pd.to_numeric(merged["ib"], errors="coerce") - pd.to_numeric(merged["oancf"], errors="coerce"))
        / at.replace(0.0, np.nan)
    )
    return merged


def _observation_row(
    factor_id: str,
    signal_column: str,
    frame: pd.DataFrame,
    signal_date: pd.Timestamp,
    history_cutoff: pd.Timestamp,
    tradable_date: pd.Timestamp,
    horizon_end: pd.Timestamp,
    qqq_return: float | None,
) -> dict[str, Any] | None:
    factor_rows = frame[
        [
            "asset_id",
            signal_column,
            "forward_return",
            "sector",
            "industry",
            "market_cap",
            "liquidity_score_60d",
            "volatility_score_60d",
        ]
    ].dropna(subset=[signal_column, "forward_return"])
    if len(factor_rows) < _MIN_ASSETS:
        return None
    ic = factor_rows[signal_column].corr(factor_rows["forward_return"], method="spearman")
    gross_spread = _top_bottom_spread(factor_rows, signal_column)
    sector_metrics = _adjusted_metrics(factor_rows, signal_column, include_sector=True, include_style=False)
    style_metrics = _adjusted_metrics(factor_rows, signal_column, include_sector=True, include_style=True)
    return {
        "schema_version": "real_rolling_oos_observation.v1",
        "factor_id": factor_id,
        "rebalance_date": _date_str(signal_date),
        "history_cutoff_date": _date_str(history_cutoff),
        "signal_date": _date_str(signal_date),
        "tradable_date": _date_str(tradable_date),
        "horizon_end_date": _date_str(horizon_end),
        "same_close_trading_used": False,
        "full_sample_icir_used": False,
        "prior_history_only": True,
        "rank_ic": round(float(ic), 8) if pd.notna(ic) else None,
        "gross_spread": round(float(gross_spread), 8),
        "qqq_return": round(float(qqq_return), 8) if qqq_return is not None else None,
        "qqq_relative_spread": round(float(gross_spread - (qqq_return or 0.0)), 8),
        "beta_adjusted_spread": round(float(gross_spread - 0.5 * (qqq_return or 0.0)), 8),
        "sector_adjusted_rank_ic": _round_optional(sector_metrics["rank_ic"]),
        "sector_adjusted_spread": _round_optional(sector_metrics["spread"]),
        "sector_adjusted_status": sector_metrics["status"],
        "style_adjusted_rank_ic": _round_optional(style_metrics["rank_ic"]),
        "style_adjusted_spread": _round_optional(style_metrics["spread"]),
        "style_adjusted_status": style_metrics["status"],
        "style_model_scope": str(style_metrics["status"]).replace("observed_", ""),
        "net_spread": round(float(gross_spread - _COST_DRAG), 8),
        "sector_adjusted_net_spread": _round_optional(float(sector_metrics["spread"]) - _COST_DRAG)
        if pd.notna(sector_metrics["spread"])
        else None,
        "style_adjusted_net_spread": _round_optional(float(style_metrics["spread"]) - _COST_DRAG)
        if pd.notna(style_metrics["spread"])
        else None,
        "cost_drag": _COST_DRAG,
        "asset_count": len(factor_rows),
        "observation_source": _observation_source(factor_id),
        "not_alpha_evidence": True,
    }


def _observation_source(factor_id: str) -> str:
    if factor_id == "liquidity_turnover":
        return "wrds_daily_price_volume_trailing_signal"
    if factor_id in _FUNDAMENTAL_FACTORS:
        return "wrds_compustat_lagged_fundamental_signal"
    return "unknown"


def _merge_observations(source: pd.DataFrame, generated: pd.DataFrame) -> pd.DataFrame:
    if source.empty:
        expanded = generated.copy()
    elif generated.empty:
        expanded = source.copy()
    else:
        expanded = pd.concat([source, generated], ignore_index=True, sort=False)
    if expanded.empty:
        return expanded
    expanded["factor_id"] = expanded["factor_id"].astype(str)
    expanded["rebalance_date"] = pd.to_datetime(expanded["rebalance_date"], errors="coerce")
    expanded = expanded.sort_values(["rebalance_date", "factor_id"]).drop_duplicates(
        ["factor_id", "rebalance_date"],
        keep="first",
    )
    expanded["rebalance_date"] = expanded["rebalance_date"].dt.date.astype(str)
    return expanded


def _enablement_row(factor_id: str, rows: list[dict[str, Any]], fundamentals_manifest_exists: bool) -> dict[str, Any]:
    if rows:
        status = "generated_lagged_fundamental_oos_observations" if factor_id in _FUNDAMENTAL_FACTORS else "generated_price_volume_oos_observations"
        reason = "observed"
    elif factor_id in _FUNDAMENTAL_FACTORS and not fundamentals_manifest_exists:
        status = "unavailable_missing_fundamentals_manifest"
        reason = "missing_fundamentals_manifest"
    else:
        status = "unavailable_insufficient_oos_coverage"
        reason = "insufficient_coverage"
    return {
        "schema_version": "component_oos_observation_enablement.v1",
        "factor_id": factor_id,
        "enablement_status": status,
        "unavailable_reason": reason,
        "generated_observation_count": len(rows),
        "reporting_lag_days": 90 if factor_id in _FUNDAMENTAL_FACTORS else 0,
        "fabricated_returns": False,
        "not_alpha_evidence": True,
        "production_approval": False,
        "direct_q2_entry": False,
    }


def _unavailable_enablement_row(factor_id: str) -> dict[str, Any]:
    status = "unavailable_missing_event_visibility_path" if factor_id == "sue_event_reference" else "unavailable_not_targeted"
    reason = "missing_event_timestamp" if factor_id == "sue_event_reference" else "not_targeted"
    return {
        "schema_version": "component_oos_observation_enablement.v1",
        "factor_id": factor_id,
        "enablement_status": status,
        "unavailable_reason": reason,
        "generated_observation_count": 0,
        "reporting_lag_days": 0,
        "fabricated_returns": False,
        "not_alpha_evidence": True,
        "production_approval": False,
        "direct_q2_entry": False,
    }


def _finalize_enablement_report(
    rows: list[dict[str, Any]],
    observed_after: list[str],
    unavailable_after: list[str],
) -> pd.DataFrame:
    by_factor = {str(row["factor_id"]): row for row in rows}
    for factor_id in observed_after:
        by_factor.setdefault(
            factor_id,
            {
                "schema_version": "component_oos_observation_enablement.v1",
                "factor_id": factor_id,
                "enablement_status": "already_observed",
                "unavailable_reason": "observed",
                "generated_observation_count": 0,
                "reporting_lag_days": 0,
                "fabricated_returns": False,
                "not_alpha_evidence": True,
                "production_approval": False,
                "direct_q2_entry": False,
            },
        )
    for factor_id in unavailable_after:
        by_factor.setdefault(factor_id, _unavailable_enablement_row(factor_id))
    return pd.DataFrame([by_factor[key] for key in sorted(by_factor)])


def _summary(
    source_observation_path: Path,
    component_pool_path: Path,
    daily_manifest_path: Path,
    fundamentals_manifest_path: Path,
    source_observed_count: int,
    generated_ids: list[str],
    observed_after: list[str],
    unavailable_after: list[str],
    generated_row_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": "component_oos_observation_expansion_summary.v1",
        "source_observation_path": str(source_observation_path),
        "component_pool_path": str(component_pool_path),
        "daily_manifest_path": str(daily_manifest_path),
        "fundamentals_manifest_path": str(fundamentals_manifest_path),
        "source_observed_factor_count": source_observed_count,
        "generated_factor_ids": generated_ids,
        "generated_observation_count": generated_row_count,
        "observed_factor_ids_after_expansion": observed_after,
        "observed_factor_count_after_expansion": len(observed_after),
        "unavailable_factor_ids_after_expansion": unavailable_after,
        "unavailable_factor_count_after_expansion": len(unavailable_after),
        "fabricated_returns": False,
        "same_close_trading_used": False,
        "full_sample_icir_used": False,
        "prior_history_only": True,
        "alpha_success_claimed": False,
        "or_optimizer_unlocked": False,
        "security_level_portfolio_construction_used": False,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "not_alpha_evidence": True,
    }


def _zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    std = float(numeric.std(ddof=0))
    if not np.isfinite(std) or std == 0.0:
        return pd.Series(0.0, index=series.index)
    return (numeric.fillna(numeric.median()) - float(numeric.mean())) / std


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _normalize_identifier(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text
