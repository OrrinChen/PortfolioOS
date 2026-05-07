from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from multifactor_alpha_validation.data_contract import run_research_mode_preflight


@dataclass(frozen=True)
class RealRollingOOSResult:
    preflight_ready: bool
    oos_status: str
    dataset_frequency: str
    uses_full_sample_icir: bool
    alpha_success_claimed: bool
    output_dir: str
    summary_path: str
    evidence_path: str
    observation_path: str
    exposure_path: str
    neutralization_path: str
    benchmark_attribution_path: str
    survival_funnel_path: str
    readiness_path: str


_FACTORS = ("momentum_12_1", "reversal_5_1", "low_vol_60d")
_MIN_HISTORY_DAYS = 252
_COST_DRAG = 0.001


def run_first_real_rolling_oos_evidence(manifest_path: Path, output_dir: Path) -> RealRollingOOSResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = run_research_mode_preflight(manifest_path, output_dir / "preflight")
    if not preflight.research_mode_ready:
        raise ValueError(f"research preflight is blocked: {list(preflight.blockers)}")

    manifest = _load_manifest(manifest_path)
    universe = _load_csv(manifest, manifest_path, "universe")
    prices = _load_csv(manifest, manifest_path, "prices")
    benchmark = _load_csv(manifest, manifest_path, "benchmark")
    universe = _normalize_universe(universe)
    prices = _normalize_prices(prices)
    benchmark = _normalize_benchmark(benchmark)
    frequency = _detect_frequency(prices)

    summary_path = output_dir / "real_oos_summary.json"
    evidence_path = output_dir / "real_oos_factor_evidence.csv"
    observation_path = output_dir / "real_oos_observations.csv"
    exposure_path = output_dir / "real_oos_exposure_panel.csv"
    neutralization_path = output_dir / "real_oos_neutralization_report.csv"
    benchmark_path = output_dir / "real_oos_benchmark_attribution.csv"
    survival_path = output_dir / "real_oos_survival_funnel.csv"
    readiness_path = output_dir / "real_oos_readiness.md"

    if frequency != "daily":
        _write_empty_outputs(evidence_path, observation_path, exposure_path, neutralization_path, benchmark_path, survival_path)
        summary = _summary(
            manifest_path=manifest_path,
            frequency=frequency,
            status="needs_daily_price_volume",
            blocker="daily_price_volume_required",
            observation_count=0,
        )
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        readiness_path.write_text(_render_readiness(summary), encoding="utf-8")
        return RealRollingOOSResult(
            preflight_ready=True,
            oos_status="needs_daily_price_volume",
            dataset_frequency=frequency,
            uses_full_sample_icir=False,
            alpha_success_claimed=False,
            output_dir=str(output_dir),
            summary_path=str(summary_path),
            evidence_path=str(evidence_path),
            observation_path=str(observation_path),
            exposure_path=str(exposure_path),
            neutralization_path=str(neutralization_path),
            benchmark_attribution_path=str(benchmark_path),
            survival_funnel_path=str(survival_path),
            readiness_path=str(readiness_path),
        )

    observations, exposure_panel = _build_observations(prices, benchmark, universe)
    if observations.empty:
        _write_empty_outputs(evidence_path, observation_path, exposure_path, neutralization_path, benchmark_path, survival_path)
        summary = _summary(
            manifest_path=manifest_path,
            frequency=frequency,
            status="blocked",
            blocker="insufficient_daily_observations",
            observation_count=0,
        )
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        readiness_path.write_text(_render_readiness(summary), encoding="utf-8")
        return RealRollingOOSResult(
            preflight_ready=True,
            oos_status="blocked",
            dataset_frequency=frequency,
            uses_full_sample_icir=False,
            alpha_success_claimed=False,
            output_dir=str(output_dir),
            summary_path=str(summary_path),
            evidence_path=str(evidence_path),
            observation_path=str(observation_path),
            exposure_path=str(exposure_path),
            neutralization_path=str(neutralization_path),
            benchmark_attribution_path=str(benchmark_path),
            survival_funnel_path=str(survival_path),
            readiness_path=str(readiness_path),
        )

    evidence = _build_factor_evidence(observations)
    neutralization = _build_neutralization_report(evidence)
    benchmark_attribution = _build_benchmark_attribution(evidence)
    survival = _build_survival_funnel(evidence)
    summary = _summary(
        manifest_path=manifest_path,
        frequency=frequency,
        status="evidence_ready",
        blocker=None,
        observation_count=len(observations),
    )

    observations.to_csv(observation_path, index=False)
    exposure_panel.to_csv(exposure_path, index=False)
    evidence.to_csv(evidence_path, index=False)
    neutralization.to_csv(neutralization_path, index=False)
    benchmark_attribution.to_csv(benchmark_path, index=False)
    survival.to_csv(survival_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    readiness_path.write_text(_render_readiness(summary), encoding="utf-8")
    return RealRollingOOSResult(
        preflight_ready=True,
        oos_status="evidence_ready",
        dataset_frequency=frequency,
        uses_full_sample_icir=False,
        alpha_success_claimed=False,
        output_dir=str(output_dir),
        summary_path=str(summary_path),
        evidence_path=str(evidence_path),
        observation_path=str(observation_path),
        exposure_path=str(exposure_path),
        neutralization_path=str(neutralization_path),
        benchmark_attribution_path=str(benchmark_path),
        survival_funnel_path=str(survival_path),
        readiness_path=str(readiness_path),
    )


def _load_manifest(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("research dataset manifest must be a mapping")
    return payload


def _load_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    section_payload = manifest.get(section)
    if not isinstance(section_payload, Mapping):
        raise ValueError(f"{section} section is required")
    raw_path = Path(str(section_payload.get("path", "")))
    path = raw_path if raw_path.is_absolute() else manifest_path.parent / raw_path
    return pd.read_csv(path)


def _normalize_universe(universe: pd.DataFrame) -> pd.DataFrame:
    normalized = universe.copy()
    if "asset_id" not in normalized.columns:
        if "permno" in normalized.columns:
            normalized["asset_id"] = normalized["permno"].astype(str)
        elif "ticker" in normalized.columns:
            normalized["asset_id"] = normalized["ticker"].astype(str)
        else:
            raise ValueError("universe panel requires asset_id, permno, or ticker")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("membership_start", "membership_end", "entry_date", "exit_date", "date"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if "membership_start" not in normalized.columns:
        normalized["membership_start"] = normalized.get("entry_date", pd.NaT)
    if "membership_end" not in normalized.columns:
        normalized["membership_end"] = normalized.get("exit_date", pd.NaT)
    normalized["membership_start"] = normalized["membership_start"].fillna(pd.Timestamp("1900-01-01"))
    normalized["membership_end"] = normalized["membership_end"].fillna(pd.Timestamp("2100-01-01"))
    if "sector" not in normalized.columns:
        normalized["sector"] = pd.NA
    if "industry" not in normalized.columns:
        normalized["industry"] = pd.NA
    normalized["sector"] = normalized["sector"].fillna("unknown_sector").astype(str)
    normalized["industry"] = normalized["industry"].fillna("unknown_industry").astype(str)
    return normalized.sort_values(["asset_id", "membership_start"])


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    normalized = prices.copy()
    if "asset_id" not in normalized.columns:
        if "permno" in normalized.columns:
            normalized["asset_id"] = normalized["permno"].astype(str)
        elif "ticker" in normalized.columns:
            normalized["asset_id"] = normalized["ticker"].astype(str)
        else:
            raise ValueError("price panel requires asset_id, permno, or ticker")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    normalized["volume"] = pd.to_numeric(normalized.get("volume"), errors="coerce")
    for optional in ("market_cap", "dlycap", "shrout", "dlyprcvol"):
        if optional in normalized.columns:
            normalized[optional] = pd.to_numeric(normalized[optional], errors="coerce")
    normalized = normalized.dropna(subset=["asset_id", "date", "adjusted_close"]).sort_values(["asset_id", "date"])
    normalized["daily_return"] = normalized.groupby("asset_id")["adjusted_close"].pct_change()
    return normalized


def _normalize_benchmark(benchmark: pd.DataFrame) -> pd.DataFrame:
    normalized = benchmark.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "adjusted_close"]).sort_values("date")
    normalized["benchmark_forward_return"] = normalized["adjusted_close"].shift(-1) / normalized["adjusted_close"] - 1.0
    return normalized


def _detect_frequency(prices: pd.DataFrame) -> str:
    convention = ""
    if "adjusted_price_convention" in prices.columns and not prices.empty:
        convention = str(prices["adjusted_price_convention"].dropna().astype(str).head(1).iloc[0]).lower()
    if "monthly" in convention or "mth" in convention:
        return "monthly"
    dates = pd.Series(sorted(prices["date"].dropna().unique()))
    if len(dates) < 2:
        return "unknown"
    median_gap = dates.diff().dropna().dt.days.median()
    if median_gap <= 7:
        return "daily"
    if median_gap >= 25:
        return "monthly"
    return "unknown"


def _build_observations(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    universe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(pd.Timestamp(value) for value in prices["date"].dropna().unique())
    rebalance_dates = _month_end_trading_dates(dates)
    benchmark_lookup = benchmark.set_index("date")["adjusted_close"].to_dict()
    rows: list[dict[str, object]] = []
    exposure_rows: list[dict[str, object]] = []
    for index, signal_date in enumerate(rebalance_dates[:-1]):
        signal_position = _date_position(dates, signal_date)
        if signal_position is None or signal_position < _MIN_HISTORY_DAYS:
            continue
        tradable_position = signal_position + 1
        if tradable_position >= len(dates):
            continue
        tradable_date = dates[tradable_position]
        horizon_end = rebalance_dates[index + 1]
        if horizon_end <= tradable_date:
            continue
        history_cutoff = dates[signal_position - 1]
        signal_frame = _signals_for_date(prices, signal_date)
        exposure_frame = _exposures_for_date(universe, signal_frame, signal_date)
        forward_frame = _forward_returns(prices, tradable_date, horizon_end)
        merged = signal_frame.merge(exposure_frame, on="asset_id", how="left").merge(forward_frame, on="asset_id", how="inner")
        if len(merged) < 3:
            continue
        exposure_rows.extend(_exposure_records(signal_date, merged))
        qqq_return = _benchmark_return(benchmark_lookup, tradable_date, horizon_end)
        for factor_id in _FACTORS:
            column = f"{factor_id}_signal"
            factor_rows = merged[
                [
                    "asset_id",
                    column,
                    "forward_return",
                    "sector",
                    "industry",
                    "liquidity_score_60d",
                    "volatility_score_60d",
                ]
            ].dropna(subset=[column, "forward_return"])
            if len(factor_rows) < 3:
                continue
            ic = factor_rows[column].corr(factor_rows["forward_return"], method="spearman")
            gross_spread = _top_bottom_spread(factor_rows, column)
            sector_metrics = _adjusted_metrics(factor_rows, column, include_sector=True, include_style=False)
            style_metrics = _adjusted_metrics(factor_rows, column, include_sector=True, include_style=True)
            rows.append(
                {
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
                    "gross_spread": round(gross_spread, 8),
                    "qqq_return": round(qqq_return, 8) if qqq_return is not None else None,
                    "qqq_relative_spread": round(gross_spread - (qqq_return or 0.0), 8),
                    "beta_adjusted_spread": round(gross_spread - 0.5 * (qqq_return or 0.0), 8),
                    "sector_adjusted_rank_ic": _round_optional(sector_metrics["rank_ic"]),
                    "sector_adjusted_spread": _round_optional(sector_metrics["spread"]),
                    "sector_adjusted_status": sector_metrics["status"],
                    "style_adjusted_rank_ic": _round_optional(style_metrics["rank_ic"]),
                    "style_adjusted_spread": _round_optional(style_metrics["spread"]),
                    "style_adjusted_status": style_metrics["status"],
                    "style_model_scope": "price_volume_proxy",
                    "net_spread": round(gross_spread - _COST_DRAG, 8),
                    "sector_adjusted_net_spread": _round_optional(float(sector_metrics["spread"]) - _COST_DRAG)
                    if pd.notna(sector_metrics["spread"])
                    else None,
                    "style_adjusted_net_spread": _round_optional(float(style_metrics["spread"]) - _COST_DRAG)
                    if pd.notna(style_metrics["spread"])
                    else None,
                    "cost_drag": _COST_DRAG,
                    "asset_count": len(factor_rows),
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(exposure_rows)


def _signals_for_date(prices: pd.DataFrame, signal_date: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for asset_id, history in prices[prices["date"] <= signal_date].groupby("asset_id"):
        history = history.sort_values("date")
        if len(history) <= _MIN_HISTORY_DAYS:
            continue
        close = history["adjusted_close"].reset_index(drop=True)
        returns = history["daily_return"].reset_index(drop=True)
        volume = pd.to_numeric(history["volume"], errors="coerce").reset_index(drop=True)
        dollar_volume = close * volume
        rows.append(
            {
                "asset_id": str(asset_id),
                "momentum_12_1_signal": close.iloc[-22] / close.iloc[-253] - 1.0,
                "reversal_5_1_signal": -(close.iloc[-1] / close.iloc[-6] - 1.0),
                "low_vol_60d_signal": -float(returns.iloc[-60:].std()),
                "market_cap": _latest_market_cap(history),
                "liquidity_score_60d": float(np.log1p(dollar_volume.iloc[-60:].mean())),
                "volatility_score_60d": float(returns.iloc[-60:].std()),
                "style_source": "price_volume_proxy",
            }
        )
    return pd.DataFrame(rows)


def _latest_market_cap(history: pd.DataFrame) -> float | None:
    for column in ("market_cap", "dlycap"):
        if column in history.columns:
            value = pd.to_numeric(history[column], errors="coerce").dropna()
            if not value.empty:
                return float(value.iloc[-1])
    if {"shrout", "adjusted_close"} <= set(history.columns):
        shrout = pd.to_numeric(history["shrout"], errors="coerce").dropna()
        close = pd.to_numeric(history["adjusted_close"], errors="coerce").dropna()
        if not shrout.empty and not close.empty:
            return float(shrout.iloc[-1] * close.iloc[-1])
    return None


def _exposures_for_date(universe: pd.DataFrame, signal_frame: pd.DataFrame, signal_date: pd.Timestamp) -> pd.DataFrame:
    active = universe[
        (universe["membership_start"] <= signal_date)
        & (universe["membership_end"] >= signal_date)
    ].copy()
    if active.empty:
        active = universe.copy()
    active = active.sort_values(["asset_id", "membership_start"]).drop_duplicates("asset_id", keep="last")
    exposures = signal_frame[["asset_id"]].merge(active[["asset_id", "sector", "industry"]], on="asset_id", how="left")
    exposures["sector"] = exposures["sector"].fillna("unknown_sector").astype(str)
    exposures["industry"] = exposures["industry"].fillna("unknown_industry").astype(str)
    return exposures


def _exposure_records(signal_date: pd.Timestamp, merged: pd.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    columns = [
        "asset_id",
        "sector",
        "industry",
        "market_cap",
        "liquidity_score_60d",
        "volatility_score_60d",
        "style_source",
    ]
    for row in merged[columns].drop_duplicates("asset_id").itertuples(index=False):
        records.append(
            {
                "schema_version": "real_oos_exposure_panel.v1",
                "signal_date": _date_str(signal_date),
                "asset_id": row.asset_id,
                "sector": row.sector,
                "industry": row.industry,
                "market_cap": row.market_cap if pd.notna(row.market_cap) else None,
                "liquidity_score_60d": _round_optional(row.liquidity_score_60d),
                "volatility_score_60d": _round_optional(row.volatility_score_60d),
                "style_source": row.style_source,
            }
        )
    return records


def _forward_returns(prices: pd.DataFrame, tradable_date: pd.Timestamp, horizon_end: pd.Timestamp) -> pd.DataFrame:
    start = prices[prices["date"] == tradable_date][["asset_id", "adjusted_close"]].rename(
        columns={"adjusted_close": "start_close"}
    )
    end = prices[prices["date"] == horizon_end][["asset_id", "adjusted_close"]].rename(
        columns={"adjusted_close": "end_close"}
    )
    merged = start.merge(end, on="asset_id", how="inner")
    merged["forward_return"] = merged["end_close"] / merged["start_close"] - 1.0
    return merged[["asset_id", "forward_return"]]


def _benchmark_return(
    benchmark_lookup: Mapping[pd.Timestamp, float],
    tradable_date: pd.Timestamp,
    horizon_end: pd.Timestamp,
) -> float | None:
    start = benchmark_lookup.get(tradable_date)
    end = benchmark_lookup.get(horizon_end)
    if start is None or end is None or start == 0:
        return None
    return float(end / start - 1.0)


def _month_end_trading_dates(dates: list[pd.Timestamp]) -> list[pd.Timestamp]:
    frame = pd.DataFrame({"date": dates})
    frame["month"] = frame["date"].dt.to_period("M")
    return list(frame.groupby("month")["date"].max())


def _date_position(dates: list[pd.Timestamp], value: pd.Timestamp) -> int | None:
    try:
        return dates.index(value)
    except ValueError:
        return None


def _top_bottom_spread(frame: pd.DataFrame, signal_column: str) -> float:
    ranked = frame.sort_values(signal_column)
    tail = max(1, len(ranked) // 5)
    bottom = ranked.head(tail)["forward_return"].mean()
    top = ranked.tail(tail)["forward_return"].mean()
    return float(top - bottom)


def _adjusted_metrics(
    frame: pd.DataFrame,
    signal_column: str,
    include_sector: bool,
    include_style: bool,
) -> dict[str, float | str]:
    working = frame.dropna(subset=[signal_column, "forward_return"]).copy()
    if len(working) < 3:
        return {"rank_ic": np.nan, "spread": np.nan, "status": "unavailable_insufficient_assets"}
    design_parts: list[np.ndarray] = [np.ones((len(working), 1))]
    if include_sector:
        sector = working.get("sector")
        if sector is None or sector.fillna("unknown_sector").nunique() < 2:
            return {"rank_ic": np.nan, "spread": np.nan, "status": "unavailable_no_sector_panel"}
        dummies = pd.get_dummies(sector.fillna("unknown_sector").astype(str), drop_first=True, dtype=float)
        if not dummies.empty:
            design_parts.append(dummies.to_numpy(dtype=float))
    if include_style:
        style_parts: list[np.ndarray] = []
        for column in ("liquidity_score_60d", "volatility_score_60d"):
            if column not in working.columns:
                continue
            series = pd.to_numeric(working[column], errors="coerce")
            if series.notna().sum() >= 3 and float(series.std(ddof=0)) > 0.0:
                filled = series.fillna(series.median())
                style_parts.append(_zscore(filled).to_numpy().reshape(-1, 1))
        if not style_parts:
            return {"rank_ic": np.nan, "spread": np.nan, "status": "unavailable_no_style_panel"}
        design_parts.extend(style_parts)
    x = np.column_stack(design_parts)
    y = pd.to_numeric(working["forward_return"], errors="coerce").to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    residual = y - x @ beta
    adjusted = working[[signal_column]].copy()
    adjusted["forward_return"] = residual
    rank_ic = adjusted[signal_column].corr(adjusted["forward_return"], method="spearman")
    spread = _top_bottom_spread(adjusted, signal_column)
    status = "observed_price_volume_proxy" if include_style else "observed"
    return {"rank_ic": float(rank_ic) if pd.notna(rank_ic) else np.nan, "spread": spread, "status": status}


def _zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0.0:
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


def _build_factor_evidence(observations: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for factor_id, group in observations.groupby("factor_id", sort=False):
        rows.append(
            {
                "schema_version": "real_oos_factor_evidence.v1",
                "factor_id": factor_id,
                "rebalance_count": len(group),
                "asset_count_mean": round(float(group["asset_count"].mean()), 4),
                "full_sample_icir_used": False,
                "prior_history_only": True,
                "raw_rank_ic_mean": round(float(group["rank_ic"].mean()), 8),
                "gross_spread_mean": round(float(group["gross_spread"].mean()), 8),
                "qqq_relative_spread_mean": round(float(group["qqq_relative_spread"].mean()), 8),
                "beta_adjusted_spread_mean": round(float(group["beta_adjusted_spread"].mean()), 8),
                "sector_adjusted_rank_ic_mean": round(float(group["sector_adjusted_rank_ic"].mean()), 8),
                "sector_adjusted_spread_mean": round(float(group["sector_adjusted_spread"].mean()), 8),
                "sector_adjusted_status": _status_summary(group["sector_adjusted_status"], "observed"),
                "style_adjusted_rank_ic_mean": round(float(group["style_adjusted_rank_ic"].mean()), 8),
                "style_adjusted_spread_mean": round(float(group["style_adjusted_spread"].mean()), 8),
                "style_adjusted_status": _status_summary(group["style_adjusted_status"], "observed_price_volume_proxy"),
                "net_spread_mean": round(float(group["net_spread"].mean()), 8),
                "sector_adjusted_net_spread_mean": round(float(group["sector_adjusted_net_spread"].mean()), 8),
                "style_adjusted_net_spread_mean": round(float(group["style_adjusted_net_spread"].mean()), 8),
                "cost_drag_mean": round(float(group["cost_drag"].mean()), 8),
                "evidence_status": "first_real_oos_diagnostic",
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _status_summary(series: pd.Series, expected: str) -> str:
    values = set(series.dropna().astype(str))
    if values == {expected}:
        return expected
    if any(value.startswith("observed") for value in values):
        return "partial"
    return sorted(values)[0] if values else "unavailable"


def _build_neutralization_report(evidence: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "real_oos_neutralization.v1",
                "factor_id": row.factor_id,
                "beta_adjusted_spread_mean": row.beta_adjusted_spread_mean,
                "sector_adjusted_rank_ic_mean": row.sector_adjusted_rank_ic_mean,
                "sector_adjusted_spread_mean": row.sector_adjusted_spread_mean,
                "sector_adjusted_net_spread_mean": row.sector_adjusted_net_spread_mean,
                "sector_adjusted_status": row.sector_adjusted_status,
                "style_adjusted_rank_ic_mean": row.style_adjusted_rank_ic_mean,
                "style_adjusted_spread_mean": row.style_adjusted_spread_mean,
                "style_adjusted_net_spread_mean": row.style_adjusted_net_spread_mean,
                "style_adjusted_status": row.style_adjusted_status,
                "style_model_scope": "price_volume_proxy",
                "attribution_complete": False,
            }
            for row in evidence.itertuples(index=False)
        ]
    )


def _build_benchmark_attribution(evidence: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "real_oos_benchmark_attribution.v1",
                "factor_id": row.factor_id,
                "raw_spread_mean": row.gross_spread_mean,
                "qqq_relative_spread_mean": row.qqq_relative_spread_mean,
                "beta_adjusted_spread_mean": row.beta_adjusted_spread_mean,
                "sector_adjusted_spread_mean": row.sector_adjusted_spread_mean,
                "style_adjusted_spread_mean": row.style_adjusted_spread_mean,
                "readout_status": "diagnostic_only_not_alpha_claim",
            }
            for row in evidence.itertuples(index=False)
        ]
    )


def _build_survival_funnel(evidence: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": "real_oos_survival_funnel.v1",
                "layer": "pit_pass",
                "factor_count": len(evidence),
            },
            {
                "schema_version": "real_oos_survival_funnel.v1",
                "layer": "first_oos_evaluated",
                "factor_count": len(evidence),
            },
            {
                "schema_version": "real_oos_survival_funnel.v1",
                "layer": "attribution_complete",
                "factor_count": int(
                    (
                        evidence["sector_adjusted_status"].eq("observed")
                        & evidence["style_adjusted_status"].astype(str).str.startswith("observed")
                    ).sum()
                )
                if not evidence.empty
                else 0,
            },
            {
                "schema_version": "real_oos_survival_funnel.v1",
                "layer": "net_positive",
                "factor_count": int(evidence["net_spread_mean"].gt(0).sum()) if not evidence.empty else 0,
            },
            {
                "schema_version": "real_oos_survival_funnel.v1",
                "layer": "style_adjusted_net_positive",
                "factor_count": int(evidence["style_adjusted_net_spread_mean"].gt(0).sum()) if not evidence.empty else 0,
            },
        ]
    )


def _write_empty_outputs(*paths: Path) -> None:
    for path in paths:
        pd.DataFrame().to_csv(path, index=False)


def _summary(
    manifest_path: Path,
    frequency: str,
    status: str,
    blocker: str | None,
    observation_count: int,
) -> dict[str, object]:
    return {
        "schema_version": "real_oos_summary.v1",
        "manifest_path": str(manifest_path),
        "dataset_frequency": frequency,
        "oos_status": status,
        "decision_blocker": blocker,
        "observation_count": observation_count,
        "full_sample_icir_used": False,
        "prior_history_only": True,
        "allocator_ran": False,
        "alpha_success_claimed": False,
        "production_approval": False,
        "live_trading": False,
        "direct_q2_entry": False,
        "not_alpha_evidence": True,
    }


def _round_optional(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 8)


def _render_readiness(summary: Mapping[str, object]) -> str:
    if summary["oos_status"] == "needs_daily_price_volume":
        body = "Daily price-volume data is required before real rolling OOS evidence can be claimed."
    elif summary["oos_status"] == "evidence_ready":
        body = "First real rolling OOS diagnostic evidence was generated."
    else:
        body = f"Rolling OOS evidence is blocked by {summary['decision_blocker']}."
    return "\n".join(
        [
            "# Real Rolling OOS Readiness",
            "",
            f"Status: `{summary['oos_status']}`",
            "",
            body,
            "",
            "This output does not claim alpha success, production approval, live trading, orders, or direct Q2 entry.",
            "",
        ]
    )


def _date_str(value: pd.Timestamp) -> str:
    return value.date().isoformat()
