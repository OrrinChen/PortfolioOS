"""FD-R3 real daily factor replay on admitted PIT data."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml

from .factor_formula_registry import (
    FACTOR_FORMULA_REGISTRY,
    FORMULA_VERSION,
    FormulaInputs,
    compute_factor_frame,
    orient_factor_values,
    required_lookback_days,
)
from .teaching_baseline import FACTOR_NAMES


TRADING_DAYS_PER_MONTH = 21


@dataclass(frozen=True)
class FDRealFactorReplayResult:
    """Artifacts and summary for FD-R3."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_factor_replay(
    manifest_path: str | Path,
    output_dir: str | Path,
    factor_spec_dir: str | Path = Path(
        "projects/multifactor_alpha_validation/factor_discovery_sandbox/factor_specs/price_volume_29"
    ),
) -> FDRealFactorReplayResult:
    """Replay the 29 FD price-volume factors on a daily PIT bundle."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(manifest_file)
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    specs = _load_factor_specs(Path(factor_spec_dir))

    frequency = _detect_frequency(prices)
    if frequency != "daily":
        raise ValueError("FD-R3 requires daily price-volume data")

    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    volume = prices.pivot_table(index="date", columns="asset_id", values="volume", aggfunc="last").reindex(close.index)
    shares_float = _pivot_optional(prices, "shares_float", close.index)
    shares_outstanding = _pivot_optional(prices, "shares_outstanding", close.index)
    classifications = _classification_panel(universe)
    formula_inputs = FormulaInputs(
        close=close,
        volume=volume,
        classifications=classifications,
        shares_float=shares_float,
        shares_outstanding=shares_outstanding,
    )
    qqq_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    signal_dates = _month_end_signal_dates(close.index, qqq_close.index)
    if not signal_dates:
        raise ValueError("FD-R3 requires at least one signal date with a next trading session")

    active_members_by_date = _active_members_by_signal_date(universe, signal_dates)
    price_assets_by_date = _priced_assets_by_date(close, signal_dates)
    next_trading_date = _next_trading_date_map(close.index, signal_dates)

    rows: list[dict[str, object]] = []
    timestamp_rows: list[dict[str, object]] = []
    trading_index = pd.Index(close.index)

    for signal_date in signal_dates:
        tradable_date = next_trading_date[signal_date]
        timestamp_rows.append(
            {
                "schema_version": "fd_real_factor_timestamp_audit.v1",
                "signal_date": _date_str(signal_date),
                "signal_timestamp": f"{_date_str(signal_date)}T16:00:00",
                "visibility_timestamp": f"{_date_str(signal_date + pd.Timedelta(days=1))}T00:00:00",
                "tradable_timestamp": f"{_date_str(tradable_date)}T16:00:00",
                "same_close_trading_used": False,
                "tradable_after_signal": bool(tradable_date > signal_date),
                "timestamp_contract_status": "passed" if tradable_date > signal_date else "blocked",
                "not_alpha_evidence": True,
            }
        )

    for spec in specs:
        factor_id = str(spec["factor_id"])
        formula_spec = FACTOR_FORMULA_REGISTRY[factor_id]
        required_days = int(spec.get("required_lookback_days", required_lookback_days(factor_id)))
        skip_months = int(spec.get("skip", 0))
        skip_days = skip_months * TRADING_DAYS_PER_MONTH
        formula_frame = compute_factor_frame(factor_id, formula_inputs)
        raw_values = formula_frame.values
        oriented_values = orient_factor_values(factor_id, raw_values)
        observations = _lookback_observations(
            factor_id,
            close,
            volume,
            required_days,
            skip_days,
            formula_spec.required_inputs,
        )

        raw_rows = []
        for signal_date in signal_dates:
            active_members = active_members_by_date[signal_date]
            priced_assets = price_assets_by_date[signal_date]
            tradable_date = next_trading_date[signal_date]
            lookback_start, lookback_end = _lookback_bounds(trading_index, signal_date, required_days, skip_days)
            for member in active_members.itertuples(index=False):
                asset_id = str(member.asset_id)
                ticker = str(member.ticker) if "ticker" in active_members.columns and pd.notna(member.ticker) else ""
                raw_value = raw_values.at[signal_date, asset_id] if asset_id in raw_values.columns else np.nan
                oriented_score = (
                    oriented_values.at[signal_date, asset_id] if asset_id in oriented_values.columns else np.nan
                )
                obs = observations.at[signal_date, asset_id] if asset_id in observations.columns else 0
                coverage_status, abstain_reason = _coverage_status(
                    oriented_score,
                    obs,
                    required_days + 1,
                    asset_id,
                    priced_assets,
                )
                fallback_used = (
                    formula_frame.fallback_used.at[signal_date, asset_id]
                    if asset_id in formula_frame.fallback_used.columns
                    else False
                )
                fallback_reason = (
                    formula_frame.fallback_reason.at[signal_date, asset_id]
                    if asset_id in formula_frame.fallback_reason.columns
                    else ""
                )
                evidence_quality = (
                    formula_frame.research_evidence_quality.at[signal_date, asset_id]
                    if asset_id in formula_frame.research_evidence_quality.columns
                    else "standard"
                )
                raw_rows.append(
                    {
                        "schema_version": "fd_real_factor_panel.v2",
                        "factor_id": factor_id,
                        "formula_version": FORMULA_VERSION,
                        "formula_hash": formula_spec.formula_hash,
                        "mechanism_family": formula_spec.mechanism_family,
                        "date": _date_str(signal_date),
                        "rebalance_date": _date_str(signal_date),
                        "asset_id": asset_id,
                        "ticker": ticker,
                        "sector": _member_field(member, "sector"),
                        "industry": _member_field(member, "industry"),
                        "classification_pit_safe": _member_bool(member, "source_is_pit", default=True),
                        "fallback_used": bool(fallback_used),
                        "fallback_reason": str(fallback_reason) if str(fallback_reason) else "none",
                        "research_evidence_quality": str(evidence_quality) if str(evidence_quality) else "standard",
                        "raw_value": raw_value if coverage_status == "active_view" else np.nan,
                        "oriented_score": oriented_score if coverage_status == "active_view" else np.nan,
                        "coverage_status": coverage_status,
                        "abstain_reason": abstain_reason,
                        "signal_timestamp": f"{_date_str(signal_date)}T16:00:00",
                        "visibility_timestamp": f"{_date_str(signal_date + pd.Timedelta(days=1))}T00:00:00",
                        "tradable_timestamp": f"{_date_str(tradable_date)}T16:00:00",
                        "lookback_start": _date_str(lookback_start),
                        "lookback_end": _date_str(lookback_end),
                        "skip_days": skip_days,
                        "lookback_observations": int(obs) if pd.notna(obs) else 0,
                        "expected_horizon": spec.get("expected_horizon", "next_month_excess_return"),
                        "known_correlation_family": spec.get("known_correlation_family", ""),
                        "no_view_is_not_zero_alpha": True,
                        "not_alpha_evidence": True,
                        "direct_q2_entry_allowed": False,
                    }
                )
        factor_frame = pd.DataFrame(raw_rows)
        factor_frame["normalized_value"] = _normalize_factor_values(factor_frame)
        factor_frame["cross_sectional_rank"] = _rank_factor_values(factor_frame)
        rows.extend(factor_frame.to_dict(orient="records"))

    factor_panel = pd.DataFrame(rows)
    factor_panel = factor_panel[
        [
            "schema_version",
            "factor_id",
            "formula_version",
            "formula_hash",
            "mechanism_family",
            "date",
            "rebalance_date",
            "asset_id",
            "ticker",
            "sector",
            "industry",
            "classification_pit_safe",
            "fallback_used",
            "fallback_reason",
            "research_evidence_quality",
            "raw_value",
            "oriented_score",
            "normalized_value",
            "cross_sectional_rank",
            "coverage_status",
            "abstain_reason",
            "signal_timestamp",
            "visibility_timestamp",
            "tradable_timestamp",
            "lookback_start",
            "lookback_end",
            "skip_days",
            "lookback_observations",
            "expected_horizon",
            "known_correlation_family",
            "no_view_is_not_zero_alpha",
            "not_alpha_evidence",
            "direct_q2_entry_allowed",
        ]
    ]
    coverage = _build_coverage(factor_panel)
    timestamp_audit = pd.DataFrame(timestamp_rows)
    parquet_status = _write_parquet_if_available(factor_panel, output_path / "real_factor_panel.parquet")

    artifacts = {
        "real_factor_panel": output_path / "real_factor_panel.csv",
        "real_factor_coverage": output_path / "real_factor_coverage.csv",
        "real_factor_timestamp_audit": output_path / "real_factor_timestamp_audit.csv",
        "real_factor_replay_report": output_path / "real_factor_replay_report.md",
        "real_factor_replay_summary": output_path / "real_factor_replay_summary.json",
        "parquet_status": output_path / "parquet_status.json",
    }
    factor_panel.to_csv(artifacts["real_factor_panel"], index=False)
    coverage.to_csv(artifacts["real_factor_coverage"], index=False)
    timestamp_audit.to_csv(artifacts["real_factor_timestamp_audit"], index=False)
    artifacts["parquet_status"].write_text(json.dumps(parquet_status, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    summary = {
        "schema_version": "fd_real_factor_replay_summary.v1",
        "stage": "FD-R3",
        "dataset_frequency": frequency,
        "manifest_path": str(manifest_file),
        "factor_count": len(specs),
        "formula_version": FORMULA_VERSION,
        "signal_date_count": len(signal_dates),
        "row_count": int(len(factor_panel)),
        "active_view_rows": int((factor_panel["coverage_status"] == "active_view").sum()),
        "explicit_abstain_rows": int((factor_panel["coverage_status"] == "explicit_abstain").sum()),
        "same_close_trading_used": bool(timestamp_audit["same_close_trading_used"].any()),
        "factor_ranking_ran": False,
        "allocator_ran": False,
        "alpha_success_claimed": False,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "parquet_written": parquet_status["parquet_written"],
    }
    artifacts["real_factor_replay_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["real_factor_replay_report"].write_text(_render_report(summary, coverage, parquet_status), encoding="utf-8")
    return FDRealFactorReplayResult(summary=summary, artifacts=artifacts)


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("FD-R3 manifest must be a mapping")
    return payload


def _load_section_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    section_payload = manifest.get(section)
    if not isinstance(section_payload, Mapping):
        raise ValueError(f"manifest section is required: {section}")
    raw_path = section_payload.get("path")
    if not raw_path:
        raise ValueError(f"manifest section path is required: {section}")
    path = Path(str(raw_path))
    if not path.is_absolute():
        path = manifest_path.parent / path
    return pd.read_csv(path)


def _load_factor_specs(spec_dir: Path) -> list[dict[str, object]]:
    specs = []
    for factor_id in FACTOR_NAMES:
        path = spec_dir / f"{factor_id}.yaml"
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError(f"invalid FactorSpec: {path}")
        specs.append(payload)
    return specs


def _normalize_universe(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if "asset_id" not in normalized.columns and "permno" in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("membership_start", "membership_end", "as_of_timestamp", "entry_date", "exit_date"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    return normalized


def _normalize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if "asset_id" not in normalized.columns and "permno" in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    for column in ("adjusted_close", "adjusted_open", "volume", "return", "shares_float", "shares_outstanding"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def _normalize_benchmark(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    for column in ("adjusted_close", "adjusted_open", "volume", "return"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def _detect_frequency(prices: pd.DataFrame) -> str:
    dates = pd.Series(sorted(prices["date"].dropna().unique()))
    if len(dates) < 2:
        return "unknown"
    median_gap = float(dates.diff().dropna().dt.days.median())
    convention = ""
    if "adjusted_price_convention" in prices.columns and prices["adjusted_price_convention"].notna().any():
        convention = str(prices["adjusted_price_convention"].dropna().iloc[0]).lower()
    if "daily" in convention or median_gap <= 7:
        return "daily"
    if "monthly" in convention or "mth" in convention or median_gap >= 25:
        return "monthly"
    return "unknown"


def _month_end_signal_dates(price_dates: pd.Index, benchmark_dates: pd.Index) -> list[pd.Timestamp]:
    benchmark_set = set(pd.to_datetime(benchmark_dates))
    aligned_dates = pd.Index([date for date in pd.to_datetime(price_dates) if date in benchmark_set]).sort_values()
    signal_dates = []
    for _period, group in pd.Series(aligned_dates, index=aligned_dates).groupby(aligned_dates.to_period("M")):
        signal_dates.append(pd.Timestamp(group.iloc[-1]))
    price_set = set(pd.to_datetime(price_dates))
    return [date for date in signal_dates if _next_date(pd.to_datetime(price_dates), date) in price_set]


def _next_date(all_dates: pd.Index, date: pd.Timestamp) -> pd.Timestamp | None:
    position = all_dates.get_loc(date)
    if isinstance(position, slice) or isinstance(position, np.ndarray):
        raise ValueError("price dates must be unique")
    next_position = int(position) + 1
    if next_position >= len(all_dates):
        return None
    return pd.Timestamp(all_dates[next_position])


def _next_trading_date_map(all_dates: pd.Index, signal_dates: list[pd.Timestamp]) -> dict[pd.Timestamp, pd.Timestamp]:
    return {date: _next_date(all_dates, date) for date in signal_dates if _next_date(all_dates, date) is not None}


def _active_members_by_signal_date(universe: pd.DataFrame, signal_dates: list[pd.Timestamp]) -> dict[pd.Timestamp, pd.DataFrame]:
    rows = {}
    max_date = max(signal_dates)
    start = universe["membership_start"].fillna(pd.Timestamp.min)
    end = universe["membership_end"].fillna(max_date)
    as_of = universe["as_of_timestamp"].fillna(start)
    for date in signal_dates:
        active = universe[(start <= date) & (end >= date) & (as_of <= date)].copy()
        if "ticker" not in active.columns:
            active["ticker"] = ""
        for column in ("sector", "industry", "source_is_pit"):
            if column not in active.columns:
                active[column] = "" if column != "source_is_pit" else True
        active = active[["asset_id", "ticker", "sector", "industry", "source_is_pit"]].drop_duplicates(
            "asset_id",
            keep="last",
        ).sort_values("asset_id")
        active["asset_id"] = active["asset_id"].astype(str)
        rows[date] = active
    return rows


def _classification_panel(universe: pd.DataFrame) -> pd.DataFrame:
    columns = ["asset_id", "sector", "industry", "source_is_pit"]
    normalized = universe.copy()
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = "" if column != "source_is_pit" else True
    return normalized[columns].drop_duplicates("asset_id", keep="last")


def _pivot_optional(prices: pd.DataFrame, column: str, index: pd.Index) -> pd.DataFrame | None:
    if column not in prices.columns:
        return None
    return prices.pivot_table(index="date", columns="asset_id", values=column, aggfunc="last").reindex(index)


def _member_field(member: object, field: str) -> str:
    if not hasattr(member, field):
        return ""
    value = getattr(member, field)
    if pd.isna(value):
        return ""
    return str(value)


def _member_bool(member: object, field: str, default: bool) -> bool:
    if not hasattr(member, field):
        return default
    value = getattr(member, field)
    if pd.isna(value):
        return default
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes"}
    return bool(value)


def _priced_assets_by_date(close: pd.DataFrame, signal_dates: list[pd.Timestamp]) -> dict[pd.Timestamp, set[str]]:
    rows = {}
    for date in signal_dates:
        row = close.loc[date]
        rows[date] = set(row[row.notna()].index.astype(str))
    return rows


def _lookback_observations(
    factor_id: str,
    close: pd.DataFrame,
    volume: pd.DataFrame,
    required_days: int,
    skip_days: int,
    required_inputs: list[str],
) -> pd.DataFrame:
    count_frames = []
    if "adjusted_close" in required_inputs or "pit_sector_or_industry" in required_inputs:
        count_frames.append(close.shift(skip_days).rolling(required_days + 1, min_periods=1).count())
    if any("volume" in item or "shares" in item for item in required_inputs):
        count_frames.append(volume.shift(skip_days).rolling(required_days + 1, min_periods=1).count())
    if not count_frames:
        count_frames.append(close.shift(skip_days).rolling(required_days + 1, min_periods=1).count())
    stacked = [frame.stack(future_stack=True) for frame in count_frames]
    return pd.concat(stacked, axis=1).min(axis=1).unstack()


def _lookback_bounds(
    trading_index: pd.Index,
    signal_date: pd.Timestamp,
    lookback_days: int,
    skip_days: int,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    position = trading_index.get_loc(signal_date)
    if isinstance(position, slice) or isinstance(position, np.ndarray):
        raise ValueError("trading dates must be unique")
    end_position = int(position) - skip_days
    start_position = end_position - lookback_days
    if end_position < 0:
        return None, None
    start_position = max(start_position, 0)
    return pd.Timestamp(trading_index[start_position]), pd.Timestamp(trading_index[end_position])


def _coverage_status(
    raw_value: object,
    observations: object,
    minimum_observations: int,
    asset_id: str,
    priced_assets: set[str],
) -> tuple[str, str]:
    if asset_id not in priced_assets:
        return "explicit_abstain", "not_tradable_on_signal_date"
    obs = int(observations) if pd.notna(observations) else 0
    if obs < minimum_observations:
        return "explicit_abstain", "insufficient_price_volume_history"
    if raw_value is None or pd.isna(raw_value) or not np.isfinite(float(raw_value)):
        return "explicit_abstain", "factor_value_unavailable"
    return "active_view", ""


def _normalize_factor_values(factor_frame: pd.DataFrame) -> pd.Series:
    normalized = pd.Series(np.nan, index=factor_frame.index, dtype="float64")
    active = factor_frame["coverage_status"] == "active_view"
    for (_date, _factor), group in factor_frame[active].groupby(["date", "factor_id"]):
        values = pd.to_numeric(group["oriented_score"], errors="coerce")
        std = float(values.std(ddof=0))
        if not np.isfinite(std) or std == 0.0:
            normalized.loc[group.index] = 0.0
        else:
            normalized.loc[group.index] = (values - float(values.mean())) / std
    return normalized


def _rank_factor_values(factor_frame: pd.DataFrame) -> pd.Series:
    ranked = pd.Series(np.nan, index=factor_frame.index, dtype="float64")
    active = factor_frame["coverage_status"] == "active_view"
    for (_date, _factor), group in factor_frame[active].groupby(["date", "factor_id"]):
        values = pd.to_numeric(group["oriented_score"], errors="coerce")
        ranked.loc[group.index] = values.rank(method="average", pct=True)
    return ranked


def _build_coverage(panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for factor_id, group in panel.groupby("factor_id", sort=True):
        total = len(group)
        active = int((group["coverage_status"] == "active_view").sum())
        abstain = int((group["coverage_status"] == "explicit_abstain").sum())
        rows.append(
            {
                "schema_version": "fd_real_factor_coverage.v1",
                "factor_id": factor_id,
                "total_rows": total,
                "covered_rows": active,
                "abstain_rows": abstain,
                "coverage_ratio": round(active / total, 6) if total else 0.0,
                "no_view_is_not_zero_alpha": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _write_parquet_if_available(panel: pd.DataFrame, parquet_path: Path) -> dict[str, object]:
    if importlib.util.find_spec("pyarrow") is None and importlib.util.find_spec("fastparquet") is None:
        return {
            "parquet_written": False,
            "path": str(parquet_path),
            "reason": "pyarrow or fastparquet is not installed; wrote CSV factor panel instead",
        }
    panel.to_parquet(parquet_path, index=False)
    return {"parquet_written": True, "path": str(parquet_path), "reason": ""}


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _render_report(summary: Mapping[str, object], coverage: pd.DataFrame, parquet_status: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# FD-R3 Real Factor Replay Report",
            "",
            "not alpha evidence",
            "formula mechanism validation only",
            "factor ranking: not run",
            "allocator: not run",
            "direct Q2 entry: not allowed",
            "",
            f"- dataset frequency: {summary['dataset_frequency']}",
            f"- factor count: {summary['factor_count']}",
            f"- formula version: {summary['formula_version']}",
            f"- signal date count: {summary['signal_date_count']}",
            f"- panel rows: {summary['row_count']}",
            f"- active view rows: {summary['active_view_rows']}",
            f"- explicit abstain rows: {summary['explicit_abstain_rows']}",
            f"- parquet written: {str(parquet_status['parquet_written']).lower()}",
            "",
            "## Coverage",
            *[
                f"- {row.factor_id}: coverage={row.coverage_ratio:.6f}, abstain_rows={row.abstain_rows}"
                for row in coverage.itertuples(index=False)
            ],
            "",
        ]
    )
