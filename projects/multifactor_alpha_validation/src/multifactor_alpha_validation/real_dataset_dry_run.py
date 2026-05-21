from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from multifactor_alpha_validation.data_contract import run_research_mode_preflight


@dataclass(frozen=True)
class RealDatasetDryRunResult:
    preflight_ready: bool
    dataset_frequency: str
    daily_price_volume_required_for_final_validation: bool
    daily_price_volume_validation_started: bool
    allocator_ran: bool
    factor_ranking_ran: bool
    strategy_return_claimed: bool
    alpha_conclusion_claimed: bool
    output_dir: str
    summary_path: str
    coverage_path: str
    timestamp_alignment_path: str
    universe_snapshot_path: str
    benchmark_alignment_path: str
    delisting_coverage_path: str
    signal_availability_path: str
    daily_long_task_path: str
    report_path: str


def run_real_dataset_dry_run(manifest_path: Path, output_dir: Path) -> RealDatasetDryRunResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = run_research_mode_preflight(manifest_path, output_dir / "preflight")
    if not preflight.research_mode_ready:
        raise ValueError(f"research preflight is blocked: {list(preflight.blockers)}")

    manifest = _load_manifest(manifest_path)
    universe = _load_csv(manifest, manifest_path, "universe")
    prices = _load_csv(manifest, manifest_path, "prices")
    benchmark = _load_csv(manifest, manifest_path, "benchmark")
    delistings = _load_csv(manifest, manifest_path, "delisting")

    universe_key = _asset_key(universe, "historical universe")
    price_key = _asset_key(prices, "price panel")
    delisting_key = _asset_key(delistings, "delisting panel") if not delistings.empty else price_key
    universe = _normalize_universe(universe, universe_key)
    prices = _normalize_dates(prices, ("date",), price_key)
    benchmark = _normalize_dates(benchmark, ("date",))
    delistings = _normalize_dates(delistings, ("delisting_date", "last_trade_date"), delisting_key)

    price_dates = tuple(sorted(date for date in prices["date"].dropna().unique()))
    dataset_frequency, median_gap_days = _detect_frequency(prices)
    monthly_bundle = dataset_frequency == "monthly"

    coverage = _build_coverage(universe, prices, benchmark, universe_key, price_key, price_dates)
    timestamp_alignment = _build_timestamp_alignment(price_dates, manifest.get("timestamp_policy", {}), monthly_bundle)
    universe_snapshots = _build_universe_snapshots(universe, universe_key, price_dates)
    benchmark_alignment = _build_benchmark_alignment(prices, benchmark, price_key, price_dates)
    delisting_coverage = _build_delisting_coverage(universe, delistings, universe_key, delisting_key, price_dates)
    signal_availability = _build_signal_availability(prices, price_key, dataset_frequency)

    coverage_path = output_dir / "real_dataset_coverage.csv"
    timestamp_path = output_dir / "timestamp_alignment.csv"
    universe_snapshot_path = output_dir / "universe_snapshot_summary.csv"
    benchmark_path = output_dir / "benchmark_alignment.csv"
    delisting_path = output_dir / "delisting_coverage.csv"
    signal_path = output_dir / "signal_availability.csv"
    daily_long_task_path = output_dir / "daily_price_volume_long_task.md"
    summary_path = output_dir / "real_dataset_summary.json"
    report_path = output_dir / "real_dataset_dry_run_report.md"

    coverage.to_csv(coverage_path, index=False)
    timestamp_alignment.to_csv(timestamp_path, index=False)
    universe_snapshots.to_csv(universe_snapshot_path, index=False)
    benchmark_alignment.to_csv(benchmark_path, index=False)
    delisting_coverage.to_csv(delisting_path, index=False)
    signal_availability.to_csv(signal_path, index=False)

    summary = _build_summary(
        manifest_path,
        manifest,
        preflight.rows_checked,
        universe,
        prices,
        benchmark,
        delistings,
        dataset_frequency,
        median_gap_days,
        monthly_bundle,
        {
            "coverage": str(coverage_path),
            "timestamp_alignment": str(timestamp_path),
            "universe_snapshots": str(universe_snapshot_path),
            "benchmark_alignment": str(benchmark_path),
            "delisting_coverage": str(delisting_path),
            "signal_availability": str(signal_path),
            "daily_price_volume_long_task": str(daily_long_task_path),
            "report": str(report_path),
        },
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    daily_long_task_path.write_text(_render_daily_long_task(summary), encoding="utf-8")
    report_path.write_text(_render_report(summary), encoding="utf-8")

    return RealDatasetDryRunResult(
        preflight_ready=True,
        dataset_frequency=dataset_frequency,
        daily_price_volume_required_for_final_validation=monthly_bundle,
        daily_price_volume_validation_started=False,
        allocator_ran=False,
        factor_ranking_ran=False,
        strategy_return_claimed=False,
        alpha_conclusion_claimed=False,
        output_dir=str(output_dir),
        summary_path=str(summary_path),
        coverage_path=str(coverage_path),
        timestamp_alignment_path=str(timestamp_path),
        universe_snapshot_path=str(universe_snapshot_path),
        benchmark_alignment_path=str(benchmark_path),
        delisting_coverage_path=str(delisting_path),
        signal_availability_path=str(signal_path),
        daily_long_task_path=str(daily_long_task_path),
        report_path=str(report_path),
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
    raw_path = section_payload.get("path")
    if not raw_path:
        raise ValueError(f"{section} path is required")
    path = Path(str(raw_path))
    if not path.is_absolute():
        path = manifest_path.parent / path
    return pd.read_csv(path)


def _asset_key(frame: pd.DataFrame, label: str) -> str:
    for key in ("asset_id", "permno", "ticker"):
        if key in frame.columns:
            return key
    raise ValueError(f"{label} must include asset_id, permno, or ticker")


def _normalize_dates(frame: pd.DataFrame, date_columns: tuple[str, ...], asset_key: str | None = None) -> pd.DataFrame:
    normalized = frame.copy()
    for column in date_columns:
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if asset_key and asset_key in normalized.columns:
        normalized[asset_key] = normalized[asset_key].astype(str)
    return normalized


def _normalize_universe(universe: pd.DataFrame, asset_key: str) -> pd.DataFrame:
    normalized = _normalize_dates(
        universe,
        ("date", "membership_start", "membership_end", "entry_date", "exit_date", "as_of_timestamp"),
        asset_key,
    )
    if "membership_start" not in normalized.columns:
        normalized["membership_start"] = pd.NaT
    if "membership_end" not in normalized.columns:
        normalized["membership_end"] = pd.NaT
    if "source_is_pit" in normalized.columns:
        normalized["source_is_pit"] = normalized["source_is_pit"].astype(str).str.lower().isin({"true", "1", "yes"})
    return normalized


def _detect_frequency(prices: pd.DataFrame) -> tuple[str, float | None]:
    dates = pd.Series(sorted(prices["date"].dropna().unique()))
    if len(dates) < 2:
        median_gap_days = None
    else:
        gaps = dates.diff().dropna().dt.days
        median_gap_days = float(gaps.median()) if not gaps.empty else None
    convention = ""
    if "adjusted_price_convention" in prices.columns and not prices.empty:
        convention = str(prices["adjusted_price_convention"].dropna().astype(str).head(1).iloc[0]).lower()
    if "monthly" in convention or "mth" in convention:
        return "monthly", median_gap_days
    if median_gap_days is not None and median_gap_days >= 25:
        return "monthly", median_gap_days
    if median_gap_days is not None and median_gap_days <= 7:
        return "daily_or_weekly", median_gap_days
    return "unknown", median_gap_days


def _build_coverage(
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    universe_key: str,
    price_key: str,
    price_dates: tuple[pd.Timestamp, ...],
) -> pd.DataFrame:
    benchmark_dates = set(benchmark["date"].dropna())
    price_assets_by_date = _asset_sets_by_date(prices, price_key)
    rows: list[dict[str, object]] = []
    max_date = max(price_dates) if price_dates else pd.Timestamp.today().normalize()
    for date in price_dates:
        active_assets = _active_assets(universe, universe_key, date, max_date)
        priced_assets = price_assets_by_date.get(date, set())
        priced_active = active_assets & priced_assets
        active_count = len(active_assets)
        rows.append(
            {
                "schema_version": "real_dataset_coverage.v1",
                "date": _date_str(date),
                "active_assets": active_count,
                "priced_active_assets": len(priced_active),
                "missing_active_assets": max(active_count - len(priced_active), 0),
                "coverage_ratio": round(len(priced_active) / active_count, 6) if active_count else None,
                "benchmark_available": date in benchmark_dates,
                "qqq_benchmark_aligned": date in benchmark_dates,
                "readiness_check_only": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_timestamp_alignment(
    price_dates: tuple[pd.Timestamp, ...],
    timestamp_policy: object,
    monthly_bundle: bool,
) -> pd.DataFrame:
    policy = timestamp_policy if isinstance(timestamp_policy, Mapping) else {}
    rows: list[dict[str, object]] = []
    for index, date in enumerate(price_dates):
        next_dataset_date = price_dates[index + 1] if index + 1 < len(price_dates) else pd.NaT
        tradable_available = not pd.isna(next_dataset_date)
        rows.append(
            {
                "schema_version": "real_dataset_timestamp_alignment.v1",
                "signal_date": _date_str(date),
                "signal_timestamp_rule": policy.get("signal", ""),
                "visibility_timestamp_rule": policy.get("visibility", ""),
                "tradable_timestamp_rule": policy.get("tradable", ""),
                "tradable_date_proxy": _date_str(next_dataset_date) if tradable_available else "",
                "tradable_timestamp_basis": "next_available_monthly_observation"
                if monthly_bundle
                else "next_available_dataset_observation",
                "same_close_trading_used": False if tradable_available else None,
                "tradable_after_signal": bool(next_dataset_date > date) if tradable_available else False,
                "allow_same_close_trading": bool(policy.get("allow_same_close_trading")),
                "daily_next_session_proof": False,
                "daily_price_volume_validation_required": monthly_bundle,
                "readiness_check_only": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_universe_snapshots(
    universe: pd.DataFrame,
    universe_key: str,
    price_dates: tuple[pd.Timestamp, ...],
) -> pd.DataFrame:
    max_date = max(price_dates) if price_dates else pd.Timestamp.today().normalize()
    rows: list[dict[str, object]] = []
    for date in price_dates:
        active = _active_assets(universe, universe_key, date, max_date)
        visible = universe[
            (universe["membership_start"].fillna(pd.Timestamp.min) <= date)
            & (universe["as_of_timestamp"].fillna(universe["membership_start"]).fillna(pd.Timestamp.min) <= date)
        ]
        rows.append(
            {
                "schema_version": "real_dataset_universe_snapshot.v1",
                "date": _date_str(date),
                "active_assets": len(active),
                "membership_rows_visible": int(len(visible)),
                "source": _first_value(universe, "source"),
                "source_is_pit": bool(universe.get("source_is_pit", pd.Series(dtype=bool)).all()),
                "readiness_check_only": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_benchmark_alignment(
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    price_key: str,
    price_dates: tuple[pd.Timestamp, ...],
) -> pd.DataFrame:
    benchmark_by_date = benchmark.drop_duplicates("date").set_index("date") if "date" in benchmark.columns else pd.DataFrame()
    price_count_by_date = prices.groupby("date")[price_key].nunique().to_dict()
    rows: list[dict[str, object]] = []
    for date in price_dates:
        qqq_available = date in benchmark_by_date.index
        qqq_close = benchmark_by_date.loc[date, "adjusted_close"] if qqq_available and "adjusted_close" in benchmark_by_date else None
        rows.append(
            {
                "schema_version": "real_dataset_benchmark_alignment.v1",
                "date": _date_str(date),
                "price_assets": int(price_count_by_date.get(date, 0)),
                "benchmark_id": "QQQ",
                "qqq_available": bool(qqq_available),
                "qqq_adjusted_close_available": bool(qqq_available and pd.notna(qqq_close)),
                "benchmark_alignment_status": "aligned" if qqq_available else "missing_benchmark_date",
                "readiness_check_only": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_delisting_coverage(
    universe: pd.DataFrame,
    delistings: pd.DataFrame,
    universe_key: str,
    delisting_key: str,
    price_dates: tuple[pd.Timestamp, ...],
) -> pd.DataFrame:
    if not price_dates:
        return pd.DataFrame(
            [
                {
                    "schema_version": "real_dataset_delisting_coverage.v1",
                    "asset_id": "",
                    "coverage_status": "no_price_dates_available",
                    "readiness_check_only": True,
                    "not_alpha_evidence": True,
                }
            ]
        )
    max_date = max(price_dates)
    membership_end = universe["membership_end"].fillna(max_date)
    inactive = universe[membership_end < max_date].copy()
    if inactive.empty:
        return pd.DataFrame(
            [
                {
                    "schema_version": "real_dataset_delisting_coverage.v1",
                    "asset_id": "",
                    "coverage_status": "no_inactive_membership_rows_in_bundle_window",
                    "inactive_membership_rows": 0,
                    "delisting_rows": int(len(delistings)),
                    "readiness_check_only": True,
                    "not_alpha_evidence": True,
                }
            ]
        )
    delisting_lookup = {}
    if not delistings.empty and delisting_key in delistings.columns:
        for row in delistings.sort_values("delisting_date").itertuples(index=False):
            row_map = row._asdict()
            delisting_lookup.setdefault(str(row_map[delisting_key]), row_map)
    rows: list[dict[str, object]] = []
    for row in inactive.sort_values(["membership_end", universe_key]).itertuples(index=False):
        row_map = row._asdict()
        asset = str(row_map[universe_key])
        delisting_row = delisting_lookup.get(asset, {})
        rows.append(
            {
                "schema_version": "real_dataset_delisting_coverage.v1",
                "asset_id": asset,
                "ticker": row_map.get("ticker", ""),
                "membership_end": _date_str(row_map.get("membership_end")),
                "exit_date": _date_str(row_map.get("exit_date")),
                "delisting_record_available": bool(delisting_row),
                "delisting_date": _date_str(delisting_row.get("delisting_date")),
                "inactive_reason": delisting_row.get("inactive_reason", ""),
                "coverage_status": "delisting_record_present"
                if delisting_row
                else "index_exit_without_crsp_delisting_record",
                "readiness_check_only": True,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_signal_availability(prices: pd.DataFrame, price_key: str, dataset_frequency: str) -> pd.DataFrame:
    obs_by_asset = prices.groupby(price_key)["date"].nunique() if not prices.empty else pd.Series(dtype=int)
    max_obs = int(obs_by_asset.max()) if not obs_by_asset.empty else 0
    monthly_ready = dataset_frequency == "monthly" and max_obs >= 13
    rows = [
        {
            "schema_version": "real_dataset_signal_availability.v1",
            "factor_id": "momentum_12_1",
            "required_input": "monthly adjusted close history",
            "monthly_bundle_status": "available_for_monthly_dry_run" if monthly_ready else "insufficient_monthly_history",
            "daily_validation_required_for_factor": False,
            "factor_ranking_ran": False,
            "claim_status": "no_factor_claim",
        },
        {
            "schema_version": "real_dataset_signal_availability.v1",
            "factor_id": "reversal_5_1",
            "required_input": "daily adjusted close history",
            "monthly_bundle_status": "not_validated_from_monthly_bundle",
            "daily_validation_required_for_factor": True,
            "factor_ranking_ran": False,
            "claim_status": "no_factor_claim",
        },
        {
            "schema_version": "real_dataset_signal_availability.v1",
            "factor_id": "low_vol_60d",
            "required_input": "daily adjusted close history",
            "monthly_bundle_status": "not_validated_from_monthly_bundle",
            "daily_validation_required_for_factor": True,
            "factor_ranking_ran": False,
            "claim_status": "no_factor_claim",
        },
    ]
    return pd.DataFrame(rows)


def _build_summary(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    rows_checked: Mapping[str, int],
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    delistings: pd.DataFrame,
    dataset_frequency: str,
    median_gap_days: float | None,
    monthly_bundle: bool,
    output_paths: Mapping[str, str],
) -> dict[str, object]:
    price_dates = prices["date"].dropna()
    source_provenance = manifest.get("source_provenance", {})
    if not isinstance(source_provenance, Mapping):
        source_provenance = {}
    return {
        "schema_version": "real_dataset_dry_run.v1",
        "manifest_path": str(manifest_path),
        "preflight_ready": True,
        "source_provider": source_provenance.get("provider", ""),
        "source_license_mode": source_provenance.get("license_mode", ""),
        "rows_checked": dict(rows_checked),
        "row_counts": {
            "historical_universe": int(len(universe)),
            "adjusted_price_volume": int(len(prices)),
            "qqq_benchmark": int(len(benchmark)),
            "delistings": int(len(delistings)),
        },
        "unique_assets": int(prices[_asset_key(prices, "price panel")].nunique()) if not prices.empty else 0,
        "date_range": {
            "start": _date_str(price_dates.min()) if not price_dates.empty else "",
            "end": _date_str(price_dates.max()) if not price_dates.empty else "",
        },
        "dataset_frequency": dataset_frequency,
        "median_observation_gap_days": median_gap_days,
        "daily_price_volume_required_for_final_validation": monthly_bundle,
        "daily_price_volume_validation_status": "separate_long_task_not_started" if monthly_bundle else "not_required_by_frequency_check",
        "daily_price_volume_validation_started": False,
        "allocator_ran": False,
        "factor_ranking_ran": False,
        "optimizer_weights_ran": False,
        "strategy_return_claimed": False,
        "alpha_conclusion_claimed": False,
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "outputs": dict(output_paths),
    }


def _asset_sets_by_date(prices: pd.DataFrame, price_key: str) -> dict[pd.Timestamp, set[str]]:
    grouped: dict[pd.Timestamp, set[str]] = {}
    for date, group in prices.groupby("date"):
        grouped[date] = set(group[price_key].astype(str))
    return grouped


def _active_assets(universe: pd.DataFrame, universe_key: str, date: pd.Timestamp, max_date: pd.Timestamp) -> set[str]:
    start = universe["membership_start"].fillna(pd.Timestamp.min)
    end = universe["membership_end"].fillna(max_date)
    active = universe[(start <= date) & (end >= date)]
    return set(active[universe_key].astype(str))


def _first_value(frame: pd.DataFrame, column: str) -> object:
    if column not in frame.columns or frame.empty:
        return ""
    values = frame[column].dropna()
    return values.iloc[0] if not values.empty else ""


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _render_daily_long_task(summary: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Daily Price-Volume Validation Long Task",
            "",
            "Status: `not_started`",
            "",
            "This is a separate long task for daily CRSP price-volume validation. It was not started by the monthly PIT dry run.",
            "",
            "Scope:",
            "",
            "- Pull or load daily adjusted price-volume data under a resumable local cache.",
            "- Prove next-session tradability with daily trading dates.",
            "- Validate daily `reversal_5_1` and `low_vol_60d` availability before any rolling OOS evidence.",
            "- Keep outputs out of Q2 until Phase 64 import, Q1 evidence, Promotion Gate, and Alpha Registry decisions.",
            "",
            "Current monthly bundle:",
            "",
            "```json",
            json.dumps(
                {
                    "dataset_frequency": summary["dataset_frequency"],
                    "date_range": summary["date_range"],
                    "daily_price_volume_validation_status": summary["daily_price_volume_validation_status"],
                    "daily_price_volume_validation_started": summary["daily_price_volume_validation_started"],
                    "not_alpha_evidence": summary["not_alpha_evidence"],
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _render_report(summary: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Real Dataset Dry Run",
            "",
            "This MF-R7 dry run reads the real PIT dataset bundle and writes data-readiness checks only.",
            "",
            "It does not rank factors, run allocator weights, claim strategy returns, claim alpha success, approve production, or enter Q2.",
            "",
            "```json",
            json.dumps(summary, indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
