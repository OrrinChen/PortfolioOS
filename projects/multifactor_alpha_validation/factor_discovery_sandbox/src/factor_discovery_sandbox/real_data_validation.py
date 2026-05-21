"""Real-data admission, PIT universe, and return audit for FD validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import yaml


@dataclass(frozen=True)
class FDRealDataValidationResult:
    """Artifacts and summary for FD-R0 through FD-R2."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_data_validation_r0_r2(manifest_path: str | Path, output_dir: str | Path) -> FDRealDataValidationResult:
    """Run FD-R0/R1/R2 real-data checks without factor or alpha claims."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(manifest_file)
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    delistings = _normalize_delistings(_load_section_csv(manifest, manifest_file, "delisting"))

    quality = _build_data_quality_summary(manifest, universe, prices, benchmark, delistings)
    blockers = quality.loc[quality["status"] == "fail", "detail"].astype(str).tolist()
    if blockers:
        raise ValueError(f"FD real-data admission blocked: {blockers}")

    data_manifest = _build_data_manifest(manifest_file, manifest, universe, prices, benchmark, delistings, quality)
    pit_universe = _build_pit_universe_panel(universe, prices)
    universe_coverage = _build_universe_coverage_report(pit_universe)
    symbol_mapping = _build_symbol_mapping_audit(universe, prices)
    returns_panel = _build_returns_panel(prices)
    benchmark_returns = _build_benchmark_returns(benchmark, prices)
    corporate_action = _build_corporate_action_audit(prices, returns_panel)

    artifacts = {
        "data_admission_report": output_path / "data_admission_report.md",
        "data_manifest": output_path / "data_manifest.json",
        "data_quality_summary": output_path / "data_quality_summary.csv",
        "pit_universe_panel": output_path / "pit_universe_panel.csv",
        "universe_coverage_report": output_path / "universe_coverage_report.csv",
        "symbol_mapping_audit": output_path / "symbol_mapping_audit.csv",
        "survivorship_bias_audit": output_path / "survivorship_bias_audit.md",
        "returns_panel": output_path / "returns_panel.csv",
        "benchmark_returns": output_path / "benchmark_returns.csv",
        "corporate_action_audit": output_path / "corporate_action_audit.csv",
        "return_quality_report": output_path / "return_quality_report.md",
    }

    quality.to_csv(artifacts["data_quality_summary"], index=False)
    artifacts["data_manifest"].write_text(json.dumps(data_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pit_universe.to_csv(artifacts["pit_universe_panel"], index=False)
    universe_coverage.to_csv(artifacts["universe_coverage_report"], index=False)
    symbol_mapping.to_csv(artifacts["symbol_mapping_audit"], index=False)
    artifacts["survivorship_bias_audit"].write_text(_render_survivorship_audit(data_manifest), encoding="utf-8")
    returns_panel.to_csv(artifacts["returns_panel"], index=False)
    benchmark_returns.to_csv(artifacts["benchmark_returns"], index=False)
    corporate_action.to_csv(artifacts["corporate_action_audit"], index=False)
    artifacts["return_quality_report"].write_text(
        _render_return_quality_report(data_manifest, returns_panel, benchmark_returns, corporate_action),
        encoding="utf-8",
    )
    artifacts["data_admission_report"].write_text(
        _render_data_admission_report(data_manifest, quality, artifacts),
        encoding="utf-8",
    )

    summary = {
        "schema_version": "fd_real_data_validation_r0_r2.v1",
        "admission_status": data_manifest["admission_status"],
        "dataset_id": data_manifest["dataset_id"],
        "historical_constituents": data_manifest["has_historical_constituents"],
        "current_constituent_backfill_detected": data_manifest["current_constituent_backfill_detected"],
        "full_daily_price_volume_ready": data_manifest["full_daily_price_volume_ready"],
        "direct_q2_entry_allowed": False,
        "factor_ranking_ran": False,
        "allocator_ran": False,
        "alpha_success_claimed": False,
        "not_alpha_evidence": True,
        "completed_stages": ["FD-R0", "FD-R1", "FD-R2"],
    }
    return FDRealDataValidationResult(summary=summary, artifacts=artifacts)


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("FD real-data manifest must be a mapping")
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


def _normalize_universe(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    for column in ("date", "membership_start", "membership_end", "entry_date", "exit_date", "as_of_timestamp"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if "asset_id" not in normalized.columns and "permno" in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    if "source_is_pit" in normalized.columns:
        normalized["source_is_pit"] = normalized["source_is_pit"].astype(str).str.lower().isin({"true", "1", "yes"})
    return normalized


def _normalize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    if "asset_id" not in normalized.columns and "permno" in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("adjusted_open", "adjusted_close", "volume", "return"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def _normalize_benchmark(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    for column in ("adjusted_open", "adjusted_close", "volume", "return"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def _normalize_delistings(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column) for column in normalized.columns]
    if "asset_id" not in normalized.columns and "permno" in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    if "asset_id" in normalized.columns:
        normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("delisting_date", "last_trade_date"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if "delisting_return" in normalized.columns:
        normalized["delisting_return"] = pd.to_numeric(normalized["delisting_return"], errors="coerce")
    return normalized


def _build_data_quality_summary(
    manifest: Mapping[str, Any],
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    delistings: pd.DataFrame,
) -> pd.DataFrame:
    universe_section = _section(manifest, "universe")
    price_section = _section(manifest, "prices")
    benchmark_section = _section(manifest, "benchmark")
    timestamp_policy = _section(manifest, "timestamp_policy")
    source_provenance = _section(manifest, "source_provenance")
    price_frequency = _detect_frequency(prices)

    checks = [
        _quality_row(
            "manifest_schema",
            manifest.get("schema_version") == "research_mode_dataset_manifest.v1",
            "research_mode_dataset_manifest.v1 required",
        ),
        _quality_row(
            "snapshot_manifest",
            bool(str(manifest.get("content_hash", "")).strip())
            and bool(str(source_provenance.get("as_of_timestamp", "")).strip()),
            "content hash and source timestamp required",
        ),
        _quality_row(
            "historical_constituents",
            universe_section.get("constituent_mode") == "historical_membership"
            and bool(universe_section.get("source_is_pit"))
            and not universe.empty,
            "historical PIT membership required; current constituents are not admitted",
        ),
        _quality_row(
            "adjusted_prices",
            bool(price_section.get("adjusted")) and {"date", "asset_id", "adjusted_close"}.issubset(prices.columns),
            "adjusted close with asset/date required",
        ),
        _quality_row("volume", "volume" in prices.columns and prices["volume"].notna().any(), "volume required"),
        _quality_row(
            "benchmark",
            benchmark_section.get("benchmark_id") == "QQQ" and {"date", "adjusted_close"}.issubset(benchmark.columns),
            "QQQ benchmark with adjusted close required",
        ),
        _quality_row(
            "delisting_records",
            not delistings.empty and {"asset_id", "delisting_date", "delisting_return"}.issubset(delistings.columns),
            "explicit delisting records required",
        ),
        _quality_row(
            "timestamp_policy",
            timestamp_policy.get("allow_same_close_trading") is False
            and all(timestamp_policy.get(key) for key in ("signal", "visibility", "tradable")),
            "signal, visibility, tradability, and no-same-close policy required",
        ),
        _quality_row(
            "raw_prices",
            {"raw_open", "raw_close"}.issubset(prices.columns),
            "raw price fields are required for deeper corporate-action audit",
            warn_only=True,
        ),
        _quality_row(
            "sector_or_style_exposure",
            any(column in prices.columns for column in ("sector", "style"))
            or any(column in universe.columns for column in ("sector", "style")),
            "sector or style exposure is required for later attribution diagnostics",
            warn_only=True,
        ),
        _quality_row(
            "daily_price_volume",
            price_frequency == "daily",
            "daily price-volume is required before full 29-factor validation",
            warn_only=True,
        ),
    ]
    return pd.DataFrame(checks)


def _quality_row(check_name: str, passed: bool, detail: str, warn_only: bool = False) -> dict[str, object]:
    if passed:
        status = "pass"
    elif warn_only:
        status = "warning"
    else:
        status = "fail"
    return {
        "schema_version": "fd_real_data_quality_summary.v1",
        "check_name": check_name,
        "status": status,
        "detail": detail,
        "not_alpha_evidence": True,
    }


def _build_data_manifest(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    benchmark: pd.DataFrame,
    delistings: pd.DataFrame,
    quality: pd.DataFrame,
) -> dict[str, object]:
    source = _section(manifest, "source_provenance")
    universe_section = _section(manifest, "universe")
    price_dates = prices["date"].dropna()
    price_frequency = _detect_frequency(prices)
    current_backfill = universe_section.get("constituent_mode") == "current_constituents"
    warning_count = int((quality["status"] == "warning").sum())
    return {
        "schema_version": "fd_real_data_manifest.v1",
        "dataset_id": _dataset_id(source, price_frequency),
        "source_manifest_path": str(manifest_path),
        "vendor_source": source.get("provider", ""),
        "download_timestamp": source.get("as_of_timestamp", ""),
        "license_mode": source.get("license_mode", ""),
        "coverage_start": _date_str(price_dates.min()) if not price_dates.empty else "",
        "coverage_end": _date_str(price_dates.max()) if not price_dates.empty else "",
        "symbol_id_type": "permno_asset_id",
        "row_counts": {
            "historical_constituents": int(len(universe)),
            "price_volume": int(len(prices)),
            "benchmark": int(len(benchmark)),
            "delistings": int(len(delistings)),
        },
        "has_historical_constituents": universe_section.get("constituent_mode") == "historical_membership",
        "has_delisted_names": not delistings.empty,
        "has_adjusted_prices": {"adjusted_close", "adjusted_open"}.issubset(prices.columns),
        "has_raw_prices": {"raw_open", "raw_close"}.issubset(prices.columns),
        "has_volume": "volume" in prices.columns,
        "has_benchmark": _section(manifest, "benchmark").get("benchmark_id") == "QQQ",
        "has_sector_or_style_exposure": any(column in prices.columns for column in ("sector", "style"))
        or any(column in universe.columns for column in ("sector", "style")),
        "price_frequency": price_frequency,
        "full_daily_price_volume_ready": price_frequency == "daily",
        "is_pit_safe": bool(universe_section.get("source_is_pit"))
        and universe_section.get("constituent_mode") == "historical_membership",
        "survivorship_bias_risk": "low_historical_membership_used" if not current_backfill else "blocked_current_constituents",
        "current_constituent_backfill_detected": current_backfill,
        "admission_status": "admitted_for_daily_pit_r0_r2"
        if price_frequency == "daily"
        else "admitted_for_monthly_pit_r0_r2",
        "warning_count": warning_count,
        "blocked": False,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "factor_ranking_ran": False,
        "allocator_ran": False,
    }


def _build_pit_universe_panel(universe: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    price_dates = tuple(sorted(prices["date"].dropna().unique()))
    max_date = max(price_dates) if price_dates else pd.Timestamp.today().normalize()
    price_lookup = _priced_asset_lookup(prices)
    rows: list[dict[str, object]] = []
    for date in price_dates:
        start = universe["membership_start"].fillna(pd.Timestamp.min)
        end = universe["membership_end"].fillna(max_date)
        visible = universe[(start <= date) & (end >= date)].sort_values("asset_id")
        priced_assets = price_lookup.get(date, set())
        for row in visible.itertuples(index=False):
            asset_id = str(getattr(row, "asset_id"))
            is_tradable = asset_id in priced_assets
            rows.append(
                {
                    "schema_version": "fd_pit_universe_panel.v1",
                    "date": _date_str(date),
                    "asset_id": asset_id,
                    "ticker": getattr(row, "ticker", ""),
                    "membership_start": _date_str(getattr(row, "membership_start")),
                    "membership_end": _date_str(getattr(row, "membership_end")),
                    "is_member_asof_date": True,
                    "is_tradable_asof_date": is_tradable,
                    "reason_if_not_tradable": "" if is_tradable else "missing_price_volume_on_date",
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows)


def _build_universe_coverage_report(pit_universe: pd.DataFrame) -> pd.DataFrame:
    if pit_universe.empty:
        return pd.DataFrame(
            [
                {
                    "schema_version": "fd_universe_coverage_report.v1",
                    "date": "",
                    "active_members": 0,
                    "tradable_members": 0,
                    "coverage_ratio": 0.0,
                    "not_alpha_evidence": True,
                }
            ]
        )
    grouped = pit_universe.groupby("date", as_index=False).agg(
        active_members=("asset_id", "nunique"),
        tradable_members=("is_tradable_asof_date", "sum"),
    )
    grouped["coverage_ratio"] = (grouped["tradable_members"] / grouped["active_members"]).round(6)
    grouped.insert(0, "schema_version", "fd_universe_coverage_report.v1")
    grouped["not_alpha_evidence"] = True
    return grouped


def _build_symbol_mapping_audit(universe: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat(
        [
            universe[["asset_id", "ticker"]].assign(source_panel="universe") if "ticker" in universe.columns else universe[["asset_id"]].assign(ticker="", source_panel="universe"),
            prices[["asset_id", "ticker"]].assign(source_panel="prices") if "ticker" in prices.columns else prices[["asset_id"]].assign(ticker="", source_panel="prices"),
        ],
        ignore_index=True,
    )
    rows = []
    for asset_id, group in combined.groupby("asset_id"):
        tickers = sorted(set(str(value) for value in group["ticker"].dropna() if str(value)))
        rows.append(
            {
                "schema_version": "fd_symbol_mapping_audit.v1",
                "asset_id": str(asset_id),
                "ticker_count": len(tickers),
                "tickers": "|".join(tickers),
                "source_panels": "|".join(sorted(set(group["source_panel"].astype(str)))),
                "mapping_status": "stable" if len(tickers) <= 1 else "ticker_changed_or_share_class_changed",
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _build_returns_panel(prices: pd.DataFrame) -> pd.DataFrame:
    working = prices.sort_values(["asset_id", "date"], kind="mergesort").copy()
    working["adjusted_close_return"] = working.groupby("asset_id")["adjusted_close"].pct_change()
    missing_close = working["adjusted_close"].isna()
    zero_volume = working["volume"].fillna(0) <= 0 if "volume" in working.columns else pd.Series(False, index=working.index)
    extreme_return = working["adjusted_close_return"].abs() > 0.5
    working["return_quality_status"] = "ok"
    working.loc[working["adjusted_close_return"].isna(), "return_quality_status"] = "missing_initial_or_price_return"
    working.loc[missing_close, "return_quality_status"] = "missing_adjusted_close"
    working.loc[zero_volume, "return_quality_status"] = "zero_volume"
    working.loc[extreme_return.fillna(False), "return_quality_status"] = "extreme_return_review"
    columns = [
        "date",
        "asset_id",
        "ticker",
        "adjusted_open",
        "adjusted_close",
        "volume",
        "adjusted_close_return",
        "return_quality_status",
    ]
    available = [column for column in columns if column in working.columns]
    output = working[available].copy()
    output["date"] = output["date"].dt.date.astype(str)
    output["not_alpha_evidence"] = True
    return output


def _build_benchmark_returns(benchmark: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    working = benchmark.sort_values("date", kind="mergesort").copy()
    working["benchmark_return"] = working["adjusted_close"].pct_change()
    price_dates = set(prices["date"].dropna())
    working["benchmark_alignment_status"] = working["date"].map(lambda value: "aligned" if value in price_dates else "missing_price_panel_date")
    if "benchmark" not in working.columns:
        working["benchmark"] = "QQQ"
    output = working[["date", "benchmark", "adjusted_close", "benchmark_return", "benchmark_alignment_status"]].copy()
    output["date"] = output["date"].dt.date.astype(str)
    output["not_alpha_evidence"] = True
    return output


def _build_corporate_action_audit(prices: pd.DataFrame, returns_panel: pd.DataFrame) -> pd.DataFrame:
    convention_column = "adjusted_price_convention"
    if convention_column not in prices.columns:
        prices = prices.copy()
        prices[convention_column] = "unknown"
    rows = []
    for convention, group in prices.groupby(convention_column, dropna=False):
        group_returns = returns_panel[returns_panel["asset_id"].astype(str).isin(group["asset_id"].astype(str))]
        rows.append(
            {
                "schema_version": "fd_corporate_action_audit.v1",
                "adjusted_price_convention": str(convention),
                "row_count": int(len(group)),
                "asset_count": int(group["asset_id"].nunique()),
                "raw_price_available": {"raw_open", "raw_close"}.issubset(group.columns),
                "zero_volume_rows": int((group["volume"].fillna(0) <= 0).sum()) if "volume" in group.columns else 0,
                "missing_adjusted_close_rows": int(group["adjusted_close"].isna().sum()),
                "extreme_return_rows": int((group_returns["adjusted_close_return"].abs() > 0.5).sum()),
                "audit_status": "review_raw_prices_when_available",
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows)


def _section(manifest: Mapping[str, Any], name: str) -> dict[str, Any]:
    section = manifest.get(name)
    return dict(section) if isinstance(section, Mapping) else {}


def _detect_frequency(prices: pd.DataFrame) -> str:
    if prices.empty or "date" not in prices.columns:
        return "unknown"
    dates = pd.Series(sorted(prices["date"].dropna().unique()))
    if len(dates) < 2:
        return "unknown"
    median_gap = float(dates.diff().dropna().dt.days.median())
    convention = ""
    if "adjusted_price_convention" in prices.columns and prices["adjusted_price_convention"].notna().any():
        convention = str(prices["adjusted_price_convention"].dropna().iloc[0]).lower()
    if "monthly" in convention or "mth" in convention or median_gap >= 25:
        return "monthly"
    if median_gap <= 7:
        return "daily"
    return "unknown"


def _dataset_id(source: Mapping[str, Any], frequency: str) -> str:
    provider = str(source.get("provider", "")).lower()
    if provider == "wrds" and frequency == "monthly":
        return "wrds_nasdaq100_monthly_pit"
    if provider == "wrds" and frequency == "daily":
        return "wrds_nasdaq100_daily_pit"
    return f"{provider or 'unknown'}_factor_discovery_{frequency}_pit"


def _priced_asset_lookup(prices: pd.DataFrame) -> dict[pd.Timestamp, set[str]]:
    lookup: dict[pd.Timestamp, set[str]] = {}
    for date, group in prices.groupby("date"):
        tradable = group[group["adjusted_close"].notna()]
        if "volume" in tradable.columns:
            tradable = tradable[tradable["volume"].fillna(0) > 0]
        lookup[date] = set(tradable["asset_id"].astype(str))
    return lookup


def _date_str(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).date().isoformat()


def _render_survivorship_audit(data_manifest: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Survivorship Bias Audit",
            "",
            "not alpha evidence",
            "",
            f"- historical constituents: {str(data_manifest['has_historical_constituents']).lower()}",
            f"- current constituent backfill detected: {str(data_manifest['current_constituent_backfill_detected']).lower()}",
            f"- survivorship bias risk: {data_manifest['survivorship_bias_risk']}",
            "",
            "This FD real-data line is admitted only because the universe is historical and PIT-labeled.",
            "",
        ]
    )


def _render_data_admission_report(
    data_manifest: Mapping[str, object],
    quality: pd.DataFrame,
    artifacts: Mapping[str, Path],
) -> str:
    return "\n".join(
        [
            "# FD-R0 Real Data Admission Report",
            "",
            "not alpha evidence",
            "factor ranking: not run",
            "allocator: not run",
            "direct Q2 entry: not allowed",
            "",
            f"- dataset id: {data_manifest['dataset_id']}",
            f"- admission status: {data_manifest['admission_status']}",
            f"- coverage: {data_manifest['coverage_start']} to {data_manifest['coverage_end']}",
            f"- price frequency: {data_manifest['price_frequency']}",
            f"- full daily price-volume ready: {str(data_manifest['full_daily_price_volume_ready']).lower()}",
            "",
            "## Quality Checks",
            *[f"- {row.check_name}: {row.status} - {row.detail}" for row in quality.itertuples(index=False)],
            "",
            "## Artifacts",
            *[f"- {name}: {path}" for name, path in artifacts.items()],
            "",
        ]
    )


def _render_return_quality_report(
    data_manifest: Mapping[str, object],
    returns_panel: pd.DataFrame,
    benchmark_returns: pd.DataFrame,
    corporate_action: pd.DataFrame,
) -> str:
    status_counts = returns_panel["return_quality_status"].value_counts().to_dict()
    benchmark_missing = int((benchmark_returns["benchmark_alignment_status"] != "aligned").sum())
    zero_volume_rows = int(corporate_action["zero_volume_rows"].sum())
    extreme_rows = int(corporate_action["extreme_return_rows"].sum())
    return "\n".join(
        [
            "# FD-R2 Return Quality Report",
            "",
            "not alpha evidence",
            "strategy returns: not claimed",
            "",
            f"- dataset id: {data_manifest['dataset_id']}",
            f"- price frequency: {data_manifest['price_frequency']}",
            f"- benchmark missing aligned dates: {benchmark_missing}",
            f"- zero-volume rows: {zero_volume_rows}",
            f"- extreme return rows: {extreme_rows}",
            "",
            "## Return Quality Counts",
            *[f"- {status}: {count}" for status, count in sorted(status_counts.items())],
            "",
        ]
    )
