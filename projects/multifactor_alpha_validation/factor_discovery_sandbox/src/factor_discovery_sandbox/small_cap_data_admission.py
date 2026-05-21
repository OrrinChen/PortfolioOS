"""FD-S0 small-cap data admission gate."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from pandas.errors import EmptyDataError
import yaml


GUARDS = {
    "allocator_entry_allowed": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "production_approval_claimed": False,
    "direct_q2_entry_allowed": False,
    "not_alpha_evidence": True,
}


@dataclass(frozen=True)
class FDSmallCapAdmissionResult:
    """Artifacts and summary for FD-S0."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_cap_data_admission(
    manifest_path: str | Path,
    output_dir: str | Path,
    report_path: str | Path | None = None,
) -> FDSmallCapAdmissionResult:
    """Validate whether a local PIT bundle can support small-cap family research."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(manifest_file)
    universe = _load_section_csv_if_present(manifest, manifest_file, "universe")
    prices = _load_section_csv_if_present(manifest, manifest_file, "prices")
    benchmark = _load_section_csv_if_present(manifest, manifest_file, "benchmark")
    delistings = _load_section_csv_if_present(manifest, manifest_file, "delisting")

    checks = _admission_checks(manifest, universe, prices, benchmark, delistings)
    hard_failures = checks[(checks["required_for_family_run"]) & (checks["status"] == "fail")]
    delisting_status = _status_for(checks, "delisting_return_or_event_handling")
    spread_status = _status_for(checks, "spread_or_spread_proxy")
    liquidity_cost_status = "pass" if spread_status == "pass" else "degraded_without_spread_proxy"
    candidate_allowed = hard_failures.empty

    report = {
        "schema_version": "fd_small_cap_data_admission.v1",
        "stage": "FD-S0",
        "dataset_id": str(manifest.get("dataset_id") or manifest.get("content_hash") or manifest_file.stem),
        "source_manifest": str(manifest_file),
        "small_cap_research_admitted": bool(candidate_allowed),
        "microcap_quarantine_required": True,
        "delisting_handling_status": delisting_status,
        "liquidity_cost_data_status": liquidity_cost_status,
        "candidate_family_run_allowed": bool(candidate_allowed),
        "hard_failure_count": int(len(hard_failures)),
        "failed_required_checks": hard_failures["check_name"].astype(str).tolist(),
        **GUARDS,
    }

    artifacts = {
        "data_admission_report": output_path / "data_admission_report.json",
        "data_quality_summary": output_path / "data_quality_summary.csv",
        "data_admission_markdown": Path(report_path)
        if report_path is not None
        else output_path / "factor_discovery_small_cap_data_admission.md",
    }
    artifacts["data_admission_report"].write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checks.to_csv(artifacts["data_quality_summary"], index=False)
    artifacts["data_admission_markdown"].parent.mkdir(parents=True, exist_ok=True)
    artifacts["data_admission_markdown"].write_text(_render_report(report, checks), encoding="utf-8")

    summary = {
        "schema_version": "fd_small_cap_data_admission_summary.v1",
        "stage": "FD-S0",
        "small_cap_research_admitted": report["small_cap_research_admitted"],
        "candidate_family_run_allowed": report["candidate_family_run_allowed"],
        "delisting_handling_status": delisting_status,
        "liquidity_cost_data_status": liquidity_cost_status,
        **GUARDS,
    }
    return FDSmallCapAdmissionResult(summary=summary, artifacts=artifacts)


def _admission_checks(
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

    checks = [
        _check(
            "manifest_schema",
            manifest.get("schema_version") == "research_mode_dataset_manifest.v1",
            "research_mode_dataset_manifest.v1 is required",
        ),
        _check(
            "historical_pit_universe",
            universe_section.get("constituent_mode") == "historical_membership"
            and bool(universe_section.get("source_is_pit"))
            and not universe.empty,
            "historical PIT membership is required",
        ),
        _check(
            "pit_market_cap",
            _has_market_cap(prices),
            "PIT market cap or computable close * PIT shares is required",
        ),
        _check(
            "shares_outstanding_or_float",
            any(column in prices.columns for column in ("shares_outstanding", "shares_float"))
            or any(column in universe.columns for column in ("shares_outstanding", "shares_float")),
            "shares outstanding or float is required",
        ),
        _check(
            "adjusted_and_raw_prices",
            bool(price_section.get("adjusted"))
            and {"adjusted_close", "raw_close"}.issubset(prices.columns),
            "adjusted and raw prices are required",
        ),
        _check("volume", "volume" in prices.columns and prices["volume"].notna().any(), "volume is required"),
        _check(
            "corporate_action_handling",
            "adjusted_price_convention" in prices.columns and prices["adjusted_price_convention"].notna().any(),
            "adjustment convention or corporate action handling is required",
        ),
        _check(
            "delisting_return_or_event_handling",
            _section(manifest, "delisting").get("handling") == "explicit_file"
            and not delistings.empty
            and {"asset_id", "delisting_date", "delisting_return"}.issubset(delistings.columns),
            "explicit delisting returns or event handling is required",
        ),
        _check(
            "exchange_share_class_filters",
            any(column in prices.columns for column in ("exchange_code", "exchange", "share_code", "common_share"))
            or any(column in universe.columns for column in ("exchange_code", "exchange", "share_code", "common_share")),
            "exchange and share-class filters are required",
        ),
        _check(
            "sector_or_industry",
            any(column in prices.columns for column in ("sector", "industry"))
            or any(column in universe.columns for column in ("sector", "industry")),
            "sector or industry classification is required",
        ),
        _check(
            "benchmark_returns",
            not benchmark.empty
            and {"date", "adjusted_close"}.issubset(benchmark.columns)
            and bool(benchmark_section.get("benchmark_id")),
            "small-cap or market benchmark returns are required",
        ),
        _check(
            "adv",
            {"adjusted_close", "volume"}.issubset(prices.columns)
            and prices[["adjusted_close", "volume"]].notna().all(axis=1).any(),
            "ADV must be computable from price and volume",
        ),
        _check(
            "spread_or_spread_proxy",
            any(
                column in prices.columns
                for column in ("bid_ask_spread", "spread", "quoted_spread", "effective_spread", "high", "low")
            ),
            "spread or spread proxy is preferred for cost diagnostics",
            required=False,
        ),
        _check(
            "timestamp_policy",
            timestamp_policy.get("allow_same_close_trading") is False,
            "same-close trading must be disabled",
        ),
    ]
    return pd.DataFrame(checks)


def _check(check_name: str, passed: bool, detail: str, required: bool = True) -> dict[str, object]:
    return {
        "schema_version": "fd_small_cap_data_quality_summary.v1",
        "check_name": check_name,
        "status": "pass" if passed else "fail",
        "detail": detail,
        "required_for_family_run": bool(required),
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def _has_market_cap(prices: pd.DataFrame) -> bool:
    if "market_cap" in prices.columns and pd.to_numeric(prices["market_cap"], errors="coerce").notna().any():
        return True
    share_column = "shares_float" if "shares_float" in prices.columns else "shares_outstanding"
    return (
        share_column in prices.columns
        and "adjusted_close" in prices.columns
        and prices[[share_column, "adjusted_close"]].notna().all(axis=1).any()
    )


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("small-cap data admission manifest must be a mapping")
    return payload


def _load_section_csv_if_present(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    section_payload = manifest.get(section)
    if not isinstance(section_payload, Mapping) or not section_payload.get("path"):
        return pd.DataFrame()
    path = Path(str(section_payload["path"]))
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _section(manifest: Mapping[str, Any], section: str) -> Mapping[str, Any]:
    payload = manifest.get(section)
    return payload if isinstance(payload, Mapping) else {}


def _status_for(checks: pd.DataFrame, check_name: str) -> str:
    rows = checks[checks["check_name"] == check_name]
    if rows.empty:
        return "fail"
    return str(rows["status"].iloc[0])


def _render_report(report: Mapping[str, object], checks: pd.DataFrame) -> str:
    lines = [
        "# FD-S0 Small-Cap Data Admission",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- small-cap research admitted: {str(report['small_cap_research_admitted']).lower()}",
        f"- candidate family run allowed: {str(report['candidate_family_run_allowed']).lower()}",
        f"- delisting handling status: {report['delisting_handling_status']}",
        f"- liquidity cost data status: {report['liquidity_cost_data_status']}",
        "",
        "## Checks",
    ]
    for row in checks.itertuples(index=False):
        lines.append(f"- {row.check_name}: {row.status} - {row.detail}")
    lines.append("")
    return "\n".join(lines)
