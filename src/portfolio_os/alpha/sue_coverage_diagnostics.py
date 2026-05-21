"""Coverage, linkage, and price diagnostics for WRDS/PIT SUE panels.

This module diagnoses why an expanded historical SUE panel loses coverage. It
does not run Q2, optimizers, brokers, orders, paper workflows, or production
approval paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict
import yaml

from portfolio_os.alpha.sue_historical_schema import (
    MISLEADING_REPORT_CLAIMS,
    SueHistoricalEventRow,
    validate_no_forward_return_feature_columns,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_COVERAGE_DIAGNOSTICS_SCHEMA_VERSION = "sue_coverage_linkage_price_diagnostics.v1"
LOCAL_MISLEADING_CLAIMS = (
    *MISLEADING_REPORT_CLAIMS,
    "live trading",
    "broker",
    "order",
)

DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_COVERAGE_RESCUE_REPORT_PATH = "outputs/sue_historical_event_panel_expanded/coverage_rescue_report.json"
DEFAULT_LINKAGE_FAILURE_REPORT_PATH = "outputs/sue_historical_event_panel_expanded/linkage_failure_report.csv"
DEFAULT_MISSING_PRICE_REPORT_PATH = "outputs/sue_historical_event_panel_expanded/missing_price_report.csv"
DEFAULT_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_coverage_linkage_price_diagnostics"
DEFAULT_REPORT_PATH = "reports/sue_coverage_linkage_price_diagnostics_report.md"


class SueCoverageDiagnosticsConfig(BaseModel):
    """Config for SUE coverage/linkage/price diagnostics."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    coverage_rescue_report_path: str = DEFAULT_COVERAGE_RESCUE_REPORT_PATH
    linkage_failure_report_path: str = DEFAULT_LINKAGE_FAILURE_REPORT_PATH
    missing_price_report_path: str = DEFAULT_MISSING_PRICE_REPORT_PATH
    crsp_daily_path: str = DEFAULT_CRSP_DAILY_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH


@dataclass(frozen=True)
class SueCoverageDiagnosticsResult:
    """In-memory coverage/linkage/price diagnostic result."""

    config: SueCoverageDiagnosticsConfig
    coverage_loss_waterfall: pd.DataFrame
    linkage_loss_by_symbol: pd.DataFrame
    linkage_loss_by_month: pd.DataFrame
    price_loss_by_permno: pd.DataFrame
    price_loss_by_month: pd.DataFrame
    price_cache_gap_report: pd.DataFrame
    diagnostic_summary: dict[str, Any]
    report_text: str

    @property
    def summary(self) -> dict[str, Any]:
        """Backward-compatible short alias for the diagnostic summary."""

        return self.diagnostic_summary


def load_sue_coverage_diagnostics_config(path: str | Path) -> SueCoverageDiagnosticsConfig:
    """Load diagnostics config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    return SueCoverageDiagnosticsConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        coverage_rescue_report_path=str(
            inputs.get("coverage_rescue_report_path")
            or payload.get("coverage_rescue_report_path")
            or DEFAULT_COVERAGE_RESCUE_REPORT_PATH
        ),
        linkage_failure_report_path=str(
            inputs.get("linkage_failure_report_path")
            or payload.get("linkage_failure_report_path")
            or DEFAULT_LINKAGE_FAILURE_REPORT_PATH
        ),
        missing_price_report_path=str(
            inputs.get("missing_price_report_path")
            or payload.get("missing_price_report_path")
            or DEFAULT_MISSING_PRICE_REPORT_PATH
        ),
        crsp_daily_path=str(inputs.get("crsp_daily_path") or payload.get("crsp_daily_path") or DEFAULT_CRSP_DAILY_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
    )


def build_sue_coverage_linkage_price_diagnostics(
    config: SueCoverageDiagnosticsConfig | None = None,
) -> SueCoverageDiagnosticsResult:
    """Build coverage, linkage, and price diagnostics from H1C artifacts."""

    resolved = config or SueCoverageDiagnosticsConfig()
    coverage = _read_json(Path(resolved.coverage_rescue_report_path))
    events = _load_events(Path(resolved.events_path))
    linkage = _read_optional_csv(Path(resolved.linkage_failure_report_path))
    missing_prices = _read_optional_csv(Path(resolved.missing_price_report_path))
    crsp_daily = _load_crsp_daily(Path(resolved.crsp_daily_path))

    coverage_loss_waterfall = _coverage_loss_waterfall(coverage)
    linkage_loss_by_symbol, linkage_loss_by_month = _linkage_diagnostics(linkage)
    price_cache_gap_report = _price_cache_gap_report(missing_prices, crsp_daily)
    price_loss_by_permno, price_loss_by_month = _price_diagnostics(price_cache_gap_report)
    summary = _diagnostic_summary(
        coverage=coverage,
        events=events,
        linkage_loss_by_symbol=linkage_loss_by_symbol,
        price_cache_gap_report=price_cache_gap_report,
        crsp_daily=crsp_daily,
    )
    report_text = render_sue_coverage_diagnostics_report(summary)
    validate_sue_coverage_diagnostics_report_language(report_text)
    return SueCoverageDiagnosticsResult(
        config=resolved,
        coverage_loss_waterfall=coverage_loss_waterfall,
        linkage_loss_by_symbol=linkage_loss_by_symbol,
        linkage_loss_by_month=linkage_loss_by_month,
        price_loss_by_permno=price_loss_by_permno,
        price_loss_by_month=price_loss_by_month,
        price_cache_gap_report=price_cache_gap_report,
        diagnostic_summary=summary,
        report_text=report_text,
    )


def write_sue_coverage_linkage_price_diagnostics_artifacts(
    result: SueCoverageDiagnosticsResult,
) -> dict[str, Path]:
    """Write coverage/linkage/price diagnostic artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    paths = {
        "coverage_loss_waterfall": output_dir / "coverage_loss_waterfall.csv",
        "linkage_loss_by_symbol": output_dir / "linkage_loss_by_symbol.csv",
        "linkage_loss_by_month": output_dir / "linkage_loss_by_month.csv",
        "price_loss_by_permno": output_dir / "price_loss_by_permno.csv",
        "price_loss_by_month": output_dir / "price_loss_by_month.csv",
        "price_cache_gap_report": output_dir / "price_cache_gap_report.csv",
        "diagnostic_summary": output_dir / "diagnostic_summary.json",
        "report": report_path,
    }
    result.coverage_loss_waterfall.to_csv(paths["coverage_loss_waterfall"], index=False)
    result.linkage_loss_by_symbol.to_csv(paths["linkage_loss_by_symbol"], index=False)
    result.linkage_loss_by_month.to_csv(paths["linkage_loss_by_month"], index=False)
    result.price_loss_by_permno.to_csv(paths["price_loss_by_permno"], index=False)
    result.price_loss_by_month.to_csv(paths["price_loss_by_month"], index=False)
    result.price_cache_gap_report.to_csv(paths["price_cache_gap_report"], index=False)
    _write_json(paths["diagnostic_summary"], result.diagnostic_summary)
    validate_sue_coverage_diagnostics_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_coverage_diagnostics_report(summary: dict[str, Any]) -> str:
    """Render a short coverage/linkage/price diagnostic report."""

    lines = [
        "# SUE Coverage / Linkage / Price Diagnostics",
        "",
        "This report diagnoses coverage, linkage, and price-window loss in the expanded WRDS/PIT SUE panel.",
        "This does not run Q2 or optimizer-path evaluation.",
        "This does not prove SUE alpha success by itself.",
        "This does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "Missing SUE or price coverage remains explicit diagnostic/no_view and is not encoded as zero alpha.",
        "",
        "## Summary",
        "",
        f"- event_count: `{summary['event_count']}`",
        f"- final_pit_safe_rows: `{summary['final_pit_safe_rows']}`",
        f"- unlinked_ibes_crsp_rows: `{summary['unlinked_ibes_crsp_rows']}`",
        f"- missing_price_rows: `{summary['missing_price_rows']}`",
        f"- missing_return_windows: `{summary['missing_return_windows']}`",
        f"- diagnostic_only_rows: `{summary['diagnostic_only_rows']}`",
        f"- crsp_cache_rows: `{summary['crsp_cache_rows']}`",
        f"- crsp_cache_start_date: `{summary['crsp_cache_start_date']}`",
        f"- crsp_cache_end_date: `{summary['crsp_cache_end_date']}`",
        f"- recommended_next_action: `{summary['recommended_next_action']}`",
        "",
        "## Price Gap Classification",
        "",
    ]
    for name, count in sorted(summary["price_gap_classifications"].items()):
        lines.append(f"- {name}: `{count}`")
    lines.extend(
        [
            "",
            "## Boundaries",
            "",
            f"- q2_evaluation_ran: `{summary['q2_evaluation_ran']}`",
            f"- optimizer_path_evaluation_ran: `{summary['optimizer_path_evaluation_ran']}`",
            f"- alpha_registry_promoted: `{summary['alpha_registry_promoted']}`",
            f"- production_approval_claimed: `{summary['production_approval_claimed']}`",
            f"- missing_coverage_encoded_as_zero_alpha: `{summary['missing_coverage_encoded_as_zero_alpha']}`",
            "",
        ]
    )
    return "\n".join(lines)


def validate_sue_coverage_diagnostics_report_language(text: str) -> None:
    """Reject misleading diagnostics report claims while allowing explicit non-claims."""

    lowered = str(text).lower()
    scrubbed = lowered
    allowed_phrases = [
        "it does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "this does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "it does not run q2 or optimizer-path evaluation.",
        "this does not run q2 or optimizer-path evaluation.",
        "it does not prove sue alpha success by itself.",
        "this does not prove sue alpha success by itself.",
        "production_approval_claimed",
        "q2_evaluation_ran",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in LOCAL_MISLEADING_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE coverage diagnostic claim detected: {claim}")


def _load_events(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(events.columns))
    records = _records_with_none(events)
    for record in records:
        if (
            str(record.get("pit_safety_status", "")).startswith("diagnostic_missing_estimate")
            and (record.get("expected_eps") == 0.0 or record.get("sue_value") == 0.0)
        ):
            raise ValueError("missing expected EPS cannot be encoded as zero SUE")
        SueHistoricalEventRow.model_validate(record)
    return events


def _load_crsp_daily(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(frame.columns))
    if frame.empty:
        return pd.DataFrame(columns=["permno", "date"])
    required = {"permno", "date"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("CRSP daily input missing columns: " + ", ".join(sorted(missing)))
    frame = frame.copy()
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce").astype("Int64")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.date
    return frame


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    frame = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(frame.columns))
    return frame


def _coverage_loss_waterfall(coverage: dict[str, Any]) -> pd.DataFrame:
    event_count = _int(coverage, "event_count", "total_raw_events", "row_count")
    linked_rows = event_count - _int(coverage, "unlinked_ibes_crsp_rows", "unlinked_rows")
    rows = [
        ("source_events", event_count, 0, event_count),
        ("unlinked_ibes_crsp_rows", _int(coverage, "unlinked_ibes_crsp_rows", "unlinked_rows"), _int(coverage, "unlinked_ibes_crsp_rows", "unlinked_rows"), linked_rows),
        ("linked_rows", linked_rows, 0, linked_rows),
        ("missing_expected_eps", _int(coverage, "missing_expected_eps", "missing_estimates"), _int(coverage, "missing_expected_eps", "missing_estimates"), None),
        ("missing_actual_eps", _int(coverage, "missing_actual_eps", "missing_actuals"), _int(coverage, "missing_actual_eps", "missing_actuals"), None),
        ("missing_estimate_snapshot_date", _int(coverage, "missing_estimate_snapshot_date", "missing_estimate_snapshot_dates"), _int(coverage, "missing_estimate_snapshot_date", "missing_estimate_snapshot_dates"), None),
        ("missing_price_rows", _int(coverage, "missing_price_rows", "missing_prices"), _int(coverage, "missing_price_rows", "missing_prices"), None),
        ("missing_return_windows", _int(coverage, "missing_return_windows"), _int(coverage, "missing_return_windows"), None),
        ("diagnostic_only_rows", _int(coverage, "diagnostic_only_rows"), _int(coverage, "diagnostic_only_rows"), None),
        ("final_pit_safe_rows", _int(coverage, "pit_safe_rows", "final_pit_safe_rows"), 0, _int(coverage, "pit_safe_rows", "final_pit_safe_rows")),
    ]
    return pd.DataFrame(rows, columns=["stage", "row_count", "loss_count", "retained_count"])


def _linkage_diagnostics(linkage: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if linkage.empty:
        columns = ["symbol", "ibes_ticker", "cusip", "failure_reason", "failed_event_count"]
        month_columns = ["month", "failure_reason", "failed_event_count"]
        return pd.DataFrame(columns=columns), pd.DataFrame(columns=month_columns)
    frame = linkage.copy()
    frame["announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="coerce")
    frame["month"] = frame["announcement_date"].dt.to_period("M").astype(str)
    by_symbol = (
        frame.groupby(["symbol", "ibes_ticker", "cusip", "failure_reason"], dropna=False)
        .size()
        .reset_index(name="failed_event_count")
        .sort_values(["failed_event_count", "symbol"], ascending=[False, True])
    )
    by_month = (
        frame.groupby(["month", "failure_reason"], dropna=False)
        .size()
        .reset_index(name="failed_event_count")
        .sort_values(["month", "failure_reason"])
    )
    return by_symbol, by_month


def _price_cache_gap_report(missing_prices: pd.DataFrame, crsp_daily: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "event_id",
        "symbol",
        "permno",
        "announcement_date",
        "return_window_start",
        "return_window_end",
        "permno_in_crsp_cache",
        "crsp_first_date",
        "crsp_last_date",
        "price_gap_classification",
    ]
    if missing_prices.empty:
        return pd.DataFrame(columns=columns)
    cache_ranges = _crsp_cache_ranges(crsp_daily)
    rows: list[dict[str, Any]] = []
    for record in missing_prices.to_dict(orient="records"):
        permno = _optional_int(record.get("permno"))
        return_start = pd.Timestamp(record.get("return_window_start")).date()
        return_end = pd.Timestamp(record.get("return_window_end")).date()
        cache_range = cache_ranges.get(permno) if permno is not None else None
        if cache_range is None:
            in_cache = False
            first = None
            last = None
            classification = "permno_absent_from_crsp_cache"
        else:
            in_cache = True
            first, last = cache_range
            if return_start < first:
                classification = "return_window_before_crsp_cache_start"
            elif return_end > last:
                classification = "return_window_after_crsp_cache_end"
            else:
                classification = "price_window_missing_despite_cache_range"
        rows.append(
            {
                "event_id": record.get("event_id"),
                "symbol": record.get("symbol"),
                "permno": permno,
                "announcement_date": record.get("announcement_date"),
                "return_window_start": return_start.isoformat(),
                "return_window_end": return_end.isoformat(),
                "permno_in_crsp_cache": in_cache,
                "crsp_first_date": first.isoformat() if first else None,
                "crsp_last_date": last.isoformat() if last else None,
                "price_gap_classification": classification,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _price_diagnostics(price_gaps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if price_gaps.empty:
        return (
            pd.DataFrame(columns=["permno", "symbol", "price_gap_classification", "missing_price_event_count"]),
            pd.DataFrame(columns=["month", "price_gap_classification", "missing_price_event_count"]),
        )
    frame = price_gaps.copy()
    frame["announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="coerce")
    frame["month"] = frame["announcement_date"].dt.to_period("M").astype(str)
    by_permno = (
        frame.groupby(["permno", "symbol", "price_gap_classification"], dropna=False)
        .size()
        .reset_index(name="missing_price_event_count")
        .sort_values(["missing_price_event_count", "symbol"], ascending=[False, True])
    )
    by_month = (
        frame.groupby(["month", "price_gap_classification"], dropna=False)
        .size()
        .reset_index(name="missing_price_event_count")
        .sort_values(["month", "price_gap_classification"])
    )
    return by_permno, by_month


def _diagnostic_summary(
    *,
    coverage: dict[str, Any],
    events: pd.DataFrame,
    linkage_loss_by_symbol: pd.DataFrame,
    price_cache_gap_report: pd.DataFrame,
    crsp_daily: pd.DataFrame,
) -> dict[str, Any]:
    price_gap_classifications = (
        price_cache_gap_report["price_gap_classification"].value_counts().sort_index().to_dict()
        if not price_cache_gap_report.empty
        else {}
    )
    event_count = _int(coverage, "event_count", "total_raw_events", "row_count")
    pit_safe_rows = _int(coverage, "pit_safe_rows", "final_pit_safe_rows")
    crsp_dates = crsp_daily["date"].dropna() if "date" in crsp_daily.columns else pd.Series(dtype=object)
    payload = {
        "schema_version": SUE_COVERAGE_DIAGNOSTICS_SCHEMA_VERSION,
        "run_id": "sue_coverage_linkage_price_diagnostics",
        "status": "completed",
        "event_count": event_count,
        "events_csv_rows": int(len(events)),
        "rebalance_date_count": _int(coverage, "rebalance_date_count"),
        "final_pit_safe_rows": pit_safe_rows,
        "pit_safe_share": round(pit_safe_rows / event_count, 8) if event_count else 0.0,
        "unlinked_ibes_crsp_rows": _int(coverage, "unlinked_ibes_crsp_rows", "unlinked_rows"),
        "missing_expected_eps": _int(coverage, "missing_expected_eps", "missing_estimates"),
        "missing_actual_eps": _int(coverage, "missing_actual_eps", "missing_actuals"),
        "missing_estimate_snapshot_date": _int(coverage, "missing_estimate_snapshot_date", "missing_estimate_snapshot_dates"),
        "missing_price_rows": _int(coverage, "missing_price_rows", "missing_prices"),
        "missing_return_windows": _int(coverage, "missing_return_windows"),
        "diagnostic_only_rows": _int(coverage, "diagnostic_only_rows"),
        "linkage_failed_symbol_count": int(linkage_loss_by_symbol["symbol"].nunique()) if not linkage_loss_by_symbol.empty else 0,
        "price_gap_classifications": {str(key): int(value) for key, value in price_gap_classifications.items()},
        "crsp_cache_rows": int(len(crsp_daily)),
        "crsp_cache_permnos": int(crsp_daily["permno"].nunique()) if "permno" in crsp_daily.columns else 0,
        "crsp_cache_start_date": min(crsp_dates).isoformat() if len(crsp_dates) else None,
        "crsp_cache_end_date": max(crsp_dates).isoformat() if len(crsp_dates) else None,
        "recommended_next_action": _recommended_next_action(coverage, price_gap_classifications),
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "missing_coverage_encoded_as_zero_alpha": bool(coverage.get("missing_coverage_encoded_as_zero_alpha", False)),
        "no_view_not_zero_alpha": True,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _recommended_next_action(coverage: dict[str, Any], classifications: dict[str, int]) -> str:
    if _int(coverage, "unlinked_ibes_crsp_rows", "unlinked_rows") > 0 or classifications:
        return "rescue_linkage_and_price_coverage_before_q2"
    if _int(coverage, "pit_safe_rows", "final_pit_safe_rows") == 0:
        return "inspect_pit_filters_before_evidence"
    return "rerun_historical_evidence_after_diagnostics"


def _crsp_cache_ranges(crsp_daily: pd.DataFrame) -> dict[int, tuple[Any, Any]]:
    if crsp_daily.empty:
        return {}
    ranges: dict[int, tuple[Any, Any]] = {}
    for permno, group in crsp_daily.dropna(subset=["permno", "date"]).groupby("permno"):
        dates = sorted(group["date"].dropna())
        if dates:
            ranges[int(permno)] = (dates[0], dates[-1])
    return ranges


def _records_with_none(frame: pd.DataFrame) -> list[dict[str, Any]]:
    cleaned = frame.astype(object).where(pd.notna(frame), None)
    records = cleaned.to_dict(orient="records")
    text_columns = {
        "event_id",
        "symbol",
        "ibes_ticker",
        "cusip",
        "fiscal_period",
        "announcement_date",
        "event_available_timestamp",
        "tradable_timestamp",
        "rebalance_date",
        "sue_definition",
        "estimate_snapshot_date",
        "price_anchor_date",
        "return_window_start",
        "return_window_end",
        "data_source",
        "link_method",
        "pit_safety_status",
        "fetched_at",
    }
    for record in records:
        for column in text_columns:
            if record.get(column) is not None:
                record[column] = str(record[column])
    return records


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _int(payload: dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key in payload and payload[key] is not None:
            return int(payload[key])
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
