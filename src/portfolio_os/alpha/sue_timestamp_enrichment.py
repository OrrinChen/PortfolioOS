"""SUE timestamp-source enrichment.

Reopen-H1E.5 enriches SUE events with optional local timestamp sources so we
can decide whether an auditable earlier actual-EPS public availability
timestamp exists. It does not select scores, run Q2, run optimizer-path
evaluation, promote Alpha Registry state, create broker/order/live workflows, or
approve production use.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_schema import (
    SUE_HISTORICAL_EVENT_COLUMNS,
    SueHistoricalEventRow,
    validate_no_forward_return_feature_columns,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_TIMESTAMP_ENRICHMENT_SCHEMA_VERSION = "sue_timestamp_enrichment.v1"
DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_OUTPUT_DIR = "outputs/sue_timestamp_enrichment"
DEFAULT_REPORT_PATH = "reports/sue_timestamp_enrichment_report.md"

MISLEADING_TIMESTAMP_ENRICHMENT_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live trading",
    "broker execution",
    "order generation",
    "selected production score",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "sue alpha is proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueTimestampEnrichmentConfig(BaseModel):
    """Config for H1E.5 timestamp source enrichment."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    report_path: str = DEFAULT_REPORT_PATH
    ibes_actuals_path: str | None = None
    compustat_quarterly_path: str | None = None
    release_timestamps_path: str | None = None
    sec_filing_timestamps_path: str | None = None
    allow_sec_as_first_public_release: bool = False
    before_open_utc_hour: int = Field(default=14, ge=0, le=23)
    after_close_utc_hour: int = Field(default=21, ge=0, le=23)
    market_open_utc_hour: int = Field(default=14, ge=0, le=23)
    market_open_utc_minute: int = Field(default=30, ge=0, le=59)


@dataclass(frozen=True)
class SueTimestampEnrichmentResult:
    """In-memory H1E.5 enrichment result."""

    config: SueTimestampEnrichmentConfig
    timestamp_source_comparison: pd.DataFrame
    source_coverage_report: dict[str, Any]
    date_disagreement_report: pd.DataFrame
    repairable_event_candidates: pd.DataFrame
    nonrepairable_event_report: pd.DataFrame
    timestamp_enrichment_decision: dict[str, Any]
    report_text: str


def load_sue_timestamp_enrichment_config(path: str | Path) -> SueTimestampEnrichmentConfig:
    """Load H1E.5 timestamp enrichment config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    policy = payload.get("policy") or {}
    return SueTimestampEnrichmentConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        ibes_actuals_path=_optional_path(inputs.get("ibes_actuals_path") or payload.get("ibes_actuals_path")),
        compustat_quarterly_path=_optional_path(
            inputs.get("compustat_quarterly_path") or payload.get("compustat_quarterly_path")
        ),
        release_timestamps_path=_optional_path(
            inputs.get("release_timestamps_path") or payload.get("release_timestamps_path")
        ),
        sec_filing_timestamps_path=_optional_path(
            inputs.get("sec_filing_timestamps_path") or payload.get("sec_filing_timestamps_path")
        ),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        report_path=str(outputs.get("report_path") or payload.get("report_path") or DEFAULT_REPORT_PATH),
        allow_sec_as_first_public_release=bool(
            policy.get("allow_sec_as_first_public_release", payload.get("allow_sec_as_first_public_release", False))
        ),
        before_open_utc_hour=int(policy.get("before_open_utc_hour", payload.get("before_open_utc_hour", 14))),
        after_close_utc_hour=int(policy.get("after_close_utc_hour", payload.get("after_close_utc_hour", 21))),
        market_open_utc_hour=int(policy.get("market_open_utc_hour", payload.get("market_open_utc_hour", 14))),
        market_open_utc_minute=int(policy.get("market_open_utc_minute", payload.get("market_open_utc_minute", 30))),
    )


def build_sue_timestamp_enrichment(
    config: SueTimestampEnrichmentConfig | None = None,
) -> SueTimestampEnrichmentResult:
    """Build H1E.5 timestamp-source enrichment artifacts."""

    resolved = config or SueTimestampEnrichmentConfig()
    events = _load_events(resolved.events_path)
    sources = _load_sources(resolved)
    comparison = _timestamp_source_comparison(events=events, sources=sources, config=resolved)
    repairable = _repairable_event_candidates(comparison)
    nonrepairable = _nonrepairable_event_report(comparison)
    coverage = _source_coverage_report(
        comparison=comparison,
        repairable_event_candidates=repairable,
        nonrepairable_event_report=nonrepairable,
        sources=sources,
    )
    disagreement = _date_disagreement_report(comparison)
    decision = _timestamp_enrichment_decision(
        comparison=comparison,
        repairable_event_candidates=repairable,
        coverage=coverage,
    )
    report_text = render_sue_timestamp_enrichment_report(
        source_coverage_report=coverage,
        timestamp_enrichment_decision=decision,
    )
    validate_sue_timestamp_enrichment_report_language(report_text)
    return SueTimestampEnrichmentResult(
        config=resolved,
        timestamp_source_comparison=comparison,
        source_coverage_report=coverage,
        date_disagreement_report=disagreement,
        repairable_event_candidates=repairable,
        nonrepairable_event_report=nonrepairable,
        timestamp_enrichment_decision=decision,
        report_text=report_text,
    )


def write_sue_timestamp_enrichment_artifacts(result: SueTimestampEnrichmentResult) -> dict[str, Path]:
    """Write H1E.5 timestamp enrichment artifacts."""

    output_dir = Path(result.config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(result.config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    paths = {
        "timestamp_source_comparison": output_dir / "timestamp_source_comparison.csv",
        "source_coverage_report": output_dir / "source_coverage_report.json",
        "date_disagreement_report": output_dir / "date_disagreement_report.csv",
        "repairable_event_candidates": output_dir / "repairable_event_candidates.csv",
        "nonrepairable_event_report": output_dir / "nonrepairable_event_report.csv",
        "timestamp_enrichment_decision": output_dir / "timestamp_enrichment_decision.json",
        "report": report_path,
    }
    result.timestamp_source_comparison.to_csv(paths["timestamp_source_comparison"], index=False)
    _write_json(paths["source_coverage_report"], result.source_coverage_report)
    result.date_disagreement_report.to_csv(paths["date_disagreement_report"], index=False)
    result.repairable_event_candidates.to_csv(paths["repairable_event_candidates"], index=False)
    result.nonrepairable_event_report.to_csv(paths["nonrepairable_event_report"], index=False)
    _write_json(paths["timestamp_enrichment_decision"], result.timestamp_enrichment_decision)
    validate_sue_timestamp_enrichment_report_language(result.report_text)
    paths["report"].write_text(result.report_text, encoding="utf-8")
    return paths


def render_sue_timestamp_enrichment_report(
    *,
    source_coverage_report: dict[str, Any],
    timestamp_enrichment_decision: dict[str, Any],
) -> str:
    """Render H1E.5 timestamp enrichment report."""

    lines = [
        "# SUE Timestamp Source Enrichment Report",
        "",
        "This is timestamp-source enrichment only.",
        "It does not prove SUE alpha.",
        "It does not select a SUE score.",
        "It does not run Q2 or optimizer-path evaluation.",
        "-5/-10 shifted windows are not tradable unless public availability is proven.",
        "No paper/live/broker/order/production workflow is approved.",
        "",
        "## Decision",
        "",
        f"- schema_version: `{timestamp_enrichment_decision['schema_version']}`",
        f"- decision_label: `{timestamp_enrichment_decision['decision_label']}`",
        f"- event_count: `{timestamp_enrichment_decision['event_count']}`",
        f"- repairable_event_count: `{timestamp_enrichment_decision['repairable_event_count']}`",
        f"- selected_score: `{timestamp_enrichment_decision['selected_score']}`",
        f"- q2_evaluation_ran: `{timestamp_enrichment_decision['q2_evaluation_ran']}`",
        f"- optimizer_path_evaluation_ran: `{timestamp_enrichment_decision['optimizer_path_evaluation_ran']}`",
        f"- production_approval_claimed: `{timestamp_enrichment_decision['production_approval_claimed']}`",
        "",
        "## Source Coverage",
        "",
        f"- ibes_anndats_act_count: `{source_coverage_report['ibes_anndats_act_count']}`",
        f"- compustat_rdq_count: `{source_coverage_report['compustat_rdq_count']}`",
        f"- exact_release_timestamp_count: `{source_coverage_report['exact_release_timestamp_count']}`",
        f"- sec_filing_timestamp_count: `{source_coverage_report['sec_filing_timestamp_count']}`",
        f"- date_only_no_repair_count: `{source_coverage_report['date_only_no_repair_count']}`",
        f"- no_auditable_source_count: `{source_coverage_report['no_auditable_source_count']}`",
        "",
        "## Boundaries",
        "",
        "- Repair candidates are written for later review only; H1E is not rerun here.",
        "- Date-only source fields are audit evidence but not exact tradable timestamps.",
        "- SEC filing timestamps are cross-checks unless a source proves first public release.",
        "",
    ]
    return "\n".join(lines)


def validate_sue_timestamp_enrichment_report_language(text: str) -> None:
    """Reject misleading H1E.5 claims while allowing explicit non-claims."""

    scrubbed = str(text).lower()
    allowed_phrases = [
        "it does not prove sue alpha.",
        "it does not select a sue score.",
        "it does not run q2 or optimizer-path evaluation.",
        "no paper/live/broker/order/production workflow is approved.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_TIMESTAMP_ENRICHMENT_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE timestamp enrichment claim detected: {claim}")


def _load_events(path: str | Path) -> pd.DataFrame:
    raw = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(raw.columns))
    missing = set(SUE_HISTORICAL_EVENT_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError("SUE events missing required columns: " + ", ".join(sorted(missing)))
    rows = []
    for record in raw.loc[:, SUE_HISTORICAL_EVENT_COLUMNS].to_dict(orient="records"):
        clean = {key: (None if pd.isna(value) else value) for key, value in record.items()}
        for text_field in [
            "event_id",
            "symbol",
            "ibes_ticker",
            "cusip",
            "fiscal_period",
            "sue_definition",
            "data_source",
            "link_method",
            "pit_safety_status",
        ]:
            if clean.get(text_field) is not None:
                clean[text_field] = str(clean[text_field])
        rows.append(SueHistoricalEventRow.model_validate(clean).model_dump(mode="json"))
    frame = pd.DataFrame(rows)
    for column in raw.columns:
        if column not in frame.columns:
            frame[column] = raw[column].values
    return frame


def _load_sources(config: SueTimestampEnrichmentConfig) -> dict[str, pd.DataFrame]:
    return {
        "ibes": _read_optional_frame(config.ibes_actuals_path),
        "compustat": _read_optional_frame(config.compustat_quarterly_path),
        "release": _read_optional_frame(config.release_timestamps_path),
        "sec": _read_optional_frame(config.sec_filing_timestamps_path),
    }


def _timestamp_source_comparison(
    *,
    events: pd.DataFrame,
    sources: dict[str, pd.DataFrame],
    config: SueTimestampEnrichmentConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    lookups = _source_lookup_maps(sources)
    for event in events.to_dict(orient="records"):
        current_available = _to_utc_timestamp(event["event_available_timestamp"])
        current_tradable = _to_utc_timestamp(event["tradable_timestamp"])
        ibes = _lookup_ibes_source(event, lookups["ibes"])
        comp = _lookup_compustat_source(event, lookups["compustat"])
        release = _lookup_release_source(event, lookups["release"])
        sec = _lookup_sec_source(event, lookups["sec"], config)
        source_payloads = [item for item in [ibes, comp, release, sec] if item is not None]
        exact_candidates = [
            item
            for item in source_payloads
            if item["precision"] == "timestamp_precise" and item["usable_for_repair"]
        ]
        best = min(exact_candidates, key=lambda item: item["timestamp"]) if exact_candidates else None
        repaired_tradable = _tradable_from_timestamp(best["timestamp"], best.get("before_after_marker"), config) if best else None
        repairable = bool(
            best is not None
            and best["timestamp"] < current_available
            and repaired_tradable is not None
            and repaired_tradable < current_tradable
            and repaired_tradable >= best["timestamp"]
        )
        flags = _flags(
            event=event,
            current_available=current_available,
            ibes=ibes,
            comp=comp,
            release=release,
            sec=sec,
            repairable=repairable,
        )
        rows.append(
            {
                "schema_version": "sue_timestamp_source_comparison.v1",
                "event_id": event["event_id"],
                "symbol": event["symbol"],
                "ibes_ticker": event.get("ibes_ticker"),
                "fiscal_period": event["fiscal_period"],
                "announcement_date": event["announcement_date"],
                "current_event_available_timestamp": current_available.isoformat(),
                "current_tradable_timestamp": current_tradable.isoformat(),
                "ibes_anndats_act": _source_timestamp(ibes),
                "ibes_actual_eps": ibes.get("actual_eps") if ibes else None,
                "ibes_source_table_name": ibes.get("source_table_name") if ibes else None,
                "compustat_rdq": _source_timestamp(comp),
                "compustat_gvkey": comp.get("gvkey") if comp else None,
                "compustat_datadate": comp.get("datadate") if comp else None,
                "compustat_fqtr": comp.get("fqtr") if comp else None,
                "compustat_fyearq": comp.get("fyearq") if comp else None,
                "exact_release_datetime": _source_timestamp(release),
                "release_source_vendor": release.get("source_vendor") if release else None,
                "release_confidence_flag": release.get("confidence_flag") if release else None,
                "before_after_marker": release.get("before_after_marker") if release else None,
                "sec_filing_timestamp": _source_timestamp(sec),
                "sec_accession_id": sec.get("accession_id") if sec else None,
                "sec_filing_type": sec.get("filing_type") if sec else None,
                "sec_cross_check_only": bool(sec is not None and not sec.get("usable_for_repair", False)),
                "primary_flag": flags[0],
                "flags": ";".join(flags),
                "repairable": repairable,
                "repair_source": best["source_name"] if best else None,
                "repair_source_vendor": best.get("source_vendor") if best else None,
                "repaired_event_available_timestamp": best["timestamp"].isoformat() if repairable else None,
                "repaired_tradable_timestamp": repaired_tradable.isoformat() if repairable else None,
                "inferred_from_returns": False,
                "shifted_performance_used_as_evidence": False,
            }
        )
    return pd.DataFrame(rows)


def _source_coverage_report(
    *,
    comparison: pd.DataFrame,
    repairable_event_candidates: pd.DataFrame,
    nonrepairable_event_report: pd.DataFrame,
    sources: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    payload = {
        "schema_version": "sue_timestamp_source_coverage_report.v1",
        "event_count": int(len(comparison)),
        "ibes_anndats_act_count": int(comparison["ibes_anndats_act"].notna().sum()),
        "compustat_rdq_count": int(comparison["compustat_rdq"].notna().sum()),
        "exact_release_timestamp_count": int(comparison["exact_release_datetime"].notna().sum()),
        "sec_filing_timestamp_count": int(comparison["sec_filing_timestamp"].notna().sum()),
        "sec_cross_check_only_count": int(comparison["sec_cross_check_only"].sum()),
        "repairable_event_count": int(len(repairable_event_candidates)),
        "nonrepairable_event_count": int(len(nonrepairable_event_report)),
        "date_only_no_repair_count": int(comparison["flags"].str.contains("date_only_no_repair", regex=False).sum()),
        "no_auditable_source_count": int(comparison["flags"].str.contains("no_auditable_source", regex=False).sum()),
        "current_date_later_than_compustat_rdq_count": int(
            comparison["flags"].str.contains("current_date_later_than_compustat_rdq", regex=False).sum()
        ),
        "current_date_later_than_ibes_anndats_act_count": int(
            comparison["flags"].str.contains("current_date_later_than_ibes_anndats_act", regex=False).sum()
        ),
        "conflicting_ibes_compustat_dates_count": int(
            comparison["flags"].str.contains("conflicting_ibes_compustat_dates", regex=False).sum()
        ),
        "source_files_present": {name: not frame.empty for name, frame in sources.items()},
        "score_selection_ran": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _date_disagreement_report(comparison: pd.DataFrame) -> pd.DataFrame:
    mask = comparison["flags"].str.contains("current_date_later_than_|conflicting_ibes_compustat_dates", regex=True)
    columns = [
        "schema_version",
        "event_id",
        "symbol",
        "announcement_date",
        "current_event_available_timestamp",
        "ibes_anndats_act",
        "compustat_rdq",
        "exact_release_datetime",
        "sec_filing_timestamp",
        "flags",
    ]
    return comparison.loc[mask, columns].reset_index(drop=True)


def _repairable_event_candidates(comparison: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "schema_version",
        "event_id",
        "symbol",
        "fiscal_period",
        "repair_source",
        "repair_source_vendor",
        "repaired_event_available_timestamp",
        "repaired_tradable_timestamp",
        "current_event_available_timestamp",
        "current_tradable_timestamp",
        "inferred_from_returns",
        "shifted_performance_used_as_evidence",
        "flags",
    ]
    frame = comparison.loc[comparison["repairable"].astype(bool), columns].copy()
    frame["schema_version"] = "sue_timestamp_repairable_event_candidate.v1"
    return frame.reset_index(drop=True)


def _nonrepairable_event_report(comparison: pd.DataFrame) -> pd.DataFrame:
    frame = comparison.loc[~comparison["repairable"].astype(bool)].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=["schema_version", "event_id", "symbol", "nonrepairable_reason", "flags"]
        )
    reason = frame["primary_flag"].where(frame["primary_flag"].notna(), "nonrepairable_unknown")
    return pd.DataFrame(
        {
            "schema_version": "sue_timestamp_nonrepairable_event.v1",
            "event_id": frame["event_id"],
            "symbol": frame["symbol"],
            "nonrepairable_reason": reason,
            "flags": frame["flags"],
        }
    ).reset_index(drop=True)


def _timestamp_enrichment_decision(
    *,
    comparison: pd.DataFrame,
    repairable_event_candidates: pd.DataFrame,
    coverage: dict[str, Any],
) -> dict[str, Any]:
    repairable_count = int(len(repairable_event_candidates))
    if repairable_count == 0:
        decision = "timestamp_enrichment_no_repair_sue_blocked"
    elif repairable_count == len(comparison):
        decision = "timestamp_enrichment_repair_available"
    elif repairable_count > 0:
        decision = "timestamp_enrichment_partial_repair_available"
    else:
        decision = "timestamp_enrichment_inconclusive"
    payload = {
        "schema_version": SUE_TIMESTAMP_ENRICHMENT_SCHEMA_VERSION,
        "decision_label": decision,
        "event_count": int(len(comparison)),
        "repairable_event_count": repairable_count,
        "selected_score": None,
        "score_selection_ran": False,
        "h1e_rerun_attempted": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "typed_projection_ran": False,
        "ibes_anndats_act_count": coverage["ibes_anndats_act_count"],
        "compustat_rdq_count": coverage["compustat_rdq_count"],
        "exact_release_timestamp_count": coverage["exact_release_timestamp_count"],
        "sec_filing_timestamp_count": coverage["sec_filing_timestamp_count"],
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _source_lookup_maps(sources: dict[str, pd.DataFrame]) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        "ibes": _build_source_lookup(sources["ibes"], _ibes_lookup_keys),
        "compustat": _build_source_lookup(sources["compustat"], _compustat_lookup_keys),
        "release": _build_source_lookup(sources["release"], _generic_lookup_keys),
        "sec": _build_source_lookup(sources["sec"], _generic_lookup_keys),
    }


def _build_source_lookup(
    frame: pd.DataFrame,
    key_builder: Any,
) -> dict[str, dict[str, Any]]:
    if frame.empty:
        return {}
    lookup: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        for key in key_builder(row):
            lookup.setdefault(key, row)
    return lookup


def _event_lookup_keys(event: dict[str, Any]) -> list[str]:
    keys = [f"event_id::{event['event_id']}"]
    fiscal_period = str(event["fiscal_period"])
    symbol = str(event["symbol"]).upper()
    ibes_ticker = str(event.get("ibes_ticker") or symbol).upper()
    keys.append(f"symbol_fiscal::{symbol}::{fiscal_period}")
    keys.append(f"ibes_fiscal::{ibes_ticker}::{fiscal_period}")
    return keys


def _generic_lookup_keys(row: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    event_id = row.get("event_id")
    fiscal_period = row.get("fiscal_period")
    if event_id is not None and not pd.isna(event_id):
        keys.append(f"event_id::{event_id}")
    if fiscal_period is not None and not pd.isna(fiscal_period):
        if row.get("symbol") is not None and not pd.isna(row.get("symbol")):
            keys.append(f"symbol_fiscal::{str(row['symbol']).upper()}::{fiscal_period}")
        if row.get("ibes_ticker") is not None and not pd.isna(row.get("ibes_ticker")):
            keys.append(f"ibes_fiscal::{str(row['ibes_ticker']).upper()}::{fiscal_period}")
    return keys


def _ibes_lookup_keys(row: dict[str, Any]) -> list[str]:
    return _generic_lookup_keys(row)


def _compustat_lookup_keys(row: dict[str, Any]) -> list[str]:
    return _generic_lookup_keys(row)


def _lookup_row(event: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    for key in _event_lookup_keys(event):
        if key in lookup:
            return lookup[key]
    return None


def _lookup_ibes_source(event: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not lookup:
        return None
    row = _lookup_row(event, lookup)
    if row is None:
        return None
    timestamp = _parse_timestamp(row.get("anndats_act_timestamp") or row.get("anndats_act"), force_date_only="anndats_act_timestamp" not in row)
    if timestamp is None:
        return None
    return {
        "source_name": "ibes_anndats_act",
        "timestamp": timestamp[0],
        "precision": timestamp[1],
        "actual_eps": row.get("actual_eps"),
        "source_table_name": row.get("source_table_name") or "wrds_ibes_actuals",
        "source_extraction_timestamp": row.get("source_extraction_timestamp"),
        "usable_for_repair": timestamp[1] == "timestamp_precise",
    }


def _lookup_compustat_source(event: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not lookup:
        return None
    row = _lookup_row(event, lookup)
    if row is None:
        return None
    timestamp = _parse_timestamp(row.get("rdq"), force_date_only=True)
    if timestamp is None:
        return None
    return {
        "source_name": "compustat_rdq",
        "timestamp": timestamp[0],
        "precision": timestamp[1],
        "gvkey": row.get("gvkey"),
        "datadate": row.get("datadate"),
        "fqtr": row.get("fqtr"),
        "fyearq": row.get("fyearq"),
        "source_table_name": row.get("source_table_name") or "comp.fundq",
        "usable_for_repair": False,
    }


def _lookup_release_source(event: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if not lookup:
        return None
    row = _lookup_row(event, lookup)
    if row is None:
        return None
    timestamp = _parse_timestamp(row.get("exact_release_datetime") or row.get("release_datetime"))
    if timestamp is None:
        return None
    confidence = str(row.get("confidence_flag") or "").lower()
    return {
        "source_name": "exact_release_datetime",
        "timestamp": timestamp[0],
        "precision": timestamp[1],
        "source_vendor": row.get("source_vendor"),
        "confidence_flag": row.get("confidence_flag"),
        "before_after_marker": row.get("before_after_marker"),
        "usable_for_repair": timestamp[1] == "timestamp_precise" and confidence in {"high", "audited", "verified"},
    }


def _lookup_sec_source(
    event: dict[str, Any],
    lookup: dict[str, dict[str, Any]],
    config: SueTimestampEnrichmentConfig,
) -> dict[str, Any] | None:
    if not lookup:
        return None
    row = _lookup_row(event, lookup)
    if row is None:
        return None
    timestamp = _parse_timestamp(row.get("filing_datetime") or row.get("acceptance_datetime"))
    if timestamp is None:
        return None
    proven = bool(row.get("sec_first_public_release_proven", False))
    return {
        "source_name": "sec_filing_timestamp",
        "timestamp": timestamp[0],
        "precision": timestamp[1],
        "accession_id": row.get("accession_id"),
        "filing_type": row.get("filing_type"),
        "usable_for_repair": bool(config.allow_sec_as_first_public_release and proven),
    }


def _flags(
    *,
    event: dict[str, Any],
    current_available: pd.Timestamp,
    ibes: dict[str, Any] | None,
    comp: dict[str, Any] | None,
    release: dict[str, Any] | None,
    sec: dict[str, Any] | None,
    repairable: bool,
) -> list[str]:
    flags: list[str] = []
    current_date = current_available.date()
    if ibes and ibes["timestamp"].date() < current_date:
        flags.append("current_date_later_than_ibes_anndats_act")
    if comp and comp["timestamp"].date() < current_date:
        flags.append("current_date_later_than_compustat_rdq")
    if ibes and comp and abs((ibes["timestamp"].date() - comp["timestamp"].date()).days) > 1:
        flags.append("conflicting_ibes_compustat_dates")
    if release and release["precision"] == "timestamp_precise":
        flags.append("exact_timestamp_available")
        marker = str(release.get("before_after_marker") or "").lower()
        if marker == "before_open":
            flags.append("before_open_event")
        elif marker == "after_close":
            flags.append("after_close_event")
    if sec and not sec.get("usable_for_repair", False):
        flags.append("sec_cross_check_only")
    if any(item and item["precision"] == "date_only" for item in [ibes, comp]) and not repairable:
        flags.append("date_only_no_repair")
    if not any([ibes, comp, release, sec]):
        flags.append("no_auditable_source")
    if repairable:
        flags.insert(0, "repair_candidate")
    elif "date_only_no_repair" in flags:
        flags.remove("date_only_no_repair")
        flags.insert(0, "date_only_no_repair")
    return flags or ["no_auditable_source"]


def _match_source_rows(event: dict[str, Any], frame: pd.DataFrame) -> pd.DataFrame:
    candidates = frame.copy()
    if "event_id" in candidates.columns:
        rows = candidates.loc[candidates["event_id"].astype(str).eq(str(event["event_id"]))]
        return rows
    mask = pd.Series([True] * len(candidates), index=candidates.index)
    if "fiscal_period" in candidates.columns:
        mask = mask & candidates["fiscal_period"].astype(str).eq(str(event["fiscal_period"]))
    if "symbol" in candidates.columns:
        mask = mask & candidates["symbol"].astype(str).str.upper().eq(str(event["symbol"]).upper())
    elif "ibes_ticker" in candidates.columns:
        mask = mask & candidates["ibes_ticker"].astype(str).str.upper().eq(str(event.get("ibes_ticker") or event["symbol"]).upper())
    return candidates.loc[mask]


def _tradable_from_timestamp(
    source_timestamp: pd.Timestamp,
    marker: Any,
    config: SueTimestampEnrichmentConfig,
) -> pd.Timestamp:
    open_time = time(config.market_open_utc_hour, config.market_open_utc_minute)
    marker_text = str(marker or "").lower()
    if marker_text == "before_open" or source_timestamp.hour < config.before_open_utc_hour:
        same_day_open = _market_open(source_timestamp.date(), open_time)
        return same_day_open if same_day_open >= source_timestamp else _market_open(_next_business_day(source_timestamp.date()), open_time)
    return _market_open(_next_business_day(source_timestamp.date()), open_time)


def _parse_timestamp(value: Any, *, force_date_only: bool = False) -> tuple[pd.Timestamp, str] | None:
    if value is None or pd.isna(value) or not str(value).strip():
        return None
    text = str(value).strip()
    timestamp = pd.Timestamp(text)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    else:
        timestamp = timestamp.tz_convert(timezone.utc)
    if force_date_only or len(text) <= 10 or (timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0):
        return timestamp, "date_only"
    return timestamp, "timestamp_precise"


def _to_utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(timezone.utc)
    return timestamp.tz_convert(timezone.utc)


def _source_timestamp(source: dict[str, Any] | None) -> str | None:
    if source is None:
        return None
    return source["timestamp"].isoformat()


def _read_optional_frame(path: str | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    if file_path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(file_path)
    else:
        frame = pd.read_csv(file_path)
    validate_no_forward_return_feature_columns(list(frame.columns))
    return frame


def _optional_path(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value)


def _market_open(value: Any, open_time: time) -> pd.Timestamp:
    return pd.Timestamp.combine(pd.Timestamp(value).date(), open_time).tz_localize(timezone.utc)


def _next_business_day(value: Any) -> Any:
    return (pd.Timestamp(value) + pd.offsets.BDay(1)).date()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
