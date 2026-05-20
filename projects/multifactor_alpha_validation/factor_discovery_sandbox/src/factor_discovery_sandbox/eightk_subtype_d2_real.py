"""Real local EDGAR 8-K source admission and D2 subtype replay."""

from __future__ import annotations

import html
import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from factor_discovery_sandbox.eightk_subtype_d2 import (
    DOWNSTREAM_FLAGS,
    PRIORITY_SUBTYPES,
    run_eightk_subtype_observability_d2,
)


REAL_SUMMARY_SCHEMA_VERSION = "eightk_subtype_d2_real_summary.v1"
STAGE = "D2-8K-01R"
CANDIDATE_ID = "8k_subtype_underreaction_observability_real_archive"
ACCEPTED_FORM_TYPES = {"8-K", "8-K/A"}


@dataclass(frozen=True)
class RealEightKSubtypeD2Result:
    """Artifacts and summary for D2-8K-01R."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_eightk_subtype_observability(
    source_dir: str | Path,
    price_panel_path: str | Path,
    output_dir: str | Path,
    additional_price_panel_paths: Iterable[str | Path] | None = None,
    benchmark_panel_path: str | Path | None = None,
    start_offset: int = 0,
    max_files: int | None = None,
    minimum_subtype_events: int = 100,
    minimum_event_month_count: int = 12,
    minimum_label_coverage_share: float = 0.70,
    allow_network: bool = False,
) -> RealEightKSubtypeD2Result:
    """Run local-only real EDGAR 8-K source admission and subtype D2 replay."""

    if allow_network:
        raise ValueError("D2-8K-01R is local-source-only and does not support network fetches.")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    source_path = Path(source_dir)
    price_path = Path(price_panel_path)
    additional_price_paths = [Path(path) for path in (additional_price_panel_paths or [])]

    if not source_path.exists():
        summary = _unavailable_summary("unavailable_missing_source", "blocked_source_coverage", ["source_dir"])
        _write_json(artifacts["missing_inputs_report"], {"missing_inputs": ["source_dir"], **summary})
        _write_json(artifacts["d2_8k_subtype_summary_real"], summary)
        artifacts["d2_8k_subtype_report_real"].write_text(_render_real_report(summary), encoding="utf-8")
        return RealEightKSubtypeD2Result(summary=summary, artifacts=artifacts)

    if not price_path.exists():
        summary = _unavailable_summary("unavailable_missing_price_panel", "blocked_market_coverage", ["price_panel_path"])
        _write_json(artifacts["missing_inputs_report"], {"missing_inputs": ["price_panel_path"], **summary})
        _write_json(artifacts["d2_8k_subtype_summary_real"], summary)
        artifacts["d2_8k_subtype_report_real"].write_text(_render_real_report(summary), encoding="utf-8")
        return RealEightKSubtypeD2Result(summary=summary, artifacts=artifacts)

    full_index = _load_source_index(source_path)
    source_index_total_count = int(len(full_index))
    source_index = _slice_source_index(full_index, start_offset=start_offset, max_files=max_files)
    registry, raw_locator, document_type_audit, item_header_audit = _parse_archive(source_path, source_index)
    filtered_price_path = output_path / "_real_replay_input" / "filtered_price_panel_for_d2.csv"
    filtered_price_report = _write_filtered_market_panel(
        price_paths=[price_path, *additional_price_paths],
        tickers=registry["ticker"].astype(str).replace("", pd.NA).dropna().unique().tolist() if "ticker" in registry else [],
        output_path=filtered_price_path,
    )
    joined_registry, issuer_market_join = _apply_market_coverage(registry, filtered_price_path, filtered_price_report)
    accepted_timestamp_coverage = _accepted_timestamp_coverage(joined_registry)

    d2_input_path = output_path / "_real_replay_input" / "eightk_event_registry_for_d2.csv"
    d2_input_path.parent.mkdir(parents=True, exist_ok=True)
    joined_registry.to_csv(d2_input_path, index=False)
    base_output_dir = output_path / "_base_d2_replay"
    base_result = run_eightk_subtype_observability_d2(
        event_registry_path=d2_input_path,
        price_panel_path=filtered_price_path,
        benchmark_panel_path=benchmark_panel_path,
        output_dir=base_output_dir,
        minimum_subtype_events=minimum_subtype_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )
    _copy_base_artifacts(base_result.artifacts, artifacts)

    registry_for_artifact = joined_registry.copy()
    registry_for_artifact["formula_score_written"] = False
    registry_for_artifact["measurement_spec_written"] = False
    registry_for_artifact["q1_entry_allowed"] = False
    registry_for_artifact["q2_entry_allowed"] = False
    registry_for_artifact["expected_return_panel_written"] = False
    registry_for_artifact.to_csv(artifacts["eightk_event_registry_real"], index=False)
    raw_locator.to_csv(artifacts["raw_locator_coverage_report"], index=False)
    document_type_audit.to_csv(artifacts["document_type_audit"], index=False)
    item_header_audit.to_csv(artifacts["item_header_parse_audit"], index=False)
    issuer_market_join.to_csv(artifacts["issuer_market_join_coverage"], index=False)
    accepted_timestamp_coverage.to_csv(artifacts["accepted_timestamp_coverage"], index=False)

    source_admission = _source_admission_report(
        source_path=source_path,
        source_index_total_count=source_index_total_count,
        source_index=source_index,
        raw_locator=raw_locator,
        accepted_timestamp_coverage=accepted_timestamp_coverage,
        item_header_audit=item_header_audit,
        issuer_market_join=issuer_market_join,
        filtered_price_report=filtered_price_report,
        start_offset=start_offset,
        max_files=max_files,
    )
    _write_json(artifacts["source_admission_report"], source_admission)

    summary = _build_real_summary(
        base_summary=base_result.summary,
        source_admission=source_admission,
        registry=registry_for_artifact,
        item_header_audit=item_header_audit,
        minimum_subtype_events=minimum_subtype_events,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )
    _write_json(artifacts["d2_8k_subtype_summary_real"], summary)
    artifacts["d2_8k_subtype_report_real"].write_text(
        _render_real_report(summary, source_admission, item_header_audit),
        encoding="utf-8",
    )
    return RealEightKSubtypeD2Result(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "source_admission_report": output_path / "source_admission_report.json",
        "raw_locator_coverage_report": output_path / "raw_locator_coverage_report.csv",
        "accepted_timestamp_coverage": output_path / "accepted_timestamp_coverage.csv",
        "document_type_audit": output_path / "document_type_audit.csv",
        "item_header_parse_audit": output_path / "item_header_parse_audit.csv",
        "issuer_market_join_coverage": output_path / "issuer_market_join_coverage.csv",
        "eightk_event_registry_real": output_path / "eightk_event_registry_real.csv",
        "eightk_subtype_counts_real": output_path / "eightk_subtype_counts_real.csv",
        "timestamp_audit_real": output_path / "timestamp_audit_real.csv",
        "coverage_report_real": output_path / "coverage_report_real.csv",
        "no_view_reason_report_real": output_path / "no_view_reason_report_real.csv",
        "car_window_panel_real": output_path / "car_window_panel_real.csv",
        "matched_control_panel_real": output_path / "matched_control_panel_real.csv",
        "placebo_report_real": output_path / "placebo_report_real.csv",
        "d2_8k_subtype_summary_real": output_path / "d2_8k_subtype_summary_real.json",
        "d2_8k_subtype_report_real": output_path / "d2_8k_subtype_report_real.md",
        "missing_inputs_report": output_path / "missing_inputs_report.json",
    }


def _unavailable_summary(status: str, decision: str, missing_inputs: list[str]) -> dict[str, object]:
    summary: dict[str, object] = {
        "schema_version": REAL_SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "source_type": "local_edgar_8k_archive",
        "real_data_status": status,
        "network_used": False,
        "missing_inputs": missing_inputs,
        "source_index_total_count": 0,
        "event_count": 0,
        "priority_event_count": 0,
        "overall_decision": decision,
        "decision_reason": status,
        "allow_d3_charter_for": [],
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
    }
    summary.update(DOWNSTREAM_FLAGS)
    return summary


def _load_source_index(source_path: Path) -> pd.DataFrame:
    request_specs_path = source_path / "request_specs.json"
    rows: list[dict[str, object]] = []
    if request_specs_path.exists():
        request_specs = json.loads(request_specs_path.read_text(encoding="utf-8"))
        for row in request_specs:
            form_type = _normalize_form_type(str(row.get("formType", "")))
            if form_type not in ACCEPTED_FORM_TYPES:
                continue
            relative_path = str(row.get("relative_path", ""))
            if not relative_path:
                continue
            rows.append(
                {
                    "file": relative_path,
                    "accession_number": _accession_from_relative_path(relative_path),
                    "filing_accepted_ts": str(row.get("acceptedDate", "")),
                    "symbol": str(row.get("symbol", "")),
                    "issuer_cik": str(row.get("cik", "")),
                    "form_type": form_type,
                    "source_url": str(row.get("url", "") or row.get("finalLink", "")),
                    "source_metadata_path": "request_specs.json",
                },
            )
        frame = pd.DataFrame(rows).fillna("")
        frame.attrs["source_layout"] = "sec_filing_archive_request_specs"
        return frame
    for path in sorted(source_path.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".htm", ".html", ".txt", ".xml"}:
            continue
        lower = str(path).lower()
        if "8-k" not in lower and "8_k" not in lower:
            continue
        rel = path.relative_to(source_path)
        rows.append(
            {
                "file": str(rel),
                "accession_number": _accession_from_relative_path(str(rel)),
                "filing_accepted_ts": "",
                "symbol": _symbol_from_relative_path(rel),
                "issuer_cik": "",
                "form_type": "8-K/A" if "8-k_a" in lower or "8-k/a" in lower else "8-K",
                "source_url": "",
                "source_metadata_path": "filesystem_scan",
            },
        )
    frame = pd.DataFrame(rows).fillna("")
    frame.attrs["source_layout"] = "filesystem_scan"
    return frame


def _slice_source_index(index: pd.DataFrame, start_offset: int, max_files: int | None) -> pd.DataFrame:
    if start_offset < 0:
        raise ValueError("start_offset must be non-negative.")
    if max_files is not None and max_files < 1:
        raise ValueError("max_files must be positive when provided.")
    stop = None if max_files is None else start_offset + max_files
    sliced = index.iloc[start_offset:stop].reset_index(drop=True).copy()
    sliced.attrs.update(index.attrs)
    return sliced


def _parse_archive(
    source_path: Path,
    source_index: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    registry_rows: list[dict[str, object]] = []
    raw_rows: list[dict[str, object]] = []
    document_rows: list[dict[str, object]] = []
    item_rows: list[dict[str, object]] = []
    for idx, row in enumerate(source_index.to_dict("records")):
        file_value = str(row.get("file", ""))
        accession = str(row.get("accession_number", "")) or _accession_from_relative_path(file_value)
        raw_path, raw_found, locator_strategy = _resolve_source_document(source_path, file_value, str(row.get("form_type", "")))
        resolved_file = str(raw_path.relative_to(source_path)) if raw_found else file_value
        raw_found = raw_path.exists()
        raw_rows.append(
            {
                "file": resolved_file,
                "requested_file": file_value,
                "resolved_file": resolved_file,
                "accession_number": accession,
                "raw_file_found": bool(raw_found),
                "source_locator_strategy": locator_strategy,
                "network_used": False,
            },
        )
        if not raw_found:
            document_rows.append(
                {
                    "file": resolved_file,
                    "accession_number": accession,
                    "document_type": str(row.get("form_type", "")),
                    "document_type_allowed": False,
                    "parse_status": "missing_file",
                },
            )
            item_rows.append(
                {
                    "file": resolved_file,
                    "accession_number": accession,
                    "item_header_found": False,
                    "parsed_items": "",
                    "eightk_subtype": "unknown_no_view",
                    "parse_status": "missing_file",
                },
            )
            continue
        text = raw_path.read_text(encoding="utf-8", errors="ignore")
        clean_text = _clean_document_text(text)
        document_type = _document_type(clean_text, str(row.get("form_type", "")))
        allowed_doc = document_type in ACCEPTED_FORM_TYPES
        subtype, no_view_reason, item_headers = _classify_document(clean_text, document_type)
        item_rows.append(
            {
                "file": file_value,
                "requested_file": file_value,
                "resolved_file": resolved_file,
                "accession_number": accession,
                "item_header_found": bool(item_headers),
                "parsed_items": "|".join(item_headers),
                "eightk_subtype": subtype,
                "parse_status": "parsed" if item_headers else "no_item_header",
            },
        )
        document_rows.append(
            {
                "file": file_value,
                "requested_file": file_value,
                "resolved_file": resolved_file,
                "accession_number": accession,
                "document_type": document_type,
                "document_type_allowed": bool(allowed_doc),
                "parse_status": "parsed" if allowed_doc else "unsupported_document_type",
            },
        )
        accepted = str(row.get("filing_accepted_ts", ""))
        accepted_dt = _parse_timestamp(accepted)
        visibility = accepted_dt.isoformat() if accepted_dt else ""
        tradable = _next_regular_market_open(accepted_dt).isoformat() if accepted_dt else ""
        coverage_state = "no_view" if subtype == "unknown_no_view" else "covered"
        reason = no_view_reason if subtype == "unknown_no_view" else ""
        registry_rows.append(
            {
                "event_id": f"8k_{idx}_{accession}",
                "ticker": str(row.get("symbol", "")),
                "issuer_cik": str(row.get("issuer_cik", "")),
                "accession_number": accession,
                "form_type": document_type,
                "filing_accepted_ts": visibility,
                "tradable_timestamp": tradable,
                "event_item": "|".join(item_headers),
                "event_description": _short_description(clean_text),
                "eightk_subtype": subtype,
                "sector": "",
                "size_bucket": "",
                "liquidity_bucket": "",
                "coverage_state": coverage_state,
                "no_view_reason": reason,
                "diagnostic_only": subtype == "unknown_no_view",
                "event_month": accepted_dt.strftime("%Y-%m") if accepted_dt else "",
                "raw_document_path": str(raw_path),
                "requested_document_path": str(source_path / file_value),
                "source_locator_strategy": locator_strategy,
                "no_view_not_zero_alpha": True,
                "not_alpha_evidence": True,
            },
        )
    registry = pd.DataFrame(registry_rows).fillna("")
    if registry.empty:
        registry = pd.DataFrame(
            columns=[
                "event_id",
                "ticker",
                "issuer_cik",
                "accession_number",
                "form_type",
                "filing_accepted_ts",
                "tradable_timestamp",
                "event_item",
                "event_description",
                "eightk_subtype",
                "sector",
                "size_bucket",
                "liquidity_bucket",
                "coverage_state",
                "no_view_reason",
                "diagnostic_only",
                "event_month",
                "raw_document_path",
                "requested_document_path",
                "source_locator_strategy",
                "no_view_not_zero_alpha",
                "not_alpha_evidence",
            ],
        )
    return registry, pd.DataFrame(raw_rows), pd.DataFrame(document_rows), pd.DataFrame(item_rows)


def _write_filtered_market_panel(price_paths: list[Path], tickers: list[str], output_path: Path) -> dict[str, object]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ticker_keys = {str(ticker).upper() for ticker in tickers if str(ticker).strip()}
    frames: list[pd.DataFrame] = []
    present_paths: list[str] = []
    missing_paths: list[str] = []
    for price_path in price_paths:
        if not price_path.exists():
            missing_paths.append(str(price_path))
            continue
        present_paths.append(str(price_path))
        columns = pd.read_csv(price_path, nrows=0).columns
        if "ticker" not in columns or "date" not in columns:
            continue
        desired = [
            column
            for column in (
                "ticker",
                "date",
                "adjusted_close",
                "raw_close",
                "close",
                "price",
                "dlyprc",
                "market_cap",
                "dlycap",
                "dollar_volume",
                "dlyprcvol",
                "bid_ask_spread",
                "sector",
                "industry",
            )
            if column in columns
        ]
        chunks = pd.read_csv(price_path, usecols=desired, chunksize=250_000, low_memory=False)
        for chunk in chunks:
            filtered = chunk[chunk["ticker"].astype(str).str.upper().isin(ticker_keys)].copy()
            if filtered.empty:
                continue
            filtered["_price_source_path"] = str(price_path)
            frames.append(filtered)
    if frames:
        price = pd.concat(frames, ignore_index=True).fillna("")
        price["_ticker_key"] = price["ticker"].astype(str).str.upper()
        price["_date"] = pd.to_datetime(price["date"], errors="coerce").dt.normalize()
        price = price[price["_date"].notna()].sort_values(["_ticker_key", "_date"])
        price = price.drop_duplicates(subset=["_ticker_key", "_date"], keep="first")
        price = price.drop(columns=["_ticker_key", "_date"])
    else:
        price = pd.DataFrame(columns=["ticker", "date", "adjusted_close", "_price_source_path"])
    price.to_csv(output_path, index=False)
    return {
        "price_panel_paths": present_paths,
        "missing_price_panel_paths": missing_paths,
        "price_panel_count": int(len(present_paths)),
        "filtered_price_panel_path": str(output_path),
        "filtered_price_row_count": int(len(price)),
        "filtered_price_ticker_count": int(price["ticker"].astype(str).str.upper().nunique()) if "ticker" in price else 0,
    }


def _apply_market_coverage(
    events: pd.DataFrame,
    price_panel_path: Path,
    filtered_price_report: dict[str, object],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = events.copy()
    if joined.empty:
        return joined, pd.DataFrame([{"event_count": 0, "covered_count": 0, "no_view_count": 0, "status": "empty"}])
    price = pd.read_csv(price_panel_path, low_memory=False).fillna("")
    if "ticker" not in price.columns or "date" not in price.columns:
        joined["coverage_state"] = "no_view"
        joined["no_view_reason"] = "missing_market_join"
        return joined, pd.DataFrame(
            [{"event_count": int(len(joined)), "covered_count": 0, "no_view_count": int(len(joined)), "status": "missing_price_columns"}],
        )
    price = price.copy()
    price["_ticker_key"] = price["ticker"].astype(str).str.upper()
    price["_date"] = pd.to_datetime(price["date"], errors="coerce").dt.normalize()
    by_ticker = {ticker: group for ticker, group in price.groupby("_ticker_key", sort=False)}
    for index, row in joined.iterrows():
        if str(row.get("coverage_state", "")).lower() == "no_view":
            continue
        ticker_key = str(row.get("ticker", "")).upper()
        rows = by_ticker.get(ticker_key)
        event_date = pd.to_datetime(row.get("tradable_timestamp", ""), errors="coerce", utc=True)
        if rows is None or pd.isna(event_date):
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_market_join"
            continue
        if rows["_date"].searchsorted(event_date.tz_convert(None).normalize()) >= len(rows):
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_market_join"
            continue
        joined.loc[index, "coverage_state"] = "covered"
        joined.loc[index, "no_view_reason"] = ""
        for column in ("sector", "size_bucket", "liquidity_bucket"):
            if column in rows.columns and str(joined.loc[index, column]) == "":
                value = rows[column].replace("", pd.NA).dropna()
                if not value.empty:
                    joined.loc[index, column] = value.iloc[0]
    covered = int(joined["coverage_state"].astype(str).str.lower().eq("covered").sum())
    priority = joined["eightk_subtype"].isin(PRIORITY_SUBTYPES) if "eightk_subtype" in joined else pd.Series(False, index=joined.index)
    priority_covered = joined.loc[priority, "coverage_state"].astype(str).str.lower().eq("covered") if bool(priority.any()) else pd.Series(dtype=bool)
    audit = pd.DataFrame(
        [
            {
                "event_count": int(len(joined)),
                "covered_count": covered,
                "no_view_count": int(len(joined) - covered),
                "market_coverage_share": _safe_divide(float(covered), float(len(joined))),
                "priority_event_count": int(priority.sum()),
                "priority_covered_count": int(priority_covered.sum()) if len(priority_covered) else 0,
                "priority_market_coverage_share": (
                    _safe_divide(float(priority_covered.sum()), float(priority.sum())) if bool(priority.any()) else 0.0
                ),
                "status": "joined_daily_price_panel",
                "price_panel_count": int(filtered_price_report.get("price_panel_count", 0)),
                "price_panel_paths": "|".join(str(path) for path in filtered_price_report.get("price_panel_paths", [])),
                "filtered_price_row_count": int(filtered_price_report.get("filtered_price_row_count", 0)),
                "filtered_price_ticker_count": int(filtered_price_report.get("filtered_price_ticker_count", 0)),
                "missing_coverage_policy": "no_view_abstain_not_zero",
            },
        ],
    )
    return joined, audit


def _accepted_timestamp_coverage(events: pd.DataFrame) -> pd.DataFrame:
    total = len(events)
    accepted = events["filing_accepted_ts"].astype(str).str.len().gt(0) if total else pd.Series(dtype=bool)
    tradable = events["tradable_timestamp"].astype(str).str.len().gt(0) if total else pd.Series(dtype=bool)
    return pd.DataFrame(
        [
            {
                "event_count": int(total),
                "accepted_timestamp_count": int(accepted.sum()) if total else 0,
                "tradable_timestamp_count": int(tradable.sum()) if total else 0,
                "accepted_timestamp_coverage_share": _safe_divide(float(accepted.sum()), float(total)) if total else 0.0,
                "tradable_timestamp_policy": "next_regular_market_open_after_sec_acceptance",
                "status": "pass" if total and bool(accepted.all() and tradable.all()) else "fail",
            },
        ],
    )


def _source_admission_report(
    source_path: Path,
    source_index_total_count: int,
    source_index: pd.DataFrame,
    raw_locator: pd.DataFrame,
    accepted_timestamp_coverage: pd.DataFrame,
    item_header_audit: pd.DataFrame,
    issuer_market_join: pd.DataFrame,
    filtered_price_report: dict[str, object],
    start_offset: int,
    max_files: int | None,
) -> dict[str, object]:
    raw_found = raw_locator["raw_file_found"].astype(bool) if not raw_locator.empty else pd.Series(dtype=bool)
    item_found = item_header_audit["item_header_found"].astype(bool) if not item_header_audit.empty else pd.Series(dtype=bool)
    market_coverage = float(issuer_market_join["market_coverage_share"].iloc[0]) if not issuer_market_join.empty else 0.0
    priority_market_coverage = (
        float(issuer_market_join["priority_market_coverage_share"].iloc[0]) if not issuer_market_join.empty else 0.0
    )
    accepted_coverage = (
        float(accepted_timestamp_coverage["accepted_timestamp_coverage_share"].iloc[0])
        if not accepted_timestamp_coverage.empty
        else 0.0
    )
    raw_share = _safe_divide(float(raw_found.sum()), float(len(raw_found))) if len(raw_found) else 0.0
    item_share = _safe_divide(float(item_found.sum()), float(len(item_found))) if len(item_found) else 0.0
    return {
        "schema_version": "eightk_source_admission_report.v1",
        "stage": STAGE,
        "source_type": "local_edgar_8k_archive",
        "source_dir": str(source_path),
        "source_layout": source_index.attrs.get("source_layout", "unknown"),
        "network_used": False,
        "source_index_total_count": int(source_index_total_count),
        "source_index_start_offset": int(start_offset),
        "source_index_max_files": int(max_files) if max_files is not None else None,
        "indexed_file_count": int(len(source_index)),
        "raw_file_found_count": int(raw_found.sum()) if len(raw_found) else 0,
        "raw_file_found_share": raw_share,
        "accepted_timestamp_coverage_share": accepted_coverage,
        "item_header_parse_coverage_share": item_share,
        "market_coverage_share": market_coverage,
        "priority_market_coverage_share": priority_market_coverage,
        "priority_market_event_count": int(issuer_market_join["priority_event_count"].iloc[0]) if not issuer_market_join.empty else 0,
        "priority_market_covered_count": int(issuer_market_join["priority_covered_count"].iloc[0]) if not issuer_market_join.empty else 0,
        "price_panel_count": int(filtered_price_report.get("price_panel_count", 0)),
        "price_panel_paths": filtered_price_report.get("price_panel_paths", []),
        "filtered_price_panel_path": filtered_price_report.get("filtered_price_panel_path", ""),
        "filtered_price_row_count": int(filtered_price_report.get("filtered_price_row_count", 0)),
        "filtered_price_ticker_count": int(filtered_price_report.get("filtered_price_ticker_count", 0)),
        "status": "pass" if raw_share >= 0.80 and accepted_coverage >= 0.80 else "fail",
    }


def _build_real_summary(
    base_summary: dict[str, object],
    source_admission: dict[str, object],
    registry: pd.DataFrame,
    item_header_audit: pd.DataFrame,
    minimum_subtype_events: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> dict[str, object]:
    subtype_summaries = dict(base_summary.get("subtype_summaries", {}))
    subtype_decisions: dict[str, dict[str, object]] = {}
    eligible: list[str] = []
    raw_blocks_market = False
    for subtype in PRIORITY_SUBTYPES:
        subtype_events = registry[registry["eightk_subtype"].eq(subtype)]
        raw_event_count = int(len(subtype_events))
        month_count = int(subtype_events["event_month"].astype(str).replace("", pd.NA).dropna().nunique()) if raw_event_count else 0
        coverage_share = float(subtype_summaries.get(subtype, {}).get("primary_label_coverage_share", 0.0) or 0.0)
        d2_observable = bool(subtype_summaries.get(subtype, {}).get("d2_observable", False))
        if raw_event_count >= minimum_subtype_events and month_count >= minimum_event_month_count and coverage_share < minimum_label_coverage_share:
            decision = "blocked_market_coverage"
            reason = "subtype_sample_exists_but_market_label_coverage_is_insufficient"
            raw_blocks_market = True
        elif d2_observable:
            decision = "observable"
            reason = "subtype_passes_real_d2_observability_gate"
            eligible.append(subtype)
        elif raw_event_count < minimum_subtype_events or month_count < minimum_event_month_count:
            decision = "hold_insufficient_sample"
            reason = "subtype_does_not_meet_sample_or_month_threshold"
        else:
            decision = "mixed_narrow_scope"
            reason = "subtype_sample_present_but_d2_gate_is_not_clean"
        subtype_decisions[subtype] = {
            "decision": decision,
            "reason": reason,
            "raw_event_count": raw_event_count,
            "event_month_count": month_count,
            "primary_label_coverage_share": coverage_share,
            "not_alpha_evidence": True,
        }
    market_coverage_share = float(source_admission["market_coverage_share"])
    priority_market_coverage_share = float(source_admission.get("priority_market_coverage_share", 0.0))
    priority_market_event_count = int(source_admission.get("priority_market_event_count", 0))
    if float(source_admission["raw_file_found_share"]) < 0.80:
        overall_decision = "blocked_source_coverage"
        reason = "raw_8k_locator_coverage_below_threshold"
        allow_d3: list[str] = []
    elif float(source_admission["accepted_timestamp_coverage_share"]) < 0.80:
        overall_decision = "blocked_timestamp"
        reason = "accepted_timestamp_coverage_below_threshold"
        allow_d3 = []
    elif priority_market_event_count > 0 and priority_market_coverage_share < minimum_label_coverage_share:
        overall_decision = "blocked_market_coverage"
        reason = "priority_real_8k_market_join_coverage_below_threshold"
        allow_d3 = []
    elif raw_blocks_market:
        overall_decision = "blocked_market_coverage"
        reason = "one_or_more_sample_sufficient_subtypes_failed_market_label_coverage"
        allow_d3 = []
    elif not eligible:
        overall_decision = "hold_insufficient_sample"
        reason = "no_real_8k_subtype_met_d2_admission_thresholds"
        allow_d3 = []
    elif str(base_summary.get("overall_decision")) == "blocked_placebo_dominance":
        overall_decision = "blocked_placebo_dominance"
        reason = "placebo_dominance_in_base_d2_replay"
        allow_d3 = []
    else:
        overall_decision = "observable"
        reason = "one_real_8k_subtype_passed_source_timestamp_market_and_placebo_gates"
        allow_d3 = [_first_priority(eligible)]
    summary: dict[str, object] = {
        "schema_version": REAL_SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "source_type": "local_edgar_8k_archive",
        "real_data_status": "local_8k_replay_complete",
        "network_used": False,
        "source_index_total_count": int(source_admission["source_index_total_count"]),
        "source_index_start_offset": int(source_admission["source_index_start_offset"]),
        "source_index_max_files": source_admission["source_index_max_files"],
        "indexed_file_count": int(source_admission["indexed_file_count"]),
        "raw_file_found_share": float(source_admission["raw_file_found_share"]),
        "accepted_timestamp_coverage_share": float(source_admission["accepted_timestamp_coverage_share"]),
        "item_header_parse_coverage_share": float(source_admission["item_header_parse_coverage_share"]),
        "market_coverage_share": float(source_admission["market_coverage_share"]),
        "priority_market_coverage_share": float(source_admission.get("priority_market_coverage_share", 0.0)),
        "priority_market_event_count": int(source_admission.get("priority_market_event_count", 0)),
        "priority_market_covered_count": int(source_admission.get("priority_market_covered_count", 0)),
        "price_panel_count": int(source_admission.get("price_panel_count", 0)),
        "price_panel_paths": source_admission.get("price_panel_paths", []),
        "filtered_price_panel_path": source_admission.get("filtered_price_panel_path", ""),
        "filtered_price_row_count": int(source_admission.get("filtered_price_row_count", 0)),
        "filtered_price_ticker_count": int(source_admission.get("filtered_price_ticker_count", 0)),
        "event_count": int(len(registry)),
        "priority_event_count": int(registry["eightk_subtype"].isin(PRIORITY_SUBTYPES).sum()) if not registry.empty else 0,
        "item_header_found_count": int(item_header_audit["item_header_found"].astype(bool).sum()) if not item_header_audit.empty else 0,
        "overall_decision": overall_decision,
        "decision_reason": reason,
        "allow_d3_charter_for": allow_d3,
        "subtype_level_decisions": subtype_decisions,
        "base_d2_overall_decision": base_summary.get("overall_decision"),
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
        "minimum_subtype_events": int(minimum_subtype_events),
        "minimum_event_month_count": int(minimum_event_month_count),
        "minimum_label_coverage_share": float(minimum_label_coverage_share),
    }
    summary.update(DOWNSTREAM_FLAGS)
    return summary


def _copy_base_artifacts(base_artifacts: dict[str, Path], artifacts: dict[str, Path]) -> None:
    mapping = {
        "eightk_subtype_counts": "eightk_subtype_counts_real",
        "timestamp_audit": "timestamp_audit_real",
        "coverage_report": "coverage_report_real",
        "no_view_reason_report": "no_view_reason_report_real",
        "car_window_panel": "car_window_panel_real",
        "matched_control_panel": "matched_control_panel_real",
        "placebo_report": "placebo_report_real",
    }
    for source_key, target_key in mapping.items():
        source = base_artifacts[source_key]
        target = artifacts[target_key]
        if source.exists():
            shutil.copyfile(source, target)


def _classify_document(text: str, document_type: str) -> tuple[str, str, list[str]]:
    item_headers = _item_headers(text)
    lower = text.lower()
    if any(item.startswith("4.01") for item in item_headers) or "certifying accountant" in lower or "auditor" in lower:
        return "auditor_change", "", item_headers
    if any(item.startswith("4.02") for item in item_headers) or "non-reliance" in lower or "restatement" in lower:
        return "restatement_amendment", "", item_headers
    if any(item.startswith("5.02") for item in item_headers):
        if _is_appointment_or_compensation_only(lower):
            return "unknown_no_view", "excluded_appointment_or_compensation_only_5_02", item_headers
        if _has_departure_language(lower):
            if "chief executive officer" in lower or re.search(r"\bceo\b", lower):
                return "ceo_departure", "", item_headers
            if "chief financial officer" in lower or re.search(r"\bcfo\b", lower):
                return "cfo_departure", "", item_headers
    if any(item.startswith("1.02") for item in item_headers) and "agreement" in lower and (
        "termination" in lower or "terminated" in lower
    ):
        return "material_agreement_termination", "", item_headers
    if any(item.startswith("2.02") for item in item_headers):
        return "routine_8k_control", "", item_headers
    if document_type == "8-K/A":
        return "restatement_amendment", "", item_headers
    return "unknown_no_view", "unclassified_8k_subtype", item_headers


def _resolve_source_document(source_path: Path, requested_file: str, form_type: str) -> tuple[Path, bool, str]:
    requested_path = source_path / requested_file
    if not requested_path.exists():
        return requested_path, False, "requested_path_missing"
    filing_dir = requested_path.parent
    candidates = [path for path in filing_dir.iterdir() if path.is_file() and path.suffix.lower() in {".htm", ".html", ".txt"}]
    if not candidates:
        return requested_path, True, "requested_path"
    scored: list[tuple[float, Path]] = []
    for candidate in candidates:
        score = _source_document_score(candidate, form_type, requested_path)
        scored.append((score, candidate))
    scored.sort(key=lambda item: (item[0], item[1].name), reverse=True)
    best_score, best_path = scored[0]
    requested_score = _source_document_score(requested_path, form_type, requested_path)
    if best_path != requested_path and best_score > requested_score:
        return best_path, True, "same_accession_primary_8k_candidate"
    return requested_path, True, "requested_path"


def _source_document_score(path: Path, form_type: str, requested_path: Path) -> float:
    name = path.name.lower()
    score = 0.0
    if path == requested_path:
        score += 0.25
    if "8k" in name or "8-k" in name:
        score += 2.0
    if "ex99" in name or "exhibit" in name or re.search(r"\bex[-_\\.]", name):
        score -= 3.0
    try:
        text = _clean_document_text(path.read_text(encoding="utf-8", errors="ignore"))
    except OSError:
        return -100.0
    document_type = _document_type(text, form_type)
    if document_type in ACCEPTED_FORM_TYPES:
        score += 1.0
    item_count = len(_item_headers(text))
    score += item_count * 5.0
    if "current report" in text.lower() and "securities and exchange commission" in text.lower():
        score += 0.5
    return score


def _is_appointment_or_compensation_only(lower: str) -> bool:
    appointment = "appointed" in lower or "appointment" in lower or "elected" in lower
    compensation = "compensation" in lower or "award" in lower or "bonus" in lower
    no_departure = "no resignation" in lower or "no retirement" in lower or "no removal" in lower or "no termination" in lower
    return bool((appointment or compensation) and (no_departure or not _has_departure_language(lower)))


def _has_departure_language(lower: str) -> bool:
    return any(
        token in lower
        for token in (
            "resigned",
            "resignation",
            "retired",
            "retirement",
            "departed",
            "departure",
            "terminated",
            "termination",
            "removed",
            "separation",
        )
    )


def _item_headers(text: str) -> list[str]:
    headers = []
    for match in re.finditer(r"\bitem\s+([0-9]+\.[0-9]+)\b", text, flags=re.IGNORECASE):
        item = match.group(1)
        if item not in headers:
            headers.append(item)
    return headers


def _document_type(text: str, fallback: str) -> str:
    normalized = _normalize_form_type(fallback)
    if normalized in ACCEPTED_FORM_TYPES:
        return normalized
    match = re.search(r"\bform\s+(8-k/a|8-k)\b", text, flags=re.IGNORECASE)
    if match:
        return _normalize_form_type(match.group(1))
    return normalized or "8-K"


def _clean_document_text(raw: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", raw, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _short_description(text: str) -> str:
    return text[:300]


def _render_real_report(
    summary: dict[str, object],
    source_admission: dict[str, object] | None = None,
    item_header_audit: pd.DataFrame | None = None,
) -> str:
    lines = [
        "# D2-8K-01R Real EDGAR 8-K Archive Replay",
        "",
        "This is real EDGAR 8-K archive source admission and no-formula observability only.",
        "It is not alpha evidence.",
        "It does not run Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflows.",
        "Missing coverage remains no_view / abstain, not zero alpha.",
        "",
        f"- decision: {summary['overall_decision']}",
        f"- decision_reason: {summary['decision_reason']}",
        f"- event_count: {summary['event_count']}",
        f"- allow_d3_charter_for: {summary['allow_d3_charter_for']}",
        f"- production_approval_claimed: {str(summary['production_approval_claimed']).lower()}",
    ]
    if source_admission:
        lines.extend(
            [
                "",
                "## Source Admission",
                f"- source_index_total_count: {source_admission['source_index_total_count']}",
                f"- indexed_file_count: {source_admission['indexed_file_count']}",
                f"- raw_file_found_share: {source_admission['raw_file_found_share']}",
                f"- accepted_timestamp_coverage_share: {source_admission['accepted_timestamp_coverage_share']}",
                f"- item_header_parse_coverage_share: {source_admission['item_header_parse_coverage_share']}",
                f"- market_coverage_share: {source_admission['market_coverage_share']}",
                f"- priority_market_coverage_share: {source_admission.get('priority_market_coverage_share', 0.0)}",
                f"- price_panel_count: {source_admission.get('price_panel_count', 0)}",
                f"- filtered_price_row_count: {source_admission.get('filtered_price_row_count', 0)}",
            ],
        )
    if item_header_audit is not None and not item_header_audit.empty:
        lines.extend(["", "## Item Parser Sample", item_header_audit.head(20).to_markdown(index=False)])
    lines.extend(
        [
            "",
            "## Non-Claims",
            "No subtype is promoted by this report. A D3 charter is allowed for at most one subtype only when the real D2 gates pass.",
        ],
    )
    return "\n".join(lines) + "\n"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _normalize_form_type(value: str) -> str:
    return str(value).strip().replace("_", "/").upper()


def _accession_from_relative_path(relative_path: str) -> str:
    for part in Path(relative_path).parts:
        if "_" in part:
            maybe_accession = part.rsplit("_", 1)[-1]
            if re.match(r"\d{10}-\d{2}-\d{6}$", maybe_accession):
                return maybe_accession
    return Path(relative_path).stem


def _symbol_from_relative_path(relative_path: Path) -> str:
    parts = list(relative_path.parts)
    if "documents" in parts:
        index = parts.index("documents")
        if index + 1 < len(parts):
            return parts[index + 1]
    return ""


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _next_regular_market_open(accepted: datetime) -> datetime:
    market_open = accepted.replace(hour=13, minute=30, second=0, microsecond=0)
    market_close = accepted.replace(hour=20, minute=0, second=0, microsecond=0)
    date = accepted.date()
    if accepted.weekday() >= 5 or accepted >= market_close:
        date = date + timedelta(days=1)
    elif accepted <= market_open:
        date = accepted.date()
    else:
        date = date + timedelta(days=1)
    while date.weekday() >= 5:
        date = date + timedelta(days=1)
    return datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc).replace(hour=13, minute=30)


def _first_priority(eligible: Iterable[str]) -> str:
    eligible_set = set(eligible)
    for subtype in PRIORITY_SUBTYPES:
        if subtype in eligible_set:
            return subtype
    return next(iter(eligible_set))


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator
