"""Local Form 4 extraction and D2 observability replay for insider disclosure."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_d2 import (
    DOWNSTREAM_FLAGS,
    REQUIRED_EVENT_REGISTRY_COLUMNS,
    run_insider_disclosure_d2,
)


REAL_SUMMARY_SCHEMA_VERSION = "insider_disclosure_d2_real_summary.v1"


@dataclass(frozen=True)
class RealForm4ObservabilityResult:
    """Artifacts and summary for D2-INSIDER-01R."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_form4_observability(
    source_dir: str | Path,
    output_dir: str | Path,
    market_data_path: str | Path | None = None,
    allow_network: bool = False,
    start_offset: int = 0,
    max_files: int | None = None,
) -> RealForm4ObservabilityResult:
    """Parse local Form 4 XML inputs and replay the no-formula D2 protocol."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    source_path = Path(source_dir)
    artifacts = _artifact_paths(output_path)

    if allow_network:
        raise ValueError("D2-INSIDER-01R does not support network fetches.")

    if not source_path.exists():
        summary = _unavailable_summary("unavailable_missing_source", missing_inputs=["source_dir"])
        _write_json(artifacts["missing_inputs_report"], {"missing_inputs": ["source_dir"], **summary})
        _write_json(artifacts["d2_observability_summary_real"], summary)
        artifacts["d2_insider_disclosure_observability_report_real"].write_text(
            _render_real_report(summary),
            encoding="utf-8",
        )
        return RealForm4ObservabilityResult(summary=summary, artifacts=artifacts)

    full_index = _load_source_index(source_path)
    source_index_total_count = int(len(full_index))
    index = _slice_source_index(full_index, start_offset=start_offset, max_files=max_files)
    registry, parse_coverage = _parse_form4_archive(source_path, index)
    source_manifest = {
        "schema_version": "form4_source_manifest.v1",
        "stage": "D2-INSIDER-01R",
        "source_type": "local_sec_archive",
        "source_layout": index.attrs.get("source_layout", "unknown"),
        "network_used": False,
        "source_dir": str(source_path),
        "source_index_total_count": source_index_total_count,
        "source_index_start_offset": int(start_offset),
        "source_index_max_files": int(max_files) if max_files is not None else None,
        "indexed_file_count": int(len(index)),
        "parsed_file_count": int(parse_coverage["parse_status"].eq("parsed").sum()) if not parse_coverage.empty else 0,
        "accepted_document_formats": ["ownership_xml", "sec_rendered_html_form4"],
        "post_2023_cutoff": "2023-04-01",
        "form_types": ["4", "4/A"],
    }
    market_joined, market_join_audit = _join_market_data(registry, Path(market_data_path) if market_data_path else None)

    base_replay_dir = output_path / "_base_replay"
    base_result = run_insider_disclosure_d2(output_dir=base_replay_dir, events=market_joined)

    _copy_base_artifact(base_result.artifacts["event_subset_counts"], artifacts["event_subset_counts_real"])
    _copy_base_artifact(base_result.artifacts["timestamp_audit"], artifacts["timestamp_audit_real"])
    _copy_base_artifact(base_result.artifacts["tradability_audit"], artifacts["tradability_audit_real"])
    _copy_base_artifact(base_result.artifacts["car_window_panel"], artifacts["car_window_panel_real"])
    _copy_base_artifact(base_result.artifacts["matched_control_panel"], artifacts["matched_control_panel_real"])
    _copy_base_artifact(base_result.artifacts["placebo_report"], artifacts["placebo_report_real"])

    market_joined.to_csv(artifacts["insider_event_registry_real"], index=False)
    market_joined.to_csv(artifacts["insider_event_market_join"], index=False)
    parse_coverage.to_csv(artifacts["form4_xml_parse_coverage"], index=False)
    _source_download_audit(index, source_path).to_csv(artifacts["form4_download_or_cache_audit"], index=False)
    _issuer_mapping_audit(market_joined).to_csv(artifacts["issuer_mapping_audit"], index=False)
    _timestamp_source_audit(market_joined).to_csv(artifacts["timestamp_source_audit"], index=False)
    market_join_audit.to_csv(artifacts["market_join_audit"], index=False)
    _write_json(artifacts["form4_source_manifest"], source_manifest)

    real_d2_gate = _build_real_d2_gate(market_joined)
    subset_decisions = dict(base_result.summary["subset_decisions"])
    overall_decision = str(base_result.summary["overall_decision"])
    allow_d3_charter_for = list(base_result.summary["allow_d3_charter_for"])
    open_buy_gate = real_d2_gate["open_market_buy"]
    if "open_market_insider_buying_post_2023" in allow_d3_charter_for and not open_buy_gate["passed"]:
        subset_decisions = dict(subset_decisions)
        subset_decisions["open_market_buy"] = {
            "decision": open_buy_gate["decision"],
            "reason": open_buy_gate["reason"],
            "not_alpha_evidence": True,
            "formula_score_written": False,
            "q1_entry_allowed": False,
            "q2_entry_allowed": False,
        }
        overall_decision = str(open_buy_gate["decision"])
        allow_d3_charter_for = []

    summary = {
        "schema_version": REAL_SUMMARY_SCHEMA_VERSION,
        "stage": "D2-INSIDER-01R",
        "source_type": "local_sec_archive",
        "real_data_status": "local_form4_replay_complete",
        "network_used": False,
        "source_index_total_count": source_index_total_count,
        "source_index_start_offset": int(start_offset),
        "source_index_max_files": int(max_files) if max_files is not None else None,
        "event_count": int(len(market_joined)),
        "event_month_count": int(market_joined["event_month"].nunique()) if "event_month" in market_joined else 0,
        "parsed_file_count": int(source_manifest["parsed_file_count"]),
        "market_join_covered_count": int(market_joined["coverage_state"].eq("covered").sum()),
        "market_join_no_view_count": int(market_joined["coverage_state"].eq("no_view").sum()),
        "overall_decision": overall_decision,
        "allow_d3_charter_for": allow_d3_charter_for,
        "subset_decisions": subset_decisions,
        "real_d2_gate": real_d2_gate,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        **DOWNSTREAM_FLAGS,
    }
    _write_json(artifacts["d2_observability_summary_real"], summary)
    artifacts["d2_insider_disclosure_observability_report_real"].write_text(
        _render_real_report(summary),
        encoding="utf-8",
    )
    return RealForm4ObservabilityResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "form4_source_manifest": output_path / "form4_source_manifest.json",
        "form4_download_or_cache_audit": output_path / "form4_download_or_cache_audit.csv",
        "form4_xml_parse_coverage": output_path / "form4_xml_parse_coverage.csv",
        "issuer_mapping_audit": output_path / "issuer_mapping_audit.csv",
        "timestamp_source_audit": output_path / "timestamp_source_audit.csv",
        "insider_event_registry_real": output_path / "insider_event_registry_real.csv",
        "insider_event_market_join": output_path / "insider_event_market_join.csv",
        "market_join_audit": output_path / "market_join_audit.csv",
        "event_subset_counts_real": output_path / "event_subset_counts_real.csv",
        "timestamp_audit_real": output_path / "timestamp_audit_real.csv",
        "tradability_audit_real": output_path / "tradability_audit_real.csv",
        "car_window_panel_real": output_path / "car_window_panel_real.csv",
        "matched_control_panel_real": output_path / "matched_control_panel_real.csv",
        "placebo_report_real": output_path / "placebo_report_real.json",
        "d2_observability_summary_real": output_path / "d2_observability_summary_real.json",
        "d2_insider_disclosure_observability_report_real": output_path
        / "d2_insider_disclosure_observability_report_real.md",
        "missing_inputs_report": output_path / "missing_inputs_report.json",
    }


def _unavailable_summary(status: str, missing_inputs: list[str]) -> dict[str, object]:
    return {
        "schema_version": REAL_SUMMARY_SCHEMA_VERSION,
        "stage": "D2-INSIDER-01R",
        "source_type": "local_sec_archive",
        "real_data_status": status,
        "network_used": False,
        "event_count": 0,
        "event_month_count": 0,
        "missing_inputs": missing_inputs,
        "overall_decision": "blocked_data_coverage",
        "allow_d3_charter_for": [],
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        **DOWNSTREAM_FLAGS,
    }


def _load_source_index(source_path: Path) -> pd.DataFrame:
    index_path = source_path / "form4_source_index.csv"
    if index_path.exists():
        frame = pd.read_csv(index_path).fillna("")
        frame.attrs["source_layout"] = "flat_form4_source_index"
        return frame
    request_specs_path = source_path / "request_specs.json"
    if request_specs_path.exists():
        request_specs = json.loads(request_specs_path.read_text(encoding="utf-8"))
        rows = []
        for row in request_specs:
            form_type = str(row.get("formType", "")).strip()
            if _normalize_form_type(form_type) not in {"4", "4/A"}:
                continue
            relative_path = str(row.get("relative_path", ""))
            if not relative_path:
                continue
            rows.append(
                {
                    "file": relative_path,
                    "accession_number": _accession_from_relative_path(relative_path),
                    "filing_accepted_ts": str(row.get("acceptedDate", "")),
                    "source_table_name": str(row.get("source_metadata_path", "sec_request_specs_json")),
                    "symbol": str(row.get("symbol", "")),
                    "issuer_cik": str(row.get("cik", "")),
                    "form_type": _normalize_form_type(form_type),
                    "source_url": str(row.get("url", "") or row.get("finalLink", "")),
                },
            )
        frame = pd.DataFrame(
            rows,
            columns=[
                "file",
                "accession_number",
                "filing_accepted_ts",
                "source_table_name",
                "symbol",
                "issuer_cik",
                "form_type",
                "source_url",
            ],
        ).fillna("")
        frame.attrs["source_layout"] = "sec_filing_archive_request_specs"
        return frame
    rows = [
        {
            "file": path.name,
            "accession_number": path.stem,
            "filing_accepted_ts": "",
            "source_table_name": "local_sec_archive_scan",
            "symbol": "",
            "issuer_cik": "",
            "form_type": "4",
            "source_url": "",
        }
        for path in sorted(source_path.glob("*.xml"))
    ]
    frame = pd.DataFrame(
        rows,
        columns=[
            "file",
            "accession_number",
            "filing_accepted_ts",
            "source_table_name",
            "symbol",
            "issuer_cik",
            "form_type",
            "source_url",
        ],
    )
    frame.attrs["source_layout"] = "flat_xml_scan"
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


def _parse_form4_archive(source_path: Path, index: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    for source_row in index.to_dict("records"):
        file_name = str(source_row.get("file", ""))
        xml_path = source_path / file_name
        accession = str(source_row.get("accession_number", xml_path.stem))
        accepted = str(source_row.get("filing_accepted_ts", ""))
        if not xml_path.exists():
            coverage_rows.append(
                {
                    "file": file_name,
                    "accession_number": accession,
                    "parse_status": "missing_file",
                    "parsed_transaction_count": 0,
                    "failure_reason": "missing_file",
                },
            )
            continue
        try:
            root = ET.parse(xml_path).getroot()
        except ET.ParseError as exc:
            html_transactions = _parse_form4_html_document(xml_path, source_row, accession, accepted)
            if html_transactions:
                rows.extend(html_transactions)
                coverage_rows.append(
                    {
                        "file": file_name,
                        "accession_number": accession,
                        "parse_status": "parsed",
                        "source_format": "sec_rendered_html_form4",
                        "parsed_transaction_count": len(html_transactions),
                        "failure_reason": "",
                    },
                )
            else:
                coverage_rows.append(
                    {
                        "file": file_name,
                        "accession_number": accession,
                        "parse_status": "parse_error",
                        "source_format": "unknown",
                        "parsed_transaction_count": 0,
                        "failure_reason": str(exc),
                    },
                )
            continue
        accepted = accepted or _find_text(root, ("acceptanceDatetime", "filingAcceptedDatetime"))
        if _local_name(root.tag).lower() == "html":
            transaction_rows = _parse_form4_html_document(xml_path, source_row, accession, accepted)
            source_format = "sec_rendered_html_form4"
        else:
            transaction_rows = list(_parse_form4_transactions(root, accession, accepted, source_row))
            source_format = "ownership_xml"
        rows.extend(transaction_rows)
        coverage_rows.append(
            {
                "file": file_name,
                "accession_number": accession,
                "parse_status": "parsed",
                "source_format": source_format,
                "parsed_transaction_count": len(transaction_rows),
                "failure_reason": "",
            },
        )
    registry = pd.DataFrame(rows, columns=REQUIRED_EVENT_REGISTRY_COLUMNS)
    return registry, pd.DataFrame(coverage_rows)


def _parse_form4_transactions(
    root: ET.Element,
    accession: str,
    accepted_timestamp: str,
    source_row: Mapping[str, object] | None = None,
) -> Iterable[dict[str, object]]:
    source_row = dict(source_row or {})
    issuer_cik = _find_text(root, ("issuerCik",)) or str(source_row.get("issuer_cik", ""))
    ticker = _find_text(root, ("issuerTradingSymbol",)) or str(source_row.get("symbol", ""))
    form_type = _find_text(root, ("documentType",)) or str(source_row.get("form_type", "")) or "4"
    owner_cik = _find_text(root, ("rptOwnerCik",))
    owner_name = _find_text(root, ("rptOwnerName",))
    is_director = _to_bool(_find_text(root, ("isDirector",)))
    is_officer = _to_bool(_find_text(root, ("isOfficer",)))
    officer_title = _find_text(root, ("officerTitle",))
    is_ten_pct = _to_bool(_find_text(root, ("isTenPercentOwner",)))
    role_bucket = _role_bucket(is_director, is_officer, officer_title, is_ten_pct)
    accepted_dt = _parse_timestamp(accepted_timestamp)
    visibility = accepted_dt.isoformat()
    tradable = _next_regular_market_open(accepted_dt).isoformat()

    for idx, transaction in enumerate(_find_elements(root, "nonDerivativeTransaction")):
        code = _find_text(transaction, ("transactionCode",))
        acquired_disposed = _find_nested_value(transaction, "transactionAcquiredDisposedCode")
        security_title = _canonical_security_title(
            _find_text(transaction, ("securityTitle", "value")) or _find_nested_value(transaction, "securityTitle"),
        )
        shares = _to_float(_find_nested_value(transaction, "transactionShares"))
        price = _to_float(_find_nested_value(transaction, "transactionPricePerShare"))
        plan_flag = _plan_flag(transaction)
        plan_date = _find_text(transaction, ("planAdoptionDate", "planAdoptionDt", "adoptionDate"))
        event_subset, coverage_state, no_view_reason, diagnostic_only = _classify_subset(
            transaction_code=code,
            acquired_disposed=acquired_disposed,
            security_title=security_title,
            plan_flag=plan_flag,
        )
        yield {
            "event_id": f"{accession}_{idx:03d}",
            "issuer_cik": issuer_cik,
            "ticker": ticker,
            "accession_number": accession,
            "form_type": form_type,
            "filing_accepted_ts": visibility,
            "visibility_timestamp": visibility,
            "tradable_timestamp": tradable,
            "reporting_owner_cik": owner_cik,
            "reporting_owner_name_hash": hashlib.sha256(owner_name.encode("utf-8")).hexdigest()[:16],
            "role_bucket": role_bucket,
            "is_director": is_director,
            "is_officer": is_officer,
            "officer_title_bucket": _title_bucket(officer_title),
            "is_10pct_owner": is_ten_pct,
            "transaction_code": code,
            "acquired_disposed": acquired_disposed,
            "transaction_date": _find_nested_value(transaction, "transactionDate"),
            "transaction_shares": shares,
            "transaction_price": price,
            "transaction_dollar_value": round(shares * price, 6),
            "security_title": security_title,
            "is_derivative": False,
            "ownership_direct_or_indirect": _find_nested_value(transaction, "directOrIndirectOwnership"),
            "post_transaction_holding": _to_float(_find_nested_value(transaction, "sharesOwnedFollowingTransaction")),
            "rule_10b5_1_flag": plan_flag,
            "plan_adoption_date": plan_date,
            "event_subset": event_subset,
            "event_cluster_id": f"{ticker}_{accepted_dt.date()}_{owner_cik}",
            "market_cap_at_event": 0.0,
            "adv_20d": 0.0,
            "spread_proxy": 0.0,
            "sector": "",
            "size_bucket": "",
            "liquidity_bucket": "",
            "coverage_state": coverage_state,
            "no_view_reason": no_view_reason,
            "diagnostic_only": diagnostic_only,
        }


def _parse_form4_html_document(
    xml_path: Path,
    source_row: Mapping[str, object],
    accession: str,
    accepted_timestamp: str,
) -> list[dict[str, object]]:
    html = xml_path.read_text(encoding="utf-8", errors="ignore")
    if "SEC Form 4" not in html and "SEC FORM" not in html and "Table I - Non-Derivative" not in html:
        return []
    accepted_dt = _parse_timestamp(accepted_timestamp)
    visibility = accepted_dt.isoformat()
    tradable = _next_regular_market_open(accepted_dt).isoformat()
    ticker = str(source_row.get("symbol", ""))
    issuer_cik = str(source_row.get("issuer_cik", ""))
    form_type = str(source_row.get("form_type", "")) or "4"
    owner_name = _html_owner_name(html) or accession
    owner_cik = _html_owner_cik(html)
    is_director, is_officer, officer_title, is_ten_pct = _html_role_flags(html)
    role_bucket = _role_bucket(is_director, is_officer, officer_title, is_ten_pct)
    form_plan_flag = _html_form_plan_flag(html)
    rows: list[dict[str, object]] = []
    for idx, values in enumerate(_html_table_i_rows(html)):
        security_title = _canonical_security_title(_clean_html_cell(values[0]))
        transaction_date = _normalize_html_date(_clean_html_cell(values[1]))
        code = _clean_html_cell(values[3])
        shares = _to_float(values[5])
        acquired_disposed = _clean_html_cell(values[6])
        price = _to_float(values[7])
        post_holding = _to_float(values[8])
        ownership = _clean_html_cell(values[9])
        if not code:
            continue
        plan_flag = form_plan_flag if code == "S" else ""
        event_subset, coverage_state, no_view_reason, diagnostic_only = _classify_subset(
            transaction_code=code,
            acquired_disposed=acquired_disposed,
            security_title=security_title,
            plan_flag=plan_flag,
        )
        rows.append(
            {
                "event_id": f"{accession}_{idx:03d}",
                "issuer_cik": issuer_cik,
                "ticker": ticker,
                "accession_number": accession,
                "form_type": form_type,
                "filing_accepted_ts": visibility,
                "visibility_timestamp": visibility,
                "tradable_timestamp": tradable,
                "reporting_owner_cik": owner_cik,
                "reporting_owner_name_hash": hashlib.sha256(owner_name.encode("utf-8")).hexdigest()[:16],
                "role_bucket": role_bucket,
                "is_director": is_director,
                "is_officer": is_officer,
                "officer_title_bucket": _title_bucket(officer_title),
                "is_10pct_owner": is_ten_pct,
                "transaction_code": code,
                "acquired_disposed": acquired_disposed,
                "transaction_date": transaction_date,
                "transaction_shares": shares,
                "transaction_price": price,
                "transaction_dollar_value": round(shares * price, 6),
                "security_title": security_title,
                "is_derivative": False,
                "ownership_direct_or_indirect": ownership,
                "post_transaction_holding": post_holding,
                "rule_10b5_1_flag": plan_flag,
                "plan_adoption_date": "",
                "event_subset": event_subset,
                "event_cluster_id": f"{ticker}_{accepted_dt.date()}_{owner_cik or owner_name}",
                "market_cap_at_event": 0.0,
                "adv_20d": 0.0,
                "spread_proxy": 0.0,
                "sector": "",
                "size_bucket": "",
                "liquidity_bucket": "",
                "coverage_state": coverage_state,
                "no_view_reason": no_view_reason,
                "diagnostic_only": diagnostic_only,
            },
        )
    return rows


def _join_market_data(events: pd.DataFrame, market_data_path: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = events.copy()
    if joined.empty:
        return joined, pd.DataFrame(
            [{"market_data_path": str(market_data_path or ""), "covered_count": 0, "no_view_count": 0, "status": "empty"}],
        )
    if market_data_path is None or not market_data_path.exists():
        joined["coverage_state"] = "no_view"
        joined["no_view_reason"] = "missing_market_join"
        audit = pd.DataFrame(
            [
                {
                    "market_data_path": str(market_data_path or ""),
                    "covered_count": 0,
                    "no_view_count": int(len(joined)),
                    "status": "missing_market_data",
                },
            ],
        )
        joined["event_month"] = pd.to_datetime(joined["visibility_timestamp"], errors="coerce").dt.strftime("%Y-%m")
        return joined, audit
    market = pd.read_csv(market_data_path).fillna("")
    if "ticker" in market.columns and "date" in market.columns:
        return _join_daily_market_data(joined, market, market_data_path)
    market_by_ticker = market.set_index("ticker").to_dict("index") if "ticker" in market.columns else {}
    for index, row in joined.iterrows():
        payload = market_by_ticker.get(row["ticker"])
        if payload is None:
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_market_join"
            continue
        for column in ("market_cap_at_event", "adv_20d", "spread_proxy", "sector", "size_bucket", "liquidity_bucket"):
            if column in payload:
                joined.loc[index, column] = payload[column]
        if joined.loc[index, "coverage_state"] != "no_view":
            joined.loc[index, "coverage_state"] = "covered"
            joined.loc[index, "no_view_reason"] = ""
    joined["event_month"] = pd.to_datetime(joined["visibility_timestamp"], errors="coerce").dt.strftime("%Y-%m")
    audit = pd.DataFrame(
        [
            {
                "market_data_path": str(market_data_path),
                "covered_count": int(joined["coverage_state"].eq("covered").sum()),
                "no_view_count": int(joined["coverage_state"].eq("no_view").sum()),
                "status": "joined",
            },
        ],
    )
    return joined, audit


def _join_daily_market_data(events: pd.DataFrame, market: pd.DataFrame, market_data_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = events.copy()
    market = market.copy()
    market["_ticker_key"] = market["ticker"].astype(str).str.upper()
    market["_market_date"] = pd.to_datetime(market["date"], errors="coerce").dt.normalize()
    market = market[market["_market_date"].notna()].sort_values(["_ticker_key", "_market_date"]).reset_index(drop=True)
    market = _prepare_daily_market_controls(market)
    by_ticker = {
        ticker: frame.reset_index(drop=True)
        for ticker, frame in market.groupby("_ticker_key", sort=False)
    }
    market_cap_values = pd.to_numeric(market["_market_cap_at_event"], errors="coerce")
    adv_values = pd.to_numeric(market["_adv_20d"], errors="coerce")
    market_cap_quantiles = market_cap_values.dropna().quantile([0.33, 0.66]).tolist()
    adv_quantiles = adv_values.dropna().quantile([0.33, 0.66]).tolist()

    for index, row in joined.iterrows():
        ticker_key = str(row["ticker"]).upper()
        event_date = _event_market_date(row.get("visibility_timestamp", ""))
        market_rows = by_ticker.get(ticker_key)
        if market_rows is None or event_date is None:
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_market_join"
            continue
        position = market_rows["_market_date"].searchsorted(event_date, side="right") - 1
        if position < 0:
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_market_join"
            continue
        payload = market_rows.iloc[int(position)]
        market_cap = _safe_float(payload.get("_market_cap_at_event", 0.0))
        adv_20d = _safe_float(payload.get("_adv_20d", 0.0))
        if market_cap <= 0 or adv_20d <= 0:
            joined.loc[index, "coverage_state"] = "no_view"
            joined.loc[index, "no_view_reason"] = "missing_price_volume_controls"
            continue
        joined.loc[index, "market_cap_at_event"] = market_cap
        joined.loc[index, "adv_20d"] = adv_20d
        joined.loc[index, "spread_proxy"] = _safe_float(payload.get("_spread_proxy", 0.0))
        joined.loc[index, "sector"] = str(payload.get("sector", ""))
        joined.loc[index, "size_bucket"] = _quantile_bucket(market_cap, market_cap_quantiles, ("small", "mid", "large"))
        joined.loc[index, "liquidity_bucket"] = _quantile_bucket(adv_20d, adv_quantiles, ("low", "medium", "high"))
        if joined.loc[index, "coverage_state"] != "no_view":
            joined.loc[index, "coverage_state"] = "covered"
            joined.loc[index, "no_view_reason"] = ""
    joined["event_month"] = pd.to_datetime(joined["visibility_timestamp"], errors="coerce").dt.strftime("%Y-%m")
    audit = pd.DataFrame(
        [
            {
                "market_data_path": str(market_data_path),
                "covered_count": int(joined["coverage_state"].eq("covered").sum()),
                "no_view_count": int(joined["coverage_state"].eq("no_view").sum()),
                "status": "joined_daily_price_volume",
                "market_cap_source": _first_present(market.columns, ("market_cap", "dlycap", "market_cap_at_event")),
                "adv_source": "dollar_volume_or_price_volume_rolling_20d",
                "spread_source": _first_present(market.columns, ("spread_proxy", "bid_ask_spread", "spread")),
            },
        ],
    )
    return joined, audit


def _prepare_daily_market_controls(market: pd.DataFrame) -> pd.DataFrame:
    market_cap_col = _first_present(market.columns, ("market_cap", "dlycap", "market_cap_at_event"))
    dollar_volume_col = _first_present(market.columns, ("dollar_volume", "dlyprcvol", "dollar_volume_20d"))
    price_col = _first_present(market.columns, ("adjusted_close", "raw_close", "close", "price"))
    volume_col = _first_present(market.columns, ("volume", "vol"))
    spread_col = _first_present(market.columns, ("spread_proxy", "bid_ask_spread", "spread"))
    if market_cap_col:
        market["_market_cap_at_event"] = pd.to_numeric(market[market_cap_col], errors="coerce")
    else:
        market["_market_cap_at_event"] = pd.NA
    if dollar_volume_col:
        market["_dollar_volume"] = pd.to_numeric(market[dollar_volume_col], errors="coerce")
    elif price_col and volume_col:
        market["_dollar_volume"] = (
            pd.to_numeric(market[price_col], errors="coerce") * pd.to_numeric(market[volume_col], errors="coerce")
        )
    else:
        market["_dollar_volume"] = pd.NA
    market["_adv_20d"] = market.groupby("_ticker_key")["_dollar_volume"].transform(
        lambda values: values.rolling(20, min_periods=1).mean(),
    )
    if spread_col:
        market["_spread_proxy"] = pd.to_numeric(market[spread_col], errors="coerce").fillna(0.0)
    else:
        market["_spread_proxy"] = 0.0
    if "sector" not in market.columns:
        market["sector"] = ""
    return market


def _event_market_date(value: object) -> pd.Timestamp | None:
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert(None).normalize()


def _first_present(columns: Iterable[str], candidates: tuple[str, ...]) -> str:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return ""


def _safe_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _quantile_bucket(value: float, quantiles: list[float], labels: tuple[str, str, str]) -> str:
    if len(quantiles) < 2 or any(pd.isna(item) for item in quantiles):
        return ""
    if value <= quantiles[0]:
        return labels[0]
    if value <= quantiles[1]:
        return labels[1]
    return labels[2]


def _build_real_d2_gate(events: pd.DataFrame) -> dict[str, dict[str, object]]:
    open_buy = events[events["event_subset"] == "open_market_buy"].copy()
    covered = open_buy[open_buy["coverage_state"] == "covered"]
    event_count = int(len(open_buy))
    covered_count = int(len(covered))
    coverage_share = round(covered_count / event_count, 6) if event_count else 0.0
    covered_event_month_count = int(covered["event_month"].nunique()) if "event_month" in covered else 0
    covered_cluster_count = int(covered["event_cluster_id"].nunique()) if "event_cluster_id" in covered else 0
    minimum_event_count = 300
    minimum_event_month_count = 24
    minimum_cluster_count = 50
    minimum_coverage_share = 0.8
    passed = (
        covered_count >= minimum_event_count
        and covered_event_month_count >= minimum_event_month_count
        and covered_cluster_count >= minimum_cluster_count
        and coverage_share >= minimum_coverage_share
    )
    if passed:
        decision = "coverage_passed"
        reason = "open-market buy subset satisfies real D2 market coverage gate"
    elif event_count < minimum_event_count:
        decision = "hold_insufficient_sample"
        reason = "open-market buy subset does not satisfy minimum sample contract"
    else:
        decision = "blocked_data_coverage"
        reason = "open-market buy subset does not satisfy real D2 market coverage gate"
    return {
        "open_market_buy": {
            "passed": passed,
            "decision": decision,
            "reason": reason,
            "event_count": event_count,
            "covered_count": covered_count,
            "coverage_share": coverage_share,
            "covered_event_month_count": covered_event_month_count,
            "covered_cluster_count": covered_cluster_count,
            "minimum_event_count": minimum_event_count,
            "minimum_event_month_count": minimum_event_month_count,
            "minimum_cluster_count": minimum_cluster_count,
            "minimum_coverage_share": minimum_coverage_share,
            "not_alpha_evidence": True,
        },
    }


def _source_download_audit(index: pd.DataFrame, source_path: Path) -> pd.DataFrame:
    rows = []
    for row in index.to_dict("records"):
        path = source_path / str(row.get("file", ""))
        rows.append(
            {
                "file": row.get("file", ""),
                "accession_number": row.get("accession_number", ""),
                "cache_exists": path.exists(),
                "network_used": False,
            },
        )
    return pd.DataFrame(rows)


def _normalize_form_type(value: str) -> str:
    normalized = str(value).strip().replace("_", "/").upper()
    return normalized


def _accession_from_relative_path(relative_path: str) -> str:
    for part in Path(relative_path).parts:
        if "_" in part:
            maybe_accession = part.rsplit("_", 1)[-1]
            if re.match(r"\d{10}-\d{2}-\d{6}$", maybe_accession):
                return maybe_accession
    return Path(relative_path).stem


def _issuer_mapping_audit(events: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_count": int(len(events)),
                "ticker_present_count": int(events["ticker"].astype(str).ne("").sum()) if not events.empty else 0,
                "issuer_cik_present_count": int(events["issuer_cik"].astype(str).ne("").sum()) if not events.empty else 0,
                "status": "pass" if not events.empty else "empty",
            },
        ],
    )


def _timestamp_source_audit(events: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_count": int(len(events)),
                "accepted_timestamp_source": "local_source_index_or_xml",
                "missing_accepted_timestamp_count": int(events["filing_accepted_ts"].astype(str).eq("").sum())
                if not events.empty
                else 0,
                "tradable_timestamp_policy": "next_regular_market_open_after_acceptance",
                "status": "pass" if not events.empty else "empty",
            },
        ],
    )


def _find_elements(root: ET.Element, local_name: str) -> list[ET.Element]:
    return [element for element in root.iter() if _local_name(element.tag) == local_name]


def _html_table_i_rows(html: str) -> Iterable[list[object]]:
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    rows: list[list[object]] = []
    for table in tables:
        table_text = f"{table.columns} {table.head(1).to_string() if not table.empty else ''}"
        if "Table I - Non-Derivative" not in table_text:
            continue
        if "Derivative Securities Acquired" in table_text and "Non-Derivative Securities" not in table_text:
            continue
        for _, row in table.iterrows():
            values = list(row.values)
            if len(values) < 10:
                continue
            code = _clean_html_cell(values[3])
            security_title = _clean_html_cell(values[0])
            if code in {"P", "S", "A", "M", "F"} and security_title:
                rows.append(values)
    return rows


def _html_form_plan_flag(html: str) -> object:
    for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL):
        row_html = row_match.group(1)
        if "10b5-1" not in row_html and "10b5" not in row_html:
            continue
        cleaned = _strip_html(row_html)
        return bool(re.search(r"(^|\s)X(\s|$)", cleaned))
    return ""


def _html_owner_name(html: str) -> str:
    match = re.search(
        r"Name and Address of Reporting Person\*?\s*([^<\\n]+)",
        _strip_html(html),
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else ""


def _html_owner_cik(html: str) -> str:
    match = re.search(r"CIK=(\d+)", html, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _html_role_flags(html: str) -> tuple[bool, bool, str, bool]:
    text = _strip_html(html)
    is_director = bool(re.search(r"\bX\s+Director\b", text, flags=re.IGNORECASE))
    is_officer = bool(re.search(r"\bX\s+Officer\b", text, flags=re.IGNORECASE))
    is_ten_pct = bool(re.search(r"\bX\s+10%\s+Owner\b", text, flags=re.IGNORECASE))
    title = ""
    match = re.search(
        r"Officer\s*\(give title below\)\s+(.+?)(?:\s+Other\s+\(specify below\)|\s+3\. Date|\s+4\. If Amendment|$)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        title = match.group(1).strip()[:80]
    return is_director, is_officer, title, is_ten_pct


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", text).strip()


def _clean_html_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_html_date(value: str) -> str:
    value = _clean_html_cell(value)
    if not value:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return value


def _canonical_security_title(value: str) -> str:
    return "Common Stock" if "common" in str(value).lower() else str(value)


def _find_text(root: ET.Element, names: tuple[str, ...]) -> str:
    for element in root.iter():
        if _local_name(element.tag) in names and element.text:
            return element.text.strip()
    return ""


def _find_nested_value(root: ET.Element, parent_name: str) -> str:
    for element in root.iter():
        if _local_name(element.tag) == parent_name:
            for child in element.iter():
                if _local_name(child.tag) == "value" and child.text:
                    return child.text.strip()
            if element.text:
                return element.text.strip()
    return ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_timestamp(value: str) -> datetime:
    if not value:
        return datetime(1970, 1, 1, 13, 30, tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(normalized)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _next_regular_market_open(timestamp: datetime) -> datetime:
    next_day = timestamp + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day.replace(hour=13, minute=30, second=0, microsecond=0)


def _to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_float(value: str) -> float:
    cleaned = re.sub(r"\([^)]*\)", "", str(value))
    cleaned = cleaned.replace(",", "").replace("$", "").strip()
    if cleaned.lower() in {"", "nan", "none"}:
        return 0.0
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return 0.0


def _plan_flag(transaction: ET.Element) -> object:
    for name in ("rule10b5-1", "rule10b51", "isRule10b5-1", "isRule10b51"):
        value = _find_text(transaction, (name,))
        if value:
            return _to_bool(value)
    return ""


def _classify_subset(
    transaction_code: str,
    acquired_disposed: str,
    security_title: str,
    plan_flag: object,
) -> tuple[str, str, str, bool]:
    common = "common" in security_title.lower()
    if transaction_code == "P" and acquired_disposed == "A" and common:
        return "open_market_buy", "covered", "", False
    if transaction_code == "S" and acquired_disposed == "D" and common:
        if plan_flag is True:
            return "planned_sell", "covered", "", False
        if plan_flag is False:
            return "discretionary_sell", "covered", "", False
        return "unknown_no_view", "no_view", "unknown_post_2023_plan_flag", True
    if transaction_code in {"A", "M", "F"}:
        return "compensation_control", "covered", "", True
    return "unknown_no_view", "no_view", "unsupported_transaction_code", True


def _role_bucket(is_director: bool, is_officer: bool, officer_title: str, is_ten_pct: bool) -> str:
    title = officer_title.lower()
    if "chief executive" in title or title == "ceo":
        return "ceo"
    if "chief financial" in title or title == "cfo":
        return "cfo"
    if is_officer:
        return "other_officer"
    if is_director:
        return "director"
    if is_ten_pct:
        return "ten_pct_owner"
    return "other"


def _title_bucket(officer_title: str) -> str:
    title = officer_title.lower()
    if "chief executive" in title:
        return "CEO"
    if "chief financial" in title:
        return "CFO"
    if "chief operating" in title:
        return "COO"
    return officer_title[:40]


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _copy_base_artifact(source: Path, target: Path) -> None:
    shutil.copyfile(source, target)


def _render_real_report(summary: Mapping[str, object]) -> str:
    lines = [
        "# D2 Insider Disclosure Real Form 4 Extraction Replay",
        "",
        "not alpha evidence",
        "real form 4 extraction replay",
        "no formula",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "optimizer and portfolio paths: blocked",
        "paper workflow: blocked",
        "broker/order workflow: blocked",
        "production approval: false",
        "",
        "## Summary",
        "",
        f"- status: `{summary['real_data_status']}`",
        f"- event count: {summary['event_count']}",
        f"- event months: {summary['event_month_count']}",
        f"- overall D2 decision: `{summary['overall_decision']}`",
        f"- allowed D3 charters: {', '.join(summary['allow_d3_charter_for']) or 'none'}",
        "",
        "## Boundary",
        "",
        "This replay only parses local Form 4-style inputs and reruns the no-formula D2 observability protocol.",
        "It does not write a MeasurementSpec, signal score, expected-return panel, optimizer input, Q1 handoff, Q2 handoff, or Alpha Registry decision.",
        "Missing coverage remains no-view / abstain and is not encoded as zero.",
        "",
    ]
    return "\n".join(lines)
