"""D2-INSIDER-02B source locator and 10b5-1 parser repair gate."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_plan_flag_audit import (
    ADOPTION_DATE_PATTERN,
    _checkbox_status,
    _footnote_context,
)


SUMMARY_SCHEMA_VERSION = "insider_disclosure_plan_flag_repair_summary.v1"
STAGE = "D2-INSIDER-02B"
CANDIDATE_ID = "planned_vs_discretionary_sell_contrast_post_2023"

DOWNSTREAM_FLAGS = {
    "formula_score_written": False,
    "measurement_spec_written": False,
    "q1_entry_allowed": False,
    "q2_entry_allowed": False,
    "optimizer_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "expected_return_panel_written": False,
    "production_approval_claimed": False,
}


@dataclass(frozen=True)
class PlanFlagRepairResult:
    """Artifacts and summary for D2-INSIDER-02B."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_plan_flag_source_locator_parser_repair(
    event_registry_path: str | Path,
    parse_coverage_path: str | Path,
    source_roots: Iterable[str | Path],
    output_dir: str | Path,
    minimum_planned_sell_events: int = 300,
    minimum_planned_sell_month_count: int = 24,
    minimum_known_plan_flag_share: float = 0.60,
    minimum_raw_file_found_share: float = 0.80,
    minimum_structured_or_high_confidence_source_share: float = 0.80,
) -> PlanFlagRepairResult:
    """Run the last narrow locator/parser repair gate before switching away."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    registry = _load_registry(Path(event_registry_path))
    parse_coverage = pd.read_csv(parse_coverage_path, low_memory=False).fillna("")
    source_index = _build_source_index(parse_coverage, [Path(root) for root in source_roots])
    s_code = _post_2023_s_code_events(registry)

    locator_report = _build_locator_report(s_code, parse_coverage, source_index)
    parser_report = _build_parser_report(locator_report)
    before_after = _build_before_after(s_code, parser_report)
    counts = _build_counts(s_code, locator_report, before_after)
    summary = _build_summary(
        counts=counts,
        minimum_planned_sell_events=minimum_planned_sell_events,
        minimum_planned_sell_month_count=minimum_planned_sell_month_count,
        minimum_known_plan_flag_share=minimum_known_plan_flag_share,
        minimum_raw_file_found_share=minimum_raw_file_found_share,
        minimum_structured_or_high_confidence_source_share=minimum_structured_or_high_confidence_source_share,
    )

    locator_report.to_csv(artifacts["accession_path_resolution_audit"], index=False)
    locator_report[locator_report["raw_file_found"].eq(False)].to_csv(artifacts["missing_raw_file_report"], index=False)
    _locator_coverage_frame(counts).to_csv(artifacts["raw_locator_coverage_report"], index=False)
    parser_report.to_csv(artifacts["structured_10b5_flag_coverage"], index=False)
    before_after.to_csv(artifacts["plan_flag_parser_before_after"], index=False)
    _write_json(artifacts["explicit_true_false_unknown_counts"], counts)
    _write_json(artifacts["repair_summary"], summary)
    artifacts["repair_report"].write_text(_render_report(summary, counts), encoding="utf-8")

    return PlanFlagRepairResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "raw_locator_coverage_report": output_path / "raw_locator_coverage_report.csv",
        "missing_raw_file_report": output_path / "missing_raw_file_report.csv",
        "accession_path_resolution_audit": output_path / "accession_path_resolution_audit.csv",
        "structured_10b5_flag_coverage": output_path / "structured_10b5_flag_coverage.csv",
        "plan_flag_parser_before_after": output_path / "plan_flag_parser_before_after.csv",
        "explicit_true_false_unknown_counts": output_path / "explicit_true_false_unknown_counts.json",
        "repair_summary": output_path / "repair_summary.json",
        "repair_report": output_path / "repair_report.md",
    }


def _load_registry(path: Path) -> pd.DataFrame:
    registry = pd.read_csv(path, low_memory=False).fillna("")
    required = {"event_id", "accession_number", "transaction_code", "rule_10b5_1_flag", "filing_accepted_ts"}
    missing = sorted(required - set(registry.columns))
    if missing:
        raise ValueError(f"event registry missing required columns: {', '.join(missing)}")
    return registry


def _post_2023_s_code_events(registry: pd.DataFrame) -> pd.DataFrame:
    frame = registry.copy()
    frame["_filing_timestamp"] = pd.to_datetime(frame["filing_accepted_ts"], errors="coerce", utc=True)
    frame["_filing_month"] = frame["_filing_timestamp"].dt.strftime("%Y-%m")
    frame["_registry_plan_flag"] = frame["rule_10b5_1_flag"].map(_normalize_plan_flag)
    return frame[
        frame["transaction_code"].astype(str).str.upper().eq("S")
        & (frame["_filing_timestamp"] >= pd.Timestamp("2023-04-01", tz="UTC"))
    ].reset_index(drop=True)


def _normalize_plan_flag(value: object) -> str:
    lower = str(value).strip().lower()
    if lower in {"true", "t", "1", "yes", "y"}:
        return "explicit_true"
    if lower in {"false", "f", "0", "no", "n"}:
        return "explicit_false"
    return "missing"


def _build_source_index(parse_coverage: pd.DataFrame, source_roots: list[Path]) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for row in parse_coverage.itertuples(index=False):
        accession = str(getattr(row, "accession_number", "")).strip()
        rel_file = str(getattr(row, "file", "")).strip()
        if not accession or not rel_file:
            continue
        for root in source_roots:
            candidate = root / rel_file
            if candidate.exists():
                index.setdefault(accession, candidate)
                break
    accession_pattern = re.compile(r"\d{10}-\d{2}-\d{6}")
    for root in source_roots:
        if not root.exists():
            continue
        for path in root.rglob("*.xml"):
            match = accession_pattern.search(str(path))
            if match:
                index.setdefault(match.group(0), path)
    return index


def _build_locator_report(s_code: pd.DataFrame, parse_coverage: pd.DataFrame, source_index: dict[str, Path]) -> pd.DataFrame:
    parse_by_accession = parse_coverage.drop_duplicates("accession_number").set_index("accession_number")
    rows = []
    for accession, group in s_code.groupby("accession_number", dropna=False):
        accession_key = str(accession)
        parse_row = parse_by_accession.loc[accession_key] if accession_key in parse_by_accession.index else None
        raw_path = source_index.get(accession_key)
        rows.append(
            {
                "accession_number": accession_key,
                "ticker": _first(group, "ticker"),
                "s_code_event_count": int(len(group)),
                "registry_true_event_count": int(group["_registry_plan_flag"].eq("explicit_true").sum()),
                "registry_false_event_count": int(group["_registry_plan_flag"].eq("explicit_false").sum()),
                "registry_unknown_event_count": int(group["_registry_plan_flag"].eq("missing").sum()),
                "expected_relative_path": "" if parse_row is None else str(parse_row.get("file", "")),
                "source_format": "" if parse_row is None else str(parse_row.get("source_format", "")),
                "raw_file_found": raw_path is not None,
                "resolved_raw_path": "" if raw_path is None else str(raw_path),
                "resolution_status": "resolved" if raw_path is not None else "missing_raw_file",
            },
        )
    return pd.DataFrame(rows).sort_values(["raw_file_found", "accession_number"], ascending=[False, True]).reset_index(drop=True)


def _build_parser_report(locator_report: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in locator_report.itertuples(index=False):
        raw_path = Path(str(row.resolved_raw_path)) if bool(row.raw_file_found) else None
        if raw_path is None or not raw_path.exists():
            rows.append(_parser_row(row, "missing", False, False, "missing_source"))
            continue
        raw_text = raw_path.read_text(encoding="utf-8", errors="ignore")
        structured = _checkbox_status(raw_text)
        context = _footnote_context(raw_text)
        footnote = bool(context)
        adoption = bool(re.search(ADOPTION_DATE_PATTERN, context, flags=re.IGNORECASE))
        high_confidence = structured in {"true", "false"} or (footnote and adoption)
        rows.append(_parser_row(row, structured, footnote, adoption, "structured" if structured in {"true", "false"} else "footnote_high_confidence" if high_confidence else "missing"))
    return pd.DataFrame(rows)


def _parser_row(row: object, structured: str, footnote: bool, adoption: bool, confidence: str) -> dict[str, object]:
    return {
        "accession_number": row.accession_number,
        "ticker": row.ticker,
        "raw_file_found": bool(row.raw_file_found),
        "resolved_raw_path": row.resolved_raw_path,
        "parser_after_flag": _parser_flag(structured, footnote, adoption),
        "structured_10b5_flag": structured,
        "footnote_10b5_candidate": footnote,
        "footnote_adoption_date_candidate": adoption,
        "source_confidence": confidence,
        "structured_or_high_confidence_source": confidence in {"structured", "footnote_high_confidence"},
    }


def _parser_flag(structured: str, footnote: bool, adoption: bool) -> str:
    if structured == "true":
        return "explicit_true"
    if structured == "false":
        return "explicit_false"
    if footnote and adoption:
        return "explicit_true"
    return "missing"


def _build_before_after(s_code: pd.DataFrame, parser_report: pd.DataFrame) -> pd.DataFrame:
    parser_by_accession = parser_report.set_index("accession_number")
    rows = []
    for _, event in s_code.iterrows():
        accession = str(event["accession_number"])
        parser_row = parser_by_accession.loc[accession] if accession in parser_by_accession.index else None
        parser_flag = "missing" if parser_row is None else str(parser_row["parser_after_flag"])
        repaired_subset = _repaired_subset(parser_flag)
        rows.append(
            {
                "event_id": event["event_id"],
                "accession_number": accession,
                "ticker": event.get("ticker", ""),
                "registry_before_flag": event["_registry_plan_flag"],
                "parser_after_flag": parser_flag,
                "repaired_event_subset": repaired_subset,
                "raw_file_found": False if parser_row is None else bool(parser_row["raw_file_found"]),
                "structured_or_high_confidence_source": False if parser_row is None else bool(parser_row["structured_or_high_confidence_source"]),
                "missing_plan_flag_not_discretionary": repaired_subset == "unknown_plan_flag",
                "event_month": event["_filing_month"],
            },
        )
    return pd.DataFrame(rows)


def _repaired_subset(parser_flag: str) -> str:
    if parser_flag == "explicit_true":
        return "planned_sell"
    if parser_flag == "explicit_false":
        return "discretionary_sell"
    return "unknown_plan_flag"


def _build_counts(s_code: pd.DataFrame, locator_report: pd.DataFrame, before_after: pd.DataFrame) -> dict[str, object]:
    total_accessions = int(len(locator_report))
    raw_found = int(locator_report["raw_file_found"].eq(True).sum()) if total_accessions else 0
    total_events = int(len(before_after))
    after_counts = {
        "explicit_true": int(before_after["parser_after_flag"].eq("explicit_true").sum()) if total_events else 0,
        "explicit_false": int(before_after["parser_after_flag"].eq("explicit_false").sum()) if total_events else 0,
        "missing": int(before_after["parser_after_flag"].eq("missing").sum()) if total_events else 0,
    }
    known_after = after_counts["explicit_true"] + after_counts["explicit_false"]
    high_conf = int(before_after["structured_or_high_confidence_source"].eq(True).sum()) if total_events else 0
    planned = int(before_after["repaired_event_subset"].eq("planned_sell").sum()) if total_events else 0
    planned_months = int(before_after[before_after["repaired_event_subset"].eq("planned_sell")]["event_month"].nunique()) if total_events else 0
    return {
        "s_code_event_count": total_events,
        "s_code_accession_count": total_accessions,
        "raw_file_found_count": raw_found,
        "raw_file_found_share": round(raw_found / total_accessions, 6) if total_accessions else 0.0,
        "before_registry_event_counts": {
            "explicit_true": int(s_code["_registry_plan_flag"].eq("explicit_true").sum()) if total_events else 0,
            "explicit_false": int(s_code["_registry_plan_flag"].eq("explicit_false").sum()) if total_events else 0,
            "missing": int(s_code["_registry_plan_flag"].eq("missing").sum()) if total_events else 0,
        },
        "after_parser_event_counts": after_counts,
        "known_plan_flag_share": round(known_after / total_events, 6) if total_events else 0.0,
        "structured_or_high_confidence_source_share": round(high_conf / total_events, 6) if total_events else 0.0,
        "repaired_planned_sell_event_count": planned,
        "repaired_planned_sell_month_count": planned_months,
        "repaired_discretionary_sell_event_count": int(before_after["repaired_event_subset"].eq("discretionary_sell").sum()) if total_events else 0,
        "repaired_unknown_plan_flag_event_count": int(before_after["repaired_event_subset"].eq("unknown_plan_flag").sum()) if total_events else 0,
        "missing_plan_flag_not_discretionary": True,
    }


def _build_summary(
    counts: dict[str, object],
    minimum_planned_sell_events: int,
    minimum_planned_sell_month_count: int,
    minimum_known_plan_flag_share: float,
    minimum_raw_file_found_share: float,
    minimum_structured_or_high_confidence_source_share: float,
) -> dict[str, object]:
    raw_share = float(counts["raw_file_found_share"])
    known_share = float(counts["known_plan_flag_share"])
    confidence_share = float(counts["structured_or_high_confidence_source_share"])
    planned_events = int(counts["repaired_planned_sell_event_count"])
    planned_months = int(counts["repaired_planned_sell_month_count"])
    if raw_share < minimum_raw_file_found_share:
        decision = "source_locator_repair_failed_switch_to_8k"
        reason = "raw Form 4 source locator coverage is below threshold"
        next_action = "switch_to_D2_8K_01_subtype_underreaction"
    elif (
        planned_events < minimum_planned_sell_events
        or planned_months < minimum_planned_sell_month_count
        or known_share < minimum_known_plan_flag_share
        or confidence_share < minimum_structured_or_high_confidence_source_share
    ):
        decision = "plan_flag_parser_repair_failed_switch_to_8k"
        reason = "structured or high-confidence plan-flag coverage is below threshold"
        next_action = "switch_to_D2_8K_01_subtype_underreaction"
    else:
        decision = "plan_flag_repair_gate_passed_rerun_d2_allowed"
        reason = "raw source and plan-flag coverage gates pass"
        next_action = "rerun_D2_INSIDER_02_sell_contrast"
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "overall_decision": decision,
        "decision_reason": reason,
        "next_action": next_action,
        "d2_insider_02_status": "blocked_plan_flag_coverage" if "switch_to_8k" in decision else "rerun_allowed",
        "allow_d3_charter_for": [],
        "d2_sell_contrast_rerun": decision == "plan_flag_repair_gate_passed_rerun_d2_allowed",
        "raw_file_found_share": raw_share,
        "known_plan_flag_share": known_share,
        "structured_or_high_confidence_source_share": confidence_share,
        "repaired_planned_sell_event_count": planned_events,
        "repaired_planned_sell_month_count": planned_months,
        "repaired_discretionary_sell_event_count": int(counts["repaired_discretionary_sell_event_count"]),
        "repaired_unknown_plan_flag_event_count": int(counts["repaired_unknown_plan_flag_event_count"]),
        "missing_plan_flag_not_discretionary": True,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
        **DOWNSTREAM_FLAGS,
    }


def _locator_coverage_frame(counts: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "s_code_accession_count": counts["s_code_accession_count"],
                "raw_file_found_count": counts["raw_file_found_count"],
                "raw_file_found_share": counts["raw_file_found_share"],
                "minimum_required_raw_file_found_share": 0.80,
            },
        ],
    )


def _render_report(summary: dict[str, object], counts: dict[str, object]) -> str:
    return "\n".join(
        [
            "# D2-INSIDER-02B Plan-Flag Source Locator Parser Repair",
            "",
            "This is a source/locator/parser repair attempt only and not alpha evidence.",
            "It does not write a formula, MeasurementSpec, expected-return panel, Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production artifact.",
            "Missing plan flags remain unknown/no_view and are not treated as discretionary false.",
            "",
            f"- decision: `{summary['overall_decision']}`",
            f"- reason: {summary['decision_reason']}",
            f"- next_action: `{summary['next_action']}`",
            f"- raw_file_found_share: {counts['raw_file_found_share']}",
            f"- known_plan_flag_share: {counts['known_plan_flag_share']}",
            f"- structured_or_high_confidence_source_share: {counts['structured_or_high_confidence_source_share']}",
            f"- repaired planned sell events: {counts['repaired_planned_sell_event_count']}",
            f"- repaired planned sell months: {counts['repaired_planned_sell_month_count']}",
        ],
    )


def _first(frame: pd.DataFrame, column: str) -> object:
    if column not in frame.columns or frame.empty:
        return ""
    return frame.iloc[0].get(column, "")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
