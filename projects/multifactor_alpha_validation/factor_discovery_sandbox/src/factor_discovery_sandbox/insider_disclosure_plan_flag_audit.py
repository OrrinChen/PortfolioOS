"""D2-INSIDER-02A parser/source audit for Form 4 10b5-1 plan flags."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


SUMMARY_SCHEMA_VERSION = "insider_disclosure_plan_flag_audit_summary.v1"
STAGE = "D2-INSIDER-02A"
CANDIDATE_ID = "planned_vs_discretionary_sell_contrast_post_2023"

TAG_INVENTORY_TERMS = ("10b5", "10b5-1", "10b5_1", "rule10", "plan", "adoption", "transaction", "checkbox")
FOOTNOTE_PATTERNS = (
    r"rule\s+10b5-?1",
    r"10b5-?1\(c\)",
    r"trading plan",
    r"pursuant to a plan",
    r"adopted on",
    r"adopted pursuant",
    r"intended to satisfy",
)
ADOPTION_DATE_PATTERN = (
    r"adopt(?:ed|ion)?(?:\s+date)?(?:\s+on)?\s+"
    r"((?:20\d{2}[-/]\d{1,2}[-/]\d{1,2})|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+20\d{2})"
)

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
class PlanFlagAuditResult:
    """Artifacts and summary for D2-INSIDER-02A."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_plan_flag_parser_source_audit(
    event_registry_path: str | Path,
    parse_coverage_path: str | Path,
    source_roots: Iterable[str | Path],
    output_dir: str | Path,
    max_samples_per_bucket: int = 100,
    minimum_planned_sell_events: int = 300,
    minimum_known_plan_flag_share: float = 0.60,
    minimum_high_confidence_source_share: float = 0.80,
) -> PlanFlagAuditResult:
    """Audit whether raw Form 4 sources can support planned/discretionary S-code splits."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    registry = _load_registry(Path(event_registry_path))
    parse_coverage = pd.read_csv(parse_coverage_path, low_memory=False).fillna("")
    source_index = _build_source_index(parse_coverage, [Path(root) for root in source_roots])

    s_code = _post_2023_s_code_events(registry)
    samples = _sample_events(s_code, max_samples_per_bucket, source_index)
    sample_rows, inventory_rows, footnote_rows = _audit_samples(samples, source_index)

    sample_manifest = pd.DataFrame(sample_rows)
    field_inventory = pd.DataFrame(inventory_rows)
    footnote_candidates = pd.DataFrame(footnote_rows)
    source_coverage = _build_source_coverage(s_code, sample_manifest)
    summary = _build_summary(
        s_code=s_code,
        sample_manifest=sample_manifest,
        source_coverage=source_coverage,
        minimum_planned_sell_events=minimum_planned_sell_events,
        minimum_known_plan_flag_share=minimum_known_plan_flag_share,
        minimum_high_confidence_source_share=minimum_high_confidence_source_share,
    )

    sample_manifest.to_csv(artifacts["sample_manifest"], index=False)
    field_inventory.to_csv(artifacts["raw_field_inventory"], index=False)
    footnote_candidates.to_csv(artifacts["footnote_plan_flag_candidates"], index=False)
    _write_json(artifacts["plan_flag_source_coverage"], source_coverage)
    _write_json(artifacts["plan_flag_audit_summary"], summary)
    artifacts["plan_flag_audit_report"].write_text(_render_report(summary, source_coverage), encoding="utf-8")

    return PlanFlagAuditResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "sample_manifest": output_path / "sample_manifest.csv",
        "raw_field_inventory": output_path / "raw_field_inventory.csv",
        "footnote_plan_flag_candidates": output_path / "footnote_plan_flag_candidates.csv",
        "plan_flag_source_coverage": output_path / "plan_flag_source_coverage.json",
        "plan_flag_audit_summary": output_path / "plan_flag_audit_summary.json",
        "plan_flag_audit_report": output_path / "plan_flag_audit_report.md",
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
    frame["_filing_date"] = pd.to_datetime(frame["filing_accepted_ts"], errors="coerce").dt.date.astype(str)
    frame["_plan_flag_normalized"] = frame["rule_10b5_1_flag"].map(_normalize_plan_flag)
    return frame[
        frame["transaction_code"].astype(str).str.upper().eq("S")
        & (pd.to_datetime(frame["filing_accepted_ts"], errors="coerce") >= pd.Timestamp("2023-04-01", tz="UTC"))
    ].reset_index(drop=True)


def _normalize_plan_flag(value: object) -> str:
    lower = str(value).strip().lower()
    if lower in {"true", "t", "1", "yes", "y"}:
        return "true"
    if lower in {"false", "f", "0", "no", "n"}:
        return "false"
    return "unknown"


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


def _sample_events(s_code: pd.DataFrame, max_samples_per_bucket: int, source_index: dict[str, Path]) -> pd.DataFrame:
    frames = []
    for bucket in ("unknown", "false", "true"):
        subset = s_code[s_code["_plan_flag_normalized"].eq(bucket)].drop_duplicates("accession_number")
        subset = subset.copy()
        subset["_source_available"] = subset["accession_number"].astype(str).isin(source_index)
        frames.append(
            subset.sort_values(["_source_available", "accession_number", "event_id"], ascending=[False, True, True]).head(
                max_samples_per_bucket,
            ),
        )
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _audit_samples(samples: pd.DataFrame, source_index: dict[str, Path]) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    sample_rows: list[dict[str, object]] = []
    inventory_rows: list[dict[str, object]] = []
    footnote_rows: list[dict[str, object]] = []
    for _, event in samples.iterrows():
        accession = str(event["accession_number"])
        raw_path = source_index.get(accession)
        registry_flag = str(event["_plan_flag_normalized"])
        if raw_path is None:
            sample_rows.append(_missing_sample_row(event, registry_flag))
            continue
        raw_text = raw_path.read_text(encoding="utf-8", errors="ignore")
        raw_audit = _audit_raw_text(raw_text)
        sample_rows.append(
            {
                "event_id": event["event_id"],
                "accession_number": accession,
                "ticker": event.get("ticker", ""),
                "registry_plan_flag": registry_flag,
                "raw_file": str(raw_path),
                **raw_audit["sample"],
            },
        )
        for row in raw_audit["inventory"]:
            inventory_rows.append({"accession_number": accession, "raw_file": str(raw_path), **row})
        if raw_audit["sample"]["footnote_10b5_candidate"]:
            footnote_rows.append(
                {
                    "event_id": event["event_id"],
                    "accession_number": accession,
                    "registry_plan_flag": registry_flag,
                    "raw_file": str(raw_path),
                    "footnote_10b5_candidate": True,
                    "footnote_adoption_date_candidate": raw_audit["sample"]["footnote_adoption_date_candidate"],
                    "footnote_context": raw_audit["sample"]["footnote_context"],
                    "source_confidence": raw_audit["sample"]["source_confidence"],
                },
            )
    return sample_rows, inventory_rows, footnote_rows


def _missing_sample_row(event: pd.Series, registry_flag: str) -> dict[str, object]:
    return {
        "event_id": event["event_id"],
        "accession_number": event["accession_number"],
        "ticker": event.get("ticker", ""),
        "registry_plan_flag": registry_flag,
        "raw_file": "",
        "source_found": False,
        "structured_checkbox_present": False,
        "raw_structured_plan_flag": "missing",
        "footnote_10b5_candidate": False,
        "footnote_adoption_date_candidate": False,
        "high_confidence_planned_candidate": False,
        "structured_vs_footnote_disagreement": False,
        "registry_false_without_structured_source": registry_flag == "false",
        "source_confidence": "missing_source",
        "footnote_context": "",
    }


def _audit_raw_text(raw_text: str) -> dict[str, object]:
    checkbox_status = _checkbox_status(raw_text)
    non_boilerplate = _remove_checkbox_boilerplate(raw_text)
    footnote_context = _footnote_context(non_boilerplate)
    footnote_candidate = bool(footnote_context)
    adoption_candidate = bool(re.search(ADOPTION_DATE_PATTERN, footnote_context, flags=re.IGNORECASE))
    structured_flag = checkbox_status
    high_confidence = structured_flag == "true" or (footnote_candidate and adoption_candidate)
    disagreement = structured_flag == "false" and high_confidence
    source_confidence = "structured" if structured_flag in {"true", "false"} else "footnote_weak" if footnote_candidate else "missing"
    return {
        "sample": {
            "source_found": True,
            "structured_checkbox_present": structured_flag in {"true", "false"},
            "raw_structured_plan_flag": structured_flag,
            "footnote_10b5_candidate": footnote_candidate,
            "footnote_adoption_date_candidate": adoption_candidate,
            "high_confidence_planned_candidate": high_confidence,
            "structured_vs_footnote_disagreement": disagreement,
            "registry_false_without_structured_source": False,
            "source_confidence": source_confidence,
            "footnote_context": footnote_context[:400],
        },
        "inventory": _field_inventory(raw_text),
    }


def _checkbox_status(raw_text: str) -> str:
    phrase_match = re.search(r"check this box.{0,500}?10b5", raw_text, flags=re.IGNORECASE | re.DOTALL)
    if phrase_match:
        before_phrase = _strip_html(raw_text[max(0, phrase_match.start() - 320) : phrase_match.start()])
        if re.search(r"(^|\s|[\[\(])(?:x|☒|checked)(\s|$|[\]\)])", before_phrase, flags=re.IGNORECASE):
            return "true"
        return "false"
    for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", raw_text, flags=re.IGNORECASE | re.DOTALL):
        row_html = row_match.group(1)
        row_lower = row_html.lower()
        if "10b5" not in row_lower or "check this box" not in row_lower:
            continue
        phrase_position = row_lower.find("check this box")
        before_phrase = _strip_html(row_html[:phrase_position])
        if re.search(r"(^|\s|[\[\(])(?:x|☒|checked)(\s|$|[\]\)])", before_phrase, flags=re.IGNORECASE):
            return "true"
        return "false"
    for tag_match in re.finditer(r"<([A-Za-z0-9_:\-]*10b5[A-Za-z0-9_:\-]*)[^>]*>(.*?)</", raw_text, flags=re.IGNORECASE | re.DOTALL):
        value = _strip_html(tag_match.group(2)).strip().lower()
        if value in {"true", "1", "yes", "y"}:
            return "true"
        if value in {"false", "0", "no", "n"}:
            return "false"
    return "missing"


def _remove_checkbox_boilerplate(raw_text: str) -> str:
    return re.sub(
        r"<tr[^>]*>.*?check this box.*?10b5.*?</tr>",
        " ",
        raw_text,
        flags=re.IGNORECASE | re.DOTALL,
    )


def _footnote_context(raw_text: str) -> str:
    text = _strip_html(raw_text)
    matches = []
    for pattern in FOOTNOTE_PATTERNS:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            start = max(0, match.start() - 160)
            end = min(len(text), match.end() + 220)
            context = text[start:end].strip()
            if "check this box" not in context.lower():
                matches.append(context)
    return " | ".join(dict.fromkeys(matches))


def _field_inventory(raw_text: str) -> list[dict[str, object]]:
    rows = []
    for match in re.finditer(r"<\s*/?\s*([A-Za-z0-9_:\-]+)([^>]*)>", raw_text):
        tag = match.group(1)
        attrs = match.group(2)
        lower = f"{tag} {attrs}".lower()
        terms = [term for term in TAG_INVENTORY_TERMS if term in lower]
        if terms:
            rows.append(
                {
                    "match_type": "tag_or_attribute",
                    "field_name": tag,
                    "matched_terms": "|".join(terms),
                    "context": match.group(0)[:240],
                },
            )
    for pattern in FOOTNOTE_PATTERNS:
        for match in re.finditer(pattern, raw_text, flags=re.IGNORECASE):
            rows.append(
                {
                    "match_type": "text",
                    "field_name": pattern,
                    "matched_terms": match.group(0),
                    "context": _strip_html(raw_text[max(0, match.start() - 120) : match.end() + 160])[:240],
                },
            )
    return rows


def _build_source_coverage(s_code: pd.DataFrame, sample_manifest: pd.DataFrame) -> dict[str, object]:
    if sample_manifest.empty:
        raw_count = 0
        structured_true = structured_false = structured_missing = 0
        footnote_count = adoption_count = candidate_count = disagreement = false_without_source = 0
    else:
        raw_count = int(sample_manifest["source_found"].eq(True).sum())
        structured_true = int(sample_manifest["raw_structured_plan_flag"].eq("true").sum())
        structured_false = int(sample_manifest["raw_structured_plan_flag"].eq("false").sum())
        structured_missing = int(sample_manifest["raw_structured_plan_flag"].eq("missing").sum())
        footnote_count = int(sample_manifest["footnote_10b5_candidate"].eq(True).sum())
        adoption_count = int(sample_manifest["footnote_adoption_date_candidate"].eq(True).sum())
        candidate_count = int(sample_manifest["high_confidence_planned_candidate"].eq(True).sum())
        disagreement = int(sample_manifest["structured_vs_footnote_disagreement"].eq(True).sum())
        false_without_source = int(
            (sample_manifest["registry_plan_flag"].eq("false") & sample_manifest["raw_structured_plan_flag"].eq("missing")).sum(),
        )
    total_s = len(s_code)
    known = int(s_code["_plan_flag_normalized"].isin(["true", "false"]).sum()) if total_s else 0
    unknown_or_planned_samples = sample_manifest[
        sample_manifest["registry_plan_flag"].isin(["unknown", "true"])
    ] if not sample_manifest.empty else sample_manifest
    denominator = max(1, int(unknown_or_planned_samples["source_found"].eq(True).sum()) if not sample_manifest.empty else 0)
    return {
        "s_code_event_count": int(total_s),
        "registry_true_count": int(s_code["_plan_flag_normalized"].eq("true").sum()) if total_s else 0,
        "registry_false_count": int(s_code["_plan_flag_normalized"].eq("false").sum()) if total_s else 0,
        "registry_unknown_count": int(s_code["_plan_flag_normalized"].eq("unknown").sum()) if total_s else 0,
        "registry_known_plan_flag_share": round(known / total_s, 6) if total_s else 0.0,
        "sampled_event_count": int(len(sample_manifest)),
        "sampled_raw_file_count": raw_count,
        "structured_true_count": structured_true,
        "structured_false_count": structured_false,
        "structured_missing_count": structured_missing,
        "footnote_10b5_candidate_count": footnote_count,
        "footnote_adoption_date_candidate_count": adoption_count,
        "planned_sell_structured_or_high_confidence_count": candidate_count,
        "structured_vs_footnote_disagreement_count": disagreement,
        "false_without_structured_source_count": false_without_source,
        "high_confidence_source_share": round(candidate_count / denominator, 6),
    }


def _build_summary(
    s_code: pd.DataFrame,
    sample_manifest: pd.DataFrame,
    source_coverage: dict[str, object],
    minimum_planned_sell_events: int,
    minimum_known_plan_flag_share: float,
    minimum_high_confidence_source_share: float,
) -> dict[str, object]:
    candidate_count = int(source_coverage["planned_sell_structured_or_high_confidence_count"])
    known_share = float(source_coverage["registry_known_plan_flag_share"])
    high_confidence_share = float(source_coverage["high_confidence_source_share"])
    if (
        candidate_count >= minimum_planned_sell_events
        and known_share >= minimum_known_plan_flag_share
        and high_confidence_share >= minimum_high_confidence_source_share
    ):
        decision = "plan_flag_source_repair_available"
        reason = "structured or high-confidence plan-flag source candidates are present"
    elif candidate_count > 0:
        decision = "hold_pending_clean_plan_flag_source"
        reason = "only weak or insufficient plan-flag source candidates are present"
    else:
        decision = "plan_flag_source_unavailable_keep_blocked"
        reason = "no structured or high-confidence planned-sell source candidates found"
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "candidate_id": CANDIDATE_ID,
        "overall_decision": decision,
        "decision_reason": reason,
        "d2_insider_02_status": "blocked_plan_flag_coverage",
        "allow_d3_charter_for": [],
        "sampled_event_count": int(len(sample_manifest)),
        "s_code_event_count": int(source_coverage["s_code_event_count"]),
        "registry_true_count": int(source_coverage["registry_true_count"]),
        "registry_false_count": int(source_coverage["registry_false_count"]),
        "registry_unknown_count": int(source_coverage["registry_unknown_count"]),
        "known_plan_flag_share": known_share,
        "structured_true_count": int(source_coverage["structured_true_count"]),
        "structured_false_count": int(source_coverage["structured_false_count"]),
        "structured_missing_count": int(source_coverage["structured_missing_count"]),
        "footnote_10b5_candidate_count": int(source_coverage["footnote_10b5_candidate_count"]),
        "footnote_adoption_date_candidate_count": int(source_coverage["footnote_adoption_date_candidate_count"]),
        "planned_sell_structured_or_high_confidence_count": candidate_count,
        "structured_vs_footnote_disagreement_count": int(source_coverage["structured_vs_footnote_disagreement_count"]),
        "false_without_structured_source_count": int(source_coverage["false_without_structured_source_count"]),
        "high_confidence_source_share": high_confidence_share,
        "missing_remains_unknown_no_view": True,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
        **DOWNSTREAM_FLAGS,
    }


def _render_report(summary: dict[str, object], source_coverage: dict[str, object]) -> str:
    return "\n".join(
        [
            "# D2-INSIDER-02A Plan-Flag Parser/Source Audit",
            "",
            "This is a parser/source audit only and not alpha evidence.",
            "It does not write a formula, MeasurementSpec, expected-return panel, Q1, Q2, or downstream handoff.",
            "Missing plan flags remain unknown/no_view and are never converted into discretionary sells.",
            "",
            f"- decision: `{summary['overall_decision']}`",
            f"- reason: {summary['decision_reason']}",
            f"- S-code events: {source_coverage['s_code_event_count']}",
            f"- registry false / true / unknown: {source_coverage['registry_false_count']} / {source_coverage['registry_true_count']} / {source_coverage['registry_unknown_count']}",
            f"- sampled raw files: {source_coverage['sampled_raw_file_count']}",
            f"- structured true / false / missing: {source_coverage['structured_true_count']} / {source_coverage['structured_false_count']} / {source_coverage['structured_missing_count']}",
            f"- footnote 10b5 candidates: {source_coverage['footnote_10b5_candidate_count']}",
            f"- adoption-date candidates: {source_coverage['footnote_adoption_date_candidate_count']}",
            f"- false-without-structured-source rows: {source_coverage['false_without_structured_source_count']}",
            "",
            "D2-INSIDER-02 remains blocked unless a later explicit parser/source repair produces objective plan-flag coverage.",
        ],
    )


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
