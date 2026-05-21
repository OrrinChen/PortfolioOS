from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_plan_flag_audit import run_plan_flag_parser_source_audit


def test_plan_flag_audit_finds_structured_and_footnote_candidates(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path / "registry.csv")
    parse_coverage_path = _write_parse_coverage(tmp_path / "parse_coverage.csv")
    source_root = tmp_path / "source"
    _write_raw_form4(
        source_root / "documents" / "AAA" / "4" / "2024-05-01_0000000001-24-000001" / "primary.xml",
        checkbox="",
        footnote="",
    )
    _write_raw_form4(
        source_root / "documents" / "BBB" / "4" / "2024-05-02_0000000002-24-000002" / "primary.xml",
        checkbox="X",
        footnote="Shares were sold pursuant to a Rule 10b5-1 trading plan adopted on 2024-01-15.",
    )
    _write_raw_form4(
        source_root / "documents" / "CCC" / "4" / "2024-05-03_0000000003-24-000003" / "primary.xml",
        checkbox=None,
        footnote="No structured plan checkbox appears in this local source.",
    )

    result = run_plan_flag_parser_source_audit(
        event_registry_path=registry_path,
        parse_coverage_path=parse_coverage_path,
        source_roots=[source_root],
        output_dir=tmp_path / "audit",
        max_samples_per_bucket=10,
        minimum_planned_sell_events=1,
        minimum_known_plan_flag_share=0.40,
        minimum_high_confidence_source_share=0.40,
    )

    assert result.summary["schema_version"] == "insider_disclosure_plan_flag_audit_summary.v1"
    assert result.summary["stage"] == "D2-INSIDER-02A"
    assert result.summary["candidate_id"] == "planned_vs_discretionary_sell_contrast_post_2023"
    assert result.summary["planned_sell_structured_or_high_confidence_count"] == 1
    assert result.summary["structured_true_count"] == 1
    assert result.summary["structured_false_count"] == 1
    assert result.summary["footnote_10b5_candidate_count"] == 1
    assert result.summary["footnote_adoption_date_candidate_count"] == 1
    assert result.summary["false_without_structured_source_count"] == 1
    assert result.summary["missing_remains_unknown_no_view"] is True
    assert result.summary["overall_decision"] == "plan_flag_source_repair_available"
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    coverage = json.loads(result.artifacts["plan_flag_source_coverage"].read_text(encoding="utf-8"))
    assert coverage["sampled_raw_file_count"] == 3
    assert coverage["structured_vs_footnote_disagreement_count"] == 0

    report = result.artifacts["plan_flag_audit_report"].read_text(encoding="utf-8").lower()
    assert "parser/source audit only" in report
    assert "not alpha evidence" in report
    assert "missing plan flags remain unknown/no_view" in report
    for forbidden in ["production approved", "paper ready", "live trading", "broker", "order generation"]:
        assert forbidden not in report


def test_plan_flag_audit_keeps_sell_contrast_blocked_without_candidates(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path / "registry.csv", include_candidate=False)
    parse_coverage_path = _write_parse_coverage(tmp_path / "parse_coverage.csv", include_candidate=False)
    source_root = tmp_path / "source"
    _write_raw_form4(
        source_root / "documents" / "AAA" / "4" / "2024-05-01_0000000001-24-000001" / "primary.xml",
        checkbox="",
        footnote="",
    )
    _write_raw_form4(
        source_root / "documents" / "CCC" / "4" / "2024-05-03_0000000003-24-000003" / "primary.xml",
        checkbox=None,
        footnote="No structured plan checkbox appears in this local source.",
    )

    result = run_plan_flag_parser_source_audit(
        event_registry_path=registry_path,
        parse_coverage_path=parse_coverage_path,
        source_roots=[source_root],
        output_dir=tmp_path / "audit",
        max_samples_per_bucket=10,
    )

    assert result.summary["overall_decision"] == "plan_flag_source_unavailable_keep_blocked"
    assert result.summary["d2_insider_02_status"] == "blocked_plan_flag_coverage"
    assert result.summary["allow_d3_charter_for"] == []


def _write_registry(path: Path, include_candidate: bool = True) -> Path:
    rows = [
        _registry_row("0000000001-24-000001", "AAA", "false"),
        _registry_row("0000000003-24-000003", "CCC", "false"),
    ]
    if include_candidate:
        rows.append(_registry_row("0000000002-24-000002", "BBB", ""))
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _registry_row(accession: str, ticker: str, plan_flag: str) -> dict[str, object]:
    return {
        "event_id": f"{accession}_000",
        "ticker": ticker,
        "accession_number": accession,
        "filing_accepted_ts": "2024-05-02T20:00:00+00:00",
        "tradable_timestamp": "2024-05-03T13:30:00+00:00",
        "transaction_code": "S",
        "acquired_disposed": "D",
        "rule_10b5_1_flag": plan_flag,
        "event_subset": "unknown_no_view" if plan_flag == "" else "discretionary_sell",
        "coverage_state": "no_view" if plan_flag == "" else "covered",
        "no_view_reason": "unknown_post_2023_plan_flag" if plan_flag == "" else "",
    }


def _write_parse_coverage(path: Path, include_candidate: bool = True) -> Path:
    rows = [
        _coverage_row("AAA", "0000000001-24-000001"),
        _coverage_row("CCC", "0000000003-24-000003"),
    ]
    if include_candidate:
        rows.append(_coverage_row("BBB", "0000000002-24-000002"))
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _coverage_row(ticker: str, accession: str) -> dict[str, object]:
    return {
        "file": f"documents/{ticker}/4/2024-05-01_{accession}/primary.xml",
        "accession_number": accession,
        "parse_status": "parsed",
        "source_format": "sec_rendered_html_form4",
        "parsed_transaction_count": 1,
        "failure_reason": "",
    }


def _write_raw_form4(path: Path, checkbox: str | None, footnote: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    checkbox_row = ""
    if checkbox is not None:
        checkbox_row = f"""
        <tr valign="middle">
          <td><table><tr><td>{checkbox}</td></tr></table></td>
          <td>Check this box to indicate that a transaction was made pursuant to a contract,
          instruction or written plan for the purchase or sale of equity securities of the issuer
          that is intended to satisfy the affirmative defense conditions of Rule 10b5-1(c).</td>
        </tr>
        """
    path.write_text(
        f"""
        <html><body>
        <documentType>4</documentType>
        {checkbox_row}
        <table><tr><td>Table I - Non-Derivative Securities Acquired, Disposed of</td></tr>
        <tr><td>Common Stock</td><td>2024-05-01</td><td>S</td></tr></table>
        <div>Explanation of Responses: {footnote}</div>
        </body></html>
        """,
        encoding="utf-8",
    )
