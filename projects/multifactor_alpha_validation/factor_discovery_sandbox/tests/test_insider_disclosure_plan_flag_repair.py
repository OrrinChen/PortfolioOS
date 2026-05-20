from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_plan_flag_repair import run_plan_flag_source_locator_parser_repair


def test_plan_flag_repair_switches_to_8k_when_raw_locator_coverage_is_low(tmp_path: Path) -> None:
    registry_path = _write_registry(tmp_path / "registry.csv", accessions=["0000000001-24-000001", "0000000002-24-000002", "0000000003-24-000003", "0000000004-24-000004", "0000000005-24-000005"])
    parse_coverage_path = _write_parse_coverage(tmp_path / "parse_coverage.csv", ["0000000001-24-000001", "0000000002-24-000002", "0000000003-24-000003", "0000000004-24-000004", "0000000005-24-000005"])
    source_root = tmp_path / "source"
    _write_raw_form4(source_root / "documents" / "AAA" / "4" / "2024-05-01_0000000001-24-000001" / "primary.xml", checkbox="X")

    result = run_plan_flag_source_locator_parser_repair(
        event_registry_path=registry_path,
        parse_coverage_path=parse_coverage_path,
        source_roots=[source_root],
        output_dir=tmp_path / "repair",
        minimum_raw_file_found_share=0.80,
    )

    assert result.summary["schema_version"] == "insider_disclosure_plan_flag_repair_summary.v1"
    assert result.summary["stage"] == "D2-INSIDER-02B"
    assert result.summary["overall_decision"] == "source_locator_repair_failed_switch_to_8k"
    assert result.summary["next_action"] == "switch_to_D2_8K_01_subtype_underreaction"
    assert result.summary["raw_file_found_share"] == 0.2
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["d2_sell_contrast_rerun"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    missing = pd.read_csv(result.artifacts["missing_raw_file_report"])
    assert len(missing) == 4
    report = result.artifacts["repair_report"].read_text(encoding="utf-8").lower()
    assert "source/locator/parser repair attempt only" in report
    assert "not alpha evidence" in report
    assert "switch_to_d2_8k_01_subtype_underreaction" in report


def test_plan_flag_repair_distinguishes_true_false_and_missing_without_formula(tmp_path: Path) -> None:
    accessions = ["0000000001-24-000001", "0000000002-24-000002", "0000000003-24-000003"]
    registry_path = _write_registry(tmp_path / "registry.csv", accessions=accessions)
    parse_coverage_path = _write_parse_coverage(tmp_path / "parse_coverage.csv", accessions)
    source_root = tmp_path / "source"
    _write_raw_form4(source_root / "documents" / "AAA" / "4" / "2024-05-01_0000000001-24-000001" / "primary.xml", checkbox="X")
    _write_raw_form4(source_root / "documents" / "BBB" / "4" / "2024-05-02_0000000002-24-000002" / "primary.xml", checkbox="")
    _write_raw_form4(source_root / "documents" / "CCC" / "4" / "2024-05-03_0000000003-24-000003" / "primary.xml", checkbox=None)

    result = run_plan_flag_source_locator_parser_repair(
        event_registry_path=registry_path,
        parse_coverage_path=parse_coverage_path,
        source_roots=[source_root],
        output_dir=tmp_path / "repair",
        minimum_planned_sell_events=1,
        minimum_planned_sell_month_count=1,
        minimum_known_plan_flag_share=0.60,
        minimum_raw_file_found_share=0.80,
        minimum_structured_or_high_confidence_source_share=0.60,
    )

    assert result.summary["overall_decision"] == "plan_flag_repair_gate_passed_rerun_d2_allowed"
    assert result.summary["repaired_planned_sell_event_count"] == 1
    assert result.summary["repaired_discretionary_sell_event_count"] == 1
    assert result.summary["repaired_unknown_plan_flag_event_count"] == 1
    assert result.summary["missing_plan_flag_not_discretionary"] is True
    assert result.summary["formula_score_written"] is False
    assert result.summary["expected_return_panel_written"] is False

    counts = json.loads(result.artifacts["explicit_true_false_unknown_counts"].read_text(encoding="utf-8"))
    assert counts["after_parser_event_counts"] == {"explicit_true": 1, "explicit_false": 1, "missing": 1}

    before_after = pd.read_csv(result.artifacts["plan_flag_parser_before_after"])
    missing_row = before_after[before_after["parser_after_flag"] == "missing"].iloc[0]
    assert missing_row["repaired_event_subset"] == "unknown_plan_flag"
    assert bool(missing_row["missing_plan_flag_not_discretionary"]) is True


def _write_registry(path: Path, accessions: list[str]) -> Path:
    rows = []
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
    for idx, accession in enumerate(accessions):
        rows.append(
            {
                "event_id": f"{accession}_000",
                "ticker": tickers[idx],
                "accession_number": accession,
                "filing_accepted_ts": f"2024-05-{idx + 1:02d}T20:00:00+00:00",
                "tradable_timestamp": f"2024-05-{idx + 2:02d}T13:30:00+00:00",
                "transaction_code": "S",
                "acquired_disposed": "D",
                "rule_10b5_1_flag": "",
                "event_subset": "unknown_no_view",
                "coverage_state": "no_view",
                "no_view_reason": "unknown_post_2023_plan_flag",
            },
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_parse_coverage(path: Path, accessions: list[str]) -> Path:
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
    rows = []
    for idx, accession in enumerate(accessions):
        rows.append(
            {
                "file": f"documents/{tickers[idx]}/4/2024-05-{idx + 1:02d}_{accession}/primary.xml",
                "accession_number": accession,
                "parse_status": "parsed",
                "source_format": "sec_rendered_html_form4",
                "parsed_transaction_count": 1,
                "failure_reason": "",
            },
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_raw_form4(path: Path, checkbox: str | None) -> None:
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
        {checkbox_row}
        <table><tr><td>Table I - Non-Derivative Securities Acquired, Disposed of</td></tr>
        <tr><td>Common Stock</td><td>2024-05-01</td><td>S</td></tr></table>
        </body></html>
        """,
        encoding="utf-8",
    )
