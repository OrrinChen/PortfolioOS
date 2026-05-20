from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.eightk_subtype_d2_real import (
    run_real_eightk_subtype_observability,
)


def test_real_eightk_replay_writes_unavailable_when_source_missing(tmp_path: Path) -> None:
    result = run_real_eightk_subtype_observability(
        source_dir=tmp_path / "missing_8k_archive",
        price_panel_path=tmp_path / "missing_prices.csv",
        output_dir=tmp_path / "d2_8k_real",
    )

    assert result.summary["schema_version"] == "eightk_subtype_d2_real_summary.v1"
    assert result.summary["stage"] == "D2-8K-01R"
    assert result.summary["real_data_status"] == "unavailable_missing_source"
    assert result.summary["overall_decision"] == "blocked_source_coverage"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["production_approval_claimed"] is False

    missing = json.loads(result.artifacts["missing_inputs_report"].read_text(encoding="utf-8"))
    assert missing["missing_inputs"] == ["source_dir"]


def test_real_eightk_replay_parses_archive_admits_sources_and_allows_at_most_one_d3_subtype(tmp_path: Path) -> None:
    source_dir = _write_real_eightk_archive(tmp_path / "sec_8k_archive")
    price_path = _write_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")

    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "d2_8k_real",
        minimum_subtype_events=2,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["real_data_status"] == "local_8k_replay_complete"
    assert result.summary["network_used"] is False
    assert result.summary["source_index_total_count"] == 8
    assert result.summary["raw_file_found_share"] == 1.0
    assert result.summary["accepted_timestamp_coverage_share"] == 1.0
    assert result.summary["item_header_parse_coverage_share"] >= 0.75
    assert result.summary["market_coverage_share"] >= 0.75
    assert result.summary["overall_decision"] == "observable"
    assert result.summary["allow_d3_charter_for"] == ["auditor_change"]
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["production_approval_claimed"] is False

    registry = pd.read_csv(result.artifacts["eightk_event_registry_real"])
    assert set(registry["eightk_subtype"]) >= {
        "auditor_change",
        "restatement_amendment",
        "ceo_departure",
        "cfo_departure",
        "material_agreement_termination",
        "unknown_no_view",
    }
    appointment_only = registry[registry["ticker"].eq("APP")].iloc[0]
    assert appointment_only["eightk_subtype"] == "unknown_no_view"
    assert appointment_only["no_view_reason"] == "excluded_appointment_or_compensation_only_5_02"
    assert registry["formula_score_written"].eq(False).all()
    assert "expected_return" not in registry.columns

    source = json.loads(result.artifacts["source_admission_report"].read_text(encoding="utf-8"))
    assert source["source_type"] == "local_edgar_8k_archive"
    assert source["network_used"] is False
    assert source["raw_file_found_share"] == 1.0

    raw_locator = pd.read_csv(result.artifacts["raw_locator_coverage_report"])
    assert raw_locator["raw_file_found"].all()
    document_type = pd.read_csv(result.artifacts["document_type_audit"])
    assert set(document_type["document_type"].str.upper()) >= {"8-K", "8-K/A"}
    item_audit = pd.read_csv(result.artifacts["item_header_parse_audit"])
    assert item_audit["item_header_found"].sum() >= 6
    market_join = pd.read_csv(result.artifacts["issuer_market_join_coverage"])
    assert market_join["covered_count"].iloc[0] >= 6

    placebo = pd.read_csv(result.artifacts["placebo_report_real"])
    assert {
        "shift_minus_5",
        "shift_plus_5",
        "same_coverage_random",
        "subtype_label_randomized",
        "issuer_non_event",
        "routine_8k_control",
    }.issubset(set(placebo["placebo_name"]))

    report = result.artifacts["d2_8k_subtype_report_real"].read_text(encoding="utf-8").lower()
    assert "real edgar 8-k archive source admission" in report
    assert "no-formula observability only" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report


def test_real_eightk_replay_blocks_when_market_coverage_missing(tmp_path: Path) -> None:
    source_dir = _write_real_eightk_archive(tmp_path / "sec_8k_archive")
    price_path = _write_price_fixture(tmp_path / "prices.csv", omit_tickers={"AUD"})

    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=price_path,
        output_dir=tmp_path / "d2_8k_real_missing_market",
        minimum_subtype_events=2,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["overall_decision"] == "blocked_market_coverage"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False


def test_real_eightk_replay_prefers_primary_8k_document_over_exhibit_link(tmp_path: Path) -> None:
    source_dir = tmp_path / "sec_8k_archive"
    filing_dir = source_dir / "documents" / "AUD" / "8-K" / "2024-01-03_0000000000-24-100001"
    filing_dir.mkdir(parents=True)
    exhibit = filing_dir / "primary_a-ex99_1.htm"
    exhibit.write_text("<html><body><p>Exhibit 99.1 press release only.</p></body></html>", encoding="utf-8")
    primary = filing_dir / "primary_a-20240103x8k.htm"
    primary.write_text(_auditor_doc("auditor resigned"), encoding="utf-8")
    request_specs = [
        {
            "symbol": "AUD",
            "formType": "8-K",
            "acceptedDate": "2024-01-03 21:00:00",
            "relative_path": str(exhibit.relative_to(source_dir)),
            "url": "https://www.sec.gov/Archives/a-ex99_1.htm",
            "finalLink": "https://www.sec.gov/Archives/a-ex99_1.htm",
            "cik": "0001000001",
        },
    ]
    (source_dir / "request_specs.json").write_text(json.dumps(request_specs, indent=2), encoding="utf-8")
    price_path = _write_price_fixture(tmp_path / "prices.csv")

    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=price_path,
        output_dir=tmp_path / "d2_8k_real_primary_rescue",
        minimum_subtype_events=1,
        minimum_event_month_count=1,
        minimum_label_coverage_share=0.50,
    )

    registry = pd.read_csv(result.artifacts["eightk_event_registry_real"])
    assert registry["eightk_subtype"].iloc[0] == "auditor_change"
    assert Path(registry["raw_document_path"].iloc[0]).name == "primary_a-20240103x8k.htm"
    raw_locator = pd.read_csv(result.artifacts["raw_locator_coverage_report"])
    assert raw_locator["source_locator_strategy"].iloc[0] == "same_accession_primary_8k_candidate"
    assert raw_locator["requested_file"].iloc[0].endswith("primary_a-ex99_1.htm")
    assert raw_locator["resolved_file"].iloc[0].endswith("primary_a-20240103x8k.htm")
    assert result.summary["item_header_parse_coverage_share"] == 1.0
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False


def test_real_eightk_replay_uses_additional_price_panels_for_market_coverage(tmp_path: Path) -> None:
    source_dir = _write_real_eightk_archive(tmp_path / "sec_8k_archive")
    primary_price_path = _write_price_fixture(tmp_path / "primary_prices.csv", omit_tickers={"AUD"})
    supplemental_price_path = _write_price_fixture(tmp_path / "supplemental_prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")

    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=primary_price_path,
        additional_price_panel_paths=[supplemental_price_path],
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "d2_8k_real_market_rescue",
        minimum_subtype_events=2,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["price_panel_count"] == 2
    assert result.summary["market_coverage_share"] >= 0.75
    assert result.summary["overall_decision"] == "observable"
    assert result.summary["allow_d3_charter_for"] == ["auditor_change"]
    market_join = pd.read_csv(result.artifacts["issuer_market_join_coverage"])
    assert market_join["price_panel_count"].iloc[0] == 2
    assert "supplemental_prices.csv" in market_join["price_panel_paths"].iloc[0]
    assert result.summary["q2_entry_allowed"] is False


def test_real_eightk_replay_does_not_count_unknown_no_view_rows_against_priority_market_gate(tmp_path: Path) -> None:
    source_dir = _write_real_eightk_archive(tmp_path / "sec_8k_archive")
    request_specs = json.loads((source_dir / "request_specs.json").read_text(encoding="utf-8"))
    for idx in range(20):
        ticker = f"U{idx:02d}"
        accession = f"0000000000-24-20{idx:04d}"
        relative = Path("documents") / ticker / "8-K" / f"2024-01-03_{accession}" / "primary_unknown.htm"
        path = source_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_unknown_doc(), encoding="utf-8")
        request_specs.append(
            {
                "symbol": ticker,
                "formType": "8-K",
                "acceptedDate": "2024-01-03 21:00:00",
                "relative_path": str(relative),
                "url": f"https://www.sec.gov/Archives/{accession}/primary_unknown.htm",
                "finalLink": f"https://www.sec.gov/Archives/{accession}/primary_unknown.htm",
                "cik": f"00020{idx:04d}",
            },
        )
    (source_dir / "request_specs.json").write_text(json.dumps(request_specs, indent=2), encoding="utf-8")
    price_path = _write_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")

    result = run_real_eightk_subtype_observability(
        source_dir=source_dir,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "d2_8k_real_priority_gate",
        minimum_subtype_events=2,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["market_coverage_share"] < 0.75
    assert result.summary["priority_market_coverage_share"] >= 0.75
    assert result.summary["overall_decision"] == "observable"
    assert result.summary["allow_d3_charter_for"] == ["auditor_change"]


def _write_real_eightk_archive(root: Path) -> Path:
    root.mkdir(parents=True)
    specs = [
        ("AUD", "8-K", "2024-01-03", "0000000000-24-000001", _auditor_doc("auditor resigned")),
        ("AUD", "8-K", "2024-02-05", "0000000000-24-000002", _auditor_doc("auditor dismissed")),
        ("RST", "8-K/A", "2024-01-03", "0000000000-24-000003", _restatement_doc()),
        ("CEO", "8-K", "2024-01-03", "0000000000-24-000004", _departure_doc("Chief Executive Officer resigned")),
        ("CFO", "8-K", "2024-01-03", "0000000000-24-000005", _departure_doc("Chief Financial Officer resigned")),
        ("MAT", "8-K", "2024-01-03", "0000000000-24-000006", _agreement_termination_doc()),
        ("APP", "8-K", "2024-01-03", "0000000000-24-000007", _appointment_only_doc()),
        ("UNK", "8-K", "2024-01-03", "0000000000-24-000008", _unknown_doc()),
    ]
    request_specs = []
    for ticker, form_type, accepted_date, accession, document in specs:
        folder_form = form_type.replace("/", "_")
        relative = Path("documents") / ticker / folder_form / f"{accepted_date}_{accession}" / "primary_8k.htm"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document, encoding="utf-8")
        request_specs.append(
            {
                "symbol": ticker,
                "formType": form_type,
                "acceptedDate": f"{accepted_date} 21:00:00",
                "relative_path": str(relative),
                "url": f"https://www.sec.gov/Archives/{accession}/primary_8k.htm",
                "finalLink": f"https://www.sec.gov/Archives/{accession}/primary_8k.htm",
                "cik": f"000{len(request_specs) + 1000000}",
            },
        )
    (root / "request_specs.json").write_text(json.dumps(request_specs, indent=2), encoding="utf-8")
    return root


def _auditor_doc(action: str) -> str:
    return f"""
    <html><body>
    <div>FORM 8-K</div>
    <div>Item 4.01 Changes in Registrant's Certifying Accountant.</div>
    <p>The company's independent registered public accounting firm {action}.</p>
    </body></html>
    """


def _restatement_doc() -> str:
    return """
    <html><body>
    <div>FORM 8-K/A</div>
    <div>Item 4.02 Non-Reliance on Previously Issued Financial Statements.</div>
    <p>The audit committee concluded that prior statements should be restated.</p>
    </body></html>
    """


def _departure_doc(text: str) -> str:
    return f"""
    <html><body>
    <div>FORM 8-K</div>
    <div>Item 5.02 Departure of Directors or Certain Officers.</div>
    <p>{text} and will depart from the company. No successor appointment is announced.</p>
    </body></html>
    """


def _agreement_termination_doc() -> str:
    return """
    <html><body>
    <div>FORM 8-K</div>
    <div>Item 1.02 Termination of a Material Definitive Agreement.</div>
    <p>The company terminated a material definitive agreement.</p>
    </body></html>
    """


def _appointment_only_doc() -> str:
    return """
    <html><body>
    <div>FORM 8-K</div>
    <div>Item 5.02 Departure of Directors or Certain Officers.</div>
    <p>The company appointed a new Chief Financial Officer. No resignation, retirement, removal, or termination occurred.</p>
    </body></html>
    """


def _unknown_doc() -> str:
    return """
    <html><body>
    <div>FORM 8-K</div>
    <div>Item 2.02 Results of Operations and Financial Condition.</div>
    <p>Routine earnings release exhibit.</p>
    </body></html>
    """


def _write_price_fixture(path: Path, omit_tickers: set[str] | None = None) -> Path:
    omit_tickers = omit_tickers or set()
    dates = pd.bdate_range("2023-11-01", periods=180)
    drift_by_ticker = {
        "AUD": 0.0030,
        "RST": 0.0022,
        "CEO": 0.0020,
        "CFO": 0.0020,
        "MAT": 0.0018,
        "APP": 0.0001,
        "UNK": 0.0,
    }
    event_dates = [pd.Timestamp("2024-01-04"), pd.Timestamp("2024-02-06")]
    rows = []
    for ticker, drift in drift_by_ticker.items():
        if ticker in omit_tickers:
            continue
        price = 100.0
        for date in dates:
            active_window = any(start < date <= dates[dates.searchsorted(start) + 22] for start in event_dates)
            price *= 1 + (drift if active_window else 0.0)
            rows.append(
                {
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": price,
                    "volume": 1_000_000,
                    "market_cap": 1_000_000_000,
                    "dollar_volume": price * 1_000_000,
                    "bid_ask_spread": 0.001,
                    "sector": "technology",
                },
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_benchmark_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", periods=180)
    price = 100.0
    rows = []
    for date in dates:
        price *= 1.0002
        rows.append({"date": date.date().isoformat(), "adjusted_close": price})
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
