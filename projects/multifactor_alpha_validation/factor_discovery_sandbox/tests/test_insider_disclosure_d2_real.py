from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_d2_real import run_real_form4_observability


def test_real_form4_observability_writes_unavailable_when_source_missing(tmp_path: Path) -> None:
    result = run_real_form4_observability(
        source_dir=tmp_path / "missing_sec_archive",
        output_dir=tmp_path / "d2_real",
    )

    assert result.summary["schema_version"] == "insider_disclosure_d2_real_summary.v1"
    assert result.summary["stage"] == "D2-INSIDER-01R"
    assert result.summary["real_data_status"] == "unavailable_missing_source"
    assert result.summary["event_count"] == 0
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    missing = json.loads(result.artifacts["missing_inputs_report"].read_text(encoding="utf-8"))
    assert missing["missing_inputs"] == ["source_dir"]
    assert result.artifacts["d2_observability_summary_real"].exists()


def test_real_form4_observability_parses_local_xml_and_replays_d2(tmp_path: Path) -> None:
    source_dir = _write_local_form4_archive(tmp_path / "sec_archive")
    market_path = _write_market_join_fixture(tmp_path / "market_join.csv")

    result = run_real_form4_observability(
        source_dir=source_dir,
        market_data_path=market_path,
        output_dir=tmp_path / "d2_real",
    )

    assert result.summary["real_data_status"] == "local_form4_replay_complete"
    assert result.summary["network_used"] is False
    assert result.summary["event_count"] == 6
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["overall_decision"] in {"hold_insufficient_sample", "observable"}

    registry = pd.read_csv(result.artifacts["insider_event_registry_real"])
    assert set(registry["event_subset"]) == {
        "open_market_buy",
        "discretionary_sell",
        "planned_sell",
        "compensation_control",
        "unknown_no_view",
    }
    assert registry.loc[registry["event_subset"] == "unknown_no_view", "no_view_reason"].ne("").all()
    assert registry.loc[registry["transaction_code"] == "P", "event_subset"].eq("open_market_buy").all()
    assert registry.loc[registry["transaction_code"] == "A", "event_subset"].eq("compensation_control").all()

    market_join = pd.read_csv(result.artifacts["insider_event_market_join"])
    assert market_join["coverage_state"].eq("covered").sum() == 4
    assert market_join["coverage_state"].eq("no_view").sum() == 2
    assert market_join.loc[market_join["ticker"] == "MISS", "no_view_reason"].iloc[0] == "missing_market_join"
    assert (
        market_join.loc[market_join["ticker"] == "UNKN", "no_view_reason"].iloc[0]
        == "unknown_post_2023_plan_flag"
    )

    parse_coverage = pd.read_csv(result.artifacts["form4_xml_parse_coverage"])
    assert parse_coverage["parsed_transaction_count"].sum() == 6
    assert parse_coverage["parse_status"].eq("parsed").all()

    summary = json.loads(result.artifacts["d2_observability_summary_real"].read_text(encoding="utf-8"))
    assert summary["stage"] == "D2-INSIDER-01R"
    assert summary["source_type"] == "local_sec_archive"
    assert summary["formula_score_written"] is False
    assert summary["alpha_registry_update_allowed"] is False

    report = result.artifacts["d2_insider_disclosure_observability_report_real"].read_text(encoding="utf-8").lower()
    assert "real form 4 extraction replay" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
    ]:
        assert forbidden not in report


def test_real_form4_observability_reads_nested_sec_archive_request_specs_and_html(tmp_path: Path) -> None:
    source_dir = _write_nested_sec_rendered_archive(tmp_path / "sec_rendered_archive")
    market_path = _write_market_join_fixture(tmp_path / "market_join.csv")

    result = run_real_form4_observability(
        source_dir=source_dir,
        market_data_path=market_path,
        output_dir=tmp_path / "d2_real_rendered",
    )

    assert result.summary["real_data_status"] == "local_form4_replay_complete"
    assert result.summary["network_used"] is False
    assert result.summary["event_count"] == 3
    assert result.summary["parsed_file_count"] == 3
    assert result.summary["formula_score_written"] is False
    assert result.summary["q2_entry_allowed"] is False

    source_manifest = json.loads(result.artifacts["form4_source_manifest"].read_text(encoding="utf-8"))
    assert source_manifest["source_layout"] == "sec_filing_archive_request_specs"
    assert source_manifest["indexed_file_count"] == 3

    registry = pd.read_csv(result.artifacts["insider_event_registry_real"])
    assert set(registry["event_subset"]) == {"open_market_buy", "discretionary_sell", "planned_sell"}
    assert registry["filing_accepted_ts"].str.startswith("2023-04-").all()
    assert not registry["filing_accepted_ts"].str.contains("1970").any()
    assert registry.loc[registry["transaction_code"] == "P", "event_subset"].iloc[0] == "open_market_buy"
    assert registry.loc[registry["rule_10b5_1_flag"].astype(str).str.lower() == "true", "event_subset"].iloc[0] == "planned_sell"

    parse_coverage = pd.read_csv(result.artifacts["form4_xml_parse_coverage"])
    assert parse_coverage["parse_status"].eq("parsed").all()
    assert set(parse_coverage["source_format"]) == {"sec_rendered_html_form4"}


def test_real_form4_observability_supports_batched_replay(tmp_path: Path) -> None:
    source_dir = _write_nested_sec_rendered_archive(tmp_path / "sec_rendered_archive")

    result = run_real_form4_observability(
        source_dir=source_dir,
        output_dir=tmp_path / "d2_real_batch",
        start_offset=1,
        max_files=1,
    )

    assert result.summary["event_count"] == 1
    assert result.summary["source_index_total_count"] == 3
    assert result.summary["source_index_start_offset"] == 1
    assert result.summary["source_index_max_files"] == 1

    source_manifest = json.loads(result.artifacts["form4_source_manifest"].read_text(encoding="utf-8"))
    assert source_manifest["source_index_total_count"] == 3
    assert source_manifest["indexed_file_count"] == 1
    assert source_manifest["source_index_start_offset"] == 1
    assert source_manifest["source_index_max_files"] == 1

    registry = pd.read_csv(result.artifacts["insider_event_registry_real"])
    assert registry["ticker"].tolist() == ["DISC"]


def test_real_form4_market_join_derives_daily_price_volume_controls(tmp_path: Path) -> None:
    source_dir = _write_nested_sec_rendered_archive(tmp_path / "sec_rendered_archive")
    market_path = _write_daily_market_join_fixture(tmp_path / "daily_market.csv")

    result = run_real_form4_observability(
        source_dir=source_dir,
        market_data_path=market_path,
        output_dir=tmp_path / "d2_real_daily_market",
    )

    joined = pd.read_csv(result.artifacts["insider_event_market_join"]).fillna("")
    buy_row = joined[joined["ticker"] == "BUY"].iloc[0]
    assert buy_row["coverage_state"] == "covered"
    assert buy_row["no_view_reason"] == ""
    assert buy_row["market_cap_at_event"] == 1_250_000_000
    assert buy_row["adv_20d"] > 0
    assert buy_row["spread_proxy"] == 0.0011
    assert buy_row["sector"] == "technology"
    assert buy_row["size_bucket"] in {"small", "mid", "large"}
    assert buy_row["liquidity_bucket"] in {"low", "medium", "high"}

    audit = pd.read_csv(result.artifacts["market_join_audit"])
    assert audit["status"].iloc[0] == "joined_daily_price_volume"
    assert audit["covered_count"].iloc[0] == 3


def test_real_form4_d2_blocks_d3_when_real_market_coverage_is_missing(tmp_path: Path) -> None:
    source_dir = _write_large_local_form4_archive(tmp_path / "large_sec_archive")

    result = run_real_form4_observability(
        source_dir=source_dir,
        output_dir=tmp_path / "d2_real_no_market",
    )

    assert result.summary["event_count"] == 660
    assert result.summary["real_d2_gate"]["open_market_buy"]["covered_count"] == 0
    assert result.summary["overall_decision"] == "blocked_data_coverage"
    assert result.summary["allow_d3_charter_for"] == []
    assert (
        result.summary["subset_decisions"]["open_market_buy"]["decision"]
        == "blocked_data_coverage"
    )


def _write_local_form4_archive(root: Path) -> Path:
    root.mkdir(parents=True)
    rows = []
    specs = [
        ("buy.xml", "0000000000-23-000001", "1001", "BUY", "P", "A", "", ""),
        ("disc_sell.xml", "0000000000-23-000002", "1002", "DISC", "S", "D", "false", ""),
        ("planned_sell.xml", "0000000000-23-000003", "1003", "PLAN", "S", "D", "true", "2023-01-05"),
        ("grant.xml", "0000000000-23-000004", "1004", "GRNT", "A", "A", "", ""),
        ("missing_market.xml", "0000000000-23-000005", "1005", "MISS", "P", "A", "", ""),
        ("unknown_plan_flag.xml", "0000000000-23-000006", "1006", "UNKN", "S", "D", "", ""),
    ]
    for idx, (filename, accession, cik, ticker, code, acquired_disposed, plan_flag, plan_date) in enumerate(specs):
        accepted = f"2023-04-{3 + idx:02d}T13:30:00Z"
        (root / filename).write_text(
            _ownership_xml(cik, ticker, accession, code, acquired_disposed, plan_flag, plan_date),
            encoding="utf-8",
        )
        rows.append(
            {
                "file": filename,
                "accession_number": accession,
                "filing_accepted_ts": accepted,
                "source_table_name": "local_test_sec_ownership_xml",
            },
        )
    pd.DataFrame(rows).to_csv(root / "form4_source_index.csv", index=False)
    return root


def _write_nested_sec_rendered_archive(root: Path) -> Path:
    specs = [
        ("BUY", "0000000001-23-000001", "2023-04-03 16:01:02", "P", "A", False),
        ("DISC", "0000000002-23-000002", "2023-04-04 18:01:02", "S", "D", False),
        ("PLAN", "0000000003-23-000003", "2023-04-05 18:01:02", "S", "D", True),
    ]
    request_rows = []
    for ticker, accession, accepted, code, acquired_disposed, plan_checked in specs:
        relative_path = f"documents/{ticker}/4/{accepted[:10]}_{accession}/primary_form4.xml"
        document_path = root / relative_path
        document_path.parent.mkdir(parents=True, exist_ok=True)
        document_path.write_text(
            _rendered_form4_html(ticker=ticker, code=code, acquired_disposed=acquired_disposed, plan_checked=plan_checked),
            encoding="utf-8",
        )
        request_rows.append(
            {
                "acceptedDate": accepted,
                "cik": f"000{ticker}",
                "doc_type": "primary",
                "filingDate": f"{accepted[:10]} 00:00:00",
                "finalLink": "https://www.sec.gov/Archives/edgar/data/example/xslF345X05/form4.xml",
                "formType": "4",
                "link": "https://www.sec.gov/Archives/edgar/data/example/example-index.htm",
                "relative_path": relative_path,
                "source_metadata_path": f"payloads/{ticker}/sec_filings_symbol.json",
                "symbol": ticker,
                "url": "https://www.sec.gov/Archives/edgar/data/example/xslF345X05/form4.xml",
            },
        )
    (root / "request_specs.json").write_text(json.dumps(request_rows, indent=2), encoding="utf-8")
    return root


def _write_market_join_fixture(path: Path) -> Path:
    pd.DataFrame(
        [
            {"ticker": "BUY", "market_cap_at_event": 1_000_000_000, "adv_20d": 2_000_000, "spread_proxy": 0.001, "sector": "tech", "size_bucket": "mid", "liquidity_bucket": "high"},
            {"ticker": "DISC", "market_cap_at_event": 2_000_000_000, "adv_20d": 3_000_000, "spread_proxy": 0.0012, "sector": "health", "size_bucket": "large", "liquidity_bucket": "high"},
            {"ticker": "PLAN", "market_cap_at_event": 900_000_000, "adv_20d": 1_000_000, "spread_proxy": 0.0015, "sector": "industrial", "size_bucket": "mid", "liquidity_bucket": "medium"},
            {"ticker": "GRNT", "market_cap_at_event": 700_000_000, "adv_20d": 800_000, "spread_proxy": 0.0020, "sector": "finance", "size_bucket": "small", "liquidity_bucket": "medium"},
            {"ticker": "UNKN", "market_cap_at_event": 600_000_000, "adv_20d": 700_000, "spread_proxy": 0.0025, "sector": "finance", "size_bucket": "small", "liquidity_bucket": "low"},
        ],
    ).to_csv(path, index=False)
    return path


def _write_daily_market_join_fixture(path: Path) -> Path:
    rows = []
    specs = [
        ("BUY", "technology", 1_000_000_000, 0.0010),
        ("DISC", "healthcare", 2_000_000_000, 0.0014),
        ("PLAN", "industrials", 900_000_000, 0.0018),
    ]
    for ticker, sector, base_market_cap, spread in specs:
        for day_offset, date in enumerate(pd.date_range("2023-03-20", "2023-04-05", freq="B")):
            rows.append(
                {
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": 10.0 + day_offset,
                    "volume": 100_000 + day_offset * 1_000,
                    "market_cap": base_market_cap + day_offset * 25_000_000,
                    "dollar_volume": (10.0 + day_offset) * (100_000 + day_offset * 1_000),
                    "bid_ask_spread": spread + day_offset * 0.00001,
                    "sector": sector,
                },
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_large_local_form4_archive(root: Path) -> Path:
    root.mkdir(parents=True)
    rows = []
    specs: list[tuple[str, str, str, str]] = []
    specs.extend((f"BUY{i:04d}", "P", "A", "") for i in range(360))
    specs.extend((f"DISC{i:04d}", "S", "D", "false") for i in range(150))
    specs.extend((f"PLAN{i:04d}", "S", "D", "true") for i in range(150))
    for idx, (ticker, code, acquired_disposed, plan_flag) in enumerate(specs):
        accepted_month = pd.Timestamp("2023-04-03") + pd.DateOffset(months=idx % 24)
        accepted = accepted_month.replace(day=(idx % 20) + 1).strftime("%Y-%m-%dT13:30:00Z")
        accession = f"0000000000-23-{idx:06d}"
        filename = f"{ticker.lower()}.xml"
        (root / filename).write_text(
            _ownership_xml(str(100000 + idx), ticker, accession, code, acquired_disposed, plan_flag, "2023-01-05"),
            encoding="utf-8",
        )
        rows.append(
            {
                "file": filename,
                "accession_number": accession,
                "filing_accepted_ts": accepted,
                "source_table_name": "local_large_sec_ownership_xml",
            },
        )
    pd.DataFrame(rows).to_csv(root / "form4_source_index.csv", index=False)
    return root


def _rendered_form4_html(ticker: str, code: str, acquired_disposed: str, plan_checked: bool) -> str:
    plan_marker = "X" if plan_checked else ""
    return f"""<!DOCTYPE html>
<html>
<body>SEC Form 4
<table>
  <tr><td>FORM 4</td><td>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</td></tr>
  <tr><td>{plan_marker}</td><td>Check this box to indicate that a transaction was made pursuant to a contract, instruction or written plan for the purchase or sale of equity securities of the issuer that is intended to satisfy the affirmative defense conditions of Rule 10b5-1(c).</td></tr>
</table>
<table>
  <tr>
    <td>1. Name and Address of Reporting Person* Example Owner</td>
    <td>2. Issuer Name and Ticker or Trading Symbol Example Inc. [ {ticker} ]</td>
    <td>5. Relationship of Reporting Person(s) to Issuer X Director X Officer (give title below) Chief Financial Officer</td>
  </tr>
</table>
<table>
<thead>
<tr><th colspan="11">Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned</th></tr>
<tr>
<th>1. Title of Security (Instr. 3)</th>
<th>2. Transaction Date (Month/Day/Year)</th>
<th>2A. Deemed Execution Date</th>
<th>Code</th>
<th>V</th>
<th>Amount</th>
<th>(A) or (D)</th>
<th>Price</th>
<th>5. Amount of Securities Beneficially Owned Following Reported Transaction(s)</th>
<th>6. Ownership Form: Direct (D) or Indirect (I)</th>
<th>7. Nature of Indirect Beneficial Ownership</th>
</tr>
</thead>
<tbody>
<tr>
<td>Common Stock</td>
<td>04/03/2023</td>
<td></td>
<td>{code}</td>
<td></td>
<td>1,000</td>
<td>{acquired_disposed}</td>
<td>12.50</td>
<td>10,000</td>
<td>D</td>
<td></td>
</tr>
</tbody>
</table>
</body>
</html>"""


def _ownership_xml(
    issuer_cik: str,
    ticker: str,
    accession: str,
    code: str,
    acquired_disposed: str,
    plan_flag: str,
    plan_date: str,
) -> str:
    plan_fields = ""
    if plan_flag:
        plan_fields = f"""
        <rule10b5-1>{plan_flag}</rule10b5-1>
        <planAdoptionDate>{plan_date}</planAdoptionDate>
        """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ownershipDocument>
  <schemaVersion>X0508</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2023-04-03</periodOfReport>
  <issuer>
    <issuerCik>{issuer_cik}</issuerCik>
    <issuerTradingSymbol>{ticker}</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>200{issuer_cik}</rptOwnerCik>
      <rptOwnerName>Example Owner {ticker}</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
      <isTenPercentOwner>0</isTenPercentOwner>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2023-04-03</value></transactionDate>
      <transactionCoding>
        <transactionCode>{code}</transactionCode>
        {plan_fields}
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>{acquired_disposed}</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>10000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <remarks>accession {accession}</remarks>
</ownershipDocument>
"""
