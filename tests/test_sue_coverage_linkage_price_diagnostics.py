from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_coverage_diagnostics import (
    SueCoverageDiagnosticsConfig,
    build_sue_coverage_linkage_price_diagnostics,
    validate_sue_coverage_diagnostics_report_language,
    write_sue_coverage_linkage_price_diagnostics_artifacts,
)


def _write_fixture_inputs(tmp_path: Path) -> dict[str, Path]:
    events_path = tmp_path / "events.csv"
    coverage_path = tmp_path / "coverage_rescue_report.json"
    linkage_path = tmp_path / "linkage_failure_report.csv"
    missing_price_path = tmp_path / "missing_price_report.csv"
    crsp_path = tmp_path / "crsp_daily.csv"

    pd.DataFrame(
        [
            {
                "event_id": "SUE-AAPL-20200110",
                "symbol": "AAPL",
                "permno": 1,
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2019Q4",
                "announcement_date": "2020-01-10",
                "event_available_timestamp": "2020-01-10T21:15:00Z",
                "tradable_timestamp": "2020-01-13T14:30:00Z",
                "rebalance_date": "2020-01-13",
                "actual_eps": 1.0,
                "expected_eps": 0.9,
                "sue_value": 0.1,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": "2020-01-09",
                "price_anchor_date": "2020-01-13",
                "return_window_start": "2020-01-15",
                "return_window_end": "2020-02-12",
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "ibes_idsum_cusip_sdates",
                "pit_safety_status": "pit_safe",
                "diagnostic_only": False,
                "fetched_at": "2026-05-06T00:00:00Z",
            },
            {
                "event_id": "SUE-MSFT-20200113",
                "symbol": "MSFT",
                "permno": None,
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "fiscal_period": "2019Q4",
                "announcement_date": "2020-01-13",
                "event_available_timestamp": "2020-01-13T21:15:00Z",
                "tradable_timestamp": "2020-01-14T14:30:00Z",
                "rebalance_date": "2020-01-14",
                "actual_eps": 1.0,
                "expected_eps": None,
                "sue_value": None,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": None,
                "price_anchor_date": None,
                "return_window_start": "2020-01-16",
                "return_window_end": "2020-02-13",
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "unlinked_ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_missing_estimate",
                "diagnostic_only": True,
                "fetched_at": "2026-05-06T00:00:00Z",
            },
            {
                "event_id": "SUE-NOPR-20200114",
                "symbol": "NOPR",
                "permno": 2,
                "ibes_ticker": "NOPR",
                "cusip": "22222222",
                "fiscal_period": "2019Q4",
                "announcement_date": "2020-01-14",
                "event_available_timestamp": "2020-01-14T21:15:00Z",
                "tradable_timestamp": "2020-01-15T14:30:00Z",
                "rebalance_date": "2020-01-15",
                "actual_eps": 1.0,
                "expected_eps": 0.8,
                "sue_value": 0.2,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": "2020-01-13",
                "price_anchor_date": None,
                "return_window_start": "2020-01-17",
                "return_window_end": "2020-02-14",
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_missing_price",
                "diagnostic_only": True,
                "fetched_at": "2026-05-06T00:00:00Z",
            },
            {
                "event_id": "SUE-LATE-20210104",
                "symbol": "LATE",
                "permno": 3,
                "ibes_ticker": "LATE",
                "cusip": "33333333",
                "fiscal_period": "2020Q4",
                "announcement_date": "2021-01-04",
                "event_available_timestamp": "2021-01-04T21:15:00Z",
                "tradable_timestamp": "2021-01-05T14:30:00Z",
                "rebalance_date": "2021-01-05",
                "actual_eps": 1.0,
                "expected_eps": 0.8,
                "sue_value": 0.2,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": "2021-01-03",
                "price_anchor_date": None,
                "return_window_start": "2021-01-07",
                "return_window_end": "2021-02-04",
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_missing_price",
                "diagnostic_only": True,
                "fetched_at": "2026-05-06T00:00:00Z",
            },
        ]
    ).to_csv(events_path, index=False)
    coverage_path.write_text(
        json.dumps(
            {
                "schema_version": "sue_historical_coverage_rescue_report.v1",
                "event_count": 4,
                "rebalance_date_count": 4,
                "missing_prices": 2,
                "missing_price_rows": 2,
                "missing_expected_eps": 1,
                "missing_actual_eps": 0,
                "missing_estimate_snapshot_date": 1,
                "unlinked_ibes_crsp_rows": 1,
                "invalid_link_date_rows": 0,
                "missing_return_windows": 3,
                "diagnostic_only_rows": 3,
                "pit_safe_rows": 1,
                "missing_coverage_encoded_as_zero_alpha": False,
                "q2_evaluation_ran": False,
                "optimizer_path_evaluation_ran": False,
                "alpha_registry_promoted": False,
                "production_approval_claimed": False,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "event_id": "SUE-MSFT-20200113",
                "symbol": "MSFT",
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "announcement_date": "2020-01-13",
                "link_method": "unlinked_ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_missing_estimate",
                "failure_reason": "unlinked",
            }
        ]
    ).to_csv(linkage_path, index=False)
    pd.DataFrame(
        [
            {
                "event_id": "SUE-NOPR-20200114",
                "symbol": "NOPR",
                "permno": 2,
                "announcement_date": "2020-01-14",
                "tradable_timestamp": "2020-01-15T14:30:00Z",
                "return_window_start": "2020-01-17",
                "return_window_end": "2020-02-14",
                "pit_safety_status": "diagnostic_missing_price",
                "failure_reason": "missing_price_or_return_window",
            },
            {
                "event_id": "SUE-LATE-20210104",
                "symbol": "LATE",
                "permno": 3,
                "announcement_date": "2021-01-04",
                "tradable_timestamp": "2021-01-05T14:30:00Z",
                "return_window_start": "2021-01-07",
                "return_window_end": "2021-02-04",
                "pit_safety_status": "diagnostic_missing_price",
                "failure_reason": "missing_price_or_return_window",
            },
        ]
    ).to_csv(missing_price_path, index=False)
    pd.DataFrame(
        [
            {"permno": 1, "date": "2020-01-13", "prc": 100.0, "ret": 0.01},
            {"permno": 3, "date": "2020-12-30", "prc": 50.0, "ret": -0.01},
        ]
    ).to_csv(crsp_path, index=False)
    return {
        "events": events_path,
        "coverage": coverage_path,
        "linkage": linkage_path,
        "missing_price": missing_price_path,
        "crsp": crsp_path,
    }


def test_builds_coverage_linkage_price_diagnostics(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    output_dir = tmp_path / "diagnostics"
    report_path = tmp_path / "diagnostics_report.md"

    result = build_sue_coverage_linkage_price_diagnostics(
        SueCoverageDiagnosticsConfig(
            events_path=str(paths["events"]),
            coverage_rescue_report_path=str(paths["coverage"]),
            linkage_failure_report_path=str(paths["linkage"]),
            missing_price_report_path=str(paths["missing_price"]),
            crsp_daily_path=str(paths["crsp"]),
            output_dir=str(output_dir),
            report_path=str(report_path),
        )
    )

    assert result.summary["event_count"] == 4
    assert result.summary["final_pit_safe_rows"] == 1
    assert result.summary["q2_evaluation_ran"] is False
    assert result.summary["optimizer_path_evaluation_ran"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.summary["recommended_next_action"] == "rescue_linkage_and_price_coverage_before_q2"
    assert result.summary["price_gap_classifications"]["permno_absent_from_crsp_cache"] == 1
    assert result.summary["price_gap_classifications"]["return_window_after_crsp_cache_end"] == 1
    assert "Coverage / Linkage / Price Diagnostics" in result.report_text
    assert "This does not run Q2 or optimizer-path evaluation." in result.report_text

    artifacts = write_sue_coverage_linkage_price_diagnostics_artifacts(result)
    assert artifacts["diagnostic_summary"].exists()
    assert artifacts["coverage_loss_waterfall"].exists()
    assert artifacts["linkage_loss_by_symbol"].exists()
    assert artifacts["price_cache_gap_report"].exists()
    summary = json.loads(artifacts["diagnostic_summary"].read_text(encoding="utf-8"))
    assert summary["price_gap_classifications"]["permno_absent_from_crsp_cache"] == 1
    waterfall = pd.read_csv(artifacts["coverage_loss_waterfall"])
    assert set(waterfall["stage"]).issuperset({"source_events", "unlinked_ibes_crsp_rows", "final_pit_safe_rows"})


def test_rejects_missing_expected_eps_encoded_as_zero_sue(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    events = pd.read_csv(paths["events"])
    mask = events["pit_safety_status"].eq("diagnostic_missing_estimate")
    events.loc[mask, "expected_eps"] = 0.0
    events.loc[mask, "sue_value"] = 0.0
    events.to_csv(paths["events"], index=False)

    with pytest.raises(ValueError, match="missing expected EPS"):
        build_sue_coverage_linkage_price_diagnostics(
            SueCoverageDiagnosticsConfig(
                events_path=str(paths["events"]),
                coverage_rescue_report_path=str(paths["coverage"]),
                linkage_failure_report_path=str(paths["linkage"]),
                missing_price_report_path=str(paths["missing_price"]),
                crsp_daily_path=str(paths["crsp"]),
            )
        )


def test_report_language_guard_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "real historical SUE alpha proven",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_coverage_diagnostics_report_language(f"Diagnostic says {phrase}.")
