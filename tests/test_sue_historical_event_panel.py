from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_historical_panel import (
    SueHistoricalPanelConfig,
    build_sue_historical_event_panel,
    render_sue_historical_event_panel_report,
    write_sue_historical_panel_artifacts,
)
from portfolio_os.alpha.sue_historical_schema import (
    SUE_HISTORICAL_EVENT_COLUMNS,
    SueHistoricalEventRow,
    validate_no_forward_return_feature_columns,
    validate_sue_historical_report_language,
)


def _event_payload() -> dict[str, object]:
    return {
        "event_id": "SUE-HIST-AAPL-2020Q1",
        "symbol": "AAPL",
        "permno": 14593,
        "ibes_ticker": "AAPL",
        "cusip": "03783310",
        "fiscal_period": "2020Q1",
        "announcement_date": "2020-04-30",
        "event_available_timestamp": "2020-04-30T21:15:00Z",
        "tradable_timestamp": "2020-05-01T14:30:00Z",
        "rebalance_date": "2020-05-01",
        "actual_eps": 0.64,
        "expected_eps": 0.58,
        "sue_value": 0.06,
        "sue_definition": "actual_eps_minus_latest_pit_consensus",
        "estimate_snapshot_date": "2020-04-29",
        "price_anchor_date": "2020-05-01",
        "return_window_start": "2020-05-05",
        "return_window_end": "2020-06-02",
        "data_source": "WRDS_IBES_SMOKE_FIXTURE",
        "link_method": "ibes_idsum_cusip_sdates",
        "pit_safety_status": "pit_safe",
        "diagnostic_only": False,
        "fetched_at": "2026-05-06T00:00:00Z",
    }


def test_event_row_rejects_missing_event_available_timestamp() -> None:
    payload = _event_payload()
    payload.pop("event_available_timestamp")

    with pytest.raises(ValueError, match="event_available_timestamp"):
        SueHistoricalEventRow.model_validate(payload)


def test_event_row_rejects_pit_order_violations() -> None:
    payload = _event_payload()
    payload["event_available_timestamp"] = "2020-05-01T15:00:00Z"

    with pytest.raises(ValueError, match="event_available_timestamp"):
        SueHistoricalEventRow.model_validate(payload)

    payload = _event_payload()
    payload["estimate_snapshot_date"] = "2020-05-01"

    with pytest.raises(ValueError, match="estimate_snapshot_date"):
        SueHistoricalEventRow.model_validate(payload)


def test_missing_estimate_snapshot_requires_diagnostic_only() -> None:
    payload = _event_payload()
    payload["estimate_snapshot_date"] = None

    with pytest.raises(ValueError, match="diagnostic_only"):
        SueHistoricalEventRow.model_validate(payload)

    payload["diagnostic_only"] = True
    payload["pit_safety_status"] = "diagnostic_no_view"
    payload["expected_eps"] = None
    payload["sue_value"] = None
    row = SueHistoricalEventRow.model_validate(payload)
    assert row.expected_eps is None
    assert row.sue_value is None


def test_missing_expected_eps_cannot_be_encoded_as_zero_sue() -> None:
    payload = _event_payload()
    payload["diagnostic_only"] = True
    payload["pit_safety_status"] = "diagnostic_missing_estimate"
    payload["expected_eps"] = 0.0
    payload["sue_value"] = None

    with pytest.raises(ValueError, match="missing expected_eps"):
        SueHistoricalEventRow.model_validate(payload)


def test_fmp_frozen_estimates_are_rejected_as_pit_safe_substitute() -> None:
    payload = _event_payload()
    payload["data_source"] = "FMP_FROZEN_ESTIMATE_HISTORY"

    with pytest.raises(ValueError, match="FMP frozen estimate history"):
        SueHistoricalEventRow.model_validate(payload)


def test_forward_return_feature_columns_are_rejected() -> None:
    with pytest.raises(ValueError, match="forward-return"):
        validate_no_forward_return_feature_columns(["symbol", "event_available_timestamp", "fwd_ret_5d"])


def test_report_language_guard_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live alpha orders",
        "broker execution",
        "order generation",
        "real historical SUE alpha proven",
        "guaranteed tradable alpha",
        "auto trading",
        "investment recommendation",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_historical_report_language(f"The panel is {phrase}.")


def test_smoke_builder_writes_pit_labeled_outputs_without_overclaiming(tmp_path: Path) -> None:
    result = build_sue_historical_event_panel(
        SueHistoricalPanelConfig(mode="smoke", sample_event_count=60, fetched_at="2026-05-06T00:00:00Z")
    )

    assert result.event_count == 60
    assert result.rebalance_date_count >= 12
    assert result.coverage_report["linked_rows"] > 0
    assert result.coverage_report["unlinked_rows"] > 0
    assert result.coverage_report["missing_estimates"] > 0
    assert result.coverage_report["missing_actuals"] > 0
    assert result.coverage_report["missing_prices"] > 0
    assert result.coverage_report["diagnostic_only_rows"] > 0

    artifacts = write_sue_historical_panel_artifacts(
        result,
        output_dir=tmp_path,
        report_path=tmp_path / "sue_historical_event_panel_report.md",
    )

    events = pd.read_csv(artifacts["events"])
    assert list(events.columns) == SUE_HISTORICAL_EVENT_COLUMNS
    assert len(events) == 60
    assert not events["expected_eps"].fillna("").eq("0").any()
    missing_estimate_rows = events.loc[events["pit_safety_status"].eq("diagnostic_missing_estimate")]
    assert not missing_estimate_rows.empty
    assert missing_estimate_rows["diagnostic_only"].astype(bool).all()
    assert missing_estimate_rows["expected_eps"].isna().all()

    manifest = json.loads(artifacts["data_lineage_manifest"].read_text(encoding="utf-8"))
    assert manifest["data_source"] == "WRDS_IBES_CRSP"
    assert manifest["query_timestamp"]
    assert {"ibes.actu_epsus", "ibes.statsum_epsus", "crsp.dsf"}.issubset(
        set(manifest["source_table_names"])
    )
    assert manifest["production_approval_claimed"] is False

    coverage = json.loads(artifacts["coverage_report"].read_text(encoding="utf-8"))
    for key in [
        "linked_rows",
        "unlinked_rows",
        "missing_estimates",
        "missing_actuals",
        "missing_prices",
        "diagnostic_only_rows",
    ]:
        assert key in coverage

    report = artifacts["report"].read_text(encoding="utf-8")
    assert "This is a WRDS PIT-safe or PIT-labeled SUE event panel builder." in report
    assert "It does not prove SUE alpha success by itself." in report
    assert "Downstream typed event evidence and Q2 optimizer-path evaluation require separate explicit reopen phases." in report
    validate_sue_historical_report_language(report)


def test_full_mode_joins_local_wrds_extracts_with_pit_filters(tmp_path: Path) -> None:
    earnings_path = tmp_path / "ibes_actuals.csv"
    estimates_path = tmp_path / "ibes_estimates.csv"
    links_path = tmp_path / "ibes_links.csv"
    prices_path = tmp_path / "crsp_daily.csv"

    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-04-30",
                "event_available_timestamp": "2020-04-30T21:15:00Z",
                "actual_eps": 0.64,
            },
            {
                "symbol": "MSFT",
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-04-30",
                "event_available_timestamp": "2020-04-30T21:15:00Z",
                "actual_eps": 1.20,
            },
        ]
    ).to_csv(earnings_path, index=False)
    pd.DataFrame(
        [
            {
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2020Q1",
                "estimate_snapshot_date": "2020-04-29",
                "expected_eps": 0.58,
            },
            {
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2020Q1",
                "estimate_snapshot_date": "2020-05-02",
                "expected_eps": 0.99,
            },
        ]
    ).to_csv(estimates_path, index=False)
    pd.DataFrame(
        [
            {
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "permno": 14593,
                "permco": 7,
                "link_method": "ibes_idsum_cusip_sdates",
                "link_start_date": "2010-01-01",
                "link_end_date": "2022-12-31",
                "link_validity_flag": True,
            },
            {
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "permno": 10107,
                "permco": 8,
                "link_method": "ibes_idsum_cusip_sdates",
                "link_start_date": "2021-01-01",
                "link_end_date": "2022-12-31",
                "link_validity_flag": True,
            },
        ]
    ).to_csv(links_path, index=False)
    price_rows = []
    for permno in [14593, 10107]:
        for date in pd.bdate_range("2020-05-01", periods=25):
            price_rows.append({"permno": permno, "date": date.date().isoformat(), "prc": 100.0, "ret": 0.001})
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)

    result = build_sue_historical_event_panel(
        SueHistoricalPanelConfig(
            mode="full",
            earnings_events_path=str(earnings_path),
            estimate_snapshots_path=str(estimates_path),
            security_links_path=str(links_path),
            crsp_daily_path=str(prices_path),
            fetched_at="2026-05-06T00:00:00Z",
        )
    )

    assert result.event_count == 2
    by_symbol = {row.symbol: row for row in result.event_rows}
    assert by_symbol["AAPL"].expected_eps == pytest.approx(0.58)
    assert by_symbol["AAPL"].sue_value == pytest.approx(0.06)
    assert by_symbol["AAPL"].pit_safety_status == "pit_safe"
    assert by_symbol["MSFT"].diagnostic_only is True
    assert by_symbol["MSFT"].pit_safety_status == "diagnostic_missing_estimate"
    assert result.coverage_report["linked_rows"] == 1
    assert result.coverage_report["unlinked_rows"] == 1
    assert result.coverage_report["missing_estimates"] == 1


def test_rendered_report_rejects_approval_claim_if_added() -> None:
    result = build_sue_historical_event_panel(SueHistoricalPanelConfig(mode="smoke", sample_event_count=12))
    report = render_sue_historical_event_panel_report(result)
    validate_sue_historical_report_language(report)
    with pytest.raises(ValueError):
        validate_sue_historical_report_language(report + "\nThis is paper ready.")
