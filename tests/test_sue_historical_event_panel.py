from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_crsp_price_extract import (
    SueCrspPriceExtractConfig,
    extract_crsp_prices_for_sue_links,
)
from portfolio_os.alpha.sue_historical_panel import (
    SueHistoricalPanelConfig,
    build_sue_historical_event_panel,
    build_sue_historical_coverage_rescue_report,
    load_sue_historical_panel_run_config,
    render_sue_historical_event_panel_report,
    render_sue_historical_panel_expansion_report,
    write_sue_historical_expansion_artifacts,
    write_sue_historical_missing_inputs_artifacts,
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
        "total_raw_events",
        "linked_rows",
        "unlinked_rows",
        "missing_estimates",
        "missing_estimate_snapshot_dates",
        "missing_actuals",
        "missing_prices",
        "diagnostic_only_rows",
        "final_pit_safe_rows",
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


def test_full_mode_missing_extracts_write_unavailable_audit_without_smoke_fallback(tmp_path: Path) -> None:
    output_dir = tmp_path / "full_outputs"
    report_path = tmp_path / "sue_historical_event_panel_full_report.md"
    config = SueHistoricalPanelConfig(
        mode="full",
        earnings_events_path=str(tmp_path / "missing_ibes_actuals.csv"),
        estimate_snapshots_path=str(tmp_path / "missing_ibes_estimates.csv"),
        security_links_path=str(tmp_path / "missing_ibes_links.csv"),
        crsp_daily_path=str(tmp_path / "missing_crsp_daily.csv"),
        fetched_at="2026-05-06T00:00:00Z",
    )

    artifacts = write_sue_historical_missing_inputs_artifacts(
        config,
        output_dir=output_dir,
        report_path=report_path,
    )

    assert artifacts["missing_inputs_report"].name == "missing_inputs_report.json"
    missing_report = json.loads(artifacts["missing_inputs_report"].read_text(encoding="utf-8"))
    assert missing_report["status"] == "unavailable"
    assert missing_report["mode"] == "full"
    assert missing_report["no_fake_panel_created"] is True
    assert missing_report["synthetic_historical_evidence_created"] is False
    assert missing_report["production_approval_claimed"] is False
    assert len(missing_report["missing_inputs"]) == 4
    assert {item["source_table"] for item in missing_report["missing_inputs"]} == {
        "ibes.actu_epsus",
        "ibes.statsum_epsus",
        "ibes.idsum",
        "crsp.dsf",
    }
    assert not (output_dir / "events.csv").exists()
    assert not (output_dir / "sue_values.csv").exists()

    report = report_path.read_text(encoding="utf-8")
    assert "Full WRDS SUE event panel unavailable" in report
    assert "No smoke fixture or synthetic historical evidence was substituted." in report
    assert "It does not prove SUE alpha success by itself." in report
    validate_sue_historical_report_language(report)


def test_full_mode_yaml_config_loads_output_paths_and_missing_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "wrds_sue_event_panel_full.yaml"
    output_dir = tmp_path / "configured_full_outputs"
    report_path = tmp_path / "configured_report.md"
    config_path.write_text(
        "\n".join(
            [
                "schema_version: wrds_sue_event_panel_full_config.v1",
                "mode: full",
                "fetched_at: '2026-05-06T00:00:00Z'",
                "inputs:",
                f"  earnings_events_path: {tmp_path / 'missing_actuals.csv'}",
                f"  estimate_snapshots_path: {tmp_path / 'missing_estimates.csv'}",
                f"  security_links_path: {tmp_path / 'missing_links.csv'}",
                f"  crsp_daily_path: {tmp_path / 'missing_prices.csv'}",
                "outputs:",
                f"  output_dir: {output_dir}",
                f"  report_path: {report_path}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_config = load_sue_historical_panel_run_config(config_path)

    assert run_config.panel_config.mode == "full"
    assert run_config.panel_config.earnings_events_path == str(tmp_path / "missing_actuals.csv")
    assert run_config.output_dir == str(output_dir)
    assert run_config.report_path == str(report_path)


def test_expanded_mode_config_loads_date_window_and_max_events(tmp_path: Path) -> None:
    config_path = tmp_path / "wrds_sue_event_panel_expanded.yaml"
    output_dir = tmp_path / "expanded_outputs"
    report_path = tmp_path / "expanded_report.md"
    config_path.write_text(
        "\n".join(
            [
                "schema_version: wrds_sue_event_panel_expanded_config.v1",
                "mode: full",
                "start_date: '2020-02-01'",
                "end_date: '2020-03-31'",
                "max_events: 50",
                "sampled: true",
                "sample_seed: 7",
                "fetched_at: '2026-05-06T00:00:00Z'",
                "inputs:",
                f"  earnings_events_path: {tmp_path / 'actuals.csv'}",
                f"  estimate_snapshots_path: {tmp_path / 'estimates.csv'}",
                f"  security_links_path: {tmp_path / 'links.csv'}",
                f"  crsp_daily_path: {tmp_path / 'prices.csv'}",
                "outputs:",
                f"  output_dir: {output_dir}",
                f"  report_path: {report_path}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_config = load_sue_historical_panel_run_config(config_path)

    assert run_config.panel_config.start_date == "2020-02-01"
    assert run_config.panel_config.end_date == "2020-03-31"
    assert run_config.panel_config.max_events == 50
    assert run_config.panel_config.sampled is True
    assert run_config.panel_config.sample_seed == 7


def test_expanded_panel_filters_date_window_and_writes_rescue_artifacts(tmp_path: Path) -> None:
    earnings_path = tmp_path / "actuals.csv"
    estimates_path = tmp_path / "estimates.csv"
    links_path = tmp_path / "links.csv"
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "expanded"
    report_path = tmp_path / "expanded_report.md"

    pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-01-15",
                "event_available_timestamp": "2020-01-15T21:15:00Z",
                "actual_eps": 0.65,
            },
            {
                "symbol": "MSFT",
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-02-20",
                "event_available_timestamp": "2020-02-20T21:15:00Z",
                "actual_eps": 1.20,
            },
            {
                "symbol": "NOEST",
                "ibes_ticker": "NOEST",
                "cusip": "11111111",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-03-10",
                "event_available_timestamp": "2020-03-10T21:15:00Z",
                "actual_eps": 1.00,
            },
            {
                "symbol": "NOPRICE",
                "ibes_ticker": "NOPR",
                "cusip": "22222222",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-03-11",
                "event_available_timestamp": "2020-03-11T21:15:00Z",
                "actual_eps": 1.00,
            },
        ]
    ).to_csv(earnings_path, index=False)
    pd.DataFrame(
        [
            {
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "fiscal_period": "2020Q1",
                "estimate_snapshot_date": "2020-01-14",
                "expected_eps": 0.60,
            },
            {
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "fiscal_period": "2020Q1",
                "estimate_snapshot_date": "2020-02-19",
                "expected_eps": 1.10,
            },
            {
                "ibes_ticker": "NOPR",
                "cusip": "22222222",
                "fiscal_period": "2020Q1",
                "estimate_snapshot_date": "2020-03-10",
                "expected_eps": 0.95,
            },
        ]
    ).to_csv(estimates_path, index=False)
    pd.DataFrame(
        [
            {
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "permno": 10107,
                "link_method": "ibes_idsum_cusip_sdates",
                "link_start_date": "2010-01-01",
                "link_end_date": "2022-12-31",
                "link_validity_flag": True,
            },
            {
                "ibes_ticker": "NOPR",
                "cusip": "22222222",
                "permno": 22222,
                "link_method": "ibes_idsum_cusip_sdates",
                "link_start_date": "2010-01-01",
                "link_end_date": "2022-12-31",
                "link_validity_flag": True,
            },
        ]
    ).to_csv(links_path, index=False)
    price_rows = [
        {"permno": 10107, "date": date.date().isoformat(), "prc": 100.0, "ret": 0.001}
        for date in pd.bdate_range("2020-02-21", periods=25)
    ]
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)

    result = build_sue_historical_event_panel(
        SueHistoricalPanelConfig(
            mode="full",
            start_date="2020-02-01",
            end_date="2020-03-31",
            max_events=3,
            earnings_events_path=str(earnings_path),
            estimate_snapshots_path=str(estimates_path),
            security_links_path=str(links_path),
            crsp_daily_path=str(prices_path),
            fetched_at="2026-05-06T00:00:00Z",
        )
    )
    artifacts = write_sue_historical_expansion_artifacts(result, output_dir=output_dir, report_path=report_path)

    assert result.event_count == 3
    assert {row.symbol for row in result.event_rows} == {"MSFT", "NOEST", "NOPRICE"}
    rescue = json.loads(artifacts["coverage_rescue_report"].read_text(encoding="utf-8"))
    assert rescue["missing_expected_eps"] == 1
    assert rescue["missing_price_rows"] == 1
    assert rescue["unlinked_ibes_crsp_rows"] == 1
    assert rescue["missing_coverage_encoded_as_zero_alpha"] is False
    linkage = pd.read_csv(artifacts["linkage_failure_report"])
    assert set(linkage["symbol"]) == {"NOEST"}
    missing_prices = pd.read_csv(artifacts["missing_price_report"])
    assert set(missing_prices["symbol"]) == {"NOPRICE"}
    report = artifacts["report"].read_text(encoding="utf-8")
    assert "This is expanded WRDS/PIT historical evidence, not production approval." in report
    validate_sue_historical_report_language(report)


def test_coverage_rescue_report_counts_diagnostic_rows_without_zero_alpha() -> None:
    result = build_sue_historical_event_panel(SueHistoricalPanelConfig(mode="smoke", sample_event_count=60))

    rescue = build_sue_historical_coverage_rescue_report(result)

    assert rescue["diagnostic_only_rows"] > 0
    assert rescue["missing_expected_eps"] > 0
    assert rescue["missing_coverage_encoded_as_zero_alpha"] is False
    assert rescue["q2_evaluation_ran"] is False
    assert rescue["production_approval_claimed"] is False
    report = render_sue_historical_panel_expansion_report(result, rescue)
    assert "This does not run Q2 or optimizer-path evaluation." in report
    validate_sue_historical_report_language(report)


class _FakeWrdsConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def raw_sql(self, query: str) -> pd.DataFrame:
        self.queries.append(query)
        if "10001" in query:
            return pd.DataFrame(
                [
                    {"permno": 10001, "date": "2020-01-02", "prc": 10.0, "ret": 0.01},
                    {"permno": 10001, "date": "2020-01-03", "prc": 10.5, "ret": 0.02},
                ]
            )
        if "10002" in query:
            return pd.DataFrame(
                [
                    {"permno": 10002, "date": "2020-01-02", "prc": 20.0, "ret": -0.01},
                ]
            )
        raise AssertionError(f"unexpected query: {query}")


class _FailingWrdsConnection:
    def raw_sql(self, query: str) -> pd.DataFrame:
        raise AssertionError(f"resumable extract should not query completed chunks: {query}")


def test_crsp_price_extract_writes_resumable_chunks_and_manifest(tmp_path: Path) -> None:
    links_path = tmp_path / "ibes_links.csv"
    output_path = tmp_path / "crsp_daily.csv"
    chunk_dir = tmp_path / "chunks"
    manifest_path = tmp_path / "crsp_price_extract_manifest.json"
    pd.DataFrame(
        [
            {"permno": 10001, "link_validity_flag": True},
            {"permno": 10002, "link_validity_flag": True},
            {"permno": 10001, "link_validity_flag": True},
        ]
    ).to_csv(links_path, index=False)

    result = extract_crsp_prices_for_sue_links(
        SueCrspPriceExtractConfig(
            links_path=str(links_path),
            output_path=str(output_path),
            chunk_dir=str(chunk_dir),
            manifest_path=str(manifest_path),
            start_date="2020-01-01",
            end_date="2020-01-31",
            chunk_size=1,
            max_permnos=2,
            fetched_at="2026-05-06T00:00:00Z",
        ),
        connection=_FakeWrdsConnection(),
    )

    assert result["status"] == "completed"
    assert result["queried_chunks"] == 2
    assert result["skipped_chunks"] == 0
    assert result["distinct_permnos"] == 2
    assert result["row_count"] == 3
    prices = pd.read_csv(output_path)
    assert set(prices["permno"]) == {10001, 10002}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_table"] == "crsp.dsf"
    assert manifest["resumable"] is True
    assert manifest["production_approval_claimed"] is False
    assert manifest["broker_order_workflow_added"] is False


def test_crsp_price_extract_skips_existing_chunks_on_rerun(tmp_path: Path) -> None:
    links_path = tmp_path / "ibes_links.csv"
    output_path = tmp_path / "crsp_daily.csv"
    chunk_dir = tmp_path / "chunks"
    manifest_path = tmp_path / "crsp_price_extract_manifest.json"
    pd.DataFrame([{"permno": 10001}, {"permno": 10002}]).to_csv(links_path, index=False)
    config = SueCrspPriceExtractConfig(
        links_path=str(links_path),
        output_path=str(output_path),
        chunk_dir=str(chunk_dir),
        manifest_path=str(manifest_path),
        start_date="2020-01-01",
        end_date="2020-01-31",
        chunk_size=1,
        max_permnos=2,
        fetched_at="2026-05-06T00:00:00Z",
    )

    first = extract_crsp_prices_for_sue_links(config, connection=_FakeWrdsConnection())
    second = extract_crsp_prices_for_sue_links(config, connection=_FailingWrdsConnection())

    assert first["queried_chunks"] == 2
    assert second["queried_chunks"] == 0
    assert second["skipped_chunks"] == 2
    assert second["row_count"] == 3


def test_rendered_report_rejects_approval_claim_if_added() -> None:
    result = build_sue_historical_event_panel(SueHistoricalPanelConfig(mode="smoke", sample_event_count=12))
    report = render_sue_historical_event_panel_report(result)
    validate_sue_historical_report_language(report)
    with pytest.raises(ValueError):
        validate_sue_historical_report_language(report + "\nThis is paper ready.")
