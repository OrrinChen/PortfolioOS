from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_timestamp_source_extract import (
    SueTimestampSourceExtractConfig,
    extract_wrds_sue_timestamp_sources,
)
from portfolio_os.alpha.sue_timestamp_enrichment import (
    SueTimestampEnrichmentConfig,
    build_sue_timestamp_enrichment,
    validate_sue_timestamp_enrichment_report_language,
    write_sue_timestamp_enrichment_artifacts,
)


def _write_events(tmp_path: Path) -> Path:
    rows = []
    for idx, symbol in enumerate(["AAA", "BBB", "CCC"], start=1):
        announcement_date = f"2020-02-{10 + idx:02d}"
        tradable = str((pd.Timestamp(announcement_date) + pd.offsets.BDay(1)).date())
        rows.append(
            {
                "event_id": f"SUE-ENRICH-{symbol}",
                "symbol": symbol,
                "permno": 10000 + idx,
                "ibes_ticker": symbol,
                "cusip": f"{10000 + idx:08d}",
                "fiscal_period": "2019Q4",
                "announcement_date": announcement_date,
                "event_available_timestamp": f"{announcement_date}T21:15:00Z",
                "tradable_timestamp": f"{tradable}T14:30:00Z",
                "rebalance_date": tradable,
                "actual_eps": 1.0 + idx / 10.0,
                "expected_eps": 1.0,
                "sue_value": idx / 10.0,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": "2020-02-01",
                "price_anchor_date": tradable,
                "return_window_start": str((pd.Timestamp(tradable) + pd.offsets.BDay(2)).date()),
                "return_window_end": str((pd.Timestamp(tradable) + pd.offsets.BDay(22)).date()),
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "test_exact_link",
                "pit_safety_status": "pit_safe",
                "diagnostic_only": False,
                "fetched_at": "2026-05-08T00:00:00Z",
            }
        )
    path = tmp_path / "events.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_timestamp_enrichment_date_only_sources_do_not_create_repair(tmp_path: Path) -> None:
    events_path = _write_events(tmp_path)
    ibes_path = tmp_path / "ibes_actuals.csv"
    comp_path = tmp_path / "compustat_quarterly.csv"
    pd.DataFrame(
        [
            {
                "ibes_ticker": "AAA",
                "fiscal_period": "2019Q4",
                "anndats_act": "2020-02-05",
                "actual_eps": 1.1,
                "source_table_name": "wrds_ibes.actu_epsus",
                "source_extraction_timestamp": "2026-05-08T00:00:00Z",
            }
        ]
    ).to_csv(ibes_path, index=False)
    pd.DataFrame(
        [
            {
                "symbol": "BBB",
                "fiscal_period": "2019Q4",
                "gvkey": "001234",
                "datadate": "2019-12-31",
                "fqtr": 4,
                "fyearq": 2019,
                "rdq": "2020-02-06",
                "source_table_name": "comp.fundq",
            }
        ]
    ).to_csv(comp_path, index=False)

    result = build_sue_timestamp_enrichment(
        SueTimestampEnrichmentConfig(
            events_path=str(events_path),
            ibes_actuals_path=str(ibes_path),
            compustat_quarterly_path=str(comp_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
        )
    )

    decision = result.timestamp_enrichment_decision
    assert decision["decision_label"] == "timestamp_enrichment_no_repair_sue_blocked"
    assert decision["repairable_event_count"] == 0
    assert decision["selected_score"] is None
    assert decision["q2_evaluation_ran"] is False
    assert result.source_coverage_report["ibes_anndats_act_count"] == 1
    assert result.source_coverage_report["compustat_rdq_count"] == 1
    assert result.source_coverage_report["date_only_no_repair_count"] == 2
    assert result.repairable_event_candidates.empty
    assert "date_only_no_repair" in set(result.timestamp_source_comparison["primary_flag"])
    assert "This is timestamp-source enrichment only." in result.report_text


def test_timestamp_enrichment_exact_release_timestamp_creates_repair_candidate(tmp_path: Path) -> None:
    events_path = _write_events(tmp_path)
    release_path = tmp_path / "release_timestamps.csv"
    sec_path = tmp_path / "sec_filings.csv"
    pd.DataFrame(
        [
            {
                "event_id": "SUE-ENRICH-AAA",
                "exact_release_datetime": "2020-02-11T12:00:00Z",
                "before_after_marker": "before_open",
                "source_vendor": "test_release_vendor",
                "confidence_flag": "high",
            },
            {
                "event_id": "SUE-ENRICH-BBB",
                "exact_release_datetime": "2020-02-12T22:00:00Z",
                "before_after_marker": "after_close",
                "source_vendor": "test_release_vendor",
                "confidence_flag": "high",
            },
        ]
    ).to_csv(release_path, index=False)
    pd.DataFrame(
        [
            {
                "event_id": "SUE-ENRICH-CCC",
                "filing_datetime": "2020-02-10T10:00:00Z",
                "accession_id": "000-test",
                "filing_type": "8-K",
                "sec_first_public_release_proven": False,
            }
        ]
    ).to_csv(sec_path, index=False)

    result = build_sue_timestamp_enrichment(
        SueTimestampEnrichmentConfig(
            events_path=str(events_path),
            release_timestamps_path=str(release_path),
            sec_filing_timestamps_path=str(sec_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
        )
    )

    decision = result.timestamp_enrichment_decision
    assert decision["decision_label"] == "timestamp_enrichment_partial_repair_available"
    assert decision["repairable_event_count"] == 1
    assert decision["score_selection_ran"] is False
    assert result.source_coverage_report["exact_release_timestamp_count"] == 2
    assert result.source_coverage_report["sec_filing_timestamp_count"] == 1
    assert result.source_coverage_report["sec_cross_check_only_count"] == 1
    assert len(result.repairable_event_candidates) == 1
    candidate = result.repairable_event_candidates.iloc[0].to_dict()
    assert candidate["event_id"] == "SUE-ENRICH-AAA"
    assert candidate["repair_source"] == "exact_release_datetime"
    assert candidate["repair_source_vendor"] == "test_release_vendor"
    assert candidate["repaired_event_available_timestamp"] == "2020-02-11T12:00:00+00:00"
    assert candidate["repaired_tradable_timestamp"] == "2020-02-11T14:30:00+00:00"
    assert candidate["inferred_from_returns"] is False
    assert candidate["shifted_performance_used_as_evidence"] is False

    artifacts = write_sue_timestamp_enrichment_artifacts(result)
    for key in [
        "timestamp_source_comparison",
        "source_coverage_report",
        "date_disagreement_report",
        "repairable_event_candidates",
        "nonrepairable_event_report",
        "timestamp_enrichment_decision",
        "report",
    ]:
        assert artifacts[key].exists()
    saved_decision = json.loads(artifacts["timestamp_enrichment_decision"].read_text(encoding="utf-8"))
    assert saved_decision["repairable_event_count"] == 1


def test_timestamp_enrichment_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "selected production score",
        "SUE alpha is proven",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_timestamp_enrichment_report_language(f"Report says {phrase}.")


class _FakeTimestampSourceConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def raw_sql(self, query: str) -> pd.DataFrame:
        self.queries.append(query)
        if "ibes.actu_epsus" in query:
            return pd.DataFrame(
                [
                    {
                        "ticker": "AAA",
                        "cusip": "00010001",
                        "oftic": "AAA",
                        "pends": "2019-12-31",
                        "anndats": "2020-02-05",
                        "anntims": "12:00:00",
                        "actdats": "2020-02-05",
                        "acttims": "12:00:00",
                        "value": 1.1,
                        "curr_act": "USD",
                    },
                    {
                        "ticker": "BBB",
                        "cusip": "00010002",
                        "oftic": "BBB",
                        "pends": "2019-12-31",
                        "anndats": "2020-02-12",
                        "anntims": None,
                        "actdats": "2020-02-12",
                        "acttims": None,
                        "value": 1.2,
                        "curr_act": "USD",
                    },
                ]
            )
        if "comp.fundq" in query:
            return pd.DataFrame(
                [
                    {
                        "gvkey": "001001",
                        "datadate": "2019-12-31",
                        "fyearq": 2019,
                        "fqtr": 4,
                        "tic": "AAA",
                        "cusip": "000100019",
                        "rdq": "2020-02-05",
                    }
                ]
            )
        raise AssertionError(f"unexpected query: {query}")


def test_wrds_timestamp_source_extract_writes_h1e5_source_files(tmp_path: Path) -> None:
    events_path = _write_events(tmp_path)
    output_dir = tmp_path / "timestamp_sources"
    manifest_path = tmp_path / "manifest.json"

    result = extract_wrds_sue_timestamp_sources(
        SueTimestampSourceExtractConfig(
            events_path=str(events_path),
            output_dir=str(output_dir),
            manifest_path=str(manifest_path),
            ibes_actuals_output_path=str(output_dir / "ibes_actuals.csv"),
            compustat_quarterly_output_path=str(output_dir / "compustat_quarterly.csv"),
            ticker_chunk_size=10,
            compustat_chunk_size=10,
            fetched_at="2026-05-08T00:00:00Z",
        ),
        connection=_FakeTimestampSourceConnection(),
    )

    assert result["status"] == "completed"
    assert result["event_count"] == 3
    assert result["ibes_actuals_matched_events"] == 2
    assert result["compustat_rdq_matched_events"] == 1
    assert result["q2_evaluation_ran"] is False
    assert result["optimizer_path_evaluation_ran"] is False
    assert result["production_approval_claimed"] is False
    assert result["broker_order_workflow_added"] is False

    ibes = pd.read_csv(output_dir / "ibes_actuals.csv")
    assert set(["event_id", "ibes_ticker", "fiscal_period", "anndats_act", "anndats_act_timestamp"]).issubset(
        ibes.columns
    )
    aaa = ibes.loc[ibes["event_id"].eq("SUE-ENRICH-AAA")].iloc[0].to_dict()
    assert aaa["anndats_act"] == "2020-02-05"
    assert aaa["anndats_act_timestamp"] == "2020-02-05T12:00:00Z"
    bbb = ibes.loc[ibes["event_id"].eq("SUE-ENRICH-BBB")].iloc[0].to_dict()
    assert bbb["anndats_act_timestamp"] == "2020-02-12"

    comp = pd.read_csv(output_dir / "compustat_quarterly.csv")
    assert comp.iloc[0]["event_id"] == "SUE-ENRICH-AAA"
    assert comp.iloc[0]["rdq"] == "2020-02-05"

    enriched = build_sue_timestamp_enrichment(
        SueTimestampEnrichmentConfig(
            events_path=str(events_path),
            ibes_actuals_path=str(output_dir / "ibes_actuals.csv"),
            compustat_quarterly_path=str(output_dir / "compustat_quarterly.csv"),
            output_dir=str(tmp_path / "enriched"),
            report_path=str(tmp_path / "report.md"),
        )
    )
    assert enriched.source_coverage_report["ibes_anndats_act_count"] == 2
    assert enriched.source_coverage_report["compustat_rdq_count"] == 1
    assert enriched.timestamp_enrichment_decision["q2_evaluation_ran"] is False
