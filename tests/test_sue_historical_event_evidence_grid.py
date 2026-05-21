from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_historical_event_evidence import (
    SueHistoricalEventEvidenceConfig,
    build_sue_historical_event_evidence_grid,
    render_sue_historical_event_evidence_report,
    validate_sue_historical_event_evidence_report_language,
    write_sue_historical_event_evidence_artifacts,
)


def _write_fixture_inputs(tmp_path: Path) -> dict[str, Path]:
    events_path = tmp_path / "events.csv"
    sue_values_path = tmp_path / "sue_values.csv"
    crsp_path = tmp_path / "crsp_daily.csv"
    coverage_path = tmp_path / "coverage_report.json"
    pit_path = tmp_path / "pit_visibility_report.json"
    lineage_path = tmp_path / "data_lineage_manifest.json"

    event_rows: list[dict[str, object]] = []
    price_rows: list[dict[str, object]] = []
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    for date_index, rebalance_date in enumerate(pd.bdate_range("2020-01-06", periods=4)):
        tradable = pd.Timestamp(rebalance_date).date()
        for symbol_index, symbol in enumerate(symbols):
            permno = 10000 + symbol_index
            sue_value = float(symbol_index - 2.5)
            event_id = f"SUE-{date_index}-{symbol}"
            event_rows.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "permno": permno,
                    "ibes_ticker": symbol,
                    "cusip": f"{permno:08d}",
                    "fiscal_period": "2019Q4",
                    "announcement_date": (rebalance_date - pd.offsets.BDay(1)).date().isoformat(),
                    "event_available_timestamp": (
                        rebalance_date - pd.offsets.BDay(1)
                    ).strftime("%Y-%m-%dT21:15:00Z"),
                    "tradable_timestamp": f"{tradable.isoformat()}T14:30:00Z",
                    "rebalance_date": tradable.isoformat(),
                    "actual_eps": 1.0 + sue_value / 100.0,
                    "expected_eps": 1.0,
                    "sue_value": sue_value,
                    "sue_definition": "actual_eps_minus_latest_pit_consensus",
                    "estimate_snapshot_date": (rebalance_date - pd.offsets.BDay(2)).date().isoformat(),
                    "price_anchor_date": tradable.isoformat(),
                    "return_window_start": (rebalance_date + pd.offsets.BDay(2)).date().isoformat(),
                    "return_window_end": (rebalance_date + pd.offsets.BDay(22)).date().isoformat(),
                    "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                    "link_method": "ibes_idsum_cusip_sdates",
                    "pit_safety_status": "pit_safe",
                    "diagnostic_only": False,
                    "fetched_at": "2026-05-06T00:00:00Z",
                }
            )
            for offset, price_date in enumerate(pd.bdate_range(rebalance_date - pd.offsets.BDay(12), periods=45)):
                daily_ret = 0.0005 * sue_value if offset >= 14 else -0.0001 * sue_value
                price_rows.append(
                    {
                        "permno": permno,
                        "date": price_date.date().isoformat(),
                        "prc": 100.0 + offset,
                        "ret": daily_ret,
                    }
                )

    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(event_rows).loc[:, ["event_id", "symbol", "permno", "sue_value", "diagnostic_only", "pit_safety_status"]].to_csv(
        sue_values_path, index=False
    )
    pd.DataFrame(price_rows).to_csv(crsp_path, index=False)
    coverage_path.write_text(
        json.dumps({"schema_version": "sue_historical_coverage_report.v1", "final_pit_safe_rows": len(event_rows)}),
        encoding="utf-8",
    )
    pit_path.write_text(
        json.dumps({"schema_version": "sue_historical_pit_visibility_report.v1", "pit_safe_rows": len(event_rows)}),
        encoding="utf-8",
    )
    lineage_path.write_text(
        json.dumps(
            {
                "schema_version": "sue_historical_data_lineage_manifest.v1",
                "source_table_names": ["ibes.actu_epsus", "ibes.statsum_epsus", "crsp.dsf"],
                "query_timestamp": "2026-05-06T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    return {
        "events": events_path,
        "sue_values": sue_values_path,
        "crsp": crsp_path,
        "coverage": coverage_path,
        "pit": pit_path,
        "lineage": lineage_path,
    }


def _config(paths: dict[str, Path], tmp_path: Path) -> SueHistoricalEventEvidenceConfig:
    return SueHistoricalEventEvidenceConfig(
        events_path=str(paths["events"]),
        sue_values_path=str(paths["sue_values"]),
        crsp_daily_path=str(paths["crsp"]),
        pit_visibility_report_path=str(paths["pit"]),
        coverage_report_path=str(paths["coverage"]),
        data_lineage_manifest_path=str(paths["lineage"]),
        output_dir=str(tmp_path / "evidence"),
        report_path=str(tmp_path / "sue_historical_event_evidence_report.md"),
    )


def test_bounded_sue_event_evidence_grid_writes_required_outputs(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)

    result = build_sue_historical_event_evidence_grid(_config(paths, tmp_path))
    artifacts = write_sue_historical_event_evidence_artifacts(result)

    required = {
        "event_window_grid",
        "rank_ic_by_date",
        "top_bottom_spread_by_date",
        "placebo_report",
        "coverage_by_month",
        "coverage_by_year",
        "pit_leakage_audit",
        "evidence_summary",
        "report",
    }
    assert required.issubset(artifacts)
    grid = pd.read_csv(artifacts["event_window_grid"])
    assert set(grid["window_name"]) == {"plus_2_plus_2", "plus_2_plus_3", "plus_2_plus_22"}
    assert grid["safe_row_count"].min() > 0
    assert grid["mean_rank_ic"].notna().all()

    placebo = json.loads(artifacts["placebo_report"].read_text(encoding="utf-8"))
    assert {"event_date_shift", "sign_flip_sue", "randomized_sue"}.issubset(placebo["diagnostics"])
    summary = json.loads(artifacts["evidence_summary"].read_text(encoding="utf-8"))
    assert summary["production_approval_claimed"] is False
    assert summary["q2_evaluation_ran"] is False
    assert summary["interpretation"] in {
        "sue_bounded_evidence_positive_but_needs_scale",
        "sue_bounded_evidence_mixed",
        "sue_bounded_evidence_negative",
        "sue_bounded_evidence_inconclusive",
    }
    report = artifacts["report"].read_text(encoding="utf-8")
    assert "This is a bounded WRDS/PIT-safe historical evidence grid." in report
    assert "It does not prove full historical SUE alpha." in report
    validate_sue_historical_event_evidence_report_language(report)


def test_expanded_scope_uses_expanded_interpretation_labels(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    config = _config(paths, tmp_path).model_copy(update={"evidence_scope": "expanded"})

    result = build_sue_historical_event_evidence_grid(config)

    assert result.evidence_summary["interpretation"] in {
        "sue_expanded_evidence_positive_but_needs_q2",
        "sue_expanded_evidence_mixed",
        "sue_expanded_evidence_negative",
        "sue_expanded_evidence_inconclusive",
    }
    assert "This is expanded WRDS/PIT historical evidence, not production approval." in result.report_text


def test_event_evidence_rejects_pit_order_violations(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    events = pd.read_csv(paths["events"])
    events.loc[0, "event_available_timestamp"] = "2020-01-07T15:00:00Z"
    events.to_csv(paths["events"], index=False)

    with pytest.raises(ValueError, match="event_available_timestamp"):
        build_sue_historical_event_evidence_grid(_config(paths, tmp_path))

    paths = _write_fixture_inputs(tmp_path)
    events = pd.read_csv(paths["events"])
    events.loc[0, "estimate_snapshot_date"] = "2020-01-07"
    events.to_csv(paths["events"], index=False)

    with pytest.raises(ValueError, match="estimate_snapshot_date"):
        build_sue_historical_event_evidence_grid(_config(paths, tmp_path))


def test_missing_return_window_stays_unavailable_not_zero_alpha(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    prices = pd.read_csv(paths["crsp"])
    prices = prices.loc[prices["permno"].ne(10000)]
    prices.to_csv(paths["crsp"], index=False)

    result = build_sue_historical_event_evidence_grid(_config(paths, tmp_path))

    assert result.pit_leakage_audit["missing_return_window_count"] > 0
    assert result.pit_leakage_audit["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.evidence_summary["no_view_not_zero_alpha"] is True


def test_event_evidence_report_language_rejects_misleading_claims(tmp_path: Path) -> None:
    paths = _write_fixture_inputs(tmp_path)
    result = build_sue_historical_event_evidence_grid(_config(paths, tmp_path))
    report = render_sue_historical_event_evidence_report(result)
    validate_sue_historical_event_evidence_report_language(report)

    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker",
        "order",
        "real historical SUE alpha proven",
        "SUE alpha is proven",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_historical_event_evidence_report_language(report + f"\n{phrase}")
