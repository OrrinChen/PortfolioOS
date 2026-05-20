from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_event_timing_anchor_audit import (
    SueEventTimingAnchorAuditConfig,
    build_sue_event_timing_anchor_audit,
    validate_sue_event_timing_anchor_audit_report_language,
    write_sue_event_timing_anchor_audit_artifacts,
)


def _write_anchor_fixture(tmp_path: Path) -> tuple[Path, Path]:
    events_path = tmp_path / "events.csv"
    prices_path = tmp_path / "crsp_daily.csv"
    event_rows = []
    price_rows = []
    event_specs = [
        ("A", 1.0, 1.0),
        ("B", 0.5, 1.0),
        ("C", -0.5, 1.0),
        ("D", -1.0, 2.0),
        ("E", 2.0, 2.0),
    ]
    date_specs = [
        ("2020-02-03", "2020-02-04", "21:15:00Z"),
        ("2020-03-02", "2020-03-03", "12:00:00Z"),
        ("2020-04-06", "2020-04-07", "00:00:00Z"),
    ]
    for month_idx, (announcement_date, tradable_date, available_time) in enumerate(date_specs, start=1):
        for idx, (symbol, raw, expected_eps) in enumerate(event_specs, start=1):
            permno = month_idx * 10000 + idx
            event_rows.append(
                {
                    "event_id": f"SUE-{month_idx}-{symbol}",
                    "symbol": symbol,
                    "permno": permno,
                    "ibes_ticker": symbol,
                    "cusip": f"{permno:08d}",
                    "fiscal_period": "2019Q4",
                    "announcement_date": announcement_date,
                    "event_available_timestamp": f"{announcement_date}T{available_time}",
                    "tradable_timestamp": f"{tradable_date}T14:30:00Z",
                    "rebalance_date": tradable_date,
                    "actual_eps": expected_eps + raw,
                    "expected_eps": expected_eps,
                    "sue_value": raw,
                    "sue_definition": "actual_eps_minus_latest_pit_consensus",
                    "estimate_snapshot_date": "2020-01-15",
                    "price_anchor_date": tradable_date,
                    "return_window_start": str(pd.bdate_range(tradable_date, periods=3)[2].date()),
                    "return_window_end": str(pd.bdate_range(tradable_date, periods=25)[-1].date()),
                    "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                    "link_method": "test_exact_link",
                    "pit_safety_status": "pit_safe",
                    "diagnostic_only": False,
                    "fetched_at": "2026-05-08T00:00:00Z",
                }
            )
            dates = pd.bdate_range(pd.Timestamp(tradable_date) - pd.offsets.BDay(20), periods=80)
            for date in dates:
                rel = len([d for d in dates if d <= date]) - 21
                pre_event_drift = 0.05 * raw if -10 <= rel <= -6 else 0.0
                late_live_move = 0.01 * raw if 2 <= rel <= 4 else 0.0
                price_rows.append(
                    {
                        "permno": permno,
                        "date": date.date().isoformat(),
                        "prc": 50.0 + idx,
                        "ret": pre_event_drift + late_live_move,
                    }
                )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    return events_path, prices_path


def test_event_timing_anchor_audit_compares_anchors_and_drift(tmp_path: Path) -> None:
    events_path, prices_path = _write_anchor_fixture(tmp_path)
    result = build_sue_event_timing_anchor_audit(
        SueEventTimingAnchorAuditConfig(
            events_path=str(events_path),
            crsp_daily_path=str(prices_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
            min_rank_ic_names=4,
            min_spread_names=5,
        )
    )

    anchor_definitions = set(result.anchor_grid["anchor_definition"])
    assert {
        "current_tradable",
        "announcement_plus_0_td",
        "announcement_plus_1_td",
        "announcement_plus_2_td",
        "shift_minus_2_td",
        "shift_minus_5_td",
        "shift_minus_10_td",
        "shift_plus_2_td",
        "shift_plus_5_td",
        "shift_plus_10_td",
    }.issubset(anchor_definitions)
    assert {"minus_10_minus_6", "minus_5_minus_1", "minus_2_minus_1", "zero_plus_1", "plus_2_plus_3", "plus_2_plus_22"}.issubset(
        set(result.pre_event_drift_grid["window_name"])
    )
    assert {"after_close", "before_open", "date_only"}.issubset(set(result.timing_quality_breakdown["timing_quality"]))
    assert result.window_overlap_audit["shifted_anchor_actually_changes_return_window"] is True
    assert result.window_overlap_audit["benchmark_window_uses_shifted_anchor"] is True
    assert result.window_overlap_audit["market_adjusted_spread_uses_shifted_anchor"] is True
    assert result.anchor_selection_diagnostic["selected_score"] is None
    assert result.anchor_selection_diagnostic["q2_evaluation_ran"] is False
    assert result.anchor_selection_diagnostic["optimizer_path_evaluation_ran"] is False
    assert result.anchor_selection_diagnostic["production_approval_claimed"] is False
    assert result.anchor_selection_diagnostic["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.anchor_selection_diagnostic["interpretation"] in {
        "anchor_definition_likely_late",
        "pre_event_drift_dominates",
        "shifted_placebo_window_bug",
        "market_timing_contamination",
        "timing_quality_insufficient",
        "anchor_audit_inconclusive",
        "anchor_definition_cleared",
    }
    assert "This is an event timing and anchor audit only." in result.report_text

    artifacts = write_sue_event_timing_anchor_audit_artifacts(result)
    for key in [
        "anchor_grid",
        "pre_event_drift_grid",
        "timing_quality_breakdown",
        "window_overlap_audit",
        "market_timing_audit",
        "anchor_selection_diagnostic",
        "report",
    ]:
        assert artifacts[key].exists()
    diagnostic = json.loads(artifacts["anchor_selection_diagnostic"].read_text(encoding="utf-8"))
    assert diagnostic["score_selection_ran"] is False


def test_event_timing_anchor_audit_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "SUE alpha is proven",
        "selected production score",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_event_timing_anchor_audit_report_language(f"Report says {phrase}.")
