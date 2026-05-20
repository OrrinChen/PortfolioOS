from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_announcement_timestamp_policy import (
    SueAnnouncementTimestampPolicyConfig,
    build_sue_announcement_timestamp_policy_audit,
    validate_sue_announcement_timestamp_policy_report_language,
    write_sue_announcement_timestamp_policy_artifacts,
)


def _write_policy_fixture(tmp_path: Path, *, include_repair_source: bool = False) -> Path:
    events_path = tmp_path / "events.csv"
    rows = []
    symbols = ["A", "B", "C", "D", "E"]
    event_dates = ["2020-02-10", "2020-03-10", "2020-04-10"]
    for month_idx, announcement_date in enumerate(event_dates, start=1):
        for idx, symbol in enumerate(symbols, start=1):
            raw = float(idx - 3) / 10.0
            expected_eps = 1.0 + month_idx / 10.0
            row = {
                "event_id": f"SUE-POLICY-{month_idx}-{symbol}",
                "symbol": symbol,
                "permno": month_idx * 10000 + idx,
                "ibes_ticker": symbol,
                "cusip": f"{month_idx * 10000 + idx:08d}",
                "fiscal_period": "2019Q4",
                "announcement_date": announcement_date,
                "event_available_timestamp": f"{announcement_date}T21:15:00Z",
                "tradable_timestamp": str((pd.Timestamp(announcement_date) + pd.offsets.BDay(1)).date())
                + "T14:30:00Z",
                "rebalance_date": str((pd.Timestamp(announcement_date) + pd.offsets.BDay(1)).date()),
                "actual_eps": expected_eps + raw,
                "expected_eps": expected_eps,
                "sue_value": raw,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": str((pd.Timestamp(announcement_date) - pd.offsets.BDay(1)).date()),
                "price_anchor_date": str((pd.Timestamp(announcement_date) + pd.offsets.BDay(1)).date()),
                "return_window_start": str((pd.Timestamp(announcement_date) + pd.offsets.BDay(3)).date()),
                "return_window_end": str((pd.Timestamp(announcement_date) + pd.offsets.BDay(23)).date()),
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "test_exact_link",
                "pit_safety_status": "pit_safe",
                "diagnostic_only": False,
                "fetched_at": "2026-05-08T00:00:00Z",
            }
            if include_repair_source:
                row["actual_eps_source_timestamp"] = f"{announcement_date}T12:00:00Z"
                row["actual_eps_source_table"] = "wrds_ibes_actuals_test_fixture"
                row["announcement_timing_marker"] = "before_open"
            rows.append(row)
    pd.DataFrame(rows).to_csv(events_path, index=False)
    return events_path


def test_timestamp_policy_blocks_earlier_anchor_without_auditable_source(tmp_path: Path) -> None:
    events_path = _write_policy_fixture(tmp_path, include_repair_source=False)
    result = build_sue_announcement_timestamp_policy_audit(
        SueAnnouncementTimestampPolicyConfig(
            events_path=str(events_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
        )
    )

    assert result.timing_policy_decision["decision_label"] == "no_auditable_earlier_timestamp_sue_blocked"
    assert result.timing_policy_decision["repaired_event_count"] == 0
    assert result.timing_policy_decision["q2_evaluation_ran"] is False
    assert result.timing_policy_decision["optimizer_path_evaluation_ran"] is False
    assert result.timing_policy_decision["production_approval_claimed"] is False
    assert result.repaired_h1e_summary["rerun_attempted"] is False
    assert result.repaired_h1e_summary["blocked_reason"] == "no_auditable_earlier_timestamp_source"
    assert set(result.anchor_policy_grid["candidate_anchor_policy"]).issuperset(
        {
            "current_policy",
            "conservative_date_only_next_open",
            "after_close_next_open",
            "before_open_same_day_or_next_open",
            "source_repaired_announcement_timestamp",
            "blocked_if_no_auditable_timestamp",
        }
    )
    assert "cannot be used as tradable SUE unless actual EPS availability is proven earlier" in result.report_text

    artifacts = write_sue_announcement_timestamp_policy_artifacts(result)
    for key in [
        "timestamp_source_comparison",
        "timing_repair_eligibility",
        "anchor_policy_grid",
        "repaired_h1e_summary",
        "timing_policy_decision",
        "report",
    ]:
        assert artifacts[key].exists()
    decision = json.loads(artifacts["timing_policy_decision"].read_text(encoding="utf-8"))
    assert decision["selected_score"] is None


def test_timestamp_policy_flags_auditable_early_source_without_using_blind_shift(tmp_path: Path) -> None:
    events_path = _write_policy_fixture(tmp_path, include_repair_source=True)
    result = build_sue_announcement_timestamp_policy_audit(
        SueAnnouncementTimestampPolicyConfig(
            events_path=str(events_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
            min_repaired_events_for_h1e=999,
        )
    )

    assert result.timing_policy_decision["decision_label"] == "anchor_policy_repaired_and_h1e_rerun_required"
    assert result.timing_policy_decision["repaired_event_count"] == 15
    assert result.repaired_h1e_summary["rerun_attempted"] is False
    assert result.repaired_h1e_summary["rerun_required"] is True
    assert result.timing_repair_eligibility["timing_classification"].eq("likely_late_vendor_date").all()
    assert result.timing_repair_eligibility["selected_anchor_policy"].eq("source_repaired_announcement_timestamp").all()
    repaired_tradable = pd.to_datetime(result.timing_repair_eligibility["repaired_tradable_timestamp"], utc=True)
    source_timestamp = pd.to_datetime(result.timing_repair_eligibility["earliest_auditable_source_timestamp"], utc=True)
    assert bool((repaired_tradable >= source_timestamp).all())
    assert result.timing_policy_decision["blind_shift_policy_allowed"] is False


def test_timestamp_policy_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "selected production score",
        "real historical SUE alpha proven",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_announcement_timestamp_policy_report_language(f"Report says {phrase}.")
