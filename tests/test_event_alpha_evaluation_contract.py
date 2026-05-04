from __future__ import annotations

import csv
from pathlib import Path

import pytest

from portfolio_os.alpha.event_evaluation import (
    EventEvidenceValidationError,
    EventWindowLabel,
    ToNextAnnouncementLabel,
    build_event_evidence_bundle,
    dump_event_evidence_bundle_json,
    write_event_evidence_artifacts,
)
from portfolio_os.alpha.view_contract import AlphaView, load_alpha_view


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = REPO_ROOT / "projects" / "alpha_view_contract"
SUE_VIEW = PROJECT_ROOT / "examples" / "event_sue_pead_view.json"
REVISION_VIEW = PROJECT_ROOT / "examples" / "event_revision_view.json"


def test_sue_event_window_bundle_records_required_windows_and_timestamps() -> None:
    view = load_alpha_view(SUE_VIEW)
    bundle = build_event_evidence_bundle(
        alpha_view=view,
        labels=[
            EventWindowLabel(name="sue_plus_2", start_offset_days=2, end_offset_days=2),
            EventWindowLabel(name="sue_plus_2_to_3", start_offset_days=2, end_offset_days=3),
            EventWindowLabel(name="sue_plus_2_to_22", start_offset_days=2, end_offset_days=22),
        ],
        placebo_tests_required=["event_date_shift", "cross_section_shuffle"],
        overlap_reference_family_ids=["US_ANALYST_REVISION"],
    )

    windows = {
        (row["start_offset_days"], row["end_offset_days"])
        for row in bundle.event_window_grid
        if row["label_type"] == "event_window"
    }
    assert windows == {(2, 2), (2, 3), (2, 22)}
    assert bundle.pit_visibility_report["signal_timestamp"] == "2025-02-05T21:05:00Z"
    assert bundle.pit_visibility_report["visibility_timestamp"] == "2025-02-05T21:10:00Z"
    assert bundle.pit_visibility_report["tradable_timestamp"] == "2025-02-06T14:30:00Z"
    assert bundle.pit_visibility_report["anchor_event_timestamp"] == "2025-02-05T21:05:00Z"
    assert bundle.placebo_report["planned_tests"] == ["event_date_shift", "cross_section_shuffle"]
    assert bundle.event_overlap_diagnostics["reference_family_ids"] == ["US_ANALYST_REVISION"]


def test_revision_bundle_expresses_statpers_to_next_announcement_horizon() -> None:
    view = load_alpha_view(REVISION_VIEW)
    bundle = build_event_evidence_bundle(
        alpha_view=view,
        labels=[
            ToNextAnnouncementLabel(
                name="revision_statpers_to_next_announcement",
                signal_date_field="statpers",
                entry_rule="next_trading_day_after_statpers",
                next_event_field="next_announcement_timestamp",
            )
        ],
        placebo_tests_required=["statpers_month_shuffle", "analyst_coverage_null"],
        overlap_reference_family_ids=["US_EVENT_SUE"],
    )

    assert bundle.horizon_type == "to_next_event"
    assert bundle.pit_visibility_report["pit_source"] == "WRDS"
    assert bundle.event_window_grid == [
        {
            "alpha_view_id": "AV-US-REVISION-TNA-001",
            "end_offset_days": "",
            "entry_rule": "next_trading_day_after_statpers",
            "family_id": "US_ANALYST_REVISION",
            "horizon_label": "statpers -> next_trading_day_after_statpers -> next_announcement_timestamp",
            "label_name": "revision_statpers_to_next_announcement",
            "label_type": "to_next_announcement",
            "next_event_field": "next_announcement_timestamp",
            "signal_date_field": "statpers",
            "start_offset_days": 1,
        }
    ]


def test_event_evidence_bundle_dump_and_artifacts_are_deterministic(tmp_path: Path) -> None:
    view = load_alpha_view(SUE_VIEW)
    bundle = build_event_evidence_bundle(
        alpha_view=view,
        labels=[EventWindowLabel(name="sue_plus_2_to_22", start_offset_days=2, end_offset_days=22)],
        placebo_tests_required=["event_date_shift"],
    )

    assert dump_event_evidence_bundle_json(bundle) == dump_event_evidence_bundle_json(bundle)

    artifacts = write_event_evidence_artifacts(bundle, tmp_path)
    assert set(artifacts) == {
        "event_evidence_bundle.json",
        "event_window_grid.csv",
        "event_half_life_summary.json",
        "event_overlap_diagnostics.json",
        "pit_visibility_report.json",
        "placebo_report.json",
    }
    with (tmp_path / "event_window_grid.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["horizon_label"] == "[+2,+22]"
    assert "realized_return" not in (tmp_path / "event_evidence_bundle.json").read_text(encoding="utf-8")
    assert "orders" not in (tmp_path / "event_evidence_bundle.json").read_text(encoding="utf-8")


def test_fmp_analyst_estimate_history_is_not_pit_safe_revision_source() -> None:
    payload = load_alpha_view(REVISION_VIEW).model_dump(mode="json")
    payload["pit_safety_report"]["pit_source"] = "FMP analyst estimate history"
    view = AlphaView.validate_payload(payload)

    with pytest.raises(EventEvidenceValidationError, match="FMP analyst-estimate history"):
        build_event_evidence_bundle(
            alpha_view=view,
            labels=[
                ToNextAnnouncementLabel(
                    name="revision_statpers_to_next_announcement",
                    signal_date_field="statpers",
                    entry_rule="next_trading_day_after_statpers",
                    next_event_field="next_announcement_timestamp",
                )
            ],
            placebo_tests_required=["statpers_month_shuffle"],
        )
