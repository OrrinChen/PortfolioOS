from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_d2 import (
    REQUIRED_EVENT_REGISTRY_COLUMNS,
    build_demo_insider_events,
    run_insider_disclosure_d2,
    validate_event_registry,
)


def test_insider_disclosure_d2_writes_no_formula_observability_artifacts(tmp_path: Path) -> None:
    result = run_insider_disclosure_d2(output_dir=tmp_path / "d2_insider")

    assert result.summary["schema_version"] == "insider_disclosure_d2_summary.v1"
    assert result.summary["stage"] == "D2-INSIDER-01"
    assert result.summary["no_formula_observability_only"] is True
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["overall_decision"] == "observable"
    assert result.summary["allow_d3_charter_for"] == ["open_market_insider_buying_post_2023"]

    assert set(result.artifacts) == {
        "insider_event_registry",
        "event_subset_counts",
        "timestamp_audit",
        "tradability_audit",
        "car_window_panel",
        "matched_control_panel",
        "placebo_report",
        "d2_observability_summary",
        "d2_insider_disclosure_observability_report",
    }

    registry = pd.read_csv(result.artifacts["insider_event_registry"])
    assert set(REQUIRED_EVENT_REGISTRY_COLUMNS).issubset(registry.columns)
    assert set(registry["event_subset"]) >= {
        "open_market_buy",
        "discretionary_sell",
        "planned_sell",
        "compensation_control",
        "unknown_no_view",
    }
    assert registry.loc[registry["event_subset"] == "unknown_no_view", "no_view_reason"].ne("").all()

    car = pd.read_csv(result.artifacts["car_window_panel"])
    assert set(car["window"]) >= {"pre_10_1", "post_1_22"}
    assert "formula_score" not in car.columns
    assert "expected_return" not in car.columns

    subset_decisions = result.summary["subset_decisions"]
    assert subset_decisions["open_market_buy"]["decision"] == "observable"
    assert subset_decisions["discretionary_sell"]["decision"] == "observable"
    assert subset_decisions["planned_sell"]["decision"] == "compression_observable"
    assert subset_decisions["compensation_controls"]["decision"] == "control_clean"

    placebo = json.loads(result.artifacts["placebo_report"].read_text(encoding="utf-8"))
    assert placebo["shifted_filing_dates"]["status"] == "pass"
    assert placebo["same_coverage_random"]["status"] == "pass"
    assert placebo["formula_score_written"] is False

    report = result.artifacts["d2_insider_disclosure_observability_report"].read_text(encoding="utf-8").lower()
    assert "no formula" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "q2-ready",
        "alpha passed",
    ]:
        assert forbidden not in report


def test_insider_event_registry_rejects_timestamp_and_no_view_violations() -> None:
    events = build_demo_insider_events()
    events.loc[0, "tradable_timestamp"] = events.loc[0, "visibility_timestamp"]
    events.loc[1, "event_subset"] = "unknown_no_view"
    events.loc[1, "no_view_reason"] = ""

    validation = validate_event_registry(events)

    assert validation["valid"] is False
    assert any("tradable_timestamp_not_after_visibility" in reason for reason in validation["failure_reasons"])
    assert any("unknown_no_view_missing_reason" in reason for reason in validation["failure_reasons"])


def test_insider_disclosure_d2_blocks_when_placebo_dominates(tmp_path: Path) -> None:
    events = build_demo_insider_events()
    result = run_insider_disclosure_d2(
        output_dir=tmp_path / "d2_insider_blocked",
        events=events,
        placebo_overrides={"shifted_filing_dates": 0.05},
    )

    assert result.summary["overall_decision"] == "blocked_placebo_dominance"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["formula_score_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
