from __future__ import annotations

import json
from pathlib import Path

import pytest

from factor_discovery_sandbox.track_a_forensic_workflow import (
    CandidateCharter,
    MeasurementSpec,
    MeasurementSpecRegistry,
    ResearchBoundaryGuard,
    SectorNeutralResidualMomentumSignalBuilder,
    read_canonical_panel,
    run_track_a_forensic_workflow,
    write_fixture_config,
)


def test_track_a_fixture_writes_forensic_artifacts_and_stable_content_hash(tmp_path: Path) -> None:
    config_a = write_fixture_config(tmp_path / "run_a", run_id="fixture-a")
    config_b = write_fixture_config(tmp_path / "run_b", run_id="fixture-b")

    result_a = run_track_a_forensic_workflow(config_a)
    result_b = run_track_a_forensic_workflow(config_b)

    assert result_a.decision["decision"] == "eligible_for_q1_research_review"
    assert result_a.decision["q2_allowed"] is False
    assert result_a.decision["alpha_registry_allowed"] is False
    assert result_a.decision["paper_trading_allowed"] is False
    assert result_a.decision["production_allowed"] is False

    expected_artifacts = {
        "resolved_run_config.yaml",
        "run_manifest.json",
        "pit_validation_report.json",
        "forbidden_output_report.json",
        "signal_panel.parquet",
        "label_panel.parquet",
        "coverage_panel.parquet",
        "abstain_panel.parquet",
        "tradability_panel.parquet",
        "evidence_grid.csv",
        "exposure_diagnostics.csv",
        "placebo_report.json",
        "decision.json",
        "report.md",
    }
    assert expected_artifacts.issubset({path.name for path in result_a.artifacts.values()})

    manifest_a = json.loads((result_a.output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    manifest_b = json.loads((result_b.output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest_a["content_hash"] == manifest_b["content_hash"]
    assert manifest_a["run_instance_hash"] != manifest_b["run_instance_hash"]
    assert manifest_a["candidate_charter_hash"]
    assert manifest_a["measurement_spec_hash"]
    assert manifest_a["forbidden_outputs_checked"] is True

    signal_panel = read_canonical_panel(result_a.output_dir / "signal_panel.parquet")
    required_signal_columns = {
        "date",
        "instrument_id",
        "signal_value",
        "signal_state",
        "measurement_spec_id",
        "feature_hash",
        "created_by",
    }
    assert required_signal_columns.issubset(signal_panel.columns)
    inactive = signal_panel[signal_panel["signal_state"] != "active"]
    assert inactive["signal_value"].isna().all()
    assert not signal_panel[signal_panel["signal_state"] == "active"]["signal_value"].isna().any()


def test_research_boundary_guard_blocks_hard_fail_without_evidence(tmp_path: Path) -> None:
    config = write_fixture_config(
        tmp_path / "blocked",
        overrides={"research_boundary": {"q2_allowed": True}},
    )

    result = run_track_a_forensic_workflow(config)

    assert result.decision["decision"] == "blocked"
    assert result.decision["primary_reason"] == "research_boundary_violation"
    assert (result.output_dir / "resolved_run_config.yaml").exists()
    assert (result.output_dir / "run_manifest.json").exists()
    assert (result.output_dir / "forbidden_output_report.json").exists()
    assert (result.output_dir / "decision.json").exists()
    assert (result.output_dir / "report.md").exists()
    assert not (result.output_dir / "signal_panel.parquet").exists()
    assert not (result.output_dir / "evidence_grid.csv").exists()
    assert not (result.output_dir / "placebo_report.json").exists()


def test_asof_join_violation_blocks_before_signal_outputs(tmp_path: Path) -> None:
    config = write_fixture_config(tmp_path / "pit_blocked", feature_public_ts_offset_days=2)

    result = run_track_a_forensic_workflow(config)

    assert result.decision["decision"] == "blocked"
    assert result.decision["primary_reason"] == "pit_timestamp_violation"
    assert result.decision["gate_results"]["asof_join_validator"] == "fail"
    assert not (result.output_dir / "signal_panel.parquet").exists()
    assert not (result.output_dir / "evidence_grid.csv").exists()


def test_measurement_spec_change_control_requires_supersedes() -> None:
    registry = MeasurementSpecRegistry({"sector_neutral_residual_momentum_v1": "old-hash"})
    unchanged = MeasurementSpec(
        measurement_spec_id="sector_neutral_residual_momentum_v1",
        candidate_id="sector_neutral_residual_momentum",
        spec_body={"lookback": "252d"},
        declared_hash="old-hash",
    )
    changed_without_supersedes = MeasurementSpec(
        measurement_spec_id="sector_neutral_residual_momentum_v1",
        candidate_id="sector_neutral_residual_momentum",
        spec_body={"lookback": "252d", "control": "sector"},
        declared_hash="new-hash",
    )
    changed_with_supersedes = MeasurementSpec(
        measurement_spec_id="sector_neutral_residual_momentum_v2",
        candidate_id="sector_neutral_residual_momentum",
        spec_body={"lookback": "252d", "control": "sector"},
        declared_hash="new-hash",
        supersedes="sector_neutral_residual_momentum_v1",
        change_reason="add sector residualization",
    )

    assert registry.validate(unchanged)["valid"] is True
    assert registry.validate(changed_without_supersedes)["valid"] is False
    assert registry.validate(changed_without_supersedes)["primary_reason"] == "measurement_spec_hash_mismatch"
    assert registry.validate(changed_with_supersedes)["valid"] is True


def test_boundary_guard_rejects_report_success_language() -> None:
    report_text = "This alpha passed and is Q2-ready for allocation."

    result = ResearchBoundaryGuard().scan_report_text(report_text)

    assert result["valid"] is False
    assert "alpha passed" in result["forbidden_terms"]
    assert "q2-ready" in result["forbidden_terms"]
    assert "allocation" in result["forbidden_terms"]


def test_signal_builder_cannot_read_candidate_charter() -> None:
    builder = SectorNeutralResidualMomentumSignalBuilder()
    charter = CandidateCharter(
        candidate_id="sector_neutral_residual_momentum",
        pain_point="within-sector underreaction",
        mechanism="stock-specific residual trends",
    )

    with pytest.raises(TypeError):
        builder.build(charter)  # type: ignore[arg-type]
