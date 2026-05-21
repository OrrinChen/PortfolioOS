from __future__ import annotations

import json
from pathlib import Path

from multifactor_alpha_validation.allocator import build_allocator_result
from multifactor_alpha_validation.cost_capacity import build_survival_result
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.dashboard import render_factor_dashboard, write_factor_dashboard
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.registry import build_factor_registry, write_factor_registry
from multifactor_alpha_validation.reports import build_research_report, write_release_manifest, write_research_report
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


REPO_ROOT = Path(__file__).resolve().parents[3]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def _build_week8_inputs():
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    allocator = build_allocator_result(
        specs,
        shrinkage.posterior_mu,
        covariance.shrunk_covariance,
        redundancy.marginal_value_decision_table,
    )
    survival = build_survival_result(specs, evidence.factor_evidence_table, allocator, shrinkage.posterior_mu)
    registry = build_factor_registry(specs, allocator, survival)
    return specs, evidence, redundancy, shrinkage, covariance, allocator, survival, registry


def test_factor_registry_assigns_status_and_stop_layer_for_every_factor() -> None:
    specs, *_rest, registry = _build_week8_inputs()
    table = registry.decision_table

    assert len(table) == len(specs)
    assert table["final_status"].notna().all()
    assert table["stop_layer"].notna().all()
    assert set(table["final_status"]).isdisjoint({"pass", "fail", "passed", "failed"})
    assert "analyst_revision_disabled" in set(table["factor_id"])
    assert registry.registry["non_claims"]["production_approval"] is False


def test_report_and_dashboard_state_non_claims_and_are_read_only(tmp_path: Path) -> None:
    specs, evidence, redundancy, shrinkage, covariance, allocator, survival, registry = _build_week8_inputs()

    report = build_research_report(specs, evidence, redundancy, shrinkage, covariance, allocator, survival, registry)
    dashboard = render_factor_dashboard(registry, survival, allocator)

    assert "No production approval" in report
    assert "No live trading" in report
    assert "No security-level output" in report
    lowered = dashboard.lower()
    assert "<form" not in lowered
    assert "method=\"post\"" not in lowered
    assert "broker" not in lowered
    assert "submit" not in lowered

    report_path = write_research_report(report, tmp_path / "reports")
    dashboard_path = write_factor_dashboard(dashboard, tmp_path / "dashboard")
    assert report_path.exists()
    assert dashboard_path.exists()


def test_registry_and_release_manifest_are_written(tmp_path: Path) -> None:
    *_rest, survival, registry = _build_week8_inputs()

    registry_files = write_factor_registry(registry, tmp_path / "factor_registry")
    manifest_path = write_release_manifest(
        tmp_path / "factor_release",
        artifact_paths=[
            tmp_path / "factor_registry" / "factor_registry.yaml",
            tmp_path / "factor_registry" / "factor_decision_table.csv",
            tmp_path / "dashboard" / "dashboard.html",
        ],
        survival=survival,
    )

    assert (tmp_path / "factor_registry" / "factor_registry.yaml").exists()
    assert (tmp_path / "factor_registry" / "factor_decision_table.csv").exists()
    assert "factor_registry.yaml" in registry_files
    data = json.loads(manifest_path.read_text())
    assert data["schema_version"] == "factor_release_manifest.v1"
    assert data["non_claims"]["direct_q2_entry"] is False
    assert data["survival_layers"] >= 1


def test_makefile_exposes_factor_validate_target() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text()

    assert "factor-validate:" in makefile
    assert "projects/multifactor_alpha_validation/tests" in makefile
