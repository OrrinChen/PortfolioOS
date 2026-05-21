from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.allocator import build_allocator_result
from multifactor_alpha_validation.cost_capacity import build_survival_result
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.dashboard import render_factor_dashboard
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.registry import build_factor_registry
from multifactor_alpha_validation.reports import build_research_report
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


def build_pipeline(spec_dir: Path):
    specs = load_factor_specs(spec_dir)
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
    report = build_research_report(specs, evidence, redundancy, shrinkage, covariance, allocator, survival, registry)
    dashboard = render_factor_dashboard(registry, survival, allocator)
    return specs, signals, evidence, redundancy, shrinkage, covariance, allocator, survival, registry, report, dashboard
