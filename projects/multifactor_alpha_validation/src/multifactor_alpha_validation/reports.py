from __future__ import annotations

import json
from pathlib import Path

from multifactor_alpha_validation.allocator import AllocatorResult
from multifactor_alpha_validation.cost_capacity import SurvivalResult
from multifactor_alpha_validation.covariance import CovarianceResult
from multifactor_alpha_validation.q1_evidence import Q1EvidenceResult
from multifactor_alpha_validation.redundancy_gate import RedundancyGateResult
from multifactor_alpha_validation.registry import FactorRegistryResult
from multifactor_alpha_validation.schema import FactorSpec
from multifactor_alpha_validation.shrinkage import ShrinkageResult


def build_research_report(
    specs: list[FactorSpec],
    evidence: Q1EvidenceResult,
    redundancy: RedundancyGateResult,
    shrinkage: ShrinkageResult,
    covariance: CovarianceResult,
    allocator: AllocatorResult,
    survival: SurvivalResult,
    registry: FactorRegistryResult,
) -> str:
    return "\n".join(
        [
            "# Multi-Factor Alpha Validation Report",
            "",
            "## 1. Scope",
            "This is a PIT-safe, redundancy-aware, cost-aware multi-factor validation system.",
            "It is not a production trading system.",
            "",
            "## 2. Factor Library",
            f"- factor_count: {len(specs)}",
            f"- enabled_count: {sum(spec.status == 'enabled' for spec in specs)}",
            "",
            "## 3. Data and PIT Contract",
            "- missing coverage is explicit abstain",
            "- no_view != zero_alpha",
            "",
            "## 4. Q1 Evidence",
            f"- evidence_rows: {len(evidence.factor_evidence_table)}",
            "",
            "## 5. Redundancy and Marginal Value",
            f"- cluster_count: {redundancy.marginal_value_report['cluster_count']}",
            f"- promoted_count: {redundancy.marginal_value_report['promoted_count']}",
            "",
            "## 6. Shrinkage and Covariance",
            f"- mean_shrinkage_intensity: {shrinkage.summary['mean_shrinkage_intensity']}",
            f"- condition_number_after: {covariance.diagnostics['condition_number_after']}",
            "",
            "## 7. Factor Allocator",
            f"- active_factor_count: {allocator.diagnostics['active_factor_count']}",
            f"- zero_weight_count: {allocator.diagnostics['zero_weight_count']}",
            "",
            "## 8. Zero-Weight Attribution",
            allocator.zero_weight_attribution.to_markdown(index=False),
            "",
            "## 9. Cost and Capacity Survival",
            f"- cost_killed_rows: {int((survival.cost_stress_matrix['cost_survival_status'] == 'cost_killed').sum())}",
            f"- capacity_rows: {len(survival.capacity_frontier)}",
            "",
            "## 10. Final Registry",
            registry.decision_table[["factor_id", "final_status", "stop_layer"]].to_markdown(index=False),
            "",
            "## 11. Non-Claims",
            "- No production approval.",
            "- No live trading.",
            "- No security-level output.",
            "- No direct Q2 entry.",
            "- No claim that public factors are proprietary alpha.",
            "",
        ]
    )


def write_research_report(report: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "multifactor_alpha_validation_report.md"
    path.write_text(report, encoding="utf-8")
    return path


def write_release_manifest(output_dir: Path, artifact_paths: list[Path], survival: SurvivalResult) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "factor_release_manifest.v1",
        "artifact_count": len(artifact_paths),
        "artifacts": [str(path) for path in artifact_paths],
        "survival_layers": int(len(survival.survival_funnel)),
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }
    path = output_dir / "artifact_manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path

