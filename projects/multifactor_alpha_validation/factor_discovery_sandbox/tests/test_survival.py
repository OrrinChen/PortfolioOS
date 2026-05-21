from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.survival import run_survival_analysis


def test_survival_analysis_writes_cost_capacity_benchmark_and_import_bundle(tmp_path: Path) -> None:
    result = run_survival_analysis(tmp_path)

    assert {
        "cost_stress_matrix.csv",
        "capacity_frontier.csv",
        "benchmark_attribution.csv",
        "survival_funnel.csv",
        "final_factor_discovery_report.md",
        "research_import_bundle.json",
    } == {path.name for path in result.artifacts.values()}
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["reports_benchmark_attribution"] is True

    cost = pd.read_csv(tmp_path / "cost_stress_matrix.csv")
    assert {"cost_bps", "raw_annualized_return", "cost_adjusted_annualized_return", "cost_drag"}.issubset(
        cost.columns
    )
    assert cost["cost_adjusted_annualized_return"].is_monotonic_decreasing

    capacity = pd.read_csv(tmp_path / "capacity_frontier.csv")
    assert {
        "participation_rate",
        "capacity_adjusted_annualized_return",
        "capacity_drag",
        "capacity_status",
    }.issubset(capacity.columns)

    attribution = pd.read_csv(tmp_path / "benchmark_attribution.csv")
    required_metrics = {
        "raw_annualized_return",
        "qqq_relative_annualized_return",
        "beta_adjusted_annualized_return",
        "beta",
        "sector_tech_exposure",
        "style_growth_exposure",
        "liquidity_exposure",
    }
    assert required_metrics.issubset(set(attribution["metric"]))

    funnel = pd.read_csv(tmp_path / "survival_funnel.csv")
    assert {"stage", "status", "remaining_candidate_count", "reason"}.issubset(funnel.columns)
    assert "cost_capacity_benchmark_survival" in set(funnel["stage"])

    report = (tmp_path / "final_factor_discovery_report.md").read_text(encoding="utf-8")
    assert "raw vs QQQ-relative vs beta-adjusted" in report
    assert "cost-adjusted" in report
    assert "capacity-adjusted" in report
    assert "sector / style / liquidity exposure" in report
    assert "tech concentration" in report
    assert "production approval: not claimed" in report

    bundle = json.loads((tmp_path / "research_import_bundle.json").read_text(encoding="utf-8"))
    assert bundle["schema_version"] == "factor_discovery_research_import_bundle.v1"
    assert bundle["direct_q2_entry_allowed"] is False
    assert bundle["recommended_import_decision"] in {
        "import_rejected",
        "import_needs_more_evidence",
        "import_as_calibration_only",
        "import_as_shadow_branch",
        "import_to_q1_evidence",
    }
