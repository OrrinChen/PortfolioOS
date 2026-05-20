from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.factor_design_d1 import build_design_ledger
from factor_discovery_sandbox.factor_design_d2 import (
    REQUIRED_D2_COLUMNS,
    run_factor_design_d2,
    validate_pre_formula_diagnostics,
)


def test_design_d2_writes_pre_formula_diagnostics_and_decisions(tmp_path: Path) -> None:
    result = run_factor_design_d2(output_dir=tmp_path / "outputs" / "factor_discovery" / "design_layer" / "d2")

    assert result.summary["schema_version"] == "fd_factor_design_d2_summary.v1"
    assert result.summary["stage"] == "FD-D2"
    assert result.summary["pre_formula_diagnostics_only"] is True
    assert result.summary["formula_validation_ran"] is False
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False

    assert set(result.artifacts) == {
        "pre_formula_diagnostics",
        "candidate_family_d2_decisions",
        "pre_formula_diagnostic_summary",
        "pre_formula_diagnostic_report",
    }
    assert {path.name for path in result.artifacts.values()} == {
        "pre_formula_diagnostics.csv",
        "candidate_family_d2_decisions.json",
        "pre_formula_diagnostic_summary.json",
        "pre_formula_diagnostic_report.md",
    }

    diagnostics = pd.read_csv(result.artifacts["pre_formula_diagnostics"])
    assert set(REQUIRED_D2_COLUMNS).issubset(diagnostics.columns)
    assert len(diagnostics) == 6
    assert diagnostics["not_alpha_evidence"].eq(True).all()
    assert diagnostics["direct_q2_entry_allowed"].eq(False).all()
    assert diagnostics["formula_validation_ran"].eq(False).all()

    decisions = diagnostics.set_index("candidate_family_id")["d2_decision_label"].to_dict()
    assert decisions["revision_confirmed_earnings_underreaction"] == "blocked_coverage_alignment"
    assert decisions["sue_event_timing_and_timestamp_repair"] == "blocked_timestamp_observability"
    assert decisions["small_cap_quality_residual_momentum_v1"] == "blocked_placebo_prior"
    assert decisions["sector_neutral_residual_momentum"] == "ready_for_d3_charter"
    assert decisions["liquidity_activity_shock"] == "needs_pre_formula_data_diagnostics"

    decision_payload = json.loads(result.artifacts["candidate_family_d2_decisions"].read_text(encoding="utf-8"))
    assert decision_payload["schema_version"] == "fd_candidate_family_d2_decisions.v1"
    assert decision_payload["ready_for_d3_count"] == 1
    assert decision_payload["formula_validation_allowed_count"] == 0
    assert all(item["formula_validation_allowed"] is False for item in decision_payload["candidate_families"])
    assert all(item["q1_candidate_review_eligible"] is False for item in decision_payload["candidate_families"])

    report = result.artifacts["pre_formula_diagnostic_report"].read_text(encoding="utf-8").lower()
    assert "pre-formula diagnostics" in report
    assert "not alpha evidence" in report
    assert "direct q2 entry: not allowed" in report
    assert "ready_for_d3_charter" in report


def test_pre_formula_diagnostics_validation_rejects_formula_validation_rows() -> None:
    diagnostics = pd.DataFrame(
        [
            {
                column: "documented"
                for column in REQUIRED_D2_COLUMNS
                if column
                not in {
                    "formula_validation_ran",
                    "not_alpha_evidence",
                    "direct_q2_entry_allowed",
                }
            }
        ]
    )
    diagnostics["formula_validation_ran"] = True
    diagnostics["not_alpha_evidence"] = True
    diagnostics["direct_q2_entry_allowed"] = False

    result = validate_pre_formula_diagnostics(diagnostics)

    assert result["valid"] is False
    assert result["invalid_row_count"] == 1
    assert "formula_validation_ran_in_d2" in result["failure_reasons"][0]


def test_design_d2_can_consume_existing_d1_ledger(tmp_path: Path) -> None:
    d1_ledger_path = tmp_path / "factor_design_ledger.csv"
    build_design_ledger().to_csv(d1_ledger_path, index=False)

    result = run_factor_design_d2(
        output_dir=tmp_path / "outputs" / "factor_discovery" / "design_layer" / "d2",
        d1_ledger_path=d1_ledger_path,
    )

    diagnostics = pd.read_csv(result.artifacts["pre_formula_diagnostics"])
    assert diagnostics["d1_ledger_source"].eq(str(d1_ledger_path)).all()
    assert result.summary["d1_ledger_source"] == str(d1_ledger_path)


def test_design_d2_has_no_downstream_import_surface() -> None:
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "factor_discovery_sandbox"
        / "factor_design_d2.py"
    )
    module_source = module_path.read_text(encoding="utf-8")
    import_surface = "\n".join(
        line for line in module_source.splitlines() if line.startswith(("import ", "from "))
    )
    forbidden_import_fragments = [
        "agentic_alpha_triage",
        "execution_aware_optimizer",
        "promotion_gate",
        "alpha_registry",
        "portfolio_os.alpha.projection",
        "typed_alpha_pilot",
    ]
    for fragment in forbidden_import_fragments:
        assert fragment not in import_surface
