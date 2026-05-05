from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from portfolio_os.alpha.registry_v2 import (
    AlphaRegistryEntry,
    build_default_alpha_registry_v2,
    render_alpha_registry_report,
    write_alpha_registry_v2_artifacts,
)


def test_alpha_registry_v2_freezes_required_alpha_statuses() -> None:
    registry = build_default_alpha_registry_v2()

    entries = {entry.alpha_id: entry for entry in registry.entries}
    assert {
        "sue_pead",
        "revision_1m",
        "sue_revision_composite",
        "phase_1_5_bridge",
        "qlib_fixed_horizon_revision",
        "residual_momentum_reversal",
        "ashare_anti_mom_21_5",
        "forward_return_leakage_fixture",
    }.issubset(entries)

    assert entries["sue_pead"].primary_status == "canonical_pilot"
    assert entries["sue_pead"].typed_chain_stop_layer == "q2_observed_survives_local_fixture"
    assert "eligible_for_q2_eval" in entries["sue_pead"].status_history
    assert "q2_observed_survives" in entries["sue_pead"].status_history

    assert entries["revision_1m"].primary_status == "real_shadow_branch"
    assert entries["revision_1m"].typed_chain_stop_layer == "revision_marginal_value_gate"
    assert "archived_no_marginal_value" in entries["revision_1m"].status_history

    assert entries["residual_momentum_reversal"].primary_status == "calibration_only"
    assert entries["ashare_anti_mom_21_5"].primary_status == "background_partially_real"
    assert entries["forward_return_leakage_fixture"].primary_status == "rejected_leakage"

    assert all(entry.production_approval_claimed is False for entry in registry.entries)
    assert all(entry.live_trading_allowed is False for entry in registry.entries)
    assert all(entry.primary_status not in {"pass", "passed", "fail", "failed"} for entry in registry.entries)


def test_alpha_registry_entry_rejects_pass_fail_labels_and_missing_stop_layer() -> None:
    with pytest.raises(ValueError, match="pass/fail"):
        AlphaRegistryEntry.model_validate(
            {
                "alpha_id": "bad_pass_fail",
                "display_name": "Bad Pass/Fail",
                "primary_status": "passed",
                "status_history": ["passed"],
                "typed_chain_stop_layer": "q2",
                "decision_source_phase": "test",
                "decision_source_artifact": "fixture",
                "decision_summary": "bad status",
                "next_allowed_work": "none",
            }
        )

    with pytest.raises(ValueError, match="typed_chain_stop_layer"):
        AlphaRegistryEntry.model_validate(
            {
                "alpha_id": "missing_stop",
                "display_name": "Missing Stop",
                "primary_status": "diagnostic_only",
                "status_history": ["diagnostic_only"],
                "typed_chain_stop_layer": "",
                "decision_source_phase": "test",
                "decision_source_artifact": "fixture",
                "decision_summary": "missing stop layer",
                "next_allowed_work": "none",
            }
        )


def test_alpha_registry_writer_outputs_yaml_csv_and_report(tmp_path: Path) -> None:
    registry = build_default_alpha_registry_v2()

    artifacts = write_alpha_registry_v2_artifacts(registry, tmp_path)

    assert {
        "alpha_registry.yaml",
        "alpha_registry_decision_table.csv",
        "alpha_registry_report.md",
    } == {path.name for path in artifacts.values()}

    payload = yaml.safe_load((tmp_path / "alpha_registry.yaml").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "alpha_registry.v2"
    assert payload["registry_id"] == "portfolioos_alpha_registry_v2"
    assert len(payload["entries"]) >= 8

    table = pd.read_csv(tmp_path / "alpha_registry_decision_table.csv")
    assert {
        "alpha_id",
        "primary_status",
        "typed_chain_stop_layer",
        "decision_source_phase",
        "production_approval_claimed",
        "live_trading_allowed",
    }.issubset(table.columns)
    assert not table["primary_status"].isin(["pass", "passed", "fail", "failed"]).any()
    assert table.loc[table["alpha_id"] == "revision_1m", "primary_status"].item() == "real_shadow_branch"

    report = render_alpha_registry_report(registry)
    assert "Alpha Registry v2" in report
    assert "SUE / PEAD" in report
    assert "revision_1m" in report
    assert "production approval: not claimed" in report
    assert "no live trading allowed by registry" in report
    assert "broker_output" not in report
    assert "recommended_trade" not in report
    assert "production_alpha_approved" not in report
