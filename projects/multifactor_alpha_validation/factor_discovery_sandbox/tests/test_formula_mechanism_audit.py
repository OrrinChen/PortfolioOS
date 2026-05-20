from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.formula_mechanism_audit import run_formula_mechanism_audit


def test_formula_mechanism_audit_writes_artifacts_and_flags_exact_duplicates(tmp_path: Path) -> None:
    panel_path = tmp_path / "real_factor_panel.csv"
    _write_panel(panel_path)

    result = run_formula_mechanism_audit(
        factor_panel_path=panel_path,
        output_dir=tmp_path / "audit",
        report_path=tmp_path / "factor_formula_mechanism_v2_audit.md",
    )

    assert result.summary["schema_version"] == "fd_formula_mechanism_audit_summary.v1"
    assert result.summary["formula_version"] == "price_volume_29_mechanism_v2"
    assert result.summary["allocator_entry_allowed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["not_alpha_evidence"] is True

    expected = {
        "formula_mechanism_audit",
        "duplicate_cluster_audit",
        "rank_identity_audit",
        "formula_mechanism_audit_report",
    }
    assert expected == set(result.artifacts)

    audit = pd.read_csv(result.artifacts["formula_mechanism_audit"])
    duplicate_pair = audit[
        (audit["factor_id_a"] == "factor_a") & (audit["factor_id_b"] == "factor_b_duplicate")
    ].iloc[0]
    assert duplicate_pair["is_formula_duplicate"] is True or bool(duplicate_pair["is_formula_duplicate"])
    assert bool(duplicate_pair["is_rank_duplicate"])
    assert duplicate_pair["decision"] == "hard_fail_formula_or_rank_duplicate"

    sign_pair = audit[(audit["factor_id_a"] == "factor_a") & (audit["factor_id_b"] == "factor_c_signflip")].iloc[0]
    assert bool(sign_pair["is_sign_flip_duplicate"])
    assert sign_pair["decision"] == "hard_fail_formula_or_rank_duplicate"

    clusters = pd.read_csv(result.artifacts["duplicate_cluster_audit"])
    assert {"factor_a", "factor_b_duplicate"}.issubset(set(clusters["factor_id"]))

    report = result.artifacts["formula_mechanism_audit_report"].read_text(encoding="utf-8").lower()
    assert "formula mechanism validation only" in report
    assert "allocator entry: blocked" in report


def _write_panel(path: Path) -> None:
    rows = []
    for date in ["2020-01-31", "2020-02-28"]:
        for asset_index, asset_id in enumerate(["A", "B", "C", "D"]):
            base = float(asset_index + 1)
            independent = float((asset_index * 2 + 1) % 5)
            for factor_id, value, family in [
                ("factor_a", base, "family_a"),
                ("factor_b_duplicate", base, "family_b"),
                ("factor_c_signflip", -base, "family_c"),
                ("factor_d_independent", independent, "family_d"),
            ]:
                rows.append(
                    {
                        "schema_version": "fd_real_factor_panel.v2",
                        "factor_id": factor_id,
                        "formula_version": "price_volume_29_mechanism_v2",
                        "formula_hash": factor_id,
                        "mechanism_family": family,
                        "date": date,
                        "asset_id": asset_id,
                        "coverage_status": "active_view",
                        "oriented_score": value,
                        "normalized_value": value,
                        "not_alpha_evidence": True,
                        "direct_q2_entry_allowed": False,
                    }
                )
    pd.DataFrame(rows).to_csv(path, index=False)
