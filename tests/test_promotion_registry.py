from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.domain.errors import InputValidationError


def _write_contract_fixture(
    root_dir: Path,
    *,
    bundle_name: str,
    bundle_id: str,
    research_line: str,
    candidate_status: str,
    eligible_for_stage4: bool,
    blocking_reason: str,
) -> Path:
    bundle_dir = root_dir / bundle_name
    artifacts_dir = bundle_dir / "artifacts"
    artifacts_dir.mkdir(parents=True)

    for name in (
        "signal_audit_summary.json",
        "signal_b_audit_summary.json",
        "combo_summary.json",
        "MEMORY.md",
        "ledger.md",
    ):
        path = artifacts_dir / name
        if path.suffix == ".json":
            path.write_text("{}", encoding="utf-8")
        else:
            path.write_text("# artifact\n", encoding="utf-8")

    manifest = {
        "contract_type": "portfolio_os_research_promotion_bundle",
        "contract_version": "1.0",
        "bundle_id": bundle_id,
        "created_at": "2026-04-15T12:00:00Z",
        "research_line": research_line,
        "candidate_status": candidate_status,
        "thesis": {
            "summary": f"{research_line} promotion candidate.",
            "universe_name": f"{research_line}_dynamic_universe",
        },
        "signals": [
            {
                "name": "signal_a",
                "stage_bucket": "partially_real",
                "audit_summary_path": "artifacts/signal_audit_summary.json",
            },
            {
                "name": "signal_b",
                "stage_bucket": "negative",
                "audit_summary_path": "artifacts/signal_b_audit_summary.json",
            },
        ],
        "combo": {
            "summary_path": "artifacts/combo_summary.json",
            "eligible_for_stage4": eligible_for_stage4,
            "blocking_reason": blocking_reason,
            "full_sample_ir": 0.25,
            "second_half_ir": 0.05,
        },
        "artifacts": {
            "memory_path": "artifacts/MEMORY.md",
            "ledger_path": "artifacts/ledger.md",
        },
    }
    (bundle_dir / "promotion_bundle.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return bundle_dir


def test_run_promotion_registry_builds_outputs_for_multiple_bundles(tmp_path: Path) -> None:
    from portfolio_os.workflow.promotion_registry import run_promotion_registry

    input_root = tmp_path / "bundle_root"
    output_dir = tmp_path / "registry_output"
    _write_contract_fixture(
        input_root / "ashare",
        bundle_name="bundle_a",
        bundle_id="ashare_bundle",
        research_line="ashare",
        candidate_status="stage3_candidate_not_promoted",
        eligible_for_stage4=False,
        blocking_reason="second-half combo IR is too weak",
    )
    _write_contract_fixture(
        input_root / "us",
        bundle_name="bundle_b",
        bundle_id="us_bundle",
        research_line="us",
        candidate_status="methods_asset_only",
        eligible_for_stage4=False,
        blocking_reason="package baseline underperformed pure SUE",
    )

    result = run_promotion_registry(input_root=input_root, output_dir=output_dir)

    assert result.bundle_count == 2
    assert result.registry_csv_path.exists()
    assert result.manifest_path.exists()
    assert result.summary_path.exists()

    registry = pd.read_csv(result.registry_csv_path)
    assert registry["bundle_id"].tolist() == ["ashare_bundle", "us_bundle"]
    assert registry["research_line"].tolist() == ["ashare", "us"]
    assert registry["signal_names"].tolist() == ["signal_a, signal_b", "signal_a, signal_b"]
    assert registry["combo_eligible_for_stage4"].tolist() == [False, False]

    summary = result.summary_path.read_text(encoding="utf-8")
    assert "Bundle Count: 2" in summary
    assert "stage3_candidate_not_promoted" in summary
    assert "methods_asset_only" in summary
    assert "package baseline underperformed pure SUE" in summary

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["bundle_count"] == 2
    assert manifest["input_root"] == str(input_root.resolve())


def test_run_promotion_registry_rejects_empty_scan_root(tmp_path: Path) -> None:
    from portfolio_os.workflow.promotion_registry import run_promotion_registry

    with pytest.raises(InputValidationError, match="No promotion bundles found"):
        run_promotion_registry(
            input_root=tmp_path / "empty_root",
            output_dir=tmp_path / "registry_output",
        )
