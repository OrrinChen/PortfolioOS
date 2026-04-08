from __future__ import annotations

import json
from pathlib import Path

import pytest

from portfolio_os.domain.errors import InputValidationError


def _write_contract_fixture(tmp_path: Path) -> Path:
    bundle_dir = tmp_path / "promotion_bundle"
    artifacts_dir = bundle_dir / "artifacts"
    artifacts_dir.mkdir(parents=True)

    for name in (
        "antimom_audit_summary.json",
        "institutional_crowding_audit_summary.json",
        "stage3_combo_validation_summary.json",
        "ASHARE_MEMORY.md",
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
        "bundle_id": "ashare_stage3_candidate_2026-04-08",
        "created_at": "2026-04-08T12:00:00Z",
        "research_line": "ashare",
        "candidate_status": "stage3_candidate_not_promoted",
        "thesis": {
            "summary": "A-share Stage 3 pair with partial diversification but no Stage 4 promotion.",
            "universe_name": "cn_rank_500_1500_floatcap_dynamic",
        },
        "signals": [
            {
                "name": "anti_mom_21_5",
                "stage_bucket": "partially_real",
                "audit_summary_path": "artifacts/antimom_audit_summary.json",
            },
            {
                "name": "institutional_crowding",
                "stage_bucket": "partially_real",
                "audit_summary_path": "artifacts/institutional_crowding_audit_summary.json",
            },
        ],
        "combo": {
            "summary_path": "artifacts/stage3_combo_validation_summary.json",
            "eligible_for_stage4": False,
            "blocking_reason": "second-half combo IR does not exceed the best single signal.",
            "full_sample_ir": 0.2657,
            "second_half_ir": 0.0537,
        },
        "artifacts": {
            "memory_path": "artifacts/ASHARE_MEMORY.md",
            "ledger_path": "artifacts/ledger.md",
        },
    }
    (bundle_dir / "promotion_bundle.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return bundle_dir


def test_load_promotion_contract_resolves_bundle_metadata(tmp_path: Path) -> None:
    from portfolio_os.alpha.promotion_contract import load_promotion_contract

    bundle_dir = _write_contract_fixture(tmp_path)

    contract = load_promotion_contract(bundle_dir)

    assert contract.bundle_id == "ashare_stage3_candidate_2026-04-08"
    assert contract.research_line == "ashare"
    assert contract.contract_version == "1.0"
    assert contract.candidate_status == "stage3_candidate_not_promoted"
    assert len(contract.signals) == 2
    assert contract.signals[0].audit_summary_path == bundle_dir / "artifacts" / "antimom_audit_summary.json"
    assert contract.combo.summary_path == bundle_dir / "artifacts" / "stage3_combo_validation_summary.json"
    assert contract.memory_path == bundle_dir / "artifacts" / "ASHARE_MEMORY.md"


def test_load_promotion_contract_rejects_missing_artifacts(tmp_path: Path) -> None:
    from portfolio_os.alpha.promotion_contract import load_promotion_contract

    bundle_dir = _write_contract_fixture(tmp_path)
    (bundle_dir / "artifacts" / "stage3_combo_validation_summary.json").unlink()

    with pytest.raises(InputValidationError, match="Missing promotion contract artifact"):
        load_promotion_contract(bundle_dir)
