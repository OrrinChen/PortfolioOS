from __future__ import annotations

import json
from pathlib import Path

import pytest

from evidence_bundle import deterministic_bundle_json, load_evidence_bundle


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "examples"
FORBIDDEN_OUTPUT_KEYS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "hidden_q2_results",
}


def test_valid_evidence_bundle_loads_and_serializes_deterministically() -> None:
    bundle = load_evidence_bundle(EXAMPLES_DIR / "valid_bundle.yaml")

    assert bundle.bundle_id == "EB-GUIDANCE-RAISE-DRIFT-001"
    assert bundle.status == "ready"
    assert bundle.promotion_eligibility == "eligible_for_promotion_review"
    assert bundle.rejection_reasons == []
    assert all(check.passed for check in bundle.leakage_checks)
    assert FORBIDDEN_OUTPUT_KEYS.isdisjoint(bundle.model_dump(mode="json"))

    first = deterministic_bundle_json(bundle)
    second = deterministic_bundle_json(bundle)

    assert first == second
    assert json.loads(first)["bundle_id"] == "EB-GUIDANCE-RAISE-DRIFT-001"


@pytest.mark.parametrize(
    ("example_name", "message"),
    [
        ("rejected_bundle_forward_leakage.yaml", "forward-return leakage"),
        ("rejected_bundle_missing_timestamp.yaml", "timestamp"),
        ("rejected_bundle_anchor_before_signal_timestamp.yaml", "anchor_trade_date"),
    ],
)
def test_unsafe_evidence_bundles_are_rejected(example_name: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        load_evidence_bundle(EXAMPLES_DIR / example_name)
