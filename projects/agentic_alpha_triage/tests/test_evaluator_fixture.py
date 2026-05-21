from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from agentic_alpha_triage.evaluator_fixture import (
    load_evaluator_fixture,
    load_evaluator_fixtures,
)


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
FIXTURES_DIR = EXAMPLES_DIR / "evaluator_fixtures"


def test_valid_evaluator_fixture_references_guidance_raise_example() -> None:
    fixture = load_evaluator_fixture(FIXTURES_DIR / "valid" / "guidance_raise_drift.yaml")

    assert fixture.hypothesis_id == "H-SEC-GUIDANCE-RAISE-001"
    assert fixture.signal_name == "guidance_raise_drift"
    assert fixture.evaluation_contract_path == "../../evaluation_guidance_raise_drift.yaml"
    assert fixture.uses_future_data_as_feature is False


def test_loader_rejects_leakage_risk_fixture() -> None:
    with pytest.raises(ValidationError, match="uses_future_data_as_feature"):
        load_evaluator_fixture(FIXTURES_DIR / "invalid" / "guidance_raise_forward_return_leakage.yaml")


def test_fixture_collection_loader_loads_only_valid_directory() -> None:
    fixtures = load_evaluator_fixtures(FIXTURES_DIR / "valid")

    assert [fixture.fixture_id for fixture in fixtures] == ["EV-GUIDANCE-RAISE-DRIFT-001"]
