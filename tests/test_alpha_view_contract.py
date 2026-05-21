from __future__ import annotations

from pathlib import Path

import pytest

from portfolio_os.alpha.view_contract import (
    AlphaView,
    AlphaViewValidationError,
    dump_alpha_view_json,
    load_alpha_view,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = REPO_ROOT / "projects" / "alpha_view_contract"
VALID_FIXTURES = [
    PROJECT_ROOT / "examples" / "valid" / "valid_event_sue_alpha_view.json",
    PROJECT_ROOT / "examples" / "valid" / "valid_revision_to_next_announcement_alpha_view.json",
    PROJECT_ROOT / "examples" / "valid" / "valid_residual_momentum_calibration_alpha_view.json",
]
REJECTED_FORWARD_RETURN = (
    PROJECT_ROOT
    / "examples"
    / "rejected"
    / "rejected_forward_return_leakage_alpha_view.json"
)


def test_valid_alpha_view_fixtures_load_and_dump_deterministically() -> None:
    for fixture_path in VALID_FIXTURES:
        view = load_alpha_view(fixture_path)
        dumped_once = dump_alpha_view_json(view)
        dumped_twice = dump_alpha_view_json(AlphaView.model_validate_json(dumped_once))

        assert dumped_once == dumped_twice
        assert dumped_once.endswith("\n")
        assert view.abstain_policy.mode == "explicit_abstain"
        assert view.coverage_mask.mode == "explicit_abstain"


def test_alpha_view_preserves_no_view_distinct_from_zero_alpha() -> None:
    view = load_alpha_view(VALID_FIXTURES[0])

    assert view.expected_return_view["TSLA"].state == "no_view"
    assert view.expected_return_view["TSLA"].value is None
    assert view.expected_return_view["AAPL"].state == "active_view"
    assert view.expected_return_view["AAPL"].value != 0.0


def test_rejected_forward_return_leakage_fixture_fails() -> None:
    with pytest.raises(AlphaViewValidationError, match="forward-return leakage"):
        load_alpha_view(REJECTED_FORWARD_RETURN)


def test_alpha_view_rejects_forbidden_trading_outputs() -> None:
    payload = load_alpha_view(VALID_FIXTURES[0]).model_dump(mode="json")
    payload["broker_output"] = {"status": "filled"}

    with pytest.raises(AlphaViewValidationError, match="forbidden alpha view field"):
        AlphaView.validate_payload(payload)


def test_alpha_view_rejects_missing_no_view_reason() -> None:
    payload = load_alpha_view(VALID_FIXTURES[0]).model_dump(mode="json")
    payload["expected_return_view"]["TSLA"] = {"state": "no_view"}

    with pytest.raises(AlphaViewValidationError, match="no_view entries require reason"):
        AlphaView.validate_payload(payload)


def test_alpha_view_rejects_zero_alpha_without_active_view_state() -> None:
    payload = load_alpha_view(VALID_FIXTURES[0]).model_dump(mode="json")
    payload["expected_return_view"]["TSLA"] = {"reason": "coverage_missing", "state": "no_view", "value": 0.0}

    with pytest.raises(AlphaViewValidationError, match="no_view entries cannot carry value"):
        AlphaView.validate_payload(payload)
