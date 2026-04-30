from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentic_alpha_triage.evaluator_contract import EvaluationContract


def test_evaluation_contract_requires_leakage_and_placebo_tests() -> None:
    contract = EvaluationContract(
        event_available_timestamp="event_available_timestamp",
        anchor_trade_date="next_open_trade_date",
        entry_rule="enter after event is tradable and visible",
        holding_windows=["1d", "5d", "20d"],
        benchmark="sector_neutral_equal_weight",
        cost_assumptions={"commission_bps": 1.0, "spread_bps": 5.0},
        placebo_tests_required=True,
        leakage_tests_required=True,
    )

    assert contract.placebo_tests_required is True
    assert contract.leakage_tests_required is True


def test_evaluation_contract_rejects_disabled_leakage_tests() -> None:
    with pytest.raises(ValidationError, match="leakage_tests_required"):
        EvaluationContract(
            event_available_timestamp="event_available_timestamp",
            anchor_trade_date="next_open_trade_date",
            entry_rule="enter after visibility",
            holding_windows=["5d"],
            benchmark="market",
            cost_assumptions={"commission_bps": 1.0},
            placebo_tests_required=True,
            leakage_tests_required=False,
        )
