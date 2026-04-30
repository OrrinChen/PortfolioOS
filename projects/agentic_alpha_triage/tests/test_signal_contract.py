from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentic_alpha_triage.signal_contract import SignalContract


def test_signal_contract_requires_timestamp_safe_outputs() -> None:
    contract = SignalContract(
        signal_name="guidance_raise_drift",
        input_fields=["event_available_timestamp", "symbol", "guidance_delta"],
        output_column="alpha_score",
        valid_universe="US common stocks with timestamp-safe SEC events",
        timestamp_column="event_available_timestamp",
        no_future_data_required=True,
        implementation_path="signals/guidance_raise_drift.py",
    )

    assert contract.output_column == "alpha_score"
    assert contract.no_future_data_required is True


def test_signal_contract_rejects_future_data_dependency() -> None:
    with pytest.raises(ValidationError, match="no_future_data_required"):
        SignalContract(
            signal_name="bad_signal",
            input_fields=["future_return"],
            output_column="alpha_score",
            valid_universe="US stocks",
            timestamp_column="event_available_timestamp",
            no_future_data_required=False,
        )
