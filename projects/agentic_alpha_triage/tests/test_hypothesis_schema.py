from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentic_alpha_triage.hypothesis_schema import Hypothesis


def test_hypothesis_schema_accepts_complete_hypothesis() -> None:
    hypothesis = Hypothesis(
        hypothesis_id="H-001",
        title="Announcement drift after clean guidance raise",
        description="Tests whether post-event drift survives timestamp-safe evaluation.",
        economic_rationale="Investors may underreact to credible guidance revisions.",
        required_data=["sec_filings", "prices", "fundamentals"],
        signal_direction="positive",
        expected_horizon="5-20 trading days",
        timestamp_assumptions=["event timestamp is filing acceptance time"],
        risk_notes="Crowded earnings-momentum overlap must be tested.",
    )

    assert hypothesis.hypothesis_id == "H-001"
    assert hypothesis.required_data == ["sec_filings", "prices", "fundamentals"]


def test_hypothesis_schema_rejects_missing_required_data() -> None:
    with pytest.raises(ValidationError):
        Hypothesis(
            hypothesis_id="H-002",
            title="Incomplete",
            description="Missing data requirements.",
            economic_rationale="N/A",
            required_data=[],
            signal_direction="positive",
            expected_horizon="5 days",
            timestamp_assumptions=["strict close-to-close alignment"],
            risk_notes="N/A",
        )
