from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from agentic_alpha_triage.event_registry_schema import (
    load_event_registry_example,
    load_event_registry_examples,
)


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
EVENT_REGISTRY_DIR = EXAMPLES_DIR / "event_registry"


def test_valid_event_registry_example_records_timestamp_safe_guidance_event() -> None:
    example = load_event_registry_example(EVENT_REGISTRY_DIR / "valid" / "guidance_raise_event.yaml")

    assert example.registry_id == "REG-GUIDANCE-RAISE-001"
    assert example.hypothesis_id == "H-SEC-GUIDANCE-RAISE-001"
    assert example.events[0].event_id == "EVT-GUIDANCE-RAISE-ACME-2026Q1"
    assert example.events[0].anchor_trade_date == "2026-02-03"


def test_event_registry_loader_rejects_missing_event_timestamp() -> None:
    with pytest.raises(ValidationError, match="event_available_timestamp"):
        load_event_registry_example(EVENT_REGISTRY_DIR / "invalid" / "guidance_raise_missing_timestamp.yaml")


def test_event_registry_loader_rejects_anchor_before_event_visibility() -> None:
    with pytest.raises(ValidationError, match="anchor_trade_date"):
        load_event_registry_example(EVENT_REGISTRY_DIR / "invalid" / "guidance_raise_anchor_before_event.yaml")


def test_event_registry_collection_loader_loads_valid_examples() -> None:
    examples = load_event_registry_examples(EVENT_REGISTRY_DIR / "valid")

    assert [example.registry_id for example in examples] == ["REG-GUIDANCE-RAISE-001"]
