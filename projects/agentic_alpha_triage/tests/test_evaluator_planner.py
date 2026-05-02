from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from agentic_alpha_triage.evaluator_planner import build_evaluator_plan


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
FIXTURE_PATH = EXAMPLES_DIR / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
EVENT_REGISTRY_DIR = EXAMPLES_DIR / "event_registry" / "valid"


def test_build_evaluator_plan_from_guidance_raise_fixture() -> None:
    plan = build_evaluator_plan(FIXTURE_PATH, event_registry_dir=EVENT_REGISTRY_DIR)

    assert plan.plan_id == "PLAN-EV-GUIDANCE-RAISE-DRIFT-001"
    assert plan.fixture_id == "EV-GUIDANCE-RAISE-DRIFT-001"
    assert plan.hypothesis_id == "H-SEC-GUIDANCE-RAISE-001"
    assert plan.signal_name == "guidance_raise_drift"
    assert plan.event_registry_ids == ["REG-GUIDANCE-RAISE-001"]
    assert plan.required_input_columns == [
        "event_available_timestamp",
        "anchor_trade_date",
        "symbol",
        "guidance_delta",
        "prior_guidance_delta",
        "sector",
        "alpha_score",
    ]
    assert plan.feature_columns == ["guidance_delta", "prior_guidance_delta", "sector"]
    assert plan.output_column == "alpha_score"
    assert plan.label_column == "realized_forward_return_5d"
    assert plan.holding_windows == ["1d", "5d", "20d"]
    assert plan.benchmark == "sector_neutral_equal_weight_event_cohort"
    assert plan.cost_assumptions == {
        "commission_bps": 1.0,
        "half_spread_bps": 5.0,
        "slippage_bps": 5.0,
    }
    assert plan.status == "ready_for_local_evaluation"
    assert plan.rejection_reasons == []
    dumped = plan.model_dump(mode="json")
    assert "realized_return" not in dumped
    assert "orders" not in dumped


def test_evaluator_plan_rejects_signal_name_mismatch(tmp_path: Path) -> None:
    copied_examples = _copy_examples(tmp_path)
    fixture_path = copied_examples / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
    signal_path = copied_examples / "signal_guidance_raise_drift.yaml"
    payload = _load_yaml(signal_path)
    payload["signal_name"] = "different_signal"
    signal_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="signal_name"):
        build_evaluator_plan(fixture_path, event_registry_dir=copied_examples / "event_registry" / "valid")


def test_evaluator_plan_rejects_event_registry_hypothesis_mismatch(tmp_path: Path) -> None:
    copied_examples = _copy_examples(tmp_path)
    fixture_path = copied_examples / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
    registry_path = copied_examples / "event_registry" / "valid" / "guidance_raise_event.yaml"
    payload = _load_yaml(registry_path)
    payload["hypothesis_id"] = "H-DIFFERENT"
    registry_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Event registry hypothesis_id"):
        build_evaluator_plan(fixture_path, event_registry_dir=copied_examples / "event_registry" / "valid")


def test_evaluator_plan_rejects_evaluation_timestamp_field_mismatch(tmp_path: Path) -> None:
    copied_examples = _copy_examples(tmp_path)
    fixture_path = copied_examples / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
    evaluation_path = copied_examples / "evaluation_guidance_raise_drift.yaml"
    payload = _load_yaml(evaluation_path)
    payload["event_available_timestamp"] = "different_timestamp_column"
    evaluation_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="event_available_timestamp"):
        build_evaluator_plan(fixture_path, event_registry_dir=copied_examples / "event_registry" / "valid")


def _copy_examples(tmp_path: Path) -> Path:
    copied_examples = tmp_path / "examples"
    shutil.copytree(EXAMPLES_DIR, copied_examples)
    return copied_examples


def _load_yaml(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    assert isinstance(payload, dict)
    return payload
