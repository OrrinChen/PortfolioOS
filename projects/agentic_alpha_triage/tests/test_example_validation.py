from pathlib import Path

import pytest

from agentic_alpha_triage.example_validation import validate_example_directory


def test_committed_examples_validate_against_contracts() -> None:
    examples_dir = Path(__file__).resolve().parents[1] / "examples"

    result = validate_example_directory(examples_dir)

    assert result.hypothesis_count == 1
    assert result.signal_contract_count == 1
    assert result.evaluation_contract_count == 1
    assert result.evaluator_fixture_count == 1
    assert result.rejected_evaluator_fixture_count == 1
    assert result.event_registry_example_count == 1
    assert result.rejected_event_registry_example_count == 2
    assert result.evaluator_plan_manifest_count == 1
    assert sorted(Path(path).name for path in result.validated_paths) == [
        "evaluation_guidance_raise_drift.yaml",
        "evaluator_plan_manifest.yaml",
        "hypothesis_guidance_raise_drift.yaml",
        "signal_guidance_raise_drift.yaml",
    ]


def test_example_validation_requires_each_contract_type(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No example files matched hypothesis_.*yaml"):
        validate_example_directory(tmp_path)
