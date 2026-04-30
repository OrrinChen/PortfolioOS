"""Validate committed Q1 example artifacts against strict contracts."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from agentic_alpha_triage.evaluator_contract import EvaluationContract
from agentic_alpha_triage.hypothesis_schema import Hypothesis
from agentic_alpha_triage.signal_contract import SignalContract


class ExampleValidationResult(BaseModel):
    """Summary of validated Q1 example artifacts."""

    hypothesis_count: int
    signal_contract_count: int
    evaluation_contract_count: int
    validated_paths: list[str] = Field(default_factory=list)


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _validate_pattern(
    examples_dir: Path,
    pattern: str,
    model_cls: type[BaseModel],
) -> list[Path]:
    paths = sorted(examples_dir.glob(pattern))
    if not paths:
        raise ValueError(f"No example files matched {pattern} in {examples_dir}")

    for path in paths:
        model_cls.model_validate(_load_yaml_mapping(path))

    return paths


def validate_example_directory(examples_dir: str | Path) -> ExampleValidationResult:
    """Validate a Q1 examples directory against hypothesis, signal, and evaluation contracts."""

    resolved_dir = Path(examples_dir)
    if not resolved_dir.exists():
        raise ValueError(f"Examples directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise ValueError(f"Examples path is not a directory: {resolved_dir}")

    hypothesis_paths = _validate_pattern(resolved_dir, "hypothesis_*.yaml", Hypothesis)
    signal_paths = _validate_pattern(resolved_dir, "signal_*.yaml", SignalContract)
    evaluation_paths = _validate_pattern(resolved_dir, "evaluation_*.yaml", EvaluationContract)

    validated_paths = [str(path) for path in hypothesis_paths + signal_paths + evaluation_paths]
    return ExampleValidationResult(
        hypothesis_count=len(hypothesis_paths),
        signal_contract_count=len(signal_paths),
        evaluation_contract_count=len(evaluation_paths),
        validated_paths=validated_paths,
    )
