"""Local evaluator-plan manifest schema for Q1 dry-run inspection."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


ManifestExpectedStatus = Literal["ready_for_local_evaluation", "rejected"]


class EvaluatorPlanManifestEntry(BaseModel):
    """One local evaluator fixture target listed by a Q1 manifest."""

    entry_id: str
    fixture_path: str
    event_registry_dir: str
    expected_status: ManifestExpectedStatus
    description: str

    @field_validator("entry_id", "fixture_path", "event_registry_dir", "description")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank manifest entry text fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text


class EvaluatorPlanManifest(BaseModel):
    """Local-only Q1 manifest for deterministic evaluator-plan dry-run targets."""

    manifest_id: str
    description: str
    entries: list[EvaluatorPlanManifestEntry] = Field(min_length=1)

    @field_validator("manifest_id", "description")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank manifest text fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @model_validator(mode="after")
    def require_unique_entry_ids(self) -> Self:
        """Reject ambiguous manifests with duplicate entry ids."""

        entry_ids = [entry.entry_id for entry in self.entries]
        if len(entry_ids) != len(set(entry_ids)):
            raise ValueError("entry_id values must be unique")
        return self


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _require_referenced_paths(path: Path, manifest: EvaluatorPlanManifest) -> None:
    for entry in manifest.entries:
        fixture_path = (path.parent / entry.fixture_path).resolve()
        if not fixture_path.exists():
            raise ValueError(f"Referenced evaluator fixture path does not exist: {entry.fixture_path}")
        if not fixture_path.is_file():
            raise ValueError(f"Referenced evaluator fixture path is not a file: {entry.fixture_path}")

        event_registry_dir = (path.parent / entry.event_registry_dir).resolve()
        if not event_registry_dir.exists():
            raise ValueError(f"Referenced event registry directory does not exist: {entry.event_registry_dir}")
        if not event_registry_dir.is_dir():
            raise ValueError(f"Referenced event registry path is not a directory: {entry.event_registry_dir}")


def load_evaluator_plan_manifest(path: str | Path) -> EvaluatorPlanManifest:
    """Load one local evaluator-plan manifest without executing planner targets."""

    resolved_path = Path(path)
    manifest = EvaluatorPlanManifest.model_validate(_load_yaml_mapping(resolved_path))
    _require_referenced_paths(resolved_path, manifest)
    return manifest
