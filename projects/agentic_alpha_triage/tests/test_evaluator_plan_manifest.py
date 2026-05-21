from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from agentic_alpha_triage.evaluator_plan_manifest import load_evaluator_plan_manifest


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
MANIFEST_PATH = EXAMPLES_DIR / "evaluator_plan_manifest.yaml"
FORBIDDEN_TRADING_KEYS = {"realized_return", "alpha_performance", "orders"}


def test_load_committed_evaluator_plan_manifest() -> None:
    manifest = load_evaluator_plan_manifest(MANIFEST_PATH)

    assert manifest.manifest_id == "MANIFEST-Q1-EVALUATOR-PLAN-001"
    assert [entry.entry_id for entry in manifest.entries] == [
        "ready_guidance_raise_drift",
        "rejected_guidance_raise_forward_return_leakage",
    ]
    assert [entry.expected_status for entry in manifest.entries] == [
        "ready_for_local_evaluation",
        "rejected",
    ]
    assert manifest.entries[0].fixture_path == "evaluator_fixtures/valid/guidance_raise_drift.yaml"
    assert manifest.entries[1].fixture_path == (
        "evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml"
    )
    assert all(entry.event_registry_dir == "event_registry/valid" for entry in manifest.entries)
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(manifest.model_dump(mode="json"))


def test_evaluator_plan_manifest_rejects_duplicate_entry_ids(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    fixture_path = tmp_path / "fixture.yaml"
    registry_dir = tmp_path / "event_registry"
    fixture_path.write_text("fixture_id: placeholder\n", encoding="utf-8")
    registry_dir.mkdir()
    manifest_path.write_text(
        yaml.safe_dump(
            {
                "manifest_id": "MANIFEST-DUPLICATE",
                "description": "Duplicate entry ids should be rejected.",
                "entries": [
                    {
                        "entry_id": "duplicate",
                        "fixture_path": fixture_path.name,
                        "event_registry_dir": registry_dir.name,
                        "expected_status": "ready_for_local_evaluation",
                        "description": "First entry.",
                    },
                    {
                        "entry_id": "duplicate",
                        "fixture_path": fixture_path.name,
                        "event_registry_dir": registry_dir.name,
                        "expected_status": "rejected",
                        "description": "Second entry.",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="entry_id values must be unique"):
        load_evaluator_plan_manifest(manifest_path)


def test_evaluator_plan_manifest_requires_referenced_paths(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        yaml.safe_dump(
            {
                "manifest_id": "MANIFEST-MISSING-PATH",
                "description": "Missing fixture path should be rejected.",
                "entries": [
                    {
                        "entry_id": "missing_fixture",
                        "fixture_path": "missing_fixture.yaml",
                        "event_registry_dir": "missing_registry",
                        "expected_status": "rejected",
                        "description": "Missing local paths.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Referenced evaluator fixture path does not exist"):
        load_evaluator_plan_manifest(manifest_path)
