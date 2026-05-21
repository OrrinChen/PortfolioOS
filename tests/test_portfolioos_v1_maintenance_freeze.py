from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FREEZE_NOTE = REPO_ROOT / "docs" / "releases" / "portfolioos_v1_maintenance_freeze.md"
FUTURE_BACKLOG = REPO_ROOT / "docs" / "strategy" / "portfolioos_future_backlog.md"
ROADMAP = REPO_ROOT / "ROADMAP.md"
TASK_MEMORY = REPO_ROOT / "TASK_MEMORY.md"
VALIDATION = REPO_ROOT / "VALIDATION.md"

FORBIDDEN_FREEZE_CLAIMS = [
    "production approved",
    "paper ready",
    "live-ready alpha",
    "broker execution",
    "order generation",
    "auto trading",
    "real historical sue proven",
    "factor discovery active implementation",
    "roadmap auto-expansion",
]


def test_maintenance_freeze_note_locks_phase_66_boundaries() -> None:
    note = FREEZE_NOTE.read_text(encoding="utf-8")
    lower_note = note.lower()

    assert "phase 56a and phase 65 are closed" in lower_note
    assert "sue remains an expanded deterministic typed-q2 candidate benchmark" in lower_note
    assert "not production-approved" in lower_note
    assert "not paper-ready" in lower_note
    assert "not live-ready" in lower_note
    assert "not historically proven" in lower_note
    assert "existing local fixture adapter hooks" in lower_note
    assert "future work is backlog-only unless explicitly reopened" in lower_note
    assert "phase 65 release commit was phase65-only" in lower_note
    assert "head may include other prior multifactor commits" in lower_note
    _assert_no_forbidden_freeze_claims(note)


def test_future_backlog_is_locked_to_explicit_reopen_paths() -> None:
    backlog = FUTURE_BACKLOG.read_text(encoding="utf-8")
    lower_backlog = backlog.lower()

    required_categories = [
        "future real historical sue panel",
        "wrds/pit required",
        "future paper-overlay calibration",
        "not alpha promotion",
        "future factor discovery import review",
        "phase 64 only",
        "future live/broker/order work",
        "explicit approval only",
        "future production approval path",
        "locked until real historical evidence and governance exist",
    ]
    for category in required_categories:
        assert category in lower_backlog
    _assert_no_forbidden_freeze_claims(backlog)


def test_phase_66_updates_repo_operating_instructions() -> None:
    roadmap = ROADMAP.read_text(encoding="utf-8")
    memory = TASK_MEMORY.read_text(encoding="utf-8")
    validation = VALIDATION.read_text(encoding="utf-8")
    combined = f"{roadmap}\n{memory}\n{validation}"
    lower_combined = combined.lower()

    assert "phase 66 is the final freeze phase" in lower_combined
    assert "no automatic roadmap expansion after phase 66" in lower_combined
    assert "new work requires explicit reopen decision" in lower_combined
    assert "existing unrelated multifactor / factor discovery working-tree changes are not part of the v1 freeze" in lower_combined
    assert "test_portfolioos_v1_maintenance_freeze.py" in validation


def _assert_no_forbidden_freeze_claims(text: str) -> None:
    lower_text = text.lower()
    for phrase in FORBIDDEN_FREEZE_CLAIMS:
        assert phrase not in lower_text
