from __future__ import annotations

from pathlib import Path

import yaml

from portfolio_os.alpha.registry_v2 import build_default_alpha_registry_v2


REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_NOTE = REPO_ROOT / "docs" / "releases" / "portfolioos_v1_research_audit_release.md"
ALPHA_REGISTRY_REPORT = REPO_ROOT / "reports" / "alpha_registry_report.md"
SUE_EXPANDED_CLOSEOUT = REPO_ROOT / "reports" / "sue_expanded_typed_q2_closeout.md"
SUE_EXPANDED_SURVIVAL_REPORT = REPO_ROOT / "reports" / "sue_expanded_typed_q2_survival_report.md"
ALPHA_REGISTRY_OUTPUT = REPO_ROOT / "outputs" / "alpha_registry_v2" / "alpha_registry.yaml"

FORBIDDEN_RELEASE_CLAIMS = [
    "production approved",
    "paper ready",
    "live alpha orders",
    "broker execution",
    "real historical sue alpha proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
]


def test_v1_research_audit_release_note_covers_required_boundaries() -> None:
    note = RELEASE_NOTE.read_text(encoding="utf-8")
    lower_note = note.lower()

    required_sections = [
        "q1 alpha evidence / triage boundary",
        "evidence bundle / promotion gate boundary",
        "typed alpha view contract",
        "sue local typed-q2 pilot",
        "sue expanded deterministic typed-q2 candidate benchmark",
        "alpha registry v2 decision state machine",
        "dashboard / audit / provenance / no-network safeguards",
        "validation summary",
        "explicit non-goals",
    ]
    for section in required_sections:
        assert section in lower_note

    assert "no production approval" in lower_note
    assert "no live trading" in lower_note
    assert "no broker/order path" in lower_note
    assert "no paper-ready alpha claim" in lower_note
    _assert_no_forbidden_release_claims(note)


def test_release_artifacts_keep_sue_as_canonical_pilot_only() -> None:
    registry = build_default_alpha_registry_v2()
    sue = next(entry for entry in registry.entries if entry.alpha_id == "sue_pead")

    assert sue.primary_status == "canonical_pilot"
    assert "q2_observed_survives" in sue.status_history
    assert "production_not_approved" in sue.status_history
    assert sue.production_approval_claimed is False
    assert sue.live_trading_allowed is False

    decision = sue.decision_history[-1]
    assert decision.decision_label == "sue_expanded_fixture_q2_observed_survives"
    assert decision.evidence_type == "deterministic_expanded_fixture"
    assert decision.event_count == 120
    assert decision.rebalance_date_count == 12
    assert decision.active_rebalance_count == 12
    assert decision.q2_observed_rows == 30
    assert decision.production_approval_claimed is False

    report_text = ALPHA_REGISTRY_REPORT.read_text(encoding="utf-8")
    assert "sue_expanded_fixture_q2_observed_survives" in report_text
    assert "deterministic_expanded_fixture" in report_text
    _assert_no_forbidden_release_claims(report_text)


def test_phase_56a_release_artifacts_state_deterministic_fixture_limits() -> None:
    closeout = SUE_EXPANDED_CLOSEOUT.read_text(encoding="utf-8")
    survival = SUE_EXPANDED_SURVIVAL_REPORT.read_text(encoding="utf-8")
    combined = f"{closeout}\n{survival}"
    lower_combined = combined.lower()

    assert "deterministic expanded fixture" in lower_combined
    assert "event_count: `120`" in lower_combined
    assert "rebalance_date_count: `12`" in lower_combined
    assert "active_rebalance_count: `12`" in lower_combined
    assert "q2_observed_rows: `30`" in lower_combined
    assert "q2 observed rows remain mapped through existing local fixture adapter" in lower_combined
    assert "production_approval_claimed=false" in lower_combined
    _assert_no_forbidden_release_claims(combined)


def test_alpha_registry_output_matches_v2_sue_decision_when_present() -> None:
    if not ALPHA_REGISTRY_OUTPUT.exists():
        return

    payload = yaml.safe_load(ALPHA_REGISTRY_OUTPUT.read_text(encoding="utf-8"))
    sue = next(entry for entry in payload["entries"] if entry["alpha_id"] == "sue_pead")

    assert sue["primary_status"] == "canonical_pilot"
    assert sue["production_approval_claimed"] is False
    assert sue["live_trading_allowed"] is False
    assert sue["decision_history"][-1]["decision_label"] == "sue_expanded_fixture_q2_observed_survives"
    assert sue["decision_history"][-1]["evidence_type"] == "deterministic_expanded_fixture"
    assert sue["decision_history"][-1]["event_count"] == 120
    assert sue["decision_history"][-1]["production_approval_claimed"] is False


def _assert_no_forbidden_release_claims(text: str) -> None:
    lower_text = text.lower()
    for phrase in FORBIDDEN_RELEASE_CLAIMS:
        assert phrase not in lower_text
