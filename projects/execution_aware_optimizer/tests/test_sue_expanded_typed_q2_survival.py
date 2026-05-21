from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.sue_expanded_survival_schema import (
    ExpandedSueEventRow,
    SueExpandedTypedQ2SurvivalInput,
)
from execution_aware_optimizer.sue_expanded_typed_q2_survival import (
    run_sue_expanded_typed_q2_survival,
    write_sue_expanded_typed_q2_survival_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_CONFIG = REPO_ROOT / "projects" / "typed_alpha_pilot" / "fixtures" / "sue_expanded" / "fixture_config.json"
SURVIVAL_FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "sue_expanded_survival"
PHASE_56A_CLOSEOUT = REPO_ROOT / "reports" / "sue_expanded_typed_q2_closeout.md"


def _expanded_input(*, allow_portfolioos_run: bool = True) -> SueExpandedTypedQ2SurvivalInput:
    return SueExpandedTypedQ2SurvivalInput.model_validate(
        {
            "adapter_config_path": str(SURVIVAL_FIXTURE_DIR / "adapter_config.yaml"),
            "allow_portfolioos_run": allow_portfolioos_run,
            "fixture_config_path": str(FIXTURE_CONFIG),
            "local_backtest_manifest_path": str(
                REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
            ),
            "local_rebalance_date": "2026-02-27",
            "no_broker": True,
            "no_network": True,
            "run_id": "sue-expanded-typed-q2-candidate-test",
        }
    )


def test_expanded_sue_event_row_rejects_pit_timestamp_violation() -> None:
    payload = {
        "event_id": "bad-sue-row",
        "symbol": "AAPL",
        "event_timestamp": "2025-01-08T21:05:00Z",
        "event_available_timestamp": "2025-01-10T15:00:00Z",
        "tradable_timestamp": "2025-01-10T14:30:00Z",
        "rebalance_date": "2025-01-10",
        "sue_score": 2.1,
        "expected_return": 0.006,
    }

    with pytest.raises(ValueError, match="event_available_timestamp"):
        ExpandedSueEventRow.model_validate(payload)


def test_expanded_sue_survival_meets_breadth_and_safety_thresholds(tmp_path: Path) -> None:
    result = run_sue_expanded_typed_q2_survival(_expanded_input())

    assert result.summary.event_count >= 100
    assert result.summary.rebalance_date_count >= 12
    assert result.summary.active_rebalance_count >= 8
    assert result.summary.median_active_names_per_active_date >= 5
    assert result.summary.active_name_count >= 10
    assert result.summary.expected_return_used_share > 0.0
    assert result.summary.abstain_count > 0
    assert result.summary.coverage_loss_count > 0
    assert result.summary.q2_observed_rows > 0
    assert result.summary.q2_unavailable_rows >= 0
    assert result.summary.production_approval_claimed is False
    assert result.evidence_mode == "deterministic_fixture"

    artifacts = write_sue_expanded_typed_q2_survival_artifacts(result, tmp_path)

    panel = pd.read_csv(artifacts["expected_return_panel"])
    assert len(panel) >= 100
    assert panel["date"].nunique() >= 12
    assert not (panel["expected_return"].astype(float) == 0.0).any()

    abstain = json.loads(artifacts["abstain_report"].read_text(encoding="utf-8"))["abstain_report"]
    assert any(row["reason"] in {"coverage_missing", "explicit_no_view"} for row in abstain)

    summary = json.loads(artifacts["summary"].read_text(encoding="utf-8"))
    assert "q2_observed_rows" in summary
    assert "q2_unavailable_rows" in summary
    assert summary["production_approval_claimed"] is False

    matrix = pd.read_csv(artifacts["q2_matrix"])
    assert "status" in matrix.columns
    assert set(matrix["status"]).issubset({"observed", "unavailable", "rejected"})

    report = artifacts["report"].read_text(encoding="utf-8")
    assert "This is an expanded typed-Q2 candidate benchmark, not production approval." in report
    assert "deterministic fixture evidence" in report
    assert "real historical evidence: not claimed" in report
    assert "production approval: not claimed" in report
    _assert_no_phase_56a_approval_claims(
        report,
        allowed_boundary_sentences=[
            "- no broker workflow",
            "- no orders or trading instructions",
        ],
    )

    combined_text = "\n".join(path.read_text(encoding="utf-8") for path in artifacts.values() if path.suffix != ".csv")
    assert "broker_output" not in combined_text
    assert "submitted_order" not in combined_text
    assert "production_alpha_approved" not in combined_text


def test_phase_56a_closeout_preserves_non_claim_language() -> None:
    report = PHASE_56A_CLOSEOUT.read_text(encoding="utf-8")
    lower_report = report.lower()

    assert "phase 56a expands deterministic sue fixture breadth" in lower_report
    assert "this does not prove real historical sue alpha" in lower_report
    assert "it does not expand live/paper/broker/order workflows" in lower_report
    assert "q2 observed rows remain mapped through existing local fixture adapter" in lower_report
    assert "missing coverage remains explicit abstain/no_view, not zero alpha" in lower_report

    _assert_no_phase_56a_approval_claims(
        report,
        allowed_boundary_sentences=["it does not expand live/paper/broker/order workflows."],
    )


def _assert_no_phase_56a_approval_claims(report: str, *, allowed_boundary_sentences: list[str]) -> None:
    lower_report = report.lower()
    forbidden_claims = [
        "production approved",
        "paper ready",
        "live trading",
        "real alpha proven",
        "historical alpha proven",
    ]
    assert not any(claim in lower_report for claim in forbidden_claims)

    scrubbed = lower_report
    for sentence in allowed_boundary_sentences:
        scrubbed = scrubbed.replace(sentence.lower(), "")
    assert "broker" not in scrubbed
    assert "order" not in scrubbed
