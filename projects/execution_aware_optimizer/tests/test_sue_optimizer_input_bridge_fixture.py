from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.sue_optimizer_input_bridge import (
    SueOptimizerInputBridgeInput,
    run_sue_optimizer_input_bridge_fixture,
    write_sue_optimizer_input_bridge_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_CONFIG = REPO_ROOT / "projects" / "typed_alpha_pilot" / "fixtures" / "sue_expanded" / "fixture_config.json"
SURVIVAL_FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "sue_expanded_survival"


def _bridge_input(*, allow: bool = True) -> SueOptimizerInputBridgeInput:
    return SueOptimizerInputBridgeInput.model_validate(
        {
            "allow_typed_alpha_optimizer_injection": allow,
            "fixture_config_path": str(FIXTURE_CONFIG),
            "local_backtest_manifest_path": str(
                REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
            ),
            "local_rebalance_date": "2026-02-27",
            "no_broker": True,
            "no_network": True,
            "run_id": "sue-optimizer-input-bridge-test",
        }
    )


def test_sue_optimizer_bridge_default_is_not_injected() -> None:
    result = run_sue_optimizer_input_bridge_fixture(_bridge_input(allow=False))

    assert result.bridge_status == "unavailable"
    assert result.expected_return_reached_actual_optimizer_input is False
    assert result.optimizer_decision_used_typed_expected_return is False
    assert result.actual_optimizer_output_rows == 0
    assert result.adapter_hook_only is False


def test_sue_optimizer_bridge_uses_actual_optimizer_decision_path(tmp_path: Path) -> None:
    result = run_sue_optimizer_input_bridge_fixture(_bridge_input())

    assert result.bridge_status == "observed"
    assert result.expected_return_reached_actual_optimizer_input is True
    assert result.optimizer_decision_used_typed_expected_return is True
    assert result.sue_rank_weight_alignment_observed is True
    assert result.sign_flip_reversal_observed is True
    assert result.scaled_alpha_monotonicity_observed is True
    assert result.no_view_not_encoded_as_zero is True
    assert result.actual_optimizer_output_rows > 0
    assert result.adapter_hook_only is False
    assert result.production_approval_claimed is False

    artifacts = write_sue_optimizer_input_bridge_artifacts(
        result,
        tmp_path,
        report_path=tmp_path / "sue_optimizer_input_bridge_report.md",
    )

    summary = json.loads(artifacts["summary"].read_text(encoding="utf-8"))
    assert summary["adapter_hook_only"] is False
    assert summary["optimizer_decision_used_typed_expected_return"] is True

    matrix = pd.read_csv(artifacts["q2_rows"])
    assert set(matrix["status"]) == {"observed"}
    assert matrix["adapter_hook_only"].eq(False).all()
    assert matrix["actual_optimizer_output"].eq(True).all()

    optimizer_input = pd.read_csv(artifacts["optimizer_input"])
    no_view = optimizer_input.loc[optimizer_input["typed_alpha_view_status"] == "no_view"]
    assert not no_view.empty
    assert no_view["typed_alpha_expected_return"].isna().all()
    assert no_view["expected_return_source"].eq("no_view_abstain_objective_neutral_fill").all()

    report = artifacts["report"].read_text(encoding="utf-8")
    assert "This proves local optimizer-path integration only." in report
    assert "This does not prove real historical SUE alpha." in report
    assert "This does not prove paper readiness or production approval." in report
    assert "Q2 rows in this report are based on actual local optimizer outputs, not adapter-hook mapping." in report
    _assert_no_misleading_claims(report)


def _assert_no_misleading_claims(report: str) -> None:
    lower = report.lower()
    assert "production approved" not in lower
    assert "paper ready" not in lower
    assert "live trading" not in lower
    assert "real historical sue alpha proven" not in lower
    assert "historical sue alpha proven" not in lower
    scrubbed = lower.replace("broker/order/live workflows", "")
    assert "broker execution" not in scrubbed
    assert "order generation" not in scrubbed
