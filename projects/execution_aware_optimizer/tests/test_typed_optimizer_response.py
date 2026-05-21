from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.typed_optimizer_response import (
    run_typed_optimizer_response_acceptance,
    write_typed_optimizer_response_artifacts,
)
from execution_aware_optimizer.typed_optimizer_response_schema import (
    TypedOptimizerResponseInput,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_injection"


def _response_input(*, allow_portfolioos_run: bool = True) -> TypedOptimizerResponseInput:
    return TypedOptimizerResponseInput.model_validate(
        {
            "allow_portfolioos_run": allow_portfolioos_run,
            "expected_return_panel_path": str(FIXTURE_DIR / "expected_return_panel.csv"),
            "local_backtest_manifest_path": str(
                REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
            ),
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": str(FIXTURE_DIR / "projection_manifest.json"),
            "q2_input_contract_v2_path": str(FIXTURE_DIR / "q2_input_contract_v2.json"),
            "run_id": "typed-optimizer-response-acceptance",
        }
    )


def test_response_input_requires_no_network_and_no_broker() -> None:
    payload = _response_input().model_dump(mode="json")
    payload["no_network"] = False
    with pytest.raises(ValueError, match="no_network"):
        TypedOptimizerResponseInput.model_validate(payload)

    payload = _response_input().model_dump(mode="json")
    payload["no_broker"] = False
    with pytest.raises(ValueError, match="no_broker"):
        TypedOptimizerResponseInput.model_validate(payload)


def test_disabled_portfolioos_run_returns_structured_unavailable() -> None:
    result = run_typed_optimizer_response_acceptance(_response_input(allow_portfolioos_run=False))

    assert result.response_status == "unavailable"
    assert result.summary.optimizer_status == "not_run"
    assert result.summary.unavailable_reason
    assert result.no_live_data_confirmed is True
    assert result.no_orders_confirmed is True
    assert result.no_broker_confirmed is True
    assert result.response_rows == []


def test_typed_optimizer_response_grid_covers_phase_49_semantics() -> None:
    result = run_typed_optimizer_response_acceptance(_response_input())

    assert result.response_status == "observed"
    rows = {row.panel_name: row for row in result.response_rows}
    assert set(rows) == {
        "positive_panel",
        "scaled_0_5x_panel",
        "scaled_1_0x_panel",
        "scaled_2_0x_panel",
        "sign_flipped_panel",
        "zero_panel",
        "abstain_panel",
    }

    assert rows["positive_panel"].rank_alignment > 0
    assert rows["positive_panel"].top_minus_bottom_weight_delta > 0
    assert rows["sign_flipped_panel"].rank_alignment < 0
    assert rows["sign_flipped_panel"].top_minus_bottom_weight_delta < 0

    scaled_rewards = [
        rows["scaled_0_5x_panel"].alpha_reward_share,
        rows["scaled_1_0x_panel"].alpha_reward_share,
        rows["scaled_2_0x_panel"].alpha_reward_share,
    ]
    assert scaled_rewards == sorted(scaled_rewards)
    assert scaled_rewards[0] < scaled_rewards[-1]

    assert rows["zero_panel"].view_state == "zero_alpha"
    assert rows["zero_panel"].expected_return_used_share == pytest.approx(1.0)
    assert rows["zero_panel"].alpha_reward_share == pytest.approx(0.0)

    assert rows["abstain_panel"].view_state == "no_view"
    assert rows["abstain_panel"].expected_return_used_share == pytest.approx(0.0)
    assert rows["abstain_panel"].alpha_reward_share == pytest.approx(0.0)
    assert rows["abstain_panel"].zero_alpha_distinct_from_no_view is True

    assert all(row.repair_retention is not None for row in result.response_rows)
    assert result.summary.positive_rank_alignment_passed is True
    assert result.summary.scaled_alpha_reward_monotone is True
    assert result.summary.sign_flip_reverses_ordering is True
    assert result.summary.no_view_distinct_from_zero_alpha is True
    assert result.summary.repair_retention_reported is True


def test_response_artifact_writer_outputs_required_reports(tmp_path: Path) -> None:
    result = run_typed_optimizer_response_acceptance(_response_input())

    artifacts = write_typed_optimizer_response_artifacts(result, tmp_path)

    expected_names = {
        "optimizer_response_summary.json",
        "optimizer_response_grid.csv",
        "sign_flip_diagnostics.json",
        "abstain_vs_zero_report.json",
    }
    assert expected_names == {path.name for path in artifacts.values()}

    summary = json.loads((tmp_path / "optimizer_response_summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == "typed_optimizer_response_summary.v1"
    assert summary["response_status"] == "observed"
    assert summary["sign_flip_reverses_ordering"] is True

    grid = pd.read_csv(tmp_path / "optimizer_response_grid.csv")
    assert set(grid["panel_name"]) == {
        "positive_panel",
        "scaled_0_5x_panel",
        "scaled_1_0x_panel",
        "scaled_2_0x_panel",
        "sign_flipped_panel",
        "zero_panel",
        "abstain_panel",
    }
    assert {"optimizer_status", "alpha_reward_share", "repair_retention"}.issubset(grid.columns)

    sign_flip = json.loads((tmp_path / "sign_flip_diagnostics.json").read_text(encoding="utf-8"))
    assert sign_flip["sign_flip_reverses_ordering"] is True
    assert sign_flip["positive_top_minus_bottom_weight_delta"] > 0
    assert sign_flip["sign_flipped_top_minus_bottom_weight_delta"] < 0

    abstain_report = json.loads((tmp_path / "abstain_vs_zero_report.json").read_text(encoding="utf-8"))
    assert abstain_report["zero_panel"]["view_state"] == "zero_alpha"
    assert abstain_report["abstain_panel"]["view_state"] == "no_view"
    assert abstain_report["no_view_distinct_from_zero_alpha"] is True

    serialized = "\n".join(path.read_text(encoding="utf-8") for path in artifacts.values() if path.suffix != ".csv")
    assert "broker_output" not in serialized
    assert "recommended_trade" not in serialized
    assert "production_alpha_approved" not in serialized
