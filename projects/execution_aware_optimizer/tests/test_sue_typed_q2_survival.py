from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.sue_typed_q2_survival import (
    run_sue_typed_q2_survival,
    write_sue_typed_q2_survival_artifacts,
)
from execution_aware_optimizer.sue_typed_q2_survival_schema import SueTypedQ2SurvivalInput


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "sue_survival"


def _survival_input(*, allow_portfolioos_run: bool = True) -> SueTypedQ2SurvivalInput:
    return SueTypedQ2SurvivalInput.model_validate(
        {
            "adapter_config_path": str(FIXTURE_DIR / "adapter_config.yaml"),
            "allow_portfolioos_run": allow_portfolioos_run,
            "expected_return_panel_path": str(FIXTURE_DIR / "expected_return_panel.csv"),
            "local_backtest_manifest_path": str(
                REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
            ),
            "local_rebalance_date": "2026-02-27",
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": str(FIXTURE_DIR / "projection_manifest.json"),
            "q2_input_contract_v2_path": str(FIXTURE_DIR / "q2_input_contract_v2.json"),
            "run_id": "sue-typed-q2-survival-fixture",
        }
    )


def test_sue_survival_input_requires_no_network_and_no_broker() -> None:
    payload = _survival_input().model_dump(mode="json")
    payload["no_network"] = False
    with pytest.raises(ValueError, match="no_network"):
        SueTypedQ2SurvivalInput.model_validate(payload)

    payload = _survival_input().model_dump(mode="json")
    payload["no_broker"] = False
    with pytest.raises(ValueError, match="no_broker"):
        SueTypedQ2SurvivalInput.model_validate(payload)


def test_disabled_sue_survival_returns_structured_unavailable() -> None:
    result = run_sue_typed_q2_survival(_survival_input(allow_portfolioos_run=False))

    assert result.survival_status == "unavailable"
    assert result.injection_status == "unavailable"
    assert result.expected_return_reached_optimizer_input is False
    assert result.matrix_rows
    assert {row.status for row in result.matrix_rows} == {"unavailable"}
    assert all(row.unavailable_reason for row in result.matrix_rows)
    assert result.no_live_data_confirmed is True
    assert result.no_orders_confirmed is True
    assert result.no_broker_confirmed is True


def test_sue_survival_injects_expected_returns_and_preserves_observed_unavailable_rows() -> None:
    result = run_sue_typed_q2_survival(_survival_input())

    assert result.survival_status == "partially_observed"
    assert result.injection_status == "injected"
    assert result.expected_return_reached_optimizer_input is True
    assert result.optimizer_rebalance_date == "2026-02-27"
    assert result.original_projection_dates == ["2025-02-10"]
    assert result.local_rebalance_date == "2026-02-27"
    assert result.active_rebalance_count == 1
    assert result.active_name_count == 2
    assert result.expected_return_used_share == pytest.approx(2 / 3)
    assert result.q2_observed_rows > 0
    assert result.q2_unavailable_rows > 0
    assert result.production_approval_claimed is False

    observed_rows = [row for row in result.matrix_rows if row.status == "observed"]
    unavailable_rows = [row for row in result.matrix_rows if row.status == "unavailable"]
    assert observed_rows
    assert unavailable_rows
    assert all(row.alpha_family == "SUE" for row in result.matrix_rows)
    assert all(row.projection_policy == "event_window_decay" for row in result.matrix_rows)
    assert all(row.abstain_policy == "explicit_abstain" for row in result.matrix_rows)
    assert all(row.gross_return is not None for row in observed_rows)
    assert all(row.net_return is not None for row in observed_rows)
    assert all(row.turnover is not None for row in observed_rows)
    assert all(row.cost_drag is not None for row in observed_rows)
    assert all(row.gross_to_net_retention is not None for row in observed_rows)
    assert all(row.unavailable_reason for row in unavailable_rows)
    assert all(row.gross_return is None for row in unavailable_rows)
    assert all(row.net_return is None for row in unavailable_rows)
    assert all(row.turnover is None for row in unavailable_rows)


def test_sue_survival_artifact_writer_outputs_required_phase_50_files(tmp_path: Path) -> None:
    result = run_sue_typed_q2_survival(_survival_input())

    artifacts = write_sue_typed_q2_survival_artifacts(result, tmp_path)

    expected = {
        "sue_typed_q2_execution_matrix.csv",
        "sue_typed_q2_survival_summary.json",
        "sue_optimizer_input_snapshot.csv",
        "sue_injection_manifest.json",
        "sue_q2_trace.jsonl",
        "sue_typed_q2_survival_result.json",
    }
    assert expected.issubset({path.name for path in artifacts.values()})

    snapshot = pd.read_csv(tmp_path / "sue_optimizer_input_snapshot.csv")
    expected_return = snapshot.set_index("ticker")["expected_return"].astype(float)
    source = snapshot.set_index("ticker")["expected_return_source"]
    assert expected_return["AAPL"] == pytest.approx(0.006547895567)
    assert expected_return["MSFT"] == pytest.approx(0.003273947784)
    assert source["AAPL"] == "typed_expected_return_panel"
    assert source["MSFT"] == "typed_expected_return_panel"

    summary = json.loads((tmp_path / "sue_typed_q2_survival_summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == "sue_typed_q2_survival_summary.v1"
    assert summary["survival_status"] == "partially_observed"
    assert summary["expected_return_reached_optimizer_input"] is True
    assert summary["sue_status"] == "integration_benchmark_q2_candidate"
    assert summary["production_approval_claimed"] is False

    matrix = pd.read_csv(tmp_path / "sue_typed_q2_execution_matrix.csv")
    assert {"observed", "unavailable"}.issubset(set(matrix["status"]))
    assert {
        "active_rebalance_count",
        "active_name_count",
        "expected_return_used_share",
        "gross_return",
        "net_return",
        "turnover",
        "cost_drag",
        "gross_to_net_retention",
        "repair_retention",
        "unavailable_reason",
        "projection_policy",
        "abstain_policy",
        "source_config_hash",
    }.issubset(matrix.columns)

    serialized = "\n".join(path.read_text(encoding="utf-8") for path in artifacts.values() if path.suffix != ".csv")
    assert "broker_output" not in serialized
    assert "recommended_trade" not in serialized
    assert "production_alpha_approved" not in serialized
