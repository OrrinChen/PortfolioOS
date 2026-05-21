from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.typed_expected_return_injection import (
    run_typed_expected_return_injection,
    write_typed_expected_return_injection_artifacts,
)
from execution_aware_optimizer.typed_injection_schema import (
    TypedExpectedReturnInjectionInput,
    TypedExpectedReturnInjectionResult,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_injection"


def _injection_input(*, allow_portfolioos_run: bool, scale: float = 1.0, sign: int = 1) -> TypedExpectedReturnInjectionInput:
    return TypedExpectedReturnInjectionInput.model_validate(
        {
            "adapter_config_path": str(FIXTURE_DIR / "adapter_config.yaml"),
            "allow_portfolioos_run": allow_portfolioos_run,
            "expected_return_panel_path": str(FIXTURE_DIR / "expected_return_panel.csv"),
            "expected_return_scale": scale,
            "expected_return_sign": sign,
            "local_backtest_manifest_path": str(
                REPO_ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
            ),
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": str(FIXTURE_DIR / "projection_manifest.json"),
            "q2_input_contract_v2_path": str(FIXTURE_DIR / "q2_input_contract_v2.json"),
            "run_id": "typed-expected-return-injection-fixture",
        }
    )


def test_injection_input_requires_no_network_and_no_broker() -> None:
    payload = _injection_input(allow_portfolioos_run=False).model_dump(mode="json")
    payload["no_network"] = False
    with pytest.raises(ValueError, match="no_network"):
        TypedExpectedReturnInjectionInput.model_validate(payload)

    payload = _injection_input(allow_portfolioos_run=False).model_dump(mode="json")
    payload["no_broker"] = False
    with pytest.raises(ValueError, match="no_broker"):
        TypedExpectedReturnInjectionInput.model_validate(payload)


def test_allow_portfolioos_run_false_returns_structured_unavailable() -> None:
    result = run_typed_expected_return_injection(_injection_input(allow_portfolioos_run=False))

    assert result.injection_status == "unavailable"
    assert result.expected_return_reached_optimizer_input is False
    assert result.optimizer_input_snapshot_rows == 0
    assert result.injected_expected_return_count == 0
    assert result.q2_adapter_status == "unavailable"
    assert result.unavailable_reason
    assert result.no_live_data_confirmed is True
    assert result.no_orders_confirmed is True
    assert result.no_broker_confirmed is True


def test_allow_portfolioos_run_true_writes_optimizer_input_snapshot(tmp_path: Path) -> None:
    result = run_typed_expected_return_injection(_injection_input(allow_portfolioos_run=True))

    assert result.injection_status == "injected"
    assert result.expected_return_reached_optimizer_input is True
    assert result.optimizer_input_snapshot_rows > 0
    assert result.injected_expected_return_count == 2
    assert result.rejection_reasons == []
    assert result.optimizer_rebalance_date == "2026-02-27"

    artifacts = write_typed_expected_return_injection_artifacts(result, tmp_path)
    snapshot = pd.read_csv(artifacts["optimizer_input_snapshot"])

    expected = snapshot.set_index("ticker")["expected_return"].astype(float)
    assert expected["AAPL"] == pytest.approx(0.004)
    assert expected["MSFT"] == pytest.approx(0.002)
    assert expected.drop(index=["AAPL", "MSFT"]).abs().max() == pytest.approx(0.0)

    source = snapshot.set_index("ticker")["expected_return_source"]
    assert source["AAPL"] == "typed_expected_return_panel"
    assert source["MSFT"] == "typed_expected_return_panel"


def test_scaled_and_sign_flipped_panels_are_supported(tmp_path: Path) -> None:
    result = run_typed_expected_return_injection(
        _injection_input(allow_portfolioos_run=True, scale=2.0, sign=-1)
    )

    artifacts = write_typed_expected_return_injection_artifacts(result, tmp_path)
    panel = pd.read_csv(artifacts["injected_expected_return_panel"])
    snapshot = pd.read_csv(artifacts["optimizer_input_snapshot"])

    panel_expected = panel.set_index("symbol")["expected_return"].astype(float)
    snapshot_expected = snapshot.set_index("ticker")["expected_return"].astype(float)
    assert panel_expected["AAPL"] == pytest.approx(-0.008)
    assert panel_expected["MSFT"] == pytest.approx(-0.004)
    assert snapshot_expected["AAPL"] == pytest.approx(-0.008)
    assert snapshot_expected["MSFT"] == pytest.approx(-0.004)


def test_missing_expected_return_panel_rejects_without_snapshot(tmp_path: Path) -> None:
    payload = _injection_input(allow_portfolioos_run=True).model_dump(mode="json")
    payload["expected_return_panel_path"] = str(tmp_path / "missing.csv")
    injection_input = TypedExpectedReturnInjectionInput.model_validate(payload)

    result = run_typed_expected_return_injection(injection_input)

    assert isinstance(result, TypedExpectedReturnInjectionResult)
    assert result.injection_status == "rejected"
    assert result.optimizer_input_snapshot_rows == 0
    assert any("expected_return_panel" in reason for reason in result.rejection_reasons)


def test_injection_artifact_writer_outputs_manifest_matrix_and_trace(tmp_path: Path) -> None:
    result = run_typed_expected_return_injection(_injection_input(allow_portfolioos_run=True))

    artifacts = write_typed_expected_return_injection_artifacts(result, tmp_path)

    expected_names = {
        "typed_expected_return_injection_result.json",
        "optimizer_input_snapshot.csv",
        "injected_expected_return_panel.csv",
        "typed_q2_execution_matrix_injected.csv",
        "typed_q2_injection_robustness_summary.json",
        "typed_q2_injection_manifest.json",
        "typed_q2_injection_trace.jsonl",
    }
    assert expected_names == {path.name for path in artifacts.values()}

    manifest = json.loads((tmp_path / "typed_q2_injection_manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "typed_expected_return_injection_manifest.v1"
    assert manifest["content_hash"]
    assert manifest["no_live_data_confirmed"] is True
    assert manifest["no_orders_confirmed"] is True
    assert manifest["no_broker_confirmed"] is True

    matrix = pd.read_csv(tmp_path / "typed_q2_execution_matrix_injected.csv")
    assert set(matrix["status"]) == {"observed"}
