from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.typed_adapter_schema import (
    TypedQ2AdapterInput,
    TypedQ2AdapterResult,
)
from execution_aware_optimizer.typed_portfolioos_adapter import (
    run_typed_q2_adapter,
    write_typed_q2_adapter_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "fixtures" / "typed_q2"


@dataclass(frozen=True)
class FakeBacktestResult:
    period_attribution: pd.DataFrame


def _fake_backtest_result() -> FakeBacktestResult:
    return FakeBacktestResult(
        period_attribution=pd.DataFrame(
            [
                {
                    "strategy": "alpha_only_top_quintile",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 2.0,
                    "active_trading_pnl": 1.0,
                    "trading_cost_pnl": -0.5,
                    "period_return": 0.025,
                    "turnover": 0.40,
                    "commission_cost": 0.2,
                    "spread_cost": 0.3,
                },
                {
                    "strategy": "naive_pro_rata",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 1.7,
                    "active_trading_pnl": 0.5,
                    "trading_cost_pnl": -0.3,
                    "period_return": 0.019,
                    "turnover": 0.30,
                    "commission_cost": 0.1,
                    "spread_cost": 0.2,
                },
                {
                    "strategy": "optimizer",
                    "end_date": "2026-02-28",
                    "start_nav": 100.0,
                    "holding_pnl": 1.5,
                    "active_trading_pnl": 0.7,
                    "trading_cost_pnl": -0.2,
                    "period_return": 0.020,
                    "turnover": 0.21,
                    "commission_cost": 0.1,
                    "spread_cost": 0.1,
                },
            ]
        )
    )


def _fake_backtest_result_without_risk_controlled() -> FakeBacktestResult:
    fixture = _fake_backtest_result().period_attribution
    return FakeBacktestResult(period_attribution=fixture.loc[fixture["strategy"] != "naive_pro_rata"].copy())


def _adapter_input(*, allow_portfolioos_run: bool) -> TypedQ2AdapterInput:
    return TypedQ2AdapterInput.model_validate(
        {
            "adapter_config_path": str(FIXTURE_DIR / "adapter_config.yaml"),
            "allow_portfolioos_run": allow_portfolioos_run,
            "expected_return_panel_path": str(FIXTURE_DIR / "expected_return_panel.csv"),
            "local_backtest_manifest_path": str(FIXTURE_DIR / "local_backtest_manifest.yaml"),
            "no_broker": True,
            "no_network": True,
            "projection_manifest_path": str(FIXTURE_DIR / "projection_manifest.json"),
            "q2_input_contract_v2_path": str(FIXTURE_DIR / "q2_input_contract_v2.json"),
            "run_id": "typed-q2-fixture",
        }
    )


def test_typed_q2_adapter_input_requires_no_network_and_no_broker() -> None:
    payload = _adapter_input(allow_portfolioos_run=False).model_dump(mode="json")
    payload["no_network"] = False

    with pytest.raises(ValueError, match="no_network"):
        TypedQ2AdapterInput.model_validate(payload)

    payload = _adapter_input(allow_portfolioos_run=False).model_dump(mode="json")
    payload["no_broker"] = False

    with pytest.raises(ValueError, match="no_broker"):
        TypedQ2AdapterInput.model_validate(payload)


def test_allow_portfolioos_run_false_returns_structured_unavailable_rows() -> None:
    called = False

    def _runner(_manifest: str | Path) -> FakeBacktestResult:
        nonlocal called
        called = True
        return _fake_backtest_result()

    result = run_typed_q2_adapter(_adapter_input(allow_portfolioos_run=False), backtest_runner=_runner)

    assert called is False
    assert result.adapter_status == "unavailable"
    assert result.observed_rows == 0
    assert result.unavailable_rows > 0
    assert result.no_live_data_confirmed is True
    assert result.no_orders_confirmed is True
    assert result.no_broker_confirmed is True
    assert all(row.status == "unavailable" for row in result.matrix_rows)
    assert all(row.net_return is None for row in result.matrix_rows)
    assert all(row.unavailable_reason for row in result.matrix_rows)


def test_allow_portfolioos_run_true_maps_local_fixture_to_observed_rows() -> None:
    result = run_typed_q2_adapter(
        _adapter_input(allow_portfolioos_run=True),
        backtest_runner=lambda _manifest: _fake_backtest_result(),
    )

    assert result.adapter_status == "observed"
    assert result.observed_rows == 3
    assert result.unavailable_rows == 0
    assert result.rejection_reasons == []
    assert result.source_config_hash
    assert set(result.input_artifact_hashes) == {
        "adapter_config",
        "expected_return_panel",
        "local_backtest_manifest",
        "projection_manifest",
        "q2_input_contract_v2",
    }

    raw = next(row for row in result.matrix_rows if row.layer == "raw_top_alpha_equal_weight")
    risk = next(row for row in result.matrix_rows if row.layer == "risk_controlled")
    full = next(row for row in result.matrix_rows if row.layer == "full_execution_aware_cost_adjusted")

    assert raw.status == "observed"
    assert raw.gross_return == pytest.approx(0.03)
    assert raw.net_return == pytest.approx(0.025)
    assert raw.turnover == pytest.approx(0.40)
    assert raw.cost_drag == pytest.approx(0.005)
    assert raw.gross_to_net_retention == pytest.approx(0.025 / 0.03)
    assert risk.status == "observed"
    assert risk.gross_return == pytest.approx(0.022)
    assert risk.net_return == pytest.approx(0.019)
    assert risk.turnover == pytest.approx(0.30)
    assert risk.cost_drag == pytest.approx(0.003)
    assert full.status == "observed"
    assert full.net_return == pytest.approx(0.020)
    assert all(row.unavailable_reason is None for row in result.matrix_rows)


def test_missing_risk_controlled_strategy_remains_structured_unavailable() -> None:
    result = run_typed_q2_adapter(
        _adapter_input(allow_portfolioos_run=True),
        backtest_runner=lambda _manifest: _fake_backtest_result_without_risk_controlled(),
    )

    assert result.adapter_status == "partially_observed"
    assert result.observed_rows == 2
    assert result.unavailable_rows == 1

    unavailable = next(row for row in result.matrix_rows if row.layer == "risk_controlled")
    assert unavailable.status == "unavailable"
    assert unavailable.net_return is None
    assert "naive_pro_rata" in str(unavailable.unavailable_reason)


def test_missing_expected_return_panel_rejects_without_backtest_run(tmp_path: Path) -> None:
    payload = _adapter_input(allow_portfolioos_run=True).model_dump(mode="json")
    payload["expected_return_panel_path"] = str(tmp_path / "missing.csv")
    adapter_input = TypedQ2AdapterInput.model_validate(payload)

    result = run_typed_q2_adapter(adapter_input, backtest_runner=lambda _manifest: _fake_backtest_result())

    assert isinstance(result, TypedQ2AdapterResult)
    assert result.adapter_status == "rejected"
    assert result.observed_rows == 0
    assert result.unavailable_rows == 0
    assert any("expected_return_panel" in reason for reason in result.rejection_reasons)


def test_typed_q2_adapter_rejects_forbidden_output_keys(tmp_path: Path) -> None:
    bad_contract_path = tmp_path / "bad_q2_input_contract_v2.json"
    payload = json.loads((FIXTURE_DIR / "q2_input_contract_v2.json").read_text(encoding="utf-8"))
    payload["broker_output"] = {"submitted_order": {"symbol": "AAPL", "qty": 100}}
    bad_contract_path.write_text(json.dumps(payload), encoding="utf-8")

    input_payload = _adapter_input(allow_portfolioos_run=True).model_dump(mode="json")
    input_payload["q2_input_contract_v2_path"] = str(bad_contract_path)
    adapter_input = TypedQ2AdapterInput.model_validate(input_payload)

    result = run_typed_q2_adapter(adapter_input, backtest_runner=lambda _manifest: _fake_backtest_result())

    assert result.adapter_status == "rejected"
    assert result.observed_rows == 0
    assert result.unavailable_rows == 0
    assert any("forbidden output key" in reason for reason in result.rejection_reasons)


def test_typed_q2_adapter_writes_artifacts_without_trading_payloads(tmp_path: Path) -> None:
    result = run_typed_q2_adapter(
        _adapter_input(allow_portfolioos_run=True),
        backtest_runner=lambda _manifest: _fake_backtest_result(),
    )

    artifacts = write_typed_q2_adapter_artifacts(result, tmp_path)

    expected_names = {
        "typed_q2_execution_matrix.csv",
        "typed_q2_adapter_result.json",
        "typed_q2_robustness_summary.json",
        "typed_q2_adapter_manifest.json",
        "typed_q2_adapter_trace.jsonl",
    }
    assert expected_names == {path.name for path in artifacts.values()}

    matrix = pd.read_csv(tmp_path / "typed_q2_execution_matrix.csv")
    assert set(matrix["status"]) == {"observed"}
    assert "schema_version" in matrix.columns

    manifest = json.loads((tmp_path / "typed_q2_adapter_manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "typed_q2_adapter_manifest.v1"
    assert manifest["no_live_data_confirmed"] is True
    assert manifest["no_orders_confirmed"] is True
    assert manifest["no_broker_confirmed"] is True
    assert manifest["content_hash"]

    combined_text = "\n".join(path.read_text(encoding="utf-8") for path in artifacts.values())
    forbidden_payload_terms = ("submitted_order", "filled_order", "api_key", "secret")
    assert not any(term in combined_text for term in forbidden_payload_terms)
