from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.domain.errors import InputValidationError
from portfolio_os.execution.simulator import load_execution_request, run_execution_simulation


def _write_execution_fixture(
    tmp_path: Path,
    *,
    order_rows: list[dict[str, object]],
    market_rows: list[dict[str, object]],
    participation_limit: float = 0.2,
) -> tuple[Path, Path]:
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    orders_path = artifact_dir / "final_orders_oms.csv"
    market_path = tmp_path / "market.csv"
    portfolio_state_path = tmp_path / "portfolio_state.yaml"
    execution_profile_path = tmp_path / "execution_profile.yaml"
    audit_path = artifact_dir / "final_audit.json"
    freeze_manifest_path = artifact_dir / "freeze_manifest.json"
    request_path = tmp_path / "execution_request.yaml"

    pd.DataFrame(order_rows).to_csv(orders_path, index=False)
    pd.DataFrame(market_rows).to_csv(market_path, index=False)
    portfolio_state_path.write_text(
        "\n".join(
            [
                "account_id: test_account",
                'as_of_date: "2026-03-23"',
                "available_cash: 1000000",
                "min_cash_buffer: 50000",
                "account_type: public_fund",
            ]
        ),
        encoding="utf-8",
    )
    execution_profile_path.write_text(
        "\n".join(
            [
                "urgency: low",
                "slice_ratio: 0.25",
                "max_child_orders: 4",
            ]
        ),
        encoding="utf-8",
    )
    audit_path.write_text(
        json.dumps(
            {
                "disclaimer": "Auxiliary decision-support tool only. Not investment advice.",
                "inputs": {
                    "market": {"path": str(market_path)},
                    "portfolio_state": {"path": str(portfolio_state_path)},
                    "execution_profile": {"path": str(execution_profile_path)},
                },
                "parameters": {
                    "fees": {
                        "commission_rate": 0.0003,
                        "transfer_fee_rate": 0.00001,
                        "stamp_duty_rate": 0.001,
                    },
                    "slippage": {"k": 0.02},
                    "trading": {
                        "market": "CN_A",
                        "lot_size": 100,
                        "allow_fractional_shares_in_optimizer": True,
                    },
                    "constraints": {"participation_limit": participation_limit},
                    "portfolio_state": {
                        "account_id": "test_account",
                        "as_of_date": "2026-03-23",
                        "available_cash": 1000000.0,
                        "min_cash_buffer": 50000.0,
                        "account_type": "public_fund",
                    },
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    freeze_manifest_path.write_text(
        json.dumps(
            {
                "created_at": "2026-03-23T00:00:00+00:00",
                "approval_status": "approved",
                "selected_scenario": "fixture",
                "source_artifacts": {},
                "final_artifacts": {},
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    request_path.write_text(
        "\n".join(
            [
                "name: execution_test_request",
                "description: unit test execution request",
                f"artifact_dir: {artifact_dir}",
                "input_orders: final_orders_oms.csv",
                f"portfolio_state: {portfolio_state_path}",
                f"execution_profile: {execution_profile_path}",
                "market_curve:",
                "  buckets:",
                "    - label: open",
                "      volume_share: 0.5",
                "      slippage_multiplier: 1.1",
                "    - label: close",
                "      volume_share: 0.5",
                "      slippage_multiplier: 0.9",
                "simulation:",
                "  mode: participation_twap",
                "  bucket_count: 2",
                "  allow_partial_fill: true",
                "  force_completion: false",
                "  max_bucket_participation_override: null",
            ]
        ),
        encoding="utf-8",
    )
    return request_path, artifact_dir


def test_execution_request_parsing(project_root: Path) -> None:
    request = load_execution_request(
        project_root / "data" / "execution_samples" / "execution_request_example.yaml"
    )

    assert request.name == "demo_execution_request"
    assert request.simulation.mode == "participation_twap"
    assert len(request.market_curve.buckets) == 5


def test_execution_request_rejects_invalid_bucket_volume_sum(tmp_path: Path) -> None:
    request_path = tmp_path / "invalid_request.yaml"
    request_path.write_text(
        "\n".join(
            [
                "name: invalid_request",
                "artifact_dir: outputs/approval_demo",
                "input_orders: final_orders_oms.csv",
                "market_curve:",
                "  buckets:",
                "    - label: open",
                "      volume_share: 0.7",
                "      slippage_multiplier: 1.0",
                "    - label: close",
                "      volume_share: 0.2",
                "      slippage_multiplier: 1.0",
                "simulation:",
                "  mode: participation_twap",
                "  bucket_count: 2",
                "  allow_partial_fill: true",
                "  force_completion: false",
                "  max_bucket_participation_override: null",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(InputValidationError, match="volume_share must sum to 1.0"):
        load_execution_request(request_path)


def test_execution_simulation_covers_filled_partial_and_unfilled_states(tmp_path: Path) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "000001",
                "side": "BUY",
                "quantity": 100,
                "estimated_price": 10.0,
            },
            {
                "account_id": "test_account",
                "ticker": "000002",
                "side": "BUY",
                "quantity": 300,
                "estimated_price": 20.0,
            },
            {
                "account_id": "test_account",
                "ticker": "000003",
                "side": "SELL",
                "quantity": 100,
                "estimated_price": 30.0,
            },
        ],
        market_rows=[
            {
                "ticker": "000001",
                "close": 10.0,
                "vwap": 10.0,
                "adv_shares": 1000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            },
            {
                "ticker": "000002",
                "close": 20.0,
                "vwap": 20.0,
                "adv_shares": 1500,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            },
            {
                "ticker": "000003",
                "close": 30.0,
                "vwap": 30.0,
                "adv_shares": 800,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            },
        ],
    )

    result = run_execution_simulation(
        request_path,
        run_id="execution_test",
        created_at="2026-03-23T00:00:00+00:00",
    )

    statuses = {order.ticker: order.status for order in result.per_order_results}
    assert statuses["000001"] == "filled"
    assert statuses["000002"] == "partial_fill"
    assert statuses["000003"] == "unfilled"
    inactive_bucket_count = sum(
        1
        for order in result.per_order_results
        for bucket in order.bucket_results
        if bucket.status == "inactive"
    )
    assert inactive_bucket_count > 0

    summary = result.portfolio_summary
    assert summary.filled_order_count == 1
    assert summary.partial_fill_count == 1
    assert summary.unfilled_order_count == 1
    assert summary.inactive_bucket_count == inactive_bucket_count
    assert 0.0 <= summary.fill_rate <= 1.0
    assert summary.fill_rate == pytest.approx(
        (summary.total_ordered_notional - summary.total_unfilled_notional)
        / summary.total_ordered_notional
    )

    source_artifacts = result.source_artifacts
    for key in ("request", "input_orders", "audit", "freeze_manifest", "market", "portfolio_state", "execution_profile"):
        assert key in source_artifacts
        assert Path(source_artifacts[key]["path"]).exists()
        assert source_artifacts[key]["sha256"]


def test_execution_simulation_respects_bucket_participation_override_and_keeps_residual_when_not_forced(
    tmp_path: Path,
) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "000004",
                "side": "BUY",
                "quantity": 300,
                "estimated_price": 12.0,
            }
        ],
        market_rows=[
            {
                "ticker": "000004",
                "close": 12.0,
                "vwap": 12.0,
                "adv_shares": 3000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ],
    )
    request_path.write_text(
        request_path.read_text(encoding="utf-8").replace(
            "  max_bucket_participation_override: null",
            "  max_bucket_participation_override: 0.1",
        ),
        encoding="utf-8",
    )

    result = run_execution_simulation(
        request_path,
        run_id="execution_override_test",
        created_at="2026-03-23T00:00:00+00:00",
    )

    order = result.per_order_results[0]
    assert order.participation_limit_used == pytest.approx(0.1)
    assert order.status == "partial_fill"
    assert order.filled_quantity == 200
    assert order.unfilled_quantity == 100
    assert all(bucket.forced_completion is False for bucket in order.bucket_results)


def test_execution_simulation_loads_calibration_profile_and_reports_resolved_settings(
    tmp_path: Path,
) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "000005",
                "side": "BUY",
                "quantity": 100,
                "estimated_price": 15.0,
            }
        ],
        market_rows=[
            {
                "ticker": "000005",
                "close": 15.0,
                "vwap": 15.0,
                "adv_shares": 5000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ],
    )
    calibration_path = tmp_path / "calibration.yaml"
    calibration_path.write_text(
        "\n".join(
            [
                "name: unit_test_calibration",
                "description: calibration fixture",
                "market_curve:",
                "  buckets:",
                "    - label: alpha",
                "      volume_share: 0.4",
                "      slippage_multiplier: 1.05",
                "    - label: beta",
                "      volume_share: 0.6",
                "      slippage_multiplier: 0.95",
                "defaults:",
                "  participation_limit: 0.15",
                "  allow_partial_fill: true",
                "  force_completion: true",
            ]
        ),
        encoding="utf-8",
    )
    request_path.write_text(
        "\n".join(
            [
                "name: execution_test_request",
                "description: calibration test request",
                f"artifact_dir: {tmp_path / 'artifacts'}",
                "input_orders: final_orders_oms.csv",
                f"portfolio_state: {tmp_path / 'portfolio_state.yaml'}",
                f"execution_profile: {tmp_path / 'execution_profile.yaml'}",
                f"calibration_profile: {calibration_path}",
                "simulation:",
                "  mode: participation_twap",
                "  bucket_count: 2",
            ]
        ),
        encoding="utf-8",
    )

    result = run_execution_simulation(
        request_path,
        run_id="execution_calibration_test",
        created_at="2026-03-23T00:00:00+00:00",
    )

    assert result.resolved_calibration["selected_profile"]["name"] == "unit_test_calibration"
    assert result.resolved_calibration["selected_profile"]["source"] == "request"
    assert result.bucket_curve["buckets"][0]["label"] == "alpha"
    assert result.resolved_calibration["resolved_simulation_defaults"]["participation_limit"] == pytest.approx(0.15)
    assert result.resolved_calibration["resolved_simulation_defaults"]["force_completion"] is True


def test_execution_simulation_request_override_takes_priority_over_calibration_profile(
    tmp_path: Path,
) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "000006",
                "side": "BUY",
                "quantity": 100,
                "estimated_price": 18.0,
            }
        ],
        market_rows=[
            {
                "ticker": "000006",
                "close": 18.0,
                "vwap": 18.0,
                "adv_shares": 5000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ],
    )
    calibration_path = tmp_path / "calibration.yaml"
    calibration_path.write_text(
        "\n".join(
            [
                "name: cli_calibration",
                "description: calibration fixture",
                "market_curve:",
                "  buckets:",
                "    - label: alpha",
                "      volume_share: 0.4",
                "      slippage_multiplier: 1.05",
                "    - label: beta",
                "      volume_share: 0.6",
                "      slippage_multiplier: 0.95",
                "defaults:",
                "  participation_limit: 0.12",
                "  allow_partial_fill: true",
                "  force_completion: true",
            ]
        ),
        encoding="utf-8",
    )

    result = run_execution_simulation(
        request_path,
        run_id="execution_override_curve_test",
        created_at="2026-03-23T00:00:00+00:00",
        calibration_profile_path=calibration_path,
    )

    assert result.resolved_calibration["selected_profile"]["name"] == "cli_calibration"
    assert result.resolved_calibration["selected_profile"]["source"] == "cli"
    assert "market_curve" in result.resolved_calibration["overridden_fields"]
    assert "simulation.allow_partial_fill" in result.resolved_calibration["overridden_fields"]
    assert "simulation.force_completion" in result.resolved_calibration["overridden_fields"]
    assert result.bucket_curve["buckets"][0]["label"] == "open"


def test_execution_simulation_volume_shock_multiplier_reduces_fill_capacity(tmp_path: Path) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "000007",
                "side": "BUY",
                "quantity": 300,
                "estimated_price": 16.0,
            }
        ],
        market_rows=[
            {
                "ticker": "000007",
                "close": 16.0,
                "vwap": 16.0,
                "adv_shares": 3000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ],
    )
    baseline_calibration = tmp_path / "baseline_calibration.yaml"
    baseline_calibration.write_text(
        "\n".join(
            [
                "name: baseline",
                "market_curve:",
                "  buckets:",
                "    - label: open",
                "      volume_share: 0.5",
                "      slippage_multiplier: 1.0",
                "    - label: close",
                "      volume_share: 0.5",
                "      slippage_multiplier: 1.0",
                "defaults:",
                "  participation_limit: 0.2",
                "  volume_shock_multiplier: 1.0",
            ]
        ),
        encoding="utf-8",
    )
    stressed_calibration = tmp_path / "stressed_calibration.yaml"
    stressed_calibration.write_text(
        "\n".join(
            [
                "name: stressed",
                "market_curve:",
                "  buckets:",
                "    - label: open",
                "      volume_share: 0.5",
                "      slippage_multiplier: 1.0",
                "    - label: close",
                "      volume_share: 0.5",
                "      slippage_multiplier: 1.0",
                "defaults:",
                "  participation_limit: 0.2",
                "  volume_shock_multiplier: 0.2",
            ]
        ),
        encoding="utf-8",
    )

    baseline_result = run_execution_simulation(
        request_path,
        run_id="execution_shock_baseline",
        created_at="2026-03-23T00:00:00+00:00",
        calibration_profile_path=baseline_calibration,
    )
    stressed_result = run_execution_simulation(
        request_path,
        run_id="execution_shock_stressed",
        created_at="2026-03-23T00:00:00+00:00",
        calibration_profile_path=stressed_calibration,
    )

    assert baseline_result.portfolio_summary.fill_rate > stressed_result.portfolio_summary.fill_rate
    assert baseline_result.portfolio_summary.total_unfilled_notional < stressed_result.portfolio_summary.total_unfilled_notional


def test_execution_simulation_impact_aware_mode_prefers_cheaper_bucket(tmp_path: Path) -> None:
    request_path, _ = _write_execution_fixture(
        tmp_path,
        order_rows=[
            {
                "account_id": "test_account",
                "ticker": "UPST",
                "side": "BUY",
                "quantity": 800,
                "estimated_price": 25.0,
            }
        ],
        market_rows=[
            {
                "ticker": "UPST",
                "close": 25.0,
                "vwap": 25.0,
                "adv_shares": 10000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ],
        participation_limit=0.1,
    )
    request_path.write_text(
        request_path.read_text(encoding="utf-8")
        .replace("mode: participation_twap", "mode: impact_aware")
        .replace("slippage_multiplier: 1.1", "slippage_multiplier: 2.0")
        .replace("slippage_multiplier: 0.9", "slippage_multiplier: 0.5"),
        encoding="utf-8",
    )

    result = run_execution_simulation(
        request_path,
        run_id="execution_impact_aware_test",
        created_at="2026-03-23T00:00:00+00:00",
    )

    order = result.per_order_results[0]
    assert result.request_metadata["simulation"]["mode"] == "impact_aware"
    assert order.status == "filled"
    assert len(order.bucket_results) == 2
    assert order.bucket_results[1].filled_quantity > order.bucket_results[0].filled_quantity
    assert order.bucket_results[1].slippage_multiplier < order.bucket_results[0].slippage_multiplier
    if order.bucket_results[0].filled_quantity > 0:
        first_unit_cost = order.bucket_results[0].estimated_total_cost / order.bucket_results[0].filled_quantity
        second_unit_cost = order.bucket_results[1].estimated_total_cost / order.bucket_results[1].filled_quantity
        assert second_unit_cost < first_unit_cost
    else:
        assert order.bucket_results[1].filled_quantity == order.ordered_quantity
