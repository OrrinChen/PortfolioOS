from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.optimizer_input_bridge import (
    TypedAlphaOptimizerBridgeConfig,
    inject_typed_expected_returns_into_optimizer_universe,
    write_typed_alpha_optimizer_bridge_artifacts,
)


def _universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ticker": "AAPL", "quantity": 10, "target_weight": 0.2, "current_weight": 0.1},
            {"ticker": "MSFT", "quantity": 10, "target_weight": 0.2, "current_weight": 0.1},
            {"ticker": "NVDA", "quantity": 10, "target_weight": 0.2, "current_weight": 0.1},
        ]
    )


def _expected_panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-02-27",
                "symbol": "AAPL",
                "expected_return": 0.012,
                "event_available_timestamp": "2026-02-26T21:15:00Z",
                "tradable_timestamp": "2026-02-27T14:30:00Z",
                "projection_policy": "event_window_decay",
                "source_config_hash": "panel-hash",
            },
            {
                "date": "2026-02-27",
                "symbol": "MSFT",
                "expected_return": 0.0,
                "event_available_timestamp": "2026-02-26T21:15:00Z",
                "tradable_timestamp": "2026-02-27T14:30:00Z",
                "projection_policy": "event_window_decay",
                "source_config_hash": "panel-hash",
            },
        ]
    )


def _projection_manifest() -> dict[str, object]:
    return {
        "schema_version": "alpha_projection.v2",
        "content_hash": "projection-hash",
        "alpha_view_ids": ["AV-SUE-TEST"],
        "family_id": "US_EVENT_SUE",
        "rebalance_dates": ["2026-02-27"],
        "universe_symbols": ["AAPL", "MSFT", "NVDA"],
    }


def _q2_contract() -> dict[str, object]:
    return {
        "schema_version": "q2_input_contract.v2",
        "alpha_view_id": "AV-SUE-TEST",
        "projection_manifest_hash": "projection-hash",
        "allowed_consumer": "projects/execution_aware_optimizer",
    }


def _abstain_report() -> dict[str, object]:
    return {
        "abstain_report": [
            {
                "date": "2026-02-27",
                "symbol": "NVDA",
                "reason": "coverage_missing",
            }
        ]
    }


def test_default_config_does_not_inject_typed_alpha() -> None:
    run = inject_typed_expected_returns_into_optimizer_universe(
        universe=_universe(),
        expected_return_panel=_expected_panel(),
        projection_manifest=_projection_manifest(),
        q2_input_contract=_q2_contract(),
        alpha_abstain_report=_abstain_report(),
        rebalance_date="2026-02-27",
        config=TypedAlphaOptimizerBridgeConfig(),
    )

    assert run.bridge_status == "disabled"
    assert run.expected_return_reached_actual_optimizer_input is False
    assert run.optimizer_decision_used_typed_expected_return is False
    assert run.expected_return_used_count == 0
    assert run.optimizer_input_with_typed_alpha["typed_alpha_view_status"].eq("not_injected").all()


def test_active_zero_and_missing_coverage_keep_separate_semantics(tmp_path: Path) -> None:
    run = inject_typed_expected_returns_into_optimizer_universe(
        universe=_universe(),
        expected_return_panel=_expected_panel(),
        projection_manifest=_projection_manifest(),
        q2_input_contract=_q2_contract(),
        alpha_abstain_report=_abstain_report(),
        rebalance_date="2026-02-27",
        config=TypedAlphaOptimizerBridgeConfig(allow_typed_alpha_optimizer_injection=True),
    )

    assert run.bridge_status == "injected"
    assert run.expected_return_reached_actual_optimizer_input is True
    assert run.expected_return_used_count == 2
    assert run.active_name_count == 2
    assert run.missing_coverage_count == 1
    assert run.abstain_count == 1

    frame = run.optimizer_input_with_typed_alpha.set_index("ticker")
    assert frame.loc["AAPL", "expected_return"] == pytest.approx(0.012)
    assert frame.loc["AAPL", "typed_alpha_view_status"] == "active_view"
    assert frame.loc["MSFT", "expected_return"] == pytest.approx(0.0)
    assert frame.loc["MSFT", "typed_alpha_view_status"] == "active_view"
    assert frame.loc["MSFT", "typed_alpha_expected_return"] == pytest.approx(0.0)
    assert frame.loc["NVDA", "typed_alpha_view_status"] == "no_view"
    assert frame.loc["NVDA", "typed_alpha_abstain_reason"] == "coverage_missing"
    assert pd.isna(frame.loc["NVDA", "typed_alpha_expected_return"])
    assert frame.loc["NVDA", "expected_return_source"] == "no_view_abstain_objective_neutral_fill"

    artifacts = write_typed_alpha_optimizer_bridge_artifacts(run, tmp_path)
    manifest = json.loads(artifacts["manifest"].read_text(encoding="utf-8"))
    coverage = json.loads(artifacts["coverage_report"].read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "typed_alpha_optimizer_injection_manifest.v1"
    assert manifest["production_approval_claimed"] is False
    assert coverage["missing_coverage_count"] == 1
    assert coverage["no_view_not_encoded_as_zero"] is True


def test_pit_timestamp_violation_is_rejected() -> None:
    panel = _expected_panel()
    panel.loc[0, "event_available_timestamp"] = "2026-02-28T14:30:00Z"

    run = inject_typed_expected_returns_into_optimizer_universe(
        universe=_universe(),
        expected_return_panel=panel,
        projection_manifest=_projection_manifest(),
        q2_input_contract=_q2_contract(),
        alpha_abstain_report=_abstain_report(),
        rebalance_date="2026-02-27",
        config=TypedAlphaOptimizerBridgeConfig(allow_typed_alpha_optimizer_injection=True),
    )

    assert run.bridge_status == "rejected"
    assert any("event_available_timestamp" in reason for reason in run.rejection_reasons)


def test_forbidden_output_fields_are_rejected() -> None:
    contract = _q2_contract()
    contract["broker_output"] = {"symbol": "AAPL", "qty": 1}

    run = inject_typed_expected_returns_into_optimizer_universe(
        universe=_universe(),
        expected_return_panel=_expected_panel(),
        projection_manifest=_projection_manifest(),
        q2_input_contract=contract,
        alpha_abstain_report=_abstain_report(),
        rebalance_date="2026-02-27",
        config=TypedAlphaOptimizerBridgeConfig(allow_typed_alpha_optimizer_injection=True),
    )

    assert run.bridge_status == "rejected"
    assert any("forbidden" in reason.lower() for reason in run.rejection_reasons)
