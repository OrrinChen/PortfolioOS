from __future__ import annotations

import cvxpy as cp
import numpy as np
import pytest

from portfolio_os.optimizer import solver as solver_module


def _solve_with_mocked_objective(
    *,
    monkeypatch,
    sample_context: dict,
    risk_enabled: bool,
    integration_mode: str = "replace",
    component_values: dict[str, float],
    objective_value: float | None,
    weight_overrides: dict[str, float | None],
):
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = bool(risk_enabled)
    config.risk_model.integration_mode = integration_mode
    for field_name, value in weight_overrides.items():
        setattr(config.objective_weights, field_name, value)

    def _fake_build_objective(*args, **kwargs):
        _ = args
        _ = kwargs
        component_exprs = {name: cp.Constant(value) for name, value in component_values.items()}
        return cp.Constant(0.0), component_exprs

    def _fake_solve_once(*, problem, constraints, trades, solver_name, solver_kwargs):
        _ = problem
        _ = constraints
        _ = trades
        _ = solver_kwargs
        return solver_module._SolveAttempt(
            solver_name=solver_name,
            solver_kwargs={},
            status="optimal",
            objective_value=objective_value,
            trade_values=np.zeros(len(universe), dtype=float),
            constraint_residual_max=0.0,
        )

    monkeypatch.setattr(solver_module, "build_objective", _fake_build_objective)
    monkeypatch.setattr(solver_module, "build_hard_constraints", lambda *args, **kwargs: [])
    monkeypatch.setattr(solver_module, "_solver_chain", lambda _config: [("SCS", {})])
    monkeypatch.setattr(solver_module, "_solve_once", _fake_solve_once)

    return solver_module.solve_rebalance_problem(universe, config)


def test_risk_mode_objective_decomposition_fields_and_values(sample_context: dict, monkeypatch) -> None:
    result = _solve_with_mocked_objective(
        monkeypatch=monkeypatch,
        sample_context=sample_context,
        risk_enabled=True,
        integration_mode="replace",
        component_values={
            "risk_term": 10.0,
            "tracking_error": -4.0,
            "transaction_cost": 1.5,
        },
        objective_value=1.25,
        weight_overrides={
            "risk_term": 0.5,
            "tracking_error": 2.0,
            "transaction_cost": 3.0,
        },
    )

    decomposition = result.objective_decomposition
    assert decomposition["mode"] == "risk"
    assert decomposition["integration_mode"] == "replace"
    assert decomposition["objective_value"] == pytest.approx(1.25)
    assert decomposition["abs_weighted_sum"] == pytest.approx(17.5)
    assert set(decomposition["components"].keys()) == {"risk_term", "tracking_error", "transaction_cost"}

    risk_term = decomposition["components"]["risk_term"]
    assert risk_term["raw_value"] == pytest.approx(10.0)
    assert risk_term["weight"] == pytest.approx(0.5)
    assert risk_term["weighted_value"] == pytest.approx(5.0)
    assert risk_term["share_abs_weighted"] == pytest.approx(5.0 / 17.5)

    tracking_error = decomposition["components"]["tracking_error"]
    assert tracking_error["weighted_value"] == pytest.approx(-8.0)
    assert tracking_error["share_abs_weighted"] == pytest.approx(8.0 / 17.5)

    transaction_cost = decomposition["components"]["transaction_cost"]
    assert transaction_cost["weighted_value"] == pytest.approx(4.5)
    assert transaction_cost["share_abs_weighted"] == pytest.approx(4.5 / 17.5)


def test_legacy_mode_objective_decomposition_fields_and_values(sample_context: dict, monkeypatch) -> None:
    result = _solve_with_mocked_objective(
        monkeypatch=monkeypatch,
        sample_context=sample_context,
        risk_enabled=False,
        component_values={
            "target_deviation": 1.0,
            "transaction_fee": 2.0,
            "turnover_penalty": 3.0,
            "slippage_penalty": 4.0,
        },
        objective_value=9.9,
        weight_overrides={
            "target_deviation": 2.0,
            "transaction_fee": 4.0,
            "turnover_penalty": 0.5,
            "slippage_penalty": 1.5,
        },
    )

    decomposition = result.objective_decomposition
    assert decomposition["mode"] == "legacy"
    assert decomposition["integration_mode"] == "legacy"
    assert decomposition["objective_value"] == pytest.approx(9.9)
    assert decomposition["abs_weighted_sum"] == pytest.approx(17.5)
    assert set(decomposition["components"].keys()) == {
        "target_deviation",
        "transaction_fee",
        "turnover_penalty",
        "slippage_penalty",
    }
    assert decomposition["components"]["target_deviation"]["weighted_value"] == pytest.approx(2.0)
    assert decomposition["components"]["transaction_fee"]["weighted_value"] == pytest.approx(8.0)
    assert decomposition["components"]["turnover_penalty"]["weighted_value"] == pytest.approx(1.5)
    assert decomposition["components"]["slippage_penalty"]["weighted_value"] == pytest.approx(6.0)


def test_objective_decomposition_sets_share_zero_when_abs_weighted_sum_zero(
    sample_context: dict,
    monkeypatch,
) -> None:
    result = _solve_with_mocked_objective(
        monkeypatch=monkeypatch,
        sample_context=sample_context,
        risk_enabled=True,
        integration_mode="augment",
        component_values={
            "risk_term": 10.0,
            "tracking_error": 5.0,
            "transaction_cost": 2.0,
            "target_deviation": 1.0,
            "transaction_fee": 2.0,
            "turnover_penalty": 3.0,
            "slippage_penalty": 4.0,
        },
        objective_value=None,
        weight_overrides={
            "risk_term": 0.0,
            "tracking_error": 0.0,
            "transaction_cost": 0.0,
            "target_deviation": 0.0,
            "transaction_fee": 0.0,
            "turnover_penalty": 0.0,
            "slippage_penalty": 0.0,
        },
    )

    decomposition = result.objective_decomposition
    assert decomposition["integration_mode"] == "augment"
    assert set(decomposition["components"].keys()) == {
        "risk_term",
        "tracking_error",
        "transaction_cost",
        "target_deviation",
        "transaction_fee",
        "turnover_penalty",
        "slippage_penalty",
    }
    assert decomposition["abs_weighted_sum"] == pytest.approx(0.0)
    for payload in decomposition["components"].values():
        assert payload["share_abs_weighted"] == pytest.approx(0.0)


def test_augment_mode_objective_decomposition_has_seven_components_and_shares_sum_to_one(
    sample_context: dict,
    monkeypatch,
) -> None:
    result = _solve_with_mocked_objective(
        monkeypatch=monkeypatch,
        sample_context=sample_context,
        risk_enabled=True,
        integration_mode="augment",
        component_values={
            "risk_term": 1.0,
            "tracking_error": 2.0,
            "transaction_cost": -3.0,
            "target_deviation": 4.0,
            "transaction_fee": -5.0,
            "turnover_penalty": 6.0,
            "slippage_penalty": 7.0,
        },
        objective_value=0.0,
        weight_overrides={
            "risk_term": 1.0,
            "tracking_error": 1.0,
            "transaction_cost": 1.0,
            "target_deviation": 1.0,
            "transaction_fee": 1.0,
            "turnover_penalty": 1.0,
            "slippage_penalty": 1.0,
        },
    )

    decomposition = result.objective_decomposition
    assert decomposition["mode"] == "risk"
    assert decomposition["integration_mode"] == "augment"
    assert set(decomposition["components"].keys()) == {
        "risk_term",
        "tracking_error",
        "transaction_cost",
        "target_deviation",
        "transaction_fee",
        "turnover_penalty",
        "slippage_penalty",
    }
    share_sum = sum(float(payload["share_abs_weighted"]) for payload in decomposition["components"].values())
    assert decomposition["abs_weighted_sum"] > 0.0
    assert share_sum == pytest.approx(1.0, abs=1e-12)
