from __future__ import annotations

import cvxpy as cp
import numpy as np
import pytest
from pydantic import ValidationError

from portfolio_os.optimizer.objective import build_objective
from portfolio_os.risk.model import RiskModelContext
from portfolio_os.utils.config import RiskModelConfig


def _pre_trade_nav(universe, config) -> float:
    prices = universe["estimated_price"].to_numpy(dtype=float)
    quantities = universe["quantity"].to_numpy(dtype=float)
    return float(np.sum(prices * quantities) + config.portfolio_state.available_cash)


def _mock_risk_context(universe) -> RiskModelContext:
    size = len(universe)
    return RiskModelContext(
        sigma=np.eye(size, dtype=float),
        factor_matrix=np.zeros((size, 1), dtype=float),
        factor_names=["market"],
        target_factor_exposure=np.zeros(1, dtype=float),
        returns_observation_count=252,
        estimator="sample",
    )


def test_replace_mode_keeps_risk_components_only(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = "replace"
    config.objective_weights.risk_term = 2.0
    config.objective_weights.tracking_error = 3.0
    config.objective_weights.transaction_cost = 4.0
    config.objective_weights.target_deviation = 999.0
    config.objective_weights.transaction_fee = 999.0
    config.objective_weights.turnover_penalty = 999.0
    config.objective_weights.slippage_penalty = 999.0

    trades = cp.Variable(len(universe))
    trades.value = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=_mock_risk_context(universe),
    )

    assert set(components.keys()) == {"risk_term", "tracking_error", "transaction_cost", "alpha_reward"}
    assert float(components["alpha_reward"].value) == pytest.approx(0.0)
    risk_total = (
        config.objective_weights.risk_term * float(components["risk_term"].value)
        + config.objective_weights.tracking_error * float(components["tracking_error"].value)
        + config.objective_weights.transaction_cost * float(components["transaction_cost"].value)
    )
    assert float(objective.value) == pytest.approx(risk_total, abs=1e-10)


def test_augment_mode_combines_legacy_and_risk_components(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = "augment"
    config.objective_weights.risk_term = 2.0
    config.objective_weights.tracking_error = 3.0
    config.objective_weights.transaction_cost = 4.0
    config.objective_weights.target_deviation = 5.0
    config.objective_weights.transaction_fee = 6.0
    config.objective_weights.turnover_penalty = 7.0
    config.objective_weights.slippage_penalty = 8.0

    trades = cp.Variable(len(universe))
    trades.value = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=_mock_risk_context(universe),
    )

    assert set(components.keys()) == {
        "alpha_reward",
        "risk_term",
        "tracking_error",
        "transaction_cost",
        "target_deviation",
        "transaction_fee",
        "turnover_penalty",
        "slippage_penalty",
    }
    assert float(components["alpha_reward"].value) == pytest.approx(0.0)
    risk_total = (
        config.objective_weights.risk_term * float(components["risk_term"].value)
        + config.objective_weights.tracking_error * float(components["tracking_error"].value)
        + config.objective_weights.transaction_cost * float(components["transaction_cost"].value)
    )
    legacy_total = (
        float(config.objective_weights.target_deviation or 0.0) * float(components["target_deviation"].value)
        + float(config.objective_weights.transaction_fee or 0.0) * float(components["transaction_fee"].value)
        + float(config.objective_weights.turnover_penalty or 0.0) * float(components["turnover_penalty"].value)
        + float(config.objective_weights.slippage_penalty or 0.0) * float(components["slippage_penalty"].value)
    )
    assert float(objective.value) == pytest.approx(risk_total + legacy_total, abs=1e-10)


def test_alpha_weight_zero_keeps_objective_value_unchanged(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    expected_returns = np.linspace(-0.05, 0.10, len(universe), dtype=float)
    universe["expected_return"] = expected_returns
    config = sample_context["config"].model_copy(deep=True)
    config.objective_weights.alpha_weight = 0.0

    trades = cp.Variable(len(universe))
    trades.value = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=None,
    )

    legacy_total = (
        float(config.objective_weights.target_deviation or 0.0) * float(components["target_deviation"].value)
        + float(config.objective_weights.transaction_fee or 0.0) * float(components["transaction_fee"].value)
        + float(config.objective_weights.turnover_penalty or 0.0) * float(components["turnover_penalty"].value)
        + float(config.objective_weights.slippage_penalty or 0.0) * float(components["slippage_penalty"].value)
    )
    assert "alpha_reward" in components
    assert float(components["alpha_reward"].value) != 0.0
    assert float(objective.value) == pytest.approx(legacy_total, abs=1e-10)


def test_alpha_weight_adds_negative_alpha_reward_term(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    expected_returns = np.linspace(0.20, -0.10, len(universe), dtype=float)
    universe["expected_return"] = expected_returns
    config = sample_context["config"].model_copy(deep=True)
    config.objective_weights.alpha_weight = 2.5

    trades = cp.Variable(len(universe))
    trades.value = np.zeros(len(universe), dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=None,
    )

    alpha_reward = float(components["alpha_reward"].value)
    legacy_total = (
        float(config.objective_weights.target_deviation or 0.0) * float(components["target_deviation"].value)
        + float(config.objective_weights.transaction_fee or 0.0) * float(components["transaction_fee"].value)
        + float(config.objective_weights.turnover_penalty or 0.0) * float(components["turnover_penalty"].value)
        + float(config.objective_weights.slippage_penalty or 0.0) * float(components["slippage_penalty"].value)
    )

    assert "alpha_reward" in components
    assert alpha_reward != 0.0
    assert float(objective.value) == pytest.approx(
        legacy_total - float(config.objective_weights.alpha_weight) * alpha_reward,
        abs=1e-10,
    )


def test_invalid_integration_mode_is_rejected_by_config_schema() -> None:
    with pytest.raises(ValidationError):
        RiskModelConfig.model_validate({"enabled": False, "integration_mode": "bad_mode"})

