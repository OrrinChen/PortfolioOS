from __future__ import annotations

import cvxpy as cp
import numpy as np
import pytest
from pydantic import ValidationError

from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.optimizer.objective import build_objective
from portfolio_os.risk.model import RiskModelContext
from portfolio_os.utils.config import ObjectiveWeights, RiskModelConfig


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

    assert set(components.keys()) == {
        "risk_term",
        "tracking_error",
        "transaction_cost",
        "transaction_cost_currency",
        "transaction_cost_fraction",
        "alpha_reward",
    }
    assert float(components["alpha_reward"].value) == pytest.approx(0.0)
    risk_total = (
        config.objective_weights.risk_term * float(components["risk_term"].value)
        + config.objective_weights.tracking_error * float(components["tracking_error"].value)
        + config.objective_weights.transaction_cost * float(components["transaction_cost"].value)
    )
    assert float(objective.value) == pytest.approx(risk_total, abs=1e-10)


def test_augment_mode_uses_only_economic_core_components(sample_context: dict) -> None:
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
        "transaction_cost_currency",
        "transaction_cost_fraction",
    }
    assert float(components["alpha_reward"].value) == pytest.approx(0.0)
    risk_total = (
        config.objective_weights.risk_term * float(components["risk_term"].value)
        + config.objective_weights.tracking_error * float(components["tracking_error"].value)
        + config.objective_weights.transaction_cost * float(components["transaction_cost"].value)
    )
    assert float(objective.value) == pytest.approx(risk_total, abs=1e-10)


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


def test_raw_currency_transaction_cost_mode_preserves_existing_expression(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = "replace"
    config.objective_weights.transaction_cost = 1.0
    config.objective_weights.transaction_cost_objective_mode = "raw_currency"

    trades = cp.Variable(len(universe))
    trade_values = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    trades.value = trade_values
    pre_trade_nav = _pre_trade_nav(universe, config)
    objective, components = build_objective(
        trades,
        universe,
        config,
        pre_trade_nav,
        risk_context=_mock_risk_context(universe),
    )

    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    expected_currency_cost = float(
        estimate_fee_array(trade_values, prices, config.fees).sum()
        + estimate_slippage_array(trade_values, prices, adv_shares, config.slippage).sum()
    )

    assert float(components["transaction_cost_currency"].value) == pytest.approx(expected_currency_cost)
    assert float(components["transaction_cost_fraction"].value) == pytest.approx(expected_currency_cost / pre_trade_nav)
    assert float(components["transaction_cost"].value) == pytest.approx(expected_currency_cost)
    assert float(objective.value) >= float(components["transaction_cost"].value)


def test_nav_fraction_transaction_cost_mode_normalizes_by_pre_trade_nav(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = "replace"
    config.objective_weights.risk_term = 0.0
    config.objective_weights.tracking_error = 0.0
    config.objective_weights.transaction_cost = 1.0
    config.objective_weights.transaction_cost_objective_mode = "nav_fraction"

    trades = cp.Variable(len(universe))
    trade_values = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    trades.value = trade_values
    pre_trade_nav = _pre_trade_nav(universe, config)
    objective, components = build_objective(
        trades,
        universe,
        config,
        pre_trade_nav,
        risk_context=_mock_risk_context(universe),
    )

    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    expected_currency_cost = float(
        estimate_fee_array(trade_values, prices, config.fees).sum()
        + estimate_slippage_array(trade_values, prices, adv_shares, config.slippage).sum()
    )
    expected_fraction_cost = expected_currency_cost / pre_trade_nav

    assert float(components["transaction_cost_currency"].value) == pytest.approx(expected_currency_cost)
    assert float(components["transaction_cost_fraction"].value) == pytest.approx(expected_fraction_cost)
    assert float(components["transaction_cost"].value) == pytest.approx(expected_fraction_cost)
    assert float(objective.value) == pytest.approx(expected_fraction_cost, abs=1e-10)


def test_invalid_transaction_cost_objective_mode_is_rejected_by_schema() -> None:
    with pytest.raises(ValidationError):
        ObjectiveWeights.model_validate({"transaction_cost_objective_mode": "bad_mode"})


def test_objective_weights_default_to_nav_fraction_cost_mode() -> None:
    weights = ObjectiveWeights()

    assert weights.transaction_cost_objective_mode == "nav_fraction"


def test_decision_horizon_days_scales_risk_term_to_period_covariance(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    universe["decision_horizon_days"] = 21
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = True
    config.risk_model.integration_mode = "replace"
    config.risk_model.annualization_factor = 252.0
    config.objective_weights.risk_term = 1.0
    config.objective_weights.tracking_error = 0.0
    config.objective_weights.transaction_cost = 0.0

    pre_trade_nav = _pre_trade_nav(universe, config)
    prices = universe["estimated_price"].to_numpy(dtype=float)
    quantities = universe["quantity"].to_numpy(dtype=float)
    weights = prices * quantities / pre_trade_nav
    annualized_sigma = np.eye(len(universe), dtype=float) * config.risk_model.annualization_factor
    annualized_variance = float(weights @ annualized_sigma @ weights)
    expected_period_variance = annualized_variance * (21.0 / 252.0)

    trades = cp.Variable(len(universe))
    trades.value = np.zeros(len(universe), dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        pre_trade_nav,
        risk_context=RiskModelContext(
            sigma=annualized_sigma,
            factor_matrix=np.zeros((len(universe), 1), dtype=float),
            factor_names=["market"],
            target_factor_exposure=np.zeros(1, dtype=float),
            returns_observation_count=252,
            estimator="sample",
        ),
    )

    assert float(components["risk_term"].value) == pytest.approx(expected_period_variance, abs=1e-10)
    assert float(objective.value) == pytest.approx(expected_period_variance, abs=1e-10)

