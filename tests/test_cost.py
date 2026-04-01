from __future__ import annotations

import numpy as np

from portfolio_os.constraints.base import gross_traded_notional
from portfolio_os.cost.fee import estimate_fee, estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage, estimate_slippage_array


def test_fee_direction_and_stamp_duty(sample_context: dict) -> None:
    fee_config = sample_context["config"].fees
    buy_fee = estimate_fee(1000, 50.0, fee_config)
    sell_fee = estimate_fee(-1000, 50.0, fee_config)
    assert buy_fee > 0
    assert sell_fee > buy_fee


def test_slippage_scales_with_order_size(sample_context: dict) -> None:
    slippage_config = sample_context["config"].slippage
    small = estimate_slippage(100.0, 60.0, 2_000_000.0, slippage_config)
    large = estimate_slippage(5000.0, 60.0, 2_000_000.0, slippage_config)
    assert large > small > 0


def test_slippage_alpha_half_matches_legacy_square_root_form(sample_context: dict) -> None:
    slippage_config = sample_context["config"].slippage.model_copy(deep=True)
    slippage_config.alpha = 0.5
    quantity = 1200.0
    price = 60.0
    adv = 2_000_000.0
    expected = price * slippage_config.k * quantity * np.sqrt(quantity / adv)
    assert np.isclose(estimate_slippage(quantity, price, adv, slippage_config), expected)


def test_slippage_alpha_0_6_penalizes_large_orders_more_than_alpha_0_5(sample_context: dict) -> None:
    slippage_half = sample_context["config"].slippage.model_copy(deep=True)
    slippage_half.alpha = 0.5
    slippage_six = sample_context["config"].slippage.model_copy(deep=True)
    slippage_six.alpha = 0.6

    small_q = 500.0
    large_q = 20_000.0
    price = 60.0
    adv = 2_000_000.0
    ratio_half = estimate_slippage(large_q, price, adv, slippage_half) / estimate_slippage(
        small_q, price, adv, slippage_half
    )
    ratio_six = estimate_slippage(large_q, price, adv, slippage_six) / estimate_slippage(
        small_q, price, adv, slippage_six
    )
    assert ratio_six > ratio_half


def test_vector_costs_match_scalar_costs(sample_context: dict) -> None:
    fee_config = sample_context["config"].fees
    slippage_config = sample_context["config"].slippage
    quantities = np.array([100.0, -200.0, 300.0], dtype=float)
    prices = np.array([10.0, 20.0, 30.0], dtype=float)
    adv = np.array([1_000_000.0, 2_000_000.0, 3_000_000.0], dtype=float)

    fee_vector = estimate_fee_array(quantities, prices, fee_config)
    slippage_vector = estimate_slippage_array(quantities, prices, adv, slippage_config)

    assert np.isclose(fee_vector.sum(), sum(estimate_fee(q, p, fee_config) for q, p in zip(quantities, prices, strict=True)))
    assert np.isclose(
        slippage_vector.sum(),
        sum(estimate_slippage(q, p, a, slippage_config) for q, p, a in zip(quantities, prices, adv, strict=True)),
    )


def test_gross_traded_notional_formula_is_fixed() -> None:
    quantities = np.array([100.0, -250.0, 300.0], dtype=float)
    prices = np.array([10.0, 20.0, 30.0], dtype=float)
    assert gross_traded_notional(quantities, prices) == 100.0 * 10.0 + 250.0 * 20.0 + 300.0 * 30.0
