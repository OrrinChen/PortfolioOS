from __future__ import annotations

import pytest
import pandas as pd

from portfolio_os.execution.calibration import ExecutionMarketCurve
from portfolio_os.optimizer.multi_period import (
    build_multi_period_plan,
    describe_future_multi_period_support,
)
from portfolio_os.utils.config import FeeConfig, SlippageConfig, TradingConfig


def _curve(*, first_multiplier: float, second_multiplier: float) -> ExecutionMarketCurve:
    return ExecutionMarketCurve.model_validate(
        {
            "buckets": [
                {"label": "open", "volume_share": 0.5, "slippage_multiplier": first_multiplier},
                {"label": "close", "volume_share": 0.5, "slippage_multiplier": second_multiplier},
            ]
        }
    )


def test_multi_period_plan_prefers_cheaper_bucket_when_liquidity_is_equal() -> None:
    orders = pd.DataFrame(
        [
            {"ticker": "UPST", "side": "BUY", "quantity": 400, "estimated_price": 25.0},
        ]
    )
    market = pd.DataFrame(
        [
            {"ticker": "UPST", "adv_shares": 10000.0},
        ]
    )

    plan = build_multi_period_plan(
        orders_frame=orders,
        market_frame=market,
        market_curve=_curve(first_multiplier=2.0, second_multiplier=0.5),
        fee_config=FeeConfig(commission_rate=0.0003, transfer_fee_rate=0.0, stamp_duty_rate=0.0),
        slippage_config=SlippageConfig(k=1.0, alpha=0.6),
        trading_config=TradingConfig(market="us", lot_size=1),
        participation_limit=0.1,
        allow_partial_fill=True,
        force_completion=False,
        volume_shock_multiplier=1.0,
    )

    order_plan = plan.orders[0]
    assert order_plan.planned_quantity == 400
    assert order_plan.residual_quantity == 0
    first_bucket = order_plan.bucket_allocations[0]
    second_bucket = order_plan.bucket_allocations[1]
    assert second_bucket.planned_quantity > first_bucket.planned_quantity
    assert second_bucket.impact_coefficient < first_bucket.impact_coefficient


def test_multi_period_plan_keeps_residual_when_total_capacity_is_insufficient() -> None:
    orders = pd.DataFrame(
        [
            {"ticker": "RIVN", "side": "SELL", "quantity": 1000, "estimated_price": 15.0},
        ]
    )
    market = pd.DataFrame(
        [
            {"ticker": "RIVN", "adv_shares": 2000.0},
        ]
    )

    plan = build_multi_period_plan(
        orders_frame=orders,
        market_frame=market,
        market_curve=_curve(first_multiplier=1.0, second_multiplier=1.0),
        fee_config=FeeConfig(commission_rate=0.0003, transfer_fee_rate=0.0, stamp_duty_rate=0.0),
        slippage_config=SlippageConfig(k=0.5, alpha=0.6),
        trading_config=TradingConfig(market="us", lot_size=1),
        participation_limit=0.1,
        allow_partial_fill=True,
        force_completion=False,
        volume_shock_multiplier=1.0,
    )

    order_plan = plan.orders[0]
    assert order_plan.planned_quantity == 200
    assert order_plan.residual_quantity == 800
    assert plan.summary.constrained_order_count == 1


def test_multi_period_plan_force_completion_pushes_residual_into_last_bucket() -> None:
    orders = pd.DataFrame(
        [
            {"ticker": "PLTR", "side": "BUY", "quantity": 1000, "estimated_price": 30.0},
        ]
    )
    market = pd.DataFrame(
        [
            {"ticker": "PLTR", "adv_shares": 2000.0},
        ]
    )

    plan = build_multi_period_plan(
        orders_frame=orders,
        market_frame=market,
        market_curve=_curve(first_multiplier=1.0, second_multiplier=1.0),
        fee_config=FeeConfig(commission_rate=0.0003, transfer_fee_rate=0.0, stamp_duty_rate=0.0),
        slippage_config=SlippageConfig(k=0.5, alpha=0.6),
        trading_config=TradingConfig(market="us", lot_size=1),
        participation_limit=0.1,
        allow_partial_fill=True,
        force_completion=True,
        volume_shock_multiplier=1.0,
    )

    order_plan = plan.orders[0]
    assert order_plan.planned_quantity == 1000
    assert order_plan.residual_quantity == 0
    assert order_plan.bucket_allocations[-1].forced_completion is True
    assert plan.summary.forced_completion_order_count == 1


def test_describe_multi_period_support_mentions_cost_aware_bucket_allocation() -> None:
    description = describe_future_multi_period_support()
    assert "cost-aware" in description
    assert "bucket" in description
