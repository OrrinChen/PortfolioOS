"""Cost-aware multi-period execution planning helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any

import pandas as pd

from portfolio_os.cost.fee import estimate_fee
from portfolio_os.cost.slippage import estimate_slippage
from portfolio_os.domain.enums import OrderSide
from portfolio_os.execution.calibration import ExecutionMarketCurve
from portfolio_os.execution.slicer import floor_to_lot_size
from portfolio_os.utils.config import FeeConfig, SlippageConfig, TradingConfig


@dataclass(frozen=True)
class MultiPeriodBucketAllocation:
    """One bucket-level allocation inside the multi-period plan."""

    bucket_index: int
    bucket_label: str
    bucket_volume_share: float
    bucket_available_volume: float
    bucket_capacity: int
    planned_quantity: int
    residual_after_bucket: int
    slippage_multiplier: float
    impact_coefficient: float
    estimated_fee: float
    estimated_slippage: float
    estimated_total_cost: float
    estimated_fill_price: float | None
    forced_completion: bool = False
    capacity_constrained: bool = False


@dataclass(frozen=True)
class MultiPeriodOrderPlan:
    """One order-level multi-period execution plan."""

    ticker: str
    side: str
    ordered_quantity: int
    planned_quantity: int
    residual_quantity: int
    base_price: float
    participation_limit_used: float
    effective_adv_shares: float
    ordered_notional: float
    planned_notional: float
    residual_notional: float
    estimated_fee: float
    estimated_slippage: float
    estimated_total_cost: float
    bucket_allocations: list[MultiPeriodBucketAllocation] = field(default_factory=list)


@dataclass(frozen=True)
class MultiPeriodPlanSummary:
    """Portfolio-level summary for one multi-period plan."""

    order_count: int
    total_ordered_notional: float
    total_planned_notional: float
    total_residual_notional: float
    total_fee: float
    total_slippage: float
    total_cost: float
    planned_fill_ratio: float
    constrained_order_count: int
    forced_completion_order_count: int


@dataclass(frozen=True)
class MultiPeriodPlan:
    """Multi-order cost-aware execution schedule."""

    market_curve: dict[str, Any]
    participation_limit: float
    allow_partial_fill: bool
    force_completion: bool
    volume_shock_multiplier: float
    slippage: dict[str, float]
    orders: list[MultiPeriodOrderPlan] = field(default_factory=list)
    summary: MultiPeriodPlanSummary | None = None


def describe_future_multi_period_support() -> str:
    """Return a short status description for the current multi-period framework."""

    return (
        "Multi-period optimization now supports cost-aware bucket allocation using "
        "market-curve liquidity, participation limits, and power-law slippage inputs."
    )


def _normalize_side(raw_value: Any) -> str:
    side = str(raw_value or "").strip().upper()
    if side in {OrderSide.BUY.value, "BUY"}:
        return OrderSide.BUY.value
    if side in {OrderSide.SELL.value, "SELL"}:
        return OrderSide.SELL.value
    raise ValueError(f"Unsupported order side: {raw_value!r}")


def _ensure_orders_frame(orders_frame: pd.DataFrame) -> pd.DataFrame:
    if orders_frame.empty:
        return pd.DataFrame(columns=["ticker", "side", "quantity", "estimated_price"])
    work = orders_frame.copy()
    if "side" not in work.columns and "direction" in work.columns:
        work["side"] = work["direction"]
    required = {"ticker", "side", "quantity", "estimated_price"}
    missing = sorted(required - set(work.columns))
    if missing:
        raise ValueError("orders_frame missing required columns: " + ", ".join(missing))
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["side"] = work["side"].apply(_normalize_side)
    work["quantity"] = pd.to_numeric(work["quantity"], errors="raise").astype(int)
    work["estimated_price"] = pd.to_numeric(work["estimated_price"], errors="raise").astype(float)
    if (work["quantity"] <= 0).any():
        raise ValueError("orders_frame contains non-positive quantity values.")
    if (work["estimated_price"] <= 0).any():
        raise ValueError("orders_frame contains non-positive estimated_price values.")
    return work[["ticker", "side", "quantity", "estimated_price"]].reset_index(drop=True)


def _ensure_market_frame(market_frame: pd.DataFrame) -> pd.DataFrame:
    if market_frame.empty:
        raise ValueError("market_frame must not be empty.")
    work = market_frame.copy()
    required = {"ticker", "adv_shares"}
    missing = sorted(required - set(work.columns))
    if missing:
        raise ValueError("market_frame missing required columns: " + ", ".join(missing))
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["adv_shares"] = pd.to_numeric(work["adv_shares"], errors="raise").astype(float)
    if (work["adv_shares"] <= 0).any():
        raise ValueError("market_frame contains non-positive adv_shares values.")
    return work


def _bucket_capacity(
    *,
    available_volume: float,
    participation_limit: float,
    lot_size: int,
) -> int:
    return floor_to_lot_size(float(available_volume) * float(participation_limit), lot_size)


def _allocation_weight(
    *,
    available_volume: float,
    slippage_multiplier: float,
    alpha: float,
) -> float:
    if available_volume <= 0:
        return 0.0
    safe_multiplier = max(float(slippage_multiplier), 1e-12)
    if alpha <= 0:
        return float(available_volume / safe_multiplier)
    return float(available_volume / math.pow(safe_multiplier, 1.0 / alpha))


def _impact_coefficient(
    *,
    price: float,
    available_volume: float,
    slippage_multiplier: float,
    slippage_config: SlippageConfig,
) -> float:
    safe_volume = max(float(available_volume), 1.0)
    return float(
        price
        * float(slippage_config.k)
        * float(slippage_multiplier)
        / math.pow(safe_volume, float(slippage_config.alpha))
    )


def _solve_bucket_quantities(
    *,
    ordered_quantity: int,
    bucket_available_volumes: list[float],
    slippage_multipliers: list[float],
    participation_limit: float,
    lot_size: int,
    alpha: float,
    allow_partial_fill: bool,
    force_completion: bool,
) -> tuple[list[int], list[int], int]:
    caps = [
        _bucket_capacity(
            available_volume=volume,
            participation_limit=participation_limit,
            lot_size=lot_size,
        )
        for volume in bucket_available_volumes
    ]
    if ordered_quantity <= 0:
        return caps, [0 for _ in caps], 0

    total_capacity = int(sum(caps))
    if not allow_partial_fill and not force_completion and total_capacity < ordered_quantity:
        return caps, [0 for _ in caps], ordered_quantity

    target_quantity = int(ordered_quantity if force_completion else min(ordered_quantity, total_capacity))
    if target_quantity <= 0:
        return caps, [0 for _ in caps], ordered_quantity

    weights = [
        _allocation_weight(
            available_volume=volume,
            slippage_multiplier=multiplier,
            alpha=alpha,
        )
        for volume, multiplier in zip(bucket_available_volumes, slippage_multipliers, strict=True)
    ]
    fills_float = [0.0 for _ in caps]
    remaining = float(target_quantity)
    active = [index for index, cap in enumerate(caps) if cap > 0]

    while remaining > 1e-9 and active:
        total_weight = sum(weights[index] for index in active)
        if total_weight <= 0:
            break
        proposed = {
            index: remaining * weights[index] / total_weight
            for index in active
        }
        saturated: list[int] = []
        saturated_any = False
        for index in active:
            capacity_left = float(max(caps[index] - fills_float[index], 0.0))
            allocation = float(proposed[index])
            if allocation >= capacity_left - 1e-9:
                fills_float[index] += capacity_left
                remaining -= capacity_left
                saturated.append(index)
                saturated_any = True
        if saturated_any:
            active = [index for index in active if index not in saturated]
            continue
        for index in active:
            fills_float[index] += float(proposed[index])
        remaining = 0.0

    fills = [floor_to_lot_size(quantity, lot_size) for quantity in fills_float]
    allocated = int(sum(fills))
    residual = int(max(target_quantity - allocated, 0))

    order_rank = sorted(
        range(len(caps)),
        key=lambda index: (
            _impact_coefficient(
                price=1.0,
                available_volume=bucket_available_volumes[index],
                slippage_multiplier=slippage_multipliers[index],
                slippage_config=SlippageConfig(k=1.0, alpha=alpha),
            ),
            -bucket_available_volumes[index],
            index,
        ),
    )
    while residual >= max(lot_size, 1):
        moved = False
        for index in order_rank:
            if fills[index] + lot_size <= caps[index]:
                fills[index] += lot_size
                residual -= lot_size
                moved = True
                break
        if not moved:
            break

    overflow = 0
    if force_completion:
        overflow = max(ordered_quantity - sum(fills), 0)
        if overflow > 0 and fills:
            fills[-1] += overflow
            residual = 0

    residual_quantity = max(ordered_quantity - sum(fills), 0)
    return caps, fills, residual_quantity


def build_multi_period_plan(
    *,
    orders_frame: pd.DataFrame,
    market_frame: pd.DataFrame,
    market_curve: ExecutionMarketCurve,
    fee_config: FeeConfig,
    slippage_config: SlippageConfig,
    trading_config: TradingConfig,
    participation_limit: float,
    allow_partial_fill: bool,
    force_completion: bool,
    volume_shock_multiplier: float = 1.0,
) -> MultiPeriodPlan:
    """Build a cost-aware multi-period allocation plan across liquidity buckets."""

    orders = _ensure_orders_frame(orders_frame)
    market = _ensure_market_frame(market_frame)
    market_lookup = market.set_index("ticker", drop=False)
    order_plans: list[MultiPeriodOrderPlan] = []

    for row in orders.to_dict(orient="records"):
        ticker = str(row["ticker"])
        if ticker not in market_lookup.index:
            raise ValueError(f"Ticker {ticker} is missing from market_frame.")
        side = _normalize_side(row["side"])
        ordered_quantity = int(row["quantity"])
        base_price = float(row["estimated_price"])
        adv_shares = float(market_lookup.loc[ticker, "adv_shares"])
        effective_adv_shares = max(adv_shares * float(volume_shock_multiplier), 1.0)
        bucket_available_volumes = [
            float(effective_adv_shares * bucket.volume_share)
            for bucket in market_curve.buckets
        ]
        slippage_multipliers = [float(bucket.slippage_multiplier) for bucket in market_curve.buckets]
        caps, fills, residual_quantity = _solve_bucket_quantities(
            ordered_quantity=ordered_quantity,
            bucket_available_volumes=bucket_available_volumes,
            slippage_multipliers=slippage_multipliers,
            participation_limit=float(participation_limit),
            lot_size=int(trading_config.lot_size),
            alpha=float(slippage_config.alpha),
            allow_partial_fill=bool(allow_partial_fill),
            force_completion=bool(force_completion),
        )

        remaining = ordered_quantity
        total_fee = 0.0
        total_slippage = 0.0
        total_planned_notional = 0.0
        bucket_allocations: list[MultiPeriodBucketAllocation] = []
        for bucket_index, bucket in enumerate(market_curve.buckets):
            planned_quantity = int(fills[bucket_index])
            signed_quantity = planned_quantity if side == OrderSide.BUY.value else -planned_quantity
            available_volume = float(bucket_available_volumes[bucket_index])
            estimated_slippage = 0.0
            estimated_fee = 0.0
            estimated_fill_price = None
            if planned_quantity > 0:
                estimated_slippage = estimate_slippage(
                    signed_quantity,
                    base_price,
                    available_volume,
                    slippage_config,
                ) * float(bucket.slippage_multiplier)
                price_bump = estimated_slippage / max(base_price * planned_quantity, 1e-12)
                if side == OrderSide.BUY.value:
                    estimated_fill_price = base_price * (1.0 + price_bump)
                else:
                    estimated_fill_price = max(base_price * (1.0 - price_bump), 0.0)
                estimated_fee = estimate_fee(signed_quantity, estimated_fill_price, fee_config)
            estimated_total_cost = float(estimated_fee + estimated_slippage)
            total_fee += float(estimated_fee)
            total_slippage += float(estimated_slippage)
            total_planned_notional += float((estimated_fill_price or base_price) * planned_quantity)
            remaining = max(remaining - planned_quantity, 0)
            bucket_allocations.append(
                MultiPeriodBucketAllocation(
                    bucket_index=bucket_index + 1,
                    bucket_label=bucket.label,
                    bucket_volume_share=float(bucket.volume_share),
                    bucket_available_volume=available_volume,
                    bucket_capacity=int(caps[bucket_index]),
                    planned_quantity=planned_quantity,
                    residual_after_bucket=remaining,
                    slippage_multiplier=float(bucket.slippage_multiplier),
                    impact_coefficient=_impact_coefficient(
                        price=base_price,
                        available_volume=available_volume,
                        slippage_multiplier=float(bucket.slippage_multiplier),
                        slippage_config=slippage_config,
                    ),
                    estimated_fee=float(estimated_fee),
                    estimated_slippage=float(estimated_slippage),
                    estimated_total_cost=estimated_total_cost,
                    estimated_fill_price=estimated_fill_price,
                    forced_completion=bool(force_completion and bucket_index == len(market_curve.buckets) - 1 and planned_quantity > caps[bucket_index]),
                    capacity_constrained=bool(int(caps[bucket_index]) > 0 and planned_quantity >= int(caps[bucket_index])),
                )
            )

        ordered_notional = float(ordered_quantity * base_price)
        residual_notional = float(residual_quantity * base_price)
        order_plans.append(
            MultiPeriodOrderPlan(
                ticker=ticker,
                side=side,
                ordered_quantity=ordered_quantity,
                planned_quantity=int(sum(fills)),
                residual_quantity=int(residual_quantity),
                base_price=base_price,
                participation_limit_used=float(participation_limit),
                effective_adv_shares=float(effective_adv_shares),
                ordered_notional=ordered_notional,
                planned_notional=float(total_planned_notional),
                residual_notional=residual_notional,
                estimated_fee=float(total_fee),
                estimated_slippage=float(total_slippage),
                estimated_total_cost=float(total_fee + total_slippage),
                bucket_allocations=bucket_allocations,
            )
        )

    total_ordered_notional = float(sum(plan.ordered_notional for plan in order_plans))
    total_planned_notional = float(sum(plan.planned_notional for plan in order_plans))
    total_residual_notional = float(sum(plan.residual_notional for plan in order_plans))
    summary = MultiPeriodPlanSummary(
        order_count=int(len(order_plans)),
        total_ordered_notional=total_ordered_notional,
        total_planned_notional=total_planned_notional,
        total_residual_notional=total_residual_notional,
        total_fee=float(sum(plan.estimated_fee for plan in order_plans)),
        total_slippage=float(sum(plan.estimated_slippage for plan in order_plans)),
        total_cost=float(sum(plan.estimated_total_cost for plan in order_plans)),
        planned_fill_ratio=(
            total_planned_notional / total_ordered_notional if total_ordered_notional > 0 else 0.0
        ),
        constrained_order_count=int(sum(1 for plan in order_plans if plan.residual_quantity > 0)),
        forced_completion_order_count=int(
            sum(
                1
                for plan in order_plans
                if any(bucket.forced_completion for bucket in plan.bucket_allocations)
            )
        ),
    )
    return MultiPeriodPlan(
        market_curve=market_curve.model_dump(mode="json"),
        participation_limit=float(participation_limit),
        allow_partial_fill=bool(allow_partial_fill),
        force_completion=bool(force_completion),
        volume_shock_multiplier=float(volume_shock_multiplier),
        slippage=slippage_config.model_dump(mode="json"),
        orders=order_plans,
        summary=summary,
    )
