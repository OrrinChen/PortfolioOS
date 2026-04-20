"""Simple execution slicing helpers."""

from __future__ import annotations

import math

from portfolio_os.domain.models import Basket
from portfolio_os.utils.config import ExecutionProfile


def suggested_child_orders(basket: Basket, profile: ExecutionProfile) -> int:
    """Return a simple slice-count suggestion for the basket."""

    if not basket.orders:
        return 0
    return min(profile.max_child_orders, max(1, len(basket.orders)))


def floor_to_lot_size(quantity: float, lot_size: int) -> int:
    """Floor a floating share quantity to the nearest executable lot."""

    if quantity <= 0:
        return 0
    if lot_size <= 1:
        return int(math.floor(quantity))
    return int(math.floor(quantity / lot_size) * lot_size)


def build_bucket_fill_plan(
    *,
    ordered_quantity: int,
    bucket_available_volumes: list[float],
    participation_limit: float,
    lot_size: int,
    allow_partial_fill: bool,
    force_completion: bool,
) -> tuple[list[int], list[int]]:
    """Return per-bucket caps and fill quantities for one order."""

    caps = [
        floor_to_lot_size(volume * participation_limit, lot_size)
        for volume in bucket_available_volumes
    ]
    if ordered_quantity <= 0:
        return caps, [0 for _ in bucket_available_volumes]
    if not allow_partial_fill and not force_completion and sum(caps) < ordered_quantity:
        return caps, [0 for _ in bucket_available_volumes]

    remaining_quantity = ordered_quantity
    fills: list[int] = []
    last_bucket_index = len(bucket_available_volumes) - 1
    for bucket_index, cap in enumerate(caps):
        fill_quantity = 0
        if remaining_quantity > 0:
            if bucket_index == last_bucket_index and force_completion:
                fill_quantity = remaining_quantity
            elif remaining_quantity <= cap:
                fill_quantity = remaining_quantity
            elif allow_partial_fill:
                fill_quantity = cap
        fills.append(fill_quantity)
        remaining_quantity -= fill_quantity
    return caps, fills
