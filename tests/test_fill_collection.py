from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.execution import fill_collection
from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord, ReconciliationReport


def _requested_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["sample_id", "ticker", "direction", "requested_qty", "reference_price", "estimated_price", "requested_notional"],
    )


def _filled_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "sample_id",
            "ticker",
            "direction",
            "requested_qty",
            "filled_qty",
            "avg_fill_price",
            "reference_price",
            "estimated_price",
            "requested_notional",
            "filled_notional",
            "fill_ratio",
            "status",
            "reject_reason",
            "broker_order_id",
            "submitted_at_utc",
            "terminal_at_utc",
            "latency_seconds",
            "poll_count",
            "timeout_cancelled",
            "cancel_requested",
            "cancel_acknowledged",
            "avg_fill_price_fallback_used",
            "status_history",
        ],
    )


def _execution_result(
    *,
    orders: list[OrderExecutionRecord],
    submitted_count: int,
    filled_count: int,
    partial_count: int,
    unfilled_count: int,
    rejected_count: int,
    timeout_cancelled_count: int,
) -> ExecutionResult:
    return ExecutionResult(
        orders=orders,
        submitted_count=submitted_count,
        filled_count=filled_count,
        partial_count=partial_count,
        unfilled_count=unfilled_count,
        rejected_count=rejected_count,
        timeout_cancelled_count=timeout_cancelled_count,
    )


def test_load_orders_from_orders_oms_preserves_extended_hours(tmp_path: Path) -> None:
    orders_path = tmp_path / "orders_oms.csv"
    pd.DataFrame(
        [
            {
                "sample_id": "sample_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 1,
                "reference_price": 99.5,
                "estimated_price": 100.0,
                "price_limit": "",
                "extended_hours": True,
            }
        ]
    ).to_csv(orders_path, index=False, encoding="utf-8")

    baskets = fill_collection.load_orders_from_orders_oms(orders_path)
    assert len(baskets) == 1
    orders = baskets[0].orders
    assert "extended_hours" in orders.columns
    assert bool(orders.loc[0, "extended_hours"]) is True
    assert float(orders.loc[0, "reference_price"]) == pytest.approx(99.5)


def test_fill_manifest_scenarios_cover_empty_cancelled_and_partial() -> None:
    scenarios = [
        {
            "name": "empty_basket",
            "requested": _requested_frame([]),
            "filled": _filled_frame([]),
            "execution_result": _execution_result(
                orders=[],
                submitted_count=0,
                filled_count=0,
                partial_count=0,
                unfilled_count=0,
                rejected_count=0,
                timeout_cancelled_count=0,
            ),
            "latencies": [],
            "expected": {
                "order_count": 0,
                "submitted_count": 0,
                "filled_count": 0,
                "partial_count": 0,
                "unfilled_count": 0,
                "rejected_count": 0,
                "timeout_cancelled_count": 0,
                "fill_rate": None,
                "total_requested_notional": 0.0,
                "total_filled_notional": 0.0,
                "avg_fill_price_mean": None,
                "has_any_filled_orders": False,
            },
        },
        {
            "name": "all_cancelled",
            "requested": _requested_frame(
                [
                    {
                        "sample_id": "basket_01",
                        "ticker": "AAPL",
                        "direction": "buy",
                        "requested_qty": 10.0,
                        "reference_price": 100.0,
                        "estimated_price": 100.0,
                        "requested_notional": 1000.0,
                    },
                    {
                        "sample_id": "basket_01",
                        "ticker": "MSFT",
                        "direction": "sell",
                        "requested_qty": 5.0,
                        "reference_price": 200.0,
                        "estimated_price": 200.0,
                        "requested_notional": 1000.0,
                    },
                ]
            ),
            "filled": _filled_frame(
                [
                    {
                        "sample_id": "basket_01",
                        "ticker": "AAPL",
                        "direction": "buy",
                        "requested_qty": 10.0,
                        "filled_qty": 0.0,
                        "avg_fill_price": None,
                        "reference_price": 100.0,
                        "estimated_price": 100.0,
                        "requested_notional": 1000.0,
                        "filled_notional": None,
                        "fill_ratio": 0.0,
                        "status": "timeout_cancelled",
                        "reject_reason": "timed out",
                        "broker_order_id": "o-1",
                        "submitted_at_utc": "2026-03-26T14:00:00+00:00",
                        "terminal_at_utc": "2026-03-26T14:05:00+00:00",
                        "latency_seconds": 300.0,
                        "poll_count": 2,
                        "timeout_cancelled": True,
                        "cancel_requested": True,
                        "cancel_acknowledged": True,
                        "avg_fill_price_fallback_used": False,
                        "status_history": [],
                    },
                    {
                        "sample_id": "basket_01",
                        "ticker": "MSFT",
                        "direction": "sell",
                        "requested_qty": 5.0,
                        "filled_qty": 0.0,
                        "avg_fill_price": None,
                        "reference_price": 200.0,
                        "estimated_price": 200.0,
                        "requested_notional": 1000.0,
                        "filled_notional": None,
                        "fill_ratio": 0.0,
                        "status": "timeout_cancelled",
                        "reject_reason": "timed out",
                        "broker_order_id": "o-2",
                        "submitted_at_utc": "2026-03-26T14:00:10+00:00",
                        "terminal_at_utc": "2026-03-26T14:05:10+00:00",
                        "latency_seconds": 300.0,
                        "poll_count": 2,
                        "timeout_cancelled": True,
                        "cancel_requested": True,
                        "cancel_acknowledged": False,
                        "avg_fill_price_fallback_used": False,
                        "status_history": [],
                    },
                ]
            ),
            "execution_result": _execution_result(
                orders=[],
                submitted_count=2,
                filled_count=0,
                partial_count=0,
                unfilled_count=2,
                rejected_count=0,
                timeout_cancelled_count=2,
            ),
            "latencies": [300.0, 300.0],
            "expected": {
                "order_count": 2,
                "submitted_count": 2,
                "filled_count": 0,
                "partial_count": 0,
                "unfilled_count": 2,
                "rejected_count": 0,
                "timeout_cancelled_count": 2,
                "fill_rate": 0.0,
                "total_requested_notional": 2000.0,
                "total_filled_notional": 0.0,
                "avg_fill_price_mean": None,
                "has_any_filled_orders": False,
            },
        },
        {
            "name": "partial_fill",
            "requested": _requested_frame(
                [
                    {
                        "sample_id": "basket_02",
                        "ticker": "AAPL",
                        "direction": "buy",
                        "requested_qty": 10.0,
                        "reference_price": 100.0,
                        "estimated_price": 100.0,
                        "requested_notional": 1000.0,
                    },
                    {
                        "sample_id": "basket_02",
                        "ticker": "MSFT",
                        "direction": "sell",
                        "requested_qty": 5.0,
                        "reference_price": 200.0,
                        "estimated_price": 200.0,
                        "requested_notional": 1000.0,
                    },
                ]
            ),
            "filled": _filled_frame(
                [
                    {
                        "sample_id": "basket_02",
                        "ticker": "AAPL",
                        "direction": "buy",
                        "requested_qty": 10.0,
                        "filled_qty": 10.0,
                        "avg_fill_price": 101.0,
                        "reference_price": 100.0,
                        "estimated_price": 100.0,
                        "requested_notional": 1000.0,
                        "filled_notional": 1010.0,
                        "fill_ratio": 1.0,
                        "status": "filled",
                        "reject_reason": None,
                        "broker_order_id": "o-3",
                        "submitted_at_utc": "2026-03-26T14:00:00+00:00",
                        "terminal_at_utc": "2026-03-26T14:00:02+00:00",
                        "latency_seconds": 2.0,
                        "poll_count": 1,
                        "timeout_cancelled": False,
                        "cancel_requested": False,
                        "cancel_acknowledged": False,
                        "avg_fill_price_fallback_used": False,
                        "status_history": [],
                    },
                    {
                        "sample_id": "basket_02",
                        "ticker": "MSFT",
                        "direction": "sell",
                        "requested_qty": 5.0,
                        "filled_qty": 2.0,
                        "avg_fill_price": 110.0,
                        "reference_price": 200.0,
                        "estimated_price": 200.0,
                        "requested_notional": 1000.0,
                        "filled_notional": 220.0,
                        "fill_ratio": 0.4,
                        "status": "partially_filled",
                        "reject_reason": None,
                        "broker_order_id": "o-4",
                        "submitted_at_utc": "2026-03-26T14:00:10+00:00",
                        "terminal_at_utc": "2026-03-26T14:00:12+00:00",
                        "latency_seconds": 2.0,
                        "poll_count": 2,
                        "timeout_cancelled": False,
                        "cancel_requested": False,
                        "cancel_acknowledged": False,
                        "avg_fill_price_fallback_used": False,
                        "status_history": [],
                    },
                ]
            ),
            "execution_result": _execution_result(
                orders=[],
                submitted_count=2,
                filled_count=1,
                partial_count=1,
                unfilled_count=0,
                rejected_count=0,
                timeout_cancelled_count=0,
            ),
            "latencies": [2.0, 2.0],
            "expected": {
                "order_count": 2,
                "submitted_count": 2,
                "filled_count": 1,
                "partial_count": 1,
                "unfilled_count": 0,
                "rejected_count": 0,
                "timeout_cancelled_count": 0,
                "fill_rate": 0.5,
                "total_requested_notional": 2000.0,
                "total_filled_notional": 1230.0,
                "avg_fill_price_mean": pytest.approx(1230.0 / 12.0),
                "has_any_filled_orders": True,
            },
        },
    ]

    for scenario in scenarios:
        manifest = fill_collection.build_fill_manifest(
            run_id=f"run_{scenario['name']}",
            created_at="2026-03-26T14:10:00+00:00",
            market="us",
            broker="alpaca",
            notes=scenario["name"],
            source_type="orders_oms",
            source_path="C:/tmp/orders.csv",
            requested_order_rows=scenario["requested"],
            filled_order_rows=scenario["filled"],
            execution_result=scenario["execution_result"],
            latency_values=scenario["latencies"],
            avg_fill_price_fallback_used=False,
        )

        for key, value in scenario["expected"].items():
            assert manifest[key] == value
        assert manifest["avg_fill_price_mean_definition"] == "sum(filled_notional) / sum(filled_qty) for rows with filled_qty > 0"


def test_fill_collection_order_and_event_schema() -> None:
    rows = [
        {
            "sample_id": "basket_03",
            "ticker": "AAPL",
            "direction": "buy",
            "requested_qty": 10.0,
            "filled_qty": 10.0,
            "avg_fill_price": 101.0,
            "estimated_price": 100.0,
            "requested_notional": 1000.0,
            "filled_notional": 1010.0,
            "fill_ratio": 1.0,
            "status": "filled",
            "reject_reason": None,
            "broker_order_id": "o-5",
            "submitted_at_utc": "2026-03-26T14:00:00+00:00",
            "terminal_at_utc": "2026-03-26T14:00:01+00:00",
            "latency_seconds": 1.0,
            "poll_count": 1,
            "timeout_cancelled": False,
            "cancel_requested": False,
            "cancel_acknowledged": False,
            "avg_fill_price_fallback_used": False,
            "status_history": [
                {
                    "event_type": "terminal",
                    "event_at_utc": "2026-03-26T14:00:01+00:00",
                    "status": "filled",
                    "filled_qty": 10.0,
                    "filled_avg_price": 101.0,
                    "reject_reason": None,
                }
            ],
        }
    ]

    order_frame = fill_collection.build_fill_orders_frame(rows)
    event_frame = fill_collection.build_fill_events_frame(rows)

    assert list(order_frame.columns) == [
        "sample_id",
        "ticker",
        "direction",
            "requested_qty",
            "filled_qty",
            "avg_fill_price",
            "reference_price",
            "estimated_price",
            "requested_notional",
            "filled_notional",
        "fill_ratio",
        "status",
        "reject_reason",
        "broker_order_id",
        "submitted_at_utc",
        "terminal_at_utc",
        "latency_seconds",
        "poll_count",
        "timeout_cancelled",
        "cancel_requested",
        "cancel_acknowledged",
        "avg_fill_price_fallback_used",
        "status_history",
    ]
    assert list(event_frame.columns) == [
        "sample_id",
        "ticker",
        "broker_order_id",
        "event_at_utc",
        "status",
        "filled_qty",
        "filled_avg_price",
        "reject_reason",
        "event_type",
    ]
    assert len(order_frame) == 1
    assert len(event_frame) == 1
    assert event_frame.iloc[0]["event_type"] == "terminal"


def test_generated_fill_collection_batch_orders_oms_is_loader_compatible(tmp_path: Path) -> None:
    orders_path = tmp_path / "orders_oms.csv"
    pd.DataFrame(
        [
            {
                "sample_id": "fill_collection_batch",
                "ticker": "SOFI",
                "direction": "buy",
                "quantity": 7,
                "reference_price": 15.23,
                "estimated_price": 15.23,
                "price_limit": "",
                "extended_hours": False,
                "target_participation_bucket": "0.1%",
                "actual_participation": 0.001,
            }
        ]
    ).to_csv(orders_path, index=False, encoding="utf-8")

    baskets = fill_collection.load_orders_from_orders_oms(orders_path)

    assert len(baskets) == 1
    orders = baskets[0].orders
    assert len(orders) == 1
    assert baskets[0].sample_id == "fill_collection_batch"
    assert orders.loc[0, "ticker"] == "SOFI"
    assert float(orders.loc[0, "requested_qty"]) == pytest.approx(7.0)
    assert bool(orders.loc[0, "extended_hours"]) is False
