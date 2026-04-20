from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from portfolio_os.execution.alpaca_adapter import AlpacaAdapter


class _FakeOrder:
    def __init__(
        self,
        order_id: str,
        status: str,
        filled_qty: float,
        avg_price: float | None,
        *,
        submitted_at: str | None = None,
        filled_at: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        self.id = order_id
        self.status = status
        self.filled_qty = filled_qty
        self.filled_avg_price = avg_price
        self.reject_reason = None
        self.submitted_at = submitted_at
        self.filled_at = filled_at
        self.updated_at = updated_at


class _FakeTradingClient:
    def __init__(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs
        self._orders: dict[str, _FakeOrder] = {}
        self._counter = 0
        self.canceled = False

    def get_account(self):
        return SimpleNamespace(account_number="paper-demo")

    def submit_order(self, order_data):
        self._counter += 1
        order_id = f"order-{self._counter}"
        symbol = str(getattr(order_data, "symbol", ""))
        qty = float(getattr(order_data, "qty", 0.0))
        if symbol == "AAPL":
            self._orders[order_id] = _FakeOrder(order_id, "filled", qty, 190.0)
        else:
            self._orders[order_id] = _FakeOrder(order_id, "partially_filled", qty / 2.0, 410.0)
        return SimpleNamespace(id=order_id)

    def get_order_by_id(self, order_id: str):
        return self._orders[order_id]

    def get_all_positions(self):
        return [
            SimpleNamespace(
                symbol="AAPL",
                qty="10",
                market_value="1900",
                avg_entry_price="180",
                current_price="190",
                unrealized_pl="100",
            ),
            SimpleNamespace(
                symbol="MSFT",
                qty="5",
                market_value="2050",
                avg_entry_price="390",
                current_price="410",
                unrealized_pl="100",
            ),
        ]

    def cancel_orders(self):
        self.canceled = True
        return []

    def cancel_order_by_id(self, order_id: str):
        _ = order_id
        return True


class _PendingOnlyTradingClient:
    def __init__(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs
        self._orders: dict[str, _FakeOrder] = {}
        self._counter = 0
        self.cancelled_order_ids: list[str] = []

    def get_account(self):
        return SimpleNamespace(account_number="paper-demo")

    def submit_order(self, order_data):
        self._counter += 1
        order_id = f"pending-{self._counter}"
        qty = float(getattr(order_data, "qty", 0.0))
        # Use enum-style status string to verify adapter normalization logic.
        self._orders[order_id] = _FakeOrder(order_id, "OrderStatus.Accepted", 0.0, None)
        self._orders[order_id].requested_qty = qty
        return SimpleNamespace(id=order_id)

    def get_order_by_id(self, order_id: str):
        return self._orders[order_id]

    def cancel_order_by_id(self, order_id: str):
        self.cancelled_order_ids.append(order_id)
        return True

    def get_all_positions(self):
        return []

    def cancel_orders(self):
        return []


class _ProgressiveFillTradingClient:
    def __init__(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs
        self._orders: dict[str, _FakeOrder] = {}
        self._counter = 0
        self._poll_counts: dict[str, int] = {}
        self.cancelled_order_ids: list[str] = []

    def get_account(self):
        return SimpleNamespace(account_number="paper-demo")

    def submit_order(self, order_data):
        self._counter += 1
        order_id = f"progressive-{self._counter}"
        qty = float(getattr(order_data, "qty", 0.0))
        self._orders[order_id] = _FakeOrder(
            order_id,
            "new",
            0.0,
            None,
            submitted_at=None,
            filled_at=None,
            updated_at="2026-03-26T14:00:00+00:00",
        )
        self._orders[order_id].requested_qty = qty
        self._poll_counts[order_id] = 0
        return SimpleNamespace(id=order_id)

    def get_order_by_id(self, order_id: str):
        self._poll_counts[order_id] += 1
        order = self._orders[order_id]
        if self._poll_counts[order_id] >= 2:
            order.status = "filled"
            order.filled_qty = float(getattr(order, "requested_qty", 0.0))
            order.filled_avg_price = 190.25
            order.filled_at = "2026-03-26T14:00:05+00:00"
            order.updated_at = "2026-03-26T14:00:05+00:00"
        return order

    def cancel_order_by_id(self, order_id: str):
        self.cancelled_order_ids.append(order_id)
        return True

    def get_all_positions(self):
        return []

    def cancel_orders(self):
        return []

    def get_clock(self):
        return SimpleNamespace(is_open=True)


class _OrderSide:
    BUY = "buy"
    SELL = "sell"


class _TimeInForce:
    DAY = "day"


class _LimitOrderRequest:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class _MarketOrderRequest:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class _LatestTradeRequest:
    def __init__(self, **kwargs) -> None:
        self.symbol_or_symbols = kwargs.get("symbol_or_symbols")


class _LatestQuoteRequest:
    def __init__(self, **kwargs) -> None:
        self.symbol_or_symbols = kwargs.get("symbol_or_symbols")


class _FakeStockDataClient:
    def __init__(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs

    def get_stock_latest_trade(self, request_params):
        symbols = request_params.symbol_or_symbols
        if isinstance(symbols, str):
            symbols = [symbols]
        return {
            str(symbol).strip().upper(): SimpleNamespace(
                symbol=str(symbol).strip().upper(),
                timestamp="2026-04-15T14:29:59+00:00",
                price=500.10,
            )
            for symbol in symbols
        }

    def get_stock_latest_quote(self, request_params):
        symbols = request_params.symbol_or_symbols
        if isinstance(symbols, str):
            symbols = [symbols]
        return {
            str(symbol).strip().upper(): SimpleNamespace(
                symbol=str(symbol).strip().upper(),
                timestamp="2026-04-15T14:30:00+00:00",
                bid_price=500.00,
                ask_price=500.20,
            )
            for symbol in symbols
        }


def _build_adapter(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    adapter = AlpacaAdapter(timeout_seconds=3)
    adapter._alpaca_classes = {
        "TradingClient": _FakeTradingClient,
        "OrderSide": _OrderSide,
        "TimeInForce": _TimeInForce,
        "LimitOrderRequest": _LimitOrderRequest,
        "MarketOrderRequest": _MarketOrderRequest,
    }
    return adapter


def test_alpaca_adapter_query_reference_prices_uses_latest_trade_and_quote(monkeypatch) -> None:
    adapter = _build_adapter(monkeypatch)
    adapter._alpaca_data_classes = {
        "StockHistoricalDataClient": _FakeStockDataClient,
        "StockLatestTradeRequest": _LatestTradeRequest,
        "StockLatestQuoteRequest": _LatestQuoteRequest,
    }

    frame = adapter.query_reference_prices([" spy "])

    assert frame["ticker"].tolist() == ["SPY"]
    assert float(frame.loc[0, "latest_trade_price"]) == pytest.approx(500.10)
    assert float(frame.loc[0, "mid_price"]) == pytest.approx(500.10)
    assert float(frame.loc[0, "reference_price"]) == pytest.approx(500.10)
    assert float(frame.loc[0, "spread_bps"]) == pytest.approx((0.20 / 500.10) * 10000.0)
    assert frame.loc[0, "reference_price_source"] == "mid_price"


def test_alpaca_adapter_submit_orders_and_reconcile(monkeypatch) -> None:
    adapter = _build_adapter(monkeypatch)
    orders = pd.DataFrame(
        [
            {"ticker": "AAPL", "direction": "buy", "quantity": 3, "price_limit": None},
            {"ticker": "MSFT", "direction": "sell", "quantity": 4, "price_limit": 420.0},
        ]
    )

    result = adapter.submit_orders_with_telemetry(orders)
    assert result.submitted_count == 2
    assert result.filled_count == 1
    assert result.partial_count == 1
    assert result.unfilled_count == 0
    assert result.orders[1].status == "partially_filled"
    assert result.orders[1].filled_qty == pytest.approx(2.0)
    assert result.orders[1].avg_fill_price == pytest.approx(410.0)

    positions = adapter.query_positions()
    assert set(positions.columns) == {
        "ticker",
        "quantity",
        "market_value",
        "avg_entry_price",
        "current_price",
        "unrealized_pnl",
    }

    expected = pd.DataFrame(
        [
            {"ticker": "AAPL", "expected_quantity": 10},
            {"ticker": "MSFT", "expected_quantity": 7},
        ]
    )
    report = adapter.reconcile(expected)
    assert report.matched_count == 1
    assert report.mismatched_count >= 1


def test_alpaca_adapter_submit_orders_with_telemetry_records_lifecycle(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    adapter = AlpacaAdapter(timeout_seconds=3, poll_interval_seconds=0.1)
    adapter._alpaca_classes = {
        "TradingClient": _ProgressiveFillTradingClient,
        "OrderSide": _OrderSide,
        "TimeInForce": _TimeInForce,
        "LimitOrderRequest": _LimitOrderRequest,
        "MarketOrderRequest": _MarketOrderRequest,
    }

    orders = pd.DataFrame(
        [
            {"ticker": "AAPL", "direction": "buy", "quantity": 3, "price_limit": None},
        ]
    )

    result = adapter.submit_orders_with_telemetry(orders)
    assert result.submitted_count == 1
    assert result.filled_count == 1
    record = result.orders[0]
    assert record.status == "filled"
    assert record.filled_qty == pytest.approx(3.0)
    assert record.avg_fill_price == pytest.approx(190.25)
    assert record.submitted_at_utc is not None
    assert record.terminal_at_utc is not None
    assert record.poll_count >= 2
    assert isinstance(record.status_history, list)
    assert len(record.status_history) >= 2
    submitted = pd.Timestamp(record.submitted_at_utc)
    terminal = pd.Timestamp(record.terminal_at_utc)
    if submitted.tzinfo is None:
        submitted = submitted.tz_localize("UTC")
    if terminal.tzinfo is None:
        terminal = terminal.tz_localize("UTC")
    assert terminal >= submitted


def test_alpaca_adapter_cancel_all_returns_true(monkeypatch) -> None:
    adapter = _build_adapter(monkeypatch)
    assert adapter.cancel_all() is True


def test_alpaca_adapter_timeout_orders_are_cancelled_and_archived(monkeypatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "demo_key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "demo_secret")
    adapter = AlpacaAdapter(timeout_seconds=1, poll_interval_seconds=0.2)
    adapter._alpaca_classes = {
        "TradingClient": _PendingOnlyTradingClient,
        "OrderSide": _OrderSide,
        "TimeInForce": _TimeInForce,
        "LimitOrderRequest": _LimitOrderRequest,
        "MarketOrderRequest": _MarketOrderRequest,
    }

    tick = {"value": 0.0}

    def _fake_time() -> float:
        tick["value"] += 0.6
        return tick["value"]

    monkeypatch.setattr("portfolio_os.execution.alpaca_adapter.time.sleep", lambda _x: None)
    monkeypatch.setattr("portfolio_os.execution.alpaca_adapter.time.time", _fake_time)

    orders = pd.DataFrame(
        [
            {"ticker": "AAPL", "direction": "buy", "quantity": 3, "price_limit": None},
        ]
    )
    result = adapter.submit_orders(orders)

    assert result.submitted_count == 1
    assert result.unfilled_count == 1
    assert result.filled_count == 0
    assert result.partial_count == 0
    assert result.rejected_count == 0
    assert result.orders[0].status == "timeout_cancelled"
    assert "timed out" in str(result.orders[0].reject_reason or "").lower()
    assert result.orders[0].timeout_cancelled is True
    assert result.orders[0].submitted_at_utc is not None
    assert result.orders[0].terminal_at_utc is not None
    assert result.orders[0].poll_count >= 1
    assert isinstance(result.orders[0].status_history, list)
    assert result.orders[0].status_history
    client = adapter._client_instance()
    assert "pending-1" in client.cancelled_order_ids


@pytest.mark.integration
def test_alpaca_adapter_integration_submit_query_cancel_cycle() -> None:
    api_key = str(os.getenv("ALPACA_API_KEY", "")).strip()
    secret_key = str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    if not api_key or not secret_key:
        pytest.skip("ALPACA_API_KEY/ALPACA_SECRET_KEY not configured")

    adapter = AlpacaAdapter(api_key=api_key, secret_key=secret_key, paper=True, timeout_seconds=5)
    assert adapter.connect() is True
    _ = adapter.query_account()

    orders = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 1,
                "price_limit": 1.0,
            }
        ]
    )
    result = adapter.submit_orders(orders)
    assert result.submitted_count == 1
    assert result.orders
    assert adapter.cancel_all() in {True, False}
