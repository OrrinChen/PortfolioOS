"""Alpaca paper-trading adapter."""

from __future__ import annotations

import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from portfolio_os.domain.errors import InputValidationError, ProviderRuntimeError
from portfolio_os.execution.adapters.base import BrokerAdapter
from portfolio_os.execution.models import (
    ExecutionResult,
    OrderExecutionRecord,
    ReconciliationDetail,
    ReconciliationReport,
)


TERMINAL_STATUSES = {"filled", "partially_filled", "canceled", "expired", "rejected"}
PENDING_STATUSES = {"new", "accepted", "pending_new", "pending_replace", "accepted_for_bidding"}


def _resolve_alpaca_keys(
    api_key: str | None,
    secret_key: str | None,
) -> tuple[str, str]:
    resolved_key = str(api_key or "").strip() or str(os.getenv("ALPACA_API_KEY", "")).strip()
    resolved_secret = str(secret_key or "").strip() or str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    if not resolved_key or not resolved_secret:
        raise InputValidationError("Missing Alpaca credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY.")
    return resolved_key, resolved_secret


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp_to_utc_iso(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        ts = pd.Timestamp(raw)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.isoformat()


class AlpacaAdapter(BrokerAdapter):
    """Broker adapter for Alpaca paper trading."""

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
        paper: bool = True,
        *,
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float = 60.0,
    ) -> None:
        self._api_key, self._secret_key = _resolve_alpaca_keys(api_key, secret_key)
        self._paper = bool(paper)
        self._poll_interval_seconds = max(0.2, float(poll_interval_seconds))
        self._timeout_seconds = max(1.0, float(timeout_seconds))
        self._client = None
        self._stock_data_client = None
        self._alpaca_classes: dict[str, Any] | None = None
        self._alpaca_data_classes: dict[str, Any] | None = None

    def _load_alpaca_classes(self) -> dict[str, Any]:
        if self._alpaca_classes is not None:
            return self._alpaca_classes
        try:
            from alpaca.trading.client import TradingClient  # type: ignore
            from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce  # type: ignore
            from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest, MarketOrderRequest  # type: ignore
        except ImportError as exc:
            raise ProviderRuntimeError(
                "alpaca-py is required for AlpacaAdapter. Install with `pip install alpaca-py`."
            ) from exc
        self._alpaca_classes = {
            "TradingClient": TradingClient,
            "OrderSide": OrderSide,
            "QueryOrderStatus": QueryOrderStatus,
            "TimeInForce": TimeInForce,
            "GetOrdersRequest": GetOrdersRequest,
            "LimitOrderRequest": LimitOrderRequest,
            "MarketOrderRequest": MarketOrderRequest,
        }
        return self._alpaca_classes

    def _client_instance(self):
        if self._client is None:
            classes = self._load_alpaca_classes()
            self._client = classes["TradingClient"](
                api_key=self._api_key,
                secret_key=self._secret_key,
                paper=self._paper,
            )
        return self._client

    def _load_alpaca_data_classes(self) -> dict[str, Any]:
        if self._alpaca_data_classes is not None:
            return self._alpaca_data_classes
        try:
            from alpaca.data.historical import StockHistoricalDataClient  # type: ignore
            from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest  # type: ignore
        except ImportError as exc:
            raise ProviderRuntimeError(
                "alpaca-py market-data classes are required for Alpaca reference snapshots."
            ) from exc
        self._alpaca_data_classes = {
            "StockHistoricalDataClient": StockHistoricalDataClient,
            "StockLatestTradeRequest": StockLatestTradeRequest,
            "StockLatestQuoteRequest": StockLatestQuoteRequest,
        }
        return self._alpaca_data_classes

    def _stock_data_client_instance(self):
        if self._stock_data_client is None:
            classes = self._load_alpaca_data_classes()
            self._stock_data_client = classes["StockHistoricalDataClient"](
                api_key=self._api_key,
                secret_key=self._secret_key,
            )
        return self._stock_data_client

    def connect(self) -> bool:
        """Validate connectivity by fetching account metadata."""

        try:
            self._client_instance().get_account()
            return True
        except Exception as exc:
            raise ProviderRuntimeError(f"Alpaca connection failed: {exc}") from exc

    @staticmethod
    def _validate_orders_frame(orders_df: pd.DataFrame) -> pd.DataFrame:
        required_columns = {"ticker", "direction", "quantity"}
        missing = sorted(required_columns - set(orders_df.columns))
        if missing:
            raise InputValidationError(f"orders_df missing required columns: {', '.join(missing)}")
        frame = orders_df.copy()
        frame["ticker"] = frame["ticker"].astype(str).str.strip()
        frame["direction"] = frame["direction"].astype(str).str.strip().str.lower()
        invalid_direction = sorted(set(frame.loc[~frame["direction"].isin({"buy", "sell"}), "direction"]))
        if invalid_direction:
            raise InputValidationError(f"Unsupported direction values: {', '.join(invalid_direction)}")
        frame["quantity"] = pd.to_numeric(frame["quantity"], errors="raise")
        if (frame["quantity"] <= 0).any():
            raise InputValidationError("orders_df contains non-positive quantity values.")
        frame["price_limit"] = pd.to_numeric(frame.get("price_limit"), errors="coerce")
        return frame

    def _submit_single_order(self, row: pd.Series) -> str:
        classes = self._load_alpaca_classes()
        order_side = classes["OrderSide"].BUY if row["direction"] == "buy" else classes["OrderSide"].SELL
        extended_hours = bool(row.get("extended_hours", False))
        if pd.notna(row["price_limit"]):
            request_kwargs: dict[str, Any] = {
                "symbol": str(row["ticker"]),
                "qty": float(row["quantity"]),
                "side": order_side,
                "time_in_force": classes["TimeInForce"].DAY,
                "limit_price": float(row["price_limit"]),
            }
            if extended_hours:
                request_kwargs["extended_hours"] = True
            request = classes["LimitOrderRequest"](**request_kwargs)
        else:
            request = classes["MarketOrderRequest"](
                symbol=str(row["ticker"]),
                qty=float(row["quantity"]),
                side=order_side,
                time_in_force=classes["TimeInForce"].DAY,
            )
        response = self._client_instance().submit_order(order_data=request)
        return str(getattr(response, "id", "")).strip()

    @staticmethod
    def _normalize_order_status(raw_status: Any) -> str:
        """Normalize Alpaca status payloads into plain lower-case status tokens."""

        status = str(raw_status or "").strip().lower()
        if "." in status:
            status = status.split(".")[-1].strip()
        if "partial" in status:
            return "partially_filled"
        if status in {"partialfill", "partial_filled", "partialfilled"}:
            return "partially_filled"
        if status in {"timeoutcancelled", "timeout_cancelled", "timeout-cancelled"}:
            return "timeout_cancelled"
        return status

    @staticmethod
    def _coerce_float(raw_value: Any, default: float = 0.0) -> float:
        try:
            parsed = float(pd.to_numeric(raw_value, errors="coerce"))
        except Exception:
            return default
        if pd.isna(parsed):
            return default
        return float(parsed)

    @classmethod
    def _order_event_snapshot(
        cls,
        order: Any,
        *,
        event_type: str,
        event_at_utc: str | None = None,
        status_override: str | None = None,
    ) -> dict[str, Any]:
        status = status_override or cls._normalize_order_status(getattr(order, "status", ""))
        filled_qty = cls._coerce_float(getattr(order, "filled_qty", 0.0), 0.0)
        filled_avg_raw = pd.to_numeric(getattr(order, "filled_avg_price", None), errors="coerce")
        filled_avg_price = float(filled_avg_raw) if pd.notna(filled_avg_raw) else None
        reject_reason = str(
            getattr(order, "reject_reason", "")
            or getattr(order, "rejected_reason", "")
            or ""
        ).strip() or None
        return {
            "event_type": str(event_type),
            "event_at_utc": event_at_utc or _utc_now_iso(),
            "status": status or "unknown",
            "filled_qty": filled_qty,
            "filled_avg_price": filled_avg_price,
            "reject_reason": reject_reason,
            "broker_order_id": str(getattr(order, "id", "")).strip() or None,
        }

    @staticmethod
    def _snapshot_timestamp(order: Any, *, fallback: str | None = None) -> str:
        for attr in ("submitted_at", "filled_at", "updated_at", "created_at"):
            timestamp = _timestamp_to_utc_iso(getattr(order, attr, None))
            if timestamp is not None:
                return timestamp
        return fallback or _utc_now_iso()

    def _cancel_single_order(self, order_id: str) -> tuple[bool, str | None]:
        """Try best-effort single-order cancellation across alpaca-py client variants."""

        client = self._client_instance()
        cancel_error: str | None = None
        for method_name in ("cancel_order_by_id", "cancel_order"):
            method = getattr(client, method_name, None)
            if method is None:
                continue
            try:
                method(order_id)
                return True, None
            except TypeError:
                try:
                    method(order_id=order_id)
                    return True, None
                except Exception as exc:  # pragma: no cover - defensive fallback
                    cancel_error = str(exc)
            except Exception as exc:  # pragma: no cover - defensive fallback
                cancel_error = str(exc)
        return False, cancel_error

    def _poll_order_terminal(self, order_id: str) -> dict[str, Any]:
        started_at = time.time()
        last_payload: dict[str, Any] = {}
        status_history: list[dict[str, Any]] = []
        submitted_at_utc: str | None = None
        terminal_at_utc: str | None = None
        poll_count = 0
        cancel_requested = False
        cancel_acknowledged = False
        timeout_cancelled = False
        while True:
            order = self._client_instance().get_order_by_id(order_id)
            poll_count += 1
            status = self._normalize_order_status(getattr(order, "status", ""))
            if submitted_at_utc is None:
                submitted_at_utc = self._snapshot_timestamp(order)
            event_at_utc = self._snapshot_timestamp(order, fallback=_utc_now_iso())
            event_type = "terminal" if status in TERMINAL_STATUSES else "poll"
            snapshot = self._order_event_snapshot(
                order,
                event_type=event_type,
                event_at_utc=event_at_utc,
                status_override=status,
            )
            status_history.append(snapshot)
            last_payload = {
                "status": snapshot["status"],
                "filled_qty": snapshot["filled_qty"],
                "filled_avg_price": snapshot["filled_avg_price"],
                "reject_reason": snapshot["reject_reason"],
                "submitted_at_utc": submitted_at_utc,
                "terminal_at_utc": event_at_utc if status in TERMINAL_STATUSES else None,
                "poll_count": poll_count,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "broker_order_id": snapshot["broker_order_id"],
                "status_history": status_history.copy(),
            }
            if status in TERMINAL_STATUSES:
                terminal_at_utc = event_at_utc
                return last_payload
            if status not in PENDING_STATUSES:
                terminal_at_utc = event_at_utc
                return last_payload
            if (time.time() - started_at) > self._timeout_seconds:
                cancel_ok, cancel_error = self._cancel_single_order(order_id)
                timeout_cancelled = True
                cancel_requested = True
                cancel_acknowledged = bool(cancel_ok)
                terminal_at_utc = _utc_now_iso()
                timeout_event = {
                    "event_type": "timeout_cancel_requested",
                    "event_at_utc": terminal_at_utc,
                    "status": "timeout_cancelled",
                    "filled_qty": last_payload["filled_qty"],
                    "filled_avg_price": last_payload["filled_avg_price"],
                    "reject_reason": last_payload["reject_reason"],
                    "broker_order_id": last_payload["broker_order_id"],
                }
                status_history.append(timeout_event)
                if cancel_acknowledged:
                    status_history.append(
                        {
                            "event_type": "timeout_cancel_acknowledged",
                            "event_at_utc": _utc_now_iso(),
                            "status": "timeout_cancelled",
                            "filled_qty": last_payload["filled_qty"],
                            "filled_avg_price": last_payload["filled_avg_price"],
                            "reject_reason": last_payload["reject_reason"],
                            "broker_order_id": last_payload["broker_order_id"],
                        }
                    )
                last_payload["status"] = "timeout_cancelled"
                if not last_payload.get("reject_reason"):
                    last_payload["reject_reason"] = "order status polling timed out; cancellation requested"
                if cancel_error:
                    last_payload["reject_reason"] = (
                        f"{last_payload.get('reject_reason')}; cancel_error={cancel_error}"
                    )
                last_payload["submitted_at_utc"] = submitted_at_utc
                last_payload["terminal_at_utc"] = terminal_at_utc
                last_payload["poll_count"] = poll_count
                last_payload["timeout_cancelled"] = timeout_cancelled
                last_payload["cancel_requested"] = cancel_requested
                last_payload["cancel_acknowledged"] = cancel_acknowledged
                last_payload["status_history"] = status_history.copy()
                return last_payload
            time.sleep(self._poll_interval_seconds)

    def _build_execution_result(self, orders_df: pd.DataFrame) -> ExecutionResult:
        frame = self._validate_orders_frame(orders_df)
        self.connect()

        records: list[OrderExecutionRecord] = []
        for row in frame.to_dict(orient="records"):
            series = pd.Series(row)
            try:
                order_id = self._submit_single_order(series)
                status_payload = self._poll_order_terminal(order_id)
                filled_qty = self._coerce_float(status_payload.get("filled_qty"), 0.0)
                avg_fill_raw = pd.to_numeric(status_payload.get("filled_avg_price"), errors="coerce")
                avg_fill_price = float(avg_fill_raw) if pd.notna(avg_fill_raw) else None
                status = str(status_payload.get("status", "unknown"))
                records.append(
                    OrderExecutionRecord(
                        ticker=str(series["ticker"]),
                        direction=str(series["direction"]),
                        requested_qty=float(series["quantity"]),
                        filled_qty=filled_qty,
                        avg_fill_price=avg_fill_price,
                        status=status,
                        reject_reason=status_payload.get("reject_reason"),
                        order_id=order_id,
                        broker_order_id=status_payload.get("broker_order_id") or order_id,
                        submitted_at_utc=status_payload.get("submitted_at_utc"),
                        terminal_at_utc=status_payload.get("terminal_at_utc"),
                        poll_count=int(status_payload.get("poll_count", 0) or 0),
                        timeout_cancelled=bool(status_payload.get("timeout_cancelled", False)),
                        cancel_requested=bool(status_payload.get("cancel_requested", False)),
                        cancel_acknowledged=bool(status_payload.get("cancel_acknowledged", False)),
                        status_history=[dict(item) for item in list(status_payload.get("status_history") or [])],
                    )
                )
            except Exception as exc:
                records.append(
                    OrderExecutionRecord(
                        ticker=str(series["ticker"]),
                        direction=str(series["direction"]),
                        requested_qty=float(series["quantity"]),
                        filled_qty=0.0,
                        avg_fill_price=None,
                        status="rejected",
                        reject_reason=str(exc),
                        order_id=None,
                    )
                )

        result = ExecutionResult(orders=records, submitted_count=len(records))
        for record in records:
            status = record.status.lower()
            if status == "filled":
                result.filled_count += 1
            elif status == "partially_filled" or (
                record.filled_qty > 0 and record.filled_qty < record.requested_qty
            ):
                result.partial_count += 1
            elif status == "timeout_cancelled":
                result.timeout_cancelled_count += 1
                result.unfilled_count += 1
            elif status == "rejected":
                result.rejected_count += 1
                result.unfilled_count += 1
            elif record.filled_qty <= 0:
                result.unfilled_count += 1
            else:
                result.partial_count += 1
        return result

    def submit_orders(self, orders_df: pd.DataFrame) -> ExecutionResult:
        """Submit orders and wait for terminal statuses."""

        return self._build_execution_result(orders_df)

    def submit_orders_with_telemetry(self, orders_df: pd.DataFrame) -> ExecutionResult:
        """Submit orders and return execution telemetry alongside terminal statuses."""

        return self._build_execution_result(orders_df)

    def query_clock(self) -> dict[str, Any]:
        """Return Alpaca clock payload as a plain mapping."""

        self.connect()
        clock = self._client_instance().get_clock()
        if hasattr(clock, "model_dump"):
            return dict(clock.model_dump(mode="json"))
        if hasattr(clock, "__dict__"):
            return {key: value for key, value in vars(clock).items() if not key.startswith("_")}
        return {"raw": str(clock)}

    def query_positions(self) -> pd.DataFrame:
        """Query Alpaca positions and return a normalized DataFrame."""

        self.connect()
        positions = self._client_instance().get_all_positions()
        rows = []
        for item in positions or []:
            qty = pd.to_numeric(getattr(item, "qty", 0.0), errors="coerce")
            market_value = pd.to_numeric(getattr(item, "market_value", 0.0), errors="coerce")
            avg_entry_price = pd.to_numeric(getattr(item, "avg_entry_price", 0.0), errors="coerce")
            current_price = pd.to_numeric(getattr(item, "current_price", 0.0), errors="coerce")
            unrealized_pnl = pd.to_numeric(getattr(item, "unrealized_pl", 0.0), errors="coerce")
            rows.append(
                {
                    "ticker": str(getattr(item, "symbol", "")).strip(),
                    "quantity": float(qty) if pd.notna(qty) else 0.0,
                    "market_value": float(market_value) if pd.notna(market_value) else 0.0,
                    "avg_entry_price": float(avg_entry_price) if pd.notna(avg_entry_price) else 0.0,
                    "current_price": float(current_price) if pd.notna(current_price) else 0.0,
                    "unrealized_pnl": float(unrealized_pnl) if pd.notna(unrealized_pnl) else 0.0,
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "ticker",
                "quantity",
                "market_value",
                "avg_entry_price",
                "current_price",
                "unrealized_pnl",
            ],
        )

    def query_reference_prices(self, tickers: list[str]) -> pd.DataFrame:
        """Query the latest trade/quote snapshot for a ticker list."""

        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        columns = [
            "ticker",
            "captured_at_utc",
            "latest_trade_price",
            "latest_trade_at_utc",
            "bid_price",
            "ask_price",
            "mid_price",
            "spread_bps",
            "reference_price",
            "reference_price_source",
        ]
        if not normalized:
            return pd.DataFrame(columns=columns)

        classes = self._load_alpaca_data_classes()
        stock_client = self._stock_data_client_instance()
        trade_request = classes["StockLatestTradeRequest"](symbol_or_symbols=normalized)
        quote_request = classes["StockLatestQuoteRequest"](symbol_or_symbols=normalized)
        raw_trades = stock_client.get_stock_latest_trade(trade_request) or {}
        raw_quotes = stock_client.get_stock_latest_quote(quote_request) or {}

        rows: list[dict[str, Any]] = []
        captured_at_utc = _utc_now_iso()
        for ticker in normalized:
            trade = raw_trades.get(ticker)
            quote = raw_quotes.get(ticker)
            latest_trade_price = pd.to_numeric(getattr(trade, "price", None), errors="coerce")
            bid_price = pd.to_numeric(getattr(quote, "bid_price", None), errors="coerce")
            ask_price = pd.to_numeric(getattr(quote, "ask_price", None), errors="coerce")
            mid_price = None
            if pd.notna(bid_price) and pd.notna(ask_price):
                mid_price = float((float(bid_price) + float(ask_price)) / 2.0)
            reference_price = mid_price
            reference_price_source = "mid_price" if reference_price is not None else None
            if reference_price is None and pd.notna(latest_trade_price):
                reference_price = float(latest_trade_price)
                reference_price_source = "latest_trade_price"
            spread_bps = None
            if mid_price is not None and mid_price > 0 and pd.notna(bid_price) and pd.notna(ask_price):
                spread_bps = float((float(ask_price) - float(bid_price)) / mid_price * 10000.0)
            rows.append(
                {
                    "ticker": ticker,
                    "captured_at_utc": captured_at_utc,
                    "latest_trade_price": float(latest_trade_price) if pd.notna(latest_trade_price) else None,
                    "latest_trade_at_utc": _timestamp_to_utc_iso(getattr(trade, "timestamp", None)),
                    "bid_price": float(bid_price) if pd.notna(bid_price) else None,
                    "ask_price": float(ask_price) if pd.notna(ask_price) else None,
                    "mid_price": mid_price,
                    "spread_bps": spread_bps,
                    "reference_price": reference_price,
                    "reference_price_source": reference_price_source,
                }
            )
        return pd.DataFrame(rows, columns=columns)

    def query_open_orders(self) -> pd.DataFrame:
        """Query Alpaca open orders and return a normalized DataFrame."""

        self.connect()
        classes = self._load_alpaca_classes()
        request = classes["GetOrdersRequest"](status=classes["QueryOrderStatus"].OPEN, limit=500, nested=True)
        orders = self._client_instance().get_orders(filter=request)
        rows = []
        for item in orders or []:
            raw_limit_price = pd.to_numeric(getattr(item, "limit_price", None), errors="coerce")
            raw_stop_price = pd.to_numeric(getattr(item, "stop_price", None), errors="coerce")
            raw_qty = pd.to_numeric(getattr(item, "qty", None), errors="coerce")
            raw_filled_qty = pd.to_numeric(getattr(item, "filled_qty", None), errors="coerce")
            rows.append(
                {
                    "order_id": str(getattr(item, "id", "")).strip(),
                    "client_order_id": str(getattr(item, "client_order_id", "")).strip(),
                    "ticker": str(getattr(item, "symbol", "")).strip(),
                    "direction": str(getattr(item, "side", "")).strip().lower(),
                    "order_type": str(getattr(item, "type", "") or getattr(item, "order_type", "")).strip().lower(),
                    "time_in_force": str(getattr(item, "time_in_force", "")).strip().lower(),
                    "status": self._normalize_order_status(getattr(item, "status", "")),
                    "quantity": float(raw_qty) if pd.notna(raw_qty) else 0.0,
                    "filled_qty": float(raw_filled_qty) if pd.notna(raw_filled_qty) else 0.0,
                    "limit_price": float(raw_limit_price) if pd.notna(raw_limit_price) else None,
                    "stop_price": float(raw_stop_price) if pd.notna(raw_stop_price) else None,
                    "extended_hours": bool(getattr(item, "extended_hours", False)),
                    "submitted_at": _timestamp_to_utc_iso(getattr(item, "submitted_at", None)),
                    "created_at": _timestamp_to_utc_iso(getattr(item, "created_at", None)),
                    "updated_at": _timestamp_to_utc_iso(getattr(item, "updated_at", None)),
                    "filled_at": _timestamp_to_utc_iso(getattr(item, "filled_at", None)),
                    "rejected_reason": str(getattr(item, "reject_reason", "") or getattr(item, "rejected_reason", "") or "").strip(),
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "order_id",
                "client_order_id",
                "ticker",
                "direction",
                "order_type",
                "time_in_force",
                "status",
                "quantity",
                "filled_qty",
                "limit_price",
                "stop_price",
                "extended_hours",
                "submitted_at",
                "created_at",
                "updated_at",
                "filled_at",
                "rejected_reason",
            ],
        )

    def query_account(self) -> dict:
        """Return account payload as a plain mapping."""

        self.connect()
        account = self._client_instance().get_account()
        if hasattr(account, "model_dump"):
            return dict(account.model_dump(mode="json"))
        if hasattr(account, "__dict__"):
            return {key: value for key, value in vars(account).items() if not key.startswith("_")}
        return {"raw": str(account)}

    def cancel_all(self) -> bool:
        """Cancel all open orders."""

        self.connect()
        try:
            self._client_instance().cancel_orders()
            return True
        except Exception:
            return False

    def reconcile(self, expected_positions: pd.DataFrame) -> ReconciliationReport:
        """Reconcile expected positions with Alpaca broker positions."""

        if "ticker" not in expected_positions.columns:
            raise InputValidationError("expected_positions must include a ticker column.")
        expected = expected_positions.copy()
        quantity_column = "quantity" if "quantity" in expected.columns else "expected_quantity"
        if quantity_column not in expected.columns:
            raise InputValidationError(
                "expected_positions must include quantity or expected_quantity column."
            )
        expected["ticker"] = expected["ticker"].astype(str).str.strip()
        expected["expected_quantity"] = pd.to_numeric(expected[quantity_column], errors="coerce").fillna(0.0)
        if "expected_value" not in expected.columns:
            if "market_value" in expected.columns:
                expected["expected_value"] = pd.to_numeric(
                    expected["market_value"],
                    errors="coerce",
                ).fillna(0.0)
            else:
                expected["expected_value"] = 0.0
        else:
            expected["expected_value"] = pd.to_numeric(expected["expected_value"], errors="coerce").fillna(0.0)

        broker = self.query_positions().copy()
        if broker.empty:
            broker = pd.DataFrame(columns=["ticker", "quantity", "market_value"])
        broker["ticker"] = broker["ticker"].astype(str).str.strip()
        broker["quantity"] = pd.to_numeric(broker["quantity"], errors="coerce").fillna(0.0)
        broker["market_value"] = pd.to_numeric(broker["market_value"], errors="coerce").fillna(0.0)

        merged = expected[["ticker", "expected_quantity", "expected_value"]].merge(
            broker[["ticker", "quantity", "market_value"]].rename(
                columns={"quantity": "actual_quantity", "market_value": "actual_value"}
            ),
            on="ticker",
            how="outer",
        )
        merged["quantity_diff"] = merged["actual_quantity"].fillna(0.0) - merged["expected_quantity"].fillna(0.0)
        merged["value_diff"] = merged["actual_value"].fillna(0.0) - merged["expected_value"].fillna(0.0)

        details = [
            ReconciliationDetail(
                ticker=str(row["ticker"]),
                expected_quantity=(
                    float(row["expected_quantity"]) if pd.notna(row.get("expected_quantity")) else None
                ),
                actual_quantity=(float(row["actual_quantity"]) if pd.notna(row.get("actual_quantity")) else None),
                quantity_diff=float(row["quantity_diff"]),
                expected_value=(float(row["expected_value"]) if pd.notna(row.get("expected_value")) else None),
                actual_value=(float(row["actual_value"]) if pd.notna(row.get("actual_value")) else None),
                value_diff=float(row["value_diff"]),
            )
            for row in merged.to_dict(orient="records")
        ]

        missing_in_broker = sorted(
            set(
                merged.loc[
                    merged["actual_quantity"].isna() & merged["expected_quantity"].notna(),
                    "ticker",
                ].astype(str)
            )
        )
        missing_in_system = sorted(
            set(
                merged.loc[
                    merged["expected_quantity"].isna() & merged["actual_quantity"].notna(),
                    "ticker",
                ].astype(str)
            )
        )
        matched_count = int(
            (
                merged["expected_quantity"].notna()
                & merged["actual_quantity"].notna()
                & (merged["quantity_diff"].abs() < 1e-9)
            ).sum()
        )
        mismatched_count = int(len(merged) - matched_count)
        return ReconciliationReport(
            matched_count=matched_count,
            mismatched_count=mismatched_count,
            missing_in_broker=missing_in_broker,
            missing_in_system=missing_in_system,
            details=details,
        )

    @staticmethod
    def execution_result_to_frame(result: ExecutionResult) -> pd.DataFrame:
        """Convert execution result into a CSV-friendly frame."""

        rows = [asdict(item) for item in result.orders]
        return pd.DataFrame(
            rows,
            columns=[
                "ticker",
                "direction",
                "requested_qty",
                "filled_qty",
                "avg_fill_price",
                "status",
                "reject_reason",
                "order_id",
                "submitted_at_utc",
                "terminal_at_utc",
                "poll_count",
                "timeout_cancelled",
                "cancel_requested",
                "cancel_acknowledged",
                "broker_order_id",
                "status_history",
            ],
        )
