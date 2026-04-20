"""Bucket-targeted Alpaca fill collection campaign helpers."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from portfolio_os.execution import fill_collection
from portfolio_os.execution.alpaca_adapter import AlpacaAdapter


DEFAULT_SCALE_FACTORS = [0.25, 0.5, 1.0, 2.0, 4.0]
PARTICIPATION_BUCKETS = [
    ("p001_p005", 0.001, 0.005),
    ("p005_p01", 0.005, 0.01),
    ("p01_p02", 0.01, 0.02),
    ("p02_p05", 0.02, 0.05),
    ("p05_plus", 0.05, math.inf),
]
NOTIONAL_BUCKETS = [
    ("n5k_25k", 5_000.0, 25_000.0),
    ("n25k_100k", 25_000.0, 100_000.0),
    ("n100k_250k", 100_000.0, 250_000.0),
]
REDUCTION_PRIORITY_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
REDUCTION_CASH_BUFFER = 5_000.0
REDUCTION_CASH_BUFFER_RATIO = 0.05


@dataclass(frozen=True)
class BucketTarget:
    name: str
    side: str
    participation_min: float
    participation_max: float
    notional_min: float
    notional_max: float
    min_filled_orders: int = 3

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FillCampaignPlan:
    path: Path
    targets: list[BucketTarget]
    selection: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "targets": [target.to_dict() for target in self.targets],
            "selection": self.selection,
        }


@dataclass(frozen=True)
class FillCampaignRun:
    campaign_run_id: str
    task_id: str
    task_hash: str
    source_sample_id: str
    source_path: Path
    scale_factor: float
    target_name: str
    target_side: str
    status: str
    run_dir: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_run_id": self.campaign_run_id,
            "task_id": self.task_id,
            "task_hash": self.task_hash,
            "source_sample_id": self.source_sample_id,
            "source_path": self.source_path,
            "scale_factor": self.scale_factor,
            "target_name": self.target_name,
            "target_side": self.target_side,
            "status": self.status,
            "run_dir": self.run_dir,
        }


def _now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat()


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return default if pd.isna(parsed) else float(parsed)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return default
    return parsed


def _norm_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side in {"long", "buy"}:
        return "buy"
    if side in {"short", "sell"}:
        return "sell"
    return side


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return default


def _normalize_side_scope(value: Any) -> str:
    scope = str(value or "").strip().lower()
    if not scope:
        return "buy-only"
    if scope not in {"buy-only", "sell-only", "both"}:
        raise ValueError("side_scope must be one of: buy-only, sell-only, both")
    return scope


def _normalize_campaign_preset(value: Any) -> str:
    preset = str(value or "").strip().lower()
    if not preset:
        return "coverage"
    if preset not in {"coverage", "seed-inventory", "reduce-positions"}:
        raise ValueError("campaign_preset must be one of: coverage, seed-inventory, reduce-positions")
    return preset


def _collect_tradable_alpaca_symbols(symbols: list[str]) -> set[str]:
    unique_symbols = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    if not unique_symbols:
        return set()
    client = AlpacaAdapter()._client_instance()
    valid_symbols: set[str] = set()
    for symbol in unique_symbols:
        try:
            asset = client.get_asset(symbol)
        except Exception:
            continue
        status = str(getattr(asset, "status", "")).strip().lower()
        tradable = bool(getattr(asset, "tradable", False))
        if tradable and status.endswith("active"):
            valid_symbols.add(symbol)
    return valid_symbols


def _side_scope_allows_direction(scope: str, direction: Any) -> bool:
    normalized = _norm_side(direction)
    if scope == "both":
        return normalized in {"buy", "sell"}
    if scope == "buy-only":
        return normalized == "buy"
    if scope == "sell-only":
        return normalized == "sell"
    raise ValueError(f"unsupported side_scope: {scope}")


def _apply_side_scope(frame: pd.DataFrame, scope: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    allowed_mask = work["direction"].apply(lambda value: _side_scope_allows_direction(scope, value))
    return work.loc[allowed_mask].reset_index(drop=True)


def _task_ticker_summary(frame: pd.DataFrame) -> tuple[str, list[str]]:
    if frame.empty or "ticker" not in frame.columns:
        return "", []
    tickers = sorted({str(value).strip() for value in frame["ticker"].astype(str).tolist() if str(value).strip()})
    return (tickers[0] if tickers else ""), tickers


def _task_notional_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "order_count": 0,
            "total_notional": 0.0,
            "min_notional": None,
            "max_notional": None,
            "max_participation": None,
            "min_participation": None,
            "primary_ticker": "",
            "tickers": [],
        }
    notional_series = pd.to_numeric(frame.get("requested_notional"), errors="coerce")
    participation_series = pd.to_numeric(frame.get("estimated_participation"), errors="coerce")
    primary_ticker, tickers = _task_ticker_summary(frame)
    return {
        "order_count": int(len(frame)),
        "total_notional": float(notional_series.fillna(0.0).sum()),
        "min_notional": float(notional_series.min()) if notional_series.notna().any() else None,
        "max_notional": float(notional_series.max()) if notional_series.notna().any() else None,
        "max_participation": float(participation_series.max()) if participation_series.notna().any() else None,
        "min_participation": float(participation_series.min()) if participation_series.notna().any() else None,
        "primary_ticker": primary_ticker,
        "tickers": tickers,
    }


def _seed_task_sort_key(row: pd.Series) -> tuple[Any, ...]:
    primary_ticker = str(row.get("primary_ticker", "")).strip()
    return (
        float(row.get("estimated_total_notional", 0.0) or 0.0),
        float(row.get("estimated_participation_max", 0.0) or 0.0),
        primary_ticker,
        str(row.get("source_sample_id", "")),
        str(row.get("task_id", "")),
    )


def _trim_seed_inventory_orders(frame: pd.DataFrame, *, remaining_budget: float) -> tuple[pd.DataFrame, float, int]:
    if frame.empty or remaining_budget <= 0:
        return frame.iloc[0:0].copy(), 0.0, 0
    work = frame.copy().reset_index(drop=True)
    if "quantity" not in work.columns and "requested_qty" in work.columns:
        work["quantity"] = work["requested_qty"]
    work["quantity"] = pd.to_numeric(work["quantity"], errors="coerce").fillna(0.0)
    work["price_limit"] = pd.to_numeric(work.get("price_limit"), errors="coerce")
    work["estimated_price"] = pd.to_numeric(work.get("estimated_price"), errors="coerce")
    work["requested_notional"] = pd.to_numeric(work.get("requested_notional"), errors="coerce")
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    if "estimated_notional" not in work.columns:
        work["estimated_notional"] = work["requested_notional"]

    selected_rows: list[dict[str, Any]] = []
    selected_notional = 0.0
    order_count = 0
    ordered = work.sort_values(
        by=["requested_notional", "estimated_participation", "ticker", "source_order_index"],
        ascending=[True, True, True, True],
        kind="mergesort",
        ignore_index=True,
    )
    for row in ordered.to_dict(orient="records"):
        if remaining_budget <= 0:
            break
        price = _f(row.get("price_limit"), None)
        if price is None or price <= 0:
            price = _f(row.get("estimated_price"), None)
        if price is None or price <= 0:
            continue
        requested_qty = max(0.0, _f(row.get("quantity"), 0.0) or 0.0)
        if requested_qty <= 0:
            continue
        order_notional = requested_qty * price
        final_qty = requested_qty
        if order_notional > remaining_budget:
            affordable_qty = math.floor(remaining_budget / price)
            if affordable_qty <= 0:
                break
            final_qty = float(affordable_qty)
            order_notional = final_qty * price
        if final_qty <= 0 or order_notional <= 0:
            continue
        trimmed = dict(row)
        trimmed["quantity"] = float(final_qty)
        trimmed["requested_qty"] = float(final_qty)
        trimmed["requested_notional"] = float(order_notional)
        selected_rows.append(trimmed)
        selected_notional += float(order_notional)
        remaining_budget = max(0.0, float(remaining_budget) - float(order_notional))
        order_count += 1
    return (
        pd.DataFrame(selected_rows, columns=list(frame.columns) if list(frame.columns) else None).reset_index(drop=True),
        float(selected_notional),
        int(order_count),
    )


def _reduction_priority_rank(ticker: str) -> int:
    normalized = str(ticker or "").strip().upper()
    if normalized in REDUCTION_PRIORITY_TICKERS:
        return REDUCTION_PRIORITY_TICKERS.index(normalized)
    return len(REDUCTION_PRIORITY_TICKERS)


def _build_reduce_positions_selection(
    *,
    broker_state_snapshot: dict[str, Any] | None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, Any]]:
    broker_state_summary = _broker_state_snapshot_summary(broker_state_snapshot)
    account = dict((broker_state_snapshot or {}).get("account") or {})
    positions_raw = broker_state_summary.get("positions") or []
    positions = pd.DataFrame(list(positions_raw))
    if positions.empty:
        tasks = pd.DataFrame(
            columns=[
                "task_id",
                "task_hash",
                "source_sample_id",
                "source_path",
                "source_root",
                "scale_factor",
                "target_name",
                "target_side",
                "target_participation_min",
                "target_participation_max",
                "target_notional_min",
                "target_notional_max",
                "side_scope",
                "side_scope_allowed",
                "selected",
                "status",
                "selection_reason",
                "run_dir",
                "input_orders_oms_path",
                "matched_orders_estimate",
                "matched_orders_total_estimate",
                "candidate_order_count",
                "estimated_total_notional",
                "primary_ticker",
                "candidate_tickers_json",
                "mixed_side",
                "missing_adv_count",
                "missing_reference_price_count",
                "estimated_participation_min",
                "estimated_participation_max",
                "estimated_notional_min",
                "estimated_notional_max",
                "broker_state_available",
                "broker_precheck_submittable_count",
                "broker_precheck_buy_submittable_count",
                "broker_precheck_sell_submittable_count",
                "broker_precheck_blocked_order_count",
                "broker_precheck_blocked_reason_counts_json",
                "broker_precheck_blocked_side_counts_json",
                "broker_precheck_status",
                "match_details_json",
                "task_priority",
                "reduction_rank",
                "reduction_requested_qty",
                "reduction_requested_notional",
                "reduction_estimated_price",
                "reduction_cumulative_notional",
                "reduction_target_notional",
            ]
        )
        summary = {
            "reduction_mode": True,
            "reduction_target_notional": 0.0,
            "reduction_selected_task_count": 0,
            "reduction_selected_order_count": 0,
            "reduction_selected_notional": 0.0,
            "reduction_remaining_notional_budget": 0.0,
            "reduction_selected_tickers": [],
            "reduction_selected_quantities": [],
            "reduction_selected_notionals": [],
            "reduction_blocked_by_reason": {"no_positions_available": 1},
            "reduction_blocked_by_side": {"sell": 1},
            "reduction_excluded_not_relevant_task_count": 0,
            "reduction_preset_status": "reduction_not_ready",
            "reduction_sell_only_ready": False,
        }
        return tasks, {}, summary

    if "ticker" not in positions.columns and "symbol" in positions.columns:
        positions["ticker"] = positions["symbol"]
    positions["ticker"] = positions["ticker"].astype(str).str.strip()
    positions["quantity"] = pd.to_numeric(positions.get("quantity"), errors="coerce").fillna(0.0)
    positions["market_value"] = pd.to_numeric(positions.get("market_value"), errors="coerce")
    positions["current_price"] = pd.to_numeric(positions.get("current_price"), errors="coerce")
    positions["avg_entry_price"] = pd.to_numeric(positions.get("avg_entry_price"), errors="coerce")
    positions["estimated_price"] = positions["current_price"].where(positions["current_price"] > 0)
    positions["estimated_price"] = positions["estimated_price"].fillna(positions["market_value"] / positions["quantity"])
    positions["estimated_price"] = positions["estimated_price"].fillna(positions["avg_entry_price"])
    positions = positions.loc[positions["quantity"] > 0].copy().reset_index(drop=True)
    if positions.empty:
        tasks = pd.DataFrame(
            columns=[
                "task_id",
                "task_hash",
                "source_sample_id",
                "source_path",
                "source_root",
                "scale_factor",
                "target_name",
                "target_side",
                "target_participation_min",
                "target_participation_max",
                "target_notional_min",
                "target_notional_max",
                "side_scope",
                "side_scope_allowed",
                "selected",
                "status",
                "selection_reason",
                "run_dir",
                "input_orders_oms_path",
                "matched_orders_estimate",
                "matched_orders_total_estimate",
                "candidate_order_count",
                "estimated_total_notional",
                "primary_ticker",
                "candidate_tickers_json",
                "mixed_side",
                "missing_adv_count",
                "missing_reference_price_count",
                "estimated_participation_min",
                "estimated_participation_max",
                "estimated_notional_min",
                "estimated_notional_max",
                "broker_state_available",
                "broker_precheck_submittable_count",
                "broker_precheck_buy_submittable_count",
                "broker_precheck_sell_submittable_count",
                "broker_precheck_blocked_order_count",
                "broker_precheck_blocked_reason_counts_json",
                "broker_precheck_blocked_side_counts_json",
                "broker_precheck_status",
                "match_details_json",
                "task_priority",
                "reduction_rank",
                "reduction_requested_qty",
                "reduction_requested_notional",
                "reduction_estimated_price",
                "reduction_cumulative_notional",
                "reduction_target_notional",
            ]
        )
        summary = {
            "reduction_mode": True,
            "reduction_target_notional": 0.0,
            "reduction_selected_task_count": 0,
            "reduction_selected_order_count": 0,
            "reduction_selected_notional": 0.0,
            "reduction_remaining_notional_budget": 0.0,
            "reduction_selected_tickers": [],
            "reduction_selected_quantities": [],
            "reduction_selected_notionals": [],
            "reduction_blocked_by_reason": {"no_positions_available": 1},
            "reduction_blocked_by_side": {"sell": 1},
            "reduction_excluded_not_relevant_task_count": 0,
            "reduction_preset_status": "reduction_not_ready",
            "reduction_sell_only_ready": False,
        }
        return tasks, {}, summary

    cash = _f(account.get("cash"), 0.0) or 0.0
    buying_power = _f(account.get("buying_power"), 0.0) or 0.0
    target_release_notional = 0.0
    if buying_power <= 0.0 or cash < 0.0:
        cash_gap = max(0.0, -float(cash))
        target_release_notional = cash_gap + max(REDUCTION_CASH_BUFFER, cash_gap * REDUCTION_CASH_BUFFER_RATIO)
    positions["reduction_rank"] = positions["ticker"].map(_reduction_priority_rank).astype(int)
    positions["estimated_notional"] = positions["quantity"] * positions["estimated_price"]
    positions = positions.sort_values(
        by=["reduction_rank", "market_value", "ticker"],
        ascending=[True, False, True],
        kind="mergesort",
        ignore_index=True,
    )

    rows: list[dict[str, Any]] = []
    task_inputs: dict[str, pd.DataFrame] = {}
    selected_tickers: list[str] = []
    selected_quantities: list[float] = []
    selected_notionals: list[float] = []
    selected_task_count = 0
    selected_order_count = 0
    selected_notional = 0.0
    blocked_by_reason: Counter[str] = Counter()
    blocked_by_side: Counter[str] = Counter()
    remaining_notional_budget = float(target_release_notional)

    for index, row in positions.iterrows():
        ticker = str(row.get("ticker", "")).strip().upper()
        quantity = max(0.0, _f(row.get("quantity"), 0.0) or 0.0)
        estimated_price = _f(row.get("estimated_price"), 0.0) or 0.0
        market_value = _f(row.get("market_value"), 0.0) or float(quantity * estimated_price)
        if not ticker:
            blocked_by_reason["broker_state_stale"] += 1
            blocked_by_side["sell"] += 1
            continue
        if quantity <= 0 or estimated_price <= 0:
            blocked_by_reason["insufficient_quantity"] += 1
            blocked_by_side["sell"] += 1
            continue
        if remaining_notional_budget <= 0 and selected_task_count > 0:
            rows.append(
                {
                    "task_id": f"reduce_{len(rows) + 1:04d}",
                    "task_hash": "",
                    "source_sample_id": f"broker_position_{ticker}",
                    "source_path": "broker_state_snapshot",
                    "source_root": "",
                    "scale_factor": 1.0,
                    "target_name": "reduce_positions",
                    "target_side": "sell",
                    "target_participation_min": None,
                    "target_participation_max": None,
                    "target_notional_min": None,
                    "target_notional_max": None,
                    "side_scope": "sell-only",
                    "side_scope_allowed": True,
                    "selected": False,
                    "status": "skipped",
                    "selection_reason": "target_release_notional_reached",
                    "run_dir": "",
                    "input_orders_oms_path": "",
                    "matched_orders_estimate": 1,
                    "matched_orders_total_estimate": 1,
                    "candidate_order_count": 1,
                    "estimated_total_notional": float(market_value),
                    "primary_ticker": ticker,
                    "candidate_tickers_json": json.dumps([ticker], ensure_ascii=False, sort_keys=True),
                    "mixed_side": False,
                    "missing_adv_count": 0,
                    "missing_reference_price_count": 0,
                    "estimated_participation_min": None,
                    "estimated_participation_max": None,
                    "estimated_notional_min": float(market_value),
                    "estimated_notional_max": float(market_value),
                    "broker_state_available": True,
                    "broker_precheck_submittable_count": 0,
                    "broker_precheck_buy_submittable_count": 0,
                    "broker_precheck_sell_submittable_count": 0,
                    "broker_precheck_blocked_order_count": 0,
                    "broker_precheck_blocked_reason_counts_json": json.dumps({}, ensure_ascii=False, sort_keys=True),
                    "broker_precheck_blocked_side_counts_json": json.dumps({}, ensure_ascii=False, sort_keys=True),
                    "broker_precheck_status": "blocked",
                    "match_details_json": json.dumps([], ensure_ascii=False, sort_keys=True),
                    "task_priority": float(len(REDUCTION_PRIORITY_TICKERS) - _reduction_priority_rank(ticker)),
                    "reduction_rank": int(_reduction_priority_rank(ticker)),
                    "reduction_requested_qty": 0.0,
                    "reduction_requested_notional": 0.0,
                    "reduction_estimated_price": float(estimated_price),
                    "reduction_cumulative_notional": float(selected_notional),
                    "reduction_target_notional": float(target_release_notional),
                }
            )
            continue

        requested_qty = quantity
        if remaining_notional_budget > 0:
            floor_qty = math.floor(remaining_notional_budget / estimated_price) if estimated_price > 0 else 0
            requested_qty = min(quantity, float(max(1, floor_qty)))
        requested_qty = float(min(quantity, max(1.0, requested_qty)))
        requested_notional = float(requested_qty * estimated_price)
        if requested_notional <= 0:
            blocked_by_reason["insufficient_quantity"] += 1
            blocked_by_side["sell"] += 1
            continue

        task_id = f"reduce_{len(rows) + 1:04d}"
        order_frame = pd.DataFrame(
            [
                {
                    "sample_id": f"broker_position_{ticker}",
                    "ticker": ticker,
                    "direction": "sell",
                    "quantity": float(requested_qty),
                    "requested_qty": float(requested_qty),
                    "estimated_price": float(estimated_price),
                    "price_limit": float(round(estimated_price * 0.995, 2)),
                    "extended_hours": True,
                    "requested_notional": float(requested_notional),
                    "estimated_participation": pd.NA,
                    "source_order_index": 0,
                }
            ]
        )
        task_inputs[task_id] = order_frame
        rows.append(
            {
                "task_id": task_id,
                "task_hash": hashlib.sha256(f"reduce:{ticker}:{requested_qty}:{estimated_price}".encode("utf-8")).hexdigest()[:16],
                "source_sample_id": f"broker_position_{ticker}",
                "source_path": "broker_state_snapshot",
                "source_root": "",
                "scale_factor": 1.0,
                "target_name": "reduce_positions",
                "target_side": "sell",
                "target_participation_min": None,
                "target_participation_max": None,
                "target_notional_min": None,
                "target_notional_max": None,
                "side_scope": "sell-only",
                "side_scope_allowed": True,
                "selected": True,
                "status": "selected",
                "selection_reason": "selected_for_reduce_positions",
                "run_dir": "",
                "input_orders_oms_path": "",
                "matched_orders_estimate": 1,
                "matched_orders_total_estimate": 1,
                "candidate_order_count": 1,
                "estimated_total_notional": float(market_value),
                "primary_ticker": ticker,
                "candidate_tickers_json": json.dumps([ticker], ensure_ascii=False, sort_keys=True),
                "mixed_side": False,
                "missing_adv_count": 0,
                "missing_reference_price_count": 0,
                "estimated_participation_min": None,
                "estimated_participation_max": None,
                "estimated_notional_min": float(requested_notional),
                "estimated_notional_max": float(requested_notional),
                "broker_state_available": True,
                "broker_precheck_submittable_count": 1,
                "broker_precheck_buy_submittable_count": 0,
                "broker_precheck_sell_submittable_count": 1,
                "broker_precheck_blocked_order_count": 0,
                "broker_precheck_blocked_reason_counts_json": json.dumps({}, ensure_ascii=False, sort_keys=True),
                "broker_precheck_blocked_side_counts_json": json.dumps({}, ensure_ascii=False, sort_keys=True),
                "broker_precheck_status": "ready",
                "match_details_json": json.dumps(
                    [
                        {
                            "target_name": "reduce_positions",
                            "side": "sell",
                            "match_count": 1,
                            "participation_min": None,
                            "participation_max": None,
                            "notional_min": None,
                            "notional_max": None,
                        }
                    ],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "task_priority": float(len(REDUCTION_PRIORITY_TICKERS) - _reduction_priority_rank(ticker)),
                "reduction_rank": int(_reduction_priority_rank(ticker)),
                "reduction_requested_qty": float(requested_qty),
                "reduction_requested_notional": float(requested_notional),
                "reduction_estimated_price": float(estimated_price),
                "reduction_cumulative_notional": float(selected_notional + requested_notional),
                "reduction_target_notional": float(target_release_notional),
            }
        )
        selected_tickers.append(ticker)
        selected_quantities.append(float(requested_qty))
        selected_notionals.append(float(requested_notional))
        selected_task_count += 1
        selected_order_count += 1
        selected_notional += float(requested_notional)
        remaining_notional_budget = max(0.0, float(remaining_notional_budget) - float(requested_notional))

    tasks = pd.DataFrame(rows)
    if tasks.empty:
        tasks = pd.DataFrame(
            columns=[
                "task_id",
                "task_hash",
                "source_sample_id",
                "source_path",
                "source_root",
                "scale_factor",
                "target_name",
                "target_side",
                "target_participation_min",
                "target_participation_max",
                "target_notional_min",
                "target_notional_max",
                "side_scope",
                "side_scope_allowed",
                "selected",
                "status",
                "selection_reason",
                "run_dir",
                "input_orders_oms_path",
                "matched_orders_estimate",
                "matched_orders_total_estimate",
                "candidate_order_count",
                "estimated_total_notional",
                "primary_ticker",
                "candidate_tickers_json",
                "mixed_side",
                "missing_adv_count",
                "missing_reference_price_count",
                "estimated_participation_min",
                "estimated_participation_max",
                "estimated_notional_min",
                "estimated_notional_max",
                "broker_state_available",
                "broker_precheck_submittable_count",
                "broker_precheck_buy_submittable_count",
                "broker_precheck_sell_submittable_count",
                "broker_precheck_blocked_order_count",
                "broker_precheck_blocked_reason_counts_json",
                "broker_precheck_blocked_side_counts_json",
                "broker_precheck_status",
                "match_details_json",
                "task_priority",
                "reduction_rank",
                "reduction_requested_qty",
                "reduction_requested_notional",
                "reduction_estimated_price",
                "reduction_cumulative_notional",
                "reduction_target_notional",
            ]
        )
    tasks["selected"] = tasks.get("selected", False).astype(bool) if "selected" in tasks.columns else False
    summary = {
        "reduction_mode": True,
        "reduction_target_notional": float(target_release_notional),
        "reduction_selected_task_count": int(selected_task_count),
        "reduction_selected_order_count": int(selected_order_count),
        "reduction_selected_notional": float(selected_notional),
        "reduction_remaining_notional_budget": float(max(0.0, remaining_notional_budget)),
        "reduction_selected_tickers": selected_tickers,
        "reduction_selected_quantities": selected_quantities,
        "reduction_selected_notionals": selected_notionals,
        "reduction_blocked_by_reason": {str(key): int(value) for key, value in blocked_by_reason.items()},
        "reduction_blocked_by_side": {str(key): int(value) for key, value in blocked_by_side.items()},
        "reduction_excluded_not_relevant_task_count": int((tasks["selection_reason"].astype(str).str.strip() == "target_release_notional_reached").sum())
        if not tasks.empty and "selection_reason" in tasks.columns
        else 0,
        "reduction_preset_status": (
            "reduction_successful" if selected_order_count > 0 else "reduction_limited_by_broker_state"
        ),
        "reduction_sell_only_ready": bool(selected_order_count > 0),
    }
    return tasks.reset_index(drop=True), task_inputs, summary


def _apply_seed_inventory_selection(
    *,
    tasks: pd.DataFrame,
    task_inputs: dict[str, pd.DataFrame],
    broker_state_snapshot: dict[str, Any] | None,
    max_seed_notional: float,
    max_seed_orders: int,
    use_extended_hours: bool = False,
    seed_safety_factor: float = 0.8,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, Any]]:
    extended_hours_price_limit_multiplier = 1.01 if use_extended_hours else 1.0
    broker_state_summary = _broker_state_snapshot_summary(broker_state_snapshot)
    available_buying_power = max(0.0, _f(broker_state_summary.get("buying_power"), 0.0) or 0.0)
    seed_notional_budget = max(0.0, min(float(max_seed_notional), available_buying_power * float(seed_safety_factor)))

    work = tasks.copy()
    if work.empty:
        return work, task_inputs, {
            "seed_inventory_mode": True,
            "available_buying_power": float(available_buying_power),
            "seed_notional_budget": float(seed_notional_budget),
            "seed_safety_factor": float(seed_safety_factor),
            "seed_selected_task_count": 0,
            "seed_selected_order_count": 0,
            "seed_selected_notional": 0.0,
            "seed_remaining_notional_budget": float(seed_notional_budget),
            "seed_excluded_not_relevant_task_count": int(
                (work.get("selection_reason", pd.Series(dtype="object")).astype(str).str.strip() == "side_scope_excluded").sum()
            )
            if "selection_reason" in work.columns
            else 0,
            "seed_preset_status": "seed_inventory_limited_by_buying_power",
        }

    for column in (
        "selected",
        "status",
        "selection_reason",
        "run_dir",
        "input_orders_oms_path",
        "broker_precheck_submittable_count",
        "broker_precheck_buy_submittable_count",
        "broker_precheck_sell_submittable_count",
        "broker_precheck_blocked_order_count",
        "broker_precheck_blocked_reason_counts_json",
        "broker_precheck_blocked_side_counts_json",
        "broker_precheck_status",
    ):
        if column in work.columns:
            if column in {"selected"}:
                work[column] = False
            elif column in {"status"}:
                work[column] = "skipped"
            else:
                work[column] = ""

    selected_rows: list[int] = []
    selected_task_ids: set[str] = set()
    selected_task_count = 0
    selected_order_count = 0
    selected_notional = 0.0
    blocked_order_count = 0
    budget_limited_count = 0
    max_orders_reached_count = 0
    asset_unavailable_count = 0

    buy_mask = work["target_side"].astype(str).str.lower().eq("buy") if "target_side" in work.columns else pd.Series(False, index=work.index)
    candidate_indices = work.loc[buy_mask].copy()
    if not candidate_indices.empty:
        candidate_indices = candidate_indices.sort_values(
            by=["estimated_total_notional", "estimated_participation_max", "primary_ticker", "source_sample_id", "task_id"],
            ascending=[True, True, True, True, True],
            kind="mergesort",
            ignore_index=False,
        )
    valid_buy_symbols = (
        _collect_tradable_alpaca_symbols(candidate_indices["primary_ticker"].astype(str).tolist())
        if not candidate_indices.empty and "primary_ticker" in candidate_indices.columns
        else set()
    )

    for index, row in candidate_indices.iterrows():
        task_id = str(row.get("task_id", ""))
        if not task_id or task_id in selected_task_ids:
            continue
        ticker = str(row.get("primary_ticker", "")).strip().upper()
        if valid_buy_symbols and ticker not in valid_buy_symbols:
            work.loc[index, "selection_reason"] = "broker_asset_not_tradable"
            work.loc[index, "broker_precheck_blocked_reason_counts_json"] = json.dumps(
                {"broker_asset_not_tradable": 1},
                ensure_ascii=False,
                sort_keys=True,
            )
            work.loc[index, "broker_precheck_blocked_side_counts_json"] = json.dumps(
                {"buy": 1},
                ensure_ascii=False,
                sort_keys=True,
            )
            asset_unavailable_count += 1
            blocked_order_count += int(_safe_int(work.loc[index, "candidate_order_count"], 1))
            continue
        if selected_task_count >= int(max_seed_orders):
            work.loc[index, "selection_reason"] = "seed_inventory_max_orders_reached"
            max_orders_reached_count += 1
            continue
        remaining_budget = max(0.0, float(seed_notional_budget) - float(selected_notional))
        if remaining_budget <= 0:
            work.loc[index, "selection_reason"] = "buying_power_budget_exhausted"
            budget_limited_count += 1
            continue
        task_frame = task_inputs.get(task_id, pd.DataFrame()).copy()
        if task_frame.empty:
            work.loc[index, "selection_reason"] = "no_candidate_tasks"
            continue
        trimmed_frame, trimmed_notional, trimmed_order_count = _trim_seed_inventory_orders(
            task_frame,
            remaining_budget=remaining_budget,
        )
        if trimmed_frame.empty or trimmed_order_count <= 0 or trimmed_notional <= 0:
            work.loc[index, "selection_reason"] = "buying_power_budget_exhausted"
            budget_limited_count += 1
            blocked_order_count += int(len(task_frame))
            continue
        task_inputs[task_id] = trimmed_frame.reset_index(drop=True)
        if "price_limit" not in task_inputs[task_id].columns:
            task_inputs[task_id]["price_limit"] = pd.NA
        task_inputs[task_id]["price_limit"] = pd.to_numeric(task_inputs[task_id].get("price_limit"), errors="coerce").fillna(
            pd.to_numeric(task_inputs[task_id].get("estimated_price"), errors="coerce")
        )
        if use_extended_hours:
            task_inputs[task_id]["extended_hours"] = True
            buy_mask = task_inputs[task_id]["direction"].astype(str).str.lower().eq("buy")
            if buy_mask.any():
                buy_price_limit = pd.to_numeric(task_inputs[task_id].loc[buy_mask, "price_limit"], errors="coerce")
                buy_aggressive_limit = pd.to_numeric(task_inputs[task_id].loc[buy_mask, "estimated_price"], errors="coerce").mul(
                    extended_hours_price_limit_multiplier
                )
                buy_limit_frame = pd.concat(
                    [buy_price_limit, buy_aggressive_limit],
                    axis=1,
                ).max(axis=1)
                task_inputs[task_id].loc[buy_mask, "price_limit"] = buy_limit_frame.apply(
                    lambda value: math.ceil(float(value) * 100.0) / 100.0 if pd.notna(value) else value
                )
        precheck_frame, precheck = _broker_state_precheck(trimmed_frame, broker_state_snapshot=broker_state_snapshot)
        work.loc[index, "selected"] = True
        work.loc[index, "status"] = "selected"
        work.loc[index, "selection_reason"] = "seed_inventory_selected"
        work.loc[index, "broker_precheck_submittable_count"] = int(precheck.get("submitted_count", 0) or 0)
        work.loc[index, "broker_precheck_buy_submittable_count"] = int(precheck.get("submitted_buy_count", 0) or 0)
        work.loc[index, "broker_precheck_sell_submittable_count"] = int(precheck.get("submitted_sell_count", 0) or 0)
        work.loc[index, "broker_precheck_blocked_order_count"] = int(precheck.get("skipped_count", 0) or 0)
        work.loc[index, "broker_precheck_blocked_reason_counts_json"] = json.dumps(
            precheck.get("blocked_by_reason", {}),
            ensure_ascii=False,
            sort_keys=True,
        )
        work.loc[index, "broker_precheck_blocked_side_counts_json"] = json.dumps(
            precheck.get("blocked_by_side", {}),
            ensure_ascii=False,
            sort_keys=True,
        )
        work.loc[index, "broker_precheck_status"] = "ready" if int(precheck.get("submitted_count", 0) or 0) > 0 else "blocked"
        work.loc[index, "seed_inventory_requested_notional"] = float(_f(row.get("estimated_total_notional"), 0.0) or 0.0)
        work.loc[index, "seed_inventory_selected_notional"] = float(trimmed_notional)
        work.loc[index, "seed_inventory_selected_order_count"] = int(trimmed_order_count)
        selected_rows.append(index)
        selected_task_ids.add(task_id)
        selected_task_count += 1
        selected_order_count += int(trimmed_order_count)
        selected_notional += float(trimmed_notional)

    if "seed_inventory_requested_notional" not in work.columns:
        work["seed_inventory_requested_notional"] = 0.0
    if "seed_inventory_selected_notional" not in work.columns:
        work["seed_inventory_selected_notional"] = 0.0
    if "seed_inventory_selected_order_count" not in work.columns:
        work["seed_inventory_selected_order_count"] = 0

    for index in work.index:
        task_id = str(work.loc[index, "task_id"])
        if work.loc[index, "selected"]:
            continue
        if str(work.loc[index, "target_side"]).strip().lower() != "buy":
            if work.loc[index, "selection_reason"] == "":
                work.loc[index, "selection_reason"] = "side_scope_excluded"
            continue
        if work.loc[index, "selection_reason"] == "":
            work.loc[index, "selection_reason"] = "buying_power_budget_exhausted" if seed_notional_budget <= 0 else "seed_inventory_not_selected"
        if "broker_precheck_blocked_reason_counts_json" in work.columns and not work.loc[index, "broker_precheck_blocked_reason_counts_json"]:
            work.loc[index, "broker_precheck_blocked_reason_counts_json"] = json.dumps({"buying_power_budget_exhausted": 1}, ensure_ascii=False, sort_keys=True)
        if "broker_precheck_blocked_side_counts_json" in work.columns and not work.loc[index, "broker_precheck_blocked_side_counts_json"]:
            work.loc[index, "broker_precheck_blocked_side_counts_json"] = json.dumps({"buy": 1}, ensure_ascii=False, sort_keys=True)
        if work.loc[index, "selection_reason"] == "buying_power_budget_exhausted":
            budget_limited_count += 1
            blocked_order_count += int(_safe_int(work.loc[index, "candidate_order_count"], 1))

    work["selected"] = work["selected"].astype(bool)
    seed_summary = {
        "seed_inventory_mode": True,
        "available_buying_power": float(available_buying_power),
        "seed_notional_budget": float(seed_notional_budget),
        "seed_safety_factor": float(seed_safety_factor),
        "seed_extended_hours_price_limit_multiplier": float(extended_hours_price_limit_multiplier),
        "seed_selected_task_count": int(selected_task_count),
        "seed_selected_order_count": int(selected_order_count),
        "seed_selected_notional": float(selected_notional),
        "seed_remaining_notional_budget": float(max(0.0, seed_notional_budget - selected_notional)),
        "seed_budget_limited_task_count": int(budget_limited_count),
        "seed_max_orders_limited_task_count": int(max_orders_reached_count),
        "seed_asset_not_tradable_task_count": int(asset_unavailable_count),
        "seed_excluded_not_relevant_task_count": int(
            (work["selection_reason"].astype(str).str.strip() == "side_scope_excluded").sum()
        ),
        "seed_preset_status": (
            "seed_inventory_successful"
            if selected_order_count > 0
            else ("seed_inventory_limited_by_broker_assets" if asset_unavailable_count > 0 else "seed_inventory_limited_by_buying_power")
        ),
    }
    return work.reset_index(drop=True), task_inputs, seed_summary


def _parse_json_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def _parse_json_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _broker_state_snapshot_summary(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not snapshot:
        return {
            "available": False,
            "captured_at_utc": None,
            "cash": None,
            "buying_power": None,
            "equity": None,
            "portfolio_value": None,
            "positions_count": 0,
            "position_tickers": [],
            "position_quantity_by_ticker": {},
            "positions": [],
            "open_orders_count": 0,
            "open_order_tickers": [],
            "open_order_status_counts": {},
            "open_orders": [],
        }

    if "account" not in snapshot and "positions" not in snapshot and any(
        key in snapshot for key in ("cash", "buying_power", "positions_count", "position_tickers", "position_quantity_by_ticker")
    ):
        return {
            "available": bool(snapshot.get("available", True)),
            "captured_at_utc": snapshot.get("captured_at_utc"),
            "cash": snapshot.get("cash"),
            "buying_power": snapshot.get("buying_power"),
            "equity": snapshot.get("equity"),
            "regt_buying_power": snapshot.get("regt_buying_power"),
            "portfolio_value": snapshot.get("portfolio_value"),
            "positions_count": int(snapshot.get("positions_count", 0) or 0),
            "position_tickers": list(snapshot.get("position_tickers", []) or []),
            "position_quantity_by_ticker": dict(snapshot.get("position_quantity_by_ticker", {}) or {}),
            "positions": list(snapshot.get("positions", []) or []),
            "open_orders_count": int(snapshot.get("open_orders_count", 0) or 0),
            "open_order_tickers": list(snapshot.get("open_order_tickers", []) or []),
            "open_order_status_counts": dict(snapshot.get("open_order_status_counts", {}) or {}),
            "open_orders": list(snapshot.get("open_orders", []) or []),
        }

    account = snapshot.get("account") or {}
    positions_raw = snapshot.get("positions")
    if positions_raw is None:
        positions_raw = []
    if isinstance(positions_raw, pd.DataFrame):
        positions_frame = positions_raw.copy()
    else:
        positions_frame = pd.DataFrame(list(positions_raw))
    if not positions_frame.empty:
        if "ticker" not in positions_frame.columns and "symbol" in positions_frame.columns:
            positions_frame["ticker"] = positions_frame["symbol"]
        positions_frame["ticker"] = positions_frame["ticker"].astype(str).str.strip()
    position_tickers = sorted(
        {str(value).strip() for value in positions_frame.get("ticker", pd.Series(dtype="object")).tolist() if str(value).strip()}
    )
    positions_detail: list[dict[str, Any]] = []
    if not positions_frame.empty:
        for row in positions_frame.to_dict(orient="records"):
            positions_detail.append(
                {
                    "ticker": str(row.get("ticker", "")).strip(),
                    "quantity": float(_f(row.get("quantity"), 0.0) or 0.0),
                    "market_value": float(_f(row.get("market_value"), 0.0) or 0.0),
                    "avg_entry_price": float(_f(row.get("avg_entry_price"), 0.0) or 0.0),
                    "current_price": float(_f(row.get("current_price"), 0.0) or 0.0),
                    "unrealized_pnl": float(_f(row.get("unrealized_pnl"), 0.0) or 0.0),
                }
            )
        positions_detail.sort(
            key=lambda item: (
                -float(item.get("market_value", 0.0) or 0.0),
                str(item.get("ticker", "")),
            )
        )
    quantity_col = None
    for candidate in ("quantity", "expected_quantity", "shares"):
        if candidate in positions_frame.columns:
            quantity_col = candidate
            break
    quantity_map: dict[str, float] = {}
    if quantity_col is not None and not positions_frame.empty:
        for row in positions_frame.to_dict(orient="records"):
            ticker = str(row.get("ticker", "")).strip()
            if not ticker:
                continue
            quantity_map[ticker] = float(_f(row.get(quantity_col), 0.0) or 0.0)

    open_orders_raw = snapshot.get("open_orders")
    if open_orders_raw is None:
        open_orders_frame = pd.DataFrame()
    elif isinstance(open_orders_raw, pd.DataFrame):
        open_orders_frame = open_orders_raw.copy()
    else:
        open_orders_frame = pd.DataFrame(list(open_orders_raw))
    open_order_details: list[dict[str, Any]] = []
    open_order_status_counts: dict[str, int] = {}
    open_order_tickers: list[str] = []
    if not open_orders_frame.empty:
        if "ticker" not in open_orders_frame.columns and "symbol" in open_orders_frame.columns:
            open_orders_frame["ticker"] = open_orders_frame["symbol"]
        if "direction" not in open_orders_frame.columns and "side" in open_orders_frame.columns:
            open_orders_frame["direction"] = open_orders_frame["side"]
        open_orders_frame["ticker"] = open_orders_frame["ticker"].astype(str).str.strip()
        open_orders_frame["direction"] = open_orders_frame["direction"].astype(str).str.strip().str.lower()
        open_orders_frame["status"] = open_orders_frame.get("status", pd.Series(dtype="object")).astype(str).str.strip().str.lower()
        open_order_tickers = sorted(
            {str(value).strip().upper() for value in open_orders_frame["ticker"].tolist() if str(value).strip()}
        )
        open_order_status_counts = {
            str(key): int(value)
            for key, value in open_orders_frame["status"].value_counts(dropna=False).to_dict().items()
        }
        now_utc = pd.Timestamp.now(tz="UTC")
        for row in open_orders_frame.to_dict(orient="records"):
            submitted_at = row.get("submitted_at") or row.get("created_at") or row.get("updated_at")
            submitted_ts = pd.to_datetime(submitted_at, utc=True, errors="coerce")
            age_minutes = None
            if pd.notna(submitted_ts):
                age_minutes = float(max(0.0, (now_utc - submitted_ts).total_seconds() / 60.0))
            status = str(row.get("status", "")).strip().lower()
            direction = str(row.get("direction", "")).strip().lower()
            locks_buying_power = direction == "buy"
            locks_inventory = direction == "sell"
            is_stale = bool(age_minutes is not None and age_minutes >= 120.0 and status in {"open", "new", "accepted", "pending_new"})
            long_lived = bool(age_minutes is not None and age_minutes >= 60.0 and status in {"open", "new", "accepted", "pending_new"})
            needs_manual_confirmation = bool(age_minutes is not None and age_minutes >= 240.0)
            reason_bits = []
            if locks_buying_power:
                reason_bits.append("buy order can reserve buying power")
            if locks_inventory:
                reason_bits.append("sell order can reserve inventory")
            if is_stale:
                reason_bits.append("stale or long-lived")
            open_order_details.append(
                {
                    "order_id": str(row.get("order_id", "")).strip(),
                    "client_order_id": str(row.get("client_order_id", "")).strip(),
                    "ticker": str(row.get("ticker", "")).strip().upper(),
                    "direction": direction,
                    "order_type": str(row.get("order_type", "")).strip().lower(),
                    "time_in_force": str(row.get("time_in_force", "")).strip().lower(),
                    "status": status,
                    "quantity": float(_f(row.get("quantity"), 0.0) or 0.0),
                    "filled_qty": float(_f(row.get("filled_qty"), 0.0) or 0.0),
                    "limit_price": _f(row.get("limit_price"), None),
                    "stop_price": _f(row.get("stop_price"), None),
                    "extended_hours": bool(row.get("extended_hours", False)),
                    "submitted_at": row.get("submitted_at"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "filled_at": row.get("filled_at"),
                    "age_minutes": age_minutes,
                    "locks_buying_power": locks_buying_power,
                    "locks_inventory": locks_inventory,
                    "long_lived": long_lived,
                    "stale": is_stale,
                    "needs_manual_confirmation": needs_manual_confirmation,
                    "suggested_action": "consider_cancel" if is_stale or needs_manual_confirmation else "keep",
                    "impact_reason": "; ".join(reason_bits),
                }
            )
        open_order_details.sort(
            key=lambda item: (
                -float(item.get("age_minutes", 0.0) or 0.0),
                str(item.get("ticker", "")),
                str(item.get("order_id", "")),
            )
        )
    return {
        "available": True,
        "captured_at_utc": snapshot.get("captured_at_utc"),
        "cash": _f(account.get("cash"), None),
        "buying_power": _f(account.get("buying_power"), None),
        "equity": _f(account.get("equity"), None),
        "regt_buying_power": _f(account.get("regt_buying_power"), None),
        "portfolio_value": _f(account.get("portfolio_value"), None),
        "positions_count": int(len(positions_frame)),
        "position_tickers": position_tickers,
        "position_quantity_by_ticker": quantity_map,
        "positions": positions_detail,
        "open_orders_count": int(len(open_order_details)),
        "open_order_tickers": open_order_tickers,
        "open_order_status_counts": open_order_status_counts,
        "open_orders": open_order_details,
    }


def _broker_state_precheck(
    orders_df: pd.DataFrame,
    *,
    broker_state_snapshot: dict[str, Any] | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if orders_df.empty:
        return orders_df.copy(), {
            "broker_state_available": bool(broker_state_snapshot),
            "input_count": 0,
            "submitted_count": 0,
            "skipped_count": 0,
            "clipped_count": 0,
            "buying_power": None,
            "buy_budget_80pct": None,
            "buy_budget_remaining": None,
            "submitted_buy_count": 0,
            "submitted_sell_count": 0,
            "blocked_by_reason": {},
            "blocked_by_side": {},
            "notes": ["empty_orders"],
        }

    work = orders_df.copy()
    if "quantity" not in work.columns and "requested_qty" in work.columns:
        work["quantity"] = work["requested_qty"]
    if "quantity" not in work.columns:
        work["quantity"] = 0.0
    if "price_limit" not in work.columns:
        work["price_limit"] = pd.NA
    if "estimated_price" not in work.columns:
        work["estimated_price"] = pd.NA
    work["quantity"] = pd.to_numeric(work["quantity"], errors="coerce").fillna(0.0)
    work["price_limit"] = pd.to_numeric(work.get("price_limit"), errors="coerce")
    work["estimated_price"] = pd.to_numeric(work.get("estimated_price"), errors="coerce")
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()

    snapshot_summary = _broker_state_snapshot_summary(broker_state_snapshot)
    account = broker_state_snapshot.get("account") if broker_state_snapshot else {}
    positions_raw = broker_state_snapshot.get("positions") if broker_state_snapshot else []
    positions_frame = pd.DataFrame(list(positions_raw)) if not isinstance(positions_raw, pd.DataFrame) else positions_raw.copy()
    if not positions_frame.empty and "ticker" not in positions_frame.columns and "symbol" in positions_frame.columns:
        positions_frame["ticker"] = positions_frame["symbol"]
    if not positions_frame.empty:
        positions_frame["ticker"] = positions_frame["ticker"].astype(str).str.strip()
    position_map = snapshot_summary.get("position_quantity_by_ticker", {}) if snapshot_summary.get("available") else {}

    buying_power = _f(
        (account or {}).get("buying_power")
        or (account or {}).get("cash")
        or (account or {}).get("regt_buying_power"),
        0.0,
    )
    if buying_power is None:
        buying_power = 0.0
    buy_budget = max(0.0, float(buying_power) * 0.8) if snapshot_summary.get("available") else None
    buy_budget_remaining = float(buy_budget) if buy_budget is not None else None

    prepared_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    clipped_rows: list[dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()
    side_counter: Counter[str] = Counter()
    submitted_buy_count = 0
    submitted_sell_count = 0

    records = list(work.to_dict(orient="records"))

    for row in records:
        ticker = str(row["ticker"]).strip()
        direction = str(row["direction"]).strip().lower()
        requested_qty = max(0.0, _f(row["quantity"], 0.0) or 0.0)
        if requested_qty <= 0:
            continue
        if direction != "sell":
            continue
        available_qty = max(0.0, _f(position_map.get(ticker), 0.0) or 0.0)
        if snapshot_summary.get("available") and available_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "no_broker_position_for_sell",
                }
            )
            reason_counter["no_broker_position_for_sell"] += 1
            side_counter["sell"] += 1
            continue
        final_qty = requested_qty
        if snapshot_summary.get("available") and final_qty > available_qty:
            final_qty = available_qty
            clipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "submitted_qty": final_qty,
                    "reason": "clipped_to_available_position",
                }
            )
            reason_counter["clipped_to_available_position"] += 1
        if final_qty <= 0:
            continue
        adjusted_row = dict(row)
        adjusted_row["quantity"] = float(final_qty)
        prepared_rows.append(adjusted_row)
        submitted_sell_count += 1

    buy_candidates: list[dict[str, Any]] = []
    for row in records:
        ticker = str(row["ticker"]).strip()
        direction = str(row["direction"]).strip().lower()
        requested_qty = max(0.0, _f(row["quantity"], 0.0) or 0.0)
        if requested_qty <= 0:
            continue
        if direction not in {"buy", "sell"}:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "unsupported_direction",
                }
            )
            reason_counter["unsupported_direction"] += 1
            side_counter[direction or "unknown"] += 1
            continue
        if direction != "buy":
            continue
        price = _f(row.get("price_limit"), 0.0) or 0.0
        if price <= 0:
            price = _f(row.get("estimated_price"), 0.0) or 0.0
        if price <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "missing_or_invalid_price",
                }
            )
            reason_counter["missing_or_invalid_price"] += 1
            side_counter["buy"] += 1
            continue
        buy_candidates.append(
            {
                "row": row,
                "ticker": ticker,
                "requested_qty": requested_qty,
                "price": price,
                "requested_notional": requested_qty * price,
            }
        )

    total_buy_notional = sum(float(item["requested_notional"]) for item in buy_candidates)
    buy_scale = 1.0
    if snapshot_summary.get("available") and total_buy_notional > 0 and buy_budget_remaining is not None:
        buy_scale = min(1.0, buy_budget_remaining / total_buy_notional)

    for item in buy_candidates:
        row = dict(item["row"])
        ticker = str(item["ticker"])
        requested_qty = float(item["requested_qty"])
        price = float(item["price"])
        if snapshot_summary.get("available") and (buy_budget_remaining is not None) and buy_budget_remaining <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            reason_counter["buying_power_budget_exhausted"] += 1
            side_counter["buy"] += 1
            continue
        max_affordable_qty = int(buy_budget_remaining / price) if snapshot_summary.get("available") and buy_budget_remaining is not None and price > 0 else int(requested_qty)
        if snapshot_summary.get("available") and max_affordable_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            reason_counter["buying_power_budget_exhausted"] += 1
            side_counter["buy"] += 1
            continue
        scaled_target_qty = int(requested_qty * buy_scale) if snapshot_summary.get("available") and buy_budget_remaining is not None else int(requested_qty)
        if snapshot_summary.get("available") and scaled_target_qty <= 0 and max_affordable_qty >= 1:
            scaled_target_qty = 1
        final_qty = float(min(max_affordable_qty, scaled_target_qty))
        if snapshot_summary.get("available") and final_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            reason_counter["buying_power_budget_exhausted"] += 1
            side_counter["buy"] += 1
            continue
        if snapshot_summary.get("available") and final_qty < requested_qty:
            clipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "submitted_qty": final_qty,
                    "reason": "clipped_to_buying_power_budget",
                }
            )
            reason_counter["clipped_to_buying_power_budget"] += 1
        if snapshot_summary.get("available") and buy_budget_remaining is not None:
            buy_budget_remaining = max(0.0, buy_budget_remaining - (final_qty * price))
        row["quantity"] = final_qty
        prepared_rows.append(row)
        submitted_buy_count += 1

    prepared = pd.DataFrame(
        prepared_rows,
        columns=["ticker", "direction", "quantity", "price_limit", "estimated_price"],
    )
    precheck = {
        "broker_state_available": bool(snapshot_summary.get("available")),
        "input_count": int(len(work)),
        "submitted_count": int(len(prepared)),
        "skipped_count": int(len(skipped_rows)),
        "clipped_count": int(len(clipped_rows)),
        "buying_power": float(buying_power),
        "buy_budget_80pct": float(buy_budget) if buy_budget is not None else None,
        "buy_budget_remaining": float(buy_budget_remaining) if buy_budget_remaining is not None else None,
        "submitted_buy_count": int(submitted_buy_count),
        "submitted_sell_count": int(submitted_sell_count),
        "blocked_by_reason": {str(key): int(value) for key, value in reason_counter.items()},
        "blocked_by_side": {str(key): int(value) for key, value in side_counter.items()},
        "skipped_orders": skipped_rows,
        "clipped_orders": clipped_rows,
    }
    return prepared, precheck


def _parse_float_list(values: Any, *, default: list[float]) -> list[float]:
    if values is None:
        return list(default)
    if not isinstance(values, list):
        raise ValueError("selection.scale_factors must be a list")
    parsed: list[float] = []
    for value in values:
        try:
            parsed_value = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid scale factor: {value!r}") from exc
        if not math.isfinite(parsed_value) or parsed_value <= 0:
            raise ValueError(f"scale factors must be positive finite values: {value!r}")
        parsed.append(float(parsed_value))
    if not parsed:
        raise ValueError("selection.scale_factors must not be empty")
    return parsed


def _validate_target_mapping(item: dict[str, Any]) -> BucketTarget:
    name = str(item.get("name", "")).strip()
    side = _norm_side(item.get("side"))
    if not name:
        raise ValueError("each target requires a non-empty name")
    if side not in {"buy", "sell"}:
        raise ValueError(f"target {name!r} must declare side buy or sell")
    try:
        participation_min = float(item["participation_min"])
        participation_max = float(item["participation_max"])
        notional_min = float(item["notional_min"])
        notional_max = float(item["notional_max"])
    except KeyError as exc:
        raise ValueError(f"target {name!r} is missing required range field: {exc.args[0]}") from exc
    min_filled_orders = int(item.get("min_filled_orders", 3))
    if participation_min < 0 or participation_max <= participation_min:
        raise ValueError(f"target {name!r} must define an increasing participation range")
    if notional_min < 0 or notional_max <= notional_min:
        raise ValueError(f"target {name!r} must define an increasing notional range")
    if min_filled_orders <= 0:
        raise ValueError(f"target {name!r} must require at least one filled order")
    return BucketTarget(
        name=name,
        side=side,
        participation_min=participation_min,
        participation_max=participation_max,
        notional_min=notional_min,
        notional_max=notional_max,
        min_filled_orders=min_filled_orders,
    )


def _resolve_path(raw: Any, base_dir: Path) -> Path:
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else (base_dir / path).resolve()


def _bucket_label(value: float | None, buckets: list[tuple[str, float, float]]) -> str | None:
    if value is None or pd.isna(value):
        return None
    for name, minimum, maximum in buckets:
        if value >= minimum and (math.isinf(maximum) or value < maximum):
            return name
    return None


def _frame_signature(frame: pd.DataFrame) -> str:
    payload = json.dumps(frame.to_dict(orient="records"), sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_execution_context(sample_root: Path) -> pd.DataFrame:
    report_path = sample_root / "execution" / "execution_report.json"
    if not report_path.exists():
        return pd.DataFrame()
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(payload.get("per_order_results") or []):
        buckets = item.get("bucket_results") or []
        active = next((bucket for bucket in buckets if str(bucket.get("status", "")).strip().lower() != "inactive"), buckets[0] if buckets else {})
        rows.append(
            {
                "source_order_index": index,
                "ticker": str(item.get("ticker", "")).strip(),
                "direction": _norm_side(item.get("side") or item.get("direction")),
                "adv_shares": _f(active.get("bucket_available_volume"), None),
                "bucket_available_volume": _f(active.get("bucket_available_volume"), None),
                "bucket_participation_limit": _f(active.get("bucket_participation_limit"), None),
                "participation_limit_used": _f(item.get("participation_limit_used"), None),
            }
        )
    return pd.DataFrame(rows)


def _load_market_context(sample_root: Path) -> pd.DataFrame:
    candidates = []
    candidates.extend(sorted(sample_root.glob("**/*market*.csv")))
    candidates.extend(sorted(sample_root.glob("**/*market*.json")))
    rows: list[dict[str, Any]] = []
    for path in candidates:
        if path.suffix.lower() == ".csv":
            try:
                frame = pd.read_csv(path, encoding="utf-8-sig")
            except Exception:
                continue
            columns = {str(column).strip().lower(): column for column in frame.columns}
            ticker_col = next((columns[key] for key in ("ticker", "symbol") if key in columns), None)
            adv_col = next((columns[key] for key in ("adv_shares", "average_daily_volume", "avg_daily_volume", "daily_volume", "volume", "adv") if key in columns), None)
            if adv_col is None:
                continue
            if ticker_col is None and len(frame) != 1:
                continue
            for index, item in frame.iterrows():
                rows.append(
                    {
                        "source_order_index": index,
                        "ticker": str(item.get(ticker_col, "")).strip() if ticker_col is not None else "",
                        "adv_shares": _f(item.get(adv_col), None),
                    }
                )
        else:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items = payload.get("rows") or payload.get("data") or [payload]
            elif isinstance(payload, list):
                items = payload
            else:
                items = []
            for index, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                adv = next((_f(item.get(key), None) for key in ("adv_shares", "average_daily_volume", "avg_daily_volume", "daily_volume", "volume", "adv")), None)
                if adv is None:
                    continue
                rows.append({"source_order_index": index, "ticker": str(item.get("ticker") or item.get("symbol") or "").strip(), "adv_shares": adv})
    return pd.DataFrame(rows)


def _sample_root_from_source_path(source_path: Path) -> Path | None:
    parts = source_path.resolve().parts
    if len(parts) >= 3:
        return source_path.parents[1]
    return None


def _attach_context(
    orders: pd.DataFrame,
    *,
    source_run_root: Path | None,
    context_root: Path | None,
    source_path: Path,
) -> pd.DataFrame:
    work = orders.copy().reset_index(drop=True)
    if work.empty:
        return work
    work["source_order_index"] = range(len(work))
    work["source_path"] = str(source_path)
    work["source_root"] = str(source_run_root) if source_run_root is not None else ""
    work["requested_qty"] = pd.to_numeric(work["requested_qty"], errors="coerce")
    work["estimated_price"] = pd.to_numeric(work.get("estimated_price"), errors="coerce")
    work["price_limit"] = pd.to_numeric(work.get("price_limit"), errors="coerce")
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    work["ticker"] = work["ticker"].astype(str).str.strip()

    context = _load_execution_context(context_root) if context_root is not None else pd.DataFrame()
    if context.empty:
        context = _load_market_context(context_root) if context_root is not None else pd.DataFrame()
    if not context.empty:
        context = context.drop_duplicates(subset=["source_order_index"], keep="first").set_index("source_order_index")
        work = work.join(context[["adv_shares", "bucket_available_volume", "bucket_participation_limit", "participation_limit_used"]], on="source_order_index")
    else:
        work["adv_shares"] = pd.NA
        work["bucket_available_volume"] = pd.NA
        work["bucket_participation_limit"] = pd.NA
        work["participation_limit_used"] = pd.NA
    work["estimated_reference_price"] = work["estimated_price"].fillna(work["price_limit"])
    work["source_notional"] = work["requested_qty"] * work["estimated_reference_price"]
    work["estimated_participation"] = pd.to_numeric(work["requested_qty"], errors="coerce") / pd.to_numeric(work["adv_shares"], errors="coerce")
    return work


def load_bucket_plan(plan_path: Path) -> dict[str, Any]:
    resolved = Path(plan_path).resolve()
    base_dir = resolved.parent
    payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    targets_raw = list(payload.get("targets") or [])
    if not targets_raw:
        raise ValueError("bucket plan must define targets")
    targets: list[BucketTarget] = []
    for item in targets_raw:
        if not isinstance(item, dict):
            raise ValueError("each target must be a mapping")
        targets.append(_validate_target_mapping(item))
    selection = payload.get("selection") or {}
    source_run_roots = [_resolve_path(value, base_dir) for value in list(selection.get("source_run_roots") or []) if str(value).strip()]
    source_orders_oms = [_resolve_path(value, base_dir) for value in list(selection.get("source_orders_oms") or []) if str(value).strip()]
    scale_factors = _parse_float_list(selection.get("scale_factors"), default=DEFAULT_SCALE_FACTORS)
    return {
        "path": resolved,
        "targets": [target.to_dict() for target in targets],
        "selection": {
            "source_run_roots": source_run_roots,
            "source_orders_oms": source_orders_oms,
            "scale_factors": scale_factors,
            "prefer_mixed_side_baskets": _parse_bool(selection.get("prefer_mixed_side_baskets"), default=True),
            "max_tasks_per_target": int(selection.get("max_tasks_per_target", 8)),
        },
    }


def load_campaign_sources(
    *,
    source_run_roots: list[Path] | None = None,
    source_orders_oms: list[Path] | None = None,
) -> list[dict[str, Any]]:
    baskets: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()

    for root in list(source_run_roots or []):
        resolved_root = Path(root).resolve()
        if not resolved_root.exists():
            raise FileNotFoundError(f"source run root does not exist: {resolved_root}")
        for basket in fill_collection.load_orders_from_run_root(resolved_root):
            sample_root = _sample_root_from_source_path(Path(basket.source_path).resolve())
            orders = _attach_context(
                basket.orders,
                source_run_root=resolved_root,
                context_root=sample_root,
                source_path=Path(basket.source_path).resolve(),
            )
            if orders.empty:
                continue
            if "estimated_reference_price" not in orders.columns:
                if "estimated_price" in orders.columns or "price_limit" in orders.columns:
                    estimated_price = pd.to_numeric(orders.get("estimated_price"), errors="coerce") if "estimated_price" in orders.columns else pd.Series([pd.NA] * len(orders))
                    price_limit = pd.to_numeric(orders.get("price_limit"), errors="coerce") if "price_limit" in orders.columns else pd.Series([pd.NA] * len(orders))
                    orders["estimated_reference_price"] = estimated_price.fillna(price_limit)
                else:
                    orders["estimated_reference_price"] = pd.NA
            signature = _frame_signature(orders[["sample_id", "ticker", "direction", "requested_qty", "estimated_reference_price"]].copy())
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            baskets.append(
                {
                    "sample_id": basket.sample_id,
                    "source_path": Path(basket.source_path).resolve(),
                    "source_root": resolved_root,
                    "orders": orders,
                }
            )

    for path in list(source_orders_oms or []):
        resolved_path = Path(path).resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"source orders oms path does not exist: {resolved_path}")
        for basket in fill_collection.load_orders_from_orders_oms(resolved_path):
            orders = _attach_context(
                basket.orders,
                source_run_root=None,
                context_root=None,
                source_path=Path(basket.source_path).resolve(),
            )
            if orders.empty:
                continue
            if "estimated_reference_price" not in orders.columns:
                if "estimated_price" in orders.columns or "price_limit" in orders.columns:
                    estimated_price = pd.to_numeric(orders.get("estimated_price"), errors="coerce") if "estimated_price" in orders.columns else pd.Series([pd.NA] * len(orders))
                    price_limit = pd.to_numeric(orders.get("price_limit"), errors="coerce") if "price_limit" in orders.columns else pd.Series([pd.NA] * len(orders))
                    orders["estimated_reference_price"] = estimated_price.fillna(price_limit)
                else:
                    orders["estimated_reference_price"] = pd.NA
            signature = _frame_signature(orders[["sample_id", "ticker", "direction", "requested_qty", "estimated_reference_price"]].copy())
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            baskets.append(
                {
                    "sample_id": basket.sample_id,
                    "source_path": Path(basket.source_path).resolve(),
                    "source_root": None,
                    "orders": orders,
                }
            )

    return baskets


def _source_basket_diagnostics(source_baskets: list[dict[str, Any]]) -> dict[str, int]:
    basket_count = int(len(source_baskets))
    order_count = 0
    missing_adv_order_count = 0
    missing_reference_price_order_count = 0
    mixed_side_basket_count = 0
    for basket in source_baskets:
        orders = basket.get("orders")
        if not isinstance(orders, pd.DataFrame) or orders.empty:
            continue
        order_count += int(len(orders))
        missing_adv_order_count += int(pd.to_numeric(orders.get("adv_shares"), errors="coerce").isna().sum()) if "adv_shares" in orders.columns else 0
        missing_reference_price_order_count += (
            int(pd.to_numeric(orders.get("estimated_reference_price"), errors="coerce").isna().sum())
            if "estimated_reference_price" in orders.columns
            else 0
        )
        side_counts = orders["direction"].astype(str).str.lower().value_counts().to_dict() if "direction" in orders.columns else {}
        if len([key for key, value in side_counts.items() if int(value) > 0]) > 1:
            mixed_side_basket_count += 1
    return {
        "source_basket_count": basket_count,
        "source_order_count": int(order_count),
        "candidate_missing_adv_order_count": int(missing_adv_order_count),
        "candidate_missing_reference_price_order_count": int(missing_reference_price_order_count),
        "candidate_mixed_side_basket_count": int(mixed_side_basket_count),
    }


def _scale_orders(source_orders: pd.DataFrame, scale_factor: float) -> pd.DataFrame:
    if source_orders.empty:
        return source_orders.copy()
    work = source_orders.copy()
    work["requested_qty"] = pd.to_numeric(work["requested_qty"], errors="coerce").fillna(0.0)
    work["estimated_reference_price"] = pd.to_numeric(work["estimated_reference_price"], errors="coerce")
    work["adv_shares"] = pd.to_numeric(work["adv_shares"], errors="coerce")
    work["requested_qty"] = (work["requested_qty"] * float(scale_factor)).apply(lambda value: math.floor(float(value)))
    work = work.loc[work["requested_qty"] > 0].copy()
    if work.empty:
        return work
    work["requested_qty"] = work["requested_qty"].astype(float)
    work["requested_notional"] = work["requested_qty"] * work["estimated_reference_price"]
    work["estimated_participation"] = work["requested_qty"] / work["adv_shares"]
    return work.sort_values(
        by=["direction", "ticker", "requested_qty", "estimated_reference_price"],
        key=lambda series: series.map(lambda value: 0 if series.name == "direction" and _norm_side(value) == "sell" else 1 if series.name == "direction" else value),
        kind="mergesort",
        ignore_index=True,
    )


def _evaluate_candidate(candidate_orders: pd.DataFrame, targets: list[dict[str, Any]]) -> dict[str, Any]:
    match_details: list[dict[str, Any]] = []
    total_matches = 0
    side_counts = candidate_orders["direction"].astype(str).str.lower().value_counts().to_dict() if not candidate_orders.empty else {}
    for target in targets:
        side = str(target["side"]).lower()
        target_rows = candidate_orders.loc[candidate_orders["direction"].astype(str).str.lower() == side].copy()
        if target_rows.empty:
            match_count = 0
        else:
            if math.isinf(float(target["participation_max"])):
                participation_mask = target_rows["estimated_participation"] >= float(target["participation_min"])
            else:
                participation_mask = (target_rows["estimated_participation"] >= float(target["participation_min"])) & (
                    target_rows["estimated_participation"] < float(target["participation_max"])
                )
            notional_mask = (target_rows["requested_notional"] >= float(target["notional_min"])) & (
                target_rows["requested_notional"] < float(target["notional_max"])
            )
            target_rows = target_rows.loc[
                target_rows["estimated_participation"].notna()
                & target_rows["requested_notional"].notna()
                & participation_mask
                & notional_mask
            ].copy()
            match_count = int(len(target_rows))
        total_matches += match_count
        match_details.append(
            {
                "target_name": target["name"],
                "side": side,
                "match_count": match_count,
                "participation_min": float(target["participation_min"]),
                "participation_max": float(target["participation_max"]),
                "notional_min": float(target["notional_min"]),
                "notional_max": float(target["notional_max"]),
            }
        )
    primary = max(match_details, key=lambda item: item["match_count"], default=None)
    participation_values = candidate_orders.loc[candidate_orders["estimated_participation"].notna(), "estimated_participation"].astype(float).tolist()
    notional_values = candidate_orders.loc[candidate_orders["requested_notional"].notna(), "requested_notional"].astype(float).tolist()
    return {
        "matches": match_details,
        "total_match_count": int(total_matches),
        "primary_target": primary["target_name"] if primary and primary["match_count"] > 0 else None,
        "primary_score": int(primary["match_count"]) if primary and primary["match_count"] > 0 else 0,
        "mixed_side": len([key for key, value in side_counts.items() if int(value) > 0]) > 1,
        "side_counts": side_counts,
        "missing_adv_count": int(candidate_orders["adv_shares"].isna().sum()) if "adv_shares" in candidate_orders.columns else 0,
        "missing_reference_price_count": int(candidate_orders["estimated_reference_price"].isna().sum()) if "estimated_reference_price" in candidate_orders.columns else 0,
        "estimated_participation_min": float(min(participation_values)) if participation_values else None,
        "estimated_participation_max": float(max(participation_values)) if participation_values else None,
        "estimated_notional_min": float(min(notional_values)) if notional_values else None,
        "estimated_notional_max": float(max(notional_values)) if notional_values else None,
    }


def generate_campaign_tasks(
    *,
    plan: dict[str, Any],
    source_baskets: list[dict[str, Any]],
    side_scope: str = "buy-only",
    broker_state_snapshot: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    side_scope = _normalize_side_scope(side_scope)
    targets = list(plan["targets"])
    target_lookup = {str(target["name"]): dict(target) for target in targets}
    selection = plan["selection"]
    scale_factors = list(selection["scale_factors"])
    prefer_mixed_side_baskets = bool(selection["prefer_mixed_side_baskets"])
    max_tasks_per_target = int(selection["max_tasks_per_target"])

    rows: list[dict[str, Any]] = []
    task_inputs: dict[str, pd.DataFrame] = {}
    source_signatures: set[str] = set()
    task_index = 0
    broker_state_available = bool(_broker_state_snapshot_summary(broker_state_snapshot).get("available"))

    for basket in source_baskets:
        source_orders = basket["orders"]
        if source_orders.empty:
            continue
        scoped_source_orders = _apply_side_scope(source_orders, side_scope)
        if scoped_source_orders.empty:
            continue
        source_signature = _frame_signature(scoped_source_orders[["sample_id", "ticker", "direction", "requested_qty", "estimated_reference_price"]].copy())
        for scale_factor in scale_factors:
            task_frame = _scale_orders(scoped_source_orders, scale_factor)
            if task_frame.empty:
                continue
            task_signature = _frame_signature(task_frame[["sample_id", "ticker", "direction", "requested_qty", "requested_notional"]].copy())
            dedupe_key = hashlib.sha256(f"{source_signature}:{scale_factor}:{task_signature}".encode("utf-8")).hexdigest()
            if dedupe_key in source_signatures:
                continue
            source_signatures.add(dedupe_key)
            evaluation = _evaluate_candidate(task_frame, targets)
            if evaluation["total_match_count"] <= 0:
                continue
            broker_precheck_frame, broker_precheck = _broker_state_precheck(task_frame, broker_state_snapshot=broker_state_snapshot)
            task_summary = _task_notional_summary(task_frame)
            task_index += 1
            task_id = f"task_{task_index:04d}"
            rows.append(
                {
                    "task_id": task_id,
                    "task_hash": dedupe_key[:16],
                    "source_sample_id": basket["sample_id"],
                    "source_path": str(Path(basket["source_path"]).resolve()),
                    "source_root": str(basket["source_root"]) if basket["source_root"] is not None else "",
                    "scale_factor": float(scale_factor),
                    "target_name": evaluation["primary_target"] or "",
                    "target_side": next((match["side"] for match in evaluation["matches"] if match["target_name"] == evaluation["primary_target"]), ""),
                    "target_participation_min": next((match["participation_min"] for match in evaluation["matches"] if match["target_name"] == evaluation["primary_target"]), None),
                    "target_participation_max": next((match["participation_max"] for match in evaluation["matches"] if match["target_name"] == evaluation["primary_target"]), None),
                    "target_notional_min": next((match["notional_min"] for match in evaluation["matches"] if match["target_name"] == evaluation["primary_target"]), None),
                    "target_notional_max": next((match["notional_max"] for match in evaluation["matches"] if match["target_name"] == evaluation["primary_target"]), None),
                    "side_scope": side_scope,
                    "side_scope_allowed": True,
                    "selected": False,
                    "status": "planned",
                    "selection_reason": "",
                    "run_dir": "",
                    "input_orders_oms_path": "",
                    "matched_orders_estimate": int(evaluation["primary_score"]),
                    "matched_orders_total_estimate": int(evaluation["total_match_count"]),
                    "candidate_order_count": int(task_summary["order_count"]),
                    "estimated_total_notional": float(task_summary["total_notional"]),
                    "primary_ticker": str(task_summary["primary_ticker"]),
                    "candidate_tickers_json": json.dumps(task_summary["tickers"], ensure_ascii=False, sort_keys=True),
                    "mixed_side": bool(evaluation["mixed_side"]),
                    "missing_adv_count": int(evaluation["missing_adv_count"]),
                    "missing_reference_price_count": int(evaluation["missing_reference_price_count"]),
                    "estimated_participation_min": evaluation["estimated_participation_min"],
                    "estimated_participation_max": evaluation["estimated_participation_max"],
                    "estimated_notional_min": evaluation["estimated_notional_min"],
                    "estimated_notional_max": evaluation["estimated_notional_max"],
                    "broker_state_available": broker_state_available,
                    "broker_precheck_submittable_count": int(broker_precheck["submitted_count"]),
                    "broker_precheck_buy_submittable_count": int(broker_precheck["submitted_buy_count"]),
                    "broker_precheck_sell_submittable_count": int(broker_precheck["submitted_sell_count"]),
                    "broker_precheck_blocked_order_count": int(broker_precheck["skipped_count"]),
                    "broker_precheck_blocked_reason_counts_json": json.dumps(broker_precheck["blocked_by_reason"], ensure_ascii=False, sort_keys=True),
                    "broker_precheck_blocked_side_counts_json": json.dumps(broker_precheck["blocked_by_side"], ensure_ascii=False, sort_keys=True),
                    "broker_precheck_status": "ready" if int(broker_precheck["submitted_count"]) > 0 else ("blocked" if broker_state_available else "unknown"),
                    "match_details_json": json.dumps(evaluation["matches"], ensure_ascii=False, sort_keys=True),
                    "task_priority": float(evaluation["total_match_count"]) + (0.5 if prefer_mixed_side_baskets and evaluation["mixed_side"] else 0.0) + float(scale_factor) * 0.01 + (0.001 * int(broker_precheck["submitted_count"])),
                }
            )
            task_inputs[task_id] = task_frame.reset_index(drop=True)

    columns = [
        "task_id",
        "task_hash",
        "source_sample_id",
        "source_path",
        "source_root",
        "scale_factor",
        "target_name",
        "target_side",
        "target_participation_min",
        "target_participation_max",
        "target_notional_min",
        "target_notional_max",
        "side_scope",
        "side_scope_allowed",
        "selected",
        "status",
        "selection_reason",
        "run_dir",
        "input_orders_oms_path",
        "matched_orders_estimate",
        "matched_orders_total_estimate",
        "candidate_order_count",
        "estimated_total_notional",
        "primary_ticker",
        "candidate_tickers_json",
        "mixed_side",
        "missing_adv_count",
        "missing_reference_price_count",
        "estimated_participation_min",
        "estimated_participation_max",
        "estimated_notional_min",
        "estimated_notional_max",
        "broker_state_available",
        "broker_precheck_submittable_count",
        "broker_precheck_buy_submittable_count",
        "broker_precheck_sell_submittable_count",
        "broker_precheck_blocked_order_count",
        "broker_precheck_blocked_reason_counts_json",
        "broker_precheck_blocked_side_counts_json",
        "broker_precheck_status",
        "match_details_json",
        "task_priority",
    ]
    tasks = pd.DataFrame(rows, columns=columns)
    if tasks.empty:
        return tasks, task_inputs

    tasks = tasks.sort_values(
        by=["task_priority", "matched_orders_total_estimate", "matched_orders_estimate", "source_sample_id", "source_path", "task_id"],
        ascending=[False, False, False, True, True, True],
        kind="mergesort",
        ignore_index=True,
    )
    remaining_need = {str(target["name"]): int(target["min_filled_orders"]) for target in targets}
    selected_counts = {str(target["name"]): 0 for target in targets}
    selected_task_ids: set[str] = set()

    while True:
        best_index = None
        best_key: tuple[Any, ...] | None = None
        for index, row in tasks.iterrows():
            if str(row["task_id"]) in selected_task_ids:
                continue
            if not bool(row.get("side_scope_allowed", True)):
                continue
            if bool(row.get("broker_state_available", False)) and int(row.get("broker_precheck_submittable_count", 0) or 0) <= 0:
                continue
            match_details = json.loads(str(row["match_details_json"]))
            gain = 0
            primary_target = None
            primary_gain = -1
            for match in match_details:
                target_name = str(match["target_name"])
                match_count = min(int(match["match_count"]), remaining_need.get(target_name, 0))
                gain += match_count
                if match_count > primary_gain:
                    primary_target = target_name
                    primary_gain = match_count
            if gain <= 0 or primary_target is None or selected_counts.get(primary_target, 0) >= max_tasks_per_target:
                continue
            key = (gain, int(row["mixed_side"]), float(row["task_priority"]), -float(row["scale_factor"]), str(row["task_id"]))
            if best_key is None or key > best_key:
                best_index = index
                best_key = key
        if best_index is None:
            break
        row = tasks.loc[best_index]
        match_details = json.loads(str(row["match_details_json"]))
        primary_target = None
        primary_gain = -1
        for match in match_details:
            target_name = str(match["target_name"])
            match_count = min(int(match["match_count"]), remaining_need.get(target_name, 0))
            if match_count > primary_gain:
                primary_target = target_name
                primary_gain = match_count
        if primary_target is None:
            break
        selected_target = target_lookup.get(primary_target)
        tasks.loc[best_index, "selected"] = True
        tasks.loc[best_index, "status"] = "selected"
        tasks.loc[best_index, "selection_reason"] = f"selected_for_{primary_target}"
        tasks.loc[best_index, "target_name"] = primary_target
        if selected_target is not None:
            tasks.loc[best_index, "target_side"] = str(selected_target["side"])
            tasks.loc[best_index, "target_participation_min"] = float(selected_target["participation_min"])
            tasks.loc[best_index, "target_participation_max"] = float(selected_target["participation_max"])
        tasks.loc[best_index, "target_notional_min"] = float(selected_target["notional_min"])
        tasks.loc[best_index, "target_notional_max"] = float(selected_target["notional_max"])
        selected_task_ids.add(str(row["task_id"]))
        selected_counts[primary_target] = selected_counts.get(primary_target, 0) + 1
        for match in match_details:
            target_name = str(match["target_name"])
            remaining_need[target_name] = max(0, remaining_need.get(target_name, 0) - int(match["match_count"]))
        if all(value <= 0 for value in remaining_need.values()):
            break

    tasks.loc[~tasks["selected"].astype(bool), "status"] = "skipped"
    if broker_state_available:
        blocked_mask = ~tasks["selected"].astype(bool) & (pd.to_numeric(tasks["broker_precheck_submittable_count"], errors="coerce").fillna(0) <= 0)
        tasks.loc[blocked_mask & (tasks["selection_reason"] == ""), "selection_reason"] = "broker_state_blocked"
    tasks.loc[~tasks["selected"].astype(bool) & (tasks["selection_reason"] == ""), "selection_reason"] = "not_selected"
    tasks["selected"] = tasks["selected"].astype(bool)
    return tasks.reset_index(drop=True), task_inputs


def _classify_completed_orders(orders: pd.DataFrame, targets: list[dict[str, Any]]) -> pd.DataFrame:
    if orders.empty:
        return orders.copy()
    work = orders.copy()
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    work["filled_qty"] = pd.to_numeric(work.get("filled_qty"), errors="coerce").fillna(0.0)
    work["avg_fill_price"] = pd.to_numeric(work.get("avg_fill_price"), errors="coerce")
    work["estimated_price"] = pd.to_numeric(work.get("estimated_price"), errors="coerce")
    work["adv_shares"] = pd.to_numeric(work.get("adv_shares"), errors="coerce")
    if work["avg_fill_price"].isna().all() and "estimated_price" in work.columns:
        work["avg_fill_price"] = work["estimated_price"]
    work["filled_notional"] = work["filled_qty"] * work["avg_fill_price"]
    work["actual_participation"] = work["filled_qty"] / work["adv_shares"]
    work["side_sign"] = work["direction"].map({"buy": 1, "sell": -1}).fillna(0)
    work["realized_slippage_notional"] = work["side_sign"] * (work["avg_fill_price"] - work["estimated_price"]) * work["filled_qty"]
    work["positive_realized_slippage_notional"] = work["realized_slippage_notional"].clip(lower=0.0)
    work["participation_bucket"] = work["actual_participation"].apply(lambda value: _bucket_label(value, PARTICIPATION_BUCKETS))
    work["notional_bucket"] = work["filled_notional"].apply(lambda value: _bucket_label(value, NOTIONAL_BUCKETS))
    matched_target_name = pd.Series([None] * len(work), index=work.index, dtype="object")
    for target in targets:
        mask = work["direction"] == str(target["side"]).lower()
        if math.isinf(float(target["participation_max"])):
            participation_mask = work["actual_participation"] >= float(target["participation_min"])
        else:
            participation_mask = (work["actual_participation"] >= float(target["participation_min"])) & (
                work["actual_participation"] < float(target["participation_max"])
            )
        notional_mask = (work["filled_notional"] >= float(target["notional_min"])) & (
            work["filled_notional"] < float(target["notional_max"])
        )
        matched_target_name.loc[mask & participation_mask & notional_mask] = str(target["name"])
    work["matched_target_name"] = matched_target_name
    return work


def score_bucket_coverage(
    *,
    tasks: pd.DataFrame,
    plan: dict[str, Any],
    source_baskets: list[dict[str, Any]] | None = None,
    side_scope: str = "buy-only",
    campaign_preset: str = "coverage",
    seed_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    side_scope = _normalize_side_scope(side_scope)
    campaign_preset = _normalize_campaign_preset(campaign_preset)
    if campaign_preset == "reduce-positions":
        return _score_reduce_positions_campaign(tasks=tasks)
    targets = list(plan["targets"])
    status_series = tasks["status"].astype(str).str.lower() if "status" in tasks.columns else pd.Series(dtype="object")
    selected_series = tasks["selected"].astype(bool) if "selected" in tasks.columns else pd.Series(dtype=bool)
    completed_rows = tasks.loc[status_series == "completed"].copy() if not tasks.empty and "status" in tasks.columns else tasks.iloc[0:0].copy()
    failed_rows = tasks.loc[status_series == "failed"].copy() if not tasks.empty and "status" in tasks.columns else tasks.iloc[0:0].copy()
    failed_task_count = int(len(failed_rows))
    selected_task_count = int(selected_series.sum()) if not selected_series.empty else 0
    selected_buy_task_count = 0
    selected_sell_task_count = 0
    completed_buy_task_count = 0
    completed_sell_task_count = 0
    submitted_buy_order_count = 0
    submitted_sell_order_count = 0
    filled_buy_order_count = 0
    filled_sell_order_count = 0
    blocked_reason_counts: Counter[str] = Counter()
    blocked_side_counts: Counter[str] = Counter()
    excluded_not_relevant_task_count = 0
    task_records: list[dict[str, Any]] = []
    if not tasks.empty:
        for row in tasks.to_dict(orient="records"):
            record = dict(row)
            record["_selected"] = bool(record.get("selected", False))
            record["_status"] = str(record.get("status", "")).strip().lower()
            record["_selection_reason"] = str(record.get("selection_reason", "")).strip()
            record["_target_side"] = _norm_side(record.get("target_side") or record.get("side"))
            record["_target_name"] = str(record.get("target_name", "")).strip()
            record["_match_details"] = _parse_json_list(record.get("match_details_json"))
            record["_blocked_reason_counts"] = _parse_json_mapping(record.get("broker_precheck_blocked_reason_counts_json"))
            record["_blocked_side_counts"] = _parse_json_mapping(record.get("broker_precheck_blocked_side_counts_json"))
            task_records.append(record)
            if record["_selection_reason"] == "side_scope_excluded":
                excluded_not_relevant_task_count += 1
            if record["_selected"]:
                if record["_target_side"] == "buy":
                    selected_buy_task_count += 1
                elif record["_target_side"] == "sell":
                    selected_sell_task_count += 1
            if record["_status"] == "completed":
                if record["_target_side"] == "buy":
                    completed_buy_task_count += 1
                elif record["_target_side"] == "sell":
                    completed_sell_task_count += 1
            for reason, count in record["_blocked_reason_counts"].items():
                blocked_reason_counts[str(reason)] += _safe_int(count, 0)
            for side, count in record["_blocked_side_counts"].items():
                blocked_side_counts[str(side)] += _safe_int(count, 0)

    selected_target_counts = (
        tasks.loc[selected_series if not selected_series.empty else [], "target_name"].astype(str).str.strip().replace({"": pd.NA}).dropna().value_counts().to_dict()
        if not tasks.empty and "target_name" in tasks.columns
        else {}
    )
    matched_total_estimate = 0.0
    if not tasks.empty and "matched_orders_total_estimate" in tasks.columns:
        matched_total_estimate = float(pd.to_numeric(tasks["matched_orders_total_estimate"], errors="coerce").fillna(0.0).sum())
    source_diagnostics = _source_basket_diagnostics(source_baskets or [])

    output_frames: list[pd.DataFrame] = []
    missing_run_dirs: list[str] = []
    for row in completed_rows.to_dict(orient="records"):
        run_dir = Path(str(row.get("run_dir", ""))).resolve()
        orders_path = run_dir / "alpaca_fill_orders.csv"
        manifest_path = run_dir / "alpaca_fill_manifest.json"
        if manifest_path.exists():
            try:
                run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                run_manifest = {}
            side = _norm_side(row.get("target_side") or row.get("side"))
            submitted_count = int(run_manifest.get("submitted_count", 0) or 0)
            if side == "buy":
                submitted_buy_order_count += submitted_count
            elif side == "sell":
                submitted_sell_order_count += submitted_count
        if not orders_path.exists():
            missing_run_dirs.append(str(run_dir))
            continue
        output_frame = pd.read_csv(orders_path, encoding="utf-8-sig")
        input_path = Path(str(row.get("input_orders_oms_path", ""))).resolve()
        input_frame = pd.read_csv(input_path, encoding="utf-8-sig") if input_path.exists() else pd.DataFrame()
        if output_frame.empty or input_frame.empty:
            continue
        merged = output_frame.merge(
            input_frame,
            on=["sample_id", "ticker", "direction", "requested_qty", "estimated_price"],
            how="left",
            suffixes=("_actual", "_input"),
        )
        merged["task_id"] = str(row.get("task_id", ""))
        merged["run_dir"] = str(run_dir)
        output_frames.append(merged)

    combined = pd.concat(output_frames, ignore_index=True) if output_frames else pd.DataFrame()
    combined = _classify_completed_orders(combined, targets)
    filled_rows = combined.loc[pd.to_numeric(combined.get("filled_qty"), errors="coerce").fillna(0.0) > 0].copy() if not combined.empty else pd.DataFrame()
    if not filled_rows.empty:
        filled_buy_order_count = int((filled_rows["direction"].astype(str).str.lower() == "buy").sum())
        filled_sell_order_count = int((filled_rows["direction"].astype(str).str.lower() == "sell").sum())

    side_counts = filled_rows["direction"].astype(str).str.lower().value_counts().to_dict() if not filled_rows.empty else {}
    participation_values = filled_rows["actual_participation"].dropna().astype(float).tolist() if not filled_rows.empty else []
    notional_values = filled_rows["filled_notional"].dropna().astype(float).tolist() if not filled_rows.empty else []
    positive_rows = filled_rows.loc[filled_rows["positive_realized_slippage_notional"] > 0].copy() if not filled_rows.empty else pd.DataFrame()
    negative_rows = filled_rows.loc[filled_rows["realized_slippage_notional"] < 0].copy() if not filled_rows.empty else pd.DataFrame()
    missing_adv_count = int((filled_rows["adv_shares"].isna() | (filled_rows["adv_shares"] <= 0)).sum()) if not filled_rows.empty else 0

    bucket_coverage: dict[str, dict[str, Any]] = {}
    missing_buckets: list[dict[str, Any]] = []
    task_records_frame = pd.DataFrame(task_records) if task_records else tasks.iloc[0:0].copy()
    for target in targets:
        target_name = str(target["name"])
        target_side = str(target["side"]).lower()
        target_allowed = _side_scope_allows_direction(side_scope, target_side)
        candidate_rows = task_records_frame.iloc[0:0].copy()
        selected_candidate_rows = candidate_rows
        completed_candidate_rows = candidate_rows
        precheck_candidate_rows = candidate_rows
        if not task_records_frame.empty and "_match_details" in task_records_frame.columns:
            candidate_mask = task_records_frame["_match_details"].apply(
                lambda matches: any(
                    str(match.get("target_name", "")).strip() == target_name
                    and _safe_int(match.get("match_count"), 0) > 0
                    for match in list(matches or [])
                )
            )
            candidate_rows = task_records_frame.loc[candidate_mask].copy()
            selected_candidate_rows = candidate_rows.loc[candidate_rows["_selected"].astype(bool)].copy()
            completed_candidate_rows = candidate_rows.loc[candidate_rows["_status"] == "completed"].copy()
            precheck_candidate_rows = candidate_rows.loc[
                pd.to_numeric(candidate_rows.get("broker_precheck_submittable_count"), errors="coerce").fillna(0) > 0
            ].copy()
        if filled_rows.empty or "matched_target_name" not in filled_rows.columns:
            target_rows = pd.DataFrame()
        else:
            target_rows = filled_rows.loc[filled_rows["matched_target_name"].astype(str) == target_name].copy()
        filled_count = int(len(target_rows))
        candidate_task_count = int(len(candidate_rows))
        selected_task_count_for_target = int(len(selected_candidate_rows))
        completed_task_count_for_target = int(len(completed_candidate_rows))
        precheck_blocked_task_count = max(0, candidate_task_count - int(len(precheck_candidate_rows)))
        missing_reason = ""
        if filled_count < int(target["min_filled_orders"]):
            if not target_allowed:
                missing_reason = "side_scope_excluded"
            elif (source_diagnostics["candidate_missing_adv_order_count"] > 0 or missing_adv_count > 0) and candidate_task_count <= 0:
                missing_reason = "missing_adv"
            elif candidate_task_count <= 0:
                missing_reason = "no_candidate_tasks"
            elif len(precheck_candidate_rows) <= 0:
                missing_reason = "broker_state_blocked"
            elif selected_task_count_for_target <= 0:
                missing_reason = "no_selected_tasks"
            elif completed_task_count_for_target <= 0:
                missing_reason = "no_completed_tasks"
            else:
                missing_reason = "insufficient_filled_orders"
        bucket_coverage[str(target["name"])] = {
            "side": str(target["side"]),
            "min_filled_orders": int(target["min_filled_orders"]),
            "filled_order_count": filled_count,
            "candidate_task_count": candidate_task_count,
            "selected_task_count": selected_task_count_for_target,
            "completed_task_count": completed_task_count_for_target,
            "precheck_blocked_task_count": precheck_blocked_task_count,
            "side_scope_allowed": bool(target_allowed),
            "observed_participation_min": float(target_rows["actual_participation"].min()) if not target_rows.empty else None,
            "observed_participation_max": float(target_rows["actual_participation"].max()) if not target_rows.empty else None,
            "observed_notional_min": float(target_rows["filled_notional"].min()) if not target_rows.empty else None,
            "observed_notional_max": float(target_rows["filled_notional"].max()) if not target_rows.empty else None,
            "status": "complete" if filled_count >= int(target["min_filled_orders"]) else "missing",
            "missing_count": max(0, int(target["min_filled_orders"]) - filled_count),
            "missing_reason": missing_reason,
        }
        if filled_count < int(target["min_filled_orders"]):
            missing_buckets.append(
                {
                    "name": target_name,
                    "side": target_side,
                    "reason": missing_reason or "insufficient_filled_orders",
                    "missing_count": max(0, int(target["min_filled_orders"]) - filled_count),
                    "candidate_task_count": candidate_task_count,
                    "selected_task_count": selected_task_count_for_target,
                    "completed_task_count": completed_task_count_for_target,
                    "precheck_blocked_task_count": precheck_blocked_task_count,
                }
            )

    coverage_summary = {
        "selected_task_count": selected_task_count,
        "selected_buy_task_count": int(selected_buy_task_count),
        "selected_sell_task_count": int(selected_sell_task_count),
        "completed_task_count": int(len(completed_rows)),
        "completed_buy_task_count": int(completed_buy_task_count),
        "completed_sell_task_count": int(completed_sell_task_count),
        "failed_task_count": failed_task_count,
        "failed_task_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in Counter(
                str(value).strip()
                for value in failed_rows["selection_reason"].tolist()
                if str(value).strip()
            ).most_common()
        ],
        "filled_order_count": int(len(filled_rows)),
        "positive_signal_count": int(len(positive_rows)),
        "negative_signal_count": int(len(negative_rows)),
        "missing_adv_count": missing_adv_count,
        "selected_target_counts": {str(key): int(value) for key, value in selected_target_counts.items()},
        "observed_side_counts": {str(key): int(value) for key, value in side_counts.items()},
        "observed_participation_min": float(min(participation_values)) if participation_values else None,
        "observed_participation_max": float(max(participation_values)) if participation_values else None,
        "observed_notional_min": float(min(notional_values)) if notional_values else None,
        "observed_notional_max": float(max(notional_values)) if notional_values else None,
        "missing_output_run_dirs": missing_run_dirs,
        "submitted_buy_order_count": int(submitted_buy_order_count),
        "submitted_sell_order_count": int(submitted_sell_order_count),
        "filled_buy_order_count": int(filled_buy_order_count),
        "filled_sell_order_count": int(filled_sell_order_count),
        "blocked_by_reason": {str(key): int(value) for key, value in blocked_reason_counts.items()},
        "blocked_by_side": {str(key): int(value) for key, value in blocked_side_counts.items()},
        "excluded_not_relevant_task_count": int(excluded_not_relevant_task_count),
        **source_diagnostics,
    }
    recommendation = "ready_for_slippage_calibration" if not missing_buckets and selected_task_count > 0 else "continue_campaign"
    if campaign_preset == "seed-inventory":
        if coverage_summary["filled_buy_order_count"] > 0:
            recommendation = "seed_inventory_successful"
        elif coverage_summary["submitted_buy_order_count"] > 0:
            recommendation = "seed_inventory_ready"
        else:
            recommendation = "seed_inventory_limited_by_buying_power"
    if campaign_preset == "seed-inventory":
        scope_conclusion = recommendation
    elif side_scope == "buy-only":
        scope_conclusion = (
            "buy-only blocked by buying_power"
            if coverage_summary["blocked_by_reason"].get("buying_power_budget_exhausted", 0) > 0
            else "buy-only recommended"
        )
    elif side_scope == "sell-only":
        scope_conclusion = (
            "sell-only blocked by inventory"
            if coverage_summary["blocked_by_reason"].get("no_broker_position_for_sell", 0) > 0
            else "sell-only ready"
        )
    else:
        scope_conclusion = "both ready" if coverage_summary["submitted_buy_order_count"] > 0 and coverage_summary["submitted_sell_order_count"] > 0 else "both not ready"
    return {
        "side_scope": side_scope,
        "campaign_preset": campaign_preset,
        "seed_inventory_mode": campaign_preset == "seed-inventory",
        "seed_summary": dict(seed_summary or {}),
        "coverage_summary": coverage_summary,
        "bucket_coverage": bucket_coverage,
        "missing_buckets": missing_buckets,
        "recommendation": recommendation,
        "scope_conclusion": scope_conclusion,
    }


def build_campaign_manifest(
    *,
    campaign_run_id: str,
    created_at: str,
    market: str,
    broker: str,
    notes: str,
    campaign_preset: str,
    side_scope: str,
    broker_state_snapshot: dict[str, Any] | None,
    seed_summary: dict[str, Any] | None,
    bucket_plan_path: Path,
    plan: dict[str, Any],
    tasks: pd.DataFrame,
    score_payload: dict[str, Any],
    source_run_roots: list[Path],
    source_orders_oms: list[Path],
    max_runs: int,
) -> dict[str, Any]:
    side_scope = _normalize_side_scope(side_scope)
    campaign_preset = _normalize_campaign_preset(campaign_preset)
    broker_state_summary = _broker_state_snapshot_summary(broker_state_snapshot)
    seed_summary = dict(seed_summary or {})
    selected_rows = tasks.loc[tasks["selected"].astype(bool)].copy()
    used_run_roots = sorted(
        {
            str(Path(value).resolve())
            for value in selected_rows.get("source_root", pd.Series(dtype="object")).tolist()
            if pd.notna(value) and str(value).strip()
        }
    )
    if not used_run_roots:
        used_run_roots = [str(Path(path).resolve()) for path in source_run_roots]
    return {
        "campaign_run_id": campaign_run_id,
        "created_at": created_at,
        "market": market,
        "broker": broker,
        "notes": notes,
        "campaign_preset": campaign_preset,
        "side_scope": side_scope,
        "side_scope_conclusion": score_payload.get("scope_conclusion", ""),
        "seed_inventory_mode": bool(score_payload.get("seed_inventory_mode", False)),
        "broker_state_snapshot": broker_state_summary,
        "available_buying_power": seed_summary.get("available_buying_power", broker_state_summary.get("buying_power")),
        "seed_notional_budget": seed_summary.get("seed_notional_budget", 0.0),
        "seed_safety_factor": seed_summary.get("seed_safety_factor", 0.0),
        "seed_extended_hours_price_limit_multiplier": seed_summary.get("seed_extended_hours_price_limit_multiplier", 1.0),
        "seed_selected_task_count": seed_summary.get("seed_selected_task_count", 0),
        "seed_selected_order_count": seed_summary.get("seed_selected_order_count", 0),
        "seed_selected_notional": seed_summary.get("seed_selected_notional", 0.0),
        "seed_remaining_notional_budget": seed_summary.get("seed_remaining_notional_budget", 0.0),
        "seed_budget_limited_task_count": seed_summary.get("seed_budget_limited_task_count", 0),
        "seed_max_orders_limited_task_count": seed_summary.get("seed_max_orders_limited_task_count", 0),
        "seed_excluded_not_relevant_task_count": seed_summary.get("seed_excluded_not_relevant_task_count", score_payload["coverage_summary"].get("excluded_not_relevant_task_count", 0)),
        "seed_preset_status": seed_summary.get("seed_preset_status", score_payload.get("scope_conclusion", "")),
        "reduction_mode": campaign_preset == "reduce-positions",
        "reduction_target_notional": score_payload["coverage_summary"].get("target_notional", 0.0),
        "reduction_remaining_notional_budget": score_payload["coverage_summary"].get("remaining_notional_budget", 0.0),
        "reduction_selected_tickers": score_payload["coverage_summary"].get("selected_tickers", []),
        "reduction_selected_quantities": score_payload["coverage_summary"].get("selected_quantities", []),
        "reduction_selected_notionals": score_payload["coverage_summary"].get("selected_notionals", []),
        "reduction_preset_status": score_payload.get("recommendation", ""),
        "bucket_plan_path": str(Path(bucket_plan_path).resolve()),
        "source_run_roots": [str(Path(path).resolve()) for path in source_run_roots],
        "source_orders_oms": [str(Path(path).resolve()) for path in source_orders_oms],
        "max_runs": int(max_runs),
        "completed_tasks": int(score_payload["coverage_summary"]["completed_task_count"]),
        "failed_tasks": int(score_payload["coverage_summary"]["failed_task_count"]),
        "selected_buy_task_count": int(score_payload["coverage_summary"]["selected_buy_task_count"]),
        "selected_sell_task_count": int(score_payload["coverage_summary"]["selected_sell_task_count"]),
        "submitted_buy_order_count": int(score_payload["coverage_summary"]["submitted_buy_order_count"]),
        "submitted_sell_order_count": int(score_payload["coverage_summary"]["submitted_sell_order_count"]),
        "filled_buy_order_count": int(score_payload["coverage_summary"]["filled_buy_order_count"]),
        "filled_sell_order_count": int(score_payload["coverage_summary"]["filled_sell_order_count"]),
        "blocked_by_reason": score_payload["coverage_summary"]["blocked_by_reason"],
        "blocked_by_side": score_payload["coverage_summary"]["blocked_by_side"],
        "coverage_summary": score_payload["coverage_summary"],
        "bucket_coverage": score_payload["bucket_coverage"],
        "missing_buckets": score_payload["missing_buckets"],
        "recommendation": score_payload["recommendation"],
        "run_roots_used": used_run_roots,
        "source_run_roots_considered": [str(Path(path).resolve()) for path in source_run_roots],
        "source_orders_oms_considered": [str(Path(path).resolve()) for path in source_orders_oms],
        "task_hashes": [str(value) for value in selected_rows["task_hash"].tolist()],
        "plan_targets": plan["targets"],
    }


def render_campaign_report(*, manifest: dict[str, Any], tasks: pd.DataFrame) -> str:
    broker_state = manifest.get("broker_state_snapshot") or {}
    coverage = manifest.get("coverage_summary") or {}
    campaign_preset = str(manifest.get("campaign_preset", "coverage")).strip().lower()
    seed_mode = bool(manifest.get("seed_inventory_mode", False))
    lines = [
        "# Alpaca Fill Campaign",
        "",
        "## Summary",
        "",
        f"- campaign_run_id: {manifest['campaign_run_id']}",
        f"- market: {manifest['market']}",
        f"- broker: {manifest['broker']}",
        f"- campaign_preset: {manifest.get('campaign_preset', '')}",
        f"- side_scope: {manifest.get('side_scope', '')}",
        f"- side_scope_conclusion: {manifest.get('side_scope_conclusion', '')}",
        f"- seed_inventory_mode: {str(seed_mode).lower()}",
        f"- reduction_mode: {str(bool(manifest.get('reduction_mode', False))).lower()}",
        f"- max_runs: {manifest['max_runs']}",
        f"- completed_tasks: {manifest['completed_tasks']}",
        f"- failed_tasks: {manifest['failed_tasks']}",
        f"- selected_buy_task_count: {manifest.get('selected_buy_task_count', 0)}",
        f"- selected_sell_task_count: {manifest.get('selected_sell_task_count', 0)}",
        f"- submitted_buy_order_count: {manifest.get('submitted_buy_order_count', 0)}",
        f"- submitted_sell_order_count: {manifest.get('submitted_sell_order_count', 0)}",
        f"- filled_buy_order_count: {manifest.get('filled_buy_order_count', 0)}",
        f"- filled_sell_order_count: {manifest.get('filled_sell_order_count', 0)}",
        f"- available_buying_power: {manifest.get('available_buying_power', None)}",
        f"- seed_notional_budget: {manifest.get('seed_notional_budget', None)}",
        f"- seed_extended_hours_price_limit_multiplier: {manifest.get('seed_extended_hours_price_limit_multiplier', None)}",
        f"- seed_selected_task_count: {manifest.get('seed_selected_task_count', 0)}",
        f"- seed_selected_order_count: {manifest.get('seed_selected_order_count', 0)}",
        f"- seed_selected_notional: {manifest.get('seed_selected_notional', 0.0)}",
        f"- seed_excluded_not_relevant_task_count: {manifest.get('seed_excluded_not_relevant_task_count', 0)}",
        f"- reduction_target_notional: {manifest.get('reduction_target_notional', 0.0)}",
        f"- reduction_remaining_notional_budget: {manifest.get('reduction_remaining_notional_budget', 0.0)}",
        f"- reduction_selected_tickers: {manifest.get('reduction_selected_tickers', [])}",
        f"- reduction_selected_quantities: {manifest.get('reduction_selected_quantities', [])}",
        f"- reduction_selected_notionals: {manifest.get('reduction_selected_notionals', [])}",
        f"- blocked_by_reason: {manifest.get('blocked_by_reason', {})}",
        f"- blocked_by_side: {manifest.get('blocked_by_side', {})}",
        f"- recommendation: {manifest['recommendation']}",
        "",
        "## Broker State",
        "",
        f"- available: {broker_state.get('available', False)}",
        f"- cash: {broker_state.get('cash', None)}",
        f"- buying_power: {broker_state.get('buying_power', None)}",
        f"- regt_buying_power: {broker_state.get('regt_buying_power', None)}",
        f"- portfolio_value: {broker_state.get('portfolio_value', None)}",
        f"- positions_count: {broker_state.get('positions_count', 0)}",
        f"- position_tickers: {broker_state.get('position_tickers', [])}",
        f"- position_quantity_by_ticker: {broker_state.get('position_quantity_by_ticker', {})}",
        "",
        "## Reduction Coverage"
        if bool(manifest.get("reduction_mode", False))
        else ("## Seed Inventory Coverage" if seed_mode else "## Bucket Coverage"),
        "",
        "| target | side | candidate_tasks | selected_tasks | completed_tasks | filled_orders | min_required | missing_reason | status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for target_name, bucket in manifest["bucket_coverage"].items():
        lines.append(
            f"| {target_name} | {bucket['side']} | {bucket.get('candidate_task_count', 0)} | {bucket.get('selected_task_count', 0)} | {bucket.get('completed_task_count', 0)} | {bucket['filled_order_count']} | {bucket['min_filled_orders']} | {bucket.get('missing_reason', '')} | {bucket['status']} |"
        )
    summary = manifest["coverage_summary"]
    lines.extend(
        [
            "",
            "## Coverage Summary",
            "",
            f"- filled_order_count: {summary['filled_order_count']}",
            f"- filled_buy_order_count: {summary.get('filled_buy_order_count', 0)}",
            f"- filled_sell_order_count: {summary.get('filled_sell_order_count', 0)}",
            f"- positive_signal_count: {summary['positive_signal_count']}",
            f"- negative_signal_count: {summary['negative_signal_count']}",
            f"- missing_adv_count: {summary['missing_adv_count']}",
            f"- selected_task_count: {summary['selected_task_count']}",
            f"- selected_buy_task_count: {summary['selected_buy_task_count']}",
            f"- selected_sell_task_count: {summary['selected_sell_task_count']}",
            f"- completed_task_count: {summary['completed_task_count']}",
            f"- completed_buy_task_count: {summary['completed_buy_task_count']}",
            f"- completed_sell_task_count: {summary['completed_sell_task_count']}",
            f"- submitted_buy_order_count: {summary['submitted_buy_order_count']}",
            f"- submitted_sell_order_count: {summary['submitted_sell_order_count']}",
            f"- excluded_not_relevant_task_count: {summary.get('excluded_not_relevant_task_count', 0)}",
            f"- excluded because not sellable/not relevant: {manifest.get('seed_excluded_not_relevant_task_count', summary.get('excluded_not_relevant_task_count', 0))}",
            f"- blocked_by_reason: {summary['blocked_by_reason']}",
            f"- blocked_by_side: {summary['blocked_by_side']}",
            f"- selected_target_counts: {summary['selected_target_counts']}",
            f"- observed_side_counts: {summary['observed_side_counts']}",
            f"- observed_participation_span: {summary['observed_participation_min']} to {summary['observed_participation_max']}",
            f"- observed_notional_span: {summary['observed_notional_min']} to {summary['observed_notional_max']}",
            f"- source_basket_count: {summary['source_basket_count']}",
            f"- source_order_count: {summary['source_order_count']}",
            f"- candidate_missing_adv_order_count: {summary['candidate_missing_adv_order_count']}",
            f"- candidate_missing_reference_price_order_count: {summary['candidate_missing_reference_price_order_count']}",
            "",
            "## Failure Reasons",
            "",
        ]
    )
    failure_reasons = summary.get("failed_task_reasons") or []
    if failure_reasons:
        for item in failure_reasons:
            lines.append(f"- {item['reason']} (count={item['count']})")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Task Status",
            "",
            "| task_id | target | scale | selected | status | run_dir |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in tasks.to_dict(orient="records"):
        lines.append(
            f"| {row.get('task_id', '')} | {row.get('target_name', '')} | {float(row.get('scale_factor', 0.0) or 0.0):.2f} | {str(bool(row.get('selected', False))).lower()} | {row.get('status', '')} | {row.get('run_dir', '')} |"
        )
    lines.extend(["", "## Missing Buckets", ""])
    if manifest["missing_buckets"]:
        for missing in manifest["missing_buckets"]:
            lines.append(
                f"- {missing['name']} [{missing.get('side', '')}]: {missing['reason']} "
                f"(missing_count={missing['missing_count']}, candidate_tasks={missing.get('candidate_task_count', 0)}, "
                f"selected_tasks={missing.get('selected_task_count', 0)}, completed_tasks={missing.get('completed_task_count', 0)}, "
                f"precheck_blocked_tasks={missing.get('precheck_blocked_task_count', 0)})"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Next Step", ""])
    if campaign_preset == "seed-inventory":
        if manifest["recommendation"] == "seed_inventory_successful":
            lines.append("- Seed inventory is in place; use this run as the starting point for sell-only collection.")
        elif manifest["recommendation"] == "seed_inventory_ready":
            lines.append("- Seed inventory run submitted buy orders but still needs fills before sell-only collection.")
        else:
            lines.append("- Seed inventory is limited by buying power; increase available cash or reduce the seed budget before retrying.")
    elif bool(manifest.get("reduction_mode", False)):
        if manifest["recommendation"] == "reduction_successful":
            lines.append("- Reduction run submitted sell orders and should be followed by a fresh broker-state inspection.")
        elif manifest["recommendation"] == "reduction_ready":
            lines.append("- Reduction orders were submitted but not all were filled yet; re-inspect broker state before switching back to buy-side.")
        else:
            lines.append("- Reduction is still limited by broker state; either the inventory is gone or the broker rejected the orders.")
    else:
        lines.append(
            "- Coverage is wide enough to rerun slippage calibration."
            if manifest["recommendation"] == "ready_for_slippage_calibration"
            else "- Continue the campaign to fill the missing buckets before recalibration."
        )
    return "\n".join(lines) + "\n"


def _build_task_input_orders(
    task_row: pd.Series,
    task_inputs: dict[str, pd.DataFrame],
    *,
    side_scope: str,
) -> pd.DataFrame:
    task_id = str(task_row.get("task_id", ""))
    frame = task_inputs.get(task_id, pd.DataFrame()).copy()
    if frame.empty:
        return frame
    frame["sample_id"] = frame["sample_id"].astype(str).str.strip()
    frame["ticker"] = frame["ticker"].astype(str).str.strip()
    frame["direction"] = frame["direction"].astype(str).str.strip().str.lower()
    frame = _apply_side_scope(frame, side_scope)
    if frame.empty:
        return frame
    frame["quantity"] = frame["requested_qty"]
    frame["campaign_task_id"] = task_id
    frame["campaign_task_hash"] = str(task_row.get("task_hash", ""))
    frame["campaign_target_name"] = str(task_row.get("target_name", ""))
    frame["campaign_scale_factor"] = float(task_row.get("scale_factor", 1.0))
    frame["campaign_original_source_path"] = str(task_row.get("source_path", ""))
    frame["campaign_bucket_side"] = str(task_row.get("target_side", ""))
    frame["campaign_bucket_participation_min"] = task_row.get("target_participation_min")
    frame["campaign_bucket_participation_max"] = task_row.get("target_participation_max")
    frame["campaign_bucket_notional_min"] = task_row.get("target_notional_min")
    frame["campaign_bucket_notional_max"] = task_row.get("target_notional_max")
    frame["campaign_estimated_participation"] = frame["estimated_participation"]
    frame["campaign_estimated_notional"] = frame["requested_notional"]
    if "estimated_reference_price" not in frame.columns:
        frame["estimated_reference_price"] = pd.to_numeric(frame.get("estimated_price"), errors="coerce")
        if "price_limit" in frame.columns:
            frame["estimated_reference_price"] = frame["estimated_reference_price"].fillna(pd.to_numeric(frame.get("price_limit"), errors="coerce"))
    return frame.sort_values(by=["direction", "ticker", "requested_qty", "estimated_reference_price"], kind="mergesort", ignore_index=True)


def _score_reduce_positions_campaign(*, tasks: pd.DataFrame) -> dict[str, Any]:
    status_series = tasks["status"].astype(str).str.lower() if "status" in tasks.columns else pd.Series(dtype="object")
    selected_series = tasks["selected"].astype(bool) if "selected" in tasks.columns else pd.Series(dtype=bool)
    selected_rows = tasks.loc[selected_series].copy() if not tasks.empty else tasks.iloc[0:0].copy()
    completed_rows = tasks.loc[status_series == "completed"].copy() if not tasks.empty else tasks.iloc[0:0].copy()
    failed_rows = tasks.loc[status_series == "failed"].copy() if not tasks.empty else tasks.iloc[0:0].copy()
    skipped_rows = tasks.loc[status_series == "skipped"].copy() if not tasks.empty else tasks.iloc[0:0].copy()

    blocked_reason_counts: Counter[str] = Counter()
    blocked_side_counts: Counter[str] = Counter()
    selected_tickers: list[str] = []
    selected_quantities: list[float] = []
    selected_notionals: list[float] = []
    excluded_not_relevant_task_count = 0

    for row in tasks.to_dict(orient="records"):
        reason = str(row.get("selection_reason", "")).strip()
        side = str(row.get("target_side", "sell")).strip().lower() or "sell"
        if reason == "selected_for_reduce_positions":
            selected_tickers.append(str(row.get("primary_ticker", "")).strip())
            selected_quantities.append(float(_f(row.get("reduction_requested_qty"), 0.0) or 0.0))
            selected_notionals.append(float(_f(row.get("reduction_requested_notional"), 0.0) or 0.0))
        elif reason:
            blocked_reason_counts[reason] += 1
            blocked_side_counts[side] += 1
        if reason == "target_release_notional_reached":
            excluded_not_relevant_task_count += 1

    output_frames: list[pd.DataFrame] = []
    missing_run_dirs: list[str] = []
    submitted_sell_order_count = 0
    completed_sell_task_count = 0
    filled_sell_order_count = 0
    filled_rows = pd.DataFrame()

    for row in completed_rows.to_dict(orient="records"):
        run_dir = Path(str(row.get("run_dir", ""))).resolve()
        orders_path = run_dir / "alpaca_fill_orders.csv"
        manifest_path = run_dir / "alpaca_fill_manifest.json"
        if manifest_path.exists():
            try:
                run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                run_manifest = {}
            submitted_sell_order_count += int(run_manifest.get("submitted_count", 0) or 0)
        if not orders_path.exists():
            missing_run_dirs.append(str(run_dir))
            continue
        output_frame = pd.read_csv(orders_path, encoding="utf-8-sig")
        input_path = Path(str(row.get("input_orders_oms_path", ""))).resolve()
        input_frame = pd.read_csv(input_path, encoding="utf-8-sig") if input_path.exists() else pd.DataFrame()
        if output_frame.empty or input_frame.empty:
            continue
        merged = output_frame.merge(
            input_frame,
            on=["sample_id", "ticker", "direction", "requested_qty", "estimated_price"],
            how="left",
            suffixes=("_actual", "_input"),
        )
        merged["task_id"] = str(row.get("task_id", ""))
        merged["run_dir"] = str(run_dir)
        output_frames.append(merged)

    combined = pd.concat(output_frames, ignore_index=True) if output_frames else pd.DataFrame()
    if not combined.empty:
        if "filled_qty" not in combined.columns:
            combined["filled_qty"] = pd.NA
        filled_rows = combined.loc[pd.to_numeric(combined.get("filled_qty"), errors="coerce").fillna(0.0) > 0].copy()
        if not filled_rows.empty:
            filled_sell_order_count = int((filled_rows["direction"].astype(str).str.lower() == "sell").sum())
            completed_sell_task_count = int((combined["direction"].astype(str).str.lower() == "sell").sum())

    selected_task_count = int(len(selected_rows))
    selected_order_count = int(selected_task_count)
    selected_notional = float(sum(selected_notionals))
    target_notional = float(pd.to_numeric(tasks.get("reduction_target_notional"), errors="coerce").fillna(0.0).max()) if not tasks.empty and "reduction_target_notional" in tasks.columns else 0.0
    remaining_notional_budget = max(0.0, target_notional - selected_notional)
    coverage_summary = {
        "selected_task_count": selected_task_count,
        "selected_buy_task_count": 0,
        "selected_sell_task_count": selected_task_count,
        "completed_task_count": int(len(completed_rows)),
        "completed_buy_task_count": 0,
        "completed_sell_task_count": int(len(completed_rows)),
        "failed_task_count": int(len(failed_rows)),
        "failed_task_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in Counter(
                str(value).strip()
                for value in failed_rows["selection_reason"].tolist()
                if str(value).strip()
            ).most_common()
        ],
        "filled_order_count": int(len(filled_rows)),
        "positive_signal_count": int(len(filled_rows)),
        "negative_signal_count": int(max(0, len(completed_rows) - len(filled_rows))),
        "missing_adv_count": 0,
        "selected_target_counts": {"reduce_positions": int(selected_task_count)},
        "observed_side_counts": {"sell": int(len(filled_rows)) if not filled_rows.empty else int(selected_task_count)},
        "observed_participation_min": None,
        "observed_participation_max": None,
        "observed_notional_min": float(filled_rows["filled_notional"].min()) if not filled_rows.empty else (float(min(selected_notionals)) if selected_notionals else None),
        "observed_notional_max": float(filled_rows["filled_notional"].max()) if not filled_rows.empty else (float(max(selected_notionals)) if selected_notionals else None),
        "missing_output_run_dirs": missing_run_dirs,
        "submitted_buy_order_count": 0,
        "submitted_sell_order_count": int(submitted_sell_order_count),
        "filled_buy_order_count": 0,
        "filled_sell_order_count": int(filled_sell_order_count),
        "blocked_by_reason": {str(key): int(value) for key, value in blocked_reason_counts.items()},
        "blocked_by_side": {str(key): int(value) for key, value in blocked_side_counts.items()},
        "excluded_not_relevant_task_count": int(excluded_not_relevant_task_count),
        "source_basket_count": 0,
        "source_order_count": 0,
        "candidate_missing_adv_order_count": 0,
        "candidate_missing_reference_price_order_count": 0,
        "candidate_mixed_side_basket_count": 0,
        "selected_tickers": selected_tickers,
        "selected_quantities": selected_quantities,
        "selected_notionals": selected_notionals,
        "target_notional": float(target_notional),
        "remaining_notional_budget": float(remaining_notional_budget),
        "selected_order_count": int(selected_order_count),
    }
    recommendation = "reduction_successful" if int(filled_sell_order_count) > 0 else ("reduction_ready" if int(submitted_sell_order_count) > 0 else "reduction_limited_by_broker_state")
    scope_conclusion = "sell-only ready" if int(submitted_sell_order_count) > 0 else "sell-only blocked by inventory"
    bucket_coverage = {
        ticker: {
            "side": "sell",
            "candidate_task_count": 1,
            "selected_task_count": 1,
            "completed_task_count": 1 if ticker in selected_tickers else 0,
            "filled_order_count": 1 if ticker in filled_rows.get("ticker", pd.Series(dtype="object")).astype(str).tolist() else 0,
            "min_filled_orders": 0,
            "missing_reason": "",
            "status": "filled" if ticker in filled_rows.get("ticker", pd.Series(dtype="object")).astype(str).tolist() else ("submitted" if ticker in selected_tickers else "blocked"),
        }
        for ticker in selected_tickers
    }
    return {
        "side_scope": "sell-only",
        "campaign_preset": "reduce-positions",
        "seed_inventory_mode": False,
        "coverage_summary": coverage_summary,
        "bucket_coverage": bucket_coverage,
        "missing_buckets": [],
        "recommendation": recommendation,
        "scope_conclusion": scope_conclusion,
    }


def run_fill_collection_campaign(
    *,
    bucket_plan_file: Path,
    output_dir: Path,
    run_root: Path | None,
    source_orders_oms: list[Path] | None,
    timeout_seconds: float,
    poll_interval_seconds: float,
    max_runs: int,
    force_outside_market_hours: bool,
    notes: str,
    campaign_preset: str = "coverage",
    side_scope: str = "buy-only",
    broker_state_snapshot: dict[str, Any] | None = None,
    max_seed_notional: float = 1000.0,
    max_seed_orders: int = 3,
    market: str = "us",
    broker: str = "alpaca",
    collection_runner: Any,
) -> Path:
    plan = load_bucket_plan(bucket_plan_file)
    campaign_preset = _normalize_campaign_preset(campaign_preset)
    side_scope = _normalize_side_scope(side_scope)
    if campaign_preset == "seed-inventory":
        side_scope = "buy-only"
    if campaign_preset == "reduce-positions":
        side_scope = "sell-only"
    if str(market).strip().lower() != "us":
        raise ValueError("collect-fills-campaign only supports market=us.")
    if str(broker).strip().lower() != "alpaca":
        raise ValueError("collect-fills-campaign only supports broker=alpaca.")
    generation_side_scope = side_scope
    if campaign_preset == "seed-inventory":
        generation_side_scope = "both"
    elif campaign_preset == "reduce-positions":
        generation_side_scope = "sell-only"
    source_run_roots = list(plan["selection"]["source_run_roots"])
    if run_root is not None:
        resolved_run_root = Path(run_root).resolve()
        if resolved_run_root not in source_run_roots:
            source_run_roots.append(resolved_run_root)
    source_orders = list(plan["selection"]["source_orders_oms"])
    for value in list(source_orders_oms or []):
        resolved = Path(value).resolve()
        if resolved not in source_orders:
            source_orders.append(resolved)

    if not force_outside_market_hours and not fill_collection.is_us_market_open():
        raise RuntimeError(
            "US market is closed; rerun with --force-outside-market-hours only if you intend to collect outside session hours."
        )

    broker_state_summary = _broker_state_snapshot_summary(broker_state_snapshot)
    reduction_summary: dict[str, Any] = {}
    if campaign_preset == "reduce-positions":
        source_baskets = []
        tasks, task_inputs, reduction_summary = _build_reduce_positions_selection(broker_state_snapshot=broker_state_snapshot)
    else:
        source_baskets = load_campaign_sources(source_run_roots=source_run_roots, source_orders_oms=source_orders)
        tasks, task_inputs = generate_campaign_tasks(
            plan=plan,
            source_baskets=source_baskets,
            side_scope=generation_side_scope,
            broker_state_snapshot=broker_state_snapshot,
        )
    seed_summary: dict[str, Any] = {}
    if campaign_preset == "seed-inventory":
        market_is_open = fill_collection.is_us_market_open()
        tasks, task_inputs, seed_summary = _apply_seed_inventory_selection(
            tasks=tasks,
            task_inputs=task_inputs,
            broker_state_snapshot=broker_state_snapshot,
            max_seed_notional=float(max_seed_notional),
            max_seed_orders=int(max_seed_orders),
            use_extended_hours=not market_is_open,
        )
    campaign_run_id = fill_collection.generate_run_id(source_type="alpaca_fill_campaign")
    campaign_root = Path(output_dir).resolve() / campaign_run_id
    campaign_root.mkdir(parents=True, exist_ok=True)
    task_input_root = campaign_root / "task_inputs"
    run_root_dir = campaign_root / "runs"
    task_input_root.mkdir(parents=True, exist_ok=True)
    run_root_dir.mkdir(parents=True, exist_ok=True)

    selected_rows = tasks.loc[tasks["selected"].astype(bool)].copy()
    selected_rows = selected_rows.head(int(max_runs)).copy()
    selected_task_ids = {str(value) for value in selected_rows["task_id"].tolist()}
    if not tasks.empty:
        overflow_mask = tasks["selected"].astype(bool) & ~tasks["task_id"].astype(str).isin(selected_task_ids)
        tasks.loc[overflow_mask, "selected"] = False
        tasks.loc[overflow_mask, "status"] = "skipped"
        tasks.loc[overflow_mask, "selection_reason"] = "max_runs_reached"
    if selected_rows.empty:
        score_payload = score_bucket_coverage(
            tasks=tasks,
            plan=plan,
            source_baskets=source_baskets,
            side_scope=side_scope,
            campaign_preset=campaign_preset,
            seed_summary=seed_summary,
        )
        manifest = build_campaign_manifest(
            campaign_run_id=campaign_run_id,
            created_at=_now_iso(),
            market=str(market).strip().lower(),
            broker=str(broker).strip().lower(),
            notes=notes,
            campaign_preset=campaign_preset,
            side_scope=side_scope,
            broker_state_snapshot=broker_state_summary,
            seed_summary=seed_summary,
            bucket_plan_path=bucket_plan_file,
            plan=plan,
            tasks=tasks,
            score_payload=score_payload,
            source_run_roots=source_run_roots,
            source_orders_oms=source_orders,
            max_runs=max_runs,
        )
        tasks.to_csv(campaign_root / "alpaca_fill_campaign_tasks.csv", index=False, encoding="utf-8")
        (campaign_root / "alpaca_fill_campaign_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        (campaign_root / "alpaca_fill_campaign_report.md").write_text(render_campaign_report(manifest=manifest, tasks=tasks), encoding="utf-8")
        return campaign_root

    for index, row in selected_rows.iterrows():
        task_id = str(row["task_id"])
        task_dir = task_input_root / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        input_path = task_dir / "orders_oms.csv"
        input_frame = _build_task_input_orders(row, task_inputs, side_scope=side_scope)
        input_frame.to_csv(input_path, index=False, encoding="utf-8")
        tasks.loc[index, "input_orders_oms_path"] = str(input_path)
        tasks.loc[index, "status"] = "selected"
        run_notes = " | ".join(
            value
            for value in [
                notes.strip(),
                f"campaign_run_id={campaign_run_id}",
                f"task_id={task_id}",
                f"target={row['target_name']}",
                f"scale_factor={float(row['scale_factor']):.4f}",
                f"side_scope={side_scope}",
            ]
            if value
        )
        try:
            run_dir = collection_runner(
                run_root=None,
                orders_oms=input_path,
                output_dir=run_root_dir,
                market=str(market),
                broker=str(broker),
                timeout_seconds=float(timeout_seconds),
                poll_interval_seconds=float(poll_interval_seconds),
                notes=run_notes,
                force_outside_market_hours=bool(force_outside_market_hours),
            )
            tasks.loc[index, "run_dir"] = str(Path(run_dir).resolve())
            tasks.loc[index, "status"] = "completed"
        except Exception as exc:  # pragma: no cover - exercise in failure tests
            tasks.loc[index, "run_dir"] = ""
            tasks.loc[index, "status"] = "failed"
            tasks.loc[index, "selection_reason"] = f"failed: {exc}"

        score_payload = score_bucket_coverage(
            tasks=tasks,
            plan=plan,
            source_baskets=source_baskets,
            side_scope=side_scope,
            campaign_preset=campaign_preset,
            seed_summary=seed_summary,
        )
        manifest = build_campaign_manifest(
            campaign_run_id=campaign_run_id,
            created_at=_now_iso(),
            market=str(market).strip().lower(),
            broker=str(broker).strip().lower(),
            notes=notes,
            campaign_preset=campaign_preset,
            side_scope=side_scope,
            broker_state_snapshot=broker_state_summary,
            seed_summary=seed_summary,
            bucket_plan_path=bucket_plan_file,
            plan=plan,
            tasks=tasks,
            score_payload=score_payload,
            source_run_roots=source_run_roots,
            source_orders_oms=source_orders,
            max_runs=max_runs,
        )
    tasks.to_csv(campaign_root / "alpaca_fill_campaign_tasks.csv", index=False, encoding="utf-8")
    (campaign_root / "alpaca_fill_campaign_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (campaign_root / "alpaca_fill_campaign_report.md").write_text(render_campaign_report(manifest=manifest, tasks=tasks), encoding="utf-8")
    return campaign_root
