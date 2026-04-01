"""Slippage calibration utilities driven by Alpaca fill telemetry."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from portfolio_os.utils.config import SlippageConfig, load_yaml_file


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "slippage_calibration_us"
DEFAULT_PREP_ROOT = DEFAULT_OUTPUT_ROOT / "prep"


@dataclass
class SlippageCalibrationResult:
    """All artifacts produced by one calibration run."""

    dataset: pd.DataFrame
    residuals: pd.DataFrame
    summary: dict[str, Any]
    report_markdown: str
    overlay_payload: dict[str, Any]
    diagnostic_manifest: dict[str, Any]


@dataclass
class SyntheticSlippageCalibrationFixture:
    """Deterministic synthetic telemetry fixture for offline calibration checks."""

    fixture_root: Path
    fill_collection_root: Path
    source_run_root: Path
    manifest_path: Path
    expected_k: float
    alpha: float


def _normalized_count_map(series: pd.Series | None, *, default_label: str = "unknown") -> dict[str, int]:
    """Return normalized value counts for audit/report payloads."""

    if series is None or len(series) == 0:
        return {}
    normalized = (
        pd.Series(series)
        .fillna(default_label)
        .astype(str)
        .str.strip()
        .replace({"": default_label})
        .str.lower()
    )
    counts = normalized.value_counts(dropna=False)
    return {str(key): int(value) for key, value in counts.items()}


def _masked_side_counts(dataset: pd.DataFrame, mask: pd.Series) -> dict[str, int]:
    """Return normalized side counts for a masked subset."""

    if dataset.empty or "direction" not in dataset.columns:
        return {}
    return _normalized_count_map(dataset.loc[mask, "direction"])


def _resolve_overlay_decision(
    *,
    candidate_k: float | None,
    fit_sample_count: int,
    enough_orders: bool,
    enough_span: bool,
    low_participation_coverage_ok: bool,
    positive_signal_ok: bool,
    metrics_improved: bool,
    update_default_config: bool,
) -> tuple[str, str, str]:
    """Classify whether the current calibration is ready for overlay usage."""

    if candidate_k is None or not math.isfinite(float(candidate_k)) or float(candidate_k) <= 0 or int(fit_sample_count) <= 0:
        return (
            "insufficient",
            "collect_more_fills",
            "no_positive_fit_candidate",
        )
    if enough_orders and enough_span and positive_signal_ok and metrics_improved:
        if update_default_config:
            return (
                "sufficient",
                "promote_to_default",
                "default_promotion_guardrails_satisfied",
            )
        return (
            "sufficient",
            "apply_as_paper_overlay",
            "overlay_guardrails_satisfied",
        )
    if low_participation_coverage_ok and positive_signal_ok and metrics_improved:
        return (
            "sufficient",
            "apply_as_paper_overlay",
            "low_participation_overlay_guardrails_satisfied",
        )
    return (
        "directional_only",
        "collect_more_fills",
        "candidate_available_but_coverage_or_quality_insufficient",
    )


def _counts_to_rows(label: str, counts: dict[str, int]) -> list[dict[str, Any]]:
    """Convert a count mapping into markdown-table rows."""

    return [{label: key, "count": int(value)} for key, value in sorted(counts.items())]


def prepare_slippage_calibration_prep(
    *,
    output_dir: Path = DEFAULT_PREP_ROOT,
    fill_collection_root: Path | None = None,
) -> dict[str, Path]:
    """Create a deterministic prep skeleton for the next calibration run."""

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    fill_collection_root = (
        Path(fill_collection_root).resolve()
        if fill_collection_root is not None
        else (ROOT / "outputs" / "alpaca_fill_collection").resolve()
    )

    subdirs = {
        "fill_collection_dir": output_dir / "fill_collection",
        "dataset_dir": output_dir / "dataset",
        "residuals_dir": output_dir / "residuals",
        "candidate_overlay_dir": output_dir / "candidate_overlay",
        "diagnostics_dir": output_dir / "diagnostics",
    }
    for path in subdirs.values():
        path.mkdir(parents=True, exist_ok=True)

    prep_manifest = {
        "generated_at_utc": _utc_now_iso(),
        "prep_root": str(output_dir),
        "fill_collection_root": str(fill_collection_root),
        "dataset_path": str((subdirs["dataset_dir"] / "slippage_calibration_dataset.csv").resolve()),
        "residuals_path": str((subdirs["residuals_dir"] / "slippage_residuals.csv").resolve()),
        "candidate_overlay_path": str((subdirs["candidate_overlay_dir"] / "slippage_candidate_overlay.yaml").resolve()),
        "diagnostic_manifest_path": str((subdirs["diagnostics_dir"] / "diagnostic_manifest.json").resolve()),
        "required_inputs": [
            "fill_collection_root",
            "dataset_path",
            "residuals_path",
            "candidate_overlay_path",
            "diagnostic_manifest_path",
        ],
        "status": "prep_only",
    }

    checklist_lines = [
        "# Slippage Calibration Prep Checklist",
        "",
        f"- generated_at_utc: {prep_manifest['generated_at_utc']}",
        f"- prep_root: {prep_manifest['prep_root']}",
        f"- fill_collection_root: {prep_manifest['fill_collection_root']}",
        "",
        "## Required Inputs",
        "",
        f"- dataset_path: {prep_manifest['dataset_path']}",
        f"- residuals_path: {prep_manifest['residuals_path']}",
        f"- candidate_overlay_path: {prep_manifest['candidate_overlay_path']}",
        f"- diagnostic_manifest_path: {prep_manifest['diagnostic_manifest_path']}",
        "",
        "## Next Steps",
        "",
        "- Wait for the tomorrow minimal buy validation fill run.",
        "- Copy the resulting alpaca fill collection outputs into the fill_collection_root reference above.",
        "- Run slippage calibration only after a filled collection run exists with auditable telemetry.",
    ]

    manifest_path = output_dir / "slippage_calibration_prep_manifest.json"
    checklist_path = output_dir / "slippage_calibration_prep_checklist.md"
    manifest_path.write_text(json.dumps(prep_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    checklist_path.write_text("\n".join(checklist_lines) + "\n", encoding="utf-8")

    return {
        "prep_root": output_dir,
        "fill_collection_dir": subdirs["fill_collection_dir"],
        "dataset_dir": subdirs["dataset_dir"],
        "residuals_dir": subdirs["residuals_dir"],
        "candidate_overlay_dir": subdirs["candidate_overlay_dir"],
        "diagnostics_dir": subdirs["diagnostics_dir"],
        "slippage_calibration_prep_manifest": manifest_path,
        "slippage_calibration_prep_checklist": checklist_path,
    }


def _write_synthetic_source_run_root(
    fixture_root: Path,
    *,
    sample_id: str,
    source_rows: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
) -> Path:
    source_root = fixture_root / "pilot_validation_source"
    sample_dir = source_root / "samples" / sample_id / "main"
    sample_dir.mkdir(parents=True, exist_ok=True)

    market_path = fixture_root / "market.csv"
    pd.DataFrame(market_rows).to_csv(market_path, index=False, encoding="utf-8")
    pd.DataFrame(source_rows).to_csv(sample_dir / "orders.csv", index=False, encoding="utf-8")
    (sample_dir / "audit.json").write_text(
        json.dumps({"inputs": {"market": {"path": str(market_path)}}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return source_root


def _write_synthetic_fill_collection_run(
    fixture_root: Path,
    *,
    run_name: str,
    source_root: Path,
    fill_rows: list[dict[str, Any]],
) -> Path:
    run_dir = fixture_root / "alpaca_fill_collection" / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_name,
        "created_at": "2026-03-26T16:06:00+00:00",
        "market": "us",
        "broker": "alpaca",
        "notes": "synthetic slippage calibration fixture",
        "source_type": "run_root",
        "source_path": str(source_root),
        "order_count": len(fill_rows),
        "submitted_count": sum(
            1
            for row in fill_rows
            if float(row.get("filled_qty", 0) or 0) > 0
            or str(row.get("status", "")).lower() in {"timeout_cancelled", "rejected"}
        ),
        "filled_count": sum(1 for row in fill_rows if float(row.get("filled_qty", 0) or 0) > 0),
        "partial_count": sum(1 for row in fill_rows if str(row.get("status", "")).lower() == "partially_filled"),
        "unfilled_count": sum(1 for row in fill_rows if float(row.get("filled_qty", 0) or 0) <= 0),
        "rejected_count": sum(1 for row in fill_rows if str(row.get("status", "")).lower() == "rejected"),
        "timeout_cancelled_count": sum(1 for row in fill_rows if str(row.get("status", "")).lower() == "timeout_cancelled"),
        "avg_fill_price_mean": None,
        "has_any_filled_orders": any(float(row.get("filled_qty", 0) or 0) > 0 for row in fill_rows),
        "event_granularity": "polled_history",
    }
    (run_dir / "alpaca_fill_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame(fill_rows).to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")
    pd.DataFrame([]).to_csv(run_dir / "alpaca_fill_events.csv", index=False, encoding="utf-8")
    return run_dir


def create_synthetic_slippage_calibration_fixture(
    *,
    output_dir: Path,
    sample_id: str = "sample_us_01",
    positive_count: int = 24,
    negative_count: int = 4,
    include_missing_adv: bool = True,
    include_timeout: bool = True,
    true_k: float = 0.02,
    alpha: float = 0.6,
) -> SyntheticSlippageCalibrationFixture:
    """Create a deterministic offline calibration fixture that mimics one fill-collection run."""

    fixture_root = Path(output_dir).resolve()
    fixture_root.mkdir(parents=True, exist_ok=True)

    source_rows: list[dict[str, Any]] = []
    market_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []

    for index in range(int(positive_count)):
        ticker = f"T{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 100.0 + 100.0 * index
        adv = 10000.0 + 500.0 * index
        reference_price = 100.0 + float(index)
        slippage_notional = reference_price * float(true_k) * qty * ((qty / adv) ** float(alpha))
        avg_fill_price = (
            reference_price + slippage_notional / qty
            if direction == "buy"
            else reference_price - slippage_notional / qty
        )
        source_rows.append(
            {
                "ticker": ticker,
                "side": direction.upper(),
                "quantity": qty,
                "estimated_price": reference_price,
                "estimated_notional": reference_price * qty,
            }
        )
        market_rows.append(
            {
                "ticker": ticker,
                "close": reference_price,
                "vwap": reference_price,
                "adv_shares": adv,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        )
        fill_rows.append(
            {
                "sample_id": sample_id,
                "ticker": ticker,
                "direction": direction,
                "requested_qty": qty,
                "filled_qty": qty,
                "avg_fill_price": avg_fill_price,
                "estimated_price": reference_price,
                "requested_notional": reference_price * qty,
                "filled_notional": avg_fill_price * qty,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": None,
                "broker_order_id": f"order-{index}",
                "submitted_at_utc": f"2026-03-26T16:06:{index:02d}+00:00",
                "terminal_at_utc": f"2026-03-26T16:06:{index + 1:02d}+00:00",
                "latency_seconds": 1.0,
                "poll_count": 2,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    for index in range(int(negative_count)):
        ticker = f"N{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 250.0 + 25.0 * index
        adv = 15000.0 + 250.0 * index
        reference_price = 200.0 + float(index)
        slippage_notional = reference_price * float(true_k) * qty * ((qty / adv) ** float(alpha))
        avg_fill_price = (
            reference_price - slippage_notional / qty
            if direction == "buy"
            else reference_price + slippage_notional / qty
        )
        source_rows.append(
            {
                "ticker": ticker,
                "side": direction.upper(),
                "quantity": qty,
                "estimated_price": reference_price,
                "estimated_notional": reference_price * qty,
            }
        )
        market_rows.append(
            {
                "ticker": ticker,
                "close": reference_price,
                "vwap": reference_price,
                "adv_shares": adv,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        )
        fill_rows.append(
            {
                "sample_id": sample_id,
                "ticker": ticker,
                "direction": direction,
                "requested_qty": qty,
                "filled_qty": qty,
                "avg_fill_price": avg_fill_price,
                "estimated_price": reference_price,
                "requested_notional": reference_price * qty,
                "filled_notional": avg_fill_price * qty,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": None,
                "broker_order_id": f"neg-{index}",
                "submitted_at_utc": f"2026-03-26T16:07:{index:02d}+00:00",
                "terminal_at_utc": f"2026-03-26T16:07:{index + 1:02d}+00:00",
                "latency_seconds": 1.0,
                "poll_count": 2,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    if include_missing_adv:
        ticker = "MISSING_ADV"
        direction = "buy"
        qty = 500.0
        reference_price = 300.0
        source_rows.append(
            {
                "ticker": ticker,
                "side": direction.upper(),
                "quantity": qty,
                "estimated_price": reference_price,
                "estimated_notional": reference_price * qty,
            }
        )
        fill_rows.append(
            {
                "sample_id": sample_id,
                "ticker": ticker,
                "direction": direction,
                "requested_qty": qty,
                "filled_qty": qty,
                "avg_fill_price": reference_price * 1.01,
                "estimated_price": reference_price,
                "requested_notional": reference_price * qty,
                "filled_notional": reference_price * 1.01 * qty,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": None,
                "broker_order_id": "missing-adv",
                "submitted_at_utc": "2026-03-26T16:08:00+00:00",
                "terminal_at_utc": "2026-03-26T16:08:01+00:00",
                "latency_seconds": 1.0,
                "poll_count": 2,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    if include_timeout:
        fill_rows.append(
            {
                "sample_id": sample_id,
                "ticker": "TIMEOUT",
                "direction": "buy",
                "requested_qty": 100.0,
                "filled_qty": 0.0,
                "avg_fill_price": None,
                "estimated_price": 150.0,
                "requested_notional": 15000.0,
                "filled_notional": 0.0,
                "fill_ratio": 0.0,
                "status": "timeout_cancelled",
                "reject_reason": "timed out",
                "broker_order_id": "timeout-1",
                "submitted_at_utc": "2026-03-26T16:09:00+00:00",
                "terminal_at_utc": "2026-03-26T16:14:00+00:00",
                "latency_seconds": 300.0,
                "poll_count": 2,
                "timeout_cancelled": True,
                "cancel_requested": True,
                "cancel_acknowledged": True,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    source_run_root = _write_synthetic_source_run_root(
        fixture_root,
        sample_id=sample_id,
        source_rows=source_rows,
        market_rows=market_rows,
    )
    fill_run_dir = _write_synthetic_fill_collection_run(
        fixture_root,
        run_name="run_root_20260326T160600_e0c9c68f",
        source_root=source_run_root,
        fill_rows=fill_rows,
    )
    manifest_path = fixture_root / "synthetic_fixture_manifest.json"
    manifest_payload = {
        "generated_at_utc": _utc_now_iso(),
        "sample_id": sample_id,
        "fixture_root": str(fixture_root),
        "fill_collection_root": str((fixture_root / "alpaca_fill_collection").resolve()),
        "source_run_root": str(source_run_root.resolve()),
        "fill_run_dir": str(fill_run_dir.resolve()),
        "expected_k": float(true_k),
        "alpha": float(alpha),
        "positive_count": int(positive_count),
        "negative_count": int(negative_count),
        "include_missing_adv": bool(include_missing_adv),
        "include_timeout": bool(include_timeout),
        "filled_order_count": int(sum(1 for row in fill_rows if float(row.get("filled_qty", 0) or 0) > 0)),
        "total_fill_rows": int(len(fill_rows)),
        "notes": "Synthetic fixture for offline slippage calibration workflow validation.",
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return SyntheticSlippageCalibrationFixture(
        fixture_root=fixture_root,
        fill_collection_root=(fixture_root / "alpaca_fill_collection").resolve(),
        source_run_root=source_run_root.resolve(),
        manifest_path=manifest_path.resolve(),
        expected_k=float(true_k),
        alpha=float(alpha),
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return float(parsed)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"buy", "b"}:
        return "buy"
    if text in {"sell", "s"}:
        return "sell"
    return ""


def _first_existing_path(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _resolve_path(raw_path: Any, *, anchors: list[Path]) -> Path | None:
    if raw_path is None:
        return None
    text = str(raw_path).strip()
    if not text:
        return None
    candidate = Path(text)
    if candidate.is_absolute():
        return candidate
    for anchor in anchors:
        resolved = (anchor / candidate).resolve()
        if resolved.exists():
            return resolved
    return (anchors[0] / candidate).resolve() if anchors else candidate.resolve()


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    clean = pd.concat([values, weights], axis=1).dropna()
    if clean.empty:
        return None
    v = clean.iloc[:, 0].astype(float).to_numpy()
    w = clean.iloc[:, 1].astype(float).to_numpy()
    w_sum = float(np.sum(w))
    if w_sum <= 0:
        return None
    return float(np.sum(v * w) / w_sum)


def _weighted_rmse(values: pd.Series, weights: pd.Series) -> float | None:
    clean = pd.concat([values, weights], axis=1).dropna()
    if clean.empty:
        return None
    v = clean.iloc[:, 0].astype(float).to_numpy()
    w = clean.iloc[:, 1].astype(float).to_numpy()
    w_sum = float(np.sum(w))
    if w_sum <= 0:
        return None
    return float(math.sqrt(np.sum((v * v) * w) / w_sum))


def _weighted_mape(predicted: pd.Series, actual: pd.Series, weights: pd.Series) -> float | None:
    clean = pd.concat([predicted, actual, weights], axis=1).dropna()
    if clean.empty:
        return None
    pred = clean.iloc[:, 0].astype(float).to_numpy()
    target = clean.iloc[:, 1].astype(float).to_numpy()
    w = clean.iloc[:, 2].astype(float).to_numpy()
    mask = target > 0
    if not np.any(mask):
        return None
    pred = pred[mask]
    target = target[mask]
    w = w[mask]
    denom = np.maximum(target, 1e-12)
    values = np.abs(pred - target) / denom
    w_sum = float(np.sum(w))
    if w_sum <= 0:
        return None
    return float(np.sum(values * w) / w_sum)


def _weighted_median(values: pd.Series, weights: pd.Series) -> float | None:
    clean = pd.concat([values, weights], axis=1).dropna()
    if clean.empty:
        return None
    ordered = clean.sort_values(by=clean.columns[0], kind="mergesort")
    vals = ordered.iloc[:, 0].astype(float).to_numpy()
    w = ordered.iloc[:, 1].astype(float).to_numpy()
    total = float(np.sum(w))
    if total <= 0:
        return None
    midpoint = 0.5 * total
    cumsum = np.cumsum(w)
    index = int(np.searchsorted(cumsum, midpoint, side="left"))
    index = min(index, len(vals) - 1)
    return float(vals[index])


def _bucket_participation(value: float | None) -> str:
    if value is None or math.isnan(float(value)):
        return "missing"
    pct = float(value)
    bins = [
        (0.1, "0-0.1%"),
        (0.5, "0.1-0.5%"),
        (1.0, "0.5-1.0%"),
        (2.0, "1.0-2.0%"),
        (5.0, "2.0-5.0%"),
        (10.0, "5.0-10.0%"),
    ]
    lower = 0.0
    for upper, label in bins:
        if lower <= pct < upper:
            return label
        lower = upper
    return "10.0%+"


def _bucket_notional(value: float | None) -> str:
    if value is None or math.isnan(float(value)):
        return "missing"
    notional = float(value)
    bins = [
        (5_000.0, "<5k"),
        (25_000.0, "5k-25k"),
        (100_000.0, "25k-100k"),
        (250_000.0, "100k-250k"),
        (500_000.0, "250k-500k"),
    ]
    lower = 0.0
    for upper, label in bins:
        if lower <= notional < upper:
            return label
        lower = upper
    return "500k+"


def _direction_sign(direction: str) -> int:
    normalized = _normalize_direction(direction)
    if normalized == "buy":
        return 1
    if normalized == "sell":
        return -1
    return 0


def _build_orders_lookup_from_frame(
    *,
    order_frame: pd.DataFrame,
    order_path: Path,
    source_anchor: Path,
    market_frame: pd.DataFrame | None = None,
    market_path: Path | None = None,
    audit_path: Path | None = None,
    sample_id_default: str | None = None,
    sample_dir: Path | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if order_frame.empty:
        return lookup

    work = order_frame.copy()
    default_sample_id = str(sample_id_default or order_path.stem).strip() or order_path.stem
    if "ticker" not in work.columns and "symbol" in work.columns:
        work["ticker"] = work["symbol"]
    if "direction" not in work.columns and "side" in work.columns:
        work["direction"] = work["side"]
    if "requested_qty" not in work.columns and "quantity" in work.columns:
        work["requested_qty"] = work["quantity"]
    if "reference_price" not in work.columns:
        work["reference_price"] = work.get("estimated_price", pd.NA)
    if "estimated_price" not in work.columns:
        work["estimated_price"] = pd.NA
    work["sample_id"] = work.get("sample_id", default_sample_id)
    work["sample_id"] = work["sample_id"].astype(str).str.strip().replace({"": default_sample_id})
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    work["requested_qty"] = pd.to_numeric(work["requested_qty"], errors="coerce")
    work["reference_price"] = pd.to_numeric(work["reference_price"], errors="coerce")
    work["estimated_price"] = pd.to_numeric(work["estimated_price"], errors="coerce")

    market_lookup: dict[str, float | None] = {}
    if market_frame is not None and not market_frame.empty and "ticker" in market_frame.columns and "adv_shares" in market_frame.columns:
        market_work = market_frame.copy()
        market_work["ticker"] = market_work["ticker"].astype(str).str.strip()
        market_work["adv_shares"] = pd.to_numeric(market_work["adv_shares"], errors="coerce")
        market_lookup = {
            str(row["ticker"]).strip(): _safe_float(row.get("adv_shares"), None)
            for row in market_work.to_dict(orient="records")
        }

    for row in work.to_dict(orient="records"):
        ticker = str(row.get("ticker", "")).strip()
        sample_id = str(row.get("sample_id", default_sample_id)).strip() or default_sample_id
        if not ticker:
            continue
        lookup[(sample_id, ticker)] = {
            "source_run_root": str(source_anchor),
            "source_sample_dir": str(sample_dir) if sample_dir is not None else None,
            "source_order_path": str(order_path),
            "source_audit_path": str(audit_path) if audit_path is not None else None,
            "source_market_path": str(market_path) if market_path is not None else None,
            "source_ticker": ticker,
            "source_direction": _normalize_direction(row.get("direction")),
            "source_requested_qty": _safe_float(row.get("requested_qty"), None),
            "source_reference_price": _safe_float(row.get("reference_price"), None),
            "source_estimated_price": _safe_float(row.get("estimated_price"), None),
            "source_adv_shares": _safe_float(market_lookup.get(ticker), None),
        }
    return lookup


def _build_source_order_lookup(source_run_root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if source_run_root.is_file():
        order_path = source_run_root
        order_frame = _read_csv(order_path)
        market_path = _first_existing_path(
            [
                order_path.with_name("fill_collection_batch.csv"),
                order_path.with_name("market.csv"),
            ]
        )
        market_frame = _read_csv(market_path) if market_path is not None else pd.DataFrame()
        return _build_orders_lookup_from_frame(
            order_frame=order_frame,
            order_path=order_path,
            source_anchor=order_path.parent,
            market_frame=market_frame,
            market_path=market_path,
            sample_id_default=order_path.stem,
        )

    samples_root = source_run_root / "samples"
    if not samples_root.exists():
        return lookup

    for sample_dir in sorted([path for path in samples_root.iterdir() if path.is_dir()]):
        sample_id = sample_dir.name.strip()
        if not sample_id:
            continue
        order_path = _first_existing_path(
            [
                sample_dir / "approval" / "final_orders_oms.csv",
                sample_dir / "main" / "orders_oms.csv",
                sample_dir / "main" / "orders.csv",
            ]
        )
        if order_path is None:
            continue
        order_frame = _read_csv(order_path)
        if order_frame.empty:
            continue
        if "ticker" not in order_frame.columns and "symbol" in order_frame.columns:
            order_frame["ticker"] = order_frame["symbol"]
        if "direction" not in order_frame.columns and "side" in order_frame.columns:
            order_frame["direction"] = order_frame["side"]
        if "requested_qty" not in order_frame.columns and "quantity" in order_frame.columns:
            order_frame["requested_qty"] = order_frame["quantity"]
        if "reference_price" not in order_frame.columns:
            order_frame["reference_price"] = order_frame.get("estimated_price", pd.NA)
        if "estimated_price" not in order_frame.columns:
            order_frame["estimated_price"] = pd.NA
        order_frame["sample_id"] = order_frame.get("sample_id", sample_id)
        order_frame["sample_id"] = order_frame["sample_id"].astype(str).str.strip().replace({"": sample_id})
        order_frame["ticker"] = order_frame["ticker"].astype(str).str.strip()
        order_frame["direction"] = order_frame["direction"].astype(str).str.strip().str.lower()
        order_frame["requested_qty"] = pd.to_numeric(order_frame["requested_qty"], errors="coerce")
        order_frame["reference_price"] = pd.to_numeric(order_frame["reference_price"], errors="coerce")
        order_frame["estimated_price"] = pd.to_numeric(order_frame["estimated_price"], errors="coerce")

        audit_path = _first_existing_path(
            [
                sample_dir / "main" / "audit.json",
                sample_dir / "approval" / "final_audit.json",
            ]
        )
        market_path: Path | None = None
        if audit_path is not None:
            audit_payload = _read_json(audit_path)
            market_path = _resolve_path(
                (((audit_payload.get("inputs") or {}).get("market") or {}).get("path")),
                anchors=[audit_path.parent, source_run_root],
            )
        market_frame = _read_csv(market_path) if market_path is not None else pd.DataFrame()
        lookup.update(
            _build_orders_lookup_from_frame(
                order_frame=order_frame,
                order_path=order_path,
                source_anchor=source_run_root,
                market_frame=market_frame,
                market_path=market_path,
                audit_path=audit_path,
                sample_id_default=sample_id,
                sample_dir=sample_dir,
            )
        )
    return lookup


def load_fill_collection(fill_collection_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load all Alpaca fill collection runs under a root directory."""

    manifests: list[dict[str, Any]] = []
    rows: list[pd.DataFrame] = []
    manifest_paths = sorted(fill_collection_root.rglob("alpaca_fill_manifest.json"))
    for manifest_path in manifest_paths:
        run_dir = manifest_path.parent
        manifest = _read_json(manifest_path)
        orders_path = run_dir / "alpaca_fill_orders.csv"
        orders_frame = _read_csv(orders_path)
        manifests.append(
            {
                "collection_run_dir": str(run_dir),
                "collection_manifest_path": str(manifest_path),
                "collection_orders_path": str(orders_path),
                "collection_run_id": manifest.get("run_id"),
                "collection_source_path": manifest.get("source_path"),
                "collection_market": manifest.get("market"),
                "collection_broker": manifest.get("broker"),
                "order_count": _safe_int(manifest.get("order_count"), 0),
                "submitted_count": _safe_int(manifest.get("submitted_count"), 0),
                "filled_count": _safe_int(manifest.get("filled_count"), 0),
                "partial_count": _safe_int(manifest.get("partial_count"), 0),
                "unfilled_count": _safe_int(manifest.get("unfilled_count"), 0),
                "rejected_count": _safe_int(manifest.get("rejected_count"), 0),
                "timeout_cancelled_count": _safe_int(manifest.get("timeout_cancelled_count"), 0),
                "avg_fill_price_mean": _safe_float(manifest.get("avg_fill_price_mean"), None),
                "has_any_filled_orders": bool(manifest.get("has_any_filled_orders", False)),
            }
        )
        if orders_frame.empty:
            continue
        orders_frame = orders_frame.copy()
        orders_frame["collection_run_dir"] = str(run_dir)
        orders_frame["collection_manifest_path"] = str(manifest_path)
        orders_frame["collection_orders_path"] = str(orders_path)
        orders_frame["collection_run_id"] = manifest.get("run_id")
        orders_frame["collection_source_path"] = manifest.get("source_path")
        orders_frame["collection_notes"] = manifest.get("notes")
        orders_frame["collection_market"] = manifest.get("market")
        orders_frame["collection_broker"] = manifest.get("broker")
        rows.append(orders_frame)

    if rows:
        orders = pd.concat(rows, ignore_index=True)
    else:
        orders = pd.DataFrame()
    manifests_frame = pd.DataFrame(manifests)
    return orders, manifests_frame


def build_slippage_calibration_dataset(
    *,
    fill_collection_root: Path,
    source_run_root: Path | None = None,
    alpha: float = 0.6,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build a row-level calibration dataset from fill telemetry and source market data."""

    collection_orders, _manifest_rows = load_fill_collection(fill_collection_root)
    if collection_orders.empty:
        empty = collection_orders.copy()
        return empty, {
            "sample_count": 0,
            "filled_order_count": 0,
            "positive_signal_count": 0,
            "negative_signal_count": 0,
            "missing_adv_count": 0,
            "missing_reference_price_count": 0,
            "partial_count": 0,
            "timeout_cancelled_count": 0,
            "rejected_count": 0,
            "unfilled_count": 0,
            "fit_eligible_count": 0,
            "fit_sample_count": 0,
            "positive_signal_notional": 0.0,
            "negative_signal_notional": 0.0,
            "positive_signal_share": None,
            "participation_span": None,
        }

    work = collection_orders.copy()
    for column in ["requested_qty", "filled_qty", "avg_fill_price", "estimated_price", "filled_notional", "requested_notional"]:
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
        else:
            work[column] = pd.NA
    if "direction" in work.columns:
        work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    if "sample_id" in work.columns:
        work["sample_id"] = work["sample_id"].astype(str).str.strip()
    if "ticker" in work.columns:
        work["ticker"] = work["ticker"].astype(str).str.strip()

    source_cache: dict[Path, dict[tuple[str, str], dict[str, Any]]] = {}
    source_root_resolved = source_run_root.resolve() if source_run_root is not None else None
    source_lookup_rows: list[dict[str, Any]] = []
    for row in work.to_dict(orient="records"):
        manifest_source = _resolve_path(row.get("collection_source_path"), anchors=[fill_collection_root, fill_collection_root.parent])
        effective_source_root = source_root_resolved or (manifest_source.resolve() if manifest_source is not None else None)
        if effective_source_root is None:
            lookup = {}
        else:
            if effective_source_root not in source_cache:
                source_cache[effective_source_root] = _build_source_order_lookup(effective_source_root)
            lookup = source_cache[effective_source_root]
        sample_id = str(row.get("sample_id", "")).strip()
        ticker = str(row.get("ticker", "")).strip()
        source_row = lookup.get((sample_id, ticker), {})
        source_lookup_rows.append(
            {
                "source_run_root": str(effective_source_root) if effective_source_root is not None else None,
                "source_sample_dir": source_row.get("source_sample_dir"),
                "source_order_path": source_row.get("source_order_path"),
                "source_audit_path": source_row.get("source_audit_path"),
                "source_market_path": source_row.get("source_market_path"),
                "source_ticker": source_row.get("source_ticker"),
                "source_direction": source_row.get("source_direction"),
                "source_requested_qty": source_row.get("source_requested_qty"),
                "source_reference_price": source_row.get("source_reference_price"),
                "source_estimated_price": source_row.get("source_estimated_price"),
                "source_adv_shares": source_row.get("source_adv_shares"),
            }
        )

    source_frame = pd.DataFrame(source_lookup_rows)
    work = pd.concat([work.reset_index(drop=True), source_frame], axis=1)

    if "reference_price" in work.columns:
        work["reference_price"] = work["reference_price"].where(work["reference_price"].notna(), work.get("source_reference_price"))
    else:
        work["reference_price"] = work.get("source_reference_price")
    if "estimated_price" in work.columns:
        work["estimated_price"] = work["estimated_price"].where(work["estimated_price"].notna(), work["source_estimated_price"])
    else:
        work["estimated_price"] = work["source_estimated_price"]
    if "requested_qty" in work.columns:
        work["requested_qty"] = work["requested_qty"].where(work["requested_qty"].notna(), work["source_requested_qty"])
    else:
        work["requested_qty"] = work["source_requested_qty"]
    if "direction" in work.columns:
        work["direction"] = work["direction"].where(work["direction"].astype(str).str.strip() != "", work["source_direction"])
    else:
        work["direction"] = work["source_direction"]
    if "ticker" in work.columns:
        work["ticker"] = work["ticker"].where(work["ticker"].astype(str).str.strip() != "", work["source_ticker"])
    else:
        work["ticker"] = work["source_ticker"]

    work["requested_qty"] = pd.to_numeric(work["requested_qty"], errors="coerce")
    work["filled_qty"] = pd.to_numeric(work["filled_qty"], errors="coerce").fillna(0.0)
    work["avg_fill_price"] = pd.to_numeric(work["avg_fill_price"], errors="coerce")
    work["reference_price"] = pd.to_numeric(work["reference_price"], errors="coerce")
    work["estimated_price"] = pd.to_numeric(work["estimated_price"], errors="coerce")
    work["source_adv_shares"] = pd.to_numeric(work["source_adv_shares"], errors="coerce")
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    work["ticker"] = work["ticker"].astype(str).str.strip()

    work["reference_price"] = work["reference_price"].where(work["reference_price"].notna(), work["estimated_price"])
    work["side_sign"] = work["direction"].map({"buy": 1, "sell": -1}).fillna(0).astype(int)
    work["filled_notional"] = pd.to_numeric(work.get("filled_notional"), errors="coerce")
    work["requested_notional"] = pd.to_numeric(work.get("requested_notional"), errors="coerce")
    work["reference_notional"] = work["reference_price"] * work["filled_qty"]
    work["realized_slippage_per_share"] = work["side_sign"] * (work["avg_fill_price"] - work["reference_price"])
    work["realized_slippage_notional"] = work["realized_slippage_per_share"] * work["filled_qty"]
    work["positive_realized_slippage_notional"] = work["realized_slippage_notional"].clip(lower=0.0)
    work["positive_realized_slippage_bps"] = np.where(
        work["reference_notional"] > 0,
        work["positive_realized_slippage_notional"] / work["reference_notional"] * 10000.0,
        np.nan,
    )
    work["realized_slippage_bps"] = np.where(
        work["reference_notional"] > 0,
        work["realized_slippage_notional"] / work["reference_notional"] * 10000.0,
        np.nan,
    )
    work["participation_rate"] = np.where(
        work["source_adv_shares"] > 0,
        work["filled_qty"] / work["source_adv_shares"],
        np.nan,
    )
    work["participation_pct"] = work["participation_rate"] * 100.0
    work["model_scale"] = np.where(
        (work["reference_price"] > 0) & (work["filled_qty"] > 0) & (work["source_adv_shares"] > 0),
        work["reference_price"] * work["filled_qty"] * np.power(work["filled_qty"] / work["source_adv_shares"], alpha),
        np.nan,
    )
    work["missing_reference_price"] = work["reference_price"].isna()
    work["missing_adv"] = work["source_adv_shares"].isna() | (work["source_adv_shares"] <= 0)
    work["is_filled"] = work["filled_qty"] > 0
    work["fit_eligible"] = (
        work["is_filled"]
        & ~work["missing_reference_price"]
        & ~work["missing_adv"]
        & (work["side_sign"] != 0)
        & work["model_scale"].notna()
    )
    work["fit_reason"] = np.select(
        [
            ~work["is_filled"],
            work["missing_reference_price"],
            work["missing_adv"],
            work["side_sign"] == 0,
        ],
        [
            "not_filled",
            "missing_reference_price",
            "missing_adv",
            "missing_direction",
        ],
        default="eligible",
    )
    work["positive_signal"] = work["fit_eligible"] & (work["realized_slippage_notional"] > 0)
    work["negative_signal"] = work["fit_eligible"] & (work["realized_slippage_notional"] < 0)
    work["positive_signal_notional"] = work["positive_realized_slippage_notional"]
    work["fit_target_notional"] = np.where(work["positive_signal"], work["positive_realized_slippage_notional"], np.nan)
    work["fit_weight"] = np.where(
        work["fit_eligible"],
        work["filled_notional"].where(work["filled_notional"] > 0, work["reference_notional"]),
        np.nan,
    )

    dataset_summary = {
        "sample_count": int(len(work)),
        "filled_order_count": int((work["filled_qty"] > 0).sum()),
        "positive_signal_count": int(work["positive_signal"].sum()),
        "negative_signal_count": int(work["negative_signal"].sum()),
        "missing_adv_count": int((work["missing_adv"] & work["is_filled"]).sum()),
        "missing_reference_price_count": int((work["missing_reference_price"] & work["is_filled"]).sum()),
        "partial_count": int((work["status"].astype(str).str.lower() == "partially_filled").sum()) if "status" in work.columns else 0,
        "timeout_cancelled_count": int((work["status"].astype(str).str.lower() == "timeout_cancelled").sum()) if "status" in work.columns else 0,
        "rejected_count": int((work["status"].astype(str).str.lower() == "rejected").sum()) if "status" in work.columns else 0,
        "unfilled_count": int((work["filled_qty"] <= 0).sum()),
        "fit_eligible_count": int(work["fit_eligible"].sum()),
        "fit_sample_count": int(work["positive_signal"].sum()),
        "positive_signal_notional": float(work.loc[work["positive_signal"], "positive_realized_slippage_notional"].sum()),
        "negative_signal_notional": float(work.loc[work["negative_signal"], "realized_slippage_notional"].sum()),
        "positive_signal_share": None,
        "participation_span": None,
    }
    abs_realized = float(work.loc[work["fit_eligible"], "realized_slippage_notional"].abs().sum())
    if abs_realized > 0:
        dataset_summary["positive_signal_share"] = float(dataset_summary["positive_signal_notional"] / abs_realized)
    fit_participation = work.loc[work["positive_signal"], "participation_pct"].dropna()
    if not fit_participation.empty:
        dataset_summary["participation_span"] = float(fit_participation.max() - fit_participation.min())

    work.attrs.update(dataset_summary)
    return work, dataset_summary


def fit_slippage_k(
    dataset: pd.DataFrame,
    *,
    alpha: float,
    current_k: float,
    min_filled_orders: int = 20,
    min_participation_span: float = 10.0,
) -> dict[str, Any]:
    """Fit a non-negative candidate `k` using positive slippage observations only."""

    fit_frame = dataset.loc[dataset["positive_signal"]].copy()
    fit_frame = fit_frame.loc[fit_frame["model_scale"].notna() & (fit_frame["model_scale"] > 0)]
    if fit_frame.empty:
        return {
            "current_k": float(current_k),
            "candidate_k": None,
            "alpha_used": float(alpha),
            "alpha_candidate": None,
            "fit_sample_count": 0,
            "participation_span": 0.0,
            "positive_signal_share": 0.0,
            "insufficient_positive_signal": True,
            "recommendation": "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE",
            "recommendation_reason": "no_positive_fit_samples",
            "enough_orders": False,
            "enough_span": False,
        }

    target = fit_frame["positive_realized_slippage_notional"].astype(float)
    scale = fit_frame["model_scale"].astype(float)
    weights = fit_frame["fit_weight"].astype(float)
    ratio = target / scale
    ratio_weights = weights * scale
    candidate_k = _weighted_median(ratio, ratio_weights)
    if candidate_k is None or not math.isfinite(candidate_k) or candidate_k < 0:
        numerator = float(np.sum(weights * scale * target))
        denominator = float(np.sum(weights * scale * scale))
        candidate_k = numerator / denominator if denominator > 0 else None
    if candidate_k is not None:
        candidate_k = float(max(candidate_k, 0.0))

    positive_signal_share = _safe_float(dataset.attrs.get("positive_signal_share"), None)
    if positive_signal_share is None:
        abs_target = float(target.abs().sum())
        positive_signal_share = float(target.sum() / max(abs_target, 1e-12))
    participation_span = _safe_float(dataset.attrs.get("participation_span"), None)
    if participation_span is None:
        participation_span = float(fit_frame["participation_pct"].max() - fit_frame["participation_pct"].min())

    enough_orders = int(dataset["filled_qty"].gt(0).sum()) >= int(min_filled_orders)
    enough_span = float(participation_span) >= float(min_participation_span)
    enough_signal = bool(positive_signal_share >= 0.25 and len(fit_frame) >= max(5, min(3, min_filled_orders)))

    alpha_candidate = None
    if len(fit_frame) >= max(15, min_filled_orders):
        grid = np.arange(max(0.1, alpha - 0.3), min(1.2, alpha + 0.3) + 1e-9, 0.05)
        best: tuple[float, float] | None = None
        for alpha_try in grid:
            scale_try = fit_frame["reference_price"].astype(float) * fit_frame["filled_qty"].astype(float) * np.power(
                fit_frame["filled_qty"].astype(float) / fit_frame["source_adv_shares"].astype(float),
                float(alpha_try),
            )
            candidate_try = _weighted_median(target / scale_try, weights * scale_try)
            if candidate_try is None or not math.isfinite(candidate_try) or candidate_try < 0:
                continue
            residual = candidate_try * scale_try - target
            mae_try = float(np.sum(np.abs(residual) * weights) / max(float(np.sum(weights)), 1e-12))
            if best is None or mae_try < best[1]:
                best = (float(alpha_try), mae_try)
        if best is not None:
            alpha_candidate = best[0]

    recommendation: str
    recommendation_reason: str
    if not enough_orders or not enough_span or candidate_k is None or candidate_k <= 0:
        recommendation = "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE"
        if not enough_orders:
            recommendation_reason = "insufficient_filled_order_count"
        elif not enough_span:
            recommendation_reason = "insufficient_participation_span"
        elif candidate_k is None or candidate_k <= 0:
            recommendation_reason = "candidate_k_not_positive"
        else:
            recommendation_reason = "insufficient_data"
    elif not enough_signal:
        recommendation = "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE"
        recommendation_reason = "insufficient_positive_signal"
    else:
        recommendation = "provisional_only"
        recommendation_reason = "fit_available_but_rollout_conditions_not_met"

    return {
        "current_k": float(current_k),
        "candidate_k": candidate_k,
        "alpha_used": float(alpha),
        "alpha_candidate": alpha_candidate,
        "fit_sample_count": int(len(fit_frame)),
        "participation_span": float(participation_span),
        "positive_signal_share": float(positive_signal_share),
        "insufficient_positive_signal": bool(not enough_signal),
        "enough_orders": bool(enough_orders),
        "enough_span": bool(enough_span),
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
    }


def score_slippage_model(dataset: pd.DataFrame, *, k: float, alpha: float) -> dict[str, Any]:
    """Score one slippage model on the positive-signal subset."""

    if k is None or not math.isfinite(float(k)) or float(k) < 0:
        return {
            "mae_bps": None,
            "rmse_bps": None,
            "bias_bps": None,
            "weighted_mape": None,
            "observations": 0,
        }
    scoring = dataset.loc[dataset["positive_signal"]].copy()
    if scoring.empty:
        return {
            "mae_bps": None,
            "rmse_bps": None,
            "bias_bps": None,
            "weighted_mape": None,
            "observations": 0,
        }
    scale = scoring["reference_price"].astype(float) * scoring["filled_qty"].astype(float) * np.power(
        scoring["filled_qty"].astype(float) / scoring["source_adv_shares"].astype(float),
        float(alpha),
    )
    predicted = scale * float(k)
    actual = scoring["positive_realized_slippage_notional"].astype(float)
    weights = scoring["fit_weight"].astype(float)
    reference_notional = scoring["reference_notional"].astype(float).replace(0, np.nan)
    error_bps = (predicted - actual) / reference_notional * 10000.0
    abs_error_bps = error_bps.abs()
    mae = _weighted_mean(abs_error_bps, weights)
    rmse = _weighted_rmse(error_bps, weights)
    bias = _weighted_mean(error_bps, weights)
    weighted_mape = _weighted_mape(predicted, actual, weights)
    return {
        "mae_bps": mae,
        "rmse_bps": rmse,
        "bias_bps": bias,
        "weighted_mape": weighted_mape,
        "observations": int(len(scoring)),
    }


def _bias_not_materially_worse(current_bias: float | None, candidate_bias: float | None) -> bool:
    if current_bias is None or candidate_bias is None:
        return False
    current_abs = abs(float(current_bias))
    candidate_abs = abs(float(candidate_bias))
    if current_abs <= 1e-12:
        return candidate_abs <= 0.5
    return candidate_abs <= current_abs * 1.1


def _render_markdown_table(headers: list[str], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows._"
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        values = []
        for header in headers:
            value = row.get(header)
            if value is None or (isinstance(value, float) and math.isnan(value)):
                values.append("N/A")
            elif isinstance(value, float):
                values.append(f"{value:.6g}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _bucket_summary(
    dataset: pd.DataFrame,
    *,
    group_name: str,
    label_series: pd.Series,
    current_k: float,
    candidate_k: float | None,
    alpha: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped = dataset.assign(bucket_label=label_series)
    for bucket_label, frame in grouped.groupby("bucket_label", dropna=False):
        current_score = score_slippage_model(frame, k=current_k, alpha=alpha)
        candidate_score = score_slippage_model(frame, k=candidate_k, alpha=alpha) if candidate_k is not None else {
            "mae_bps": None,
            "rmse_bps": None,
            "bias_bps": None,
            "weighted_mape": None,
            "observations": 0,
        }
        rows.append(
            {
                group_name: str(bucket_label),
                "sample_count": int(len(frame)),
                "filled_order_count": int(frame["is_filled"].sum()),
                "positive_signal_count": int(frame["positive_signal"].sum()),
                "negative_signal_count": int(frame["negative_signal"].sum()),
                "filled_qty_sum": float(frame["filled_qty"].sum()),
                "filled_notional_sum": float(frame["filled_notional"].fillna(0.0).sum()),
                "mae_bps_current": current_score["mae_bps"],
                "mae_bps_candidate": candidate_score["mae_bps"],
                "rmse_bps_current": current_score["rmse_bps"],
                "rmse_bps_candidate": candidate_score["rmse_bps"],
                "bias_bps_current": current_score["bias_bps"],
                "bias_bps_candidate": candidate_score["bias_bps"],
                "weighted_mape_current": current_score["weighted_mape"],
                "weighted_mape_candidate": candidate_score["weighted_mape"],
            }
        )
    return sorted(rows, key=lambda item: str(item[group_name]))


def render_slippage_calibration_report(
    *,
    summary: dict[str, Any],
    collection_manifest_rows: pd.DataFrame,
    dataset: pd.DataFrame,
    residuals: pd.DataFrame,
    current_score: dict[str, Any],
    candidate_score: dict[str, Any],
    side_buckets: list[dict[str, Any]],
    participation_buckets: list[dict[str, Any]],
    notional_buckets: list[dict[str, Any]],
    source_run_root: Path | None,
) -> str:
    """Render an auditable markdown report for the calibration run."""

    def fmt(value: Any, suffix: str = "") -> str:
        if value is None:
            return "N/A"
        if isinstance(value, float) and math.isnan(value):
            return "N/A"
        if isinstance(value, float):
            return f"{value:.6g}{suffix}"
        return f"{value}{suffix}"

    lines = [
        "# Slippage Calibration Report",
        "",
        f"- generated_at: {_utc_now_iso()}",
        f"- recommendation: {summary.get('recommendation', 'N/A')}",
        f"- recommendation_reason: {summary.get('recommendation_reason', 'N/A')}",
        f"- overlay_readiness: {summary.get('overlay_readiness', 'N/A')}",
        f"- overlay_readiness_reason: {summary.get('overlay_readiness_reason', 'N/A')}",
        f"- next_recommended_action: {summary.get('next_recommended_action', 'N/A')}",
        f"- participation_range_note: {summary.get('participation_range_note', 'N/A')}",
        f"- alpha_used: {fmt(summary.get('alpha_used'))}",
        f"- alpha_candidate: {fmt(summary.get('alpha_candidate'))}",
        f"- source_run_root: {str(source_run_root) if source_run_root is not None else 'N/A'}",
        "",
        "## Data Source Summary",
        "",
        f"- fill_collection_dirs: {0 if collection_manifest_rows.empty else len(collection_manifest_rows)}",
        f"- total_orders_loaded: {summary.get('sample_count', 0)}",
        f"- filled_orders: {summary.get('filled_order_count', 0)}",
        f"- partial_count: {summary.get('partial_count', 0)}",
        f"- timeout_cancelled_count: {summary.get('timeout_cancelled_count', 0)}",
        "",
        _render_markdown_table(
            [
                "collection_run_dir",
                "collection_run_id",
                "collection_source_path",
                "order_count",
                "filled_count",
                "partial_count",
                "timeout_cancelled_count",
            ],
            collection_manifest_rows.to_dict(orient="records"),
        ),
        "",
        "## Calibration Data Quality",
        "",
        f"- filled_order_count: {summary.get('filled_order_count', 0)}",
        f"- positive_signal_count: {summary.get('positive_signal_count', 0)}",
        f"- negative_signal_count: {summary.get('negative_signal_count', 0)}",
        f"- missing_adv_count: {summary.get('missing_adv_count', 0)}",
        f"- missing_reference_price_count: {summary.get('missing_reference_price_count', 0)}",
        f"- positive_signal_share: {fmt(summary.get('positive_signal_share'))}",
        f"- participation_span: {fmt(summary.get('participation_span'), '%')}",
        f"- insufficient_positive_signal: {summary.get('insufficient_positive_signal', False)}",
        "",
        "### Fit Eligibility Breakdown",
        "",
        _render_markdown_table(
            ["fit_reason", "count"],
            _counts_to_rows("fit_reason", summary.get("fit_reason_counts", {})),
        ),
        "",
        "### Status Coverage",
        "",
        _render_markdown_table(
            ["status", "count"],
            _counts_to_rows("status", summary.get("status_counts", {})),
        ),
        "",
        "### Side Coverage",
        "",
        _render_markdown_table(
            ["side", "total_orders", "eligible_orders", "positive_signal_orders"],
            [
                {
                    "side": side,
                    "total_orders": summary.get("side_counts", {}).get(side, 0),
                    "eligible_orders": summary.get("eligible_side_counts", {}).get(side, 0),
                    "positive_signal_orders": summary.get("positive_signal_side_counts", {}).get(side, 0),
                }
                for side in sorted(
                    set(summary.get("side_counts", {}))
                    | set(summary.get("eligible_side_counts", {}))
                    | set(summary.get("positive_signal_side_counts", {}))
                )
            ],
        ),
        "",
        "## Model Fit Summary",
        "",
        f"- current_k: {fmt(summary.get('current_k'))}",
        f"- candidate_k: {fmt(summary.get('candidate_k'))}",
        f"- alpha_used: {fmt(summary.get('alpha_used'))}",
        f"- current_mae_bps: {fmt(current_score.get('mae_bps'))}",
        f"- candidate_mae_bps: {fmt(candidate_score.get('mae_bps'))}",
        f"- current_rmse_bps: {fmt(current_score.get('rmse_bps'))}",
        f"- candidate_rmse_bps: {fmt(candidate_score.get('rmse_bps'))}",
        f"- current_bias_bps: {fmt(current_score.get('bias_bps'))}",
        f"- candidate_bias_bps: {fmt(candidate_score.get('bias_bps'))}",
        f"- current_weighted_mape: {fmt(current_score.get('weighted_mape'), '%')}",
        f"- candidate_weighted_mape: {fmt(candidate_score.get('weighted_mape'), '%')}",
        "",
        "### Error Comparison",
        "",
        _render_markdown_table(
            ["metric", "current", "candidate"],
            [
                {"metric": "mae_bps", "current": current_score.get("mae_bps"), "candidate": candidate_score.get("mae_bps")},
                {"metric": "rmse_bps", "current": current_score.get("rmse_bps"), "candidate": candidate_score.get("rmse_bps")},
                {"metric": "bias_bps", "current": current_score.get("bias_bps"), "candidate": candidate_score.get("bias_bps")},
                {
                    "metric": "weighted_mape",
                    "current": current_score.get("weighted_mape"),
                    "candidate": candidate_score.get("weighted_mape"),
                },
            ],
        ),
        "",
        "## Side Buckets",
        "",
        _render_markdown_table(
            [
                "side",
                "sample_count",
                "filled_order_count",
                "positive_signal_count",
                "negative_signal_count",
                "mae_bps_current",
                "mae_bps_candidate",
                "weighted_mape_current",
                "weighted_mape_candidate",
            ],
            side_buckets,
        ),
        "",
        "## Participation Buckets",
        "",
        _render_markdown_table(
            [
                "participation_bucket",
                "sample_count",
                "filled_order_count",
                "positive_signal_count",
                "negative_signal_count",
                "filled_notional_sum",
                "mae_bps_current",
                "mae_bps_candidate",
            ],
            participation_buckets,
        ),
        "",
        "## Notional Buckets",
        "",
        _render_markdown_table(
            [
                "notional_bucket",
                "sample_count",
                "filled_order_count",
                "positive_signal_count",
                "negative_signal_count",
                "filled_notional_sum",
                "mae_bps_current",
                "mae_bps_candidate",
            ],
            notional_buckets,
        ),
        "",
        "## Decision",
        "",
        f"- recommendation: {summary.get('recommendation', 'N/A')}",
        f"- recommendation_reason: {summary.get('recommendation_reason', 'N/A')}",
        f"- overlay_readiness: {summary.get('overlay_readiness', 'N/A')}",
        f"- overlay_readiness_reason: {summary.get('overlay_readiness_reason', 'N/A')}",
        f"- next_recommended_action: {summary.get('next_recommended_action', 'N/A')}",
        f"- participation_range_note: {summary.get('participation_range_note', 'N/A')}",
        f"- sufficient_filled_orders: {summary.get('sufficient_filled_orders', False)}",
        f"- sufficient_participation_span: {summary.get('sufficient_participation_span', False)}",
        f"- sufficient_low_participation_coverage: {summary.get('sufficient_low_participation_coverage', False)}",
        f"- bidirectional_fit_coverage: {summary.get('bidirectional_fit_coverage', False)}",
        f"- sufficient_positive_signal: {summary.get('sufficient_positive_signal', False)}",
        f"- positive_signal_count: {summary.get('positive_signal_count', 0)}",
        f"- fit_sample_count: {summary.get('fit_sample_count', 0)}",
        "",
        "## Residuals Snapshot",
        "",
        _render_markdown_table(
            [
                "sample_id",
                "ticker",
                "direction",
                "status",
                "fit_reason",
                "positive_signal",
                "realized_slippage_notional",
                "current_residual_bps",
                "candidate_residual_bps",
            ],
            residuals.head(10).to_dict(orient="records"),
        ),
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def _load_current_slippage_config() -> SlippageConfig:
    candidates = [ROOT / "config" / "us_default.yaml", ROOT / "config" / "default.yaml"]
    for path in candidates:
        if path.exists():
            payload = load_yaml_file(path)
            slippage_payload = payload.get("slippage") or {}
            if isinstance(slippage_payload, dict):
                return SlippageConfig.model_validate(slippage_payload)
    return SlippageConfig(k=0.02)


def _build_residuals_frame(dataset: pd.DataFrame, *, current_k: float, candidate_k: float | None, alpha: float) -> pd.DataFrame:
    work = dataset.copy()
    if work.empty:
        return work
    scale = work["reference_price"].astype(float) * work["filled_qty"].astype(float) * np.power(
        work["filled_qty"].astype(float) / work["source_adv_shares"].astype(float),
        float(alpha),
    )
    work["current_predicted_slippage_notional"] = np.where(work["fit_eligible"], scale * current_k, np.nan)
    work["candidate_predicted_slippage_notional"] = np.where(
        work["fit_eligible"] & (candidate_k is not None),
        scale * float(candidate_k or 0.0),
        np.nan,
    )
    work["current_residual_notional"] = work["current_predicted_slippage_notional"] - work["positive_realized_slippage_notional"]
    work["candidate_residual_notional"] = work["candidate_predicted_slippage_notional"] - work["positive_realized_slippage_notional"]
    work["current_residual_bps"] = np.where(
        work["reference_notional"] > 0,
        work["current_residual_notional"] / work["reference_notional"] * 10000.0,
        np.nan,
    )
    work["candidate_residual_bps"] = np.where(
        work["reference_notional"] > 0,
        work["candidate_residual_notional"] / work["reference_notional"] * 10000.0,
        np.nan,
    )
    work["participation_bucket"] = work["participation_pct"].apply(_bucket_participation)
    work["notional_bucket"] = work["filled_notional"].apply(_bucket_notional)
    return work


def calibrate_slippage(
    *,
    fill_collection_root: Path,
    output_dir: Path,
    source_run_root: Path | None = None,
    alpha: float = 0.6,
    min_filled_orders: int = 20,
    min_participation_span: float = 10.0,
    update_default_config: bool = False,
) -> SlippageCalibrationResult:
    """Run the full slippage calibration workflow and return all artifact payloads."""

    collection_orders, collection_manifest_rows = load_fill_collection(fill_collection_root)
    dataset, dataset_summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_collection_root,
        source_run_root=source_run_root,
        alpha=alpha,
    )
    current_config = _load_current_slippage_config()
    current_k = float(current_config.k)
    fit_summary = fit_slippage_k(
        dataset,
        alpha=alpha,
        current_k=current_k,
        min_filled_orders=min_filled_orders,
        min_participation_span=min_participation_span,
    )
    candidate_k = fit_summary.get("candidate_k")
    current_score = score_slippage_model(dataset, k=current_k, alpha=alpha)
    candidate_score = score_slippage_model(dataset, k=candidate_k, alpha=alpha) if candidate_k is not None else {
        "mae_bps": None,
        "rmse_bps": None,
        "bias_bps": None,
        "weighted_mape": None,
        "observations": 0,
    }

    enough_orders = bool(fit_summary.get("enough_orders"))
    enough_span = bool(fit_summary.get("enough_span"))
    positive_signal_ok = bool(
        fit_summary.get("positive_signal_share") is None or float(fit_summary.get("positive_signal_share")) >= 0.25
    )
    bias_ok = _bias_not_materially_worse(current_score.get("bias_bps"), candidate_score.get("bias_bps"))
    metrics_improved = (
        candidate_score.get("mae_bps") is not None
        and current_score.get("mae_bps") is not None
        and candidate_score.get("weighted_mape") is not None
        and current_score.get("weighted_mape") is not None
        and candidate_score.get("mae_bps") < current_score.get("mae_bps")
        and candidate_score.get("weighted_mape") < current_score.get("weighted_mape")
        and bias_ok
    )
    side_buckets = _bucket_summary(
        dataset.loc[dataset["fit_eligible"]],
        group_name="side",
        label_series=dataset.loc[dataset["fit_eligible"], "direction"].astype(str).str.lower().replace({"": "missing"}),
        current_k=current_k,
        candidate_k=candidate_k,
        alpha=alpha,
    )
    participation_buckets = _bucket_summary(
        dataset.loc[dataset["fit_eligible"]],
        group_name="participation_bucket",
        label_series=dataset.loc[dataset["fit_eligible"], "participation_pct"].apply(_bucket_participation),
        current_k=current_k,
        candidate_k=candidate_k,
        alpha=alpha,
    )
    notional_buckets = _bucket_summary(
        dataset.loc[dataset["fit_eligible"]],
        group_name="notional_bucket",
        label_series=dataset.loc[dataset["fit_eligible"], "filled_notional"].apply(_bucket_notional),
        current_k=current_k,
        candidate_k=candidate_k,
        alpha=alpha,
    )

    eligible_side_counts = _masked_side_counts(dataset, dataset["fit_eligible"]) if not dataset.empty else {}
    positive_signal_side_counts = _masked_side_counts(dataset, dataset["positive_signal"]) if not dataset.empty else {}
    fit_sample_count = int(fit_summary.get("fit_sample_count") or 0)
    fit_eligible_count = int(dataset_summary.get("fit_eligible_count") or 0)
    bidirectional_fit_coverage = all(int(eligible_side_counts.get(side, 0)) > 0 for side in ("buy", "sell"))
    observed_fit_participation_buckets = {
        str(row.get("participation_bucket", "")).strip()
        for row in participation_buckets
        if int(row.get("sample_count", 0) or 0) > 0
    }
    low_participation_only = bool(observed_fit_participation_buckets) and observed_fit_participation_buckets <= {"0-0.1%"}
    low_participation_coverage_ok = bool(
        enough_orders
        and fit_eligible_count >= 30
        and bidirectional_fit_coverage
        and low_participation_only
    )
    if enough_orders and enough_span and candidate_k is not None and candidate_k > 0 and metrics_improved and positive_signal_ok:
        recommendation = "recommend_default_update"
        recommendation_reason = "all_guardrails_satisfied"
    elif low_participation_coverage_ok and candidate_k is not None and candidate_k > 0 and metrics_improved and positive_signal_ok:
        recommendation = "provisional_only"
        recommendation_reason = "low_participation_overlay_only"
    elif not enough_orders or ((not enough_span) and not low_participation_coverage_ok) or candidate_k is None or candidate_k <= 0:
        recommendation = "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE"
        if not enough_orders:
            recommendation_reason = "insufficient_filled_order_count"
        elif (not enough_span) and not low_participation_coverage_ok:
            recommendation_reason = "insufficient_participation_span"
        elif candidate_k is None or candidate_k <= 0:
            recommendation_reason = "candidate_k_not_positive"
        else:
            recommendation_reason = fit_summary.get("recommendation_reason", "insufficient_data")
    elif not positive_signal_ok:
        recommendation = "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE"
        recommendation_reason = "insufficient_positive_signal"
    else:
        recommendation = "provisional_only"
        recommendation_reason = "candidate_available_but_not_better_or_bias_worse"

    overlay_readiness, next_recommended_action, overlay_readiness_reason = _resolve_overlay_decision(
        candidate_k=candidate_k,
        fit_sample_count=fit_sample_count,
        enough_orders=enough_orders,
        enough_span=enough_span,
        low_participation_coverage_ok=low_participation_coverage_ok,
        positive_signal_ok=positive_signal_ok,
        metrics_improved=bool(metrics_improved),
        update_default_config=bool(update_default_config),
    )

    if low_participation_coverage_ok:
        participation_range_note = (
            "calibration validated only for 0-0.1% participation; "
            "do not extrapolate to higher-participation scenarios"
        )
    elif enough_span:
        participation_range_note = (
            "calibration validated across the observed participation span; "
            "re-calibrate before extrapolating beyond the observed sample distribution"
        )
    else:
        participation_range_note = (
            "calibration coverage remains narrower than the broad participation-span guardrail; "
            "do not extrapolate beyond the observed participation bucket coverage"
        )

    residuals = _build_residuals_frame(dataset, current_k=current_k, candidate_k=candidate_k, alpha=alpha)

    summary = {
        **dataset_summary,
        **fit_summary,
        **current_score,
        **{f"candidate_{key}": value for key, value in candidate_score.items()},
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
        "overlay_readiness": overlay_readiness,
        "overlay_readiness_reason": overlay_readiness_reason,
        "next_recommended_action": next_recommended_action,
        "sufficient_filled_orders": enough_orders,
        "sufficient_participation_span": enough_span,
        "sufficient_low_participation_coverage": low_participation_coverage_ok,
        "sufficient_positive_signal": positive_signal_ok,
        "current_k": current_k,
        "candidate_k": candidate_k,
        "alpha_used": float(alpha),
        "update_default_config_requested": bool(update_default_config),
        "fit_reason_counts": _normalized_count_map(dataset.get("fit_reason")),
        "status_counts": _normalized_count_map(dataset.get("status")),
        "side_counts": _normalized_count_map(dataset.get("direction")),
        "eligible_side_counts": eligible_side_counts,
        "positive_signal_side_counts": positive_signal_side_counts,
        "bidirectional_fit_coverage": bidirectional_fit_coverage,
        "participation_range_note": participation_range_note,
        "coverage_by_side": side_buckets,
        "coverage_by_participation_bucket": participation_buckets,
        "coverage_by_notional_bucket": notional_buckets,
    }

    positive_share = summary.get("positive_signal_share")
    summary["insufficient_positive_signal"] = bool(positive_share is not None and float(positive_share) < 0.25)
    summary["sufficient_positive_signal"] = positive_signal_ok
    summary["mae_bps_current"] = current_score.get("mae_bps")
    summary["mae_bps_candidate"] = candidate_score.get("mae_bps")
    summary["rmse_bps_current"] = current_score.get("rmse_bps")
    summary["rmse_bps_candidate"] = candidate_score.get("rmse_bps")
    summary["bias_bps_current"] = current_score.get("bias_bps")
    summary["bias_bps_candidate"] = candidate_score.get("bias_bps")
    summary["weighted_mape_current"] = current_score.get("weighted_mape")
    summary["weighted_mape_candidate"] = candidate_score.get("weighted_mape")

    report_markdown = render_slippage_calibration_report(
        summary=summary,
        collection_manifest_rows=collection_manifest_rows,
        dataset=dataset,
        residuals=residuals,
        current_score=current_score,
        candidate_score=candidate_score,
        side_buckets=side_buckets,
        participation_buckets=participation_buckets,
        notional_buckets=notional_buckets,
        source_run_root=Path(source_run_root).resolve() if source_run_root is not None else None,
    )
    overlay_payload = {"slippage": {"k": candidate_k, "alpha": float(alpha)}}
    diagnostic_manifest = {
        "generated_at": _utc_now_iso(),
        "fill_collection_root": str(fill_collection_root.resolve()),
        "source_run_root": str(Path(source_run_root).resolve()) if source_run_root is not None else None,
        "collection_run_ids": [str(value) for value in collection_manifest_rows.get("collection_run_id", pd.Series(dtype=str)).dropna().tolist()],
        "collection_run_dirs": [str(value) for value in collection_manifest_rows.get("collection_run_dir", pd.Series(dtype=str)).dropna().tolist()],
        "sample_count": summary.get("sample_count", 0),
        "filled_order_count": summary.get("filled_order_count", 0),
        "fit_sample_count": summary.get("fit_sample_count", 0),
        "candidate_k": candidate_k,
        "current_k": current_k,
        "alpha_used": float(alpha),
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
        "overlay_readiness": overlay_readiness,
        "overlay_readiness_reason": overlay_readiness_reason,
        "next_recommended_action": next_recommended_action,
        "participation_range_note": participation_range_note,
        "fit_reason_counts": summary.get("fit_reason_counts", {}),
        "status_counts": summary.get("status_counts", {}),
        "side_counts": summary.get("side_counts", {}),
        "eligible_side_counts": summary.get("eligible_side_counts", {}),
        "positive_signal_side_counts": summary.get("positive_signal_side_counts", {}),
        "bidirectional_fit_coverage": bidirectional_fit_coverage,
        "sufficient_low_participation_coverage": low_participation_coverage_ok,
        "coverage_by_side": summary.get("coverage_by_side", []),
        "coverage_by_participation_bucket": summary.get("coverage_by_participation_bucket", []),
        "coverage_by_notional_bucket": summary.get("coverage_by_notional_bucket", []),
        "output_dir": str(output_dir),
        "update_default_config_requested": bool(update_default_config),
    }
    return SlippageCalibrationResult(
        dataset=dataset,
        residuals=residuals,
        summary=summary,
        report_markdown=report_markdown,
        overlay_payload=overlay_payload,
        diagnostic_manifest=diagnostic_manifest,
    )


def write_slippage_calibration_artifacts(
    *,
    result: SlippageCalibrationResult,
    output_dir: Path,
) -> dict[str, Path]:
    """Persist one calibration run to disk."""

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "slippage_calibration_report.md"
    summary_path = output_dir / "slippage_calibration.json"
    dataset_path = output_dir / "slippage_calibration_dataset.csv"
    overlay_path = output_dir / "slippage_candidate_overlay.yaml"
    residuals_path = output_dir / "slippage_residuals.csv"
    diagnostic_manifest_path = output_dir / "diagnostic_manifest.json"

    report_path.write_text(result.report_markdown, encoding="utf-8")
    summary_path.write_text(json.dumps(result.summary, ensure_ascii=False, indent=2), encoding="utf-8")
    result.dataset.to_csv(dataset_path, index=False, encoding="utf-8")
    result.residuals.to_csv(residuals_path, index=False, encoding="utf-8")
    overlay_path.write_text(yaml.safe_dump(result.overlay_payload, sort_keys=False, allow_unicode=False), encoding="utf-8")
    diagnostic_manifest_path.write_text(
        json.dumps(result.diagnostic_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "slippage_calibration_report": report_path,
        "slippage_calibration_json": summary_path,
        "slippage_calibration_dataset": dataset_path,
        "slippage_candidate_overlay": overlay_path,
        "slippage_residuals": residuals_path,
        "diagnostic_manifest": diagnostic_manifest_path,
    }
