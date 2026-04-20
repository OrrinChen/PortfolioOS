from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from portfolio_os.execution.slippage_calibration import (
    build_slippage_calibration_dataset,
    calibrate_slippage,
    create_synthetic_slippage_calibration_fixture,
    prepare_slippage_calibration_prep,
    write_slippage_calibration_artifacts,
)


def _write_source_run_root(
    base_dir: Path,
    *,
    sample_id: str,
    rows: list[dict[str, object]],
    market_rows: list[dict[str, object]],
) -> Path:
    source_root = base_dir / "pilot_validation_source"
    sample_dir = source_root / "samples" / sample_id
    main_dir = sample_dir / "main"
    main_dir.mkdir(parents=True, exist_ok=True)

    market_path = base_dir / "market.csv"
    pd.DataFrame(market_rows).to_csv(market_path, index=False, encoding="utf-8")
    pd.DataFrame(rows).to_csv(main_dir / "orders.csv", index=False, encoding="utf-8")
    (main_dir / "audit.json").write_text(
        json.dumps({"inputs": {"market": {"path": str(market_path)}}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return source_root


def _write_fill_collection_run(
    base_dir: Path,
    *,
    run_name: str,
    source_root: Path,
    rows: list[dict[str, object]],
    manifest_overrides: dict[str, object] | None = None,
) -> Path:
    run_dir = base_dir / "alpaca_fill_collection" / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_name,
        "created_at": "2026-03-26T16:06:00+00:00",
        "market": "us",
        "broker": "alpaca",
        "notes": "synthetic calibration fixture",
        "source_type": "run_root",
        "source_path": str(source_root),
        "order_count": len(rows),
        "submitted_count": sum(1 for row in rows if float(row.get("filled_qty", 0) or 0) > 0 or str(row.get("status", "")).lower() in {"timeout_cancelled", "rejected"}),
        "filled_count": sum(1 for row in rows if float(row.get("filled_qty", 0) or 0) > 0),
        "partial_count": sum(1 for row in rows if str(row.get("status", "")).lower() == "partially_filled"),
        "unfilled_count": sum(1 for row in rows if float(row.get("filled_qty", 0) or 0) <= 0),
        "rejected_count": sum(1 for row in rows if str(row.get("status", "")).lower() == "rejected"),
        "timeout_cancelled_count": sum(1 for row in rows if str(row.get("status", "")).lower() == "timeout_cancelled"),
        "avg_fill_price_mean": None,
        "has_any_filled_orders": any(float(row.get("filled_qty", 0) or 0) > 0 for row in rows),
        "event_granularity": "polled_history",
    }
    if manifest_overrides:
        manifest.update(manifest_overrides)
    (run_dir / "alpaca_fill_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame(rows).to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")
    pd.DataFrame([]).to_csv(run_dir / "alpaca_fill_events.csv", index=False, encoding="utf-8")
    return run_dir


def _build_synthetic_fixtures(
    base_dir: Path,
    *,
    positive_count: int = 24,
    negative_count: int = 4,
    include_missing_adv: bool = True,
    include_timeout: bool = True,
) -> tuple[Path, Path, list[dict[str, object]]]:
    sample_id = "sample_us_01"
    alpha = 0.6
    k_true = 0.02
    source_rows: list[dict[str, object]] = []
    market_rows: list[dict[str, object]] = []
    fill_rows: list[dict[str, object]] = []

    for index in range(positive_count):
        ticker = f"T{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 100.0 + 100.0 * index
        adv = 10000.0 + 500.0 * index
        reference_price = 100.0 + float(index)
        slippage_notional = reference_price * k_true * qty * ((qty / adv) ** alpha)
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

    for index in range(negative_count):
        ticker = f"N{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 250.0 + 25.0 * index
        adv = 15000.0 + 250.0 * index
        reference_price = 200.0 + float(index)
        slippage_notional = reference_price * k_true * qty * ((qty / adv) ** alpha)
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

    source_root = _write_source_run_root(
        base_dir,
        sample_id=sample_id,
        rows=source_rows,
        market_rows=market_rows,
    )
    fill_root = base_dir / "alpaca_fill_collection"
    _write_fill_collection_run(
        base_dir,
        run_name="run_root_20260326T160600_e0c9c68f",
        source_root=source_root,
        rows=fill_rows,
    )
    return fill_root, source_root, fill_rows


def _build_low_participation_dense_fixtures(
    base_dir: Path,
    *,
    positive_count: int = 30,
    negative_count: int = 4,
) -> tuple[Path, Path]:
    sample_id = "low_participation_sample"
    alpha = 0.6
    k_true = 0.02
    participation_ratio = 0.0005  # 0.05%
    source_rows: list[dict[str, object]] = []
    market_rows: list[dict[str, object]] = []
    fill_rows: list[dict[str, object]] = []

    for index in range(positive_count):
        ticker = f"LP{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 100.0 + float(index)
        adv = qty / participation_ratio
        reference_price = 50.0 + float(index)
        slippage_notional = reference_price * k_true * qty * ((qty / adv) ** alpha)
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
                "broker_order_id": f"lp-pos-{index}",
                "submitted_at_utc": f"2026-04-01T14:00:{index:02d}+00:00",
                "terminal_at_utc": f"2026-04-01T14:00:{index + 1:02d}+00:00",
                "latency_seconds": 1.0,
                "poll_count": 2,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    for index in range(negative_count):
        ticker = f"LN{index:03d}"
        direction = "buy" if index % 2 == 0 else "sell"
        qty = 140.0 + float(index)
        adv = qty / participation_ratio
        reference_price = 90.0 + float(index)
        slippage_notional = reference_price * k_true * qty * ((qty / adv) ** alpha)
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
                "broker_order_id": f"lp-neg-{index}",
                "submitted_at_utc": f"2026-04-01T14:01:{index:02d}+00:00",
                "terminal_at_utc": f"2026-04-01T14:01:{index + 1:02d}+00:00",
                "latency_seconds": 1.0,
                "poll_count": 2,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        )

    source_root = _write_source_run_root(
        base_dir,
        sample_id=sample_id,
        rows=source_rows,
        market_rows=market_rows,
    )
    fill_root = base_dir / "alpaca_fill_collection"
    _write_fill_collection_run(
        base_dir,
        run_name="low_participation_run_20260401T140000_abcdef12",
        source_root=source_root,
        rows=fill_rows,
    )
    return fill_root, source_root


def test_build_dataset_recovers_source_fields_and_retains_negative_signals(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(tmp_path)

    dataset, summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_root,
        source_run_root=source_root,
        alpha=0.6,
    )

    assert summary["missing_adv_count"] == 1
    assert summary["missing_reference_price_count"] == 0
    assert summary["positive_signal_count"] > 0
    assert summary["negative_signal_count"] > 0
    assert "source_adv_shares" in dataset.columns
    assert "reference_price" in dataset.columns
    assert "realized_slippage_notional" in dataset.columns
    missing_adv_row = dataset.loc[dataset["ticker"] == "MISSING_ADV"].iloc[0]
    assert pd.isna(missing_adv_row["source_adv_shares"])
    assert not bool(missing_adv_row["fit_eligible"])
    negative_rows = dataset.loc[dataset["negative_signal"]]
    assert not negative_rows.empty
    assert not bool(negative_rows.iloc[0]["positive_signal"])


def test_build_dataset_prefers_reference_price_when_present(tmp_path: Path) -> None:
    source_root = tmp_path / "source_root"
    sample_main = source_root / "samples" / "sample_ref" / "main"
    sample_main.mkdir(parents=True, exist_ok=True)
    market_path = tmp_path / "market.csv"
    pd.DataFrame(
        [
            {
                "ticker": "UPST",
                "close": 25.0,
                "vwap": 25.0,
                "adv_shares": 2800000.0,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ]
    ).to_csv(market_path, index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "ticker": "UPST",
                "side": "BUY",
                "quantity": 300.0,
                "reference_price": 25.04,
                "estimated_price": 28.53,
                "estimated_notional": 8559.0,
            }
        ]
    ).to_csv(sample_main / "orders.csv", index=False, encoding="utf-8")
    (sample_main / "audit.json").write_text(
        json.dumps({"inputs": {"market": {"path": str(market_path)}}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    fill_root = tmp_path / "alpaca_fill_collection"
    run_dir = fill_root / "run_001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "alpaca_fill_manifest.json").write_text(
        json.dumps({"run_id": "run_001", "source_path": str(source_root), "market": "us", "broker": "alpaca"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "sample_id": "sample_ref",
                "ticker": "UPST",
                "direction": "buy",
                "requested_qty": 300.0,
                "filled_qty": 300.0,
                "avg_fill_price": 25.02,
                "reference_price": 25.04,
                "estimated_price": 28.53,
                "requested_notional": 8559.0,
                "filled_notional": 7506.0,
                "fill_ratio": 1.0,
                "status": "filled",
            }
        ]
    ).to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")

    dataset, summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_root,
        source_run_root=source_root,
        alpha=0.6,
    )

    assert float(dataset.loc[0, "reference_price"]) == pytest.approx(25.04)
    assert float(dataset.loc[0, "realized_slippage_notional"]) == pytest.approx(-6.0)
    assert summary["negative_signal_count"] == 1


def test_synthetic_sample_recovers_candidate_k_and_writes_artifacts(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(tmp_path)
    output_dir = tmp_path / "calibration_output"
    result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=output_dir,
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )

    assert result.summary["candidate_k"] == pytest.approx(0.02, rel=1e-6)
    assert result.summary["mae_bps_candidate"] < result.summary["mae_bps_current"]
    assert result.summary["weighted_mape_candidate"] < result.summary["weighted_mape_current"]
    assert result.summary["recommendation"] == "recommend_default_update"
    assert result.summary["negative_signal_count"] > 0
    assert result.summary["fit_sample_count"] == result.summary["positive_signal_count"]

    run_dir = output_dir / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = write_slippage_calibration_artifacts(result=result, output_dir=run_dir)
    for path in artifact_paths.values():
        assert path.exists()

    summary_payload = json.loads((run_dir / "slippage_calibration.json").read_text(encoding="utf-8"))
    overlay_payload = yaml.safe_load((run_dir / "slippage_candidate_overlay.yaml").read_text(encoding="utf-8"))
    diagnostic_payload = json.loads((run_dir / "diagnostic_manifest.json").read_text(encoding="utf-8"))
    assert summary_payload["candidate_k"] == pytest.approx(0.02, rel=1e-6)
    assert summary_payload["overlay_readiness"] == "sufficient"
    assert summary_payload["next_recommended_action"] == "apply_as_paper_overlay"
    assert overlay_payload["slippage"]["k"] == pytest.approx(0.02, rel=1e-6)
    assert diagnostic_payload["overlay_readiness"] == "sufficient"
    assert diagnostic_payload["next_recommended_action"] == "apply_as_paper_overlay"


def test_missing_adv_is_counted_and_excluded_from_fit(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(tmp_path, include_missing_adv=True)
    dataset, summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_root,
        source_run_root=source_root,
        alpha=0.6,
    )

    assert summary["missing_adv_count"] == 1
    assert not bool(dataset.loc[dataset["ticker"] == "MISSING_ADV", "fit_eligible"].iloc[0])
    fit_result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=tmp_path / "output",
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )
    assert fit_result.summary["candidate_k"] == pytest.approx(0.02, rel=1e-6)
    assert fit_result.summary["missing_adv_count"] == 1


def test_samples_below_threshold_are_marked_insufficient(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(
        tmp_path,
        positive_count=2,
        negative_count=1,
        include_missing_adv=False,
        include_timeout=False,
    )
    result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=tmp_path / "small_output",
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )

    assert result.summary["recommendation"] == "INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE"
    assert result.summary["fit_sample_count"] < 20
    assert result.summary["candidate_k"] is not None
    assert result.summary["overlay_readiness"] == "directional_only"
    assert result.summary["next_recommended_action"] == "collect_more_fills"


def test_negative_signal_samples_are_reported_but_not_used_for_main_fit(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(tmp_path)
    result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=tmp_path / "negative_signal_output",
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )

    negative_rows = result.dataset.loc[result.dataset["negative_signal"]]
    assert not negative_rows.empty
    assert result.summary["negative_signal_count"] == len(negative_rows)
    assert result.summary["fit_sample_count"] == result.summary["positive_signal_count"]


def test_calibrate_slippage_exposes_machine_readable_tca_summary(tmp_path: Path) -> None:
    fill_root, source_root, _rows = _build_synthetic_fixtures(tmp_path)

    result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=tmp_path / "tca_output",
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )

    assert result.summary["overlay_readiness"] == "sufficient"
    assert result.summary["next_recommended_action"] == "apply_as_paper_overlay"
    assert result.summary["fit_reason_counts"]["eligible"] >= 1
    assert result.summary["status_counts"]["filled"] >= 1
    assert result.summary["side_counts"]["buy"] >= 1
    assert result.summary["eligible_side_counts"]["buy"] >= 1
    assert result.summary["positive_signal_side_counts"]["buy"] >= 1
    assert isinstance(result.summary["coverage_by_participation_bucket"], list)
    assert isinstance(result.summary["coverage_by_notional_bucket"], list)
    assert result.diagnostic_manifest["overlay_readiness"] == "sufficient"
    assert result.diagnostic_manifest["next_recommended_action"] == "apply_as_paper_overlay"
    assert "overlay_readiness" in result.report_markdown
    assert "next_recommended_action" in result.report_markdown


def test_low_participation_dense_coverage_can_be_sufficient_without_span(tmp_path: Path) -> None:
    fill_root, source_root = _build_low_participation_dense_fixtures(
        tmp_path,
        positive_count=16,
        negative_count=16,
    )

    result = calibrate_slippage(
        fill_collection_root=fill_root,
        output_dir=tmp_path / "low_participation_output",
        source_run_root=source_root,
        alpha=0.6,
        min_filled_orders=20,
        min_participation_span=10.0,
        update_default_config=False,
    )

    assert result.summary["fit_eligible_count"] >= 30
    assert result.summary["fit_sample_count"] < 30
    assert result.summary["sufficient_participation_span"] is False
    assert result.summary["sufficient_low_participation_coverage"] is True
    assert result.summary["bidirectional_fit_coverage"] is True
    assert result.summary["overlay_readiness"] == "sufficient"
    assert result.summary["next_recommended_action"] == "apply_as_paper_overlay"
    assert result.summary["recommendation"] == "provisional_only"
    assert result.summary["recommendation_reason"] == "low_participation_overlay_only"
    assert "0-0.1%" in result.summary["participation_range_note"]
    assert result.diagnostic_manifest["overlay_readiness"] == "sufficient"
    assert "0-0.1%" in result.diagnostic_manifest["participation_range_note"]
    assert "participation_range_note" in result.report_markdown


def test_prepare_slippage_calibration_prep_writes_skeleton(tmp_path: Path) -> None:
    prep_paths = prepare_slippage_calibration_prep(output_dir=tmp_path / "prep")

    manifest_path = prep_paths["slippage_calibration_prep_manifest"]
    checklist_path = prep_paths["slippage_calibration_prep_checklist"]
    assert manifest_path.exists()
    assert checklist_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    checklist = checklist_path.read_text(encoding="utf-8")
    assert manifest["status"] == "prep_only"
    assert "dataset_path" in manifest
    assert "candidate_overlay_path" in manifest
    assert "Slippage Calibration Prep Checklist" in checklist


def test_create_synthetic_fixture_builds_offline_inputs_and_expected_k(tmp_path: Path) -> None:
    fixture = create_synthetic_slippage_calibration_fixture(
        output_dir=tmp_path / "synthetic_fixture",
        positive_count=12,
        negative_count=3,
        include_missing_adv=True,
        include_timeout=True,
        true_k=0.023,
        alpha=0.6,
    )

    assert fixture.fill_collection_root.exists()
    assert fixture.source_run_root.exists()
    assert fixture.manifest_path.exists()
    manifest = json.loads(fixture.manifest_path.read_text(encoding="utf-8"))
    assert manifest["expected_k"] == pytest.approx(0.023)
    assert manifest["positive_count"] == 12
    assert manifest["negative_count"] == 3

    result = calibrate_slippage(
        fill_collection_root=fixture.fill_collection_root,
        output_dir=tmp_path / "synthetic_output",
        source_run_root=fixture.source_run_root,
        alpha=fixture.alpha,
        min_filled_orders=10,
        min_participation_span=5.0,
        update_default_config=False,
    )

    assert result.summary["candidate_k"] == pytest.approx(0.023, rel=1e-6)


def test_build_dataset_reads_bom_json_audit_and_recovers_adv(tmp_path: Path) -> None:
    sample_id = "bom_json_sample"
    source_root = tmp_path / "source_root"
    sample_main = source_root / "samples" / sample_id / "main"
    sample_main.mkdir(parents=True, exist_ok=True)
    market_path = tmp_path / "market.csv"
    pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "close": 100.0,
                "vwap": 100.0,
                "adv_shares": 62000000.0,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            }
        ]
    ).to_csv(market_path, index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "side": "SELL",
                "quantity": 1.0,
                "estimated_price": 100.0,
                "estimated_notional": 100.0,
            }
        ]
    ).to_csv(sample_main / "orders.csv", index=False, encoding="utf-8")
    (sample_main / "audit.json").write_text(
        json.dumps({"inputs": {"market": {"path": str(market_path)}}}, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )

    fill_root = tmp_path / "alpaca_fill_collection"
    run_dir = fill_root / "run_001"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "alpaca_fill_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "source_path": str(source_root),
                "market": "us",
                "broker": "alpaca",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "sample_id": sample_id,
                "ticker": "AAPL",
                "direction": "sell",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "avg_fill_price": 99.95,
                "estimated_price": 100.0,
                "requested_notional": 100.0,
                "filled_notional": 99.95,
                "fill_ratio": 1.0,
                "status": "filled",
            }
        ]
    ).to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")

    dataset, summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_root,
        source_run_root=source_root,
        alpha=0.6,
    )

    assert summary["missing_adv_count"] == 0
    assert float(dataset.loc[0, "source_adv_shares"]) == pytest.approx(62000000.0)


def test_build_dataset_recovers_adv_from_orders_oms_source_path_with_batch_sidecar(tmp_path: Path) -> None:
    fill_root = tmp_path / "alpaca_fill_collection"
    run_dir = fill_root / "run_001"
    source_dir = tmp_path / "generated_batch"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "sample_id": "fill_collection_batch",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 10.0,
                "reference_price": 100.0,
                "estimated_price": 100.0,
                "price_limit": "",
                "extended_hours": False,
            }
        ]
    ).to_csv(source_dir / "orders_oms.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "sample_id": "fill_collection_batch",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 10.0,
                "reference_price": 100.0,
                "estimated_price": 100.0,
                "adv_shares": 62000000.0,
                "estimated_notional": 1000.0,
                "target_participation_bucket": "0.01%",
                "actual_participation": 10.0 / 62000000.0,
            }
        ]
    ).to_csv(source_dir / "fill_collection_batch.csv", index=False, encoding="utf-8")

    (run_dir / "alpaca_fill_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "source_type": "orders_oms",
                "source_path": str((source_dir / "orders_oms.csv").resolve()),
                "market": "us",
                "broker": "alpaca",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "sample_id": "fill_collection_batch",
                "ticker": "AAPL",
                "direction": "buy",
                "requested_qty": 10.0,
                "filled_qty": 10.0,
                "avg_fill_price": 100.05,
                "reference_price": 100.0,
                "estimated_price": 100.0,
                "requested_notional": 1000.0,
                "filled_notional": 1000.5,
                "fill_ratio": 1.0,
                "status": "filled",
            }
        ]
    ).to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")

    dataset, summary = build_slippage_calibration_dataset(
        fill_collection_root=fill_root,
        source_run_root=None,
        alpha=0.6,
    )

    assert summary["missing_adv_count"] == 0
    assert float(dataset.loc[0, "source_adv_shares"]) == pytest.approx(62000000.0)
    assert str(dataset.loc[0, "source_market_path"]).endswith("fill_collection_batch.csv")
