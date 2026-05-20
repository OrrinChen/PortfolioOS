from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_q1_label_rescue import (
    run_open_market_buying_q1_label_coverage_rescue,
)
from factor_discovery_sandbox.insider_disclosure_q1_evidence import (
    run_open_market_buying_q1_evidence_review,
)
from test_insider_disclosure_q1_evidence import (
    _write_benchmark_panel_fixture,
    _write_d3_signal_panel_fixture,
    _write_price_panel_fixture,
)


def test_q1_label_coverage_rescue_merges_only_price_labels_and_reruns_q1(tmp_path: Path) -> None:
    signal_path = _write_d3_signal_panel_fixture(tmp_path / "signal_panel.csv")
    baseline_price_path = _write_price_panel_fixture(tmp_path / "baseline_prices.csv")
    baseline_prices = pd.read_csv(baseline_price_path)
    baseline_prices = baseline_prices[baseline_prices["ticker"] != "BUYL"]
    baseline_prices.to_csv(baseline_price_path, index=False)
    benchmark_path = _write_benchmark_panel_fixture(tmp_path / "benchmark.csv")
    extra_price_path = _write_extra_price_panel(tmp_path / "extra_prices.csv")

    baseline = run_open_market_buying_q1_evidence_review(
        signal_panel_path=signal_path,
        price_panel_path=baseline_price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "baseline_q1",
        minimum_active_event_clusters=2,
        minimum_event_month_count=1,
        minimum_label_coverage_share=0.75,
    )
    assert baseline.summary["q1_decision"] == "hold_insufficient_sample"
    assert baseline.summary["label_coverage_share"] < 0.75

    result = run_open_market_buying_q1_label_coverage_rescue(
        signal_panel_path=signal_path,
        baseline_price_panel_path=baseline_price_path,
        output_dir=tmp_path / "rescue",
        benchmark_panel_path=benchmark_path,
        extra_price_panel_paths=[extra_price_path],
        minimum_active_event_clusters=2,
        minimum_event_month_count=1,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["schema_version"] == "insider_open_market_buying_q1_label_rescue_summary.v1"
    assert result.summary["stage"] == "Q1-INSIDER-01A"
    assert result.summary["signal_panel_hash_unchanged"] is True
    assert result.summary["measurement_spec_modified"] is False
    assert result.summary["formula_modified"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["rescued_label_coverage_share"] >= 0.75

    merged = pd.read_csv(result.artifacts["rescued_price_panel"])
    assert {"BUYH", "BUYL", "COMP", "MISS"}.issubset(set(merged["ticker"]))
    assert result.summary["merged_extra_price_rows"] > 0

    q1_decision = json.loads(result.artifacts["rescued_q1_decision_summary"].read_text(encoding="utf-8"))
    assert q1_decision["label_coverage_share"] >= 0.75
    assert q1_decision["q2_entry_allowed"] is False
    assert q1_decision["production_approval_claimed"] is False

    report = result.artifacts["q1_label_coverage_rescue_report"].read_text(encoding="utf-8").lower()
    assert "label coverage rescue only" in report
    assert "does not modify the d3 signal" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report


def test_q1_label_coverage_rescue_rejects_feature_or_expected_return_columns(tmp_path: Path) -> None:
    signal_path = _write_d3_signal_panel_fixture(tmp_path / "signal_panel.csv")
    baseline_price_path = _write_price_panel_fixture(tmp_path / "baseline_prices.csv")
    extra_price_path = _write_extra_price_panel(tmp_path / "leaky_extra_prices.csv")
    extra = pd.read_csv(extra_price_path)
    extra["expected_return"] = 0.01
    extra.to_csv(extra_price_path, index=False)

    try:
        run_open_market_buying_q1_label_coverage_rescue(
            signal_panel_path=signal_path,
            baseline_price_panel_path=baseline_price_path,
            output_dir=tmp_path / "rescue",
            extra_price_panel_paths=[extra_price_path],
        )
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - proves the guard fired
        raise AssertionError("expected rescue to reject leaky price inputs")

    assert "forbidden price panel columns" in message
    assert "expected_return" in message


def _write_extra_price_panel(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-15", periods=80)
    rows = []
    price = 100.0
    for date in dates:
        price *= 0.999
        rows.append(
            {
                "ticker": "BUYL",
                "date": date.date().isoformat(),
                "adjusted_close": price,
                "volume": 1_000_000,
                "market_cap": 800_000_000,
                "dollar_volume": price * 1_000_000,
                "bid_ask_spread": 0.001,
                "sector": "technology",
            },
        )
    return _append_signal_row_for_missing_ticker(path, rows)


def _append_signal_row_for_missing_ticker(path: Path, rows: list[dict[str, object]]) -> Path:
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
