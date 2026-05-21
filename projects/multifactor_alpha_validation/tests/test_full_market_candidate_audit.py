from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.full_market_candidate_audit import run_full_market_candidate_full_audit


def test_full_market_candidate_full_audit_writes_diagnostic_artifacts(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns_long.csv"
    supervisor_dir = tmp_path / "supervisor"
    output_dir = tmp_path / "candidate_audit"
    supervisor_dir.mkdir()
    _write_returns_fixture(returns_path)
    (supervisor_dir / "frozen_candidate_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "full_market_supervisor_frozen_candidate.v1",
                "candidate": {
                    "candidate_id": "reversal_1d",
                    "search_kind": "leaf",
                    "window": "post_1_1",
                    "side": "top",
                    "quantile": 0.8,
                    "feature_id": "reversal_1d",
                },
                "locked_validation_only": True,
                "not_alpha_evidence": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = run_full_market_candidate_full_audit(
        returns_panel_path=returns_path,
        supervisor_dir=supervisor_dir,
        output_dir=output_dir,
    )

    summary = json.loads((output_dir / "candidate_full_audit_summary.json").read_text(encoding="utf-8"))
    temporal = pd.read_csv(output_dir / "candidate_temporal_breadth.csv")
    tail = pd.read_csv(output_dir / "candidate_tail_concentration.csv")
    anomalies = json.loads((output_dir / "candidate_data_anomaly_audit.json").read_text(encoding="utf-8"))
    cost_capacity = json.loads((output_dir / "candidate_cost_capacity_audit.json").read_text(encoding="utf-8"))
    residual = pd.read_csv(output_dir / "candidate_benchmark_residual_audit.csv")
    report = (output_dir / "candidate_full_audit_report.md").read_text(encoding="utf-8").lower()

    assert result.validation_status == "evaluated"
    assert result.decision_label == "full_audit_passed_cost_capacity_pending"
    assert summary["decision_label"] == "full_audit_passed_cost_capacity_pending"
    assert summary["candidate"]["window"] == "post_1_1"
    assert summary["selected_row_count"] > 0
    assert summary["top10_abs_share"] <= 0.35
    assert summary["cost_capacity_status"] == "cost_capacity_inputs_unavailable"
    assert summary["d3_charter_allowed"] is False
    assert summary["measurement_spec_written"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert summary["or_optimizer_used"] is False
    assert summary["expected_return_panel_written"] is False
    assert summary["alpha_registry_update_allowed"] is False
    assert summary["production_approval"] is False
    assert summary["live_trading"] is False
    assert summary["broker_order_workflow"] is False
    assert {"split", "month"}.issubset(set(temporal["period_type"]))
    assert {"train", "validation", "test"}.issubset(set(temporal["split"].dropna()))
    assert {"issuer_abs_share", "top10_abs_share"}.issubset(tail.columns)
    assert anomalies["returns_row_count"] > 0
    assert anomalies["selected_row_count"] == summary["selected_row_count"]
    assert anomalies["extreme_return_row_count"] == 0
    assert anomalies["missing_volume_inputs"] is True
    assert anomalies["stale_price_proxy_available"] is False
    assert anomalies["delisting_inputs_available"] is False
    assert cost_capacity["adv_inputs_available"] is False
    assert cost_capacity["spread_inputs_available"] is False
    assert cost_capacity["capacity_status"] == "cost_capacity_inputs_unavailable"
    assert cost_capacity["fabricated_capacity"] is False
    assert {"mean_return", "benchmark_residual_mean_return"}.issubset(residual.columns)
    assert "full audit" in report
    assert "diagnostic only" in report
    assert "q2 remains closed" in report
    assert "cost/capacity pending" in report


def test_full_market_candidate_full_audit_blocks_missing_frozen_candidate(tmp_path: Path) -> None:
    output_dir = tmp_path / "candidate_audit"
    result = run_full_market_candidate_full_audit(
        returns_panel_path=tmp_path / "missing_returns.csv",
        supervisor_dir=tmp_path / "supervisor",
        output_dir=output_dir,
    )

    summary = json.loads((output_dir / "candidate_full_audit_summary.json").read_text(encoding="utf-8"))
    temporal = pd.read_csv(output_dir / "candidate_temporal_breadth.csv")

    assert result.validation_status == "blocked"
    assert result.decision_label == "blocked_missing_frozen_candidate"
    assert summary["decision_label"] == "blocked_missing_frozen_candidate"
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert temporal.empty


def test_full_market_candidate_full_audit_uses_existing_market_capacity_inputs(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns_long.csv"
    supervisor_dir = tmp_path / "supervisor"
    output_dir = tmp_path / "candidate_audit"
    market_reference = tmp_path / "us_universe_reference.csv"
    market_snapshot = tmp_path / "us_universe_market.csv"
    supervisor_dir.mkdir()
    _write_returns_fixture(returns_path)
    (supervisor_dir / "frozen_candidate_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "full_market_supervisor_frozen_candidate.v1",
                "candidate": {
                    "candidate_id": "reversal_1d",
                    "search_kind": "leaf",
                    "window": "post_1_1",
                    "side": "top",
                    "quantile": 0.8,
                    "feature_id": "reversal_1d",
                },
                "locked_validation_only": True,
                "not_alpha_evidence": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "ticker": f"T{index:02d}",
                "sector": "Test",
                "market_cap": 1_000_000_000 + index,
                "avg_adv_20d": 1_000_000 + index * 10_000,
                "liquidity_bucket": "high",
                "close_2026_03_27": 50.0 + index,
            }
            for index in range(50)
        ]
    ).to_csv(market_reference, index=False)
    pd.DataFrame(
        [
            {
                "ticker": f"T{index:02d}",
                "close": 50.0 + index,
                "vwap": 50.0 + index,
                "adv_shares": 1_000_000 + index * 10_000,
                "tradable": True,
            }
            for index in range(50)
        ]
    ).to_csv(market_snapshot, index=False)

    result = run_full_market_candidate_full_audit(
        returns_panel_path=returns_path,
        supervisor_dir=supervisor_dir,
        output_dir=output_dir,
        market_reference_path=market_reference,
        market_snapshot_path=market_snapshot,
    )

    summary = json.loads((output_dir / "candidate_full_audit_summary.json").read_text(encoding="utf-8"))
    anomalies = json.loads((output_dir / "candidate_data_anomaly_audit.json").read_text(encoding="utf-8"))
    cost_capacity = json.loads((output_dir / "candidate_cost_capacity_audit.json").read_text(encoding="utf-8"))

    assert result.validation_status == "evaluated"
    assert summary["decision_label"] == "full_audit_passed_cost_capacity_pending"
    assert summary["cost_capacity_status"] == "cost_capacity_proxy_evaluated_actual_spread_pending"
    assert summary["cost_capacity_market_input_coverage_share"] == 1.0
    assert anomalies["missing_volume_inputs"] is False
    assert anomalies["adv_proxy_available"] is True
    assert cost_capacity["adv_inputs_available"] is True
    assert cost_capacity["spread_inputs_available"] is True
    assert cost_capacity["real_spread_inputs_available"] is False
    assert cost_capacity["market_input_coverage_share"] == 1.0
    assert cost_capacity["fabricated_capacity"] is False
    assert len(cost_capacity["capacity_scenarios"]) == 4
    assert cost_capacity["capacity_scenarios"][0]["participation_p95"] > 0.0


def _write_returns_fixture(path: Path) -> None:
    rows: list[dict[str, object]] = []
    tickers = [f"T{index:02d}" for index in range(50)]
    dates = pd.bdate_range("2024-01-02", periods=90)
    for day_index, date in enumerate(dates):
        cycle = day_index % 2
        for ticker_index, ticker in enumerate(tickers):
            base = ((ticker_index % 7) - 3) * 0.00015
            if cycle == 0 and ticker_index < 10:
                ret = -0.018 - ticker_index * 0.0002
            elif cycle == 1 and ticker_index < 10:
                ret = 0.010 + (ticker_index % 3) * 0.0001
            else:
                ret = base
            rows.append({"date": date.date().isoformat(), "ticker": ticker, "return": ret})
    pd.DataFrame(rows).to_csv(path, index=False)
