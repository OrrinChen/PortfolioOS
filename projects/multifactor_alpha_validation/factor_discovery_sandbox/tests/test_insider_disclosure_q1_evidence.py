from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_q1_evidence import (
    run_open_market_buying_q1_evidence_review,
)


def test_q1_evidence_review_clusters_events_and_preserves_boundaries(tmp_path: Path) -> None:
    signal_path = _write_d3_signal_panel_fixture(tmp_path / "signal_panel.csv")
    price_path = _write_price_panel_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_panel_fixture(tmp_path / "benchmark.csv")

    result = run_open_market_buying_q1_evidence_review(
        signal_panel_path=signal_path,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "q1",
        minimum_active_event_clusters=2,
        minimum_event_month_count=1,
        minimum_label_coverage_share=0.5,
    )

    assert result.summary["schema_version"] == "insider_open_market_buying_q1_evidence_summary.v1"
    assert result.summary["stage"] == "Q1-INSIDER-01"
    assert result.summary["measurement_spec_id"] == "open_market_insider_buying_post_2023_v0"
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["paper_ready"] is False
    assert result.summary["live_ready"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["expected_return_panel_written"] is False

    cluster_panel = pd.read_csv(result.artifacts["q1_event_cluster_panel"]).fillna("")
    assert len(cluster_panel) == 2
    assert set(cluster_panel["issuer_event_cluster_id"]) == {
        "BUYH|1001|2024-01-03|open_market_buy",
        "BUYL|1002|2024-01-03|open_market_buy",
    }
    high_cluster = cluster_panel[cluster_panel["ticker"] == "BUYH"].iloc[0]
    assert high_cluster["source_signal_row_count"] == 2
    assert high_cluster["cluster_distinct_insiders"] == 2

    signal_label = pd.read_csv(result.artifacts["q1_signal_label_panel"]).fillna("")
    no_view = signal_label[signal_label["coverage_state"] == "no_view"]
    assert not no_view.empty
    assert no_view["q1_label_status"].eq("no_view_signal_excluded").all()
    assert no_view["primary_abnormal_return"].astype(str).eq("").all()
    assert no_view["no_view_not_zero_alpha"].eq(True).all()

    labels = pd.read_csv(result.artifacts["q1_forward_return_labels"]).fillna("")
    assert {"post_1_5", "post_1_10", "post_1_22", "post_1_44", "pre_5_1", "pre_10_1", "pre_20_1"}.issubset(
        set(labels["window"]),
    )
    primary = labels[(labels["window"] == "post_1_22") & (labels["label_status"] == "observed")]
    assert primary["abnormal_return"].astype(float).mean() > 0

    rank_ic = pd.read_csv(result.artifacts["q1_rank_ic_by_month"])
    assert rank_ic["rank_ic"].iloc[0] > 0
    top_bottom = pd.read_csv(result.artifacts["q1_top_bottom_spread"])
    assert top_bottom["top_bottom_spread"].iloc[0] > 0

    placebo = pd.read_csv(result.artifacts["q1_placebo_report"])
    assert {
        "shift_minus_5",
        "shift_plus_5",
        "shift_minus_10",
        "shift_plus_10",
        "same_coverage_random",
        "role_label_randomized",
        "issuer_non_event",
        "compensation_control",
    }.issubset(set(placebo["placebo_name"]))

    decision = json.loads(result.artifacts["q1_decision_summary"].read_text(encoding="utf-8"))
    assert decision["q1_decision"] == "passed_q1"
    assert decision["q1_result_interpretation"] == "event_footprint_and_score_ranking_observed"
    assert decision["promotion_gate_allowed"] is True
    assert decision["q2_entry_allowed"] is False

    report = result.artifacts["q1_open_market_buying_evidence_report"].read_text(encoding="utf-8").lower()
    assert "q1 evidence review only" in report
    assert "form 4 code p means open-market or private purchase" in report
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


def test_q1_evidence_review_rejects_downstream_or_forward_return_inputs(tmp_path: Path) -> None:
    signal_path = _write_d3_signal_panel_fixture(tmp_path / "leaky_signal_panel.csv")
    signals = pd.read_csv(signal_path)
    signals["forward_return_22d"] = 0.1
    signals["expected_return"] = 0.02
    signals.to_csv(signal_path, index=False)
    price_path = _write_price_panel_fixture(tmp_path / "prices.csv")

    try:
        run_open_market_buying_q1_evidence_review(
            signal_panel_path=signal_path,
            price_panel_path=price_path,
            output_dir=tmp_path / "q1",
        )
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - proves the guard fired
        raise AssertionError("expected Q1 to reject forward-return or expected-return signal inputs")

    assert "forbidden signal panel columns" in message
    assert "forward_return_22d" in message
    assert "expected_return" in message


def _write_d3_signal_panel_fixture(path: Path) -> Path:
    rows = [
        _signal_row("buy_high_1", "BUYH", "1001", "owner_a", 1.2, 40_000.0, 1.30, "active"),
        _signal_row("buy_high_2", "BUYH", "1001", "owner_b", 1.0, 40_000.0, 1.00, "active"),
        _signal_row("buy_low_1", "BUYL", "1002", "owner_c", -0.8, 12_000.0, 0.80, "active"),
        {
            **_signal_row("missing_market", "MISS", "1003", "owner_d", "", "", "", "no_view"),
            "no_view_reason": "missing_market_join_or_price_volume_controls",
        },
        {
            **_signal_row("comp_control", "COMP", "1004", "owner_e", "", "", "", "no_view"),
            "event_subset": "compensation_control",
            "transaction_code": "A",
            "no_view_reason": "not_code_p_primary_measurement",
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _signal_row(
    event_id: str,
    ticker: str,
    cik: str,
    owner: str,
    normalized_signal: float | str,
    buy_dollar_value: float | str,
    role_weight: float | str,
    coverage_state: str,
) -> dict[str, object]:
    active = coverage_state == "active"
    return {
        "event_id": event_id,
        "issuer_id": ticker,
        "ticker": ticker,
        "cik": cik,
        "accession_number": f"{event_id}_accession",
        "filing_accepted_ts": "2024-01-02T18:00:00+00:00",
        "tradable_ts": "2024-01-03T13:30:00+00:00",
        "signal_date": "2024-01-03",
        "event_subset": "open_market_buy",
        "transaction_code": "P",
        "rule_10b5_1_flag": "",
        "role_bucket": "cfo" if active else "",
        "reporting_owner_cik": owner,
        "buy_dollar_value": buy_dollar_value,
        "market_cap_at_event": 1_000_000_000 if active else "",
        "buy_value_pct": 0.00004 if active else "",
        "distinct_buying_insider_count": 2 if ticker == "BUYH" else 1 if active else "",
        "cluster_weight": 1.098612 if ticker == "BUYH" else 0.693147 if active else "",
        "holding_change_ratio": 0.25 if active else "",
        "holding_change_weight": 1.118034 if active else "",
        "role_weight": role_weight,
        "raw_buy_conviction": 0.00006 if active else "",
        "winsorized_raw_buy_conviction": 0.00006 if active else "",
        "normalized_signal": normalized_signal,
        "coverage_state": coverage_state,
        "no_view_reason": "",
        "measurement_spec_hash": "spec_hash",
        "source_manifest_hash": "source_hash",
        "transaction_code_scope": "open_market_or_private_purchase",
        "private_purchase_filter_status": "unavailable_from_form4_code_only",
        "p_code_purchase_scope_warning": "Form 4 code P means open-market or private purchase.",
        "no_view_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "adv_20d": 5_000_000 if active else 5_000_000,
        "spread_proxy": 0.001,
        "sector": "technology",
        "size_bucket": "mid",
        "liquidity_bucket": "high",
    }


def _write_price_panel_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-15", periods=80)
    rows = []
    anchor = pd.Timestamp("2024-01-03")
    event_end = dates[dates.searchsorted(anchor) + 22]
    for ticker in ("BUYH", "BUYL", "COMP", "MISS"):
        price = 100.0
        for date in dates:
            if anchor < date <= event_end:
                daily_return = {"BUYH": 0.006, "BUYL": -0.001, "COMP": 0.0001, "MISS": 0.0}[ticker]
            else:
                daily_return = 0.0
            price *= 1 + daily_return
            rows.append(
                {
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": price,
                    "volume": 1_000_000,
                    "market_cap": 1_000_000_000,
                    "dollar_volume": price * 1_000_000,
                    "bid_ask_spread": 0.001,
                    "sector": "technology",
                },
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_benchmark_panel_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-15", periods=80)
    price = 100.0
    rows = []
    for date in dates:
        price *= 1.0005
        rows.append({"date": date.date().isoformat(), "benchmark": "QQQ", "adjusted_close": price})
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
