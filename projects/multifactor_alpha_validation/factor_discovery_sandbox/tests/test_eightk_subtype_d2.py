from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.eightk_subtype_d2 import (
    run_eightk_subtype_observability_d2,
)


def test_eightk_subtype_d2_observes_priority_subtypes_without_formula(tmp_path: Path) -> None:
    event_path = _write_eightk_event_fixture(tmp_path / "events.csv")
    price_path = _write_eightk_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")

    result = run_eightk_subtype_observability_d2(
        event_registry_path=event_path,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "d2_8k",
        minimum_subtype_events=3,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["schema_version"] == "eightk_subtype_d2_observability_summary.v1"
    assert result.summary["stage"] == "D2-8K-01"
    assert result.summary["candidate_id"] == "8k_subtype_underreaction_observability"
    assert result.summary["overall_decision"] == "observable"
    assert set(result.summary["allow_d3_charter_for"]) == {
        "auditor_change",
        "cfo_departure",
        "ceo_departure",
        "material_agreement_termination",
        "restatement_amendment",
    }
    assert result.summary["no_formula_observability_only"] is True
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["no_view_not_zero_alpha"] is True

    subtype_counts = pd.read_csv(result.artifacts["eightk_subtype_counts"])
    assert set(subtype_counts["eightk_subtype"]) >= {
        "auditor_change",
        "cfo_departure",
        "ceo_departure",
        "material_agreement_termination",
        "restatement_amendment",
        "routine_8k_control",
        "unknown_no_view",
    }

    car = pd.read_csv(result.artifacts["car_window_panel"])
    assert {
        "pre_20_1",
        "pre_10_1",
        "pre_5_1",
        "post_0_1",
        "post_1_5",
        "post_1_10",
        "post_1_22",
        "post_1_44",
    }.issubset(set(car["window"]))
    observed_primary = car[(car["window"] == "post_1_22") & (car["label_status"] == "observed")]
    assert observed_primary["observed_event_count"].min() >= 3
    assert "formula_score" not in car.columns
    assert "expected_return" not in car.columns

    placebo = pd.read_csv(result.artifacts["placebo_report"])
    assert {
        "shift_minus_5",
        "shift_plus_5",
        "same_coverage_random",
        "subtype_label_randomized",
        "issuer_non_event",
        "routine_8k_control",
    }.issubset(set(placebo["placebo_name"]))
    assert placebo["status"].eq("pass").all()

    report = result.artifacts["d2_8k_subtype_report"].read_text(encoding="utf-8").lower()
    assert "no-formula observability only" in report
    assert "not alpha evidence" in report
    assert "does not run q1, q2, optimizer, portfolio, alpha registry, paper, broker, order, live, or production workflows" in report
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


def test_eightk_subtype_d2_blocks_timestamp_violations_before_d3(tmp_path: Path) -> None:
    event_path = _write_eightk_event_fixture(tmp_path / "events.csv")
    events = pd.read_csv(event_path)
    events.loc[0, "tradable_timestamp"] = "2024-01-02T13:30:00+00:00"
    events.loc[0, "filing_accepted_ts"] = "2024-01-03T21:00:00+00:00"
    events.to_csv(event_path, index=False)
    price_path = _write_eightk_price_fixture(tmp_path / "prices.csv")

    result = run_eightk_subtype_observability_d2(
        event_registry_path=event_path,
        price_panel_path=price_path,
        output_dir=tmp_path / "d2_8k_blocked",
        minimum_subtype_events=3,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["overall_decision"] == "blocked_timestamp"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False


def test_eightk_subtype_d2_rejects_downstream_inputs(tmp_path: Path) -> None:
    event_path = _write_eightk_event_fixture(tmp_path / "events.csv")
    events = pd.read_csv(event_path)
    events["expected_return"] = 0.01
    events.to_csv(event_path, index=False)
    price_path = _write_eightk_price_fixture(tmp_path / "prices.csv")

    try:
        run_eightk_subtype_observability_d2(
            event_registry_path=event_path,
            price_panel_path=price_path,
            output_dir=tmp_path / "d2_8k_rejected",
        )
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - proves the guard fired
        raise AssertionError("expected D2 8-K observability to reject downstream fields")

    assert "forbidden input columns" in message
    assert "expected_return" in message


def _write_eightk_event_fixture(path: Path) -> Path:
    rows = []
    subtypes = [
        ("AUD", "auditor_change", "Item 4.01 auditor resignation", 0.003),
        ("CFO", "cfo_departure", "Item 5.02 CFO resigned", 0.0025),
        ("CEO", "ceo_departure", "Item 5.02 CEO terminated", 0.002),
        ("MAT", "material_agreement_termination", "Item 1.02 material agreement termination", 0.0018),
        ("RST", "restatement_amendment", "Item 4.02 restatement and 8-K/A amendment", 0.0022),
    ]
    for subtype_idx, (ticker, subtype, description, _return_hint) in enumerate(subtypes):
        for event_idx, signal_date in enumerate(["2024-01-03", "2024-02-05", "2024-03-04"]):
            rows.append(
                {
                    "event_id": f"{subtype}_{event_idx}",
                    "ticker": ticker,
                    "issuer_cik": f"10{subtype_idx:03d}{event_idx:02d}",
                    "accession_number": f"0000000000-24-{subtype_idx:03d}{event_idx:03d}",
                    "form_type": "8-K/A" if subtype == "restatement_amendment" else "8-K",
                    "filing_accepted_ts": f"{signal_date}T21:00:00+00:00",
                    "tradable_timestamp": _next_day_open(signal_date),
                    "event_item": description.split()[1],
                    "event_description": description,
                    "eightk_subtype": subtype,
                    "sector": "technology" if subtype_idx % 2 == 0 else "industrials",
                    "size_bucket": "mid",
                    "liquidity_bucket": "high",
                    "coverage_state": "covered",
                    "no_view_reason": "",
                    "diagnostic_only": False,
                    "event_month": signal_date[:7],
                },
            )
    rows.append(
        {
            "event_id": "routine_control_0",
            "ticker": "CTRL",
            "issuer_cik": "199900",
            "accession_number": "0000000000-24-999000",
            "form_type": "8-K",
            "filing_accepted_ts": "2024-01-03T21:00:00+00:00",
            "tradable_timestamp": _next_day_open("2024-01-03"),
            "event_item": "2.02",
            "event_description": "routine results of operations and financial condition",
            "eightk_subtype": "routine_8k_control",
            "sector": "technology",
            "size_bucket": "mid",
            "liquidity_bucket": "high",
            "coverage_state": "covered",
            "no_view_reason": "",
            "diagnostic_only": True,
            "event_month": "2024-01",
        },
    )
    rows.append(
        {
            "event_id": "unknown_0",
            "ticker": "UNKN",
            "issuer_cik": "188800",
            "accession_number": "0000000000-24-888000",
            "form_type": "8-K",
            "filing_accepted_ts": "2024-01-03T21:00:00+00:00",
            "tradable_timestamp": _next_day_open("2024-01-03"),
            "event_item": "",
            "event_description": "",
            "eightk_subtype": "",
            "sector": "",
            "size_bucket": "",
            "liquidity_bucket": "",
            "coverage_state": "no_view",
            "no_view_reason": "unclassified_8k_subtype",
            "diagnostic_only": True,
            "event_month": "2024-01",
        },
    )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_eightk_price_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", periods=180)
    drift_by_ticker = {
        "AUD": 0.003,
        "CFO": 0.0025,
        "CEO": 0.002,
        "MAT": 0.0018,
        "RST": 0.0022,
        "CTRL": 0.0001,
        "UNKN": 0.0,
    }
    event_dates = [pd.Timestamp("2024-01-04"), pd.Timestamp("2024-02-06"), pd.Timestamp("2024-03-05")]
    rows = []
    for ticker, post_daily_return in drift_by_ticker.items():
        price = 100.0
        for date in dates:
            active_window = any(start < date <= dates[dates.searchsorted(start) + 44] for start in event_dates)
            daily_return = post_daily_return if active_window else 0.0
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


def _write_benchmark_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", periods=180)
    price = 100.0
    rows = []
    for date in dates:
        price *= 1.0002
        rows.append({"date": date.date().isoformat(), "adjusted_close": price})
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _next_day_open(signal_date: str) -> str:
    next_day = pd.bdate_range(pd.Timestamp(signal_date), periods=2)[1]
    return f"{next_day.date().isoformat()}T13:30:00+00:00"
