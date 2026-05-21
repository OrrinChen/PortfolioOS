from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.revision_confirmed_earnings_underreaction import (
    apply_revision_confirmed_promotion_gate,
    construct_revision_confirmed_signals,
    run_revision_confirmed_earnings_underreaction,
)


def test_signal_construction_respects_revision_event_and_industry_neutral_rules(tmp_path: Path) -> None:
    prices, estimates, events, universe = _fixture_frames()

    signals, audit = construct_revision_confirmed_signals(
        prices=prices,
        estimates=estimates,
        events=events,
        universe=universe,
        min_cross_section=3,
    )

    active = signals[signals["coverage_status"] == "active_view"].copy()
    assert not active.empty
    assert audit["pit_timestamp_audit_passed"] is True
    assert audit["future_estimate_timestamp_violations"] == 0
    assert {
        "revision_20d_raw",
        "revision_acceleration_raw",
        "event_confirmation_raw",
        "short_reversal_guard_raw",
        "revision_only_score",
        "revision_plus_event_confirmation_score",
        "industry_neutral_score",
        "adv_63d",
    }.issubset(active.columns)

    latest = active.sort_values("signal_date").groupby("ticker").tail(1).set_index("ticker")
    assert latest.loc["STRONG", "revision_20d_raw"] > latest.loc["WEAK", "revision_20d_raw"]
    assert latest.loc["STRONG", "industry_neutral_score"] > latest.loc["WEAK", "industry_neutral_score"]

    industry_means = active.groupby(["signal_date", "industry"])["industry_neutral_score"].mean().dropna()
    assert industry_means.abs().max() < 1e-12

    assert active["signal_trading_day_offset_from_announcement"].min() >= 5
    assert "TOO_SOON" not in set(active["ticker"])
    too_soon = signals[signals["ticker"] == "TOO_SOON"]
    assert set(too_soon["coverage_status"]) == {"explicit_abstain"}
    assert "next_earnings_within_5_trading_days" in set(too_soon["abstain_reason"])


def test_no_future_analyst_estimate_timestamp_is_used() -> None:
    prices, estimates, events, universe = _fixture_frames()

    signals, audit = construct_revision_confirmed_signals(
        prices=prices,
        estimates=estimates,
        events=events,
        universe=universe,
        min_cross_section=3,
    )

    strong = signals[(signals["ticker"] == "STRONG") & (signals["coverage_status"] == "active_view")].iloc[0]
    assert pd.Timestamp(strong["latest_estimate_snapshot_date"]) <= pd.Timestamp(strong["signal_date"])
    assert strong["latest_expected_eps"] != 99.0
    assert audit["future_estimate_timestamp_violations"] == 0
    assert audit["max_estimate_snapshot_lag_days"] >= 0


def test_earnings_date_plus_5_tradability_rule() -> None:
    prices, estimates, events, universe = _fixture_frames()

    signals, audit = construct_revision_confirmed_signals(
        prices=prices,
        estimates=estimates,
        events=events,
        universe=universe,
        min_cross_section=3,
    )

    active = signals[signals["coverage_status"] == "active_view"]
    assert active["eligible_from_date"].notna().all()
    assert (pd.to_datetime(active["signal_date"]) >= pd.to_datetime(active["eligible_from_date"])).all()
    assert audit["tradability_rule_violations"] == 0


def test_missing_sue_or_car3_sets_event_confirmation_to_neutral() -> None:
    prices, estimates, events, universe = _fixture_frames()

    signals, audit = construct_revision_confirmed_signals(
        prices=prices,
        estimates=estimates,
        events=events,
        universe=universe,
        min_cross_section=3,
    )

    missing_sue = signals[(signals["ticker"] == "MISS_SUE") & (signals["coverage_status"] == "active_view")].iloc[0]
    missing_car = signals[(signals["ticker"] == "MISS_CAR") & (signals["coverage_status"] == "active_view")].iloc[0]
    assert missing_sue["event_confirmation_raw"] == 0.0
    assert missing_car["event_confirmation_raw"] == 0.0
    assert bool(missing_sue["event_confirmation_neutralized_missing"]) is True
    assert bool(missing_car["event_confirmation_neutralized_missing"]) is True
    assert audit["event_confirmation_missing_input_rows"] >= 2


def test_placebo_failure_blocks_promotion() -> None:
    gate = apply_revision_confirmed_promotion_gate(
        results=_passing_live_results(),
        placebo_comparison=pd.DataFrame(
            [
                {
                    "candidate_variant": "industry_neutral_score",
                    "horizon": "20d",
                    "placebo_name": "shifted_event_plus_5td",
                    "live_mean_rank_ic": 0.030,
                    "placebo_mean_rank_ic": 0.041,
                },
                {
                    "candidate_variant": "industry_neutral_score",
                    "horizon": "to_next_announcement",
                    "placebo_name": "random_same_coverage_placebo",
                    "live_mean_rank_ic": 0.035,
                    "placebo_mean_rank_ic": 0.010,
                },
            ]
        ),
        capacity_diagnostics=_passing_capacity_diagnostics(),
        pit_timestamp_audit=_passing_pit_audit(),
    )

    assert gate["decision_label"] == "placebo_blocked"
    assert gate["promotion_gate_passed"] is False
    assert gate["alpha_success_claimed"] is False


def test_capacity_collapse_blocks_promotion() -> None:
    capacity = _passing_capacity_diagnostics()
    capacity.loc[
        capacity["capacity_filter"] == "remove_bottom_40pct_adv",
        ["active_row_count", "capacity_coverage_loss", "capacity_filtered_does_not_collapse"],
    ] = [1, 0.95, False]

    gate = apply_revision_confirmed_promotion_gate(
        results=_passing_live_results(),
        placebo_comparison=_passing_placebos(),
        capacity_diagnostics=capacity,
        pit_timestamp_audit=_passing_pit_audit(),
    )

    assert gate["decision_label"] == "capacity_blocked"
    assert gate["gate_checks"]["capacity_filtered_version_does_not_collapse"] is False
    assert gate["promotion_gate_passed"] is False


def test_runner_writes_only_fd_sandbox_artifacts_and_no_downstream_outputs(tmp_path: Path) -> None:
    prices, estimates, events, universe = _fixture_frames()
    prices_path = tmp_path / "prices.csv"
    estimates_path = tmp_path / "estimates.csv"
    events_path = tmp_path / "events.csv"
    universe_path = tmp_path / "universe.csv"
    prices.to_csv(prices_path, index=False)
    estimates.to_csv(estimates_path, index=False)
    events.to_csv(events_path, index=False)
    universe.to_csv(universe_path, index=False)

    result = run_revision_confirmed_earnings_underreaction(
        prices_path=prices_path,
        estimates_path=estimates_path,
        events_path=events_path,
        universe_path=universe_path,
        output_dir=tmp_path / "revision_confirmed_earnings_underreaction",
        min_cross_section=3,
        train_test_split_date="2020-06-01",
    )

    expected_artifacts = {
        "candidate_design_manifest",
        "revision_confirmed_alpha_results",
        "revision_confirmed_alpha_summary",
        "revision_confirmed_alpha_report",
        "placebo_comparison",
        "signal_decay",
        "capacity_diagnostics",
        "pit_timestamp_audit",
    }
    assert set(result.artifacts) == expected_artifacts
    assert {path.name for path in result.artifacts.values()} == {
        "revision_confirmed_alpha_results.csv",
        "revision_confirmed_alpha_summary.json",
        "revision_confirmed_alpha_report.md",
        "placebo_comparison.csv",
        "signal_decay.csv",
        "capacity_diagnostics.csv",
        "pit_timestamp_audit.json",
        "candidate_design_manifest.json",
    }
    assert {path.name for path in (tmp_path / "revision_confirmed_earnings_underreaction").iterdir()} == {
        path.name for path in result.artifacts.values()
    }

    summary = json.loads(result.artifacts["revision_confirmed_alpha_summary"].read_text(encoding="utf-8"))
    assert summary["candidate_id"] == "revision_confirmed_earnings_underreaction"
    assert summary["design_contract_valid"] is True
    assert summary["design_layer_required_before_formula"] is True
    assert summary["formula_is_measurement_not_thesis"] is True
    assert summary["sandbox_only"] is True
    assert summary["allocator_ran"] is False
    assert summary["q1_entry_written"] is False
    assert summary["q2_entry_written"] is False
    assert summary["typed_projection_ran"] is False
    assert summary["alpha_registry_updated"] is False
    assert summary["production_approval_claimed"] is False
    assert summary["alpha_success_claimed"] is False
    assert summary["decision_label"] in {
        "promotable_to_Q1_candidate_review",
        "diagnostic_only_mixed",
        "placebo_blocked",
        "capacity_blocked",
        "timestamp_blocked",
        "insufficient_support",
    }
    design_manifest = json.loads(result.artifacts["candidate_design_manifest"].read_text(encoding="utf-8"))
    assert design_manifest["candidate_validation_allowed"] is True
    assert "market_pain_point" in design_manifest["design_contract"]

    report = result.artifacts["revision_confirmed_alpha_report"].read_text(encoding="utf-8").lower()
    assert "not production alpha" in report
    assert "direct q2 entry: not allowed" in report

    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "factor_discovery_sandbox"
        / "revision_confirmed_earnings_underreaction.py"
    )
    module_source = module_path.read_text(encoding="utf-8")
    import_surface = "\n".join(
        line for line in module_source.splitlines() if line.startswith(("import ", "from "))
    )
    forbidden_import_fragments = [
        "agentic_alpha_triage",
        "execution_aware_optimizer",
        "promotion_gate",
        "alpha_registry",
        "portfolio_os.alpha.projection",
        "typed_alpha_pilot",
    ]
    for fragment in forbidden_import_fragments:
        assert fragment not in import_surface


def _fixture_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = pd.bdate_range("2020-01-02", "2020-09-30")
    assets = [
        ("10001", "STRONG", "IBSTR", 0.0013, "Technology", "Software"),
        ("10002", "WEAK", "IBWEK", -0.0002, "Technology", "Software"),
        ("10003", "MISS_SUE", "IBMSU", 0.0006, "Healthcare", "Tools"),
        ("10004", "MISS_CAR", "IBMCA", 0.0004, "Healthcare", "Tools"),
        ("10005", "STEADY", "IBSTD", 0.0008, "Industrials", "Machinery"),
        ("10006", "TOO_SOON", "IBTOO", 0.0007, "Industrials", "Machinery"),
    ]

    price_rows = []
    for index, (permno, ticker, _ibes, drift, _sector, _industry) in enumerate(assets):
        price = 20.0 + index * 3.0
        for day_index, date in enumerate(dates):
            ret = drift + (0.0003 if day_index % 2 == 0 else -0.0002)
            if ticker in {"STRONG", "MISS_SUE", "STEADY"} and date in pd.bdate_range("2020-04-01", periods=4):
                ret += 0.015
            if ticker == "WEAK" and date in pd.bdate_range("2020-04-01", periods=4):
                ret -= 0.015
            if ticker == "MISS_CAR" and date in pd.bdate_range("2020-04-01", periods=4):
                ret = float("nan")
            if pd.notna(ret):
                price *= 1.0 + float(ret)
            price_rows.append(
                {
                    "permno": permno,
                    "asset_id": permno,
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": round(price, 6),
                    "volume": 2_000_000 + index * 200_000 + day_index * 1000,
                    "return": ret,
                }
            )

    revision_paths = {
        "STRONG": [1.00, 1.05, 1.12, 1.24, 1.34, 99.00],
        "WEAK": [1.00, 0.98, 0.93, 0.88, 0.80, 50.00],
        "MISS_SUE": [1.00, 1.02, 1.05, 1.07, 1.11, 20.00],
        "MISS_CAR": [1.00, 1.01, 1.04, 1.06, 1.10, 20.00],
        "STEADY": [1.00, 1.04, 1.09, 1.15, 1.22, 25.00],
        "TOO_SOON": [1.00, 1.03, 1.08, 1.12, 1.18, 30.00],
    }
    snapshot_dates = ["2020-01-03", "2020-02-14", "2020-03-10", "2020-04-07", "2020-07-14", "2020-10-01"]
    estimate_rows = []
    for permno, ticker, ibes, _drift, _sector, _industry in assets:
        for snapshot_date, expected_eps in zip(snapshot_dates, revision_paths[ticker]):
            estimate_rows.append(
                {
                    "ibes_ticker": ibes,
                    "cusip": f"{permno}C",
                    "fiscal_period": "FY1",
                    "estimate_snapshot_date": snapshot_date,
                    "expected_eps": expected_eps,
                    "numest": 5,
                    "medest": expected_eps,
                }
            )

    event_rows = []
    sue_values = {
        "STRONG": 0.80,
        "WEAK": -0.70,
        "MISS_SUE": None,
        "MISS_CAR": 0.50,
        "STEADY": 0.35,
        "TOO_SOON": 0.45,
    }
    for permno, ticker, ibes, _drift, _sector, _industry in assets:
        event_rows.append(
            {
                "event_id": f"E-{ticker}-1",
                "symbol": ticker,
                "permno": permno,
                "ibes_ticker": ibes,
                "cusip": f"{permno}C",
                "fiscal_period": "2020Q1",
                "announcement_date": "2020-04-01",
                "event_available_timestamp": "2020-04-01T21:05:00Z",
                "actual_eps": 1.20,
                "expected_eps": 1.00,
                "sue_value": sue_values[ticker],
                "pit_safety_status": "pit_safe",
            }
        )
        next_date = "2020-04-13" if ticker == "TOO_SOON" else "2020-07-15"
        event_rows.append(
            {
                "event_id": f"E-{ticker}-2",
                "symbol": ticker,
                "permno": permno,
                "ibes_ticker": ibes,
                "cusip": f"{permno}C",
                "fiscal_period": "2020Q2",
                "announcement_date": next_date,
                "event_available_timestamp": f"{next_date}T21:05:00Z",
                "actual_eps": 1.10,
                "expected_eps": 1.00,
                "sue_value": 0.10,
                "pit_safety_status": "pit_safe",
            }
        )

    universe = pd.DataFrame(
        [
            {
                "permno": permno,
                "asset_id": permno,
                "ticker": ticker,
                "membership_start": "2020-01-01",
                "membership_end": "2020-12-31",
                "sector": sector,
                "industry": industry,
            }
            for permno, ticker, _ibes, _drift, sector, industry in assets
        ]
    )
    return pd.DataFrame(price_rows), pd.DataFrame(estimate_rows), pd.DataFrame(event_rows), universe


def _passing_live_results() -> pd.DataFrame:
    rows = []
    for variant in [
        "industry_neutral_score",
        "capacity_filtered_industry_neutral_score_remove_bottom_20pct_adv",
        "capacity_filtered_industry_neutral_score_remove_bottom_40pct_adv",
    ]:
        for horizon, rank_ic in [("10d", 0.020), ("20d", 0.030), ("to_next_announcement", 0.035)]:
            rows.append(
                {
                    "candidate_variant": variant,
                    "period": "test",
                    "horizon": horizon,
                    "mean_rank_ic": rank_ic,
                    "rank_ic_t_stat": 2.1,
                    "mean_top_bottom_spread": 0.015,
                    "mean_top_bottom_spread_after_cost": 0.010,
                    "active_row_count": 120,
                    "active_date_count": 12,
                }
            )
    return pd.DataFrame(rows)


def _passing_placebos() -> pd.DataFrame:
    rows = []
    for horizon, live in [("20d", 0.030), ("to_next_announcement", 0.035)]:
        for placebo_name in [
            "shifted_event_minus_5td",
            "shifted_event_plus_5td",
            "shifted_event_plus_10td",
            "random_same_coverage_placebo",
            "permuted_revision_timestamp_placebo",
            "industry_only_placebo",
            "short_term_return_only_placebo",
        ]:
            rows.append(
                {
                    "candidate_variant": "industry_neutral_score",
                    "horizon": horizon,
                    "placebo_name": placebo_name,
                    "live_mean_rank_ic": live,
                    "placebo_mean_rank_ic": live - 0.015,
                }
            )
    return pd.DataFrame(rows)


def _passing_capacity_diagnostics() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "capacity_filter": "none",
                "active_row_count": 150,
                "capacity_coverage_loss": 0.0,
                "capacity_filtered_does_not_collapse": True,
            },
            {
                "capacity_filter": "remove_bottom_20pct_adv",
                "active_row_count": 120,
                "capacity_coverage_loss": 0.20,
                "capacity_filtered_does_not_collapse": True,
            },
            {
                "capacity_filter": "remove_bottom_40pct_adv",
                "active_row_count": 90,
                "capacity_coverage_loss": 0.40,
                "capacity_filtered_does_not_collapse": True,
            },
        ]
    )


def _passing_pit_audit() -> dict[str, object]:
    return {
        "pit_timestamp_audit_passed": True,
        "future_estimate_timestamp_violations": 0,
        "tradability_rule_violations": 0,
        "broken_pit_timestamp_rows": 0,
        "missing_data_explains_effect": False,
        "coverage_diagnostic_block": False,
    }
