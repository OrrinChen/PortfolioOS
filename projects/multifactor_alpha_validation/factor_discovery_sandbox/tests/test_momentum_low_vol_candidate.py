from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.momentum_low_vol_candidate import run_momentum_low_vol_candidate_validation
from test_real_factor_replay import _write_daily_bundle


def test_momentum_low_vol_candidate_validates_fixed_formula_without_q2_entry(tmp_path: Path) -> None:
    manifest = _write_momentum_low_vol_bundle(tmp_path)

    result = run_momentum_low_vol_candidate_validation(
        manifest_path=manifest,
        output_dir=tmp_path / "candidate_validation",
        train_window_months=6,
        validation_window_months=3,
        horizons=(1, 3),
        min_cross_section=3,
    )

    assert result.summary["schema_version"] == "fd_momentum_low_vol_candidate_summary.v1"
    assert result.summary["candidate_id"] == "momentum_12m_ex1m_low_vol_3m"
    assert result.summary["formula_source"] == "user_supplied_fixed_formula"
    assert result.summary["alpha_success_claimed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["no_view_is_not_zero_alpha"] is True
    assert result.summary["allocator_ran"] is False

    expected_artifacts = {
        "candidate_design_manifest",
        "candidate_signal_panel",
        "candidate_validation_by_rebalance",
        "candidate_placebo_report",
        "candidate_summary",
        "candidate_report",
    }
    assert expected_artifacts == set(result.artifacts)

    panel = pd.read_csv(result.artifacts["candidate_signal_panel"])
    assert {
        "momentum_rank",
        "volatility_rank",
        "candidate_score",
        "industry_neutral_score",
        "capacity_filtered_score",
        "dollar_volume_63d",
        "capacity_rank",
        "capacity_filter_passed",
        "coverage_status",
        "abstain_reason",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "no_view_is_not_zero_alpha",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    }.issubset(panel.columns)
    assert set(panel["candidate_id"]) == {"momentum_12m_ex1m_low_vol_3m"}
    assert panel["not_alpha_evidence"].all()
    assert not panel["direct_q2_entry_allowed"].any()
    assert "explicit_abstain" in set(panel["coverage_status"])

    active_latest = panel[panel["coverage_status"] == "active_view"]
    latest_date = active_latest["rebalance_date"].max()
    latest = active_latest[active_latest["rebalance_date"] == latest_date].set_index("ticker")
    assert latest.loc["STEADY_WINNER", "candidate_score"] > latest.loc["HIGH_VOL_WINNER", "candidate_score"]
    assert latest.loc["STEADY_WINNER", "candidate_score"] > latest.loc["WEAK_LOW_VOL", "candidate_score"]

    grouped_mean = (
        active_latest.groupby(["rebalance_date", "industry"])["industry_neutral_score"]
        .mean()
        .dropna()
        .abs()
        .max()
    )
    assert grouped_mean < 1e-12
    low_capacity = active_latest[active_latest["capacity_rank"] < 0.30]
    assert not low_capacity.empty
    assert low_capacity["capacity_filter_passed"].eq(False).all()
    assert low_capacity["capacity_filtered_score"].isna().all()

    validation = pd.read_csv(result.artifacts["candidate_validation_by_rebalance"])
    assert {"rank_ic", "top_bottom_spread", "period", "horizon_months", "candidate_variant"}.issubset(
        validation.columns
    )
    assert {
        "raw_candidate_score",
        "industry_neutral_score",
        "capacity_filtered_score",
    }.issubset(set(validation["candidate_variant"]))
    assert validation["not_alpha_evidence"].all()

    placebo = pd.read_csv(result.artifacts["candidate_placebo_report"])
    required_tests = {
        "live_candidate_score",
        "sign_flipped_negative_control",
        "lagged_signal_placebo",
        "rebalance_date_shifted_placebo",
        "random_same_coverage_placebo",
        "future_return_leakage_negative_control",
    }
    assert required_tests.issubset(set(placebo["test_name"]))
    assert {
        "raw_candidate_score",
        "industry_neutral_score",
        "capacity_filtered_score",
    }.issubset(set(placebo["candidate_variant"]))
    assert placebo["not_alpha_evidence"].all()

    summary = json.loads(result.artifacts["candidate_summary"].read_text(encoding="utf-8"))
    design_manifest = json.loads(result.artifacts["candidate_design_manifest"].read_text(encoding="utf-8"))
    assert summary["design_contract_valid"] is True
    assert summary["design_layer_required_before_formula"] is True
    assert summary["formula_is_measurement_not_thesis"] is True
    assert design_manifest["candidate_validation_allowed"] is True
    assert "market_pain_point" in design_manifest["design_contract"]
    assert set(summary["variant_validation_status"]) == {
        "raw_candidate_score",
        "industry_neutral_score",
        "capacity_filtered_score",
    }
    assert summary["candidate_validation_status"] in {
        "passed_initial_diagnostic_gate",
        "mixed_initial_diagnostic_gate",
        "failed_initial_diagnostic_gate",
        "insufficient_diagnostic_evidence",
    }

    report = result.artifacts["candidate_report"].read_text(encoding="utf-8").lower()
    assert "not alpha evidence" in report
    assert "direct q2 entry: not allowed" in report


def test_momentum_low_vol_candidate_requires_daily_data(tmp_path: Path) -> None:
    manifest = _write_daily_bundle(tmp_path)
    prices = pd.read_csv(tmp_path / "adjusted_price_volume_panel.csv")
    month_end = prices.groupby("asset_id").tail(16).copy()
    month_end["date"] = pd.to_datetime(month_end["date"]).dt.to_period("M").dt.to_timestamp("M").dt.date.astype(str)
    month_end["adjusted_price_convention"] = "crsp_monthly_adjusted_fixture"
    month_end.to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)

    try:
        run_momentum_low_vol_candidate_validation(manifest, tmp_path / "candidate_validation")
    except ValueError as exc:
        assert "requires daily price-volume data" in str(exc)
    else:
        raise AssertionError("monthly data should be rejected")


def _write_momentum_low_vol_bundle(tmp_path: Path) -> Path:
    dates = pd.bdate_range("2020-01-02", "2022-05-31")
    assets = [
        ("10001", "STEADY_WINNER", 0.0018, 0.0002, "45"),
        ("10002", "HIGH_VOL_WINNER", 0.0018, 0.0100, "45"),
        ("10003", "WEAK_LOW_VOL", 0.0002, 0.0001, "20"),
        ("10004", "LOSER", -0.0004, 0.0002, "20"),
    ]
    universe = pd.DataFrame(
        [
            _membership_row(asset_id, ticker, "2020-01-01", "2022-12-31", sector)
            for asset_id, ticker, _drift, _noise, sector in assets
        ]
    )

    price_rows = []
    for asset_index, (asset_id, ticker, drift, noise, _sector) in enumerate(assets):
        price = 20.0 + asset_index * 5.0
        for day_index, date in enumerate(dates):
            shock = noise if day_index % 2 == 0 else -noise
            ret = drift + shock
            raw_open = price * (1.0 - ret / 2.0)
            price *= 1.0 + ret
            price_rows.append(
                {
                    "permno": asset_id,
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "raw_open": round(raw_open, 6),
                    "raw_close": round(price, 6),
                    "adjusted_open": round(raw_open, 6),
                    "adjusted_close": round(price, 6),
                    "volume": 1_000_000 + asset_index * 100_000 + day_index * 10,
                    "return": ret,
                    "adjusted_price_convention": "crsp_dsf_v2_daily_fixture",
                }
            )

    qqq_rows = []
    qqq_price = 100.0
    for day_index, date in enumerate(dates):
        ret = 0.0007 + 0.0002 * ((day_index % 5) - 2)
        raw_open = qqq_price * (1.0 - ret / 2.0)
        qqq_price *= 1.0 + ret
        qqq_rows.append(
            {
                "date": date.date().isoformat(),
                "benchmark": "QQQ",
                "raw_open": round(raw_open, 6),
                "raw_close": round(qqq_price, 6),
                "adjusted_open": round(raw_open, 6),
                "adjusted_close": round(qqq_price, 6),
                "volume": 5_000_000 + day_index * 100,
                "return": ret,
                "adjusted_price_convention": "crsp_dsf_v2_daily_fixture",
            }
        )

    pd.DataFrame(price_rows).to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)
    pd.DataFrame(qqq_rows).to_csv(tmp_path / "qqq_benchmark_panel.csv", index=False)
    universe.to_csv(tmp_path / "historical_universe_membership.csv", index=False)
    pd.DataFrame(columns=["permno", "asset_id", "delisting_date", "delisting_return"]).to_csv(
        tmp_path / "delisting_returns.csv",
        index=False,
    )
    manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        """
schema_version: research_mode_dataset_manifest.v1
mode: research_mode
allowed_use_mode: formal_research
content_hash: momentum-low-vol-fixture
source_provenance:
  provider: wrds
  as_of_timestamp: "2026-05-08"
  license_mode: local_research_subscription
universe:
  path: historical_universe_membership.csv
  constituent_mode: historical_membership
  source: wrds_fixture
  source_is_pit: true
prices:
  path: adjusted_price_volume_panel.csv
  source: wrds_crsp
  adjusted: true
benchmark:
  path: qqq_benchmark_panel.csv
  benchmark_id: QQQ
  source: wrds_crsp
delisting:
  handling: explicit_file
  path: delisting_returns.csv
trading_calendar:
  path: adjusted_price_volume_panel.csv
  source: wrds_crsp_trading_dates
timestamp_policy:
  signal: month_end_close
  visibility: after_month_end_close
  tradable: next_session_close
  allow_same_close_trading: false
non_claims:
  production_approval: false
  live_trading: false
  security_orders: false
  direct_q2_entry: false
""".lstrip(),
        encoding="utf-8",
    )
    return manifest


def _membership_row(asset_id: str, ticker: str, start: str, end: str, sector: str) -> dict[str, object]:
    return {
        "permno": asset_id,
        "asset_id": asset_id,
        "ticker": ticker,
        "membership_start": start,
        "membership_end": end,
        "as_of_timestamp": start,
        "sector": sector,
        "industry": f"{sector}_industry",
        "source_is_pit": True,
    }
