from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from multifactor_alpha_validation.real_dataset_dry_run import run_real_dataset_dry_run


def test_real_dataset_dry_run_writes_readiness_artifacts_only(tmp_path: Path) -> None:
    manifest = _write_ready_monthly_bundle(tmp_path)

    result = run_real_dataset_dry_run(manifest, tmp_path / "dry_run")

    assert result.preflight_ready is True
    assert result.dataset_frequency == "monthly"
    assert result.daily_price_volume_required_for_final_validation is True
    assert result.daily_price_volume_validation_started is False
    assert result.allocator_ran is False
    assert result.factor_ranking_ran is False
    assert result.strategy_return_claimed is False
    assert result.alpha_conclusion_claimed is False

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["not_alpha_evidence"] is True
    assert summary["daily_price_volume_validation_status"] == "separate_long_task_not_started"
    assert summary["allocator_ran"] is False
    assert summary["factor_ranking_ran"] is False
    assert summary["strategy_return_claimed"] is False
    assert summary["alpha_conclusion_claimed"] is False

    coverage = pd.read_csv(result.coverage_path)
    assert set(coverage["readiness_check_only"]) == {True}
    assert coverage["qqq_benchmark_aligned"].all()
    assert coverage["coverage_ratio"].min() > 0

    timestamps = pd.read_csv(result.timestamp_alignment_path)
    usable_timestamps = timestamps[timestamps["tradable_date_proxy"].notna()]
    assert set(usable_timestamps["same_close_trading_used"]) == {False}
    assert set(timestamps["daily_next_session_proof"]) == {False}
    assert set(timestamps["daily_price_volume_validation_required"]) == {True}

    signal_availability = pd.read_csv(result.signal_availability_path)
    reversal = signal_availability[signal_availability["factor_id"] == "reversal_5_1"].iloc[0]
    low_vol = signal_availability[signal_availability["factor_id"] == "low_vol_60d"].iloc[0]
    assert reversal["monthly_bundle_status"] == "not_validated_from_monthly_bundle"
    assert low_vol["monthly_bundle_status"] == "not_validated_from_monthly_bundle"
    assert bool(reversal["daily_validation_required_for_factor"]) is True
    assert bool(low_vol["daily_validation_required_for_factor"]) is True

    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "does not rank factors" in report
    assert "claim strategy returns" in report
    assert "claim alpha success" in report
    assert "enter q2" in report


def test_real_dataset_dry_run_blocks_unready_manifest(tmp_path: Path) -> None:
    manifest = _write_ready_monthly_bundle(tmp_path)
    text = manifest.read_text(encoding="utf-8")
    manifest.write_text(text.replace("allow_same_close_trading: false", "allow_same_close_trading: true"), encoding="utf-8")

    with pytest.raises(ValueError, match="research preflight is blocked"):
        run_real_dataset_dry_run(manifest, tmp_path / "dry_run")


def test_daily_price_volume_long_task_config_contains_no_credentials() -> None:
    config = Path("projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_price_volume_long_task.yaml")

    text = config.read_text(encoding="utf-8").lower()

    assert "requires_explicit_run: true" in text
    assert ("status: not_started" in text) or ("status: completed_local_wrds_pull" in text)
    assert "password" not in text
    assert "secret" not in text
    assert "api_key" not in text


def _write_ready_monthly_bundle(tmp_path: Path) -> Path:
    dates = pd.date_range("2020-01-31", periods=14, freq="ME")
    universe = pd.DataFrame(
        [
            {
                "permno": "10001",
                "asset_id": "10001",
                "ticker": "AAA",
                "membership_start": "2020-01-01",
                "membership_end": "2021-03-31",
                "as_of_timestamp": "2020-01-01",
                "date": "2020-01-01",
                "in_universe": True,
                "entry_date": "2020-01-01",
                "exit_date": "",
                "source": "wrds_fixture",
                "source_is_pit": True,
            },
            {
                "permno": "10002",
                "asset_id": "10002",
                "ticker": "BBB",
                "membership_start": "2020-01-01",
                "membership_end": "2020-10-31",
                "as_of_timestamp": "2020-01-01",
                "date": "2020-01-01",
                "in_universe": True,
                "entry_date": "2020-01-01",
                "exit_date": "2020-10-31",
                "source": "wrds_fixture",
                "source_is_pit": True,
            },
            {
                "permno": "10003",
                "asset_id": "10003",
                "ticker": "CCC",
                "membership_start": "2020-03-01",
                "membership_end": "2021-03-31",
                "as_of_timestamp": "2020-03-01",
                "date": "2020-03-01",
                "in_universe": True,
                "entry_date": "2020-03-01",
                "exit_date": "",
                "source": "wrds_fixture",
                "source_is_pit": True,
            },
        ]
    )
    price_rows = []
    for index, date in enumerate(dates):
        for asset_id, ticker, start_index in (("10001", "AAA", 0), ("10002", "BBB", 0), ("10003", "CCC", 2)):
            if index < start_index:
                continue
            if asset_id == "10002" and date > pd.Timestamp("2020-10-31"):
                continue
            price_rows.append(
                {
                    "permno": asset_id,
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_open": 20.0 + index,
                    "adjusted_close": 20.5 + index,
                    "volume": 1000000 + index,
                    "return": 0.01,
                    "adjusted_price_convention": "crsp_monthly_adjusted_fixture",
                }
            )
    prices = pd.DataFrame(price_rows)
    benchmark = pd.DataFrame(
        [
            {
                "date": date.date().isoformat(),
                "benchmark": "QQQ",
                "adjusted_open": 100.0 + index,
                "adjusted_close": 101.0 + index,
                "volume": 2000000 + index,
                "return": 0.01,
                "adjusted_price_convention": "crsp_monthly_adjusted_fixture",
            }
            for index, date in enumerate(dates)
        ]
    )
    delistings = pd.DataFrame(
        [
            {
                "permno": "10002",
                "asset_id": "10002",
                "delisting_date": "2020-11-02",
                "delisting_return": -0.05,
                "inactive_reason": "DLST",
                "last_trade_date": "2020-10-30",
            }
        ]
    )

    universe.to_csv(tmp_path / "historical_universe_membership.csv", index=False)
    prices.to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)
    benchmark.to_csv(tmp_path / "qqq_benchmark_panel.csv", index=False)
    delistings.to_csv(tmp_path / "delisting_returns.csv", index=False)
    manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        """
schema_version: research_mode_dataset_manifest.v1
mode: research_mode
allowed_use_mode: formal_research
content_hash: ready-monthly-fixture
source_provenance:
  provider: wrds
  as_of_timestamp: "2026-05-06"
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
