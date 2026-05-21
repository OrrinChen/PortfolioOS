from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from factor_discovery_sandbox.real_data_validation import run_real_data_validation_r0_r2


def test_real_data_validation_r0_r2_writes_admission_universe_and_return_artifacts(tmp_path: Path) -> None:
    manifest = _write_ready_monthly_bundle(tmp_path)

    result = run_real_data_validation_r0_r2(manifest, tmp_path / "real_data")

    assert result.summary["schema_version"] == "fd_real_data_validation_r0_r2.v1"
    assert result.summary["admission_status"] == "admitted_for_monthly_pit_r0_r2"
    assert result.summary["historical_constituents"] is True
    assert result.summary["current_constituent_backfill_detected"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["factor_ranking_ran"] is False
    assert result.summary["allocator_ran"] is False
    assert result.summary["alpha_success_claimed"] is False
    assert {
        "data_admission_report",
        "data_manifest",
        "data_quality_summary",
        "pit_universe_panel",
        "universe_coverage_report",
        "symbol_mapping_audit",
        "survivorship_bias_audit",
        "returns_panel",
        "benchmark_returns",
        "corporate_action_audit",
        "return_quality_report",
    } == set(result.artifacts)

    data_manifest = json.loads(result.artifacts["data_manifest"].read_text(encoding="utf-8"))
    assert data_manifest["dataset_id"] == "wrds_nasdaq100_monthly_pit"
    assert data_manifest["has_historical_constituents"] is True
    assert data_manifest["has_adjusted_prices"] is True
    assert data_manifest["has_benchmark"] is True
    assert data_manifest["has_delisted_names"] is True
    assert data_manifest["is_pit_safe"] is True
    assert data_manifest["full_daily_price_volume_ready"] is False

    quality = pd.read_csv(result.artifacts["data_quality_summary"])
    assert set(quality["check_name"]) >= {
        "historical_constituents",
        "adjusted_prices",
        "benchmark",
        "delisting_records",
        "daily_price_volume",
    }
    assert quality.loc[quality["check_name"] == "daily_price_volume", "status"].iloc[0] == "warning"

    universe = pd.read_csv(result.artifacts["pit_universe_panel"])
    assert {"date", "asset_id", "membership_start", "membership_end", "is_member_asof_date"}.issubset(
        universe.columns
    )
    assert universe["is_member_asof_date"].all()
    assert "2020-11-30" not in set(universe[universe["asset_id"] == "10002"]["date"])

    coverage = pd.read_csv(result.artifacts["universe_coverage_report"])
    assert coverage["coverage_ratio"].min() > 0
    assert coverage["not_alpha_evidence"].all()

    returns = pd.read_csv(result.artifacts["returns_panel"])
    assert {"adjusted_close_return", "return_quality_status", "not_alpha_evidence"}.issubset(returns.columns)
    assert returns["adjusted_close_return"].notna().any()

    benchmark = pd.read_csv(result.artifacts["benchmark_returns"])
    assert {"date", "benchmark", "benchmark_return", "benchmark_alignment_status"}.issubset(benchmark.columns)
    assert set(benchmark["benchmark_alignment_status"]) == {"aligned"}

    corporate_action = pd.read_csv(result.artifacts["corporate_action_audit"])
    assert {"adjusted_price_convention", "zero_volume_rows", "extreme_return_rows"}.issubset(
        corporate_action.columns
    )

    report = result.artifacts["data_admission_report"].read_text(encoding="utf-8").lower()
    assert "not alpha evidence" in report
    assert "factor ranking: not run" in report
    assert "allocator: not run" in report


def test_real_data_validation_blocks_current_constituent_manifest(tmp_path: Path) -> None:
    manifest = _write_ready_monthly_bundle(tmp_path)
    text = manifest.read_text(encoding="utf-8")
    manifest.write_text(
        text.replace("constituent_mode: historical_membership", "constituent_mode: current_constituents"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="FD real-data admission blocked"):
        run_real_data_validation_r0_r2(manifest, tmp_path / "real_data")


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
    pd.DataFrame(price_rows).to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)
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
