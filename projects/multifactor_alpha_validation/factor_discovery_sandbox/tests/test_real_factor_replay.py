from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from factor_discovery_sandbox.real_factor_replay import run_real_factor_replay


def test_real_factor_replay_writes_daily_factor_panel_with_timestamp_audit(tmp_path: Path) -> None:
    manifest = _write_daily_bundle(tmp_path)

    result = run_real_factor_replay(manifest, tmp_path / "fd_r3")

    assert result.summary["schema_version"] == "fd_real_factor_replay_summary.v1"
    assert result.summary["stage"] == "FD-R3"
    assert result.summary["dataset_frequency"] == "daily"
    assert result.summary["factor_count"] == 29
    assert result.summary["factor_ranking_ran"] is False
    assert result.summary["allocator_ran"] is False
    assert result.summary["alpha_success_claimed"] is False
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["no_view_is_not_zero_alpha"] is True

    expected_artifacts = {
        "real_factor_panel",
        "real_factor_coverage",
        "real_factor_timestamp_audit",
        "real_factor_replay_report",
        "real_factor_replay_summary",
        "parquet_status",
    }
    assert expected_artifacts == set(result.artifacts)

    panel = pd.read_csv(result.artifacts["real_factor_panel"])
    required_columns = {
        "factor_id",
        "date",
        "asset_id",
        "raw_value",
        "oriented_score",
        "cross_sectional_rank",
        "formula_version",
        "formula_hash",
        "mechanism_family",
        "fallback_used",
        "fallback_reason",
        "research_evidence_quality",
        "normalized_value",
        "coverage_status",
        "abstain_reason",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "lookback_start",
        "lookback_end",
        "skip_days",
        "lookback_observations",
        "no_view_is_not_zero_alpha",
        "not_alpha_evidence",
    }
    assert required_columns.issubset(panel.columns)
    assert panel["factor_id"].nunique() == 29
    assert set(panel["formula_version"]) == {"price_volume_29_mechanism_v2"}
    assert panel["formula_hash"].notna().all()
    assert "active_view" in set(panel["coverage_status"])
    assert "explicit_abstain" in set(panel["coverage_status"])
    assert panel.loc[panel["coverage_status"] == "explicit_abstain", "abstain_reason"].notna().all()
    assert panel["no_view_is_not_zero_alpha"].all()
    assert panel["not_alpha_evidence"].all()
    active = panel[panel["coverage_status"] == "active_view"]
    assert active["oriented_score"].notna().all()
    assert active["cross_sectional_rank"].between(0.0, 1.0).all()

    pivot = active.pivot_table(index=["date", "asset_id"], columns="factor_id", values="normalized_value")
    assert pivot["residual_momentum_6m"].corr(pivot["momentum_6m"], method="spearman") < 0.999
    assert pivot["trend_slope_3m"].corr(pivot["momentum_3m"], method="spearman") < 0.999
    assert pivot["drawdown_3m"].corr(pivot["price_to_high_3m"], method="spearman") < 0.999
    assert pivot["reversal_1m"].corr(-pivot["momentum_1m"], method="spearman") < 0.999

    timestamps = pd.read_csv(result.artifacts["real_factor_timestamp_audit"])
    assert timestamps["same_close_trading_used"].eq(False).all()
    assert timestamps["tradable_after_signal"].eq(True).all()
    assert timestamps["timestamp_contract_status"].eq("passed").all()

    coverage = pd.read_csv(result.artifacts["real_factor_coverage"])
    assert {"factor_id", "covered_rows", "abstain_rows", "coverage_ratio"}.issubset(coverage.columns)
    assert coverage["coverage_ratio"].between(0.0, 1.0).all()
    assert coverage["not_alpha_evidence"].all()

    parquet_status = json.loads(result.artifacts["parquet_status"].read_text(encoding="utf-8"))
    assert parquet_status["parquet_written"] is False
    assert "pyarrow" in parquet_status["reason"] or "fastparquet" in parquet_status["reason"]

    report = result.artifacts["real_factor_replay_report"].read_text(encoding="utf-8").lower()
    assert "not alpha evidence" in report
    assert "factor ranking: not run" in report
    assert "allocator: not run" in report


def test_real_factor_replay_requires_daily_price_volume(tmp_path: Path) -> None:
    manifest = _write_daily_bundle(tmp_path)
    prices = pd.read_csv(tmp_path / "adjusted_price_volume_panel.csv")
    month_end = prices.groupby("asset_id").tail(16).copy()
    month_end["date"] = pd.to_datetime(month_end["date"]).dt.to_period("M").dt.to_timestamp("M").dt.date.astype(str)
    month_end["adjusted_price_convention"] = "crsp_monthly_adjusted_fixture"
    month_end.to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)

    with pytest.raises(ValueError, match="FD-R3 requires daily price-volume data"):
        run_real_factor_replay(manifest, tmp_path / "fd_r3")


def _write_daily_bundle(tmp_path: Path) -> Path:
    dates = pd.bdate_range("2020-01-02", "2021-05-31")
    universe = pd.DataFrame(
        [
            _membership_row("10001", "AAA", "2020-01-01", "2021-12-31", "45"),
            _membership_row("10002", "BBB", "2020-01-01", "2021-12-31", "45"),
            _membership_row("10003", "CCC", "2020-04-01", "2021-12-31", "20"),
            _membership_row("10004", "DDD", "2020-04-01", "2021-12-31", "20"),
        ]
    )
    drift_by_asset = {
        "10001": 0.0020,
        "10002": 0.0018,
        "10003": 0.0016,
        "10004": -0.0005,
    }
    price_rows = []
    for asset_index, (asset_id, ticker, start, end) in enumerate(
        [
            ("10001", "AAA", pd.Timestamp("2020-01-02"), pd.Timestamp("2021-05-31")),
            ("10002", "BBB", pd.Timestamp("2020-01-02"), pd.Timestamp("2021-05-31")),
            ("10003", "CCC", pd.Timestamp("2020-04-01"), pd.Timestamp("2021-05-31")),
            ("10004", "DDD", pd.Timestamp("2020-04-01"), pd.Timestamp("2021-05-31")),
        ]
    ):
        asset_dates = [date for date in dates if start <= date <= end]
        price = 20.0 + asset_index * 5.0
        for day_index, date in enumerate(asset_dates):
            ret = drift_by_asset[asset_id] + 0.002 * ((day_index % 9) - 4) / 10.0
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
        ret = 0.0008 + 0.001 * ((day_index % 7) - 3) / 10.0
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
    delistings = pd.DataFrame(
        [
            {
                "permno": "10002",
                "asset_id": "10002",
                "delisting_date": "2021-01-04",
                "delisting_return": -0.04,
                "inactive_reason": "DLST",
                "last_trade_date": "2020-12-31",
            }
        ]
    )
    universe.to_csv(tmp_path / "historical_universe_membership.csv", index=False)
    pd.DataFrame(price_rows).to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)
    pd.DataFrame(qqq_rows).to_csv(tmp_path / "qqq_benchmark_panel.csv", index=False)
    delistings.to_csv(tmp_path / "delisting_returns.csv", index=False)

    manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        """
schema_version: research_mode_dataset_manifest.v1
mode: research_mode
allowed_use_mode: formal_research
content_hash: fd-r3-daily-fixture
source_provenance:
  provider: wrds
  as_of_timestamp: "2026-05-07"
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
        "gvkey": asset_id,
        "iid": "01",
        "membership_start": start,
        "membership_end": end,
        "as_of_timestamp": start,
        "date": start,
        "in_universe": True,
        "entry_date": start,
        "sector": sector,
        "industry": f"{sector}1010",
        "sic": "7372",
        "naics": "513210",
        "exit_date": "" if end == "2021-12-31" else end,
        "source": "wrds_fixture",
        "source_is_pit": True,
    }
