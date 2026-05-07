from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.real_evidence_closeout import run_real_evidence_closeout
from multifactor_alpha_validation.real_rolling_oos import run_first_real_rolling_oos_evidence


def test_real_rolling_oos_blocks_monthly_bundle_without_alpha_claims(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path / "monthly", frequency="monthly")

    result = run_first_real_rolling_oos_evidence(manifest, tmp_path / "r8")

    assert result.oos_status == "needs_daily_price_volume"
    assert result.preflight_ready is True
    assert result.uses_full_sample_icir is False
    assert result.alpha_success_claimed is False
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    assert summary["decision_blocker"] == "daily_price_volume_required"
    assert summary["not_alpha_evidence"] is True
    report = Path(result.readiness_path).read_text(encoding="utf-8").lower()
    assert "daily price-volume" in report
    assert "does not claim alpha" in report


def test_real_rolling_oos_daily_bundle_uses_prior_history_and_separate_readouts(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path / "daily", frequency="daily")

    result = run_first_real_rolling_oos_evidence(manifest, tmp_path / "r8")

    assert result.oos_status == "evidence_ready"
    assert result.preflight_ready is True
    assert result.uses_full_sample_icir is False
    assert result.alpha_success_claimed is False

    evidence = pd.read_csv(result.evidence_path)
    assert set(evidence["full_sample_icir_used"]) == {False}
    assert set(evidence["prior_history_only"]) == {True}
    assert {"raw_rank_ic_mean", "qqq_relative_spread_mean", "beta_adjusted_spread_mean", "net_spread_mean"}.issubset(
        evidence.columns
    )

    observations = pd.read_csv(result.observation_path)
    assert (pd.to_datetime(observations["history_cutoff_date"]) < pd.to_datetime(observations["rebalance_date"])).all()
    assert (pd.to_datetime(observations["tradable_date"]) > pd.to_datetime(observations["signal_date"])).all()
    assert set(observations["same_close_trading_used"]) == {False}

    exposure = pd.read_csv(result.exposure_path)
    assert {"sector", "liquidity_score_60d", "volatility_score_60d", "style_source"}.issubset(exposure.columns)
    assert set(exposure["style_source"]) == {"price_volume_proxy"}

    neutralization = pd.read_csv(result.neutralization_path)
    assert set(neutralization["sector_adjusted_status"]) == {"observed"}
    assert set(neutralization["style_adjusted_status"]) == {"observed_price_volume_proxy"}
    benchmark = pd.read_csv(result.benchmark_attribution_path)
    assert {
        "raw_spread_mean",
        "qqq_relative_spread_mean",
        "beta_adjusted_spread_mean",
        "sector_adjusted_spread_mean",
        "style_adjusted_spread_mean",
    }.issubset(benchmark.columns)


def test_real_evidence_closeout_is_diagnostic_when_attribution_is_incomplete(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path / "daily", frequency="daily")
    r8 = run_first_real_rolling_oos_evidence(manifest, tmp_path / "r8")

    closeout = run_real_evidence_closeout(Path(r8.output_dir), tmp_path / "r9")

    assert closeout.decision == "diagnostic_only"
    assert closeout.production_approval is False
    assert closeout.live_trading is False
    assert closeout.direct_q2_entry is False
    decision = json.loads(Path(closeout.decision_path).read_text(encoding="utf-8"))
    assert decision["decision"] == "diagnostic_only"
    assert "sector_attribution_unavailable" not in decision["decision_reasons"]
    assert "style_attribution_unavailable" not in decision["decision_reasons"]
    assert "style_proxy_only" in decision["decision_reasons"]
    report = Path(closeout.report_path).read_text(encoding="utf-8").lower()
    assert "no production approval" in report
    assert "not enter allocator" in report


def _write_bundle(root: Path, frequency: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    if frequency == "daily":
        dates = pd.bdate_range("2019-01-02", "2021-12-31")
        convention = "crsp_daily_adjusted_fixture"
    else:
        dates = pd.date_range("2019-01-31", "2021-12-31", freq="ME")
        convention = "crsp_monthly_adjusted_fixture"

    assets = [
        ("10001", "AAA", 0.0006, 0.0010),
        ("10002", "BBB", 0.0003, -0.0005),
        ("10003", "CCC", -0.0002, 0.0008),
        ("10004", "DDD", 0.0008, -0.0010),
        ("10005", "EEE", -0.0001, 0.0003),
        ("10006", "FFF", 0.0004, -0.0002),
    ]
    universe = pd.DataFrame(
        [
            {
                "permno": asset_id,
                "asset_id": asset_id,
                "ticker": ticker,
                "membership_start": "2019-01-01",
                "membership_end": "2021-12-31",
                "as_of_timestamp": "2019-01-01",
                "date": "2019-01-01",
                "in_universe": True,
                "entry_date": "2019-01-01",
                "exit_date": "",
                "sector": "45" if index % 2 == 0 else "35",
                "industry": "4510" if index % 2 == 0 else "3520",
                "source": "wrds_fixture",
                "source_is_pit": True,
            }
            for index, (asset_id, ticker, _, _) in enumerate(assets)
        ]
    )
    price_rows: list[dict[str, object]] = []
    for asset_index, (asset_id, ticker, drift, cycle) in enumerate(assets):
        price = 20.0 + asset_index
        for index, dt in enumerate(dates):
            if frequency == "daily" and asset_id == "10006" and index == 0:
                continue
            seasonal = cycle if index % 21 < 10 else -cycle / 2
            ret = drift + seasonal
            adjusted_open = price
            price = max(price * (1.0 + ret), 1.0)
            price_rows.append(
                {
                    "permno": asset_id,
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "date": dt.date().isoformat(),
                    "adjusted_open": round(adjusted_open, 6),
                    "adjusted_close": round(price, 6),
                    "volume": 1_000_000 + index * 10 + asset_index,
                    "return": ret,
                    "adjusted_price_convention": convention,
                }
            )
    prices = pd.DataFrame(price_rows)
    benchmark_price = 100.0
    benchmark_rows = []
    for index, dt in enumerate(dates):
        ret = 0.0002 + (0.0004 if index % 19 < 9 else -0.0002)
        adjusted_open = benchmark_price
        benchmark_price *= 1.0 + ret
        benchmark_rows.append(
            {
                "date": dt.date().isoformat(),
                "benchmark": "QQQ",
                "adjusted_open": round(adjusted_open, 6),
                "adjusted_close": round(benchmark_price, 6),
                "volume": 5_000_000 + index,
                "return": ret,
                "adjusted_price_convention": convention,
            }
        )
    benchmark = pd.DataFrame(benchmark_rows)
    delistings = pd.DataFrame(
        [
            {
                "permno": "10006",
                "asset_id": "10006",
                "delisting_date": "2022-01-03",
                "delisting_return": -0.05,
                "inactive_reason": "fixture_exit",
                "last_trade_date": "2021-12-31",
            }
        ]
    )

    universe.to_csv(root / "historical_universe_membership.csv", index=False)
    prices.to_csv(root / "adjusted_price_volume_panel.csv", index=False)
    benchmark.to_csv(root / "qqq_benchmark_panel.csv", index=False)
    delistings.to_csv(root / "delisting_returns.csv", index=False)
    manifest = root / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        """
schema_version: research_mode_dataset_manifest.v1
mode: research_mode
allowed_use_mode: formal_research
content_hash: ready-real-oos-fixture
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
