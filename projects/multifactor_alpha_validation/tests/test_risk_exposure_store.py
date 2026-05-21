from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.risk_exposure_store import run_pit_exposure_store


def test_pit_exposure_store_builds_timestamped_trailing_and_fundamental_exposures(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research")
    fundamentals_manifest = _write_fundamentals_bundle(tmp_path / "fundamentals")

    result = run_pit_exposure_store(research_manifest, fundamentals_manifest, tmp_path / "risk_model")

    exposures = pd.read_csv(result.exposure_panel_path)
    required_columns = {
        "schema_version",
        "date",
        "asset_id",
        "exposure_name",
        "exposure_value",
        "exposure_date",
        "visibility_timestamp",
        "tradable_timestamp",
        "source",
        "coverage_flag",
        "abstain_reason",
        "lookback_start_date",
        "lookback_end_date",
        "not_alpha_evidence",
    }
    assert required_columns.issubset(exposures.columns)
    assert result.exposure_count == len(exposures)
    assert result.date_count > 0
    assert result.production_approval is False

    covered = exposures[exposures["coverage_flag"].astype(bool)]
    assert not covered.empty
    assert (
        pd.to_datetime(covered["exposure_date"])
        <= pd.to_datetime(covered["visibility_timestamp"])
    ).all()
    assert (
        pd.to_datetime(covered["visibility_timestamp"])
        <= pd.to_datetime(covered["tradable_timestamp"])
    ).all()
    assert set(exposures["not_alpha_evidence"].astype(bool)) == {True}

    beta = covered[covered["exposure_name"].eq("trailing_market_beta_252d")]
    assert not beta.empty
    assert (pd.to_datetime(beta["lookback_end_date"]) <= pd.to_datetime(beta["date"])).all()
    assert (pd.to_datetime(beta["lookback_start_date"]) < pd.to_datetime(beta["lookback_end_date"])).all()

    fundamental = covered[
        (covered["asset_id"].astype(str).eq("10001"))
        & (covered["exposure_name"].eq("fundamental_book_to_market"))
    ].sort_values("date")
    assert not fundamental.empty
    assert set(fundamental["source"]) == {"wrds_comp_fundq"}
    assert (pd.to_datetime(fundamental["visibility_timestamp"]) <= pd.to_datetime(fundamental["date"])).all()
    assert (pd.to_datetime(fundamental["exposure_date"]) < pd.to_datetime(fundamental["date"])).all()

    missing_fundamental = exposures[
        (exposures["asset_id"].astype(str).eq("10003"))
        & (exposures["exposure_name"].eq("fundamental_profitability_roa"))
    ]
    assert not missing_fundamental.empty
    assert set(missing_fundamental["coverage_flag"].astype(bool)) == {False}
    assert set(missing_fundamental["abstain_reason"]) == {"no_visible_fundamental_row"}
    assert missing_fundamental["exposure_value"].isna().all()

    coverage = json.loads(Path(result.coverage_report_path).read_text(encoding="utf-8"))
    assert coverage["schema_version"] == "pit_exposure_coverage_report.v1"
    assert coverage["non_claims"]["not_alpha_evidence"] is True
    assert coverage["coverage_by_exposure"]["fundamental_book_to_market"]["covered_rows"] > 0

    manifest = yaml.safe_load(Path(result.exposure_manifest_path).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "pit_exposure_store_manifest.v1"
    assert manifest["allowed_use_mode"] == "risk_attribution_input_only"
    assert manifest["non_claims"]["production_approval"] is False
    assert "style_neutral_alpha" not in Path(result.exposure_manifest_path).read_text(encoding="utf-8")


def test_pit_exposure_store_records_missing_price_exposure_as_abstain(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research", drop_asset_history="10002")
    fundamentals_manifest = _write_fundamentals_bundle(tmp_path / "fundamentals")

    result = run_pit_exposure_store(research_manifest, fundamentals_manifest, tmp_path / "risk_model")

    exposures = pd.read_csv(result.exposure_panel_path)
    missing_beta = exposures[
        (exposures["asset_id"].astype(str).eq("10002"))
        & (exposures["exposure_name"].eq("trailing_market_beta_252d"))
    ]
    assert not missing_beta.empty
    assert set(missing_beta["coverage_flag"].astype(bool)) == {False}
    assert "insufficient_trailing_price_history" in set(missing_beta["abstain_reason"])
    assert missing_beta["exposure_value"].isna().all()


def _write_research_bundle(root: Path, drop_asset_history: str | None = None) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    dates = pd.bdate_range("2019-01-02", "2021-03-31")
    assets = [
        ("10001", "001001", "AAA", 0.0005, 0.0010),
        ("10002", "001002", "BBB", 0.0002, -0.0004),
        ("10003", "001003", "CCC", -0.0001, 0.0007),
    ]
    universe = pd.DataFrame(
        [
            {
                "permno": asset_id,
                "asset_id": asset_id,
                "ticker": ticker,
                "gvkey": gvkey,
                "membership_start": "2019-01-01",
                "membership_end": "2021-12-31",
                "as_of_timestamp": "2019-01-01",
                "date": "2019-01-01",
                "in_universe": True,
                "entry_date": "2019-01-01",
                "exit_date": "",
                "sector": "45" if index != 1 else "35",
                "industry": "451030" if index != 1 else "352010",
                "source": "wrds_fixture",
                "source_is_pit": True,
            }
            for index, (asset_id, gvkey, ticker, _, _) in enumerate(assets)
        ]
    )
    price_rows: list[dict[str, object]] = []
    for asset_index, (asset_id, _, ticker, drift, cycle) in enumerate(assets):
        price = 20.0 + asset_index
        asset_dates = dates
        if drop_asset_history == asset_id:
            asset_dates = dates[-30:]
        for index, dt in enumerate(asset_dates):
            ret = drift + (cycle if index % 21 < 10 else -cycle / 2)
            adjusted_open = price
            price = max(price * (1.0 + ret), 1.0)
            shares = 1_000_000 + asset_index * 100_000
            price_rows.append(
                {
                    "permno": asset_id,
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "date": dt.date().isoformat(),
                    "adjusted_open": round(adjusted_open, 6),
                    "adjusted_close": round(price, 6),
                    "volume": 1_000_000 + index * 10 + asset_index,
                    "dlycap": round(price * shares, 6),
                    "shrout": shares,
                    "dlyprcvol": round(price * (1_000_000 + index * 10 + asset_index), 6),
                    "return": ret,
                    "adjusted_price_convention": "crsp_daily_adjusted_fixture",
                }
            )
    benchmark_price = 100.0
    benchmark_rows: list[dict[str, object]] = []
    for index, dt in enumerate(dates):
        ret = 0.0003 + (0.0004 if index % 19 < 9 else -0.0002)
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
                "adjusted_price_convention": "crsp_daily_adjusted_fixture",
            }
        )
    universe.to_csv(root / "historical_universe_membership.csv", index=False)
    pd.DataFrame(price_rows).to_csv(root / "adjusted_price_volume_panel.csv", index=False)
    pd.DataFrame(benchmark_rows).to_csv(root / "qqq_benchmark_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "asset_id": "10003",
                "delisting_date": "2022-01-03",
                "delisting_return": -0.05,
                "inactive_reason": "fixture_exit",
                "last_trade_date": "2021-12-31",
            }
        ]
    ).to_csv(root / "delisting_returns.csv", index=False)
    manifest = root / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        """
schema_version: research_mode_dataset_manifest.v1
mode: research_mode
allowed_use_mode: formal_research
content_hash: ready-risk-exposure-fixture
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


def _write_fundamentals_bundle(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    quarterly = pd.DataFrame(
        [
            {
                "gvkey": "001001",
                "datadate": "2019-03-31",
                "rdq": "2019-05-10",
                "visibility_timestamp": "2019-05-10",
                "tradable_timestamp": "2019-05-13",
                "visibility_source": "rdq",
                "atq": 90.0,
                "ceqq": 42.0,
                "saleq": 30.0,
                "niq": 4.0,
            },
            {
                "gvkey": "001001",
                "datadate": "2020-03-31",
                "rdq": "2020-05-15",
                "visibility_timestamp": "2020-05-15",
                "tradable_timestamp": "2020-05-18",
                "visibility_source": "rdq",
                "atq": 100.0,
                "ceqq": 50.0,
                "saleq": 35.0,
                "niq": 6.0,
            },
            {
                "gvkey": "001002",
                "datadate": "2020-03-31",
                "rdq": "",
                "visibility_timestamp": "2020-06-29",
                "tradable_timestamp": "2020-06-30",
                "visibility_source": "datadate_plus_90d_lag",
                "atq": 80.0,
                "ceqq": 30.0,
                "saleq": 22.0,
                "niq": 2.0,
            },
            {
                "gvkey": "001003",
                "datadate": "2021-12-31",
                "rdq": "2022-02-15",
                "visibility_timestamp": "2022-02-15",
                "tradable_timestamp": "2022-02-16",
                "visibility_source": "rdq",
                "atq": 70.0,
                "ceqq": 20.0,
                "saleq": 18.0,
                "niq": 1.0,
            },
        ]
    )
    annual = quarterly.rename(columns={column: column.rstrip("q") for column in quarterly.columns if column.endswith("q")})
    ccm = pd.DataFrame(
        [
            {"gvkey": "001001", "lpermno": 10001, "linkdt": "2018-01-01", "linkenddt": ""},
            {"gvkey": "001002", "lpermno": 10002, "linkdt": "2018-01-01", "linkenddt": ""},
            {"gvkey": "001003", "lpermno": 10003, "linkdt": "2018-01-01", "linkenddt": ""},
        ]
    )
    quarterly.to_csv(root / "quarterly_fundamentals_panel.csv", index=False)
    annual.to_csv(root / "annual_fundamentals_panel.csv", index=False)
    ccm.to_csv(root / "ccm_link_history.csv", index=False)
    manifest = root / "fundamentals_manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "schema_version": "wrds_fundamentals_manifest.v1",
                "paths": {
                    "quarterly_fundamentals_panel": "quarterly_fundamentals_panel.csv",
                    "annual_fundamentals_panel": "annual_fundamentals_panel.csv",
                    "ccm_link_history": "ccm_link_history.csv",
                },
                "timestamp_policy": {
                    "quarterly_visibility": "rdq_when_rdq_on_or_after_datadate_else_datadate_plus_90d_lag",
                    "same_close_trading": False,
                },
                "allowed_use_mode": "risk_attribution_input_only",
                "non_claims": {
                    "not_alpha_evidence": True,
                    "production_approval": False,
                    "live_trading": False,
                    "security_orders": False,
                    "direct_q2_entry": False,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest
