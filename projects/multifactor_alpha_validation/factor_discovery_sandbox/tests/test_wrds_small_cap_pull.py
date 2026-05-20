from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from factor_discovery_sandbox.small_cap_data_admission import run_small_cap_data_admission
from factor_discovery_sandbox.wrds_small_cap_pull import run_wrds_small_cap_pull


class _FakeWRDSConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def raw_sql(self, query: str) -> pd.DataFrame:
        self.queries.append(query)
        lower = query.lower()
        if "from crsp_a_stock.dsenames" in lower and "stkdelists" not in lower and "select distinct" not in lower:
            return pd.DataFrame(
                [
                    {
                        "permno": 10001,
                        "asset_id": "10001",
                        "ticker": "AAA",
                        "membership_start": "2019-01-01",
                        "membership_end": "2024-12-31",
                        "as_of_timestamp": "2019-01-01",
                        "date": "2019-01-01",
                        "in_universe": True,
                        "entry_date": "2019-01-01",
                        "exit_date": "",
                        "share_code": 10,
                        "exchange_code": 1,
                        "common_share": True,
                        "sic": 3571,
                        "naics": 334111,
                    }
                ]
            )
        if "stkdelists" in lower:
            return pd.DataFrame(
                [
                    {
                        "permno": 10001,
                        "asset_id": "10001",
                        "delisting_date": "2024-06-03",
                        "delisting_return": -0.42,
                        "inactive_reason": "delisted",
                        "last_trade_date": "2024-05-31",
                    }
                ]
            )
        if "d.permno = 88222" in lower:
            return pd.DataFrame(
                [
                    {
                        "date": "2020-01-31",
                        "benchmark": "IWM",
                        "raw_open": 160.0,
                        "raw_close": 161.0,
                        "adjusted_open": 160.0,
                        "adjusted_close": 161.0,
                        "volume": 10_000_000,
                        "return": 0.01,
                        "adjusted_price_convention": "crsp_dsf_v2_dlyprc_div_dlycumfacpr_daily",
                    }
                ]
            )
        if "from crsp_a_stock.dsf_v2" in lower:
            return pd.DataFrame(
                [
                    {
                        "permno": 10001,
                        "asset_id": "10001",
                        "ticker": "AAA",
                        "date": "2020-01-31",
                        "raw_open": 10.0,
                        "raw_close": 10.5,
                        "adjusted_open": 10.0,
                        "adjusted_close": 10.5,
                        "volume": 500_000,
                        "return": 0.02,
                        "market_cap": 210_000_000.0,
                        "shares_outstanding": 20_000_000.0,
                        "dollar_volume": 5_250_000.0,
                        "bid_ask_spread": 0.002,
                        "high": 10.7,
                        "low": 9.9,
                        "share_code": 10,
                        "exchange_code": 1,
                        "common_share": True,
                        "sic": 3571,
                        "naics": 334111,
                        "adjusted_price_convention": "crsp_dsf_v2_dlyprc_div_dlycumfacpr_daily",
                    }
                ]
            )
        raise AssertionError(query)

    def close(self) -> None:
        self.closed = True


def test_wrds_small_cap_pull_writes_manifest_with_required_small_cap_fields(tmp_path: Path) -> None:
    connection = _FakeWRDSConnection()

    result = run_wrds_small_cap_pull(
        output_root=tmp_path / "small_cap_us_daily",
        output_dir=tmp_path / "outputs",
        start_date="2020-01-01",
        end_date="2020-12-31",
        price_start_date="2019-06-01",
        connection_factory=lambda: connection,
    )

    assert connection.closed is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.manifest_path.exists()
    payload = yaml.safe_load(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "wrds_us_small_cap_daily_v1"
    assert payload["benchmark"]["benchmark_id"] == "IWM"
    assert payload["non_claims"]["direct_q2_entry"] is False

    prices = pd.read_csv(result.standardized_files["adjusted_price_volume_panel"])
    assert {
        "market_cap",
        "shares_outstanding",
        "exchange_code",
        "share_code",
        "common_share",
        "bid_ask_spread",
        "sector",
        "industry",
    }.issubset(prices.columns)

    admission = run_small_cap_data_admission(result.manifest_path, tmp_path / "admission")
    report = json.loads(admission.artifacts["data_admission_report"].read_text(encoding="utf-8"))
    assert report["failed_required_checks"] == []


def test_wrds_small_cap_pull_uses_common_share_and_exchange_filters(tmp_path: Path) -> None:
    connection = _FakeWRDSConnection()

    run_wrds_small_cap_pull(
        output_root=tmp_path / "small_cap_us_daily",
        output_dir=tmp_path / "outputs",
        start_date="2020-01-01",
        end_date="2020-12-31",
        price_start_date="2019-06-01",
        connection_factory=lambda: connection,
    )

    all_sql = "\n".join(connection.queries).lower()
    assert "shrcd in (10, 11)" in all_sql
    assert "exchcd in (1, 2, 3)" in all_sql
    assert "crsp_a_stock.dsf_v2" in all_sql
    assert "crsp_a_stock.stkdelists" in all_sql
    assert "d.permno = 88222" in all_sql
    assert "password" not in all_sql
