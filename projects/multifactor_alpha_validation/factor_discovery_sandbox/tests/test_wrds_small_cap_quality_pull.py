from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from factor_discovery_sandbox.wrds_small_cap_quality_pull import run_wrds_small_cap_quality_pull


class _FakeQualityConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.closed = False

    def raw_sql(self, query: str) -> pd.DataFrame:
        self.queries.append(query)
        lower = query.lower()
        if "crsp_a_ccm.ccmxpf_lnkhist" in lower:
            return pd.DataFrame(
                [
                    {
                        "gvkey": "1001",
                        "lpermno": 10001,
                        "asset_id": "10001",
                        "linkdt": "2018-01-01",
                        "linkenddt": "2024-12-31",
                        "linktype": "LC",
                        "linkprim": "P",
                    },
                    {
                        "gvkey": "1002",
                        "lpermno": 10002,
                        "asset_id": "10002",
                        "linkdt": "2018-01-01",
                        "linkenddt": "2024-12-31",
                        "linktype": "LC",
                        "linkprim": "P",
                    },
                ]
            )
        if "comp.fundq" in lower:
            return pd.DataFrame(
                [
                    {
                        "gvkey": "1001",
                        "datadate": "2019-12-31",
                        "rdq": "2020-02-15",
                        "atq": 100.0,
                        "ltq": 30.0,
                        "saleq": 60.0,
                        "revtq": 60.0,
                        "cogsq": 30.0,
                        "niq": 10.0,
                        "oibdpq": 12.0,
                    },
                    {
                        "gvkey": "1002",
                        "datadate": "2019-12-31",
                        "rdq": "2020-02-15",
                        "atq": 100.0,
                        "ltq": 80.0,
                        "saleq": 40.0,
                        "revtq": 40.0,
                        "cogsq": 35.0,
                        "niq": -2.0,
                        "oibdpq": 1.0,
                    },
                ]
            )
        raise AssertionError(query)

    def close(self) -> None:
        self.closed = True


def test_wrds_small_cap_quality_pull_writes_pit_quality_scores(tmp_path: Path) -> None:
    manifest = _write_research_manifest(tmp_path)
    connection = _FakeQualityConnection()

    result = run_wrds_small_cap_quality_pull(
        research_manifest_path=manifest,
        output_root=tmp_path / "quality",
        output_dir=tmp_path / "outputs",
        start_date="2020-01-01",
        end_date="2020-04-30",
        connection_factory=lambda: connection,
    )

    assert connection.closed is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["quality_score_rows"] > 0

    scores = pd.read_csv(result.standardized_files["quality_score_panel"])
    assert {
        "asset_id",
        "date",
        "quality_score",
        "profitability_roa",
        "gross_profitability",
        "leverage",
        "visibility_timestamp",
        "tradable_timestamp",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    }.issubset(scores.columns)
    assert scores["quality_score"].notna().any()
    assert (pd.to_datetime(scores["visibility_timestamp"]) <= pd.to_datetime(scores["date"])).all()
    assert (pd.to_datetime(scores["tradable_timestamp"]) <= pd.to_datetime(scores["date"])).all()
    assert scores["not_alpha_evidence"].eq(True).all()

    updated_manifest = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    assert "quality" in updated_manifest
    assert Path(updated_manifest["quality"]["path"]).exists()

    all_sql = "\n".join(connection.queries).lower()
    assert "crsp_a_ccm.ccmxpf_lnkhist" in all_sql
    assert "comp.fundq" in all_sql
    assert "password" not in all_sql

    summary = json.loads(result.artifacts["quality_pull_summary"].read_text(encoding="utf-8"))
    assert summary["allocator_entry_allowed"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False


def _write_research_manifest(tmp_path: Path) -> Path:
    dates = pd.bdate_range("2020-01-02", "2020-04-30")
    price_rows = []
    universe_rows = []
    for asset_id, ticker, sector in [("10001", "AAA", "tech"), ("10002", "BBB", "industrial")]:
        universe_rows.append(
            {
                "asset_id": asset_id,
                "permno": asset_id,
                "ticker": ticker,
                "membership_start": "2019-01-01",
                "membership_end": "2024-12-31",
                "as_of_timestamp": "2019-01-01",
                "date": "2019-01-01",
                "in_universe": True,
                "source_is_pit": True,
                "sector": sector,
                "industry": sector,
                "exchange_code": 1,
                "share_code": 10,
                "common_share": True,
            }
        )
        for offset, day in enumerate(dates):
            price = 10.0 + offset * 0.01 + int(asset_id[-1])
            price_rows.append(
                {
                    "asset_id": asset_id,
                    "permno": asset_id,
                    "ticker": ticker,
                    "date": day.date().isoformat(),
                    "raw_open": price,
                    "raw_close": price,
                    "adjusted_open": price,
                    "adjusted_close": price,
                    "volume": 100_000,
                    "return": 0.001,
                    "market_cap": price * 20_000_000,
                    "shares_outstanding": 20_000_000,
                    "exchange_code": 1,
                    "share_code": 10,
                    "common_share": True,
                    "sector": sector,
                    "industry": sector,
                    "bid_ask_spread": 0.002,
                    "adjusted_price_convention": "fixture_adjusted",
                }
            )
    benchmark = [
        {
            "date": day.date().isoformat(),
            "benchmark": "IWM",
            "raw_open": 100.0,
            "raw_close": 100.0,
            "adjusted_open": 100.0,
            "adjusted_close": 100.0,
            "volume": 1_000_000,
            "return": 0.0,
        }
        for day in dates
    ]
    delistings = [
        {
            "asset_id": "10002",
            "permno": "10002",
            "delisting_date": "2024-01-01",
            "delisting_return": -0.3,
            "inactive_reason": "delisted",
            "last_trade_date": "2023-12-29",
        }
    ]
    paths = {
        "prices": tmp_path / "adjusted_price_volume_panel.csv",
        "universe": tmp_path / "historical_universe_membership.csv",
        "benchmark": tmp_path / "small_cap_benchmark_panel.csv",
        "delisting": tmp_path / "delisting_returns.csv",
    }
    pd.DataFrame(price_rows).to_csv(paths["prices"], index=False)
    pd.DataFrame(universe_rows).to_csv(paths["universe"], index=False)
    pd.DataFrame(benchmark).to_csv(paths["benchmark"], index=False)
    pd.DataFrame(delistings).to_csv(paths["delisting"], index=False)
    manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "schema_version": "research_mode_dataset_manifest.v1",
                "dataset_id": "quality-test",
                "universe": {
                    "path": str(paths["universe"]),
                    "constituent_mode": "historical_membership",
                    "source_is_pit": True,
                },
                "prices": {"path": str(paths["prices"]), "adjusted": True},
                "benchmark": {"path": str(paths["benchmark"]), "benchmark_id": "IWM"},
                "delisting": {"path": str(paths["delisting"]), "handling": "explicit_file"},
                "timestamp_policy": {"allow_same_close_trading": False},
                "non_claims": {
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
