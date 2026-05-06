from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from multifactor_alpha_validation.wrds_ingest import (
    WRDSQueryConfigError,
    run_wrds_multifactor_ingest,
)


class FakeWRDSConnection:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = tables
        self.queries: list[str] = []

    def raw_sql(self, query: str) -> pd.DataFrame:
        self.queries.append(query)
        key = query.strip()
        return self.tables[key].copy()

    def close(self) -> None:
        self.queries.append("close")


def _config() -> dict[str, object]:
    return {
        "schema_version": "wrds_multifactor_query_config.v1",
        "raw_output_dir": "raw",
        "standardized_output_dir": "standardized",
        "preflight_output_dir": "preflight",
        "queries": {
            "historical_universe_membership": {"sql": "universe_sql"},
            "adjusted_price_volume_panel": {"sql": "prices_sql"},
            "qqq_benchmark_panel": {"sql": "benchmark_sql"},
            "delisting_returns": {"sql": "delisting_sql"},
        },
        "timestamp_policy": {
            "signal": "month_end_close",
            "visibility": "after_month_end_close",
            "tradable": "next_session_close",
            "allow_same_close_trading": False,
        },
    }


def _tables() -> dict[str, pd.DataFrame]:
    return {
        "universe_sql": pd.DataFrame(
            [
                {
                    "permno": 14593,
                    "membership_start": "2010-01-01",
                    "membership_end": "",
                    "as_of_timestamp": "2010-01-01T00:00:00Z",
                    "source": "wrds_index_constituents",
                    "source_is_pit": True,
                }
            ]
        ),
        "prices_sql": pd.DataFrame(
            [{"date": "2010-01-29", "permno": 14593, "adjusted_close": 10.0, "volume": 1000000, "return": 0.02}]
        ),
        "benchmark_sql": pd.DataFrame(
            [{"date": "2010-01-29", "benchmark": "QQQ", "adjusted_close": 42.0, "return": 0.01, "volume": 500000}]
        ),
        "delisting_sql": pd.DataFrame(
            [{"permno": 10000, "delisting_date": "2011-06-30", "delisting_return": -0.35, "delisting_code": 500}]
        ),
    }


def test_wrds_ingest_uses_local_connection_and_writes_preflight_ready_dataset(tmp_path: Path) -> None:
    fake = FakeWRDSConnection(_tables())

    result = run_wrds_multifactor_ingest(
        _config(),
        base_dir=tmp_path,
        connection_factory=lambda: fake,
        require_ready=True,
    )

    assert result.preflight.research_mode_ready is True
    assert result.manifest_path.exists()
    assert (tmp_path / "standardized" / "historical_universe_membership.csv").exists()
    assert (tmp_path / "standardized" / "adjusted_price_volume_panel.csv").exists()
    assert (tmp_path / "standardized" / "qqq_benchmark_panel.csv").exists()
    assert (tmp_path / "standardized" / "delisting_returns.csv").exists()
    assert fake.queries == ["universe_sql", "prices_sql", "benchmark_sql", "delisting_sql", "close"]


def test_wrds_ingest_rejects_configs_that_contain_secrets(tmp_path: Path) -> None:
    config = _config()
    config["password"] = "do-not-put-this-here"

    with pytest.raises(WRDSQueryConfigError, match="must not contain credentials"):
        run_wrds_multifactor_ingest(config, base_dir=tmp_path, connection_factory=lambda: FakeWRDSConnection(_tables()))


def test_wrds_ingest_fails_closed_when_required_query_is_missing(tmp_path: Path) -> None:
    config = _config()
    queries = config["queries"]
    assert isinstance(queries, dict)
    queries["historical_universe_membership"] = {"sql": ""}

    with pytest.raises(WRDSQueryConfigError, match="missing sql"):
        run_wrds_multifactor_ingest(config, base_dir=tmp_path, connection_factory=lambda: FakeWRDSConnection(_tables()))

