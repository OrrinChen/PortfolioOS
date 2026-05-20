from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.eightk_wrds_market_rescue import (
    run_eightk_wrds_market_rescue,
)


def test_eightk_wrds_market_rescue_writes_bounded_price_cache_without_downstream(tmp_path: Path) -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "ticker": "ACGL",
                "eightk_subtype": "auditor_change",
                "tradable_timestamp": "2023-08-25T13:30:00+00:00",
                "coverage_state": "no_view",
            },
            {
                "event_id": "e2",
                "ticker": "FUTR",
                "eightk_subtype": "ceo_departure",
                "tradable_timestamp": "2025-03-03T13:30:00+00:00",
                "coverage_state": "no_view",
            },
            {
                "event_id": "e3",
                "ticker": "UNK",
                "eightk_subtype": "unknown_no_view",
                "tradable_timestamp": "2023-08-25T13:30:00+00:00",
                "coverage_state": "no_view",
            },
        ],
    )
    event_path = tmp_path / "eightk_event_registry_real.csv"
    events.to_csv(event_path, index=False)

    result = run_eightk_wrds_market_rescue(
        event_registry_path=event_path,
        output_path=tmp_path / "rescued_prices.csv",
        manifest_path=tmp_path / "manifest.json",
        connection=FakeWrdsConnection(),
        wrds_max_date="2024-12-31",
        max_events=10,
    )

    assert result["status"] == "completed"
    assert result["eligible_event_count"] == 1
    assert result["skipped_after_wrds_max_date"] == 1
    assert result["source_table"] == "crsp.dsf"
    assert result["q1_entry_allowed"] is False
    assert result["q2_entry_allowed"] is False
    assert result["measurement_spec_written"] is False
    assert result["expected_return_panel_written"] is False
    assert result["production_approval_claimed"] is False

    prices = pd.read_csv(tmp_path / "rescued_prices.csv")
    assert set(prices.columns) >= {"permno", "ticker", "date", "adjusted_close", "return", "market_cap"}
    assert prices["ticker"].eq("ACGL").all()
    assert len(prices) == 2

    manifest = (tmp_path / "manifest.json").read_text(encoding="utf-8")
    assert "not_alpha_evidence" in manifest
    assert "production_approval_claimed" in manifest


class FakeWrdsConnection:
    def raw_sql(self, query: str) -> pd.DataFrame:
        lower = query.lower()
        if "from crsp.stocknames" in lower:
            return pd.DataFrame(
                [
                    {
                        "permno": 82276,
                        "ticker": "ACGL",
                        "namedt": "2000-11-10",
                        "nameenddt": "2024-12-31",
                    },
                ],
            )
        if "from crsp.dsf" in lower:
            return pd.DataFrame(
                [
                    {
                        "permno": 82276,
                        "date": "2023-08-24",
                        "prc": 77.0,
                        "ret": 0.01,
                        "vol": 1000,
                        "shrout": 10_000,
                    },
                    {
                        "permno": 82276,
                        "date": "2023-08-25",
                        "prc": 78.0,
                        "ret": 0.012,
                        "vol": 1200,
                        "shrout": 10_000,
                    },
                ],
            )
        raise AssertionError(query)
