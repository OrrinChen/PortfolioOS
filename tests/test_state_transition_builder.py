from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.data.builders.common import builder_manifest_path
from portfolio_os.data.builders.state_transition_builder import (
    build_state_transition_daily_panel_frame,
    build_state_transition_daily_panel_manifest,
    write_state_transition_daily_panel_csv,
)
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json


class StateTransitionHistoryProvider:
    provider_name = "state_transition_history_test"
    provider_metadata = {
        "provider_token_source": "cli",
        "approximation_notes": {
            "state_transition_daily_panel": [
                "industry and issuer_total_shares are treated as static end-date reference fields."
            ]
        },
    }

    def get_state_transition_daily_panel(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        _ = (tickers, start_date, end_date)
        return pd.DataFrame(
            [
                {
                    "date": "2026-04-01",
                    "ticker": "000001",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.9,
                    "close": 11.0,
                    "volume": 1_000_000.0,
                    "amount": 10_500_000.0,
                    "upper_limit_price": 11.0,
                    "lower_limit_price": 9.0,
                    "tradable": True,
                    "industry": "Industrials",
                    "issuer_total_shares": 10_000_000.0,
                },
                {
                    "date": "2026-04-02",
                    "ticker": "000001",
                    "open": 11.1,
                    "high": 11.2,
                    "low": 10.8,
                    "close": 10.9,
                    "volume": 900_000.0,
                    "amount": 9_900_000.0,
                    "upper_limit_price": 12.1,
                    "lower_limit_price": 9.9,
                    "tradable": True,
                    "industry": "Industrials",
                    "issuer_total_shares": 10_000_000.0,
                },
            ]
        )

    def get_capability_report(self, feed_name: str):
        _ = feed_name
        return {
            "provider_capability_status": "available",
            "fallback_notes": [],
            "fallback_chain_used": [],
            "data_source_mix": ["test_provider"],
            "permission_notes": [],
            "recommended_alternative_path": None,
        }


class NoStateTransitionProvider:
    provider_name = "no_state_transition"
    provider_metadata = {}


def test_state_transition_daily_panel_builder_outputs_valid_csv_and_manifest(
    project_root: Path,
    tmp_path: Path,
) -> None:
    provider = StateTransitionHistoryProvider()
    tickers_file = project_root / "data" / "sample" / "tickers.txt"
    tickers = ["600519", "300750"]

    frame = build_state_transition_daily_panel_frame(
        provider=provider,
        tickers=tickers,
        start_date="2026-04-01",
        end_date="2026-04-02",
    )

    output_path = tmp_path / "state_transition_daily_panel.csv"
    write_state_transition_daily_panel_csv(frame, output_path)
    manifest = build_state_transition_daily_panel_manifest(
        provider=provider,
        start_date="2026-04-01",
        end_date="2026-04-02",
        tickers_file=tickers_file,
        output_path=output_path,
        tickers=tickers,
        frame=frame,
    )
    write_json(builder_manifest_path(output_path), manifest)

    assert list(frame.columns) == [
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "upper_limit_price",
        "lower_limit_price",
        "tradable",
        "industry",
        "issuer_total_shares",
    ]
    assert frame.loc[0, "ticker"] == "000001"
    assert manifest["provider"] == "state_transition_history_test"
    assert manifest["request_parameters"]["start_date"] == "2026-04-01"
    assert manifest["request_parameters"]["end_date"] == "2026-04-02"
    assert manifest["output_sha256"]


def test_state_transition_daily_panel_builder_rejects_missing_provider_capability() -> None:
    with pytest.raises(InputValidationError, match="does not support state-transition daily panel history"):
        build_state_transition_daily_panel_frame(
            provider=NoStateTransitionProvider(),
            tickers=["000001"],
            start_date="2026-04-01",
            end_date="2026-04-02",
        )

