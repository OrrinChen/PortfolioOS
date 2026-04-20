from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.data.builders.common import builder_manifest_path
from portfolio_os.data.builders.market_builder import build_market_frame, load_tickers_file
from portfolio_os.data.builders.market_builder import build_market_manifest, write_market_csv
from portfolio_os.data.builders.reference_builder import (
    build_reference_frame,
    build_reference_manifest,
    write_reference_csv,
)
from portfolio_os.data.builders.target_builder import build_target_frame, build_target_manifest, write_target_csv
from portfolio_os.data.providers import get_data_provider
from portfolio_os.domain.errors import InputValidationError, ProviderPermissionError
from portfolio_os.storage.snapshots import write_json


class HighWeightProvider:
    provider_name = "high_weight_test"

    def get_daily_market_snapshot(self, tickers: list[str], as_of_date: str):
        raise NotImplementedError

    def get_reference_snapshot(self, tickers: list[str], as_of_date: str):
        raise NotImplementedError

    def get_index_weights(self, index_code: str, as_of_date: str):
        from portfolio_os.data.providers.base import IndexWeightRow

        return [
            IndexWeightRow(ticker="600519", target_weight=0.7),
            IndexWeightRow(ticker="300750", target_weight=0.4),
        ]


class PermissionLimitedTargetProvider:
    provider_name = "permission_limited"
    provider_metadata = {
        "provider_token_source": "cli",
        "approximation_notes": {"target": []},
    }

    def get_index_weights(self, index_code: str, as_of_date: str):
        raise ProviderPermissionError("index_weight permission denied")

    def get_capability_report(self, feed_name: str):
        return {
            "provider_capability_status": "unavailable",
            "fallback_notes": ["index_weight_permission_missing"],
            "permission_notes": ["index_weight_permission_missing"],
            "recommended_alternative_path": "provide_target_csv_and_continue",
        }


def test_market_builder_outputs_valid_market_csv(project_root: Path) -> None:
    provider = get_data_provider("mock")
    tickers = load_tickers_file(project_root / "data" / "sample" / "tickers.txt")

    frame = build_market_frame(
        provider=provider,
        tickers=tickers,
        as_of_date="2026-03-23",
    )

    assert list(frame.columns) == [
        "ticker",
        "close",
        "vwap",
        "adv_shares",
        "tradable",
        "upper_limit_hit",
        "lower_limit_hit",
    ]
    assert not frame.empty
    assert set(frame["ticker"]) >= {"600519", "601012"}


def test_builder_manifests_include_provider_date_and_output_hash(project_root: Path, tmp_path: Path) -> None:
    provider = get_data_provider("mock")
    tickers_file = project_root / "data" / "sample" / "tickers.txt"
    tickers = load_tickers_file(tickers_file)

    market_output = tmp_path / "market.csv"
    market_frame = build_market_frame(provider=provider, tickers=tickers, as_of_date="2026-03-23")
    write_market_csv(market_frame, market_output)
    market_manifest = build_market_manifest(
        provider=provider,
        as_of_date="2026-03-23",
        tickers_file=tickers_file,
        output_path=market_output,
        tickers=tickers,
    )
    write_json(builder_manifest_path(market_output), market_manifest)

    reference_output = tmp_path / "reference.csv"
    reference_frame = build_reference_frame(
        provider=provider,
        tickers=tickers,
        as_of_date="2026-03-23",
        overlay_path=project_root / "data" / "sample" / "reference_overlay_example.csv",
    )
    write_reference_csv(reference_frame, reference_output)
    reference_manifest = build_reference_manifest(
        provider=provider,
        as_of_date="2026-03-23",
        tickers_file=tickers_file,
        overlay_path=project_root / "data" / "sample" / "reference_overlay_example.csv",
        output_path=reference_output,
        frame=reference_frame,
    )
    write_json(builder_manifest_path(reference_output), reference_manifest)

    target_output = tmp_path / "target.csv"
    target_frame, target_details = build_target_frame(
        provider=provider,
        index_code="000300.SH",
        as_of_date="2026-03-23",
    )
    write_target_csv(target_frame, target_output)
    target_manifest = build_target_manifest(
        provider=provider,
        as_of_date="2026-03-23",
        index_code="000300.SH",
        output_path=target_output,
        details=target_details,
        frame=target_frame,
    )

    assert market_manifest["provider"] == "mock"
    assert market_manifest["as_of_date"] == "2026-03-23"
    assert market_manifest["output_sha256"]
    assert reference_manifest["provider"] == "mock"
    assert reference_manifest["row_count"] == len(reference_frame)
    assert target_manifest["provider"] == "mock"
    assert target_manifest["output_sha256"]
    assert target_manifest["output_weight_sum"] == pytest.approx(1.0)


def test_reference_builder_outputs_valid_reference_csv_and_overlay_merge(project_root: Path) -> None:
    provider = get_data_provider("mock")
    tickers = load_tickers_file(project_root / "data" / "sample" / "tickers.txt")
    overlay_path = project_root / "data" / "sample" / "reference_overlay_example.csv"

    frame = build_reference_frame(
        provider=provider,
        tickers=tickers,
        as_of_date="2026-03-23",
        overlay_path=overlay_path,
    )

    assert list(frame.columns) == [
        "ticker",
        "industry",
        "blacklist_buy",
        "blacklist_sell",
        "benchmark_weight",
        "manager_aggregate_qty",
        "issuer_total_shares",
    ]
    row_601012 = frame.loc[frame["ticker"] == "601012"].iloc[0]
    row_000333 = frame.loc[frame["ticker"] == "000333"].iloc[0]
    assert bool(row_601012["blacklist_buy"]) is True
    assert float(row_000333["manager_aggregate_qty"]) == pytest.approx(7500.0)


def test_target_builder_outputs_valid_target_csv_and_weight_sum(project_root: Path) -> None:
    provider = get_data_provider("mock")

    frame, manifest = build_target_frame(
        provider=provider,
        index_code="000300.SH",
        as_of_date="2026-03-23",
    )

    assert list(frame.columns) == ["ticker", "target_weight"]
    assert not frame.empty
    assert frame["target_weight"].sum() == pytest.approx(1.0)
    assert manifest["normalized"] is False


def test_target_builder_rejects_weight_sum_far_above_one() -> None:
    with pytest.raises(InputValidationError, match="above the allowed tolerance"):
        build_target_frame(
            provider=HighWeightProvider(),
            index_code="TEST",
            as_of_date="2026-03-23",
        )


def test_failure_manifest_records_build_status_and_alternative_path(tmp_path: Path) -> None:
    provider = PermissionLimitedTargetProvider()
    output_path = tmp_path / "target.csv"

    manifest = build_target_manifest(
        provider=provider,
        as_of_date="2026-03-23",
        index_code="000300.SH",
        output_path=output_path,
        details={
            "index_code": "000300.SH",
            "input_weight_sum": None,
            "output_weight_sum": None,
            "normalized": False,
            "normalization_tolerance": 0.02,
        },
        frame=pd.DataFrame(columns=["ticker", "target_weight"]),
        build_status="failed_permission",
        error_message="index_weight permission denied",
    )

    assert manifest["build_status"] == "failed_permission"
    assert manifest["provider_capability_status"] == "unavailable"
    assert "index_weight_permission_missing" in manifest["fallback_notes"]
    assert manifest["recommended_alternative_path"] == "provide_target_csv_and_continue"
