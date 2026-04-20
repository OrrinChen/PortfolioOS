from __future__ import annotations

import json
from pathlib import Path

import pytest

from portfolio_os.data.builders.snapshot_builder import build_snapshot_bundle
from portfolio_os.data.providers import get_data_provider
from portfolio_os.data.providers.mock import MOCK_MARKET_DATA, MOCK_REFERENCE_DATA
from portfolio_os.domain.errors import InputValidationError, ProviderPermissionError


class PartialSnapshotProvider:
    provider_name = "partial_snapshot"
    provider_metadata = {
        "provider_token_source": "cli",
        "approximation_notes": {
            "market": [],
            "reference": [],
            "target": [],
        },
    }

    def get_daily_market_snapshot(self, tickers: list[str], as_of_date: str):
        _ = as_of_date
        return [MOCK_MARKET_DATA[ticker] for ticker in tickers]

    def get_reference_snapshot(self, tickers: list[str], as_of_date: str):
        _ = as_of_date
        return [MOCK_REFERENCE_DATA[ticker] for ticker in tickers]

    def get_index_weights(self, index_code: str, as_of_date: str):
        _ = index_code
        _ = as_of_date
        raise ProviderPermissionError("index_weight permission denied")

    def get_capability_report(self, feed_name: str):
        if feed_name == "target":
            return {
                "provider_capability_status": "unavailable",
                "fallback_notes": ["index_weight_permission_missing"],
                "permission_notes": ["index_weight_permission_missing"],
                "recommended_alternative_path": "provide_target_csv_and_continue",
            }
        return {
            "provider_capability_status": "available",
            "fallback_notes": [],
            "permission_notes": [],
            "recommended_alternative_path": None,
        }


def test_snapshot_builder_creates_full_bundle(project_root: Path, tmp_path: Path) -> None:
    provider = get_data_provider("mock")
    output_dir = tmp_path / "snapshot_bundle"

    bundle = build_snapshot_bundle(
        provider=provider,
        tickers_file=project_root / "data" / "sample" / "tickers.txt",
        index_code="000300.SH",
        as_of_date="2026-03-23",
        output_dir=output_dir,
        reference_overlay=project_root / "data" / "sample" / "reference_overlay_example.csv",
    )

    expected_files = {
        "market_path",
        "reference_path",
        "target_path",
        "market_manifest_path",
        "reference_manifest_path",
        "target_manifest_path",
        "snapshot_manifest_path",
    }
    assert expected_files.issubset(set(bundle.keys()))
    for key in expected_files:
        assert Path(bundle[key]).exists()

    with Path(bundle["snapshot_manifest_path"]).open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert manifest["provider"] == "mock"
    assert manifest["child_manifests"]["market"]["sha256"]
    assert manifest["output_files"]["target"]["sha256"]


def test_snapshot_builder_preserves_partial_outputs_and_failure_manifest(project_root: Path, tmp_path: Path) -> None:
    provider = PartialSnapshotProvider()
    output_dir = tmp_path / "snapshot_partial"

    with pytest.raises(InputValidationError, match="partial"):
        build_snapshot_bundle(
            provider=provider,
            tickers_file=project_root / "data" / "sample" / "tickers.txt",
            index_code="000300.SH",
            as_of_date="2026-03-23",
            output_dir=output_dir,
            reference_overlay=project_root / "data" / "sample" / "reference_overlay_example.csv",
        )

    assert (output_dir / "market.csv").exists()
    assert (output_dir / "reference.csv").exists()
    assert (output_dir / "market_manifest.json").exists()
    assert (output_dir / "reference_manifest.json").exists()
    assert (output_dir / "target_manifest.json").exists()
    assert not (output_dir / "target.csv").exists()
    with (output_dir / "snapshot_manifest.json").open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert manifest["steps"]["market"]["build_status"] == "success"
    assert manifest["steps"]["reference"]["build_status"] == "success"
    assert manifest["steps"]["target"]["build_status"] == "failed_permission"
    assert manifest["recommended_alternative_path"] == "provide_target_csv_and_continue"


def test_snapshot_builder_allow_partial_build_returns_bundle(project_root: Path, tmp_path: Path) -> None:
    provider = PartialSnapshotProvider()
    output_dir = tmp_path / "snapshot_partial_allowed"

    bundle = build_snapshot_bundle(
        provider=provider,
        tickers_file=project_root / "data" / "sample" / "tickers.txt",
        index_code="000300.SH",
        as_of_date="2026-03-23",
        output_dir=output_dir,
        reference_overlay=project_root / "data" / "sample" / "reference_overlay_example.csv",
        allow_partial_build=True,
    )

    assert bundle["build_status"] == "success_with_degradation"
    assert bundle["target_path"] is None
    with Path(bundle["snapshot_manifest_path"]).open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert manifest["build_status"] == "success_with_degradation"
