from __future__ import annotations

import csv
import json
from pathlib import Path

from multifactor_alpha_validation.data_contract import run_research_mode_preflight


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _valid_manifest(tmp_path: Path) -> dict[str, object]:
    universe_path = tmp_path / "historical_universe.csv"
    price_path = tmp_path / "prices.csv"
    benchmark_path = tmp_path / "benchmark.csv"
    delisting_path = tmp_path / "delistings.csv"
    calendar_path = tmp_path / "trading_calendar.csv"
    _write_csv(
        universe_path,
        [
            {
                "ticker": "AAPL",
                "membership_start": "2010-01-01",
                "membership_end": "",
                "as_of_timestamp": "2010-01-01T00:00:00Z",
                "source": "pit_index_vendor",
                "source_is_pit": "true",
            }
        ],
    )
    _write_csv(
        price_path,
        [{"date": "2010-01-29", "ticker": "AAPL", "adjusted_close": 10.0, "volume": 1000000}],
    )
    _write_csv(
        benchmark_path,
        [{"date": "2010-01-29", "benchmark": "QQQ", "adjusted_close": 42.0}],
    )
    _write_csv(
        delisting_path,
        [{"ticker": "DELISTED", "delisting_date": "2011-06-30", "delisting_return": -0.35}],
    )
    _write_csv(
        calendar_path,
        [{"date": "2010-01-29", "is_trading_day": True}],
    )
    return {
        "schema_version": "research_mode_dataset_manifest.v1",
        "mode": "research_mode",
        "allowed_use_mode": "formal_research",
        "content_hash": "fixture-content-hash",
        "source_provenance": {
            "provider": "pit_index_vendor",
            "as_of_timestamp": "2026-03-31T00:00:00Z",
            "license_mode": "local_research_fixture",
        },
        "universe": {
            "path": str(universe_path),
            "constituent_mode": "historical_membership",
            "source": "pit_index_vendor",
            "source_is_pit": True,
        },
        "prices": {
            "path": str(price_path),
            "source": "adjusted_price_vendor",
            "adjusted": True,
        },
        "benchmark": {
            "path": str(benchmark_path),
            "benchmark_id": "QQQ",
            "source": "adjusted_price_vendor",
        },
        "delisting": {
            "handling": "explicit_file",
            "path": str(delisting_path),
        },
        "trading_calendar": {
            "path": str(calendar_path),
            "source": "exchange_calendar_vendor",
        },
        "timestamp_policy": {
            "signal": "month_end_close",
            "visibility": "after_month_end_close",
            "tradable": "next_session_close",
            "allow_same_close_trading": False,
        },
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def test_research_mode_preflight_accepts_historical_pit_dataset(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"

    result = run_research_mode_preflight(_valid_manifest(tmp_path), output_dir)

    assert result.research_mode_ready is True
    assert result.blockers == ()
    assert result.rows_checked["historical_universe"] == 1
    assert result.non_claims["direct_q2_entry"] is False
    validation = json.loads((output_dir / "pit_contract_validation.json").read_text())
    assert validation["research_mode_ready"] is True
    assert (output_dir / "pit_universe_report.csv").exists()
    assert "Research mode is ready for PIT-safe validation" in (
        output_dir / "research_mode_readiness.md"
    ).read_text()


def test_research_mode_preflight_rejects_yfinance_current_constituents(tmp_path: Path) -> None:
    manifest = _valid_manifest(tmp_path)
    universe = manifest["universe"]
    assert isinstance(universe, dict)
    universe["constituent_mode"] = "current_constituents"
    universe["source"] = "yfinance"
    universe["source_is_pit"] = False
    manifest["delisting"] = {"handling": "missing"}

    result = run_research_mode_preflight(manifest, tmp_path / "outputs")

    assert result.research_mode_ready is False
    assert "historical universe membership is required" in result.blockers
    assert "current constituents are survivorship-biased" in result.blockers
    assert "yfinance cannot certify PIT historical index membership" in result.blockers
    assert "delisting handling must be explicit before research mode" in result.blockers
    readiness = (tmp_path / "outputs" / "research_mode_readiness.md").read_text()
    assert "blocked" in readiness.lower()
    assert "not alpha evidence" in readiness.lower()


def test_research_mode_preflight_rejects_same_close_trading(tmp_path: Path) -> None:
    manifest = _valid_manifest(tmp_path)
    timestamp_policy = manifest["timestamp_policy"]
    assert isinstance(timestamp_policy, dict)
    timestamp_policy["allow_same_close_trading"] = True
    timestamp_policy["tradable"] = "same_close"

    result = run_research_mode_preflight(manifest, tmp_path / "outputs")

    assert result.research_mode_ready is False
    assert "same-close trading is not allowed" in result.blockers


def test_research_mode_preflight_rejects_missing_required_manifest_sections(tmp_path: Path) -> None:
    manifest = _valid_manifest(tmp_path)
    manifest.pop("allowed_use_mode")
    manifest.pop("content_hash")
    manifest.pop("source_provenance")
    manifest.pop("trading_calendar")

    result = run_research_mode_preflight(manifest, tmp_path / "outputs")

    assert result.research_mode_ready is False
    assert "allowed use mode must be formal_research" in result.blockers
    assert "dataset content hash is required" in result.blockers
    assert "source provenance is required" in result.blockers
    assert "trading calendar path is required" in result.blockers


def test_research_mode_preflight_rejects_missing_core_panels(tmp_path: Path) -> None:
    manifest = _valid_manifest(tmp_path)
    manifest["universe"] = {
        "constituent_mode": "historical_membership",
        "source": "pit_index_vendor",
        "source_is_pit": True,
    }
    manifest["prices"] = {"source": "adjusted_price_vendor", "adjusted": True}
    manifest["benchmark"] = {"benchmark_id": "QQQ", "source": "adjusted_price_vendor"}
    manifest["delisting"] = {"handling": "explicit_file"}

    result = run_research_mode_preflight(manifest, tmp_path / "outputs")

    assert result.research_mode_ready is False
    assert "historical universe path is required" in result.blockers
    assert "price panel path is required" in result.blockers
    assert "benchmark path is required" in result.blockers
    assert "delisting panel path is required" in result.blockers


def test_research_mode_preflight_accepts_permno_identifier_without_ticker(tmp_path: Path) -> None:
    manifest = _valid_manifest(tmp_path)
    universe_path = tmp_path / "historical_universe_permno.csv"
    price_path = tmp_path / "prices_permno.csv"
    delisting_path = tmp_path / "delistings_permno.csv"
    _write_csv(
        universe_path,
        [
            {
                "permno": 14593,
                "membership_start": "2010-01-01",
                "membership_end": "",
                "as_of_timestamp": "2010-01-01T00:00:00Z",
                "source": "wrds_index_constituents",
                "source_is_pit": "true",
            }
        ],
    )
    _write_csv(
        price_path,
        [{"date": "2010-01-29", "permno": 14593, "adjusted_close": 10.0, "volume": 1000000}],
    )
    _write_csv(
        delisting_path,
        [{"permno": 10000, "delisting_date": "2011-06-30", "delisting_return": -0.35}],
    )
    universe = manifest["universe"]
    prices = manifest["prices"]
    delisting = manifest["delisting"]
    assert isinstance(universe, dict)
    assert isinstance(prices, dict)
    assert isinstance(delisting, dict)
    universe["path"] = str(universe_path)
    universe["source"] = "wrds_index_constituents"
    prices["path"] = str(price_path)
    delisting["path"] = str(delisting_path)

    result = run_research_mode_preflight(manifest, tmp_path / "outputs")

    assert result.research_mode_ready is True
    assert result.blockers == ()
