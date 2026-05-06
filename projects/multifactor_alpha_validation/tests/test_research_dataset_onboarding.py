from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from multifactor_alpha_validation.research_dataset import (
    ResearchDatasetError,
    validate_adjusted_price_volume_and_benchmark,
    load_historical_universe_membership,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _historical_membership_path(tmp_path: Path) -> Path:
    path = tmp_path / "historical_membership.csv"
    _write_csv(
        path,
        [
            {
                "date": "2010-01-01",
                "asset_id": "AAPL",
                "ticker": "AAPL",
                "in_universe": True,
                "entry_date": "2000-01-01",
                "exit_date": "",
                "source": "pit_index_vendor",
            },
            {
                "date": "2010-01-01",
                "asset_id": "OLD",
                "ticker": "OLD",
                "in_universe": True,
                "entry_date": "2009-01-01",
                "exit_date": "2011-12-31",
                "source": "pit_index_vendor",
            },
            {
                "date": "2012-01-01",
                "asset_id": "NEW",
                "ticker": "NEW",
                "in_universe": True,
                "entry_date": "2012-01-01",
                "exit_date": "",
                "source": "pit_index_vendor",
            },
        ],
    )
    return path


def test_historical_universe_loader_writes_point_in_time_snapshots(tmp_path: Path) -> None:
    output_dir = tmp_path / "research_dataset"

    result = load_historical_universe_membership(
        _historical_membership_path(tmp_path),
        rebalance_dates=["2011-06-30", "2013-06-30"],
        output_dir=output_dir,
    )

    assert result.snapshot_counts == {"2011-06-30": 2, "2013-06-30": 2}
    first_snapshot = output_dir / "universe_snapshots" / "universe_snapshot_2011-06-30.csv"
    second_snapshot = output_dir / "universe_snapshots" / "universe_snapshot_2013-06-30.csv"
    assert first_snapshot.exists()
    assert second_snapshot.exists()
    assert "OLD" in first_snapshot.read_text()
    assert "OLD" not in second_snapshot.read_text()
    validation = json.loads((output_dir / "historical_membership_validation.json").read_text())
    assert validation["schema_version"] == "historical_membership_validation.v1"
    assert validation["current_constituent_backfill_blocked"] is True
    assert validation["snapshots"]["2011-06-30"]["asset_ids"] == ["AAPL", "OLD"]
    assert validation["snapshots"]["2013-06-30"]["asset_ids"] == ["AAPL", "NEW"]


def test_historical_universe_loader_rejects_current_constituent_backfill(tmp_path: Path) -> None:
    path = tmp_path / "current_constituents.csv"
    _write_csv(
        path,
        [
            {
                "date": "2026-03-31",
                "asset_id": "AAPL",
                "ticker": "AAPL",
                "in_universe": True,
                "entry_date": "2000-01-01",
                "exit_date": "",
                "source": "current_constituents",
            }
        ],
    )

    with pytest.raises(ResearchDatasetError, match="current constituents cannot be backfilled"):
        load_historical_universe_membership(
            path,
            rebalance_dates=["2011-06-30"],
            output_dir=tmp_path / "outputs",
        )


def _price_path(tmp_path: Path, include_old: bool = True) -> Path:
    rows = [
        {
            "date": "2011-06-30",
            "asset_id": "AAPL",
            "adjusted_open": 9.9,
            "adjusted_close": 10.0,
            "volume": 1000000,
        },
        {
            "date": "2013-06-30",
            "asset_id": "AAPL",
            "adjusted_open": 12.9,
            "adjusted_close": 13.0,
            "volume": 1200000,
        },
        {
            "date": "2013-06-30",
            "asset_id": "NEW",
            "adjusted_open": 19.5,
            "adjusted_close": 20.0,
            "volume": 800000,
        },
    ]
    if include_old:
        rows.append(
            {
                "date": "2011-06-30",
                "asset_id": "OLD",
                "adjusted_open": 4.8,
                "adjusted_close": 5.0,
                "volume": 500000,
            }
        )
    path = tmp_path / "adjusted_prices.csv"
    _write_csv(path, rows)
    return path


def _benchmark_path(tmp_path: Path) -> Path:
    path = tmp_path / "qqq.csv"
    _write_csv(
        path,
        [
            {"date": "2011-06-30", "adjusted_open": 50.0, "adjusted_close": 51.0, "volume": 50000000},
            {"date": "2013-06-30", "adjusted_open": 60.0, "adjusted_close": 62.0, "volume": 60000000},
        ],
    )
    return path


def test_adjusted_price_and_benchmark_panel_cover_universe_snapshots(tmp_path: Path) -> None:
    universe = load_historical_universe_membership(
        _historical_membership_path(tmp_path),
        rebalance_dates=["2011-06-30", "2013-06-30"],
        output_dir=tmp_path / "research_dataset",
    )

    result = validate_adjusted_price_volume_and_benchmark(
        _price_path(tmp_path),
        _benchmark_path(tmp_path),
        universe.snapshot_paths,
        output_dir=tmp_path / "research_dataset",
        adjusted_price_convention="split_dividend_adjusted",
    )

    assert result.price_panel_ready is True
    assert result.missing_price_rows == 0
    assert result.benchmark_ready is True
    assert result.adjusted_price_convention == "split_dividend_adjusted"
    assert Path(result.coverage_report_path).exists()
    assert Path(result.benchmark_report_path).exists()


def test_missing_price_data_becomes_abstain_not_zero_fill(tmp_path: Path) -> None:
    universe = load_historical_universe_membership(
        _historical_membership_path(tmp_path),
        rebalance_dates=["2011-06-30", "2013-06-30"],
        output_dir=tmp_path / "research_dataset",
    )

    result = validate_adjusted_price_volume_and_benchmark(
        _price_path(tmp_path, include_old=False),
        _benchmark_path(tmp_path),
        universe.snapshot_paths,
        output_dir=tmp_path / "research_dataset",
        adjusted_price_convention="split_dividend_adjusted",
    )

    abstain_text = Path(result.abstain_report_path).read_text()
    assert result.price_panel_ready is False
    assert result.missing_price_rows == 1
    assert "OLD" in abstain_text
    assert "missing_adjusted_price_volume" in abstain_text
    assert ",0," not in abstain_text
