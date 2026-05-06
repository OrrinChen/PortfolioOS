from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path


class ResearchDatasetError(ValueError):
    pass


@dataclass(frozen=True)
class HistoricalUniverseResult:
    snapshot_counts: dict[str, int]
    snapshot_paths: dict[str, str]
    validation_path: str


_UNIVERSE_COLUMNS = {
    "date",
    "asset_id",
    "ticker",
    "in_universe",
    "entry_date",
    "exit_date",
    "source",
}


def load_historical_universe_membership(
    membership_path: Path,
    rebalance_dates: list[str],
    output_dir: Path,
) -> HistoricalUniverseResult:
    rows = _read_csv_rows(membership_path, _UNIVERSE_COLUMNS, "historical universe membership")
    if any(_is_current_constituent_source(row.get("source", "")) for row in rows):
        raise ResearchDatasetError("current constituents cannot be backfilled into historical universe")

    snapshot_dir = output_dir / "universe_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    snapshot_counts: dict[str, int] = {}
    snapshot_paths: dict[str, str] = {}
    snapshots_for_validation: dict[str, dict[str, object]] = {}
    for rebalance_date in rebalance_dates:
        snapshot = _snapshot_for_rebalance(rows, _parse_date(rebalance_date))
        path = snapshot_dir / f"universe_snapshot_{rebalance_date}.csv"
        _write_snapshot(path, snapshot)
        asset_ids = [row["asset_id"] for row in snapshot]
        snapshot_counts[rebalance_date] = len(snapshot)
        snapshot_paths[rebalance_date] = str(path)
        snapshots_for_validation[rebalance_date] = {
            "asset_count": len(snapshot),
            "asset_ids": asset_ids,
            "path": str(path),
        }

    validation_path = output_dir / "historical_membership_validation.json"
    validation = {
        "schema_version": "historical_membership_validation.v1",
        "membership_path": str(membership_path),
        "required_fields": sorted(_UNIVERSE_COLUMNS),
        "current_constituent_backfill_blocked": True,
        "snapshot_count": len(rebalance_dates),
        "snapshots": snapshots_for_validation,
        "non_claims": {
            "alpha_evidence": False,
            "production_approval": False,
            "direct_q2_entry": False,
        },
    }
    validation_path.write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return HistoricalUniverseResult(
        snapshot_counts=snapshot_counts,
        snapshot_paths=snapshot_paths,
        validation_path=str(validation_path),
    )


def _read_csv_rows(path: Path, required_columns: set[str], label: str) -> list[dict[str, str]]:
    if not path.exists():
        raise ResearchDatasetError(f"{label} file does not exist: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = sorted(required_columns - columns)
        if missing:
            raise ResearchDatasetError(f"{label} missing columns: {', '.join(missing)}")
        rows = [{key: str(value or "") for key, value in row.items()} for row in reader]
    if not rows:
        raise ResearchDatasetError(f"{label} has no rows")
    return rows


def _is_current_constituent_source(source: str) -> bool:
    normalized = source.strip().lower()
    return normalized in {"current_constituents", "yfinance", "current_constituent_snapshot"}


def _snapshot_for_rebalance(rows: list[dict[str, str]], rebalance_date: date) -> list[dict[str, str]]:
    visible: list[dict[str, str]] = []
    for row in rows:
        if not _as_bool(row["in_universe"]):
            continue
        record_date = _parse_date(row["date"])
        entry_date = _parse_date(row["entry_date"])
        exit_date = _parse_optional_date(row["exit_date"])
        if record_date <= rebalance_date and entry_date <= rebalance_date and (
            exit_date is None or rebalance_date <= exit_date
        ):
            visible.append(row)
    visible.sort(key=lambda row: row["asset_id"])
    return visible


def _write_snapshot(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = ["date", "asset_id", "ticker", "in_universe", "entry_date", "exit_date", "source"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ResearchDatasetError(f"invalid date: {value}") from exc


def _parse_optional_date(value: str) -> date | None:
    if not str(value).strip():
        return None
    return _parse_date(value)


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}
