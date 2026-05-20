"""Resumable CRSP daily price extraction for historical SUE panel audits.

This module only writes local WRDS/CRSP cache files. It does not evaluate alpha,
run Q2, call brokers, generate orders, or approve production use.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.provenance.hashing import canonical_json, hash_payload


DEFAULT_SUE_LINKS_PATH = "data/cache/wrds_sue_event_panel/ibes_links.csv"
DEFAULT_SUE_CRSP_DAILY_PATH = "data/cache/wrds_sue_event_panel/crsp_daily.csv"
DEFAULT_SUE_CRSP_CHUNK_DIR = "data/cache/wrds_sue_event_panel/crsp_daily_chunks"
DEFAULT_SUE_CRSP_MANIFEST_PATH = "outputs/sue_historical_event_panel_full/crsp_price_extract_manifest.json"


class WrdsLikeConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        """Run SQL and return a DataFrame."""


class SueCrspPriceExtractConfig(BaseModel):
    """Config for resumable CRSP price extraction."""

    model_config = ConfigDict(extra="forbid")

    links_path: str = DEFAULT_SUE_LINKS_PATH
    output_path: str = DEFAULT_SUE_CRSP_DAILY_PATH
    chunk_dir: str = DEFAULT_SUE_CRSP_CHUNK_DIR
    manifest_path: str = DEFAULT_SUE_CRSP_MANIFEST_PATH
    start_date: str = "2020-01-01"
    end_date: str = "2022-03-25"
    chunk_size: int = Field(default=1, gt=0)
    max_permnos: int | None = Field(default=None, gt=0)
    fetched_at: str | None = None
    source_table: str = "crsp.dsf"


def load_sue_crsp_price_extract_config(path: str | Path) -> SueCrspPriceExtractConfig:
    """Load H1A.2 CRSP price extract config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return SueCrspPriceExtractConfig(
        links_path=payload.get("links_path", DEFAULT_SUE_LINKS_PATH),
        output_path=payload.get("output_path", DEFAULT_SUE_CRSP_DAILY_PATH),
        chunk_dir=payload.get("chunk_dir", DEFAULT_SUE_CRSP_CHUNK_DIR),
        manifest_path=payload.get("manifest_path", DEFAULT_SUE_CRSP_MANIFEST_PATH),
        start_date=str(payload.get("start_date", "2020-01-01")),
        end_date=str(payload.get("end_date", "2022-03-25")),
        chunk_size=int(payload.get("chunk_size", 1)),
        max_permnos=payload.get("max_permnos"),
        fetched_at=payload.get("fetched_at"),
    )


def extract_crsp_prices_for_sue_links(
    config: SueCrspPriceExtractConfig,
    *,
    connection: WrdsLikeConnection,
) -> dict[str, Any]:
    """Extract CRSP daily prices for linked SUE PERMNOs with resumable chunks."""

    links = pd.read_csv(config.links_path)
    if "permno" not in links.columns:
        raise ValueError("links file must contain permno")
    permnos = _distinct_permnos(links)
    if config.max_permnos is not None:
        permnos = permnos[: config.max_permnos]

    chunk_dir = Path(config.chunk_dir)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    output_path = Path(config.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(config.manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    queried_chunks = 0
    skipped_chunks = 0
    chunk_records: list[dict[str, Any]] = []
    for chunk_index, permno_chunk in enumerate(_chunks(permnos, config.chunk_size), start=1):
        chunk_path = chunk_dir / f"crsp_daily_chunk_{chunk_index:05d}.csv"
        if chunk_path.exists():
            skipped_chunks += 1
            rows = _csv_row_count(chunk_path)
            chunk_records.append(
                {
                    "chunk_index": chunk_index,
                    "permno_count": len(permno_chunk),
                    "row_count": rows,
                    "status": "skipped_existing",
                    "path": str(chunk_path),
                }
            )
            continue
        query = _crsp_daily_query(
            permnos=permno_chunk,
            start_date=config.start_date,
            end_date=config.end_date,
            source_table=config.source_table,
        )
        frame = connection.raw_sql(query)
        frame = _normalize_crsp_daily_frame(frame)
        frame.to_csv(chunk_path, index=False)
        queried_chunks += 1
        chunk_records.append(
            {
                "chunk_index": chunk_index,
                "permno_count": len(permno_chunk),
                "row_count": len(frame),
                "status": "queried",
                "path": str(chunk_path),
            }
        )

    merged = _merge_chunk_files(chunk_dir)
    merged.to_csv(output_path, index=False)
    fetched_at = config.fetched_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    manifest = {
        "schema_version": "sue_crsp_price_extract_manifest.v1",
        "run_id": "sue_historical_crsp_price_extract",
        "status": "completed",
        "source_table": config.source_table,
        "links_path": config.links_path,
        "output_path": str(output_path),
        "chunk_dir": str(chunk_dir),
        "start_date": config.start_date,
        "end_date": config.end_date,
        "chunk_size": config.chunk_size,
        "max_permnos": config.max_permnos,
        "distinct_permnos": len(permnos),
        "chunk_count": len(chunk_records),
        "queried_chunks": queried_chunks,
        "skipped_chunks": skipped_chunks,
        "row_count": len(merged),
        "fetched_at": fetched_at,
        "resumable": True,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "chunk_records": chunk_records,
    }
    manifest["content_hash"] = hash_payload(manifest)
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    return manifest


def _distinct_permnos(links: pd.DataFrame) -> list[int]:
    values = pd.to_numeric(links["permno"], errors="coerce").dropna().astype(int)
    return sorted(set(values.tolist()))


def _chunks(values: list[int], chunk_size: int) -> list[list[int]]:
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def _crsp_daily_query(
    *,
    permnos: list[int],
    start_date: str,
    end_date: str,
    source_table: str,
) -> str:
    permno_list = ",".join(str(int(value)) for value in permnos)
    return f"""
select permno, date, prc, ret
from {source_table}
where date between '{start_date}' and '{end_date}'
  and permno in ({permno_list})
"""


def _normalize_crsp_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["permno", "date", "prc", "ret"])
    required = {"permno", "date", "prc", "ret"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("CRSP daily extract missing columns: " + ", ".join(sorted(missing)))
    normalized = frame.loc[:, ["permno", "date", "prc", "ret"]].copy()
    normalized["permno"] = pd.to_numeric(normalized["permno"], errors="raise").astype(int)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="raise").dt.date.astype(str)
    return normalized.sort_values(["permno", "date"]).reset_index(drop=True)


def _merge_chunk_files(chunk_dir: Path) -> pd.DataFrame:
    frames = [
        frame
        for frame in (pd.read_csv(path) for path in sorted(chunk_dir.glob("crsp_daily_chunk_*.csv")))
        if not frame.empty
    ]
    if not frames:
        return pd.DataFrame(columns=["permno", "date", "prc", "ret"])
    merged = pd.concat(frames, ignore_index=True)
    if merged.empty:
        return pd.DataFrame(columns=["permno", "date", "prc", "ret"])
    normalized = _normalize_crsp_daily_frame(merged)
    return normalized.drop_duplicates(subset=["permno", "date"]).reset_index(drop=True)


def _csv_row_count(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    return max(sum(1 for _ in path.open("r", encoding="utf-8")) - 1, 0)
