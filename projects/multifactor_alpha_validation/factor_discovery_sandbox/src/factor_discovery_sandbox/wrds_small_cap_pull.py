"""WRDS CRSP puller for the FD small-cap candidate-family line."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Protocol

import pandas as pd
import yaml

from .small_cap_data_admission import GUARDS


class WRDSSmallCapPullError(ValueError):
    """Raised when a small-cap WRDS pull cannot produce a usable local bundle."""


class WRDSConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        ...

    def close(self) -> None:
        ...


ConnectionFactory = Callable[[], WRDSConnection]


@dataclass(frozen=True)
class FDSmallCapWRDSPullResult:
    """Artifacts and summary for the FD small-cap WRDS data pull."""

    manifest_path: Path
    raw_files: dict[str, Path]
    standardized_files: dict[str, Path]
    summary: dict[str, object]
    artifacts: dict[str, Path]


ARTIFACT_FILENAMES = {
    "historical_universe_membership": "historical_universe_membership.csv",
    "adjusted_price_volume_panel": "adjusted_price_volume_panel.csv",
    "small_cap_benchmark_panel": "small_cap_benchmark_panel.csv",
    "delisting_returns": "delisting_returns.csv",
}
DATASET_ID = "wrds_us_small_cap_daily_v1"


def run_wrds_small_cap_pull(
    output_root: str | Path,
    output_dir: str | Path,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    price_start_date: str | None = None,
    date_chunk_years: int = 1,
    refresh: bool = False,
    connection_factory: ConnectionFactory | None = None,
) -> FDSmallCapWRDSPullResult:
    """Pull a local WRDS CRSP daily bundle for small-cap FD admission.

    The bundle is data only: it does not run allocator, Q1/Q2, registry, broker,
    paper, live, order, or production-approval workflows.
    """

    research_start = _parse_date(start_date, "start_date")
    research_end = _parse_date(end_date, "end_date")
    if research_end < research_start:
        raise WRDSSmallCapPullError("end_date must be on or after start_date")
    price_start = _parse_date(price_start_date, "price_start_date") if price_start_date else _default_price_start(research_start)
    if date_chunk_years <= 0:
        raise WRDSSmallCapPullError("date_chunk_years must be positive")

    root = Path(output_root)
    raw_dir = root / "raw"
    standardized_dir = root / "standardized"
    output_path = Path(output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    standardized_dir.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = (connection_factory or _default_wrds_connection_factory)()
    try:
        raw_universe = _query_or_cache(
            conn,
            _universe_query(price_start, research_end),
            raw_dir / ARTIFACT_FILENAMES["historical_universe_membership"],
            refresh=refresh,
            progress_label="small_cap_universe",
        )
        raw_benchmark = _query_or_cache(
            conn,
            _benchmark_query(price_start, research_end),
            raw_dir / ARTIFACT_FILENAMES["small_cap_benchmark_panel"],
            refresh=refresh,
            progress_label="small_cap_benchmark",
        )
        raw_delistings = _query_or_cache(
            conn,
            _delisting_query(research_start, research_end),
            raw_dir / ARTIFACT_FILENAMES["delisting_returns"],
            refresh=refresh,
            progress_label="small_cap_delistings",
        )
        raw_prices = _query_price_chunks(
            conn=conn,
            raw_dir=raw_dir,
            price_start=price_start,
            research_end=research_end,
            date_chunk_years=date_chunk_years,
            refresh=refresh,
        )
    finally:
        conn.close()

    standardized = {
        "historical_universe_membership": _standardize_universe(raw_universe),
        "adjusted_price_volume_panel": _standardize_prices(raw_prices),
        "small_cap_benchmark_panel": _standardize_benchmark(raw_benchmark),
        "delisting_returns": _standardize_delistings(raw_delistings),
    }
    raw_files = {
        "historical_universe_membership": raw_dir / ARTIFACT_FILENAMES["historical_universe_membership"],
        "adjusted_price_volume_panel": raw_dir / ARTIFACT_FILENAMES["adjusted_price_volume_panel"],
        "small_cap_benchmark_panel": raw_dir / ARTIFACT_FILENAMES["small_cap_benchmark_panel"],
        "delisting_returns": raw_dir / ARTIFACT_FILENAMES["delisting_returns"],
    }
    standardized_files: dict[str, Path] = {}
    for artifact, frame in standardized.items():
        path = standardized_dir / ARTIFACT_FILENAMES[artifact]
        frame.to_csv(path, index=False)
        standardized_files[artifact] = path

    manifest_path = standardized_dir / "research_mode_dataset_manifest.yaml"
    manifest = _build_manifest(standardized_files, research_start, research_end, price_start)
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    summary = {
        "schema_version": "fd_small_cap_wrds_pull_summary.v1",
        "dataset_id": DATASET_ID,
        "manifest_path": str(manifest_path),
        "research_start": research_start.isoformat(),
        "research_end": research_end.isoformat(),
        "price_start": price_start.isoformat(),
        "universe_rows": int(len(standardized["historical_universe_membership"])),
        "price_rows": int(len(standardized["adjusted_price_volume_panel"])),
        "benchmark_rows": int(len(standardized["small_cap_benchmark_panel"])),
        "delisting_rows": int(len(standardized["delisting_returns"])),
        "allocator_entry_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        "direct_q2_entry_allowed": False,
        "not_alpha_evidence": True,
    }
    summary_path = output_path / "wrds_small_cap_pull_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return FDSmallCapWRDSPullResult(
        manifest_path=manifest_path,
        raw_files=raw_files,
        standardized_files=standardized_files,
        summary=summary,
        artifacts={"wrds_small_cap_pull_summary": summary_path},
    )


def _query_or_cache(
    conn: WRDSConnection,
    query: str,
    path: Path,
    *,
    refresh: bool,
    progress_label: str,
) -> pd.DataFrame:
    if path.exists() and not refresh:
        _emit_progress(f"wrds_small_cap_cache_hit artifact={progress_label} path={path}")
        return pd.read_csv(path)
    _emit_progress(f"wrds_small_cap_query_start artifact={progress_label}")
    frame = conn.raw_sql(query)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    _emit_progress(f"wrds_small_cap_query_done artifact={progress_label} row_count={len(frame)}")
    return frame


def _query_price_chunks(
    conn: WRDSConnection,
    raw_dir: Path,
    price_start: date,
    research_end: date,
    date_chunk_years: int,
    refresh: bool,
) -> pd.DataFrame:
    chunks = _date_chunks(price_start, research_end, date_chunk_years)
    chunk_dir = raw_dir / "_chunks" / "adjusted_price_volume_panel"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    frames: list[pd.DataFrame] = []
    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        path = chunk_dir / f"adjusted_price_volume_panel_chunk_{index:04d}.csv"
        if path.exists() and not refresh:
            frame = pd.read_csv(path)
            _emit_progress(
                f"wrds_small_cap_chunk_cache_hit artifact=prices chunk={index}/{len(chunks)} row_count={len(frame)}"
            )
        else:
            _emit_progress(
                f"wrds_small_cap_chunk_start artifact=prices chunk={index}/{len(chunks)} "
                f"start={chunk_start.isoformat()} end={chunk_end.isoformat()}"
            )
            frame = conn.raw_sql(_price_query(chunk_start, chunk_end))
            frame.to_csv(path, index=False)
            _emit_progress(
                f"wrds_small_cap_chunk_done artifact=prices chunk={index}/{len(chunks)} row_count={len(frame)}"
            )
        if not frame.empty:
            frames.append(frame)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    combined_path = raw_dir / ARTIFACT_FILENAMES["adjusted_price_volume_panel"]
    combined.to_csv(combined_path, index=False)
    return combined


def _universe_query(price_start: date, research_end: date) -> str:
    return f"""
      select
        n.permno::integer as permno,
        n.permno::text as asset_id,
        n.ticker,
        greatest(n.namedt, date '{price_start.isoformat()}') as membership_start,
        least(coalesce(n.nameendt, date '{research_end.isoformat()}'), date '{research_end.isoformat()}') as membership_end,
        greatest(n.namedt, date '{price_start.isoformat()}') as as_of_timestamp,
        greatest(n.namedt, date '{price_start.isoformat()}') as date,
        true as in_universe,
        greatest(n.namedt, date '{price_start.isoformat()}') as entry_date,
        case
          when n.nameendt is null or n.nameendt > date '{research_end.isoformat()}' then null
          else least(n.nameendt, date '{research_end.isoformat()}')
        end as exit_date,
        n.shrcd::integer as share_code,
        n.exchcd::integer as exchange_code,
        true as common_share,
        n.siccd::integer as sic,
        n.naics::integer as naics,
        'wrds_crsp_a_stock_dsenames_common_shares' as source,
        true as source_is_pit
      from crsp_a_stock.dsenames n
      where n.shrcd in (10, 11)
        and n.exchcd in (1, 2, 3)
        and coalesce(n.nameendt, date '{research_end.isoformat()}') >= date '{price_start.isoformat()}'
        and n.namedt <= date '{research_end.isoformat()}'
      order by membership_start, permno
    """


def _price_query(chunk_start: date, chunk_end: date) -> str:
    return f"""
      select
        d.permno::integer as permno,
        d.permno::text as asset_id,
        coalesce(d.ticker, n.ticker) as ticker,
        d.dlycaldt as date,
        d.dlyopen::double precision as raw_open,
        d.dlyprc::double precision as raw_close,
        abs(d.dlyopen)::double precision / nullif(d.dlycumfacpr::double precision, 0) as adjusted_open,
        abs(d.dlyprc)::double precision / nullif(d.dlycumfacpr::double precision, 0) as adjusted_close,
        d.dlyvol::double precision as volume,
        d.dlyret::double precision as return,
        d.dlycap::double precision * 1000.0 as market_cap,
        d.shrout::double precision * 1000.0 as shares_outstanding,
        abs(d.dlyprc)::double precision * d.dlyvol::double precision as dollar_volume,
        case
          when d.dlyask is not null and d.dlybid is not null and abs(d.dlyprc) > 0
          then (d.dlyask::double precision - d.dlybid::double precision) / abs(d.dlyprc)::double precision
          else null
        end as bid_ask_spread,
        d.dlyhigh::double precision as high,
        d.dlylow::double precision as low,
        n.shrcd::integer as share_code,
        n.exchcd::integer as exchange_code,
        true as common_share,
        coalesce(d.siccd, n.siccd)::integer as sic,
        n.naics::integer as naics,
        'crsp_dsf_v2_dlyprc_div_dlycumfacpr_daily' as adjusted_price_convention
      from crsp_a_stock.dsf_v2 d
      join crsp_a_stock.dsenames n
        on d.permno = n.permno
       and d.dlycaldt between n.namedt and coalesce(n.nameendt, date '{chunk_end.isoformat()}')
      where d.dlycaldt between date '{chunk_start.isoformat()}' and date '{chunk_end.isoformat()}'
        and n.shrcd in (10, 11)
        and n.exchcd in (1, 2, 3)
        and d.dlyret is not null
        and d.dlyret > -0.999999
        and d.dlycumfacpr is not null
        and d.dlycumfacpr <> 0
        and d.dlyprc is not null
        and d.dlyvol is not null
      order by d.dlycaldt, d.permno
    """


def _benchmark_query(price_start: date, research_end: date) -> str:
    return f"""
      select
        d.dlycaldt as date,
        'IWM' as benchmark,
        d.dlyopen::double precision as raw_open,
        d.dlyprc::double precision as raw_close,
        abs(d.dlyopen)::double precision / nullif(d.dlycumfacpr::double precision, 0) as adjusted_open,
        abs(d.dlyprc)::double precision / nullif(d.dlycumfacpr::double precision, 0) as adjusted_close,
        d.dlyvol::double precision as volume,
        d.dlyret::double precision as return,
        'crsp_dsf_v2_dlyprc_div_dlycumfacpr_daily' as adjusted_price_convention
      from crsp_a_stock.dsf_v2 d
      where d.permno = 88222
        and d.dlycaldt between date '{price_start.isoformat()}' and date '{research_end.isoformat()}'
        and d.dlyret is not null
        and d.dlyret > -0.999999
        and d.dlycumfacpr is not null
        and d.dlycumfacpr <> 0
        and d.dlyprc is not null
      order by d.dlycaldt
    """


def _delisting_query(research_start: date, research_end: date) -> str:
    return f"""
      select
        dl.permno::integer as permno,
        dl.permno::text as asset_id,
        dl.delistingdt as delisting_date,
        dl.delret::double precision as delisting_return,
        coalesce(dl.delreasontype, dl.delstatustype, dl.delactiontype, 'delisted') as inactive_reason,
        coalesce(dl.deldlydt, dl.delistingdt) as last_trade_date,
        dl.delactiontype as delisting_action_type,
        dl.delstatustype as delisting_status_type,
        dl.delreasontype as delisting_reason_type
      from crsp_a_stock.stkdelists dl
      join (
        select distinct n.permno
        from crsp_a_stock.dsenames n
        where n.shrcd in (10, 11)
          and n.exchcd in (1, 2, 3)
      ) u
        on dl.permno = u.permno
      where dl.delistingdt between date '{research_start.isoformat()}' and date '{research_end.isoformat()}'
      order by dl.delistingdt, dl.permno
    """


def _standardize_universe(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    for column in ("membership_start", "membership_end", "as_of_timestamp", "date", "entry_date", "exit_date"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce").dt.date.astype("string")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["sector"] = normalized.get("sic", pd.Series(index=normalized.index, dtype=object)).map(_sic_sector)
    normalized["industry"] = normalized.get("sic", pd.Series(index=normalized.index, dtype=object)).map(_sic_industry)
    normalized["common_share"] = True
    normalized["source_is_pit"] = True
    normalized["in_universe"] = True
    return normalized.sort_values(["membership_start", "asset_id"]).reset_index(drop=True)


def _standardize_prices(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    if normalized.empty:
        return normalized
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.date.astype("string")
    numeric_columns = [
        "raw_open",
        "raw_close",
        "adjusted_open",
        "adjusted_close",
        "volume",
        "return",
        "market_cap",
        "shares_outstanding",
        "dollar_volume",
        "bid_ask_spread",
        "high",
        "low",
        "share_code",
        "exchange_code",
        "sic",
        "naics",
    ]
    for column in numeric_columns:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "market_cap" not in normalized.columns and {"adjusted_close", "shares_outstanding"} <= set(normalized.columns):
        normalized["market_cap"] = normalized["adjusted_close"] * normalized["shares_outstanding"]
    if "dollar_volume" not in normalized.columns and {"adjusted_close", "volume"} <= set(normalized.columns):
        normalized["dollar_volume"] = normalized["adjusted_close"] * normalized["volume"]
    normalized["sector"] = normalized.get("sic", pd.Series(index=normalized.index, dtype=object)).map(_sic_sector)
    normalized["industry"] = normalized.get("sic", pd.Series(index=normalized.index, dtype=object)).map(_sic_industry)
    normalized["common_share"] = True
    return normalized.sort_values(["date", "asset_id"]).reset_index(drop=True)


def _standardize_benchmark(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    if normalized.empty:
        return normalized
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.date.astype("string")
    for column in ("raw_open", "raw_close", "adjusted_open", "adjusted_close", "volume", "return"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["benchmark"] = "IWM"
    return normalized.sort_values("date").drop_duplicates(["date", "benchmark"]).reset_index(drop=True)


def _standardize_delistings(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    columns = [
        "permno",
        "asset_id",
        "delisting_date",
        "delisting_return",
        "inactive_reason",
        "last_trade_date",
        "delisting_action_type",
        "delisting_status_type",
        "delisting_reason_type",
    ]
    if normalized.empty:
        return pd.DataFrame(columns=columns)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("delisting_date", "last_trade_date"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce").dt.date.astype("string")
    if "delisting_return" in normalized.columns:
        normalized["delisting_return"] = pd.to_numeric(normalized["delisting_return"], errors="coerce")
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = ""
    return normalized[columns].sort_values(["delisting_date", "asset_id"]).reset_index(drop=True)


def _build_manifest(
    standardized_files: dict[str, Path],
    research_start: date,
    research_end: date,
    price_start: date,
) -> dict[str, object]:
    content_hash = _hash_standardized_files(standardized_files)
    return {
        "schema_version": "research_mode_dataset_manifest.v1",
        "dataset_id": DATASET_ID,
        "mode": "research_mode",
        "allowed_use_mode": "small_cap_candidate_family_research",
        "content_hash": content_hash,
        "research_window": {
            "research_start": research_start.isoformat(),
            "research_end": research_end.isoformat(),
            "price_start": price_start.isoformat(),
        },
        "source_provenance": {
            "provider": "wrds",
            "as_of_timestamp": date.today().isoformat(),
            "license_mode": "local_research_subscription",
        },
        "universe": {
            "path": str(standardized_files["historical_universe_membership"].resolve()),
            "constituent_mode": "historical_membership",
            "source": "wrds_crsp_a_stock_dsenames_common_shares",
            "source_is_pit": True,
        },
        "prices": {
            "path": str(standardized_files["adjusted_price_volume_panel"].resolve()),
            "source": "wrds_crsp_a_stock_dsf_v2",
            "adjusted": True,
        },
        "benchmark": {
            "path": str(standardized_files["small_cap_benchmark_panel"].resolve()),
            "benchmark_id": "IWM",
            "source": "wrds_crsp_a_stock_dsf_v2",
        },
        "delisting": {
            "handling": "explicit_file",
            "path": str(standardized_files["delisting_returns"].resolve()),
        },
        "trading_calendar": {
            "path": str(standardized_files["adjusted_price_volume_panel"].resolve()),
            "source": "wrds_crsp_trading_dates",
        },
        "timestamp_policy": {
            "signal": "month_end_close",
            "visibility": "after_month_end_close",
            "tradable": "next_session_close",
            "allow_same_close_trading": False,
        },
        "small_cap_research_contract": {
            "universe_tiers": ["large_cap_control", "small_cap_investable", "microcap_quarantine"],
            "microcap_quarantine_allowed_use": "diagnostic_only",
            "candidate_family": "small_cap_quality_residual_momentum_v1",
            **GUARDS,
        },
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def _date_chunks(start: date, end: date, chunk_years: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(date(current.year + chunk_years - 1, 12, 31), end)
        chunks.append((current, chunk_end))
        current = date(chunk_end.year + 1, 1, 1)
    return chunks


def _parse_date(value: str | None, field: str) -> date:
    if not value:
        raise WRDSSmallCapPullError(f"{field} is required")
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise WRDSSmallCapPullError(f"{field} must be YYYY-MM-DD") from exc


def _default_price_start(research_start: date) -> date:
    # The signal needs a 147-trading-day lookback; one calendar year gives a
    # conservative warmup without forcing a larger WRDS pull by default.
    return date(research_start.year - 1, research_start.month, research_start.day)


def _lower_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    return normalized


def _sic_sector(value: object) -> str:
    sic = _safe_int(value)
    if sic is None:
        return "unknown"
    if 100 <= sic <= 999:
        return "agriculture"
    if 1000 <= sic <= 1499:
        return "mining"
    if 1500 <= sic <= 1799:
        return "construction"
    if 2000 <= sic <= 3999:
        return "manufacturing"
    if 4000 <= sic <= 4999:
        return "transport_communications_utilities"
    if 5000 <= sic <= 5199:
        return "wholesale_trade"
    if 5200 <= sic <= 5999:
        return "retail_trade"
    if 6000 <= sic <= 6799:
        return "finance_real_estate"
    if 7000 <= sic <= 8999:
        return "services"
    if 9000 <= sic <= 9999:
        return "public_administration"
    return "unknown"


def _sic_industry(value: object) -> str:
    sic = _safe_int(value)
    return f"sic_{sic}" if sic is not None else "unknown"


def _safe_int(value: object) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _hash_standardized_files(files: dict[str, Path]) -> str:
    digest = hashlib.sha256()
    for key in sorted(files):
        digest.update(key.encode("utf-8"))
        digest.update(files[key].read_bytes())
    return digest.hexdigest()


def _default_wrds_connection_factory() -> WRDSConnection:
    try:
        import wrds  # type: ignore[import-not-found]
    except ImportError as exc:
        raise WRDSSmallCapPullError("wrds package is not installed in the active environment") from exc
    username = os.environ.get("WRDS_USERNAME") or os.environ.get("WRDS_USER")
    if username:
        return wrds.Connection(wrds_username=username)
    return wrds.Connection()


def _emit_progress(message: str) -> None:
    if os.environ.get("WRDS_INGEST_PROGRESS") or os.environ.get("WRDS_SMALL_CAP_PROGRESS"):
        print(message, flush=True)
