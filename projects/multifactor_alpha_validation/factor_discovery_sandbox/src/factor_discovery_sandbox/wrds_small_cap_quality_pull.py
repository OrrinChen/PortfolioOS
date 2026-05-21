"""WRDS Compustat/CCM PIT quality-score puller for FD small-cap research."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

import numpy as np
import pandas as pd
import yaml

from .small_cap_data_admission import GUARDS


class WRDSSmallCapQualityPullError(ValueError):
    """Raised when the small-cap quality bundle cannot be built."""


class WRDSConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        ...

    def close(self) -> None:
        ...


ConnectionFactory = Callable[[], WRDSConnection]


@dataclass(frozen=True)
class FDSmallCapQualityPullResult:
    manifest_path: Path
    raw_files: dict[str, Path]
    standardized_files: dict[str, Path]
    summary: dict[str, object]
    artifacts: dict[str, Path]


DATASET_ID = "wrds_small_cap_quality_v1"


def run_wrds_small_cap_quality_pull(
    research_manifest_path: str | Path,
    output_root: str | Path,
    output_dir: str | Path,
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    refresh: bool = False,
    connection_factory: ConnectionFactory | None = None,
) -> FDSmallCapQualityPullResult:
    """Pull CCM links and Compustat quarterly fundamentals, then write PIT scores."""

    research_manifest_file = Path(research_manifest_path)
    research_manifest = _load_yaml(research_manifest_file)
    research_start = _parse_date(start_date, "start_date")
    research_end = _parse_date(end_date, "end_date")
    if research_end < research_start:
        raise WRDSSmallCapQualityPullError("end_date must be on or after start_date")

    root = Path(output_root)
    raw_dir = root / "raw"
    standardized_dir = root / "standardized"
    output_path = Path(output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    standardized_dir.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)

    raw_ccm_path = raw_dir / "ccm_link_history.csv"
    raw_fundq_path = raw_dir / "quarterly_quality_fundamentals.csv"
    if raw_ccm_path.exists() and raw_fundq_path.exists() and not refresh:
        _emit_progress(f"wrds_small_cap_quality_cache_hit artifact=small_cap_quality_ccm path={raw_ccm_path}")
        _emit_progress(f"wrds_small_cap_quality_cache_hit artifact=fundq path={raw_fundq_path}")
        raw_ccm = pd.read_csv(raw_ccm_path)
        raw_fundq = pd.read_csv(raw_fundq_path)
    else:
        conn = (connection_factory or _default_wrds_connection_factory)()
        try:
            raw_ccm = _query_or_cache(
                conn,
                _ccm_query(research_start, research_end),
                raw_ccm_path,
                refresh=refresh,
                progress_label="small_cap_quality_ccm",
            )
            gvkeys = _gvkeys_from_ccm(raw_ccm)
            raw_fundq = _query_fundq_chunks(
                conn=conn,
                raw_dir=raw_dir,
                gvkeys=gvkeys,
                start=research_start,
                end=research_end,
                refresh=refresh,
            )
        finally:
            conn.close()

    ccm = _standardize_ccm(raw_ccm)
    fundq = _standardize_fundq(raw_fundq)
    prices = _load_research_prices(research_manifest, research_manifest_file)
    quality_scores = _build_quality_score_panel(prices, ccm, fundq)

    standardized_files = {
        "ccm_link_history": standardized_dir / "ccm_link_history.csv",
        "quarterly_quality_fundamentals": standardized_dir / "quarterly_quality_fundamentals.csv",
        "quality_score_panel": standardized_dir / "quality_score_panel.csv",
    }
    ccm.to_csv(standardized_files["ccm_link_history"], index=False)
    fundq.to_csv(standardized_files["quarterly_quality_fundamentals"], index=False)
    quality_scores.to_csv(standardized_files["quality_score_panel"], index=False)
    raw_files = {
        "ccm_link_history": raw_dir / "ccm_link_history.csv",
        "quarterly_quality_fundamentals": raw_dir / "quarterly_quality_fundamentals.csv",
    }

    quality_manifest_path = standardized_dir / "quality_manifest.yaml"
    quality_manifest = _build_quality_manifest(research_manifest_file, standardized_files, research_start, research_end)
    quality_manifest_path.write_text(yaml.safe_dump(quality_manifest, sort_keys=False), encoding="utf-8")
    _attach_quality_section_to_research_manifest(research_manifest_file, standardized_files["quality_score_panel"], quality_manifest_path)

    summary = {
        "schema_version": "fd_small_cap_quality_pull_summary.v1",
        "dataset_id": DATASET_ID,
        "research_manifest_path": str(research_manifest_file),
        "quality_manifest_path": str(quality_manifest_path),
        "ccm_link_rows": int(len(ccm)),
        "fundamental_rows": int(len(fundq)),
        "quality_score_rows": int(len(quality_scores)),
        "quality_covered_assets": int(quality_scores["asset_id"].nunique()) if not quality_scores.empty else 0,
        **GUARDS,
    }
    summary_path = output_path / "wrds_small_cap_quality_pull_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return FDSmallCapQualityPullResult(
        manifest_path=quality_manifest_path,
        raw_files=raw_files,
        standardized_files=standardized_files,
        summary=summary,
        artifacts={"quality_pull_summary": summary_path},
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
        _emit_progress(f"wrds_small_cap_quality_cache_hit artifact={progress_label} path={path}")
        return pd.read_csv(path)
    _emit_progress(f"wrds_small_cap_quality_query_start artifact={progress_label}")
    frame = conn.raw_sql(query)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    _emit_progress(f"wrds_small_cap_quality_query_done artifact={progress_label} row_count={len(frame)}")
    return frame


def _query_fundq_chunks(
    conn: WRDSConnection,
    raw_dir: Path,
    gvkeys: list[str],
    start: date,
    end: date,
    refresh: bool,
    chunk_size: int = 900,
) -> pd.DataFrame:
    output_path = raw_dir / "quarterly_quality_fundamentals.csv"
    if output_path.exists() and not refresh:
        return pd.read_csv(output_path)
    frames: list[pd.DataFrame] = []
    if not gvkeys:
        frame = conn.raw_sql(_fundq_query([], start, end))
        frame.to_csv(output_path, index=False)
        return frame
    chunk_dir = raw_dir / "_chunks" / "quarterly_quality_fundamentals"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for index, start_at in enumerate(range(0, len(gvkeys), chunk_size), start=1):
        chunk = gvkeys[start_at : start_at + chunk_size]
        chunk_path = chunk_dir / f"quarterly_quality_fundamentals_chunk_{index:04d}.csv"
        if chunk_path.exists() and not refresh:
            frame = pd.read_csv(chunk_path)
            _emit_progress(
                f"wrds_small_cap_quality_chunk_cache_hit artifact=fundq chunk={index} row_count={len(frame)}"
            )
        else:
            _emit_progress(
                f"wrds_small_cap_quality_chunk_start artifact=fundq chunk={index} gvkey_count={len(chunk)}"
            )
            frame = conn.raw_sql(_fundq_query(chunk, start, end))
            frame.to_csv(chunk_path, index=False)
            _emit_progress(f"wrds_small_cap_quality_chunk_done artifact=fundq chunk={index} row_count={len(frame)}")
        if not frame.empty:
            frames.append(frame)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    combined.to_csv(output_path, index=False)
    return combined


def _ccm_query(start: date, end: date) -> str:
    return f"""
      select
        l.gvkey,
        l.lpermno::integer as lpermno,
        l.lpermno::text as asset_id,
        l.linkdt,
        l.linkenddt,
        l.linktype,
        l.linkprim
      from crsp_a_ccm.ccmxpf_lnkhist l
      where l.lpermno is not null
        and l.linktype in ('LC', 'LU', 'LS')
        and l.linkprim in ('P', 'C')
        and coalesce(l.linkenddt, date '{end.isoformat()}') >= date '{start.isoformat()}'
        and l.linkdt <= date '{end.isoformat()}'
      order by l.gvkey, l.linkdt, l.lpermno
    """


def _fundq_query(gvkeys: list[str], start: date, end: date) -> str:
    gvkey_filter = ""
    if gvkeys:
        quoted = ", ".join(f"'{_escape_sql_literal(gvkey)}'" for gvkey in gvkeys)
        gvkey_filter = f"and f.gvkey in ({quoted})"
    return f"""
      select
        f.gvkey,
        f.datadate,
        f.fyearq,
        f.fqtr,
        f.rdq,
        f.atq::double precision as atq,
        f.ltq::double precision as ltq,
        f.saleq::double precision as saleq,
        f.revtq::double precision as revtq,
        f.cogsq::double precision as cogsq,
        f.niq::double precision as niq,
        f.oibdpq::double precision as oibdpq
      from comp.fundq f
      where f.datadate between date '{date(start.year - 2, 1, 1).isoformat()}' and date '{end.isoformat()}'
        and f.indfmt = 'INDL'
        and f.consol = 'C'
        and f.popsrc = 'D'
        and f.datafmt = 'STD'
        {gvkey_filter}
      order by f.gvkey, f.datadate
    """


def _standardize_ccm(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    if normalized.empty:
        return pd.DataFrame(columns=["gvkey", "asset_id", "linkdt", "linkenddt", "linktype", "linkprim", "source"])
    normalized["gvkey"] = normalized["gvkey"].astype(str)
    normalized["asset_id"] = normalized.get("asset_id", normalized["lpermno"]).astype(str).str.replace(r"\.0$", "", regex=True)
    normalized["linkdt"] = pd.to_datetime(normalized["linkdt"], errors="coerce")
    normalized["linkenddt"] = pd.to_datetime(normalized["linkenddt"], errors="coerce").fillna(pd.Timestamp.max.normalize())
    normalized["source"] = "wrds_crsp_a_ccm_ccmxpf_lnkhist"
    normalized["source_is_pit_link_table"] = True
    return normalized.sort_values(["gvkey", "linkdt", "asset_id"]).reset_index(drop=True)


def _standardize_fundq(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _lower_columns(frame)
    if normalized.empty:
        return pd.DataFrame()
    normalized["gvkey"] = normalized["gvkey"].astype(str)
    normalized["datadate"] = pd.to_datetime(normalized["datadate"], errors="coerce")
    normalized["rdq"] = pd.to_datetime(normalized.get("rdq"), errors="coerce")
    invalid_rdq = normalized["rdq"].notna() & (normalized["rdq"] < normalized["datadate"])
    normalized["visibility_timestamp"] = normalized["rdq"].where(
        normalized["rdq"].notna() & ~invalid_rdq,
        normalized["datadate"] + pd.Timedelta(days=90),
    )
    normalized["tradable_timestamp"] = normalized["visibility_timestamp"] + pd.Timedelta(days=1)
    for column in ("atq", "ltq", "saleq", "revtq", "cogsq", "niq", "oibdpq"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["profitability_roa"] = normalized[["niq", "oibdpq"]].bfill(axis=1).iloc[:, 0] / normalized["atq"]
    revenue = normalized[["revtq", "saleq"]].bfill(axis=1).iloc[:, 0]
    normalized["gross_profitability"] = (revenue - normalized["cogsq"]) / normalized["atq"]
    normalized["leverage"] = normalized["ltq"] / normalized["atq"]
    normalized["coverage_flag"] = normalized[["profitability_roa", "gross_profitability", "leverage"]].notna().all(axis=1)
    normalized["schema_version"] = "fd_small_cap_quarterly_quality_fundamentals.v1"
    normalized["source"] = "wrds_comp_fundq"
    normalized["allowed_use_mode"] = "factor_discovery_quality_control_input"
    normalized["not_alpha_evidence"] = True
    return normalized.dropna(subset=["gvkey", "datadate", "visibility_timestamp"]).sort_values(
        ["gvkey", "visibility_timestamp", "datadate"]
    )


def _build_quality_score_panel(prices: pd.DataFrame, ccm: pd.DataFrame, fundq: pd.DataFrame) -> pd.DataFrame:
    if prices.empty or ccm.empty or fundq.empty:
        return _empty_quality_score_panel()
    events = fundq.merge(ccm, on="gvkey", how="inner", suffixes=("", "_ccm"))
    datadate = pd.to_datetime(events["datadate"], errors="coerce")
    link_start = pd.to_datetime(events["linkdt"], errors="coerce").fillna(pd.Timestamp.min)
    link_end = pd.to_datetime(events["linkenddt"], errors="coerce").fillna(pd.Timestamp.max.normalize())
    events = events[(datadate >= link_start) & (datadate <= link_end)].copy()
    events["quality_score_raw"] = (
        events["profitability_roa"].astype(float)
        + events["gross_profitability"].astype(float)
        - events["leverage"].astype(float)
    )
    events = events.dropna(subset=["asset_id", "tradable_timestamp", "quality_score_raw"])
    if events.empty:
        return _empty_quality_score_panel()

    grid = _monthly_asset_grid(prices)
    if grid.empty:
        return _empty_quality_score_panel()
    mapped_frames = []
    for asset_id, asset_grid in grid.groupby("asset_id", sort=False):
        asset_events = events[events["asset_id"].astype(str) == str(asset_id)].copy()
        if asset_events.empty:
            continue
        left = asset_grid.sort_values("date").copy()
        right = asset_events.sort_values("tradable_timestamp").copy()
        mapped = pd.merge_asof(
            left,
            right,
            left_on="date",
            right_on="tradable_timestamp",
            direction="backward",
        )
        mapped["asset_id"] = str(asset_id)
        mapped_frames.append(mapped)
    if not mapped_frames:
        return _empty_quality_score_panel()
    panel = pd.concat(mapped_frames, ignore_index=True)
    panel = panel.dropna(subset=["quality_score_raw"])
    if panel.empty:
        return _empty_quality_score_panel()
    panel["quality_score"] = panel.groupby("date")["quality_score_raw"].transform(_robust_zscore)
    panel["schema_version"] = "fd_small_cap_quality_score.v1"
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.date.astype("string")
    for column in ("datadate", "visibility_timestamp", "tradable_timestamp"):
        panel[column] = pd.to_datetime(panel[column], errors="coerce").dt.date.astype("string")
    panel["source"] = "wrds_comp_fundq_ccm_pit_quality"
    panel["not_alpha_evidence"] = True
    panel["direct_q2_entry_allowed"] = False
    columns = [
        "schema_version",
        "asset_id",
        "date",
        "gvkey",
        "datadate",
        "visibility_timestamp",
        "tradable_timestamp",
        "quality_score",
        "quality_score_raw",
        "profitability_roa",
        "gross_profitability",
        "leverage",
        "source",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    ]
    return panel[columns].sort_values(["date", "asset_id"]).reset_index(drop=True)


def _monthly_asset_grid(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices[["date", "asset_id"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["asset_id"] = frame["asset_id"].astype(str)
    frame = frame.dropna(subset=["date", "asset_id"])
    month_end_dates = frame.groupby(frame["date"].dt.to_period("M"))["date"].max()
    return frame[frame["date"].isin(month_end_dates)].drop_duplicates(["date", "asset_id"]).sort_values(
        ["asset_id", "date"]
    )


def _robust_zscore(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    median = numeric.median()
    mad = (numeric - median).abs().median()
    if pd.notna(mad) and mad > 1e-12:
        return ((numeric - median) / (1.4826 * mad)).clip(-5.0, 5.0)
    std = numeric.std(ddof=0)
    if pd.notna(std) and std > 1e-12:
        return ((numeric - numeric.mean()) / std).clip(-5.0, 5.0)
    return pd.Series(0.0, index=values.index)


def _build_quality_manifest(
    research_manifest_path: Path,
    standardized_files: Mapping[str, Path],
    start: date,
    end: date,
) -> dict[str, Any]:
    return {
        "schema_version": "fd_small_cap_quality_manifest.v1",
        "dataset_id": DATASET_ID,
        "research_manifest_path": str(research_manifest_path),
        "research_window": {"start": start.isoformat(), "end": end.isoformat()},
        "source_provenance": {
            "provider": "wrds",
            "tables": ["crsp_a_ccm.ccmxpf_lnkhist", "comp.fundq"],
            "credential_storage": "external_pgpass_or_environment_only",
        },
        "paths": {key: str(path.resolve()) for key, path in standardized_files.items()},
        "score_definition": {
            "quality_score_raw": "profitability_roa + gross_profitability - leverage",
            "quality_score": "same-date robust z-score of quality_score_raw",
            "visibility": "rdq when rdq is on/after datadate, else datadate plus 90 calendar days",
            "tradable": "visibility plus one calendar day placeholder before trading-calendar alignment",
        },
        "non_claims": {
            "not_alpha_evidence": True,
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def _attach_quality_section_to_research_manifest(
    research_manifest_path: Path,
    quality_score_path: Path,
    quality_manifest_path: Path,
) -> None:
    payload = _load_yaml(research_manifest_path)
    payload["quality"] = {
        "path": str(quality_score_path.resolve()),
        "manifest_path": str(quality_manifest_path.resolve()),
        "source": "wrds_comp_fundq_ccm_pit_quality",
        "score_definition": "profitability_roa + gross_profitability - leverage, robust-z by signal date",
        "pit_safe": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }
    research_manifest_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _load_research_prices(manifest: Mapping[str, Any], manifest_path: Path) -> pd.DataFrame:
    price_section = manifest.get("prices")
    if not isinstance(price_section, Mapping) or not price_section.get("path"):
        raise WRDSSmallCapQualityPullError("research manifest missing prices.path")
    path = _resolve_path(Path(str(price_section["path"])), manifest_path)
    return pd.read_csv(path, usecols=["date", "asset_id"], dtype={"asset_id": str})


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise WRDSSmallCapQualityPullError(f"manifest must be a mapping: {path}")
    return payload


def _resolve_path(path: Path, manifest_path: Path) -> Path:
    if path.is_absolute():
        return path
    if path.exists():
        return path
    return manifest_path.parent / path


def _gvkeys_from_ccm(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "gvkey" not in frame.columns:
        return []
    return sorted(set(frame["gvkey"].dropna().astype(str)))


def _empty_quality_score_panel() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "schema_version",
            "asset_id",
            "date",
            "gvkey",
            "datadate",
            "visibility_timestamp",
            "tradable_timestamp",
            "quality_score",
            "quality_score_raw",
            "profitability_roa",
            "gross_profitability",
            "leverage",
            "source",
            "not_alpha_evidence",
            "direct_q2_entry_allowed",
        ]
    )


def _parse_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise WRDSSmallCapQualityPullError(f"{field} must be YYYY-MM-DD") from exc


def _lower_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    return normalized


def _escape_sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _default_wrds_connection_factory() -> WRDSConnection:
    try:
        import wrds  # type: ignore[import-not-found]
    except ImportError as exc:
        raise WRDSSmallCapQualityPullError("wrds package is not installed in the active environment") from exc
    username = os.environ.get("WRDS_USERNAME") or os.environ.get("WRDS_USER")
    if username:
        return wrds.Connection(wrds_username=username)
    return wrds.Connection()


def _emit_progress(message: str) -> None:
    if os.environ.get("WRDS_SMALL_CAP_QUALITY_PROGRESS") or os.environ.get("WRDS_INGEST_PROGRESS"):
        print(message, flush=True)
