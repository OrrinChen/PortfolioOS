"""WRDS timestamp-source extraction for SUE enrichment.

This module pulls auditable timestamp-source fields into local cache files that
Reopen-H1E.5 can read. It is data acquisition only: it does not select a SUE
score, run Q2, run optimizer-path evaluation, promote Alpha Registry state,
create broker/order/live workflows, or approve production use.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_schema import validate_no_forward_return_feature_columns
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_TIMESTAMP_SOURCE_EXTRACT_SCHEMA_VERSION = "sue_timestamp_source_extract.v1"
DEFAULT_EVENTS_PATH = "outputs/sue_historical_event_panel_expanded/events.csv"
DEFAULT_OUTPUT_DIR = "data/cache/wrds_sue_timestamp_sources"
DEFAULT_IBES_ACTUALS_OUTPUT_PATH = "data/cache/wrds_sue_timestamp_sources/ibes_actuals.csv"
DEFAULT_COMPUSTAT_QUARTERLY_OUTPUT_PATH = "data/cache/wrds_sue_timestamp_sources/compustat_quarterly.csv"
DEFAULT_MANIFEST_PATH = "outputs/sue_timestamp_enrichment/timestamp_source_extract_manifest.json"


class WrdsLikeConnection(Protocol):
    """Tiny WRDS connection protocol used by this extractor."""

    def raw_sql(self, query: str) -> pd.DataFrame:
        """Run a SQL query and return a DataFrame."""


class SueTimestampSourceExtractConfig(BaseModel):
    """Config for WRDS timestamp-source extraction."""

    model_config = ConfigDict(extra="forbid")

    events_path: str = DEFAULT_EVENTS_PATH
    output_dir: str = DEFAULT_OUTPUT_DIR
    ibes_actuals_output_path: str = DEFAULT_IBES_ACTUALS_OUTPUT_PATH
    compustat_quarterly_output_path: str = DEFAULT_COMPUSTAT_QUARTERLY_OUTPUT_PATH
    manifest_path: str = DEFAULT_MANIFEST_PATH
    ibes_actuals_source_table: str = "ibes.actu_epsus"
    compustat_source_table: str = "comp.fundq"
    ticker_chunk_size: int = Field(default=250, ge=1)
    compustat_chunk_size: int = Field(default=500, ge=1)
    max_events: int | None = Field(default=None, ge=1)
    fetched_at: str = "2026-05-08T00:00:00Z"


@dataclass(frozen=True)
class SueTimestampSourceExtractFrames:
    """Extracted source frames before writing."""

    ibes_actuals: pd.DataFrame
    compustat_quarterly: pd.DataFrame
    manifest: dict[str, Any]


def load_sue_timestamp_source_extract_config(path: str | Path) -> SueTimestampSourceExtractConfig:
    """Load timestamp-source extraction config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    wrds_extract = payload.get("wrds_extract") or {}
    return SueTimestampSourceExtractConfig(
        events_path=str(inputs.get("events_path") or payload.get("events_path") or DEFAULT_EVENTS_PATH),
        output_dir=str(outputs.get("output_dir") or payload.get("output_dir") or DEFAULT_OUTPUT_DIR),
        ibes_actuals_output_path=str(
            outputs.get("ibes_actuals_output_path")
            or payload.get("ibes_actuals_output_path")
            or DEFAULT_IBES_ACTUALS_OUTPUT_PATH
        ),
        compustat_quarterly_output_path=str(
            outputs.get("compustat_quarterly_output_path")
            or payload.get("compustat_quarterly_output_path")
            or DEFAULT_COMPUSTAT_QUARTERLY_OUTPUT_PATH
        ),
        manifest_path=str(outputs.get("manifest_path") or payload.get("manifest_path") or DEFAULT_MANIFEST_PATH),
        ibes_actuals_source_table=str(
            wrds_extract.get("ibes_actuals_source_table")
            or payload.get("ibes_actuals_source_table")
            or "ibes.actu_epsus"
        ),
        compustat_source_table=str(
            wrds_extract.get("compustat_source_table") or payload.get("compustat_source_table") or "comp.fundq"
        ),
        ticker_chunk_size=int(wrds_extract.get("ticker_chunk_size", payload.get("ticker_chunk_size", 250))),
        compustat_chunk_size=int(wrds_extract.get("compustat_chunk_size", payload.get("compustat_chunk_size", 500))),
        max_events=wrds_extract.get("max_events", payload.get("max_events")),
        fetched_at=str(payload.get("fetched_at") or "2026-05-08T00:00:00Z"),
    )


def extract_wrds_sue_timestamp_sources(
    config: SueTimestampSourceExtractConfig,
    *,
    connection: WrdsLikeConnection,
) -> dict[str, Any]:
    """Extract WRDS timestamp-source files for H1E.5 and write artifacts."""

    frames = build_wrds_sue_timestamp_source_frames(config, connection=connection)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    Path(config.ibes_actuals_output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.compustat_quarterly_output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.manifest_path).parent.mkdir(parents=True, exist_ok=True)
    frames.ibes_actuals.to_csv(config.ibes_actuals_output_path, index=False)
    frames.compustat_quarterly.to_csv(config.compustat_quarterly_output_path, index=False)
    Path(config.manifest_path).write_text(canonical_json(frames.manifest) + "\n", encoding="utf-8")
    return frames.manifest


def build_wrds_sue_timestamp_source_frames(
    config: SueTimestampSourceExtractConfig,
    *,
    connection: WrdsLikeConnection,
) -> SueTimestampSourceExtractFrames:
    """Build WRDS timestamp-source frames for H1E.5."""

    events = _load_target_events(config.events_path, config.max_events)
    ibes_raw = _query_ibes_actuals(connection, config, events)
    comp_raw = _query_compustat_fundq(connection, config, events)
    ibes_sources = _match_ibes_actuals_to_events(events, ibes_raw, config)
    comp_sources = _match_compustat_rdq_to_events(events, comp_raw, config)
    manifest = _manifest(config=config, events=events, ibes_sources=ibes_sources, comp_sources=comp_sources)
    return SueTimestampSourceExtractFrames(
        ibes_actuals=ibes_sources,
        compustat_quarterly=comp_sources,
        manifest=manifest,
    )


def _load_target_events(path: str | Path, max_events: int | None) -> pd.DataFrame:
    frame = pd.read_csv(path)
    validate_no_forward_return_feature_columns(list(frame.columns))
    required = {"event_id", "symbol", "ibes_ticker", "cusip", "fiscal_period", "actual_eps"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError("SUE events missing timestamp-source extraction columns: " + ", ".join(sorted(missing)))
    events = frame.copy()
    events["ibes_ticker"] = events["ibes_ticker"].astype(str).str.upper()
    events["symbol"] = events["symbol"].astype(str).str.upper()
    events["fiscal_period"] = events["fiscal_period"].map(_normalize_fiscal_period)
    events["cusip8"] = events["cusip"].map(_normalize_cusip8)
    events["actual_eps"] = pd.to_numeric(events["actual_eps"], errors="coerce")
    events = events.dropna(subset=["event_id", "ibes_ticker", "fiscal_period"])
    if max_events is not None:
        events = events.head(max_events)
    return events.reset_index(drop=True)


def _query_ibes_actuals(
    connection: WrdsLikeConnection,
    config: SueTimestampSourceExtractConfig,
    events: pd.DataFrame,
) -> pd.DataFrame:
    targets = (
        events.loc[:, ["ibes_ticker", "fiscal_period"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["ibes_ticker", "fiscal_period"])
        .to_dict(orient="records")
    )
    frames: list[pd.DataFrame] = []
    for chunk in _chunks(targets, config.ticker_chunk_size):
        values = ",".join(
            "('" + str(item["ibes_ticker"]).replace("'", "''") + "','" + str(item["fiscal_period"]) + "'::date)"
            for item in chunk
        )
        query = f"""
with targets(ticker, pends) as (values {values})
select a.ticker, a.cusip, a.oftic, a.pends, a.anndats, a.anntims,
       a.actdats, a.acttims, a.value, a.curr_act
from {config.ibes_actuals_source_table} a
join targets t
  on a.ticker = t.ticker
 and a.pends = t.pends
where a.measure = 'EPS'
  and a.usfirm = 1
"""
        frame = connection.raw_sql(query)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def _query_compustat_fundq(
    connection: WrdsLikeConnection,
    config: SueTimestampSourceExtractConfig,
    events: pd.DataFrame,
) -> pd.DataFrame:
    targets = (
        events.loc[:, ["cusip8", "fiscal_period"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["cusip8", "fiscal_period"])
        .to_dict(orient="records")
    )
    frames: list[pd.DataFrame] = []
    for chunk in _chunks(targets, config.compustat_chunk_size):
        values = ",".join(
            "('" + str(item["cusip8"]).replace("'", "''") + "','" + str(item["fiscal_period"]) + "'::date)"
            for item in chunk
            if str(item["cusip8"]).strip()
        )
        if not values:
            continue
        query = f"""
with targets(cusip8, datadate) as (values {values})
select f.gvkey, f.datadate, f.fyearq, f.fqtr, f.tic, f.cusip, f.rdq
from {config.compustat_source_table} f
join targets t
  on substring(f.cusip, 1, 8) = t.cusip8
 and f.datadate = t.datadate
where f.rdq is not null
"""
        frame = connection.raw_sql(query)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def _match_ibes_actuals_to_events(
    events: pd.DataFrame,
    raw: pd.DataFrame,
    config: SueTimestampSourceExtractConfig,
) -> pd.DataFrame:
    columns = [
        "event_id",
        "symbol",
        "ibes_ticker",
        "cusip",
        "fiscal_period",
        "anndats_act",
        "anndats_act_timestamp",
        "actual_eps",
        "source_table_name",
        "source_extraction_timestamp",
    ]
    if raw.empty:
        return pd.DataFrame(columns=columns)
    actuals = raw.copy()
    actuals["ibes_ticker"] = actuals["ticker"].astype(str).str.upper()
    actuals["fiscal_period"] = actuals["pends"].map(_normalize_fiscal_period)
    actuals["anndats_act"] = pd.to_datetime(actuals["anndats"], errors="coerce").dt.date.astype(str)
    actuals["anndats_act_timestamp"] = [
        _combine_source_datetime(date_value, time_value)
        for date_value, time_value in zip(actuals["anndats"], actuals.get("anntims", pd.Series([None] * len(actuals))))
    ]
    actuals["actual_eps_source"] = pd.to_numeric(actuals["value"], errors="coerce")
    merged = events.merge(
        actuals,
        on=["ibes_ticker", "fiscal_period"],
        how="inner",
        suffixes=("_event", "_source"),
    )
    if merged.empty:
        return pd.DataFrame(columns=columns)
    merged["eps_distance"] = (pd.to_numeric(merged["actual_eps"], errors="coerce") - merged["actual_eps_source"]).abs()
    merged = merged.sort_values(["event_id", "eps_distance", "anndats_act"])
    best = merged.groupby("event_id", as_index=False).head(1).copy()
    return pd.DataFrame(
        {
            "event_id": best["event_id"],
            "symbol": best["symbol"],
            "ibes_ticker": best["ibes_ticker"],
            "cusip": best["cusip_event"],
            "fiscal_period": best["fiscal_period"],
            "anndats_act": best["anndats_act"],
            "anndats_act_timestamp": best["anndats_act_timestamp"],
            "actual_eps": best["actual_eps_source"],
            "source_table_name": config.ibes_actuals_source_table,
            "source_extraction_timestamp": config.fetched_at,
        }
    ).reset_index(drop=True)


def _match_compustat_rdq_to_events(
    events: pd.DataFrame,
    raw: pd.DataFrame,
    config: SueTimestampSourceExtractConfig,
) -> pd.DataFrame:
    columns = [
        "event_id",
        "symbol",
        "fiscal_period",
        "gvkey",
        "datadate",
        "fqtr",
        "fyearq",
        "rdq",
        "source_table_name",
        "source_extraction_timestamp",
    ]
    if raw.empty:
        return pd.DataFrame(columns=columns)
    fundq = raw.copy()
    fundq["cusip8"] = fundq["cusip"].map(_normalize_cusip8)
    fundq["fiscal_period"] = fundq["datadate"].map(_normalize_fiscal_period)
    fundq["rdq"] = pd.to_datetime(fundq["rdq"], errors="coerce").dt.date.astype(str)
    merged = events.merge(
        fundq,
        on=["cusip8", "fiscal_period"],
        how="inner",
        suffixes=("_event", "_compustat"),
    )
    if merged.empty:
        return pd.DataFrame(columns=columns)
    merged = merged.sort_values(["event_id", "rdq", "gvkey"])
    best = merged.groupby("event_id", as_index=False).head(1).copy()
    return pd.DataFrame(
        {
            "event_id": best["event_id"],
            "symbol": best["symbol"],
            "fiscal_period": best["fiscal_period"],
            "gvkey": best["gvkey"],
            "datadate": best["datadate"],
            "fqtr": best["fqtr"],
            "fyearq": best["fyearq"],
            "rdq": best["rdq"],
            "source_table_name": config.compustat_source_table,
            "source_extraction_timestamp": config.fetched_at,
        }
    ).reset_index(drop=True)


def _combine_source_datetime(date_value: Any, time_value: Any) -> str:
    if date_value is None or pd.isna(date_value):
        return ""
    date_part = pd.Timestamp(date_value).date()
    if time_value is None or pd.isna(time_value) or not str(time_value).strip():
        return str(date_part)
    if isinstance(time_value, time):
        time_part = time_value
    else:
        parsed = pd.to_datetime(str(time_value), errors="coerce")
        if pd.isna(parsed):
            return str(date_part)
        time_part = parsed.time()
    return pd.Timestamp.combine(date_part, time_part).tz_localize("UTC").isoformat().replace("+00:00", "Z")


def _normalize_fiscal_period(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if "Q" in text.upper() and len(text) >= 6:
        try:
            return pd.Period(text.upper(), freq="Q").end_time.date().isoformat()
        except Exception:
            pass
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return text
    return parsed.date().isoformat()


def _normalize_cusip8(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) < 8:
        text = text.zfill(8)
    return text[:8]


def _manifest(
    *,
    config: SueTimestampSourceExtractConfig,
    events: pd.DataFrame,
    ibes_sources: pd.DataFrame,
    comp_sources: pd.DataFrame,
) -> dict[str, Any]:
    payload = {
        "schema_version": SUE_TIMESTAMP_SOURCE_EXTRACT_SCHEMA_VERSION,
        "status": "completed",
        "events_path": config.events_path,
        "event_count": int(len(events)),
        "ibes_actuals_source_table": config.ibes_actuals_source_table,
        "compustat_source_table": config.compustat_source_table,
        "ibes_actuals_output_path": config.ibes_actuals_output_path,
        "compustat_quarterly_output_path": config.compustat_quarterly_output_path,
        "ibes_actuals_matched_events": int(ibes_sources["event_id"].nunique()) if not ibes_sources.empty else 0,
        "compustat_rdq_matched_events": int(comp_sources["event_id"].nunique()) if not comp_sources.empty else 0,
        "fetched_at": config.fetched_at,
        "score_selection_ran": False,
        "h1e_rerun_attempted": False,
        "typed_projection_ran": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "alpha_registry_promoted": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _chunks(values: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]
