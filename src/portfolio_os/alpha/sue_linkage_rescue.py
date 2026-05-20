"""Exact-CUSIP CRSP stocknames linkage rescue for historical SUE panels.

This module only improves local IBES/CRSP link coverage for PIT-labeled panel
building. It does not evaluate alpha, run Q2, call brokers, generate orders, or
approve production use.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.provenance.hashing import canonical_json, hash_payload


DEFAULT_EXISTING_LINKS_PATH = "data/cache/wrds_sue_event_panel/ibes_links.csv"
DEFAULT_LINKAGE_FAILURE_REPORT_PATH = "outputs/sue_historical_event_panel_expanded/linkage_failure_report.csv"
DEFAULT_OUTPUT_LINKS_PATH = "data/cache/wrds_sue_event_panel/ibes_links_rescued.csv"
DEFAULT_RESCUE_REPORT_PATH = "outputs/sue_coverage_linkage_price_diagnostics/linkage_rescue_report.json"
DEFAULT_STOCKNAMES_PROBE_PATH = "outputs/sue_coverage_linkage_price_diagnostics/stocknames_failed_cusip_matches.csv"


class WrdsLikeConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        """Run SQL and return a DataFrame."""


class SueLinkageRescueConfig(BaseModel):
    """Config for exact-CUSIP SUE linkage rescue."""

    model_config = ConfigDict(extra="forbid")

    existing_links_path: str = DEFAULT_EXISTING_LINKS_PATH
    linkage_failure_report_path: str = DEFAULT_LINKAGE_FAILURE_REPORT_PATH
    output_links_path: str = DEFAULT_OUTPUT_LINKS_PATH
    rescue_report_path: str = DEFAULT_RESCUE_REPORT_PATH
    stocknames_probe_path: str = DEFAULT_STOCKNAMES_PROBE_PATH
    batch_size: int = Field(default=500, gt=0)
    fetched_at: str | None = None
    source_table: str = "crsp.stocknames"


def load_sue_linkage_rescue_config(path: str | Path) -> SueLinkageRescueConfig:
    """Load linkage rescue config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return SueLinkageRescueConfig(
        existing_links_path=payload.get("existing_links_path", DEFAULT_EXISTING_LINKS_PATH),
        linkage_failure_report_path=payload.get("linkage_failure_report_path", DEFAULT_LINKAGE_FAILURE_REPORT_PATH),
        output_links_path=payload.get("output_links_path", DEFAULT_OUTPUT_LINKS_PATH),
        rescue_report_path=payload.get("rescue_report_path", DEFAULT_RESCUE_REPORT_PATH),
        stocknames_probe_path=payload.get("stocknames_probe_path", DEFAULT_STOCKNAMES_PROBE_PATH),
        batch_size=int(payload.get("batch_size", 500)),
        fetched_at=payload.get("fetched_at"),
        source_table=str(payload.get("source_table", "crsp.stocknames")),
    )


def rescue_sue_links_from_crsp_stocknames(
    config: SueLinkageRescueConfig,
    *,
    connection: WrdsLikeConnection,
) -> dict[str, Any]:
    """Append date-valid exact-CUSIP CRSP stocknames matches to the local link cache."""

    existing = _base_links(config)
    failures = pd.read_csv(config.linkage_failure_report_path)
    required = {"event_id", "symbol", "ibes_ticker", "cusip", "announcement_date"}
    missing = required - set(failures.columns)
    if missing:
        raise ValueError("linkage failure report missing columns: " + ", ".join(sorted(missing)))

    failures = failures.copy()
    failures["cusip"] = failures["cusip"].astype(str)
    failures["announcement_date"] = pd.to_datetime(failures["announcement_date"], errors="raise").dt.date
    failed_cusips = sorted(set(failures["cusip"].dropna().astype(str)))
    stocknames = _query_stocknames(connection, failed_cusips, batch_size=config.batch_size, source_table=config.source_table)
    stocknames_path = Path(config.stocknames_probe_path)
    stocknames_path.parent.mkdir(parents=True, exist_ok=True)
    stocknames.to_csv(stocknames_path, index=False)

    rescued_links = _date_valid_exact_cusip_links(failures, stocknames)
    combined = _combined_links(existing, rescued_links)
    output_path = Path(config.output_links_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)

    fetched_at = config.fetched_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "schema_version": "sue_linkage_rescue_report.v1",
        "run_id": "sue_linkage_rescue",
        "status": "completed",
        "source_table": config.source_table,
        "existing_links_path": config.existing_links_path,
        "linkage_failure_report_path": config.linkage_failure_report_path,
        "output_links_path": str(output_path),
        "stocknames_probe_path": str(stocknames_path),
        "failed_event_rows": int(len(failures)),
        "failed_unique_cusips": int(len(failed_cusips)),
        "stocknames_match_rows": int(len(stocknames)),
        "rescued_event_rows": int(rescued_links["source_event_id"].nunique()) if not rescued_links.empty else 0,
        "rescued_symbols": int(rescued_links["ibes_ticker"].nunique()) if not rescued_links.empty else 0,
        "rescued_permnos": int(rescued_links["permno"].nunique()) if not rescued_links.empty else 0,
        "preserved_exact_cusip_rescue_link_rows": _exact_cusip_rescue_rows(existing),
        "combined_exact_cusip_rescue_link_rows": _exact_cusip_rescue_rows(combined),
        "existing_link_rows": int(len(existing)),
        "combined_link_rows": int(len(combined)),
        "ticker_only_matching_used": False,
        "exact_cusip_matching_only": True,
        "alpha_registry_promoted": False,
        "q2_evaluation_ran": False,
        "optimizer_path_evaluation_ran": False,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
        "fetched_at": fetched_at,
    }
    payload["content_hash"] = hash_payload(payload)
    report_path = Path(config.rescue_report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return payload


def _query_stocknames(
    connection: WrdsLikeConnection,
    cusips: list[str],
    *,
    batch_size: int,
    source_table: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for batch in _chunks(cusips, batch_size):
        values = ",".join("'" + value.replace("'", "''") + "'" for value in batch)
        query = f"""
select permno, permco, namedt, nameenddt, ncusip, cusip, ticker, shrcd, exchcd
from {source_table}
where ncusip in ({values}) or cusip in ({values})
"""
        frames.append(connection.raw_sql(query))
    if not frames:
        return pd.DataFrame(columns=["permno", "permco", "namedt", "nameenddt", "ncusip", "cusip", "ticker", "shrcd", "exchcd"])
    frame = pd.concat(frames, ignore_index=True).drop_duplicates()
    if frame.empty:
        return pd.DataFrame(columns=["permno", "permco", "namedt", "nameenddt", "ncusip", "cusip", "ticker", "shrcd", "exchcd"])
    for column in ["namedt", "nameenddt"]:
        frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce").astype("Int64")
    frame["permco"] = pd.to_numeric(frame["permco"], errors="coerce").astype("Int64")
    frame["ncusip"] = frame["ncusip"].astype(str)
    frame["cusip"] = frame["cusip"].astype(str)
    return frame


def _date_valid_exact_cusip_links(failures: pd.DataFrame, stocknames: pd.DataFrame) -> pd.DataFrame:
    if stocknames.empty:
        return _empty_rescued_links()
    left = failures.merge(stocknames, left_on="cusip", right_on="ncusip", how="inner")
    right = failures.merge(stocknames, left_on="cusip", right_on="cusip", how="inner", suffixes=("", "_stocknames"))
    candidates = pd.concat([left, right], ignore_index=True).drop_duplicates()
    if candidates.empty:
        return _empty_rescued_links()
    valid = candidates.loc[
        (candidates["namedt"].isna() | (candidates["namedt"] <= candidates["announcement_date"]))
        & (candidates["nameenddt"].isna() | (candidates["nameenddt"] >= candidates["announcement_date"]))
    ].copy()
    if valid.empty:
        return _empty_rescued_links()
    valid = valid.sort_values(["event_id", "namedt", "permno"]).drop_duplicates(subset=["event_id"], keep="last")
    rescued = pd.DataFrame(
        {
            "ibes_ticker": valid["ibes_ticker"].astype(str).str.upper(),
            "cusip": valid["cusip"].astype(str),
            "permno": valid["permno"].astype(int),
            "permco": valid["permco"].astype(int),
            "link_method": "crsp_stocknames_exact_cusip_rescue",
            "link_start_date": valid["namedt"].astype(str),
            "link_end_date": valid["nameenddt"].astype(str),
            "link_validity_flag": True,
            "source_event_id": valid["event_id"].astype(str),
        }
    )
    return rescued


def _combined_links(existing: pd.DataFrame, rescued_links: pd.DataFrame) -> pd.DataFrame:
    link_columns = [
        "ibes_ticker",
        "cusip",
        "permno",
        "permco",
        "link_method",
        "link_start_date",
        "link_end_date",
        "link_validity_flag",
    ]
    existing_frame = existing.reindex(columns=link_columns).copy()
    if rescued_links.empty:
        return existing_frame.drop_duplicates().reset_index(drop=True)
    rescued_frame = rescued_links.reindex(columns=link_columns).copy()
    combined = pd.concat([existing_frame, rescued_frame], ignore_index=True)
    combined["ibes_ticker"] = combined["ibes_ticker"].astype(str).str.upper()
    combined["cusip"] = combined["cusip"].astype(str)
    combined["permno"] = pd.to_numeric(combined["permno"], errors="coerce").astype("Int64")
    combined["permco"] = pd.to_numeric(combined["permco"], errors="coerce").astype("Int64")
    return combined.drop_duplicates().reset_index(drop=True)


def _base_links(config: SueLinkageRescueConfig) -> pd.DataFrame:
    frames = [pd.read_csv(config.existing_links_path)]
    output_path = Path(config.output_links_path)
    if output_path.exists():
        frames.append(pd.read_csv(output_path))
    return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)


def _exact_cusip_rescue_rows(frame: pd.DataFrame) -> int:
    if "link_method" not in frame.columns:
        return 0
    return int(frame["link_method"].astype(str).eq("crsp_stocknames_exact_cusip_rescue").sum())


def _empty_rescued_links() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ibes_ticker",
            "cusip",
            "permno",
            "permco",
            "link_method",
            "link_start_date",
            "link_end_date",
            "link_validity_flag",
            "source_event_id",
        ]
    )


def _chunks(values: list[str], batch_size: int) -> list[list[str]]:
    return [values[index : index + batch_size] for index in range(0, len(values), batch_size)]
