"""WRDS market-label rescue for real 8-K D2 source admission.

This module only writes a local CRSP daily price cache for D2 observability.
It does not produce alpha scores, expected returns, Q1/Q2 handoffs, optimizer
inputs, orders, broker workflows, paper workflows, or production approvals.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


PRIORITY_SUBTYPES = {
    "auditor_change",
    "cfo_departure",
    "ceo_departure",
    "material_agreement_termination",
    "restatement_amendment",
}


class WrdsLikeConnection(Protocol):
    def raw_sql(self, query: str) -> pd.DataFrame:
        ...


def run_eightk_wrds_market_rescue(
    *,
    event_registry_path: str | Path,
    output_path: str | Path,
    manifest_path: str | Path,
    connection: WrdsLikeConnection,
    wrds_max_date: str = "2024-12-31",
    max_events: int | None = None,
    lookback_days: int = 40,
    lookahead_days: int = 80,
    source_table: str = "crsp.dsf",
) -> dict[str, Any]:
    """Write a bounded CRSP price cache for priority 8-K D2 events."""

    events = pd.read_csv(event_registry_path, low_memory=False).fillna("")
    eligible, skipped_after_max = _eligible_events(events, wrds_max_date=wrds_max_date)
    if max_events is not None:
        eligible = eligible.head(max_events).copy()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest = Path(manifest_path)
    manifest.parent.mkdir(parents=True, exist_ok=True)

    if eligible.empty:
        prices = pd.DataFrame(
            columns=[
                "permno",
                "ticker",
                "date",
                "adjusted_close",
                "return",
                "volume",
                "market_cap",
                "source_table",
            ],
        )
        prices.to_csv(output, index=False)
        summary = _summary(
            status="completed_no_eligible_events",
            event_registry_path=event_registry_path,
            output_path=output,
            source_table=source_table,
            eligible_event_count=0,
            skipped_after_wrds_max_date=skipped_after_max,
            linked_permno_count=0,
            row_count=0,
            wrds_max_date=wrds_max_date,
        )
        manifest.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return summary

    links = _query_stocknames(connection, eligible)
    linked = _asof_link_events_to_permnos(eligible, links)
    permnos = sorted(set(pd.to_numeric(linked["permno"], errors="coerce").dropna().astype(int).tolist()))
    if permnos:
        start = (linked["tradable_date"].min() - timedelta(days=lookback_days)).date().isoformat()
        end = min(linked["tradable_date"].max() + timedelta(days=lookahead_days), pd.Timestamp(wrds_max_date)).date().isoformat()
        raw_prices = connection.raw_sql(_price_query(permnos=permnos, start_date=start, end_date=end, source_table=source_table))
        prices = _normalize_prices(raw_prices, linked, source_table)
    else:
        prices = pd.DataFrame(columns=["permno", "ticker", "date", "adjusted_close", "return", "volume", "market_cap", "source_table"])
    prices.to_csv(output, index=False)
    summary = _summary(
        status="completed",
        event_registry_path=event_registry_path,
        output_path=output,
        source_table=source_table,
        eligible_event_count=len(eligible),
        skipped_after_wrds_max_date=skipped_after_max,
        linked_permno_count=len(permnos),
        row_count=len(prices),
        wrds_max_date=wrds_max_date,
    )
    manifest.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _eligible_events(events: pd.DataFrame, *, wrds_max_date: str) -> tuple[pd.DataFrame, int]:
    frame = events.copy()
    frame["tradable_date"] = pd.to_datetime(frame["tradable_timestamp"], errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()
    max_date = pd.Timestamp(wrds_max_date)
    priority = frame["eightk_subtype"].isin(PRIORITY_SUBTYPES)
    dated = frame["tradable_date"].notna()
    before_max = frame["tradable_date"].le(max_date)
    skipped_after_max = int((priority & dated & ~before_max).sum())
    eligible = frame[priority & dated & before_max].copy()
    eligible = eligible[eligible["ticker"].astype(str).str.len().gt(0)]
    return eligible.sort_values(["ticker", "tradable_date", "event_id"]).reset_index(drop=True), skipped_after_max


def _query_stocknames(connection: WrdsLikeConnection, events: pd.DataFrame) -> pd.DataFrame:
    tickers = sorted(set(events["ticker"].astype(str).str.upper().tolist()))
    quoted = ", ".join("'" + ticker.replace("'", "''") + "'" for ticker in tickers)
    query = f"""
select permno, ticker, namedt, nameenddt
from crsp.stocknames
where ticker in ({quoted})
"""
    links = connection.raw_sql(query)
    if links.empty:
        return pd.DataFrame(columns=["permno", "ticker", "namedt", "nameenddt"])
    links = links.loc[:, ["permno", "ticker", "namedt", "nameenddt"]].copy()
    links["ticker"] = links["ticker"].astype(str).str.upper()
    links["namedt"] = pd.to_datetime(links["namedt"], errors="coerce")
    links["nameenddt"] = pd.to_datetime(links["nameenddt"], errors="coerce")
    return links.dropna(subset=["permno", "ticker", "namedt", "nameenddt"]).reset_index(drop=True)


def _asof_link_events_to_permnos(events: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    if links.empty:
        linked = events.copy()
        linked["permno"] = pd.NA
        return linked
    rows: list[dict[str, object]] = []
    for event in events.to_dict("records"):
        ticker = str(event["ticker"]).upper()
        event_date = pd.Timestamp(event["tradable_date"])
        candidates = links[
            links["ticker"].eq(ticker)
            & links["namedt"].le(event_date)
            & links["nameenddt"].ge(event_date)
        ]
        row = dict(event)
        row["permno"] = int(candidates.sort_values("namedt").iloc[-1]["permno"]) if not candidates.empty else pd.NA
        rows.append(row)
    return pd.DataFrame(rows)


def _price_query(*, permnos: list[int], start_date: str, end_date: str, source_table: str) -> str:
    values = ",".join(str(int(permno)) for permno in permnos)
    return f"""
select permno, date, prc, ret, vol, shrout
from {source_table}
where permno in ({values})
  and date between '{start_date}' and '{end_date}'
"""


def _normalize_prices(prices: pd.DataFrame, linked_events: pd.DataFrame, source_table: str) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["permno", "ticker", "date", "adjusted_close", "return", "volume", "market_cap", "source_table"])
    frame = prices.copy()
    frame["permno"] = pd.to_numeric(frame["permno"], errors="coerce").astype("Int64")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date.astype(str)
    frame["adjusted_close"] = pd.to_numeric(frame["prc"], errors="coerce").abs()
    frame["return"] = pd.to_numeric(frame["ret"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame.get("vol", pd.Series(dtype=float)), errors="coerce")
    shrout = pd.to_numeric(frame.get("shrout", pd.Series(dtype=float)), errors="coerce")
    frame["market_cap"] = frame["adjusted_close"] * shrout * 1000.0
    ticker_map = (
        linked_events.dropna(subset=["permno"])
        .assign(permno=lambda x: pd.to_numeric(x["permno"], errors="coerce").astype("Int64"))
        .dropna(subset=["permno"])
        .drop_duplicates(subset=["permno"], keep="last")
        .set_index("permno")["ticker"]
        .to_dict()
    )
    frame["ticker"] = frame["permno"].map(ticker_map)
    frame["source_table"] = source_table
    normalized = frame.loc[:, ["permno", "ticker", "date", "adjusted_close", "return", "volume", "market_cap", "source_table"]]
    normalized = normalized.dropna(subset=["permno", "ticker", "date", "adjusted_close"])
    return normalized.sort_values(["ticker", "date"]).reset_index(drop=True)


def _summary(
    *,
    status: str,
    event_registry_path: str | Path,
    output_path: Path,
    source_table: str,
    eligible_event_count: int,
    skipped_after_wrds_max_date: int,
    linked_permno_count: int,
    row_count: int,
    wrds_max_date: str,
) -> dict[str, Any]:
    return {
        "schema_version": "eightk_wrds_market_rescue_manifest.v1",
        "status": status,
        "event_registry_path": str(event_registry_path),
        "output_path": str(output_path),
        "source_table": source_table,
        "wrds_max_date": wrds_max_date,
        "eligible_event_count": int(eligible_event_count),
        "skipped_after_wrds_max_date": int(skipped_after_wrds_max_date),
        "linked_permno_count": int(linked_permno_count),
        "row_count": int(row_count),
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
