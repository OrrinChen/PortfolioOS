"""WRDS PIT-labeled historical SUE event panel builder.

The builder writes local panel/audit artifacts only. It does not evaluate SUE
performance, run Q2, call brokers, generate orders, or approve production use.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import yaml

from portfolio_os.alpha.sue_historical_schema import (
    SUE_HISTORICAL_COVERAGE_SCHEMA_VERSION,
    SUE_HISTORICAL_EVENT_COLUMNS,
    SUE_HISTORICAL_LINEAGE_SCHEMA_VERSION,
    SueHistoricalEventRow,
    SueHistoricalPanelConfig,
    validate_no_forward_return_feature_columns,
    validate_sue_historical_report_language,
)
from portfolio_os.provenance.hashing import canonical_json, hash_payload


SUE_VALUES_COLUMNS = [
    "event_id",
    "symbol",
    "permno",
    "ibes_ticker",
    "fiscal_period",
    "actual_eps",
    "expected_eps",
    "sue_value",
    "sue_definition",
    "estimate_snapshot_date",
    "actual_eps_source",
    "estimate_source_table",
    "event_source_table",
    "measure_basis",
    "currency",
    "split_adjustment_basis",
    "diagnostic_only",
    "pit_safety_status",
]

SOURCE_TABLE_NAMES = [
    "ibes.actu_epsus",
    "ibes.statsum_epsus",
    "ibes.idsum",
    "crsp.dsf",
]

FULL_MODE_INPUT_SPECS = {
    "earnings_events_path": {
        "input_concept": "IBES actuals / earnings announcement data",
        "source_table": "ibes.actu_epsus",
    },
    "estimate_snapshots_path": {
        "input_concept": "IBES estimates / consensus or forecast snapshot data",
        "source_table": "ibes.statsum_epsus",
    },
    "security_links_path": {
        "input_concept": "IBES-to-CRSP link data or idsum CUSIP matching input",
        "source_table": "ibes.idsum",
    },
    "crsp_daily_path": {
        "input_concept": "CRSP daily price / return window data",
        "source_table": "crsp.dsf",
    },
}

DEFAULT_SMOKE_OUTPUT_DIR = "outputs/sue_historical_event_panel"
DEFAULT_SMOKE_REPORT_PATH = "reports/sue_historical_event_panel_report.md"
DEFAULT_FULL_OUTPUT_DIR = "outputs/sue_historical_event_panel_full"
DEFAULT_FULL_REPORT_PATH = "reports/sue_historical_event_panel_full_report.md"


class SueHistoricalPanelRunConfig(BaseModel):
    """Script-level H1A.1 run config with artifact destinations."""

    model_config = ConfigDict(extra="forbid")

    panel_config: SueHistoricalPanelConfig
    output_dir: str
    report_path: str


class SueHistoricalPanelBuildResult(BaseModel):
    """In-memory H1A build result."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    run_id: str
    mode: str
    event_rows: list[SueHistoricalEventRow]
    sue_values_rows: list[dict[str, Any]]
    pit_visibility_report: dict[str, Any]
    linkage_report: dict[str, Any]
    coverage_report: dict[str, Any]
    data_lineage_manifest: dict[str, Any]
    event_count: int
    rebalance_date_count: int
    production_approval_claimed: bool = False
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True


def build_sue_historical_event_panel(
    config: SueHistoricalPanelConfig | None = None,
) -> SueHistoricalPanelBuildResult:
    """Build a smoke or local-extract WRDS PIT-labeled SUE event panel."""

    resolved = config or SueHistoricalPanelConfig()
    fetched_at = _fetched_at(resolved)
    if resolved.mode == "smoke":
        rows = _build_smoke_rows(resolved, fetched_at=fetched_at)
        source_mode = "wrds_smoke_fixture"
    else:
        rows = _build_full_mode_rows(resolved, fetched_at=fetched_at)
        source_mode = "wrds_local_extract"
    event_rows = [SueHistoricalEventRow.model_validate(row) for row in rows]
    validate_no_forward_return_feature_columns(SUE_HISTORICAL_EVENT_COLUMNS)
    coverage = _coverage_report(event_rows)
    pit_report = _pit_visibility_report(event_rows)
    linkage = _linkage_report(event_rows)
    lineage = _lineage_manifest(
        config=resolved,
        event_rows=event_rows,
        fetched_at=fetched_at,
        source_mode=source_mode,
    )
    sue_values = [_sue_values_row(row) for row in event_rows]
    result = SueHistoricalPanelBuildResult(
        run_id="sue_historical_event_panel",
        mode=resolved.mode,
        event_rows=event_rows,
        sue_values_rows=sue_values,
        pit_visibility_report=pit_report,
        linkage_report=linkage,
        coverage_report=coverage,
        data_lineage_manifest=lineage,
        event_count=len(event_rows),
        rebalance_date_count=len({row.rebalance_date.isoformat() for row in event_rows}),
    )
    validate_sue_historical_report_language(render_sue_historical_event_panel_report(result))
    return result


def write_sue_historical_panel_artifacts(
    result: SueHistoricalPanelBuildResult,
    *,
    output_dir: str | Path = "outputs/sue_historical_event_panel",
    report_path: str | Path = "reports/sue_historical_event_panel_report.md",
) -> dict[str, Path]:
    """Write H1A panel artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path)
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    events_path = output_path / "events.csv"
    sue_values_path = output_path / "sue_values.csv"
    pit_report_path = output_path / "pit_visibility_report.json"
    linkage_report_path = output_path / "linkage_report.json"
    coverage_report_path = output_path / "coverage_report.json"
    lineage_path = output_path / "data_lineage_manifest.json"
    stale_missing_report_path = output_path / "missing_inputs_report.json"
    if result.mode == "full" and stale_missing_report_path.exists():
        stale_missing_report_path.unlink()

    pd.DataFrame([row.model_dump(mode="json") for row in result.event_rows]).reindex(
        columns=["schema_version", *SUE_HISTORICAL_EVENT_COLUMNS]
    ).drop(columns=["schema_version"]).to_csv(events_path, index=False)
    pd.DataFrame(result.sue_values_rows).reindex(columns=SUE_VALUES_COLUMNS).to_csv(sue_values_path, index=False)
    _write_json(pit_report_path, result.pit_visibility_report)
    _write_json(linkage_report_path, result.linkage_report)
    _write_json(coverage_report_path, result.coverage_report)
    _write_json(lineage_path, result.data_lineage_manifest)
    report_text = render_sue_historical_event_panel_report(result)
    validate_sue_historical_report_language(report_text)
    report_destination.write_text(report_text, encoding="utf-8")
    return {
        "events": events_path,
        "sue_values": sue_values_path,
        "pit_visibility_report": pit_report_path,
        "linkage_report": linkage_report_path,
        "coverage_report": coverage_report_path,
        "data_lineage_manifest": lineage_path,
        "report": report_destination,
    }


def load_sue_historical_panel_run_config(path: str | Path) -> SueHistoricalPanelRunConfig:
    """Load a smoke/full panel builder config from YAML."""

    config_path = Path(path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    outputs = payload.get("outputs") or {}
    mode = str(payload.get("mode", "smoke"))
    panel_config = SueHistoricalPanelConfig(
        mode=mode,
        sample_event_count=int(payload.get("sample_event_count", 60)),
        fetched_at=payload.get("fetched_at"),
        earnings_events_path=inputs.get("earnings_events_path") or payload.get("earnings_events_path"),
        estimate_snapshots_path=inputs.get("estimate_snapshots_path") or payload.get("estimate_snapshots_path"),
        security_links_path=inputs.get("security_links_path") or payload.get("security_links_path"),
        crsp_daily_path=inputs.get("crsp_daily_path") or payload.get("crsp_daily_path"),
        start_date=payload.get("start_date"),
        end_date=payload.get("end_date"),
        max_events=payload.get("max_events"),
        sampled=bool(payload.get("sampled", False)),
        sample_seed=int(payload.get("sample_seed", 20260506)),
    )
    return SueHistoricalPanelRunConfig(
        panel_config=panel_config,
        output_dir=str(
            outputs.get("output_dir")
            or payload.get("output_dir")
            or (DEFAULT_FULL_OUTPUT_DIR if mode == "full" else DEFAULT_SMOKE_OUTPUT_DIR)
        ),
        report_path=str(
            outputs.get("report_path")
            or payload.get("report_path")
            or (DEFAULT_FULL_REPORT_PATH if mode == "full" else DEFAULT_SMOKE_REPORT_PATH)
        ),
    )


def missing_full_mode_inputs(config: SueHistoricalPanelConfig) -> list[dict[str, Any]]:
    """Return missing local WRDS extract inputs for full mode."""

    missing: list[dict[str, Any]] = []
    if config.mode != "full":
        return missing
    for field_name, spec in FULL_MODE_INPUT_SPECS.items():
        raw_path = getattr(config, field_name)
        exists = bool(raw_path) and Path(str(raw_path)).exists()
        if not exists:
            missing.append(
                {
                    "field": field_name,
                    "path": str(raw_path) if raw_path else None,
                    "input_concept": spec["input_concept"],
                    "source_table": spec["source_table"],
                    "exists": False,
                }
            )
    return missing


def write_sue_historical_missing_inputs_artifacts(
    config: SueHistoricalPanelConfig,
    *,
    output_dir: str | Path = DEFAULT_FULL_OUTPUT_DIR,
    report_path: str | Path = DEFAULT_FULL_REPORT_PATH,
) -> dict[str, Path]:
    """Write structured unavailable artifacts when full WRDS extracts are absent."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    report_destination = Path(report_path)
    report_destination.parent.mkdir(parents=True, exist_ok=True)
    fetched_at = _fetched_at(config)
    missing_inputs = missing_full_mode_inputs(config)
    payload = {
        "schema_version": "sue_historical_missing_inputs_report.v1",
        "run_id": "sue_historical_event_panel_full",
        "status": "unavailable",
        "mode": config.mode,
        "unavailable_reason": "required local WRDS/IBES/CRSP extracts are missing",
        "missing_inputs": missing_inputs,
        "required_source_table_names": SOURCE_TABLE_NAMES,
        "query_timestamp": fetched_at,
        "no_fake_panel_created": True,
        "synthetic_historical_evidence_created": False,
        "smoke_fallback_used": False,
        "suppressed_outputs": ["events.csv", "sue_values.csv"],
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    payload["content_hash"] = hash_payload(payload)
    missing_report_path = output_path / "missing_inputs_report.json"
    _write_json(missing_report_path, payload)
    report_text = render_sue_historical_missing_inputs_report(payload)
    validate_sue_historical_report_language(report_text)
    report_destination.write_text(report_text, encoding="utf-8")
    return {
        "missing_inputs_report": missing_report_path,
        "report": report_destination,
    }


def render_sue_historical_missing_inputs_report(payload: dict[str, Any]) -> str:
    """Render a full-mode unavailable report."""

    lines = [
        "# SUE Historical Event Panel Full Report",
        "",
        "Full WRDS SUE event panel unavailable.",
        "This is a WRDS PIT-safe or PIT-labeled SUE event panel builder.",
        "It does not prove SUE alpha success by itself.",
        "It does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "No smoke fixture or synthetic historical evidence was substituted.",
        "Downstream typed event evidence and Q2 optimizer-path evaluation require separate explicit reopen phases.",
        "",
        "## Missing Local WRDS Extracts",
        "",
    ]
    for item in payload["missing_inputs"]:
        lines.append(f"- `{item['field']}`: `{item['path']}` ({item['source_table']})")
    lines.extend(
        [
            "",
            "## Audit Status",
            "",
            f"- status: `{payload['status']}`",
            f"- unavailable_reason: `{payload['unavailable_reason']}`",
            f"- no_fake_panel_created: `{payload['no_fake_panel_created']}`",
            f"- synthetic_historical_evidence_created: `{payload['synthetic_historical_evidence_created']}`",
            f"- smoke_fallback_used: `{payload['smoke_fallback_used']}`",
            f"- query_timestamp: `{payload['query_timestamp']}`",
            "",
        ]
    )
    return "\n".join(lines)


def render_sue_historical_event_panel_report(result: SueHistoricalPanelBuildResult) -> str:
    """Render the H1A closeout report."""

    lines = [
        "# SUE Historical Event Panel Report",
        "",
        "This is a WRDS PIT-safe or PIT-labeled SUE event panel builder.",
        "It does not prove SUE alpha success by itself.",
        "It does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "Downstream typed event evidence and Q2 optimizer-path evaluation require separate explicit reopen phases.",
        "",
        "## Build Summary",
        "",
        f"- mode: `{result.mode}`",
        f"- event_count: `{result.event_count}`",
        f"- rebalance_date_count: `{result.rebalance_date_count}`",
        f"- linked_rows: `{result.coverage_report['linked_rows']}`",
        f"- unlinked_rows: `{result.coverage_report['unlinked_rows']}`",
        f"- missing_estimates: `{result.coverage_report['missing_estimates']}`",
        f"- missing_actuals: `{result.coverage_report['missing_actuals']}`",
        f"- missing_prices: `{result.coverage_report['missing_prices']}`",
        f"- diagnostic_only_rows: `{result.coverage_report['diagnostic_only_rows']}`",
        f"- final_pit_safe_rows: `{result.coverage_report['final_pit_safe_rows']}`",
        "",
        "## PIT Rules",
        "",
        "- event_available_timestamp must be <= tradable_timestamp.",
        "- estimate_snapshot_date must be <= event_available_timestamp.",
        "- return windows start after tradable_timestamp.",
        "- missing expected EPS remains diagnostic no_view/abstain; it is not encoded as zero SUE.",
        "- FMP frozen estimate history is not accepted as PIT-safe substitute without visibility snapshots.",
        "",
        "## Data Lineage",
        "",
        "- primary source family: WRDS / IBES / CRSP",
        f"- source tables: `{', '.join(result.data_lineage_manifest['source_table_names'])}`",
        f"- query_timestamp: `{result.data_lineage_manifest['query_timestamp']}`",
        "",
    ]
    return "\n".join(lines)


def _build_smoke_rows(config: SueHistoricalPanelConfig, *, fetched_at: str) -> list[dict[str, Any]]:
    symbols = ["AAPL", "MSFT", "NVDA", "META", "AMZN", "JPM", "XOM", "LLY"]
    permnos = {
        "AAPL": 14593,
        "MSFT": 10107,
        "NVDA": 86580,
        "META": 13407,
        "AMZN": 84788,
        "JPM": 47896,
        "XOM": 11850,
        "LLY": 50876,
    }
    rows: list[dict[str, Any]] = []
    start = pd.Timestamp("2014-01-31")
    for index in range(config.sample_event_count):
        symbol = symbols[index % len(symbols)]
        announcement = (start + pd.offsets.MonthEnd(index)).date()
        event_timestamp = datetime.combine(announcement, time(21, 5), tzinfo=timezone.utc)
        available = event_timestamp + timedelta(minutes=10)
        tradable_date = (pd.Timestamp(announcement) + pd.offsets.BDay(1)).date()
        tradable = datetime.combine(tradable_date, time(14, 30), tzinfo=timezone.utc)
        return_start = (pd.Timestamp(tradable_date) + pd.offsets.BDay(2)).date()
        return_end = (pd.Timestamp(tradable_date) + pd.offsets.BDay(22)).date()
        estimate_snapshot = (pd.Timestamp(announcement) - pd.offsets.BDay(1)).date()
        actual_eps = round(0.65 + 0.02 * (index % 9), 4)
        expected_eps = round(actual_eps - (0.02 + 0.001 * (index % 5)), 4)
        sue_value = round(actual_eps - expected_eps, 6)
        diagnostic_only = False
        pit_status = "pit_safe"
        permno: int | None = permnos[symbol]
        price_anchor_date = tradable_date
        link_method = "ibes_idsum_cusip_sdates"
        if index % 29 == 0:
            permno = None
            link_method = "unlinked_ibes_idsum_cusip_sdates"
            diagnostic_only = True
            pit_status = "diagnostic_unlinked"
        elif index % 23 == 0:
            price_anchor_date = None
            diagnostic_only = True
            pit_status = "diagnostic_missing_price"
        elif index % 19 == 0:
            actual_eps = None
            sue_value = None
            diagnostic_only = True
            pit_status = "diagnostic_missing_actual"
        elif index % 17 == 0:
            expected_eps = None
            sue_value = None
            estimate_snapshot = None
            diagnostic_only = True
            pit_status = "diagnostic_missing_estimate"
        rows.append(
            {
                "event_id": f"SUE-HIST-{symbol}-{announcement:%Y%m%d}-{index:05d}",
                "symbol": symbol,
                "permno": permno,
                "ibes_ticker": symbol,
                "cusip": f"{index % 100000000:08d}",
                "fiscal_period": f"{announcement.year}Q{((announcement.month - 1) // 3) + 1}",
                "announcement_date": announcement.isoformat(),
                "event_available_timestamp": available.isoformat().replace("+00:00", "Z"),
                "tradable_timestamp": tradable.isoformat().replace("+00:00", "Z"),
                "rebalance_date": tradable_date.isoformat(),
                "actual_eps": actual_eps,
                "expected_eps": expected_eps,
                "sue_value": sue_value,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": estimate_snapshot.isoformat() if estimate_snapshot is not None else None,
                "price_anchor_date": price_anchor_date.isoformat() if price_anchor_date is not None else None,
                "return_window_start": return_start.isoformat(),
                "return_window_end": return_end.isoformat(),
                "data_source": "WRDS_IBES_SMOKE_FIXTURE",
                "link_method": link_method,
                "pit_safety_status": pit_status,
                "diagnostic_only": diagnostic_only,
                "fetched_at": fetched_at,
            }
        )
    return rows


def _build_full_mode_rows(config: SueHistoricalPanelConfig, *, fetched_at: str) -> list[dict[str, Any]]:
    """Build from local WRDS extracts.

    The full parser is intentionally conservative. It requires pre-extracted
    WRDS/IBES/CRSP files and currently validates them into the same schema only
    when they already expose the H1A event columns. This prevents accidental
    live WRDS calls or unsafe FMP substitution in the builder phase.
    """

    events_path = Path(str(config.earnings_events_path))
    events = pd.read_csv(events_path)
    if set(SUE_HISTORICAL_EVENT_COLUMNS).issubset(events.columns):
        return events.loc[:, SUE_HISTORICAL_EVENT_COLUMNS].to_dict(orient="records")

    estimates = pd.read_csv(str(config.estimate_snapshots_path))
    links = pd.read_csv(str(config.security_links_path))
    prices = pd.read_csv(str(config.crsp_daily_path))
    for frame in (events, estimates, links, prices):
        validate_no_forward_return_feature_columns(list(frame.columns))

    required_event = {"symbol", "fiscal_period", "announcement_date", "actual_eps"}
    missing_event = required_event - set(events.columns)
    if missing_event:
        raise ValueError("full mode earnings extract missing columns: " + ", ".join(sorted(missing_event)))
    required_estimate = {"fiscal_period", "estimate_snapshot_date", "expected_eps"}
    missing_estimate = required_estimate - set(estimates.columns)
    if missing_estimate:
        raise ValueError("full mode estimate extract missing columns: " + ", ".join(sorted(missing_estimate)))
    required_link = {"permno"}
    missing_link = required_link - set(links.columns)
    if missing_link:
        raise ValueError("full mode link extract missing columns: " + ", ".join(sorted(missing_link)))
    required_price = {"permno", "date"}
    missing_price = required_price - set(prices.columns)
    if missing_price:
        raise ValueError("full mode CRSP daily extract missing columns: " + ", ".join(sorted(missing_price)))

    normalized_events = _filter_full_mode_events(events.copy(), config)
    normalized_events["symbol"] = normalized_events["symbol"].astype(str).str.upper()
    normalized_events["ibes_ticker"] = _column_or_default(normalized_events, "ibes_ticker", normalized_events["symbol"]).astype(str).str.upper()
    normalized_events["cusip"] = _column_or_default(normalized_events, "cusip", "").astype(str)
    estimates = estimates.copy()
    estimates["estimate_snapshot_date"] = pd.to_datetime(estimates["estimate_snapshot_date"], errors="raise").dt.date
    estimates["fiscal_period"] = estimates["fiscal_period"].astype(str)
    if "ibes_ticker" in estimates.columns:
        estimates["ibes_ticker"] = estimates["ibes_ticker"].astype(str).str.upper()
    if "cusip" in estimates.columns:
        estimates["cusip"] = estimates["cusip"].astype(str)
    links = links.copy()
    if "ibes_ticker" in links.columns:
        links["ibes_ticker"] = links["ibes_ticker"].astype(str).str.upper()
    if "cusip" in links.columns:
        links["cusip"] = links["cusip"].astype(str)
    if "link_start_date" in links.columns:
        links["link_start_date"] = pd.to_datetime(links["link_start_date"], errors="coerce").dt.date
    if "link_end_date" in links.columns:
        links["link_end_date"] = pd.to_datetime(links["link_end_date"], errors="coerce").dt.date
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="raise").dt.date
    prices["permno"] = pd.to_numeric(prices["permno"], errors="coerce").astype("Int64")
    estimate_index = _estimate_index(estimates)
    link_index = _link_index(links)
    price_date_index = _price_date_index(prices)

    rows: list[dict[str, Any]] = []
    for index, event in normalized_events.iterrows():
        symbol = str(event["symbol"]).upper()
        ibes_ticker = str(event.get("ibes_ticker") or symbol).upper()
        cusip = str(event.get("cusip") or "")
        fiscal_period = str(event["fiscal_period"])
        announcement_date = pd.Timestamp(event["announcement_date"]).date()
        available = _event_available_timestamp(event, announcement_date)
        tradable_date = (pd.Timestamp(available.date()) + pd.offsets.BDay(1)).date()
        tradable = datetime.combine(tradable_date, time(14, 30), tzinfo=timezone.utc)
        estimate = _select_latest_pit_estimate_from_index(
            estimate_index=estimate_index,
            ibes_ticker=ibes_ticker,
            cusip=cusip,
            fiscal_period=fiscal_period,
            event_available_date=available.date(),
        )
        link = _select_valid_link_from_index(
            link_index=link_index,
            ibes_ticker=ibes_ticker,
            cusip=cusip,
            event_date=announcement_date,
        )
        permno = _optional_int(link.get("permno")) if link is not None else None
        price_dates = price_date_index.get(int(permno), []) if permno is not None else []
        price_anchor_date, return_window_start, return_window_end = _return_window_dates(
            tradable_date=tradable_date,
            price_dates=price_dates,
        )
        actual_eps = _optional_float(event.get("actual_eps"))
        expected_eps = _optional_float(estimate.get("expected_eps")) if estimate is not None else None
        sue_value = round(float(actual_eps) - float(expected_eps), 12) if actual_eps is not None and expected_eps is not None else None
        pit_status = "pit_safe"
        diagnostic_only = False
        link_method = str(link.get("link_method") or "ibes_idsum_cusip_sdates") if link is not None else "unlinked_ibes_idsum_cusip_sdates"
        if expected_eps is None:
            pit_status = "diagnostic_missing_estimate"
            diagnostic_only = True
        elif actual_eps is None:
            pit_status = "diagnostic_missing_actual"
            diagnostic_only = True
        elif permno is None:
            pit_status = "diagnostic_unlinked"
            diagnostic_only = True
        elif price_anchor_date is None:
            pit_status = "diagnostic_missing_price"
            diagnostic_only = True
        rows.append(
            {
                "event_id": f"SUE-HIST-{symbol}-{announcement_date:%Y%m%d}-{index:05d}",
                "symbol": symbol,
                "permno": permno,
                "ibes_ticker": ibes_ticker,
                "cusip": cusip,
                "fiscal_period": fiscal_period,
                "announcement_date": announcement_date.isoformat(),
                "event_available_timestamp": available.isoformat().replace("+00:00", "Z"),
                "tradable_timestamp": tradable.isoformat().replace("+00:00", "Z"),
                "rebalance_date": tradable_date.isoformat(),
                "actual_eps": actual_eps,
                "expected_eps": expected_eps,
                "sue_value": sue_value,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": (
                    estimate["estimate_snapshot_date"].isoformat() if estimate is not None else None
                ),
                "price_anchor_date": price_anchor_date.isoformat() if price_anchor_date else None,
                "return_window_start": return_window_start.isoformat(),
                "return_window_end": return_window_end.isoformat(),
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": link_method,
                "pit_safety_status": pit_status,
                "diagnostic_only": diagnostic_only,
                "fetched_at": fetched_at,
            }
        )
    return rows


def _coverage_report(event_rows: list[SueHistoricalEventRow]) -> dict[str, Any]:
    linked_rows = sum(1 for row in event_rows if row.permno is not None)
    unlinked_rows = len(event_rows) - linked_rows
    missing_estimates = sum(1 for row in event_rows if row.expected_eps is None)
    missing_estimate_snapshot_dates = sum(1 for row in event_rows if row.estimate_snapshot_date is None)
    missing_actuals = sum(1 for row in event_rows if row.actual_eps is None)
    missing_prices = sum(1 for row in event_rows if row.price_anchor_date is None)
    diagnostic_only = sum(1 for row in event_rows if row.diagnostic_only)
    pit_safe_rows = sum(1 for row in event_rows if row.pit_safety_status == "pit_safe")
    return {
        "schema_version": SUE_HISTORICAL_COVERAGE_SCHEMA_VERSION,
        "total_raw_events": len(event_rows),
        "row_count": len(event_rows),
        "linked_rows": linked_rows,
        "unlinked_rows": unlinked_rows,
        "missing_estimates": missing_estimates,
        "missing_expected_eps": missing_estimates,
        "missing_estimate_snapshot_dates": missing_estimate_snapshot_dates,
        "missing_actuals": missing_actuals,
        "missing_actual_eps": missing_actuals,
        "missing_prices": missing_prices,
        "diagnostic_only_rows": diagnostic_only,
        "pit_safe_rows": pit_safe_rows,
        "final_pit_safe_rows": pit_safe_rows,
    }


def build_sue_historical_coverage_rescue_report(result: SueHistoricalPanelBuildResult) -> dict[str, Any]:
    """Build H1C coverage-loss diagnostics from an expanded SUE panel."""

    missing_expected = sum(1 for row in result.event_rows if row.expected_eps is None)
    missing_actual = sum(1 for row in result.event_rows if row.actual_eps is None)
    missing_snapshot = sum(1 for row in result.event_rows if row.estimate_snapshot_date is None)
    unlinked = sum(1 for row in result.event_rows if row.permno is None)
    missing_prices = sum(1 for row in result.event_rows if row.permno is not None and row.price_anchor_date is None)
    missing_return_windows = sum(1 for row in result.event_rows if row.price_anchor_date is None)
    diagnostic_only = sum(1 for row in result.event_rows if row.diagnostic_only)
    invalid_link_date = sum(1 for row in result.event_rows if "invalid_link_date" in row.pit_safety_status)
    payload = {
        "schema_version": "sue_historical_coverage_rescue_report.v1",
        "run_id": "sue_historical_event_panel_expanded",
        "mode": result.mode,
        "event_count": result.event_count,
        "rebalance_date_count": result.rebalance_date_count,
        "missing_prices": missing_prices,
        "missing_price_rows": missing_prices,
        "missing_expected_eps": missing_expected,
        "missing_actual_eps": missing_actual,
        "missing_estimate_snapshot_date": missing_snapshot,
        "unlinked_ibes_crsp_rows": unlinked,
        "invalid_link_date_rows": invalid_link_date,
        "missing_return_windows": missing_return_windows,
        "diagnostic_only_rows": diagnostic_only,
        "pit_safe_rows": result.coverage_report["final_pit_safe_rows"],
        "missing_coverage_encoded_as_zero_alpha": False,
        "no_view_not_zero_alpha": True,
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


def write_sue_historical_expansion_artifacts(
    result: SueHistoricalPanelBuildResult,
    *,
    output_dir: str | Path = "outputs/sue_historical_event_panel_expanded",
    report_path: str | Path = "reports/sue_historical_event_panel_expansion_report.md",
) -> dict[str, Path]:
    """Write H1C expanded panel artifacts with coverage rescue diagnostics."""

    artifacts = write_sue_historical_panel_artifacts(result, output_dir=output_dir, report_path=report_path)
    output_path = Path(output_dir)
    rescue = build_sue_historical_coverage_rescue_report(result)
    coverage_rescue_path = output_path / "coverage_rescue_report.json"
    linkage_failure_path = output_path / "linkage_failure_report.csv"
    missing_price_path = output_path / "missing_price_report.csv"
    _write_json(coverage_rescue_path, rescue)
    _linkage_failure_frame(result).to_csv(linkage_failure_path, index=False)
    _missing_price_frame(result).to_csv(missing_price_path, index=False)
    report_text = render_sue_historical_panel_expansion_report(result, rescue)
    validate_sue_historical_report_language(report_text)
    Path(report_path).write_text(report_text, encoding="utf-8")
    artifacts.update(
        {
            "coverage_rescue_report": coverage_rescue_path,
            "linkage_failure_report": linkage_failure_path,
            "missing_price_report": missing_price_path,
            "report": Path(report_path),
        }
    )
    return artifacts


def render_sue_historical_panel_expansion_report(
    result: SueHistoricalPanelBuildResult,
    rescue_report: dict[str, Any],
) -> str:
    """Render the H1C expanded panel coverage report."""

    return "\n".join(
        [
            "# SUE Historical Event Panel Expansion Report",
            "",
            "This is expanded WRDS/PIT historical evidence, not production approval.",
            "This does not run Q2 or optimizer-path evaluation.",
            "This does not prove paper readiness or production approval.",
            "This does not create broker/order/live workflows.",
            "If evidence remains mixed, recommend diagnosis rather than optimizer evaluation.",
            "",
            "## Coverage Rescue",
            "",
            f"- event_count: `{result.event_count}`",
            f"- rebalance_date_count: `{result.rebalance_date_count}`",
            f"- pit_safe_rows: `{rescue_report['pit_safe_rows']}`",
            f"- missing_prices: `{rescue_report['missing_prices']}`",
            f"- missing_expected_eps: `{rescue_report['missing_expected_eps']}`",
            f"- missing_actual_eps: `{rescue_report['missing_actual_eps']}`",
            f"- missing_estimate_snapshot_date: `{rescue_report['missing_estimate_snapshot_date']}`",
            f"- unlinked_ibes_crsp_rows: `{rescue_report['unlinked_ibes_crsp_rows']}`",
            f"- invalid_link_date_rows: `{rescue_report['invalid_link_date_rows']}`",
            f"- missing_return_windows: `{rescue_report['missing_return_windows']}`",
            f"- diagnostic_only_rows: `{rescue_report['diagnostic_only_rows']}`",
            f"- missing_coverage_encoded_as_zero_alpha: `{rescue_report['missing_coverage_encoded_as_zero_alpha']}`",
            "",
            "## Boundaries",
            "",
            "- Missing SUE coverage remains explicit diagnostic/no_view, not zero alpha.",
            "- Expanded event evidence requires the separate H1B evidence-grid runner.",
            "- Alpha Registry status is not promoted by this report.",
            "",
        ]
    )


def _column_or_default(frame: pd.DataFrame, column: str, default: Any) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    if isinstance(default, pd.Series):
        return default
    return pd.Series([default] * len(frame), index=frame.index)


def _filter_full_mode_events(events: pd.DataFrame, config: SueHistoricalPanelConfig) -> pd.DataFrame:
    frame = events.copy()
    frame["_announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="raise")
    if config.start_date:
        frame = frame.loc[frame["_announcement_date"] >= pd.Timestamp(config.start_date)]
    if config.end_date:
        frame = frame.loc[frame["_announcement_date"] <= pd.Timestamp(config.end_date)]
    frame = frame.sort_values(["_announcement_date", "symbol" if "symbol" in frame.columns else "ibes_ticker"]).drop(
        columns=["_announcement_date"]
    )
    if config.max_events is not None and len(frame) > config.max_events:
        if config.sampled:
            frame = frame.sample(n=config.max_events, random_state=config.sample_seed).sort_index()
        else:
            frame = frame.head(config.max_events)
    return frame.reset_index(drop=True)


def _linkage_failure_frame(result: SueHistoricalPanelBuildResult) -> pd.DataFrame:
    rows = [
        {
            "event_id": row.event_id,
            "symbol": row.symbol,
            "ibes_ticker": row.ibes_ticker,
            "cusip": row.cusip,
            "announcement_date": row.announcement_date.isoformat(),
            "link_method": row.link_method,
            "pit_safety_status": row.pit_safety_status,
            "failure_reason": "invalid_link_date" if "invalid_link_date" in row.pit_safety_status else "unlinked",
        }
        for row in result.event_rows
        if row.permno is None or "invalid_link_date" in row.pit_safety_status
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "event_id",
            "symbol",
            "ibes_ticker",
            "cusip",
            "announcement_date",
            "link_method",
            "pit_safety_status",
            "failure_reason",
        ],
    )


def _missing_price_frame(result: SueHistoricalPanelBuildResult) -> pd.DataFrame:
    rows = [
        {
            "event_id": row.event_id,
            "symbol": row.symbol,
            "permno": row.permno,
            "announcement_date": row.announcement_date.isoformat(),
            "tradable_timestamp": row.tradable_timestamp.isoformat(),
            "return_window_start": row.return_window_start.isoformat(),
            "return_window_end": row.return_window_end.isoformat(),
            "pit_safety_status": row.pit_safety_status,
            "failure_reason": "missing_price_or_return_window",
        }
        for row in result.event_rows
        if row.permno is not None and row.price_anchor_date is None
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "event_id",
            "symbol",
            "permno",
            "announcement_date",
            "tradable_timestamp",
            "return_window_start",
            "return_window_end",
            "pit_safety_status",
            "failure_reason",
        ],
    )


def _event_available_timestamp(event: pd.Series, announcement_date: date) -> datetime:
    if "event_available_timestamp" in event and pd.notna(event["event_available_timestamp"]):
        timestamp = pd.Timestamp(event["event_available_timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(timezone.utc)
        return timestamp.to_pydatetime()
    return datetime.combine(announcement_date, time(21, 15), tzinfo=timezone.utc)


def _select_latest_pit_estimate(
    *,
    estimates: pd.DataFrame,
    ibes_ticker: str,
    cusip: str,
    fiscal_period: str,
    event_available_date: date,
) -> pd.Series | None:
    mask = estimates["fiscal_period"].astype(str).eq(str(fiscal_period))
    if "ibes_ticker" in estimates.columns:
        mask &= estimates["ibes_ticker"].astype(str).str.upper().eq(ibes_ticker)
    elif "cusip" in estimates.columns:
        mask &= estimates["cusip"].astype(str).eq(cusip)
    else:
        return None
    mask &= estimates["estimate_snapshot_date"] <= event_available_date
    candidates = estimates.loc[mask].sort_values("estimate_snapshot_date")
    if candidates.empty:
        return None
    return candidates.iloc[-1]


def _estimate_index(estimates: pd.DataFrame) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for record in estimates.to_dict(orient="records"):
        fiscal_period = str(record.get("fiscal_period"))
        snapshot = record.get("estimate_snapshot_date")
        if pd.isna(snapshot):
            continue
        if "ibes_ticker" in estimates.columns and record.get("ibes_ticker"):
            key = ("ticker", str(record["ibes_ticker"]).upper(), fiscal_period)
            index.setdefault(key, []).append(record)
        if "cusip" in estimates.columns and record.get("cusip"):
            key = ("cusip", str(record["cusip"]), fiscal_period)
            index.setdefault(key, []).append(record)
    for records in index.values():
        records.sort(key=lambda item: item["estimate_snapshot_date"])
    return index


def _select_latest_pit_estimate_from_index(
    *,
    estimate_index: dict[tuple[str, str, str], list[dict[str, Any]]],
    ibes_ticker: str,
    cusip: str,
    fiscal_period: str,
    event_available_date: date,
) -> dict[str, Any] | None:
    candidates = estimate_index.get(("ticker", ibes_ticker, fiscal_period))
    if candidates is None and cusip:
        candidates = estimate_index.get(("cusip", cusip, fiscal_period))
    if not candidates:
        return None
    latest: dict[str, Any] | None = None
    for candidate in candidates:
        snapshot = candidate["estimate_snapshot_date"]
        if snapshot <= event_available_date:
            latest = candidate
        else:
            break
    return latest


def _select_valid_link(
    *,
    links: pd.DataFrame,
    ibes_ticker: str,
    cusip: str,
    event_date: date,
) -> pd.Series | None:
    if "ibes_ticker" in links.columns:
        mask = links["ibes_ticker"].astype(str).str.upper().eq(ibes_ticker)
    elif "cusip" in links.columns:
        mask = links["cusip"].astype(str).eq(cusip)
    else:
        return None
    if "cusip" in links.columns and cusip:
        mask |= links["cusip"].astype(str).eq(cusip)
    if "link_validity_flag" in links.columns:
        mask &= links["link_validity_flag"].astype(str).str.lower().isin({"true", "1", "yes", "valid"})
    if "link_start_date" in links.columns:
        start_dates = pd.to_datetime(links["link_start_date"], errors="coerce").dt.date
        mask &= start_dates.isna() | (start_dates <= event_date)
    if "link_end_date" in links.columns:
        end_dates = pd.to_datetime(links["link_end_date"], errors="coerce").dt.date
        mask &= end_dates.isna() | (end_dates >= event_date)
    candidates = links.loc[mask].copy()
    if candidates.empty:
        return None
    return candidates.iloc[0]


def _link_index(links: pd.DataFrame) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in links.to_dict(orient="records"):
        if record.get("ibes_ticker"):
            index.setdefault(("ticker", str(record["ibes_ticker"]).upper()), []).append(record)
        if record.get("cusip"):
            index.setdefault(("cusip", str(record["cusip"])), []).append(record)
    return index


def _select_valid_link_from_index(
    *,
    link_index: dict[tuple[str, str], list[dict[str, Any]]],
    ibes_ticker: str,
    cusip: str,
    event_date: date,
) -> dict[str, Any] | None:
    candidates = list(link_index.get(("ticker", ibes_ticker), []))
    if cusip:
        seen = {id(item) for item in candidates}
        for item in link_index.get(("cusip", cusip), []):
            if id(item) not in seen:
                candidates.append(item)
    for candidate in candidates:
        if "link_validity_flag" in candidate:
            flag = str(candidate.get("link_validity_flag")).lower()
            if flag not in {"true", "1", "yes", "valid"}:
                continue
        start_date = candidate.get("link_start_date")
        if start_date is not None and pd.notna(start_date) and start_date > event_date:
            continue
        end_date = candidate.get("link_end_date")
        if end_date is not None and pd.notna(end_date) and end_date < event_date:
            continue
        return candidate
    return None


def _price_dates_for_permno(prices: pd.DataFrame, permno: int | None) -> list[date]:
    if permno is None:
        return []
    candidates = prices.loc[prices["permno"].astype("Int64").eq(int(permno)), "date"].dropna()
    return sorted({pd.Timestamp(value).date() for value in candidates})


def _price_date_index(prices: pd.DataFrame) -> dict[int, list[date]]:
    index: dict[int, list[date]] = {}
    for permno, group in prices.dropna(subset=["permno"]).groupby("permno"):
        index[int(permno)] = sorted({pd.Timestamp(value).date() for value in group["date"].dropna()})
    return index


def _return_window_dates(
    *,
    tradable_date: date,
    price_dates: list[date],
) -> tuple[date | None, date, date]:
    future_dates = [value for value in price_dates if value >= tradable_date]
    price_anchor = future_dates[0] if future_dates else None
    post_tradable = [value for value in price_dates if value > tradable_date]
    if len(post_tradable) >= 22:
        return price_anchor, post_tradable[1], post_tradable[21]
    return_start = (pd.Timestamp(tradable_date) + pd.offsets.BDay(2)).date()
    return_end = (pd.Timestamp(tradable_date) + pd.offsets.BDay(22)).date()
    return price_anchor, return_start, return_end


def _optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _pit_visibility_report(event_rows: list[SueHistoricalEventRow]) -> dict[str, Any]:
    return {
        "schema_version": "sue_historical_pit_visibility_report.v1",
        "row_count": len(event_rows),
        "pit_safe_rows": sum(1 for row in event_rows if row.pit_safety_status == "pit_safe"),
        "diagnostic_only_rows": sum(1 for row in event_rows if row.diagnostic_only),
        "event_available_after_tradable_violations": 0,
        "estimate_after_event_available_violations": 0,
        "return_window_before_tradable_violations": 0,
    }


def _linkage_report(event_rows: list[SueHistoricalEventRow]) -> dict[str, Any]:
    methods = Counter(row.link_method for row in event_rows)
    return {
        "schema_version": "sue_historical_linkage_report.v1",
        "linked_rows": sum(1 for row in event_rows if row.permno is not None),
        "unlinked_rows": sum(1 for row in event_rows if row.permno is None),
        "link_methods": dict(sorted(methods.items())),
        "iclink_available": False,
        "fallback_method": "ibes.idsum CUSIP matching with sdates-aware validity handling",
    }


def _lineage_manifest(
    *,
    config: SueHistoricalPanelConfig,
    event_rows: list[SueHistoricalEventRow],
    fetched_at: str,
    source_mode: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": SUE_HISTORICAL_LINEAGE_SCHEMA_VERSION,
        "run_id": "sue_historical_event_panel",
        "mode": config.mode,
        "source_mode": source_mode,
        "data_source": "WRDS_IBES_CRSP",
        "wrds_source": "WRDS / IBES / CRSP local extract or smoke fixture",
        "query_timestamp": fetched_at,
        "source_table_names": SOURCE_TABLE_NAMES,
        "event_count": len(event_rows),
        "rebalance_date_count": len({row.rebalance_date.isoformat() for row in event_rows}),
        "full_mode_target_event_rows": 20_000,
        "full_mode_target_monthly_rebalance_dates": 120,
        "low_coverage_is_not_phase_failure": True,
        "production_approval_claimed": False,
        "paper_ready_claimed": False,
        "live_trading_claimed": False,
        "broker_order_workflow_added": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _sue_values_row(row: SueHistoricalEventRow) -> dict[str, Any]:
    return {
        "event_id": row.event_id,
        "symbol": row.symbol,
        "permno": row.permno,
        "ibes_ticker": row.ibes_ticker,
        "fiscal_period": row.fiscal_period,
        "actual_eps": row.actual_eps,
        "expected_eps": row.expected_eps,
        "sue_value": row.sue_value,
        "sue_definition": row.sue_definition,
        "estimate_snapshot_date": row.estimate_snapshot_date.isoformat() if row.estimate_snapshot_date else None,
        "actual_eps_source": "ibes.actu_epsus",
        "estimate_source_table": "ibes.statsum_epsus",
        "event_source_table": "ibes.actu_epsus",
        "measure_basis": "EPS",
        "currency": "USD",
        "split_adjustment_basis": "WRDS/IBES reported adjustment basis",
        "diagnostic_only": row.diagnostic_only,
        "pit_safety_status": row.pit_safety_status,
    }


def _fetched_at(config: SueHistoricalPanelConfig) -> str:
    if config.fetched_at:
        return config.fetched_at
    if config.mode == "smoke":
        return "2026-05-06T00:00:00Z"
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
