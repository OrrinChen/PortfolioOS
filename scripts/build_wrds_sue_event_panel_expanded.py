"""Build the expanded WRDS/PIT SUE event panel and coverage rescue report."""

from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_historical_panel import (  # noqa: E402
    build_sue_historical_coverage_rescue_report,
    build_sue_historical_event_panel,
    load_sue_historical_panel_run_config,
    missing_full_mode_inputs,
    write_sue_historical_expansion_artifacts,
    write_sue_historical_missing_inputs_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build expanded WRDS/PIT SUE event panel artifacts.")
    parser.add_argument("--config", default="configs/wrds_sue_event_panel_expanded.yaml")
    parser.add_argument("--skip-wrds-refresh", action="store_true")
    args = parser.parse_args()

    payload = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    if not args.skip_wrds_refresh and (payload.get("wrds_extract") or {}).get("enabled", False):
        _refresh_wrds_extracts(payload)

    run_config = load_sue_historical_panel_run_config(args.config)
    missing = missing_full_mode_inputs(run_config.panel_config)
    if missing:
        artifacts = write_sue_historical_missing_inputs_artifacts(
            run_config.panel_config,
            output_dir=run_config.output_dir,
            report_path=run_config.report_path,
        )
        print("status=unavailable")
        print(f"missing_inputs={len(missing)}")
        print(f"report={artifacts['report']}")
        return

    result = build_sue_historical_event_panel(run_config.panel_config)
    rescue = build_sue_historical_coverage_rescue_report(result)
    artifacts = write_sue_historical_expansion_artifacts(
        result,
        output_dir=run_config.output_dir,
        report_path=run_config.report_path,
    )
    print("status=completed")
    print(f"event_count={result.event_count}")
    print(f"rebalance_date_count={result.rebalance_date_count}")
    print(f"unlinked_ibes_crsp_rows={rescue['unlinked_ibes_crsp_rows']}")
    print(f"missing_expected_eps={rescue['missing_expected_eps']}")
    print(f"missing_actual_eps={rescue['missing_actual_eps']}")
    print(f"missing_price_rows={rescue['missing_price_rows']}")
    print(f"missing_return_windows={rescue['missing_return_windows']}")
    print(f"diagnostic_only_rows={rescue['diagnostic_only_rows']}")
    print(f"final_pit_safe_rows={rescue['pit_safe_rows']}")
    print("production_approval_claimed=False")
    print(f"events={artifacts['events']}")
    print(f"coverage_rescue_report={artifacts['coverage_rescue_report']}")
    print(f"report={artifacts['report']}")


def _refresh_wrds_extracts(payload: dict[str, Any]) -> None:
    inputs = payload.get("inputs") or {}
    extract = payload.get("wrds_extract") or {}
    actuals_path = Path(inputs["earnings_events_path"])
    estimates_path = Path(inputs["estimate_snapshots_path"])
    overwrite = bool(extract.get("overwrite", False))
    if actuals_path.exists() and estimates_path.exists() and not overwrite:
        return

    import wrds

    actuals_path.parent.mkdir(parents=True, exist_ok=True)
    estimates_path.parent.mkdir(parents=True, exist_ok=True)
    connection = wrds.Connection()
    try:
        query_strategy = str((payload.get("wrds_extract") or {}).get("query_strategy", "date_range"))
        if query_strategy == "statsum_latest":
            actuals, estimates = _query_statsum_latest_actuals_and_estimates(connection, payload)
        else:
            actuals = _query_actuals(connection, payload)
            estimates = _query_estimates(connection, payload, actuals)
        actuals.to_csv(actuals_path, index=False)
        estimates.to_csv(estimates_path, index=False)
    finally:
        connection.close()


def _query_actuals(connection: Any, payload: dict[str, Any]) -> pd.DataFrame:
    extract = payload.get("wrds_extract") or {}
    table = str(extract.get("actuals_source_table", "ibes.actu_epsus"))
    start_date = str(payload["start_date"])
    end_date = str(payload["end_date"])
    max_events = int(payload.get("max_events", 20000))
    query = f"""
select ticker, oftic, cusip, pends, anndats, anntims, value
from {table}
where measure = 'EPS'
  and usfirm = 1
  and anndats between '{start_date}' and '{end_date}'
order by anndats, ticker
limit {max_events}
"""
    raw = connection.raw_sql(query)
    if raw.empty:
        return pd.DataFrame(
            columns=[
                "ibes_ticker",
                "symbol",
                "cusip",
                "fiscal_period",
                "announcement_date",
                "event_available_timestamp",
                "actual_eps",
            ]
        )
    frame = raw.copy()
    frame["ibes_ticker"] = frame["ticker"].astype(str).str.upper()
    frame["symbol"] = frame["oftic"].fillna(frame["ticker"]).astype(str).str.upper()
    frame["cusip"] = frame["cusip"].astype(str)
    frame["fiscal_period"] = pd.to_datetime(frame["pends"], errors="coerce").dt.date.astype(str)
    frame["announcement_date"] = pd.to_datetime(frame["anndats"], errors="raise").dt.date.astype(str)
    frame["event_available_timestamp"] = [
        _event_timestamp(date_value, time_value) for date_value, time_value in zip(frame["anndats"], frame["anntims"])
    ]
    frame["actual_eps"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.loc[
        :,
        [
            "ibes_ticker",
            "symbol",
            "cusip",
            "fiscal_period",
            "announcement_date",
            "event_available_timestamp",
            "actual_eps",
        ],
    ]


def _query_statsum_latest_actuals_and_estimates(
    connection: Any,
    payload: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    extract = payload.get("wrds_extract") or {}
    table = str(extract.get("estimates_source_table", "ibes.statsum_epsus"))
    start_date = str(payload["start_date"])
    end_date = str(payload["end_date"])
    max_events = int(payload.get("max_events", 30000))
    query = f"""
select *
from (
  select ticker, oftic, cusip, fpedats, statpers, coalesce(medest, meanest) as expected_eps,
         fiscalp, fpi, curcode, numest, medest, actual, anndats_act, anntims_act,
         row_number() over (
           partition by ticker, fpedats, anndats_act
           order by statpers desc
         ) as rn
  from {table}
  where measure = 'EPS'
    and usfirm = 1
    and anndats_act between '{start_date}' and '{end_date}'
    and statpers <= anndats_act
    and coalesce(medest, meanest) is not null
    and actual is not null
) latest
where rn = 1
order by anndats_act, ticker
limit {max_events}
"""
    raw = connection.raw_sql(query)
    if raw.empty:
        return (
            pd.DataFrame(
                columns=[
                    "ibes_ticker",
                    "symbol",
                    "cusip",
                    "fiscal_period",
                    "announcement_date",
                    "event_available_timestamp",
                    "actual_eps",
                ]
            ),
            _empty_estimates_frame(),
        )
    frame = raw.copy()
    frame["ibes_ticker"] = frame["ticker"].astype(str).str.upper()
    frame["symbol"] = frame["oftic"].fillna(frame["ticker"]).astype(str).str.upper()
    frame["cusip"] = frame["cusip"].astype(str)
    frame["fiscal_period"] = pd.to_datetime(frame["fpedats"], errors="coerce").dt.date.astype(str)
    frame["announcement_date"] = pd.to_datetime(frame["anndats_act"], errors="raise").dt.date.astype(str)
    frame["event_available_timestamp"] = [
        _event_timestamp(date_value, time_value)
        for date_value, time_value in zip(frame["anndats_act"], frame["anntims_act"])
    ]
    frame["actual_eps"] = pd.to_numeric(frame["actual"], errors="coerce")
    actuals = frame.loc[
        :,
        [
            "ibes_ticker",
            "symbol",
            "cusip",
            "fiscal_period",
            "announcement_date",
            "event_available_timestamp",
            "actual_eps",
        ],
    ]
    estimates = frame.loc[
        :,
        [
            "ibes_ticker",
            "cusip",
            "fiscal_period",
            "statpers",
            "expected_eps",
            "fiscalp",
            "fpi",
            "curcode",
            "numest",
            "medest",
        ],
    ].rename(columns={"statpers": "estimate_snapshot_date"})
    estimates["estimate_snapshot_date"] = pd.to_datetime(estimates["estimate_snapshot_date"], errors="raise").dt.date.astype(str)
    estimates["expected_eps"] = pd.to_numeric(estimates["expected_eps"], errors="coerce")
    return actuals.drop_duplicates(), estimates.drop_duplicates()


def _query_estimates(connection: Any, payload: dict[str, Any], actuals: pd.DataFrame) -> pd.DataFrame:
    extract = payload.get("wrds_extract") or {}
    table = str(extract.get("estimates_source_table", "ibes.statsum_epsus"))
    start_date = (
        pd.Timestamp(str(payload["start_date"])) - timedelta(days=int(extract.get("estimate_lookback_days", 730)))
    ).date()
    end_date = str(payload["end_date"])
    forward_end = (
        pd.Timestamp(str(payload["end_date"])) + timedelta(days=int(extract.get("estimate_forward_days", 365)))
    ).date()
    query_strategy = str(extract.get("query_strategy", "date_range"))
    if query_strategy == "target_pairs":
        return _query_estimates_for_target_pairs(
            connection=connection,
            table=table,
            actuals=actuals,
            start_date=start_date,
            end_date=end_date,
            chunk_size=int(extract.get("target_pair_chunk_size", 500)),
        )
    if query_strategy == "date_range":
        query = f"""
select ticker, cusip, fpedats, statpers, coalesce(medest, meanest) as expected_eps,
       fiscalp, fpi, curcode, numest, medest
from {table}
where measure = 'EPS'
  and usfirm = 1
  and statpers between '{start_date}' and '{end_date}'
  and fpedats between '{start_date}' and '{forward_end}'
"""
        frame = connection.raw_sql(query)
        return _normalize_estimates(frame)

    chunk_size = int(extract.get("ticker_chunk_size", 250))
    tickers = sorted(set(actuals["ibes_ticker"].dropna().astype(str).str.upper()))
    frames: list[pd.DataFrame] = []
    for chunk in _chunks(tickers, chunk_size):
        ticker_sql = ",".join("'" + ticker.replace("'", "''") + "'" for ticker in chunk)
        query = f"""
select ticker, cusip, fpedats, statpers, coalesce(medest, meanest) as expected_eps,
       fiscalp, fpi, curcode, numest, medest
from {table}
where measure = 'EPS'
  and usfirm = 1
  and ticker in ({ticker_sql})
  and statpers between '{start_date}' and '{end_date}'
"""
        frame = connection.raw_sql(query)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return _empty_estimates_frame()
    return _normalize_estimates(pd.concat(frames, ignore_index=True))


def _query_estimates_for_target_pairs(
    *,
    connection: Any,
    table: str,
    actuals: pd.DataFrame,
    start_date: Any,
    end_date: Any,
    chunk_size: int,
) -> pd.DataFrame:
    targets = (
        actuals.loc[:, ["ibes_ticker", "fiscal_period"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["ibes_ticker", "fiscal_period"])
        .to_dict(orient="records")
    )
    frames: list[pd.DataFrame] = []
    for chunk in _chunks(targets, chunk_size):
        values = ",".join(
            "('" + str(item["ibes_ticker"]).replace("'", "''") + "','" + str(item["fiscal_period"]) + "'::date)"
            for item in chunk
        )
        query = f"""
with targets(ticker, fpedats) as (values {values})
select s.ticker, s.cusip, s.fpedats, s.statpers, coalesce(s.medest, s.meanest) as expected_eps,
       s.fiscalp, s.fpi, s.curcode, s.numest, s.medest
from {table} s
join targets t
  on s.ticker = t.ticker
 and s.fpedats = t.fpedats
where s.measure = 'EPS'
  and s.usfirm = 1
  and s.statpers between '{start_date}' and '{end_date}'
"""
        frame = connection.raw_sql(query)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return _empty_estimates_frame()
    return _normalize_estimates(pd.concat(frames, ignore_index=True))


def _normalize_estimates(estimates: pd.DataFrame) -> pd.DataFrame:
    if estimates.empty:
        return _empty_estimates_frame()
    estimates["ibes_ticker"] = estimates["ticker"].astype(str).str.upper()
    estimates["cusip"] = estimates["cusip"].astype(str)
    estimates["fiscal_period"] = pd.to_datetime(estimates["fpedats"], errors="coerce").dt.date.astype(str)
    estimates["estimate_snapshot_date"] = pd.to_datetime(estimates["statpers"], errors="coerce").dt.date.astype(str)
    estimates["expected_eps"] = pd.to_numeric(estimates["expected_eps"], errors="coerce")
    return estimates.loc[
        :,
        [
            "ibes_ticker",
            "cusip",
            "fiscal_period",
            "estimate_snapshot_date",
            "expected_eps",
            "fiscalp",
            "fpi",
            "curcode",
            "numest",
            "medest",
        ],
    ].dropna(subset=["fiscal_period", "estimate_snapshot_date"]).drop_duplicates()


def _empty_estimates_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ibes_ticker",
            "cusip",
            "fiscal_period",
            "estimate_snapshot_date",
            "expected_eps",
            "fiscalp",
            "fpi",
            "curcode",
            "numest",
            "medest",
        ]
    )


def _event_timestamp(date_value: Any, time_value: Any) -> str:
    date_part = pd.Timestamp(date_value).date()
    if pd.isna(time_value):
        time_part = time(21, 15)
    elif isinstance(time_value, time):
        time_part = time_value
    else:
        parsed = pd.to_datetime(str(time_value), errors="coerce")
        time_part = parsed.time() if not pd.isna(parsed) else time(21, 15)
    return datetime.combine(date_part, time_part, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _chunks(values: list[str], chunk_size: int) -> list[list[str]]:
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


if __name__ == "__main__":
    main()
