"""Run Q1-INSIDER-01A label coverage rescue for insider P-code buying."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_q1_label_rescue import (
    run_open_market_buying_q1_label_coverage_rescue,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIGNAL_PANEL = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d3_open_market_buying_v0"
    / "signal_panel.csv"
)
DEFAULT_BASELINE_PRICE_PANEL = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "market_cache"
    / "insider_replay_market_subset_all_archive_symbols.csv"
)
DEFAULT_BENCHMARK_PANEL = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "nasdaq100_daily"
    / "standardized"
    / "qqq_benchmark_panel.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "q1_label_coverage_rescue"
)
DEFAULT_LOCAL_RESCUE_PRICE_PANELS = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "nasdaq100_daily"
    / "standardized"
    / "adjusted_price_volume_panel.csv",
    DEFAULT_OUTPUT_DIR / "wrds_dsf_v2_label_rescue_price_panel.csv",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write Q1 insider buying label coverage rescue artifacts.")
    parser.add_argument("--signal-panel", default=str(DEFAULT_SIGNAL_PANEL))
    parser.add_argument("--baseline-price-panel", default=str(DEFAULT_BASELINE_PRICE_PANEL))
    parser.add_argument("--benchmark-panel", default=str(DEFAULT_BENCHMARK_PANEL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument(
        "--extra-price-panel",
        action="append",
        default=[],
        help="Additional label-only price panel to merge before rerunning Q1. Can be passed multiple times.",
    )
    parser.add_argument(
        "--no-default-local-price-caches",
        action="store_true",
        help="Do not include known local standardized price caches as rescue inputs.",
    )
    parser.add_argument(
        "--allow-wrds",
        action="store_true",
        help="Fetch label-only daily prices from WRDS CRSP dsf_v2 for active D3 tickers.",
    )
    parser.add_argument("--wrds-username", default=None)
    parser.add_argument("--wrds-chunk-size", type=int, default=50)
    args = parser.parse_args()

    extra_paths = [Path(path) for path in args.extra_price_panel]
    if not args.no_default_local_price_caches:
        extra_paths.extend(path for path in DEFAULT_LOCAL_RESCUE_PRICE_PANELS if path.exists())
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.allow_wrds:
        extra_paths.append(
            _write_wrds_dsf_v2_label_rescue_panel(
                signal_panel_path=Path(args.signal_panel),
                output_dir=output_dir,
                wrds_username=args.wrds_username,
                chunk_size=args.wrds_chunk_size,
            ),
        )

    result = run_open_market_buying_q1_label_coverage_rescue(
        signal_panel_path=args.signal_panel,
        baseline_price_panel_path=args.baseline_price_panel,
        benchmark_panel_path=args.benchmark_panel,
        output_dir=args.output_dir,
        extra_price_panel_paths=extra_paths,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"rescued_q1_decision={result.summary['rescued_q1_decision']}")
    print(f"rescued_q1_result_interpretation={result.summary['rescued_q1_result_interpretation']}")
    print(f"rescued_active_event_clusters={result.summary['rescued_active_event_clusters']}")
    print(f"rescued_observed_primary_label_clusters={result.summary['rescued_observed_primary_label_clusters']}")
    print(f"rescued_observed_event_month_count={result.summary['rescued_observed_event_month_count']}")
    print(f"rescued_label_coverage_share={result.summary['rescued_label_coverage_share']}")
    print(f"signal_panel_hash_unchanged={str(result.summary['signal_panel_hash_unchanged']).lower()}")
    print(f"measurement_spec_modified={str(result.summary['measurement_spec_modified']).lower()}")
    print(f"formula_modified={str(result.summary['formula_modified']).lower()}")
    print(f"expected_return_panel_written={str(result.summary['expected_return_panel_written']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"optimizer_entry_allowed={str(result.summary['optimizer_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


def _write_wrds_dsf_v2_label_rescue_panel(
    signal_panel_path: Path,
    output_dir: Path,
    wrds_username: str | None,
    chunk_size: int,
) -> Path:
    import wrds

    signal_panel = pd.read_csv(signal_panel_path, usecols=["ticker", "signal_date", "coverage_state"])
    active = signal_panel[signal_panel["coverage_state"].eq("active")].copy()
    if active.empty:
        raise ValueError("cannot fetch WRDS label rescue panel without active D3 signals")
    active["signal_date"] = pd.to_datetime(active["signal_date"], errors="coerce")
    active = active[active["signal_date"].notna()]
    tickers = sorted(active["ticker"].astype(str).str.upper().unique().tolist())
    start_date = (active["signal_date"].min() - pd.Timedelta(days=90)).date()
    requested_end_date = (active["signal_date"].max() + pd.Timedelta(days=90)).date()

    frames: list[pd.DataFrame] = []
    with wrds.Connection(wrds_username=wrds_username) as db:
        max_date_frame = db.raw_sql("select max(dlycaldt) as max_date from crsp.dsf_v2")
        source_max_date = pd.to_datetime(max_date_frame["max_date"].iloc[0]).date()
        end_date = min(requested_end_date, source_max_date)
        for chunk_index, chunk in enumerate(_chunks(tickers, max(1, chunk_size))):
            ticker_sql = ", ".join("'" + ticker.replace("'", "''") + "'" for ticker in chunk)
            query = f"""
                select
                    ticker,
                    dlycaldt as date,
                    abs(dlyprc) as raw_close,
                    abs(dlyprc) as adjusted_close,
                    dlyvol as volume,
                    dlycap as market_cap,
                    dlyprcvol as dollar_volume,
                    case
                        when dlybid > 0 and dlyask > 0 and dlyclose > 0
                        then (dlyask - dlybid) / dlyclose
                        else null
                    end as bid_ask_spread,
                    '' as sector,
                    'wrds_crsp_dsf_v2_label_rescue' as price_source
                from crsp.dsf_v2
                where ticker in ({ticker_sql})
                  and dlycaldt between '{start_date}' and '{end_date}'
                order by ticker, dlycaldt
            """
            frame = db.raw_sql(query)
            frame["wrds_chunk_index"] = chunk_index
            frames.append(frame)

    output_path = output_dir / "wrds_dsf_v2_label_rescue_price_panel.csv"
    panel = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    panel.to_csv(output_path, index=False)
    manifest = {
        "schema_version": "wrds_dsf_v2_label_rescue_manifest.v1",
        "source_table": "crsp.dsf_v2",
        "network_used": True,
        "purpose": "Q1-INSIDER-01A label coverage rescue only",
        "tickers_requested": len(tickers),
        "rows_written": int(len(panel)),
        "start_date": start_date.isoformat(),
        "requested_end_date": requested_end_date.isoformat(),
        "source_max_date": source_max_date.isoformat(),
        "effective_end_date": end_date.isoformat(),
        "signal_panel_path": str(signal_panel_path),
        "forbidden_outputs": {
            "expected_return_panel_written": False,
            "q2_entry_allowed": False,
            "optimizer_entry_allowed": False,
            "alpha_registry_update_allowed": False,
            "production_approval_claimed": False,
        },
    }
    (output_dir / "wrds_dsf_v2_label_rescue_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


if __name__ == "__main__":
    main()
