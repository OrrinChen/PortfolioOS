from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from portfolio_os.alpha.qualification import (
    build_family_a_monthly_signal_frame,
    build_family_b_monthly_signal_frame,
    build_family_c_monthly_signal_frame,
    run_alpha_core_candidate,
)
from portfolio_os.alpha.research import load_alpha_returns_panel


def _write_family_a_fixture(tmp_path: Path) -> tuple[Path, Path]:
    tickers = [
        "TALP",
        "THIV",
        "TMID",
        "TLAG",
        "TDEF",
        "HALP",
        "HHIV",
        "HMID",
        "HLAG",
        "HDEF",
    ]
    sector_map = {
        "TALP": "Technology",
        "THIV": "Technology",
        "TMID": "Technology",
        "TLAG": "Technology",
        "TDEF": "Technology",
        "HALP": "Health Care",
        "HHIV": "Health Care",
        "HMID": "Health Care",
        "HLAG": "Health Care",
        "HDEF": "Health Care",
    }
    drift_map = {
        "TALP": 0.0011,
        "THIV": 0.0010,
        "TMID": 0.0003,
        "TLAG": -0.0008,
        "TDEF": 0.0001,
        "HALP": 0.0010,
        "HHIV": 0.0010,
        "HMID": 0.0002,
        "HLAG": -0.0009,
        "HDEF": 0.0001,
    }
    vol_scale_map = {
        "TALP": 0.0002,
        "THIV": 0.0032,
        "TMID": 0.0006,
        "TLAG": 0.0007,
        "TDEF": 0.0003,
        "HALP": 0.0002,
        "HHIV": 0.0030,
        "HMID": 0.0006,
        "HLAG": 0.0007,
        "HDEF": 0.0003,
    }

    dates = pd.bdate_range("2025-01-02", periods=320)
    market_component = 0.00015 + 0.00045 * np.sin(np.linspace(0.0, 10.0, len(dates)))
    secondary_wave = np.sin(np.linspace(0.0, 25.0, len(dates)))

    returns_rows: list[dict[str, object]] = []
    terminal_prices: dict[str, float] = {}
    for position, ticker in enumerate(tickers, start=1):
        series = market_component + drift_map[ticker] + vol_scale_map[ticker] * secondary_wave
        series = np.clip(series, -0.08, 0.08)
        price_series = 100.0 * np.cumprod(1.0 + series)
        terminal_prices[ticker] = float(price_series[-1])
        for date_value, return_value in zip(dates, series, strict=True):
            returns_rows.append(
                {
                    "date": date_value.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "return": float(return_value),
                }
            )

    returns_path = tmp_path / "returns_long.csv"
    pd.DataFrame(returns_rows).to_csv(returns_path, index=False)

    reference_rows = []
    for idx, ticker in enumerate(tickers, start=1):
        reference_rows.append(
            {
                "ticker": ticker,
                "sector": sector_map[ticker],
                "avg_adv_20d": float(1_000_000 + idx * 50_000),
                "close_2026_03_27": terminal_prices[ticker],
            }
        )
    reference_path = tmp_path / "us_universe_reference.csv"
    pd.DataFrame(reference_rows).to_csv(reference_path, index=False)
    return returns_path, reference_path


def _write_family_b_liquidity_fixture(tmp_path: Path, returns_path: Path) -> Path:
    returns_frame = pd.read_csv(returns_path)
    returns_frame["date"] = pd.to_datetime(returns_frame["date"])
    last_date = returns_frame["date"].max()
    shock_start = last_date - pd.Timedelta(days=35)
    spike_start = last_date - pd.Timedelta(days=7)

    base_dollar_volume = {
        "TALP": 25_000_000.0,
        "THIV": 1_200_000.0,
        "TMID": 8_000_000.0,
        "TLAG": 6_000_000.0,
        "TDEF": 10_000_000.0,
        "HALP": 24_000_000.0,
        "HHIV": 1_100_000.0,
        "HMID": 8_500_000.0,
        "HLAG": 6_500_000.0,
        "HDEF": 10_500_000.0,
    }

    rows: list[dict[str, object]] = []
    for row in returns_frame.itertuples(index=False):
        date_value = pd.Timestamp(row.date)
        ticker = str(row.ticker)
        dollar_volume = float(base_dollar_volume[ticker])
        if ticker == "TMID" and date_value >= shock_start:
            dollar_volume *= 3.0
        if ticker == "THIV" and date_value >= shock_start:
            dollar_volume *= 0.35
        if ticker == "TLAG" and date_value >= spike_start:
            dollar_volume *= 5.0
        rows.append(
            {
                "date": date_value.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "dollar_volume": dollar_volume,
            }
        )
    liquidity_path = tmp_path / "liquidity_long.csv"
    pd.DataFrame(rows).to_csv(liquidity_path, index=False)
    return liquidity_path


def test_build_family_a_monthly_signal_frame_preserves_residual_momentum_direction(tmp_path: Path) -> None:
    returns_path, reference_path = _write_family_a_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)

    a1_frame = build_family_a_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="A1",
    )
    a2_frame = build_family_a_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="A2",
    )
    a3_frame = build_family_a_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="A3",
    )

    latest_date = a1_frame["date"].max()
    a1_latest = a1_frame.loc[a1_frame["date"] == latest_date].set_index("ticker")
    a2_latest = a2_frame.loc[a2_frame["date"] == latest_date].set_index("ticker")
    a3_latest = a3_frame.loc[a3_frame["date"] == latest_date].set_index("ticker")

    assert float(a1_latest.loc["TALP", "signal_value"]) > float(a1_latest.loc["TLAG", "signal_value"])
    assert float(a2_latest.loc["TALP", "signal_value"]) > float(a2_latest.loc["TLAG", "signal_value"])
    assert float(a3_latest.loc["TALP", "signal_value"]) > float(a3_latest.loc["THIV", "signal_value"])


def test_build_family_c_monthly_signal_frame_preserves_low_risk_direction(tmp_path: Path) -> None:
    returns_path, reference_path = _write_family_a_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)

    c1_frame = build_family_c_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="C1",
    )
    c2_frame = build_family_c_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="C2",
    )

    latest_date = c1_frame["date"].max()
    c1_latest = c1_frame.loc[c1_frame["date"] == latest_date].set_index("ticker")
    c2_latest = c2_frame.loc[c2_frame["date"] == latest_date].set_index("ticker")

    assert float(c1_latest.loc["TALP", "signal_value"]) > float(c1_latest.loc["THIV", "signal_value"])
    assert float(c2_latest.loc["TLAG", "signal_value"]) > float(c2_latest.loc["TALP", "signal_value"])


def test_build_family_b_monthly_signal_frame_preserves_liquidity_directions(tmp_path: Path) -> None:
    returns_path, reference_path = _write_family_a_fixture(tmp_path)
    liquidity_path = _write_family_b_liquidity_fixture(tmp_path, returns_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)
    liquidity_frame = pd.read_csv(liquidity_path)

    b1_frame = build_family_b_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        liquidity_long=liquidity_frame,
        candidate_id="B1",
    )
    b2_frame = build_family_b_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        liquidity_long=liquidity_frame,
        candidate_id="B2",
    )
    b3_frame = build_family_b_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        liquidity_long=liquidity_frame,
        candidate_id="B3",
    )

    latest_date = b1_frame["date"].max()
    b1_latest = b1_frame.loc[b1_frame["date"] == latest_date].set_index("ticker")
    b2_latest = b2_frame.loc[b2_frame["date"] == latest_date].set_index("ticker")
    b3_latest = b3_frame.loc[b3_frame["date"] == latest_date].set_index("ticker")

    assert float(b1_latest.loc["TALP", "signal_value"]) > float(b1_latest.loc["THIV", "signal_value"])
    assert float(b2_latest.loc["TMID", "signal_value"]) > float(b2_latest.loc["THIV", "signal_value"])
    assert float(b3_latest.loc["TMID", "signal_value"]) > float(b3_latest.loc["TDEF", "signal_value"])


@pytest.mark.parametrize(
    ("candidate_id", "family_id", "uses_liquidity"),
    [
        ("A1", "A", False),
        ("B1", "B", True),
        ("C1", "C", False),
    ],
)
def test_run_alpha_core_candidate_writes_week2_contract_bundle(
    project_root: Path,
    tmp_path: Path,
    candidate_id: str,
    family_id: str,
    uses_liquidity: bool,
) -> None:
    returns_path, reference_path = _write_family_a_fixture(tmp_path)
    liquidity_path = _write_family_b_liquidity_fixture(tmp_path, returns_path)
    output_dir = tmp_path / f"{candidate_id}_run"

    result = run_alpha_core_candidate(
        candidate_id=candidate_id,
        returns_file=returns_path,
        universe_reference_file=reference_path,
        liquidity_file=liquidity_path if uses_liquidity else None,
        config_file=project_root / "config" / "us_expanded_alpha_phase_1_5.yaml",
        output_dir=output_dir,
        as_of_date="2026-04-16",
    )

    expected_files = {
        "summary.json",
        "oos_metrics.csv",
        "coverage_by_month.csv",
        "spread_series.csv",
        "subperiod_metrics.csv",
        "orthogonality_vs_baseline.csv",
        "note.md",
    }
    assert expected_files == {path.name for path in output_dir.iterdir() if path.is_file()}

    summary_payload = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    oos_metrics = pd.read_csv(output_dir / "oos_metrics.csv")
    coverage = pd.read_csv(output_dir / "coverage_by_month.csv")
    spread = pd.read_csv(output_dir / "spread_series.csv")
    subperiod = pd.read_csv(output_dir / "subperiod_metrics.csv")
    orthogonality = pd.read_csv(output_dir / "orthogonality_vs_baseline.csv")
    note_text = (output_dir / "note.md").read_text(encoding="utf-8")

    assert summary_payload["candidate_id"] == candidate_id
    assert summary_payload["family_id"] == family_id
    assert summary_payload["baseline_comparator_id"] == "alt_momentum_4_1"
    assert "coverage_median" in summary_payload
    assert "winner_gate_pass" in summary_payload
    assert "platform_native_sample" in summary_payload["notes"][0]

    assert {
        "candidate_id",
        "coverage_median",
        "gross_to_net_retention",
        "oos_mean_rank_ic",
        "oos_rank_ic_tstat",
        "oos_mean_alpha_only_spread",
        "oos_alpha_only_tstat",
        "baseline_id",
        "baseline_mean_rank_ic",
        "baseline_mean_alpha_only_spread",
        "rank_ic_improvement_vs_baseline",
        "rank_ir_improvement_vs_baseline",
        "alpha_spread_improvement_vs_baseline",
    } <= set(oos_metrics.columns)
    assert {
        "date",
        "candidate_id",
        "eligible_universe_count",
        "raw_signal_count",
        "effective_signal_count",
        "effective_coverage_ratio",
        "effective_coverage_after_liquidity_cut",
    } <= set(coverage.columns)
    assert {
        "date",
        "candidate_id",
        "top_bucket_return",
        "bottom_bucket_return",
        "top_bottom_spread",
        "net_top_bottom_spread",
        "turnover",
        "benchmark_spread",
    } <= set(spread.columns)
    assert len(subperiod) == 3
    assert {
        "candidate_id",
        "baseline_id",
        "spread_corr",
        "rank_ic_improvement_vs_baseline",
        "rank_ir_improvement_vs_baseline",
        "alpha_spread_improvement_vs_baseline",
        "coverage_delta_vs_baseline",
        "retention_delta_vs_baseline",
    } <= set(orthogonality.columns)
    assert "## Candidate" in note_text
    assert "## Gate Decision" in note_text
    assert result.output_dir == output_dir
