from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from portfolio_os.alpha.discovery_calibration import (
    build_baseline_residualized_expression_summary,
    build_bootstrap_expression_rankings,
    build_calibration_signal_frame,
    build_expression_spread_correlation_matrix,
    build_shuffled_null_distribution,
    build_us_residual_momentum_calibration_registry,
    run_us_residual_momentum_calibration_from_files,
)
from portfolio_os.alpha.qualification import build_monthly_forward_return_frame
from portfolio_os.alpha.qualification import build_family_a_monthly_signal_frame
from portfolio_os.alpha.research import load_alpha_returns_panel


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    tickers = ["TALP", "THIV", "TMID", "TLAG", "HALP", "HHIV", "HMID", "HLAG"]
    sector_map = {
        "TALP": "Technology",
        "THIV": "Technology",
        "TMID": "Technology",
        "TLAG": "Technology",
        "HALP": "Health Care",
        "HHIV": "Health Care",
        "HMID": "Health Care",
        "HLAG": "Health Care",
    }
    drift_map = {
        "TALP": 0.0012,
        "THIV": 0.0010,
        "TMID": 0.0003,
        "TLAG": -0.0008,
        "HALP": 0.0011,
        "HHIV": 0.0010,
        "HMID": 0.0002,
        "HLAG": -0.0009,
    }
    vol_map = {
        "TALP": 0.0002,
        "THIV": 0.0030,
        "TMID": 0.0006,
        "TLAG": 0.0007,
        "HALP": 0.0002,
        "HHIV": 0.0031,
        "HMID": 0.0006,
        "HLAG": 0.0007,
    }

    dates = pd.bdate_range("2025-01-02", periods=320)
    market_component = 0.00015 + 0.00045 * pd.Series(range(len(dates)), index=dates).map(
        lambda idx: __import__("math").sin(float(idx) / 30.0)
    ).to_numpy()
    secondary_wave = pd.Series(range(len(dates)), index=dates).map(lambda idx: __import__("math").sin(float(idx) / 11.0)).to_numpy()

    returns_rows: list[dict[str, object]] = []
    terminal_prices: dict[str, float] = {}
    for ticker in tickers:
        series = market_component + drift_map[ticker] + vol_map[ticker] * secondary_wave
        series = series.clip(-0.08, 0.08)
        price_series = 100.0 * (1.0 + pd.Series(series)).cumprod()
        terminal_prices[ticker] = float(price_series.iloc[-1])
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
                "avg_adv_20d": float(1_000_000 + idx * 25_000),
                "close_2026_03_27": terminal_prices[ticker],
            }
        )
    reference_path = tmp_path / "us_universe_reference.csv"
    pd.DataFrame(reference_rows).to_csv(reference_path, index=False)
    return returns_path, reference_path


def test_calibration_registry_contains_three_expressions_and_three_controls() -> None:
    registry = build_us_residual_momentum_calibration_registry()
    ids = [item.expression_id for item in registry]

    assert ids == [
        "RM1_MKT_RESIDUAL",
        "RM2_SECTOR_RESIDUAL",
        "RM3_VOL_MANAGED",
        "CTRL1_SHUFFLED_PLACEBO",
        "CTRL2_PRE_WINDOW_PLACEBO",
        "CTRL3_BASELINE_MIMIC",
    ]
    assert sum(item.role == "expression" for item in registry) == 3
    assert sum(item.role == "control" for item in registry) == 3


def test_calibration_controls_preserve_expected_shape(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)

    base = build_family_a_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        candidate_id="A1",
    )
    shuffled = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        expression_id="CTRL1_SHUFFLED_PLACEBO",
        random_seed=11,
    )
    shifted = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        expression_id="CTRL2_PRE_WINDOW_PLACEBO",
        random_seed=11,
    )
    baseline = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        expression_id="CTRL3_BASELINE_MIMIC",
        random_seed=11,
    )

    assert len(shuffled) == len(base)
    latest_date = str(base["date"].max())
    base_sorted = sorted(base.loc[base["date"] == latest_date, "signal_value"].tolist())
    shuffled_sorted = sorted(shuffled.loc[shuffled["date"] == latest_date, "signal_value"].tolist())
    assert shuffled_sorted == pytest.approx(base_sorted)

    assert shifted["date"].nunique() == base["date"].nunique() - 1
    assert shifted["date"].min() > base["date"].min()

    baseline_latest = baseline.loc[baseline["date"] == latest_date].set_index("ticker")
    base_latest = base.loc[base["date"] == latest_date].set_index("ticker")
    assert float(baseline_latest.loc["TALP", "signal_value"]) != pytest.approx(
        float(base_latest.loc["TALP", "signal_value"])
    )


def test_run_us_residual_momentum_calibration_writes_expected_artifacts(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    output_dir = tmp_path / "calibration_run"

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=output_dir,
        random_seed=7,
    )

    assert (output_dir / "registry.csv").exists()
    assert (output_dir / "per_date_metrics.csv").exists()
    assert (output_dir / "summary.csv").exists()
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "note.md").exists()

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["family_id"] == "US_RESIDUAL_MOMENTUM_CALIBRATION"
    assert summary["expression_count"] == 3
    assert summary["control_count"] == 3

    assert result.summary_frame["expression_id"].nunique() == 6
    assert "calibration family" in result.note_markdown.lower()


def test_build_shuffled_null_distribution_writes_one_row_per_seed(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)

    null_frame = build_shuffled_null_distribution(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
        random_seeds=[0, 1, 2, 3],
    )

    assert list(null_frame["seed"]) == [0, 1, 2, 3]
    assert (null_frame["expression_id"] == "CTRL1_SHUFFLED_PLACEBO").all()
    assert null_frame["mean_rank_ic"].notna().all()
    assert null_frame["rank_ic_t"].notna().all()


def test_run_us_residual_momentum_calibration_adds_shuffle_null_percentiles(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    output_dir = tmp_path / "calibration_run"

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=output_dir,
        random_seed=7,
    )

    assert (output_dir / "shuffle_null_distribution.csv").exists()
    assert "shuffle_null_mean_rank_ic_percentile" in result.summary_frame.columns
    assert "shuffle_null_rank_ic_t_percentile" in result.summary_frame.columns

    expression_rows = result.summary_frame.loc[result.summary_frame["role"] == "expression"]
    assert expression_rows["shuffle_null_mean_rank_ic_percentile"].between(0.0, 1.0).all()
    assert expression_rows["shuffle_null_rank_ic_t_percentile"].between(0.0, 1.0).all()


def test_build_bootstrap_expression_rankings_returns_one_row_per_iteration_per_expression(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=reference_frame,
    )

    per_date_frames = []
    expression_ids = ["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"]
    for expression_id in expression_ids:
        signal_frame = build_calibration_signal_frame(
            returns_panel=returns_panel,
            universe_reference=reference_frame,
            expression_id=expression_id,
            random_seed=7,
        )
        merged = signal_frame.merge(
            forward_return_frame.loc[:, ["date", "ticker", "forward_return"]],
            on=["date", "ticker"],
            how="inner",
        )
        rows = []
        for date_value, date_frame in merged.groupby("date", sort=True):
            rows.append(
                {
                    "expression_id": expression_id,
                    "date": str(date_value),
                    "rank_ic": float(date_frame["signal_value"].corr(date_frame["forward_return"], method="spearman")),
                    "top_bottom_spread": float(date_frame["forward_return"].mean()),
                }
            )
        per_date_frames.append(pd.DataFrame(rows))

    per_date_frame = pd.concat(per_date_frames, ignore_index=True)
    bootstrap_frame = build_bootstrap_expression_rankings(
        per_date_frame=per_date_frame,
        expression_ids=expression_ids,
        bootstrap_iterations=5,
        random_seed=11,
    )

    assert len(bootstrap_frame) == 15
    assert set(bootstrap_frame["expression_id"]) == set(expression_ids)
    assert bootstrap_frame["bootstrap_id"].nunique() == 5
    assert {"mean_rank_ic", "mean_top_bottom_spread", "rank_by_rank_ic"}.issubset(bootstrap_frame.columns)


def test_build_expression_spread_correlation_matrix_returns_square_expression_matrix(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    returns_panel = load_alpha_returns_panel(returns_path)
    reference_frame = pd.read_csv(reference_path)

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=tmp_path / "calibration_run",
        random_seed=7,
    )

    expression_ids = ["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"]
    matrix = build_expression_spread_correlation_matrix(
        per_date_frame=result.per_date_frame,
        expression_ids=expression_ids,
    )

    assert list(matrix.index) == expression_ids
    assert list(matrix.columns) == expression_ids
    assert np.allclose(np.diag(matrix.to_numpy(dtype=float)), 1.0)


def test_run_us_residual_momentum_calibration_writes_bootstrap_and_orthogonality_artifacts(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    output_dir = tmp_path / "calibration_run"

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=output_dir,
        random_seed=7,
    )

    assert (output_dir / "bootstrap_expression_rankings.csv").exists()
    assert (output_dir / "expression_spread_correlation.csv").exists()
    expression_rows = result.summary_frame.loc[result.summary_frame["role"] == "expression"]
    assert "bootstrap_top1_frequency_rank_ic" in expression_rows.columns
    assert expression_rows["bootstrap_top1_frequency_rank_ic"].between(0.0, 1.0).all()


def test_build_baseline_residualized_summary_returns_expression_rows_only(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    residualized = build_baseline_residualized_expression_summary(
        returns_panel=load_alpha_returns_panel(returns_path),
        universe_reference=pd.read_csv(reference_path),
        expression_ids=["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"],
    )

    assert list(residualized["expression_id"]) == ["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"]
    assert {
        "residualized_evaluation_month_count",
        "residualized_mean_rank_ic",
        "residualized_rank_ic_t",
        "residualized_mean_top_bottom_spread",
    }.issubset(residualized.columns)


def test_run_us_residual_momentum_calibration_writes_residualized_summary_artifact(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    output_dir = tmp_path / "calibration_run"

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=output_dir,
        random_seed=7,
    )

    assert (output_dir / "residualized_vs_baseline_summary.csv").exists()
    expression_rows = result.summary_frame.loc[result.summary_frame["role"] == "expression"]
    assert "baseline_residualized_rank_ic_t" in expression_rows.columns
    assert expression_rows["baseline_residualized_rank_ic_t"].notna().all()
