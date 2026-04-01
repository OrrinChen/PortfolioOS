from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from portfolio_os.api.cli import alpha_research_app
from portfolio_os.alpha.research import run_alpha_research


def _write_returns_fixture(tmp_path: Path) -> Path:
    dates = pd.date_range("2026-01-02", periods=10, freq="B")
    returns_by_ticker = {
        "AAA": [0.03, 0.02, 0.02, 0.01, 0.02, 0.01, 0.02, 0.01, 0.02, 0.01],
        "BBB": [-0.03, -0.02, -0.02, -0.01, -0.02, -0.01, -0.02, -0.01, -0.02, -0.01],
        "CCC": [0.01, 0.01, 0.00, 0.01, 0.00, 0.01, 0.00, 0.01, 0.00, 0.01],
        "DDD": [-0.01, -0.01, 0.00, -0.01, 0.00, -0.01, 0.00, -0.01, 0.00, -0.01],
    }
    rows: list[dict[str, object]] = []
    for ticker, returns in returns_by_ticker.items():
        for date_value, return_value in zip(dates, returns, strict=True):
            rows.append(
                {
                    "date": date_value.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "return": return_value,
                }
            )
    returns_path = tmp_path / "returns_long.csv"
    pd.DataFrame(rows).to_csv(returns_path, index=False)
    return returns_path


def test_run_alpha_research_builds_signal_panel_with_expected_columns(tmp_path: Path) -> None:
    returns_path = _write_returns_fixture(tmp_path)
    output_dir = tmp_path / "alpha_output"

    result = run_alpha_research(
        returns_file=returns_path,
        output_dir=output_dir,
        reversal_lookback_days=2,
        momentum_lookback_days=3,
        momentum_skip_days=1,
        forward_horizon_days=2,
        reversal_weight=0.0,
        momentum_weight=1.0,
        min_assets_per_date=4,
        quantiles=2,
    )

    assert {
        "date",
        "ticker",
        "reversal_raw",
        "momentum_raw",
        "reversal_rank",
        "momentum_rank",
        "alpha_score",
        "forward_return",
    } <= set(result.signal_frame.columns)
    assert list(result.signal_frame["date"]) == sorted(result.signal_frame["date"].tolist())
    assert result.signal_frame["alpha_score"].notna().all()
    assert result.signal_frame["forward_return"].notna().all()


def test_run_alpha_research_assigns_higher_alpha_to_stronger_momentum_name(tmp_path: Path) -> None:
    returns_path = _write_returns_fixture(tmp_path)
    output_dir = tmp_path / "alpha_output"

    result = run_alpha_research(
        returns_file=returns_path,
        output_dir=output_dir,
        reversal_lookback_days=2,
        momentum_lookback_days=3,
        momentum_skip_days=1,
        forward_horizon_days=2,
        reversal_weight=0.0,
        momentum_weight=1.0,
        min_assets_per_date=4,
        quantiles=2,
    )

    latest_date = result.signal_frame["date"].max()
    latest_rows = result.signal_frame.loc[result.signal_frame["date"] == latest_date].set_index("ticker")

    assert float(latest_rows.loc["AAA", "alpha_score"]) > float(latest_rows.loc["BBB", "alpha_score"])
    assert float(latest_rows.loc["CCC", "alpha_score"]) > float(latest_rows.loc["DDD", "alpha_score"])


def test_run_alpha_research_reports_positive_rank_ic_on_trending_fixture(tmp_path: Path) -> None:
    returns_path = _write_returns_fixture(tmp_path)
    output_dir = tmp_path / "alpha_output"

    result = run_alpha_research(
        returns_file=returns_path,
        output_dir=output_dir,
        reversal_lookback_days=2,
        momentum_lookback_days=3,
        momentum_skip_days=1,
        forward_horizon_days=2,
        reversal_weight=0.0,
        momentum_weight=1.0,
        min_assets_per_date=4,
        quantiles=2,
    )

    assert result.summary_payload["evaluation_date_count"] > 0
    assert result.summary_payload["mean_rank_ic"] > 0.0
    assert result.summary_payload["mean_top_bottom_spread"] > 0.0
    assert "rank_ic" in result.ic_frame.columns


def test_run_alpha_research_writes_expected_artifacts(tmp_path: Path) -> None:
    returns_path = _write_returns_fixture(tmp_path)
    output_dir = tmp_path / "alpha_output"

    run_alpha_research(
        returns_file=returns_path,
        output_dir=output_dir,
        reversal_lookback_days=2,
        momentum_lookback_days=3,
        momentum_skip_days=1,
        forward_horizon_days=2,
        reversal_weight=0.0,
        momentum_weight=1.0,
        min_assets_per_date=4,
        quantiles=2,
    )

    assert (output_dir / "alpha_signal_panel.csv").exists()
    assert (output_dir / "alpha_ic_by_date.csv").exists()
    assert (output_dir / "alpha_research_summary.json").exists()
    assert (output_dir / "alpha_research_report.md").exists()

    summary_payload = json.loads((output_dir / "alpha_research_summary.json").read_text(encoding="utf-8"))
    report_text = (output_dir / "alpha_research_report.md").read_text(encoding="utf-8")

    assert summary_payload["ticker_count"] == 4
    assert "# Alpha Research Report" in report_text


def test_alpha_research_cli_writes_outputs(tmp_path: Path) -> None:
    returns_path = _write_returns_fixture(tmp_path)
    output_dir = tmp_path / "alpha_cli_output"
    runner = CliRunner()

    result = runner.invoke(
        alpha_research_app,
        [
            "--returns-file",
            str(returns_path),
            "--output-dir",
            str(output_dir),
            "--reversal-lookback-days",
            "2",
            "--momentum-lookback-days",
            "3",
            "--momentum-skip-days",
            "1",
            "--forward-horizon-days",
            "2",
            "--reversal-weight",
            "0.0",
            "--momentum-weight",
            "1.0",
            "--min-assets-per-date",
            "4",
            "--quantiles",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "alpha_signal_panel.csv").exists()
    assert (output_dir / "alpha_ic_by_date.csv").exists()
    assert (output_dir / "alpha_research_summary.json").exists()
    assert (output_dir / "alpha_research_report.md").exists()
