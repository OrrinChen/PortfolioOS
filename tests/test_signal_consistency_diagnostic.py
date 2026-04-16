from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.signal_consistency_diagnostic import build_signal_consistency_report


def _cross_section(date_value: str, tickers: list[str], values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date_value] * len(tickers),
            "ticker": tickers,
            "alpha_score": values,
        }
    )


def _production_view(
    date_value: str,
    tickers: list[str],
    alpha_values: list[float],
    expected_values: list[float],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date_value] * len(tickers),
            "ticker": tickers,
            "alpha_score": alpha_values,
            "expected_return": expected_values,
        }
    )


def test_build_signal_consistency_report_supports_ragged_views_and_concat_pooling() -> None:
    tickers = [f"T{i:02d}" for i in range(10)]
    canonical = pd.concat(
        [
            _cross_section("2025-10-31", tickers, [float(value) for value in range(10, 0, -1)]),
            _cross_section("2025-11-28", tickers, [float(value) for value in range(20, 10, -1)]),
        ],
        ignore_index=True,
    )
    baseline = _production_view(
        "2025-10-31",
        tickers,
        [float(value) for value in range(10, 0, -1)],
        [float(value) / 100.0 for value in range(10, 0, -1)],
    )
    signed_spread = pd.concat(
        [
            baseline,
            _production_view(
                "2025-11-28",
                tickers,
                [float(value) for value in range(20, 10, -1)],
                [float(value) / 100.0 for value in range(20, 10, -1)],
            ),
        ],
        ignore_index=True,
    )

    report = build_signal_consistency_report(
        canonical_cross_section=canonical,
        production_views={"baseline": baseline, "signed_spread": signed_spread},
    )

    assert set(report.per_month_frame["production_view"]) == {"baseline", "signed_spread"}
    assert len(report.per_month_frame) == 4

    baseline_missing = report.per_month_frame.loc[
        (report.per_month_frame["production_view"] == "baseline")
        & (report.per_month_frame["date"] == "2025-11-28")
    ].iloc[0]
    assert baseline_missing["production_ticker_count"] == 0
    assert math.isnan(float(baseline_missing["alpha_vs_canonical_spearman"]))

    signed_summary = report.pooled_summary_frame.loc[
        report.pooled_summary_frame["production_view"] == "signed_spread"
    ].iloc[0]
    assert signed_summary["pooled_observation_count"] == 20
    assert signed_summary["pooled_alpha_vs_canonical_spearman"] == pytest.approx(1.0)
    assert signed_summary["pooled_expected_return_vs_canonical_spearman"] == pytest.approx(1.0)
    assert report.metadata["pooled_method"] == "concat_then_correlate"


def test_build_signal_consistency_report_marks_low_overlap_month_as_nan() -> None:
    tickers = [f"T{i:02d}" for i in range(9)]
    canonical = _cross_section("2025-10-31", tickers, [float(value) for value in range(9, 0, -1)])
    baseline = _production_view(
        "2025-10-31",
        tickers,
        [float(value) for value in range(9, 0, -1)],
        [float(value) / 100.0 for value in range(9, 0, -1)],
    )

    report = build_signal_consistency_report(
        canonical_cross_section=canonical,
        production_views={"baseline": baseline, "signed_spread": pd.DataFrame(columns=baseline.columns)},
    )

    row = report.per_month_frame.loc[report.per_month_frame["production_view"] == "baseline"].iloc[0]
    assert row["ticker_overlap_count"] == 9
    assert math.isnan(float(row["alpha_vs_canonical_spearman"]))
    assert row["top_5_overlap"] == pytest.approx(1.0)
    assert math.isnan(float(row["top_10_overlap"]))


def test_build_signal_consistency_report_preserves_custom_metadata() -> None:
    tickers = [f"T{i:02d}" for i in range(10)]
    canonical = _cross_section("2025-10-31", tickers, [float(value) for value in range(10, 0, -1)])
    baseline = _production_view(
        "2025-10-31",
        tickers,
        [float(value) for value in range(10, 0, -1)],
        [float(value) / 100.0 for value in range(10, 0, -1)],
    )

    report = build_signal_consistency_report(
        canonical_cross_section=canonical,
        production_views={"baseline": baseline, "signed_spread": baseline.copy()},
        metadata={"portfolioos_head_sha": "abc123-dirty"},
    )

    assert report.metadata["portfolioos_head_sha"] == "abc123-dirty"
    assert report.metadata["min_overlap_threshold"] == 10


def test_signal_consistency_markdown_calls_out_promoted_month_sign_flip() -> None:
    tickers = [f"T{i:02d}" for i in range(10)]
    canonical = pd.concat(
        [
            _cross_section("2025-10-31", tickers, [float(value) for value in range(10, 0, -1)]),
            _cross_section("2025-11-28", tickers, [float(value) for value in range(20, 10, -1)]),
        ],
        ignore_index=True,
    )
    baseline = _production_view(
        "2025-10-31",
        tickers,
        [float(value) for value in range(10, 0, -1)],
        [float(value) / 100.0 for value in range(10, 0, -1)],
    )
    signed_spread = pd.concat(
        [
            baseline,
            _production_view(
                "2025-11-28",
                tickers,
                [float(value) for value in range(20, 10, -1)],
                [float(value) / -100.0 for value in range(20, 10, -1)],
            ),
        ],
        ignore_index=True,
    )

    report = build_signal_consistency_report(
        canonical_cross_section=canonical,
        production_views={"baseline": baseline, "signed_spread": signed_spread},
    )

    markdown = report.to_markdown()
    assert "promoted floor-zero months" in markdown
    assert "expected-return mapping can flip relative to canonical ordering" in markdown


def test_signal_consistency_runner_writes_expected_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.run_signal_consistency_diagnostic as runner

    tickers = [f"T{i:02d}" for i in range(10)]
    canonical = pd.concat(
        [
            _cross_section("2025-10-31", tickers, [float(value) for value in range(10, 0, -1)]),
            _cross_section("2025-11-28", tickers, [float(value) for value in range(20, 10, -1)]),
        ],
        ignore_index=True,
    )
    baseline = _production_view(
        "2025-10-31",
        tickers,
        [float(value) for value in range(10, 0, -1)],
        [float(value) / 100.0 for value in range(10, 0, -1)],
    )
    signed_spread = pd.concat(
        [
            baseline,
            _production_view(
                "2025-11-28",
                tickers,
                [float(value) for value in range(20, 10, -1)],
                [float(value) / 100.0 for value in range(20, 10, -1)],
            ),
        ],
        ignore_index=True,
    )

    monkeypatch.setattr(
        runner,
        "_build_production_views",
        lambda manifest_path: {"baseline": baseline, "signed_spread": signed_spread},
    )
    monkeypatch.setattr(runner, "_build_canonical_cross_section", lambda: canonical)
    monkeypatch.setattr(
        runner,
        "_git_head_metadata",
        lambda repo_root: {"portfolioos_head_sha": "deadbeef-dirty", "working_tree_clean": False},
    )

    output_dir = tmp_path / "signal_consistency"
    runner.main(["--output-dir", str(output_dir)])

    assert (output_dir / "signal_consistency_per_month.csv").exists()
    assert (output_dir / "signal_consistency_pooled_summary.csv").exists()
    assert (output_dir / "signal_consistency_summary.json").exists()
    assert (output_dir / "signal_consistency_note.md").exists()

    summary = json.loads((output_dir / "signal_consistency_summary.json").read_text(encoding="utf-8"))
    assert summary["metadata"]["portfolioos_head_sha"] == "deadbeef-dirty"
    assert summary["pooled_summary"][1]["production_view"] == "signed_spread"
