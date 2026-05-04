from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.paper.overlay_readiness import (
    assess_paper_overlay_readiness,
    render_paper_overlay_readiness_markdown,
    write_paper_overlay_readiness_artifacts,
)


def _observations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "spread_bps": 2.0,
                "half_spread_bps": 1.0,
                "drift_bps": 0.5,
                "drift_vs_half_spread": 0.5,
                "capture_to_submit_latency_seconds": 3.0,
                "time_of_day_bucket": "09:30-10:29",
            },
            {
                "ticker": "SPY",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "spread_bps": 4.0,
                "half_spread_bps": 2.0,
                "drift_bps": -0.4,
                "drift_vs_half_spread": -0.2,
                "capture_to_submit_latency_seconds": 8.0,
                "time_of_day_bucket": "14:30-16:00",
            },
        ]
    )


def test_paper_overlay_readiness_is_execution_calibration_not_alpha_promotion() -> None:
    result = assess_paper_overlay_readiness(
        observations=_observations(),
        requested_sample_count=50,
        max_validated_participation_rate=0.001,
    )

    assert result.summary["scope"] == "paper_overlay_execution_environment_calibration_only"
    assert result.summary["alpha_promotion_allowed"] is False
    assert result.summary["production_config_update_allowed"] is False
    assert result.summary["live_alpha_orders_allowed"] is False
    assert result.summary["max_validated_participation_rate"] == 0.001
    assert result.summary["observation_count"] == 2
    assert result.summary["readiness_status"] == "needs_more_samples"
    assert result.latency_buckets[0]["time_of_day_bucket"] == "09:30-10:29"
    assert result.spread_capture[0]["ticker"] == "SPY"


def test_paper_overlay_readiness_markdown_calls_out_scope_boundaries() -> None:
    result = assess_paper_overlay_readiness(
        observations=_observations(),
        requested_sample_count=2,
        max_validated_participation_rate=0.001,
    )

    markdown = render_paper_overlay_readiness_markdown(result)

    assert "Paper Overlay Readiness" in markdown
    assert "execution environment calibration only" in markdown
    assert "does not validate alpha" in markdown
    assert "No live alpha orders" in markdown


def test_paper_overlay_readiness_artifacts_are_written(tmp_path: Path) -> None:
    result = assess_paper_overlay_readiness(
        observations=_observations(),
        requested_sample_count=2,
        max_validated_participation_rate=0.001,
    )

    artifacts = write_paper_overlay_readiness_artifacts(result, tmp_path)

    assert set(artifacts) == {
        "paper_overlay_calibration_summary.json",
        "paper_overlay_latency_buckets.csv",
        "paper_overlay_spread_capture.csv",
        "paper_overlay_readiness.md",
    }
    assert (tmp_path / "paper_overlay_calibration_summary.json").read_text(encoding="utf-8").endswith("\n")
    assert (tmp_path / "paper_overlay_latency_buckets.csv").read_text(encoding="utf-8").splitlines()[0] == (
        "time_of_day_bucket,observation_count,median_latency_seconds,median_drift_bps,median_spread_bps"
    )
