from __future__ import annotations

from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.research_dry_run import run_first_research_dry_run


FIXTURE_MANIFEST = Path(
    "projects/multifactor_alpha_validation/fixtures/research_dataset/research_mode_dataset_manifest_fixture.yaml"
)


def test_first_research_dry_run_uses_ready_preflight_and_no_allocator(tmp_path: Path) -> None:
    result = run_first_research_dry_run(FIXTURE_MANIFEST, tmp_path / "dry_run")

    assert result.preflight_ready is True
    assert result.factor_ids == ("momentum_12_1", "reversal_5_1", "low_vol_60d")
    assert result.same_close_trading_used is False
    assert result.signal_timestamp_check_passed is True
    assert result.allocator_ran is False
    assert Path(result.report_path).exists()
    report = Path(result.report_path).read_text().lower()
    assert "does not claim alpha success" in report
    assert "allocator not run" in report


def test_first_research_dry_run_reports_separate_qqq_and_beta_adjusted_readouts(tmp_path: Path) -> None:
    result = run_first_research_dry_run(FIXTURE_MANIFEST, tmp_path / "dry_run")

    attribution = pd.read_csv(result.benchmark_attribution_path)

    assert set(attribution["factor_id"]) == {"momentum_12_1", "reversal_5_1", "low_vol_60d"}
    assert "qqq_relative_return" in attribution.columns
    assert "beta_adjusted_return" in attribution.columns
    assert result.allocator_output_path is None
