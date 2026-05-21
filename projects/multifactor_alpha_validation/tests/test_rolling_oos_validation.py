from __future__ import annotations

from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.rolling_oos_validation import run_rolling_oos_factor_validation


FIXTURE_MANIFEST = Path(
    "projects/multifactor_alpha_validation/fixtures/research_dataset/research_mode_dataset_manifest_fixture.yaml"
)


def test_rolling_oos_validation_uses_prior_history_only_and_explicit_windows(tmp_path: Path) -> None:
    result = run_rolling_oos_factor_validation(FIXTURE_MANIFEST, tmp_path / "rolling_oos")

    readout = pd.read_csv(result.factor_readout_path)

    assert result.preflight_ready is True
    assert result.uses_full_sample_icir is False
    assert result.train_window
    assert result.validation_window
    assert result.test_window
    assert (pd.to_datetime(readout["history_cutoff_date"]) < pd.to_datetime(readout["rebalance_date"])).all()
    assert (pd.to_datetime(readout["trade_date"]) > pd.to_datetime(readout["rebalance_date"])).all()


def test_rolling_oos_validation_separates_readouts_and_records_honest_null(tmp_path: Path) -> None:
    result = run_rolling_oos_factor_validation(FIXTURE_MANIFEST, tmp_path / "rolling_oos")

    readout = pd.read_csv(result.factor_readout_path)
    funnel = pd.read_csv(result.survival_funnel_path)
    report = Path(result.report_path).read_text().lower()

    assert {"raw_return", "neutralized_return", "cost_adjusted_return"}.issubset(readout.columns)
    assert result.honest_null_recorded is True
    assert "cost_adjusted_survived" in set(funnel["layer"])
    assert "weak or collapsed results are recorded honestly" in report
    assert "full-sample icir weighting is forbidden" in report
