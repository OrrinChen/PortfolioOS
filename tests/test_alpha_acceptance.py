from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from portfolio_os.api.cli import alpha_acceptance_app
from portfolio_os.alpha.acceptance import (
    AlphaRecipeConfig,
    _compute_mean_monthly_factor_turnover,
    _intersect_evaluation_dates,
    _expand_round_recipes,
    default_round_one_recipes,
    run_alpha_acceptance_gate,
)


@dataclass(frozen=True)
class SnapshotFixture:
    returns_file: Path


def _write_returns_fixture(
    tmp_path: Path,
    *,
    returns_by_ticker: dict[str, list[float]],
    start_date: str = "2025-01-02",
) -> SnapshotFixture:
    dates = pd.date_range(start_date, periods=len(next(iter(returns_by_ticker.values()))), freq="B")
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
    return SnapshotFixture(returns_file=returns_path)


def _write_acceptance_fixture(tmp_path: Path) -> SnapshotFixture:
    periods = 260
    return _write_returns_fixture(
        tmp_path,
        returns_by_ticker={
            "AAA": [0.020] * periods,
            "BBB": [-0.020] * periods,
            "CCC": [0.010] * periods,
            "DDD": [-0.010] * periods,
        },
    )


def _write_rejection_fixture(tmp_path: Path) -> SnapshotFixture:
    periods = 260
    return _write_returns_fixture(
        tmp_path,
        returns_by_ticker={
            "AAA": [0.0] * periods,
            "BBB": [0.0] * periods,
            "CCC": [0.0] * periods,
            "DDD": [0.0] * periods,
        },
    )


def _baseline_and_worse_challenger_recipes() -> list[AlphaRecipeConfig]:
    return [
        AlphaRecipeConfig(
            recipe_name="equal_weight_momentum_6_1",
            reversal_lookback_days=21,
            momentum_lookback_days=126,
            momentum_skip_days=21,
            forward_horizon_days=5,
            reversal_weight=0.0,
            momentum_weight=1.0,
            quantiles=2,
            min_assets_per_date=4,
        ),
        AlphaRecipeConfig(
            recipe_name="current_50_50",
            reversal_lookback_days=21,
            momentum_lookback_days=126,
            momentum_skip_days=21,
            forward_horizon_days=5,
            reversal_weight=0.5,
            momentum_weight=0.5,
            quantiles=2,
            min_assets_per_date=4,
        ),
    ]


def _all_bad_recipes() -> list[AlphaRecipeConfig]:
    return [
        AlphaRecipeConfig(
            recipe_name="equal_weight_momentum_6_1",
            reversal_lookback_days=21,
            momentum_lookback_days=126,
            momentum_skip_days=21,
            forward_horizon_days=5,
            reversal_weight=0.0,
            momentum_weight=1.0,
            quantiles=2,
            min_assets_per_date=4,
        ),
        AlphaRecipeConfig(
            recipe_name="momentum_heavy_10_90",
            reversal_lookback_days=21,
            momentum_lookback_days=84,
            momentum_skip_days=21,
            forward_horizon_days=5,
            reversal_weight=0.1,
            momentum_weight=0.9,
            quantiles=2,
            min_assets_per_date=4,
        ),
    ]


def _build_turnover_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"date": "2025-01-31", "ticker": "AAA", "alpha_score": 1.0},
            {"date": "2025-01-31", "ticker": "BBB", "alpha_score": -1.0},
            {"date": "2025-01-31", "ticker": "CCC", "alpha_score": 0.8},
            {"date": "2025-01-31", "ticker": "DDD", "alpha_score": -0.8},
            {"date": "2025-02-28", "ticker": "AAA", "alpha_score": 1.1},
            {"date": "2025-02-28", "ticker": "BBB", "alpha_score": -1.0},
            {"date": "2025-02-28", "ticker": "CCC", "alpha_score": 0.9},
            {"date": "2025-02-28", "ticker": "DDD", "alpha_score": -0.9},
        ]
    )


def test_run_alpha_acceptance_gate_accepts_baseline_when_it_clears_absolute_gate(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    output_dir = tmp_path / "acceptance_output"

    result = run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=output_dir,
        recipe_configs=_baseline_and_worse_challenger_recipes(),
        max_rounds=1,
    )

    assert result.decision_payload["status"] == "accepted"
    assert result.decision_payload["acceptance_mode"] == "accepted_as_baseline"
    assert result.decision_payload["accepted_recipe_name"] == "equal_weight_momentum_6_1"


def test_run_alpha_acceptance_gate_can_reject_when_no_recipe_clears_gate(tmp_path: Path) -> None:
    snapshot = _write_rejection_fixture(tmp_path)
    output_dir = tmp_path / "acceptance_output"

    result = run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=output_dir,
        recipe_configs=_all_bad_recipes(),
        max_rounds=1,
    )

    assert result.decision_payload["status"] == "rejected_but_infrastructure_complete"
    assert result.decision_payload["accepted_recipe_name"] is None


def test_run_alpha_acceptance_gate_writes_expected_artifacts(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    output_dir = tmp_path / "acceptance_output"

    run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=output_dir,
        recipe_configs=_baseline_and_worse_challenger_recipes(),
        max_rounds=1,
    )

    assert (output_dir / "alpha_sweep_summary.csv").exists()
    assert (output_dir / "alpha_sweep_manifest.json").exists()
    assert (output_dir / "alpha_acceptance_decision.json").exists()
    assert (output_dir / "alpha_acceptance_note.md").exists()

    decision_payload = json.loads((output_dir / "alpha_acceptance_decision.json").read_text(encoding="utf-8"))
    assert decision_payload["status"] == "accepted"


def test_compute_mean_monthly_factor_turnover_returns_zero_for_stable_top_bucket() -> None:
    frame = _build_turnover_fixture()

    turnover = _compute_mean_monthly_factor_turnover(frame, score_column="alpha_score", quantiles=2)

    assert turnover == 0.0


def test_intersect_evaluation_dates_uses_common_dates_only() -> None:
    first = pd.DataFrame({"date": ["2025-01-03", "2025-01-06", "2025-01-07"]})
    second = pd.DataFrame({"date": ["2025-01-06", "2025-01-07", "2025-01-08"]})

    common_dates = _intersect_evaluation_dates([first, second])

    assert common_dates == ["2025-01-06", "2025-01-07"]


def test_expand_round_candidates_deduplicates_previous_recipes() -> None:
    expanded = _expand_round_recipes(
        round_number=2,
        parent_recipes=default_round_one_recipes()[:2],
        tested_recipe_names={item.recipe_name for item in default_round_one_recipes()},
    )

    assert len({item.recipe_name for item in expanded}) == len(expanded)
    assert all(item.recipe_name not in {recipe.recipe_name for recipe in default_round_one_recipes()} for item in expanded)


def test_alpha_acceptance_cli_writes_outputs(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    output_dir = tmp_path / "cli_output"
    runner = CliRunner()

    result = runner.invoke(
        alpha_acceptance_app,
        [
            "--returns-file",
            str(snapshot.returns_file),
            "--output-dir",
            str(output_dir),
            "--max-rounds",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "alpha_acceptance_decision.json").exists()
