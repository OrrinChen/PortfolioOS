from __future__ import annotations

from pathlib import Path

import yaml

from execution_aware_optimizer.experiment_config import (
    ALPHA_DECAY_LADDER_LAYERS,
    ExperimentConfig,
    load_experiment_config,
)
from execution_aware_optimizer.ladder import build_unavailable_ladder_rows


REPO_ROOT = Path(__file__).resolve().parents[3]
Q2_CONFIG_DIR = REPO_ROOT / "projects" / "execution_aware_optimizer" / "configs"


def test_ladder_config_builds_default_layer_sequence() -> None:
    config = ExperimentConfig()

    assert [layer.layer_name for layer in config.layers] == list(ALPHA_DECAY_LADDER_LAYERS)
    assert config.cost_sensitivity_bps == [0, 5, 10, 25, 50]
    assert config.portfolioos.transaction_cost_objective_mode == "nav_fraction"


def test_ladder_config_loads_yaml_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "alpha_decay_ladder.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "experiment_name": "custom",
                "alpha_input": {
                    "path": "alpha.csv",
                    "rank_normalize_by_date": True,
                    "winsorize_quantile": 0.05,
                },
                "portfolioos": {
                    "backtest_manifest": "data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml",
                    "transaction_cost_objective_mode": "nav_fraction",
                },
                "layers": [
                    {"layer_name": "raw_top_alpha_equal_weight"},
                    {"layer_name": "full_execution_aware_cost_adjusted", "enabled": True},
                ],
            }
        ),
        encoding="utf-8",
    )

    config = load_experiment_config(config_path)

    assert config.experiment_name == "custom"
    assert config.alpha_input.path == "alpha.csv"
    assert config.alpha_input.rank_normalize_by_date is True
    assert config.alpha_input.winsorize_quantile == 0.05
    assert [layer.layer_name for layer in config.layers] == [
        "raw_top_alpha_equal_weight",
        "full_execution_aware_cost_adjusted",
    ]


def test_unavailable_ladder_rows_use_standard_failure_schema() -> None:
    rows = build_unavailable_ladder_rows(ExperimentConfig(), reason="adapter not wired")

    assert len(rows) == len(ALPHA_DECAY_LADDER_LAYERS)
    assert rows[0].layer_name == "raw_top_alpha_equal_weight"
    assert rows[0].infeasibility_reason == "adapter not wired"
    assert rows[0].estimated_transaction_cost is None
    assert rows[0].model_dump(mode="json")["binding_constraints"] == []


def test_local_executed_fixture_report_config_is_explicit_opt_in() -> None:
    config = load_experiment_config(Q2_CONFIG_DIR / "local_executed_fixture_report.yaml")

    assert config.experiment_name == "local_executed_fixture_report"
    assert config.portfolioos.allow_portfolioos_run is True
    assert config.portfolioos.backtest_manifest == "data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml"
    assert [layer.layer_name for layer in config.layers] == [
        "raw_top_alpha_equal_weight",
        "risk_controlled",
        "full_execution_aware_cost_adjusted",
    ]
    assert config.report_path.endswith("local_executed_fixture_report.md")
