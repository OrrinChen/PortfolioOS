from __future__ import annotations

import pytest

from portfolio_os.data.portfolio import load_portfolio_state
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.simulation.scenarios import (
    SCENARIO_SCORE_WEIGHTS,
    SCENARIO_TIEBREAK_WEIGHTS,
    build_app_config_for_scenario,
    load_scenario_manifest,
    resolve_base_input_paths,
    run_scenario_suite,
)


def test_scenario_manifest_parsing(scenario_manifest_path) -> None:
    manifest = load_scenario_manifest(scenario_manifest_path)

    assert manifest.name == "demo_scenario_suite"
    assert len(manifest.scenarios) == 5
    assert manifest.scenarios[0].id == "public_conservative"


def test_scenario_override_whitelist_is_enforced(tmp_path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "name: bad_suite",
                "base_inputs:",
                "  holdings: data/sample/holdings_example.csv",
                "  target: data/sample/target_example.csv",
                "  market: data/sample/market_example.csv",
                "  reference: data/sample/reference_example.csv",
                "  portfolio_state: data/sample/portfolio_state_example.yaml",
                "  config: config/default.yaml",
                "scenarios:",
                "  - id: bad",
                "    label: Bad",
                "    constraints: config/constraints/public_fund.yaml",
                "    execution_profile: config/execution/conservative.yaml",
                "    overrides:",
                "      unknown_field: 1",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(InputValidationError, match="Invalid scenario manifest"):
        load_scenario_manifest(manifest_path)


def test_scenario_overrides_apply_to_config(project_root, scenario_manifest_path) -> None:
    manifest = load_scenario_manifest(scenario_manifest_path)
    base_input_paths = resolve_base_input_paths(scenario_manifest_path, manifest)
    portfolio_state = load_portfolio_state(base_input_paths["portfolio_state"])
    scenario = next(item for item in manifest.scenarios if item.id == "public_high_cash_buffer")
    app_config, _, _ = build_app_config_for_scenario(
        base_input_paths=base_input_paths,
        portfolio_state=portfolio_state,
        scenario=scenario,
        manifest_path=scenario_manifest_path,
    )

    assert app_config.portfolio_state.min_cash_buffer == 400000
    assert app_config.constraints.min_order_notional == 15000


def test_scenario_suite_runs_and_returns_ranking(scenario_manifest_path) -> None:
    suite = run_scenario_suite(scenario_manifest_path)

    assert len(suite.scenario_runs) == 5
    assert suite.scenario_comparison_payload["ranking"]
    assert suite.scenario_comparison_payload["labels"]["recommended_scenario"]
    assert suite.scenario_comparison_payload["labels"]["lowest_cost_scenario"]
    assert suite.scenario_comparison_payload["scoring_rule"]["weights"] == SCENARIO_SCORE_WEIGHTS
    assert suite.scenario_comparison_payload["scoring_rule"]["tie_break"]["weights"] == SCENARIO_TIEBREAK_WEIGHTS
    assert "recommendation_diagnostics" in suite.scenario_comparison_payload
    assert "score_gap_to_second" in suite.scenario_comparison_payload["recommendation_diagnostics"]
    assert "scenario_with_most_regulatory_findings" in suite.scenario_comparison_payload["cross_scenario_explanation"]
