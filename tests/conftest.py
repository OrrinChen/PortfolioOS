"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from portfolio_os.api.cli import approval_app, scenario_app
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.utils.config import load_app_config


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root."""

    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def sample_context(project_root: Path) -> dict:
    """Load the sample config and universe used across tests."""

    sample_dir = project_root / "data" / "sample"
    config_dir = project_root / "config"

    holdings = load_holdings(sample_dir / "holdings_example.csv")
    targets = load_target_weights(sample_dir / "target_example.csv")
    portfolio_state = load_portfolio_state(sample_dir / "portfolio_state_example.yaml")
    app_config = load_app_config(
        default_path=config_dir / "default.yaml",
        constraints_path=config_dir / "constraints" / "public_fund.yaml",
        execution_path=config_dir / "execution" / "conservative.yaml",
        portfolio_state=portfolio_state,
    )
    portfolio_frame = build_portfolio_frame(holdings, targets)
    required_tickers = portfolio_frame["ticker"].tolist()
    market_frame = market_to_frame(load_market_snapshot(sample_dir / "market_example.csv", required_tickers))
    reference_frame = reference_to_frame(
        load_reference_snapshot(sample_dir / "reference_example.csv", required_tickers)
    )
    universe = build_universe_frame(portfolio_frame, market_frame, reference_frame, portfolio_state)
    return {
        "project_root": project_root,
        "sample_dir": sample_dir,
        "config": app_config,
        "holdings": holdings,
        "targets": targets,
        "portfolio_state": portfolio_state,
        "portfolio_frame": portfolio_frame,
        "market_frame": market_frame,
        "reference_frame": reference_frame,
        "universe": universe,
    }


@pytest.fixture(scope="session")
def replay_manifest_path(project_root: Path) -> Path:
    """Return the replay-suite manifest path."""

    return project_root / "data" / "replay_samples" / "manifest.yaml"


@pytest.fixture(scope="session")
def scenario_manifest_path(project_root: Path) -> Path:
    """Return the scenario-suite manifest path."""

    return project_root / "data" / "scenario_samples" / "manifest.yaml"


@pytest.fixture(scope="session")
def scenario_output_dir(project_root: Path, scenario_manifest_path: Path, tmp_path_factory) -> Path:
    """Build a reusable scenario output directory for approval tests."""

    runner = CliRunner()
    output_dir = tmp_path_factory.mktemp("scenario_output")
    result = runner.invoke(
        scenario_app,
        [
            "--manifest",
            str(scenario_manifest_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    if result.exit_code != 0:
        raise RuntimeError(f"scenario_app failed in fixture: {result.output}")
    return output_dir


@pytest.fixture(scope="session")
def approval_output_dir(scenario_output_dir: Path, tmp_path_factory) -> Path:
    """Build a reusable approval/freeze output directory for execution tests."""

    runner = CliRunner()
    output_dir = tmp_path_factory.mktemp("approval_output")
    request_path = output_dir / "approval_request.yaml"
    request_path.write_text(
        "\n".join(
            [
                "name: approval_fixture_request",
                "description: Freeze a final basket for execution simulation tests",
                f"scenario_output_dir: {scenario_output_dir}",
                "selected_scenario: public_conservative",
                "decision_maker: pm_fixture",
                "decision_role: portfolio_manager",
                "rationale: Fixture request for execution simulation tests.",
                "acknowledged_warning_codes: []",
            ]
        ),
        encoding="utf-8",
    )
    result = runner.invoke(
        approval_app,
        [
            "--request",
            str(request_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    if result.exit_code != 0:
        raise RuntimeError(f"approval_app failed in fixture: {result.output}")
    return output_dir
