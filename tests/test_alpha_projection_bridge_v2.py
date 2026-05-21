from __future__ import annotations

from pathlib import Path

from portfolio_os.alpha.projection import (
    AlphaProjectionConfig,
    project_alpha_views_to_expected_returns,
    write_alpha_projection_artifacts,
)
from portfolio_os.alpha.projection_diagnostics import rank_projected_expected_returns
from portfolio_os.alpha.view_contract import AlphaView, load_alpha_view


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = REPO_ROOT / "projects" / "alpha_view_contract"
SUE_VIEW = PROJECT_ROOT / "examples" / "event_sue_pead_view.json"
REVISION_VIEW = PROJECT_ROOT / "examples" / "event_revision_view.json"


def test_sue_event_view_projects_into_rebalance_expected_return_panel() -> None:
    result = project_alpha_views_to_expected_returns(
        alpha_views=[load_alpha_view(SUE_VIEW)],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10", "2025-03-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA", "NVDA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )

    active_rows = [row for row in result.expected_return_panel if row["date"] == "2025-02-10"]
    inactive_rows = [row for row in result.expected_return_panel if row["date"] == "2025-03-10"]

    assert {row["symbol"] for row in active_rows} == {"AAPL", "MSFT"}
    assert inactive_rows == []
    assert all(row["expected_return"] != 0.0 for row in active_rows)
    assert not any(row["symbol"] == "TSLA" for row in result.expected_return_panel)
    assert {
        row["symbol"]
        for row in result.alpha_abstain_report
        if row["date"] == "2025-02-10" and row["alpha_view_id"] == "AV-US-SUE-PEAD-001"
    } == {"NVDA", "TSLA"}


def test_revision_to_next_announcement_projects_until_next_event() -> None:
    result = project_alpha_views_to_expected_returns(
        alpha_views=[load_alpha_view(REVISION_VIEW)],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-04-15", "2025-05-05"],
            universe_symbols=["AAPL", "MSFT"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )

    assert {row["symbol"] for row in result.expected_return_panel if row["date"] == "2025-04-15"} == {
        "AAPL",
        "MSFT",
    }
    assert [row for row in result.expected_return_panel if row["date"] == "2025-05-05"] == []
    assert any(
        row["reason"] == "inactive_outside_view_window"
        for row in result.alpha_abstain_report
        if row["date"] == "2025-05-05"
    )


def test_projection_diagnostics_explain_active_abstain_horizon_decay_and_scale() -> None:
    result = project_alpha_views_to_expected_returns(
        alpha_views=[load_alpha_view(SUE_VIEW), load_alpha_view(REVISION_VIEW)],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-04-15"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 10.0},
        ),
    )

    diagnostic = result.alpha_projection_diagnostics[0]

    assert diagnostic["date"] == "2025-04-15"
    assert diagnostic["active_views"] == ["AV-US-REVISION-TNA-001"]
    assert "AV-US-SUE-PEAD-001" in diagnostic["abstained_views"]
    assert diagnostic["coverage_count"] == 2
    assert diagnostic["horizon_conversion"]
    assert diagnostic["decay_applied"]
    assert diagnostic["final_expected_return_scale"]


def test_explicit_abstain_is_not_encoded_as_zero_alpha() -> None:
    result = project_alpha_views_to_expected_returns(
        alpha_views=[load_alpha_view(SUE_VIEW)],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )

    assert not any(row["symbol"] == "TSLA" for row in result.expected_return_panel)
    assert any(
        row["symbol"] == "TSLA" and row["reason"] == "coverage_missing"
        for row in result.alpha_abstain_report
    )


def test_synthetic_sign_flip_changes_optimizer_input_ranking_direction() -> None:
    original = load_alpha_view(SUE_VIEW)
    flipped_payload = original.model_dump(mode="json")
    for entry in flipped_payload["expected_return_view"].values():
        if entry["state"] == "active_view":
            entry["value"] = -float(entry["value"])
    flipped = AlphaView.validate_payload(flipped_payload)

    config = AlphaProjectionConfig(
        rebalance_dates=["2025-02-10"],
        universe_symbols=["AAPL", "MSFT"],
        risk_horizon_days=21,
        cost_assumptions={"cost_bps": 5.0},
    )
    original_result = project_alpha_views_to_expected_returns(alpha_views=[original], config=config)
    flipped_result = project_alpha_views_to_expected_returns(alpha_views=[flipped], config=config)

    assert rank_projected_expected_returns(original_result.expected_return_panel, "2025-02-10") == ["AAPL", "MSFT"]
    assert rank_projected_expected_returns(flipped_result.expected_return_panel, "2025-02-10") == ["MSFT", "AAPL"]


def test_projection_artifacts_are_deterministic(tmp_path: Path) -> None:
    result = project_alpha_views_to_expected_returns(
        alpha_views=[load_alpha_view(SUE_VIEW)],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )

    artifacts = write_alpha_projection_artifacts(result, tmp_path)

    assert set(artifacts) == {
        "expected_return_panel.csv",
        "alpha_projection_manifest.json",
        "alpha_projection_diagnostics.json",
        "alpha_abstain_report.json",
    }
    assert (tmp_path / "expected_return_panel.csv").read_text(encoding="utf-8").splitlines()[0] == (
        "date,symbol,expected_return,active_alpha_views,horizon_conversion,decay_applied,confidence_weight"
    )
    assert (tmp_path / "alpha_projection_manifest.json").read_text(encoding="utf-8").endswith("\n")
