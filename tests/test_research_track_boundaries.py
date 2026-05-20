from __future__ import annotations

from pathlib import Path

import pytest

from portfolio_os.research_tracks.status_schema import (
    ResearchTrackRegistry,
    load_research_track_registry,
    validate_research_track_boundaries,
)


REGISTRY_PATH = Path("configs/research_tracks.yaml")


def test_research_track_registry_separates_single_alpha_and_multifactor_lines() -> None:
    registry = load_research_track_registry(REGISTRY_PATH)

    assert registry.schema_version == "portfolioos_research_tracks.v1"
    assert {track.track_id for track in registry.tracks} == {
        "shared_governance_platform",
        "single_alpha_research_factory",
        "multifactor_portfolio_validation",
    }

    single_alpha = registry.track_by_id("single_alpha_research_factory")
    multifactor = registry.track_by_id("multifactor_portfolio_validation")
    shared = registry.track_by_id("shared_governance_platform")

    assert "src/portfolio_os/alpha" in single_alpha.code_roots
    assert "projects/typed_alpha_pilot" in single_alpha.code_roots
    assert "projects/multifactor_alpha_validation/src/multifactor_alpha_validation" in multifactor.code_roots
    assert "projects/multifactor_alpha_validation/factor_discovery_sandbox" not in multifactor.code_roots
    assert "projects/promotion_gate" in shared.code_roots
    assert "projects/execution_aware_optimizer" in shared.code_roots

    assert registry.project_by_id("sue_historical_timing_line").track_id == "single_alpha_research_factory"
    assert registry.project_by_id("factor_discovery_sandbox").track_id == "single_alpha_research_factory"
    assert registry.project_by_id("formal_multifactor_validation_engine").track_id == "multifactor_portfolio_validation"


def test_research_track_boundaries_block_direct_q2_and_approval_claims() -> None:
    registry = load_research_track_registry(REGISTRY_PATH)
    validate_research_track_boundaries(registry)

    for project in registry.projects:
        assert project.production_approval_claimed is False
        assert project.live_trading_allowed is False
        assert project.broker_order_workflow_allowed is False
        if project.track_id != "shared_governance_platform":
            assert project.direct_q2_entry_allowed is False

    assert registry.project_by_id("sue_historical_timing_line").state == "blocked_timing"
    assert registry.project_by_id("formal_multifactor_validation_engine").state == "diagnostic_component_pool"
    assert registry.project_by_id("factor_discovery_sandbox").state == "factor_design_reset"


def test_research_track_registry_rejects_multifactor_in_single_alpha_track() -> None:
    registry = load_research_track_registry(REGISTRY_PATH)
    bad_project = registry.project_by_id("formal_multifactor_validation_engine").model_copy(
        update={"track_id": "single_alpha_research_factory"}
    )
    bad_registry = ResearchTrackRegistry(
        schema_version=registry.schema_version,
        tracks=registry.tracks,
        projects=[bad_project, *[p for p in registry.projects if p.project_id != bad_project.project_id]],
        shared_boundaries=registry.shared_boundaries,
    )

    with pytest.raises(ValueError, match="multifactor"):
        validate_research_track_boundaries(bad_registry)
