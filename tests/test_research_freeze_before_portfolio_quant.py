from __future__ import annotations

from pathlib import Path

from portfolio_os.research_tracks.freeze import load_research_freeze_registry


FREEZE_PATH = Path("configs/research_freeze_before_portfolio_quant.yaml")


def test_recent_alpha_research_lines_are_frozen_before_portfolio_quant() -> None:
    registry = load_research_freeze_registry(FREEZE_PATH)

    assert registry.schema_version == "portfolioos_research_freeze.v1"
    assert registry.freeze_reason == "pivot_to_portfolio_quant_walk_forward_v1"
    assert registry.portfolio_quant_pivot_allowed is True

    states_by_id = {line.line_id: line.state for line in registry.lines}
    assert states_by_id == {
        "sue_historical_timing_line": "blocked",
        "open_market_insider_buying_post_2023_v0": "retired_before_promotion",
        "planned_vs_discretionary_sell_contrast_post_2023": "stopped_before_d3",
        "eightk_subtype_underreaction": "hold_pending_data_coverage",
        "small_cap_shock_conditioned_emotion_liquidity": "d2_only_hold_insufficient_sample",
    }

    for line in registry.lines:
        assert line.automatic_reopen_allowed is False
        assert line.d3_allowed is False
        assert line.q1_allowed is False
        assert line.q2_allowed is False
        assert line.optimizer_entry_allowed is False
        assert line.alpha_registry_promotion_allowed is False
        assert line.paper_ready is False
        assert line.live_ready is False
        assert line.broker_order_path_opened is False
        assert line.production_approval_claimed is False


def test_research_freeze_preserves_no_direct_factor_discovery_q2_handoff() -> None:
    registry = load_research_freeze_registry(FREEZE_PATH)

    assert registry.factor_discovery_direct_q2_allowed is False
    assert registry.freeze_is_governance_only is True
    assert registry.delete_or_revert_research_artifacts is False
    assert registry.commit_required is False
