"""Alpha research helpers for PortfolioOS."""

from portfolio_os.alpha.acceptance import AlphaAcceptanceResult, AlphaRecipeConfig, default_round_one_recipes, run_alpha_acceptance_gate
from portfolio_os.alpha.event_targets import build_event_basket_target_frame, build_event_target_manifest
from portfolio_os.alpha.promotion_contract import (
    PROMOTION_CONTRACT_FILENAME,
    PROMOTION_CONTRACT_TYPE,
    PROMOTION_CONTRACT_VERSION,
    PromotionContract,
    load_promotion_contract,
    validate_promotion_contract,
)
from portfolio_os.alpha.report import render_alpha_acceptance_note, render_alpha_research_report
from portfolio_os.alpha.research import AlphaResearchResult, build_alpha_score_frame, load_alpha_returns_panel, run_alpha_research
from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_matching_covariates,
    build_upper_limit_event_conditioned_null_pool,
    build_upper_limit_matched_control_comparison_frame,
    build_upper_limit_matched_non_event_control_frame,
    build_upper_limit_pre_event_placebo_comparison_frame,
    build_state_transition_daily_panel,
    build_upper_limit_pilot_expression_frame,
    extract_upper_limit_daily_state_slice,
)

__all__ = [
    "AlphaAcceptanceResult",
    "AlphaRecipeConfig",
    "AlphaResearchResult",
    "PROMOTION_CONTRACT_FILENAME",
    "PROMOTION_CONTRACT_TYPE",
    "PROMOTION_CONTRACT_VERSION",
    "PromotionContract",
    "build_event_basket_target_frame",
    "build_event_target_manifest",
    "build_alpha_score_frame",
    "build_state_transition_matching_covariates",
    "build_upper_limit_event_conditioned_null_pool",
    "build_upper_limit_matched_control_comparison_frame",
    "build_upper_limit_matched_non_event_control_frame",
    "build_upper_limit_pre_event_placebo_comparison_frame",
    "build_upper_limit_pilot_expression_frame",
    "default_round_one_recipes",
    "load_promotion_contract",
    "load_alpha_returns_panel",
    "render_alpha_acceptance_note",
    "render_alpha_research_report",
    "build_state_transition_daily_panel",
    "extract_upper_limit_daily_state_slice",
    "run_alpha_acceptance_gate",
    "run_alpha_research",
    "validate_promotion_contract",
]
