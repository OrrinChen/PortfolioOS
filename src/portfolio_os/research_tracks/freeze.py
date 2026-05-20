"""Research freeze registry before the PortfolioOS quant walk-forward pivot."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
import yaml


FrozenResearchState = Literal[
    "blocked",
    "retired_before_promotion",
    "stopped_before_d3",
    "hold_pending_data_coverage",
    "d2_only_hold_insufficient_sample",
]


class FrozenResearchLine(BaseModel):
    """One frozen research line and its downstream boundary flags."""

    model_config = ConfigDict(extra="forbid")

    line_id: str
    title: str
    state: FrozenResearchState
    stop_reason: str
    next_allowed_work: list[str] = Field(default_factory=list)
    automatic_reopen_allowed: bool = False
    d3_allowed: bool = False
    q1_allowed: bool = False
    q2_allowed: bool = False
    optimizer_entry_allowed: bool = False
    alpha_registry_promotion_allowed: bool = False
    paper_ready: bool = False
    live_ready: bool = False
    broker_order_path_opened: bool = False
    production_approval_claimed: bool = False


class ResearchFreezeRegistry(BaseModel):
    """Machine-readable freeze status for prior alpha research lines."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    freeze_reason: str
    freeze_is_governance_only: bool = True
    delete_or_revert_research_artifacts: bool = False
    commit_required: bool = False
    portfolio_quant_pivot_allowed: bool = True
    factor_discovery_direct_q2_allowed: bool = False
    lines: list[FrozenResearchLine]

    def line_by_id(self, line_id: str) -> FrozenResearchLine:
        """Return one frozen research line by id."""

        for line in self.lines:
            if line.line_id == line_id:
                return line
        raise KeyError(line_id)


def load_research_freeze_registry(path: str | Path) -> ResearchFreezeRegistry:
    """Load and validate the research freeze registry."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    registry = ResearchFreezeRegistry.model_validate(payload)
    validate_research_freeze_registry(registry)
    return registry


def validate_research_freeze_registry(registry: ResearchFreezeRegistry) -> None:
    """Validate that frozen lines cannot advance into downstream workflows."""

    if registry.schema_version != "portfolioos_research_freeze.v1":
        raise ValueError(f"unsupported research-freeze schema: {registry.schema_version}")
    if not registry.freeze_is_governance_only:
        raise ValueError("research freeze must be governance-only")
    if registry.delete_or_revert_research_artifacts:
        raise ValueError("research freeze must not delete or revert research artifacts")
    if registry.commit_required:
        raise ValueError("research freeze must not require a commit")
    if registry.factor_discovery_direct_q2_allowed:
        raise ValueError("Factor Discovery direct Q2 handoff remains forbidden")

    required_lines = {
        "sue_historical_timing_line",
        "open_market_insider_buying_post_2023_v0",
        "planned_vs_discretionary_sell_contrast_post_2023",
        "eightk_subtype_underreaction",
        "small_cap_shock_conditioned_emotion_liquidity",
    }
    present_lines = {line.line_id for line in registry.lines}
    missing = required_lines - present_lines
    if missing:
        raise ValueError("missing frozen research line(s): " + ", ".join(sorted(missing)))

    for line in registry.lines:
        if line.automatic_reopen_allowed:
            raise ValueError(f"automatic reopen is forbidden: {line.line_id}")
        forbidden_flags = {
            "d3_allowed": line.d3_allowed,
            "q1_allowed": line.q1_allowed,
            "q2_allowed": line.q2_allowed,
            "optimizer_entry_allowed": line.optimizer_entry_allowed,
            "alpha_registry_promotion_allowed": line.alpha_registry_promotion_allowed,
            "paper_ready": line.paper_ready,
            "live_ready": line.live_ready,
            "broker_order_path_opened": line.broker_order_path_opened,
            "production_approval_claimed": line.production_approval_claimed,
        }
        enabled = [name for name, value in forbidden_flags.items() if value]
        if enabled:
            raise ValueError(
                f"frozen research line enables forbidden flag(s): {line.line_id} "
                + ", ".join(enabled)
            )
