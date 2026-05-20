"""Research-track boundary schema.

The registry separates candidate-level research from portfolio/component-level
validation while allowing both tracks to share PortfolioOS governance services.
It is a state and boundary layer only; it does not run research, Q2, broker,
paper, or production workflows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
import yaml


TrackId = Literal[
    "shared_governance_platform",
    "single_alpha_research_factory",
    "multifactor_portfolio_validation",
]


class ResearchTrack(BaseModel):
    """One research track or shared platform layer."""

    model_config = ConfigDict(extra="forbid")

    track_id: TrackId
    title: str
    question: str
    role: str
    code_roots: list[str]
    owns_q2_execution: bool = False
    may_generate_candidates: bool = False
    may_validate_portfolios: bool = False
    may_run_broker_or_live: bool = False


class ResearchProjectStatus(BaseModel):
    """Status for one project line within a research track."""

    model_config = ConfigDict(extra="forbid")

    project_id: str
    title: str
    track_id: TrackId
    state: str
    stop_layer: str
    code_roots: list[str] = Field(default_factory=list)
    evidence_artifacts: list[str] = Field(default_factory=list)
    decision_summary: str
    next_allowed_work: list[str] = Field(default_factory=list)
    direct_q2_entry_allowed: bool = False
    alpha_registry_promotion_allowed: bool = False
    production_approval_claimed: bool = False
    live_trading_allowed: bool = False
    broker_order_workflow_allowed: bool = False


class ResearchTrackRegistry(BaseModel):
    """PortfolioOS research-track registry."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    tracks: list[ResearchTrack]
    projects: list[ResearchProjectStatus]
    shared_boundaries: list[str] = Field(default_factory=list)

    def track_by_id(self, track_id: str) -> ResearchTrack:
        for track in self.tracks:
            if track.track_id == track_id:
                return track
        raise KeyError(track_id)

    def project_by_id(self, project_id: str) -> ResearchProjectStatus:
        for project in self.projects:
            if project.project_id == project_id:
                return project
        raise KeyError(project_id)


def load_research_track_registry(path: str | Path) -> ResearchTrackRegistry:
    """Load research-track registry from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    registry = ResearchTrackRegistry.model_validate(payload)
    validate_research_track_boundaries(registry)
    return registry


def validate_research_track_boundaries(registry: ResearchTrackRegistry) -> None:
    """Validate hard boundaries across research tracks."""

    if registry.schema_version != "portfolioos_research_tracks.v1":
        raise ValueError(f"unsupported research-track schema: {registry.schema_version}")
    track_ids = {track.track_id for track in registry.tracks}
    required = {
        "shared_governance_platform",
        "single_alpha_research_factory",
        "multifactor_portfolio_validation",
    }
    missing = required - track_ids
    if missing:
        raise ValueError("missing research tracks: " + ", ".join(sorted(missing)))

    for track in registry.tracks:
        if track.may_run_broker_or_live:
            raise ValueError(f"research track may not run broker/live workflows: {track.track_id}")
        if track.track_id == "multifactor_portfolio_validation" and track.may_generate_candidates:
            raise ValueError("multifactor validation track may not be candidate generation")
        if track.track_id == "single_alpha_research_factory" and track.may_validate_portfolios:
            raise ValueError("single alpha track may not validate portfolios")

    for project in registry.projects:
        if project.production_approval_claimed:
            raise ValueError(f"production approval claim is forbidden: {project.project_id}")
        if project.live_trading_allowed or project.broker_order_workflow_allowed:
            raise ValueError(f"live/broker/order workflow is forbidden: {project.project_id}")
        if project.track_id != "shared_governance_platform" and project.direct_q2_entry_allowed:
            raise ValueError(f"direct Q2 entry is forbidden outside governance: {project.project_id}")
        _validate_project_track_fit(project)


def _validate_project_track_fit(project: ResearchProjectStatus) -> None:
    text = " ".join([project.project_id, project.title, project.decision_summary]).lower()
    if "multifactor" in text and project.track_id == "single_alpha_research_factory":
        raise ValueError(f"multifactor project assigned to single-alpha track: {project.project_id}")
    if "portfolio" in text and project.track_id == "single_alpha_research_factory":
        raise ValueError(f"portfolio project assigned to single-alpha track: {project.project_id}")
    if "sue" in text and project.track_id == "multifactor_portfolio_validation":
        if project.project_id != "sue_event_reference_component":
            raise ValueError(f"SUE research line assigned to multifactor validation: {project.project_id}")
