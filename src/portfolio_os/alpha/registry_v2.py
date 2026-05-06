"""Alpha Registry v2 decision state machine.

The registry freezes alpha statuses after the typed-alpha closeout path. It is
local-only and does not trigger research, Q2 execution, paper canaries, broker
paths, orders, or production approval.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


ALPHA_REGISTRY_SCHEMA_VERSION = "alpha_registry.v2"
ALPHA_REGISTRY_ENTRY_SCHEMA_VERSION = "alpha_registry_entry.v2"

AlphaRegistryStatus = Literal[
    "canonical_pilot",
    "eligible_for_q2_eval",
    "q2_observed_survives",
    "q2_observed_fails_cost",
    "q2_observed_fails_constraints",
    "real_shadow_branch",
    "needs_marginal_value",
    "diagnostic_only",
    "calibration_only",
    "background_partially_real",
    "rejected_leakage",
    "archived_no_marginal_value",
    "production_not_approved",
]

FORBIDDEN_PASS_FAIL_LABELS = {"pass", "passed", "fail", "failed"}
FORBIDDEN_DECISION_CLAIM_PHRASES = {
    "production approved",
    "paper ready",
    "live trading",
    "broker",
    "order",
    "real alpha proven",
    "historical alpha proven",
}


class AlphaRegistryDecisionRecord(BaseModel):
    """Structured evidence record for one registry decision update."""

    model_config = ConfigDict(extra="forbid")

    decision_label: str
    evidence_type: str
    event_count: int | None = None
    rebalance_date_count: int | None = None
    active_rebalance_count: int | None = None
    median_active_names_per_active_date: float | None = None
    expected_return_used_share: float | None = None
    coverage_loss_count: int | None = None
    q2_observed_rows: int | None = None
    q2_unavailable_rows: int | None = None
    production_approval_claimed: bool = False

    @model_validator(mode="after")
    def validate_decision_record(self) -> "AlphaRegistryDecisionRecord":
        """Keep decision history as evidence metadata, not approval language."""

        text = f"{self.decision_label} {self.evidence_type}".lower()
        if any(phrase in text for phrase in FORBIDDEN_DECISION_CLAIM_PHRASES):
            raise ValueError("AlphaRegistryDecisionRecord contains forbidden approval or workflow language")
        if self.production_approval_claimed:
            raise ValueError("AlphaRegistryDecisionRecord cannot claim production approval")
        return self


class AlphaRegistryEntry(BaseModel):
    """One alpha-family decision state."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["alpha_registry_entry.v2"] = ALPHA_REGISTRY_ENTRY_SCHEMA_VERSION
    alpha_id: str
    display_name: str
    primary_status: AlphaRegistryStatus
    status_history: list[AlphaRegistryStatus] = Field(default_factory=list)
    typed_chain_stop_layer: str
    decision_source_phase: str
    decision_source_artifact: str
    decision_summary: str
    next_allowed_work: str
    production_approval_claimed: bool = False
    live_trading_allowed: bool = False
    decision_history: list[AlphaRegistryDecisionRecord] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    @field_validator("primary_status", mode="before")
    @classmethod
    def reject_primary_pass_fail_label(cls, value: str) -> str:
        """Produce a clear error before Literal validation runs."""

        if str(value).lower() in FORBIDDEN_PASS_FAIL_LABELS:
            raise ValueError("AlphaRegistryEntry must not use pass/fail labels")
        return str(value)

    @field_validator("status_history", mode="before")
    @classmethod
    def reject_history_pass_fail_labels(cls, value: object) -> object:
        """Reject pass/fail history labels before Literal validation runs."""

        if isinstance(value, list) and any(str(item).lower() in FORBIDDEN_PASS_FAIL_LABELS for item in value):
            raise ValueError("AlphaRegistryEntry must not use pass/fail labels")
        return value

    @model_validator(mode="after")
    def validate_decision_state(self) -> "AlphaRegistryEntry":
        """Keep registry labels explicit and non-binary."""

        labels = [self.primary_status, *self.status_history]
        if any(label.lower() in FORBIDDEN_PASS_FAIL_LABELS for label in labels):
            raise ValueError("AlphaRegistryEntry must not use pass/fail labels")
        if not self.typed_chain_stop_layer.strip():
            raise ValueError("typed_chain_stop_layer is required")
        if self.production_approval_claimed:
            raise ValueError("AlphaRegistryEntry cannot claim production approval")
        if self.live_trading_allowed:
            raise ValueError("AlphaRegistryEntry cannot allow live trading")
        return self


class AlphaRegistryV2(BaseModel):
    """Machine-readable alpha status registry."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["alpha_registry.v2"] = ALPHA_REGISTRY_SCHEMA_VERSION
    registry_id: str = "portfolioos_alpha_registry_v2"
    generated_from_phase: str = "Phase 55"
    operating_mode: str = "paper-stage only"
    entries: list[AlphaRegistryEntry]
    production_approval_claimed: bool = False
    live_trading_allowed: bool = False

    @model_validator(mode="after")
    def validate_registry(self) -> "AlphaRegistryV2":
        """Require unique ids and keep global approval flags false."""

        ids = [entry.alpha_id for entry in self.entries]
        if len(ids) != len(set(ids)):
            raise ValueError("AlphaRegistryV2 requires unique alpha_id values")
        if self.production_approval_claimed:
            raise ValueError("AlphaRegistryV2 cannot claim production approval")
        if self.live_trading_allowed:
            raise ValueError("AlphaRegistryV2 cannot allow live trading")
        return self


def build_default_alpha_registry_v2() -> AlphaRegistryV2:
    """Build the deterministic Phase 55 alpha registry."""

    return AlphaRegistryV2(
        entries=[
            AlphaRegistryEntry(
                alpha_id="sue_pead",
                display_name="SUE / PEAD",
                primary_status="canonical_pilot",
                status_history=[
                    "canonical_pilot",
                    "eligible_for_q2_eval",
                    "q2_observed_survives",
                    "production_not_approved",
                ],
                typed_chain_stop_layer="q2_observed_survives_local_fixture",
                decision_source_phase="Phase 50-51",
                decision_source_artifact="reports/sue_typed_q2_survival_attribution.md",
                decision_summary=(
                    "SUE reaches the local optimizer input and maps observed local Q2 rows, including "
                    "the risk-controlled fixture layer through the stable naive_pro_rata adapter."
                ),
                next_allowed_work="Keep as canonical typed-alpha pilot; do not claim paper or production approval.",
                decision_history=[
                    AlphaRegistryDecisionRecord(
                        decision_label="sue_expanded_fixture_q2_observed_survives",
                        evidence_type="deterministic_expanded_fixture",
                        event_count=120,
                        rebalance_date_count=12,
                        active_rebalance_count=12,
                        median_active_names_per_active_date=10.0,
                        expected_return_used_share=0.833333,
                        coverage_loss_count=24,
                        q2_observed_rows=30,
                        q2_unavailable_rows=0,
                        production_approval_claimed=False,
                    )
                ],
                notes=[
                    "SUE remains an integration benchmark and Q2 candidate.",
                    "Phase 51 labels the local fixture result as sue_q2_observed_survives.",
                    "Phase 56A expands deterministic fixture breadth and does not change SUE production status.",
                ],
            ),
            AlphaRegistryEntry(
                alpha_id="revision_1m",
                display_name="revision_1m",
                primary_status="real_shadow_branch",
                status_history=["real_shadow_branch", "archived_no_marginal_value", "production_not_approved"],
                typed_chain_stop_layer="revision_marginal_value_gate",
                decision_source_phase="Phase 52",
                decision_source_artifact="reports/revision_marginal_value_report.md",
                decision_summary=(
                    "Revision passes the local marginal diagnostics but fails the SUE-adjusted "
                    "cost-aware threshold, so it does not open composite evaluation."
                ),
                next_allowed_work="Keep as shadow branch unless a future explicit marginal-value fixture clears the gate.",
                notes=[
                    "WRDS remains the required PIT analyst revision source.",
                    "FMP frozen estimate history is rejected as PIT-safe revision evidence.",
                ],
            ),
            AlphaRegistryEntry(
                alpha_id="sue_revision_composite",
                display_name="SUE + revision composite",
                primary_status="archived_no_marginal_value",
                status_history=["archived_no_marginal_value", "production_not_approved"],
                typed_chain_stop_layer="not_built_phase52_archive",
                decision_source_phase="Phase 52",
                decision_source_artifact="reports/revision_marginal_value_report.md",
                decision_summary=(
                    "Composite AlphaView work remains closed because revision did not clear "
                    "the marginal-value gate."
                ),
                next_allowed_work="Do not run Phase 53-54 unless revision is explicitly promoted by a future gate result.",
            ),
            AlphaRegistryEntry(
                alpha_id="phase_1_5_bridge",
                display_name="Old real alpha package / Phase 1.5 bridge",
                primary_status="diagnostic_only",
                status_history=["diagnostic_only", "production_not_approved"],
                typed_chain_stop_layer="typed_projection_activation_gap",
                decision_source_phase="Pre-Phase 35 alpha package audit",
                decision_source_artifact="TASK_MEMORY.md",
                decision_summary=(
                    "The old alpha package remains diagnostic because activation was sparse and "
                    "some active months had wrong-way mapping under the typed-alpha lens."
                ),
                next_allowed_work="Use as historical diagnostic only; do not promote directly to Q2 or paper-stage.",
            ),
            AlphaRegistryEntry(
                alpha_id="qlib_fixed_horizon_revision",
                display_name="Qlib fixed-horizon + revision",
                primary_status="diagnostic_only",
                status_history=["diagnostic_only", "needs_marginal_value", "production_not_approved"],
                typed_chain_stop_layer="fixed_horizon_absorption_gap",
                decision_source_phase="Typed alpha closeout",
                decision_source_artifact="reports/typed_alpha_closeout_report.md",
                decision_summary=(
                    "Fixed-horizon revision remains a methods asset; event-aware marginal value "
                    "must be proven before any Q2 path."
                ),
                next_allowed_work="Import only through the typed research contract if reopened later.",
            ),
            AlphaRegistryEntry(
                alpha_id="residual_momentum_reversal",
                display_name="Residual momentum / residual reversal",
                primary_status="calibration_only",
                status_history=["calibration_only", "production_not_approved"],
                typed_chain_stop_layer="placebo_dominance_calibration",
                decision_source_phase="Phase 62 locked future work",
                decision_source_artifact="ROADMAP.md",
                decision_summary=(
                    "Residual momentum remains calibration-only because the discovery harness "
                    "must close placebo dominance before promotion can be considered."
                ),
                next_allowed_work="Do not promote before Phase 62 calibration closeout.",
            ),
            AlphaRegistryEntry(
                alpha_id="ashare_anti_mom_21_5",
                display_name="A-share anti_mom_21_5",
                primary_status="background_partially_real",
                status_history=["background_partially_real", "production_not_approved"],
                typed_chain_stop_layer="branch_frozen_requires_phase63_charter",
                decision_source_phase="Phase 63 locked future work",
                decision_source_artifact="ROADMAP.md",
                decision_summary=(
                    "The A-share line remains background and partially real; it is not reopened "
                    "without an explicit Phase 63 state-transition tranche charter."
                ),
                next_allowed_work="Reopen only through a new typed A-share tranche charter.",
            ),
            AlphaRegistryEntry(
                alpha_id="forward_return_leakage_fixture",
                display_name="Forward-return leakage fixtures",
                primary_status="rejected_leakage",
                status_history=["rejected_leakage", "production_not_approved"],
                typed_chain_stop_layer="leakage_gate",
                decision_source_phase="Q1 / Evidence Bundle / Promotion Gate",
                decision_source_artifact="projects/evidence_bundle/examples/rejected_bundle_forward_leakage.yaml",
                decision_summary=(
                    "Forward-return leakage fixtures are rejected before Q2 and cannot become "
                    "PortfolioOS optimizer or execution inputs."
                ),
                next_allowed_work="Use only as negative test fixtures.",
            ),
        ]
    )


def write_alpha_registry_v2_artifacts(
    registry: AlphaRegistryV2,
    output_dir: str | Path = "outputs/alpha_registry_v2",
    *,
    report_path: str | Path | None = None,
) -> dict[str, Path]:
    """Write registry YAML, decision table CSV, and Markdown report."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    registry_path = output_path / "alpha_registry.yaml"
    table_path = output_path / "alpha_registry_decision_table.csv"
    report_destination = Path(report_path) if report_path else output_path / "alpha_registry_report.md"
    report_destination.parent.mkdir(parents=True, exist_ok=True)

    registry_path.write_text(
        yaml.safe_dump(registry.model_dump(mode="json"), sort_keys=False),
        encoding="utf-8",
    )
    _registry_frame(registry).to_csv(table_path, index=False)
    report_destination.write_text(render_alpha_registry_report(registry), encoding="utf-8")

    return {
        "registry": registry_path,
        "decision_table": table_path,
        "report": report_destination,
    }


def render_alpha_registry_report(registry: AlphaRegistryV2) -> str:
    """Render a concise Markdown report for Phase 55."""

    status_counts = _registry_frame(registry)["primary_status"].value_counts().to_dict()
    lines = [
        "# Alpha Registry v2",
        "",
        "This registry freezes alpha decision states after the typed-alpha closeout path.",
        "production approval: not claimed",
        "no live trading allowed by registry",
        "",
        "## Summary",
        "",
        f"- registry_id: `{registry.registry_id}`",
        f"- operating_mode: `{registry.operating_mode}`",
        f"- entry_count: `{len(registry.entries)}`",
        f"- primary_status_counts: `{status_counts}`",
        "",
        "## Latest Decision History",
        "",
        "| alpha_id | latest_decision_label | evidence_type | event_count | q2_observed_rows | q2_unavailable_rows |",
        "|---|---|---|---|---|---|",
    ]
    for entry in registry.entries:
        latest = entry.decision_history[-1] if entry.decision_history else None
        lines.append(
            f"| {entry.alpha_id} | {latest.decision_label if latest else ''} | "
            f"{latest.evidence_type if latest else ''} | {latest.event_count if latest else ''} | "
            f"{latest.q2_observed_rows if latest else ''} | {latest.q2_unavailable_rows if latest else ''} |"
        )
    lines.extend(
        [
            "",
        "## Decision Table",
        "",
        "| alpha_id | display_name | primary_status | stop_layer | source_phase |",
        "|---|---|---|---|---|",
        ]
    )
    for entry in registry.entries:
        lines.append(
            f"| {entry.alpha_id} | {entry.display_name} | {entry.primary_status} | "
            f"{entry.typed_chain_stop_layer} | {entry.decision_source_phase} |"
        )
    lines.extend(
        [
            "",
            "## Non-Claims",
            "",
            "- no broker workflow",
            "- no orders or trading instructions",
            "- no production alpha approval",
            "- no paper canary approval",
            "- no new alpha research branch is opened by this registry",
            "",
        ]
    )
    return "\n".join(lines)


def _registry_frame(registry: AlphaRegistryV2) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "alpha_id": entry.alpha_id,
                "display_name": entry.display_name,
                "primary_status": entry.primary_status,
                "status_history": ", ".join(str(status) for status in entry.status_history),
                "typed_chain_stop_layer": entry.typed_chain_stop_layer,
                "decision_source_phase": entry.decision_source_phase,
                "decision_source_artifact": entry.decision_source_artifact,
                "production_approval_claimed": entry.production_approval_claimed,
                "live_trading_allowed": entry.live_trading_allowed,
                "decision_history_count": len(entry.decision_history),
                "latest_decision_label": entry.decision_history[-1].decision_label if entry.decision_history else "",
                "next_allowed_work": entry.next_allowed_work,
            }
            for entry in registry.entries
        ]
    )
