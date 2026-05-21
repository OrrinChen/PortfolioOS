from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class ComponentOOSAvailabilityResult:
    availability_report_path: str
    summary_path: str
    enablement_plan_path: str
    eligible_component_count: int
    observed_component_count: int
    unavailable_component_count: int
    hard_blocked_component_count: int
    coverage_ratio: float
    component_pool_validation_state: str
    full_pool_decision_allowed: bool
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


_MIN_COMPONENT_POOL_COVERAGE = 0.60


def run_component_oos_availability_expansion(
    spec_dir: Path,
    component_pool_path: Path,
    oos_observation_path: Path,
    portfolio_validation_dir: Path,
    output_dir: Path,
    min_coverage: float = _MIN_COMPONENT_POOL_COVERAGE,
) -> ComponentOOSAvailabilityResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    pool = _normalize_pool(_read_csv(component_pool_path))
    observations = _read_csv(oos_observation_path)
    specs = {spec.factor_id: spec for spec in load_factor_specs(spec_dir)}
    assembly_audit = _read_json(portfolio_validation_dir / "portfolio_assembly_audit.json")

    report = _build_availability_report(pool, observations, specs)
    summary = _build_summary(report, component_pool_path, oos_observation_path, assembly_audit, min_coverage)

    report_path = output_dir / "component_oos_availability_report.csv"
    summary_path = output_dir / "component_oos_availability_summary.json"
    plan_path = output_dir / "component_enablement_plan.md"

    report.to_csv(report_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    plan_path.write_text(_render_enablement_plan(report, summary), encoding="utf-8")

    return ComponentOOSAvailabilityResult(
        availability_report_path=str(report_path),
        summary_path=str(summary_path),
        enablement_plan_path=str(plan_path),
        eligible_component_count=int(summary["eligible_component_count"]),
        observed_component_count=int(summary["observed_component_count"]),
        unavailable_component_count=int(summary["unavailable_component_count"]),
        hard_blocked_component_count=int(summary["hard_blocked_component_count"]),
        coverage_ratio=float(summary["coverage_ratio"]),
        component_pool_validation_state=str(summary["component_pool_validation_state"]),
        full_pool_decision_allowed=bool(summary["full_pool_decision_allowed"]),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _normalize_pool(pool: pd.DataFrame) -> pd.DataFrame:
    if pool.empty:
        return pd.DataFrame(columns=["factor_id", "family_id", "component_pool_eligible", "component_status"])
    normalized = pool.copy()
    normalized["factor_id"] = normalized["factor_id"].astype(str)
    if "family_id" not in normalized.columns:
        normalized["family_id"] = normalized["factor_id"].str.split("_").str[0]
    if "component_pool_eligible" not in normalized.columns:
        normalized["component_pool_eligible"] = normalized.get("portfolio_validation_allowed", True)
    if "component_status" not in normalized.columns:
        normalized["component_status"] = "eligible_component"
    if "component_role" not in normalized.columns:
        normalized["component_role"] = "component"
    if "filter_class" not in normalized.columns:
        normalized["filter_class"] = "soft_resurrected"
    if "hard_exclusion_reason" not in normalized.columns:
        normalized["hard_exclusion_reason"] = ""
    return normalized


def _build_availability_report(
    pool: pd.DataFrame,
    observations: pd.DataFrame,
    specs: dict[str, FactorSpec],
) -> pd.DataFrame:
    observed_counts = _observed_counts(observations)
    rows: list[dict[str, Any]] = []
    observations_missing = observations.empty
    for row in pool.itertuples(index=False):
        factor_id = str(row.factor_id)
        spec = specs.get(factor_id)
        hard_blocked = _is_hard_blocked(row, spec)
        observation_count = int(observed_counts.get(factor_id, 0))
        if hard_blocked:
            current_status = "hard_blocked"
            unavailable_reason = "research_mode_blocked"
        elif observation_count > 0:
            current_status = "observed"
            unavailable_reason = "observed"
        else:
            current_status = "unavailable"
            unavailable_reason = _unavailable_reason(spec, observations_missing)

        required_data = _required_data(spec)
        required_signal_builder = _required_signal_builder(factor_id, spec)
        timestamp_policy = _timestamp_policy(spec)
        reporting_lag_days = _reporting_lag_days(spec)
        fundamental_lag_ok = _fundamental_reporting_lag_ok(spec)
        blocked_reason = _blocked_reason(row, spec, unavailable_reason)

        rows.append(
            {
                "schema_version": "component_oos_availability.v1",
                "factor_id": factor_id,
                "family_id": str(getattr(row, "family_id", "")),
                "component_role": str(getattr(row, "component_role", "")),
                "current_status": current_status,
                "component_status": str(getattr(row, "component_status", "")),
                "filter_class": str(getattr(row, "filter_class", "")),
                "unavailable_reason": unavailable_reason,
                "required_data": required_data,
                "required_signal_builder": required_signal_builder,
                "required_timestamp_policy": timestamp_policy,
                "reporting_lag_days": reporting_lag_days,
                "fundamental_reporting_lag_ok": fundamental_lag_ok,
                "event_visibility_required": bool(spec and spec.data_tier == "tier_3_event"),
                "horizon_type": spec.horizon.horizon_type if spec else "unknown",
                "rebalance_frequency": spec.horizon.rebalance_frequency if spec else "unknown",
                "oos_observation_count": observation_count,
                "can_enable_now": False,
                "enablement_mode": _enablement_mode(current_status, unavailable_reason, spec),
                "blocked_reason": blocked_reason,
                "fabricated_returns": False,
                "not_alpha_evidence": True,
                "production_approval": False,
                "live_trading": False,
                "security_orders": False,
                "direct_q2_entry": False,
                "or_optimizer_unlocked": False,
            }
        )
    return pd.DataFrame(rows, columns=_report_columns())


def _observed_counts(observations: pd.DataFrame) -> dict[str, int]:
    if observations.empty or "factor_id" not in observations.columns:
        return {}
    return {str(factor_id): int(count) for factor_id, count in observations.groupby("factor_id").size().items()}


def _is_hard_blocked(row: Any, spec: FactorSpec | None) -> bool:
    if spec and spec.status == "disabled":
        return True
    return (
        not _as_bool(getattr(row, "component_pool_eligible", True))
        or str(getattr(row, "filter_class", "")).lower() == "hard_excluded"
        or "blocked" in str(getattr(row, "component_status", "")).lower()
    )


def _unavailable_reason(spec: FactorSpec | None, observations_missing: bool) -> str:
    if spec is None:
        return "research_mode_blocked"
    if spec.status == "disabled":
        return "research_mode_blocked"
    if spec.data_tier == "tier_3_estimates":
        return "missing_pit_source"
    if observations_missing and spec.data_tier in {"tier_1_price", "tier_1_price_volume"}:
        return "missing_oos_return_alignment"
    if spec.data_tier == "tier_2_fundamental" and spec.pit_contract.reporting_lag_days < 45:
        return "missing_fundamental_lag"
    if spec.data_tier == "tier_3_event":
        return "missing_event_timestamp"
    if spec.horizon.rebalance_frequency not in {"monthly", "quarterly"} and spec.horizon.horizon_type != "event_window":
        return "horizon_incompatible_with_monthly_ensemble"
    return "missing_signal_panel"


def _required_data(spec: FactorSpec | None) -> str:
    if spec is None:
        return "registered FactorSpec"
    if spec.data_tier == "tier_2_fundamental":
        return "WRDS Compustat PIT fundamentals with filing or rdq visibility and configured reporting lag"
    if spec.data_tier == "tier_3_event":
        return "PIT event panel with public announcement timestamp, consensus, actual value, and trading calendar"
    if spec.data_tier == "tier_3_estimates":
        return spec.pit_source_required or "WRDS-style PIT estimate history"
    fields = ", ".join(spec.data_requirements.required_fields)
    return fields


def _required_signal_builder(factor_id: str, spec: FactorSpec | None) -> str:
    if spec is None:
        return "registered_factor_signal_builder"
    if spec.data_tier == "tier_2_fundamental":
        return f"{factor_id}_lagged_fundamental_signal_builder"
    if spec.data_tier == "tier_3_event":
        return f"{factor_id}_event_timestamp_signal_builder"
    if spec.data_tier == "tier_3_estimates":
        return f"{factor_id}_pit_estimate_revision_signal_builder"
    return f"{factor_id}_real_oos_signal_builder"


def _timestamp_policy(spec: FactorSpec | None) -> str:
    if spec is None:
        return "unknown"
    contract = spec.pit_contract
    return (
        f"signal={contract.signal_timestamp_rule}; "
        f"visibility={contract.visibility_timestamp_rule}; "
        f"tradable={contract.tradable_timestamp_rule}; "
        f"reporting_lag_days={contract.reporting_lag_days}"
    )


def _reporting_lag_days(spec: FactorSpec | None) -> int:
    if spec is None:
        return 0
    return int(spec.pit_contract.reporting_lag_days)


def _fundamental_reporting_lag_ok(spec: FactorSpec | None) -> bool:
    if spec is None or spec.data_tier != "tier_2_fundamental":
        return True
    return spec.pit_contract.reporting_lag_days >= 45


def _blocked_reason(row: Any, spec: FactorSpec | None, unavailable_reason: str) -> str:
    explicit = str(getattr(row, "hard_exclusion_reason", "") or "")
    if explicit and explicit.lower() != "nan":
        return explicit
    if spec and spec.status == "disabled":
        return spec.disabled_reason or "disabled_factor_spec"
    if unavailable_reason == "missing_fundamental_lag":
        return "fundamental_factor_requires_reporting_lag"
    if unavailable_reason == "missing_pit_source":
        return spec.pit_source_required if spec and spec.pit_source_required else "missing_pit_source"
    return ""


def _enablement_mode(current_status: str, unavailable_reason: str, spec: FactorSpec | None) -> str:
    if current_status == "observed":
        return "already_observed"
    if current_status == "hard_blocked":
        return "blocked_no_enablement"
    if unavailable_reason == "missing_signal_panel":
        if spec and spec.data_tier == "tier_2_fundamental":
            return "requires_fundamental_factor_builder_with_lagged_visibility"
        return "requires_real_oos_signal_builder"
    if unavailable_reason == "missing_event_timestamp":
        return "requires_event_panel_with_visibility_timestamp"
    if unavailable_reason == "missing_fundamental_lag":
        return "requires_factor_spec_reporting_lag_fix"
    if unavailable_reason == "missing_oos_return_alignment":
        return "requires_oos_return_alignment"
    if unavailable_reason == "missing_pit_source":
        return "requires_wrds_style_pit_source"
    if unavailable_reason == "horizon_incompatible_with_monthly_ensemble":
        return "requires_horizon_mapping"
    return "requires_research_mode_repair"


def _build_summary(
    report: pd.DataFrame,
    component_pool_path: Path,
    oos_observation_path: Path,
    assembly_audit: dict[str, Any],
    min_coverage: float,
) -> dict[str, Any]:
    if report.empty:
        eligible = pd.DataFrame()
    else:
        eligible = report.loc[~report["current_status"].eq("hard_blocked")].copy()
    observed_count = int(eligible["current_status"].eq("observed").sum()) if not eligible.empty else 0
    eligible_count = int(len(eligible))
    unavailable_count = int(eligible["current_status"].eq("unavailable").sum()) if not eligible.empty else 0
    hard_count = int(report["current_status"].eq("hard_blocked").sum()) if not report.empty else 0
    coverage = round(observed_count / eligible_count, 10) if eligible_count else 0.0
    coverage_sufficient = bool(eligible_count and coverage >= min_coverage)
    unavailable_reasons = Counter(
        str(reason)
        for reason in eligible.loc[eligible["current_status"].eq("unavailable"), "unavailable_reason"].tolist()
    ) if not eligible.empty else Counter()
    return {
        "schema_version": "component_oos_availability_summary.v1",
        "component_pool_path": str(component_pool_path),
        "oos_observation_path": str(oos_observation_path),
        "source_r15_5_reclassified_decision_state": assembly_audit.get("reclassified_decision_state", "unavailable"),
        "source_r15_5_component_pool_validation_state": assembly_audit.get(
            "component_pool_validation_state",
            "unavailable",
        ),
        "eligible_component_count": eligible_count,
        "observed_component_count": observed_count,
        "unavailable_component_count": unavailable_count,
        "hard_blocked_component_count": hard_count,
        "coverage_ratio": coverage,
        "component_pool_validation_min_coverage": min_coverage,
        "component_pool_validation_state": "component_pool_observation_coverage_sufficient"
        if coverage_sufficient
        else "component_pool_validation_incomplete",
        "full_pool_decision_allowed": coverage_sufficient,
        "full_component_pool_failure_claim_allowed": coverage_sufficient,
        "observed_subset_only": not coverage_sufficient,
        "unavailable_reason_counts": dict(sorted(unavailable_reasons.items())),
        "safe_auto_enabled_component_count": 0,
        "oos_generation_status": "no_unavailable_components_safe_to_auto_enable",
        "r15_rerun_required_after_enablement": unavailable_count > 0,
        "fabricated_returns": False,
        "alpha_success_claimed": False,
        "or_optimizer_unlocked": False,
        "security_level_portfolio_construction_used": False,
        "direct_q2_entry": False,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "not_alpha_evidence": True,
    }


def _render_enablement_plan(report: pd.DataFrame, summary: dict[str, Any]) -> str:
    unavailable = report.loc[report["current_status"].eq("unavailable")] if not report.empty else pd.DataFrame()
    hard = report.loc[report["current_status"].eq("hard_blocked")] if not report.empty else pd.DataFrame()
    lines = [
        "# Component OOS Availability Expansion",
        "",
        "This is not alpha evidence. It is an observability and enablement plan for the component pool.",
        "",
        f"- coverage_ratio: `{summary['coverage_ratio']}`",
        f"- component_pool_validation_state: `{summary['component_pool_validation_state']}`",
        f"- full_pool_decision_allowed: `{str(summary['full_pool_decision_allowed']).lower()}`",
        "- fabricated_returns: `false`",
        "",
        "OR remains locked. This step does not run an OR optimizer, security-level construction, Q2, live trading, broker/order workflows, or production approval.",
        "",
        "## Unavailable Components",
        "",
    ]
    if unavailable.empty:
        lines.append("No unavailable eligible components were found.")
    else:
        for row in unavailable.itertuples(index=False):
            lines.append(
                f"- `{row.factor_id}`: `{row.unavailable_reason}`; enablement `{row.enablement_mode}`; "
                f"required data `{row.required_data}`."
            )
    lines.extend(["", "## Hard Blocked Components", ""])
    if hard.empty:
        lines.append("No hard-blocked components were present in the component pool input.")
    else:
        for row in hard.itertuples(index=False):
            lines.append(f"- `{row.factor_id}`: remains blocked; reason `{row.blocked_reason}`.")
    lines.extend(
        [
            "",
            "After any real OOS observation expansion, rerun `make multifactor-portfolio-validation` and `make multifactor-portfolio-assembly-audit`.",
            "",
        ]
    )
    return "\n".join(lines)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _report_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "family_id",
        "component_role",
        "current_status",
        "component_status",
        "filter_class",
        "unavailable_reason",
        "required_data",
        "required_signal_builder",
        "required_timestamp_policy",
        "reporting_lag_days",
        "fundamental_reporting_lag_ok",
        "event_visibility_required",
        "horizon_type",
        "rebalance_frequency",
        "oos_observation_count",
        "can_enable_now",
        "enablement_mode",
        "blocked_reason",
        "fabricated_returns",
        "not_alpha_evidence",
        "production_approval",
        "live_trading",
        "security_orders",
        "direct_q2_entry",
        "or_optimizer_unlocked",
    ]
