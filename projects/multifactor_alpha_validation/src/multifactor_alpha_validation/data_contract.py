from __future__ import annotations

from dataclasses import dataclass

from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class PITValidationResult:
    factor_id: str
    pit_passed: bool
    reasons: tuple[str, ...]


def validate_pit_contract(spec: FactorSpec) -> PITValidationResult:
    reasons: list[str] = []
    contract = spec.pit_contract
    if not contract.signal_timestamp_rule:
        reasons.append("missing signal timestamp rule")
    if not contract.visibility_timestamp_rule:
        reasons.append("missing visibility timestamp rule")
    if not contract.tradable_timestamp_rule:
        reasons.append("missing tradable timestamp rule")
    if spec.coverage.missing_policy != "explicit_abstain":
        reasons.append("missing coverage is not explicit abstain")
    if spec.data_tier == "tier_2_fundamental" and contract.reporting_lag_days < 45:
        reasons.append("fundamental reporting lag is too short")
    if spec.factor_id == "analyst_revision_disabled" and spec.status != "disabled":
        reasons.append("analyst revision is enabled without PIT source")
    return PITValidationResult(
        factor_id=spec.factor_id,
        pit_passed=not reasons,
        reasons=tuple(reasons),
    )

