"""FactorSpec generation for the Factor Discovery Sandbox."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from .factor_design import build_design_contract, validate_design_contract
from .factor_formula_registry import FACTOR_FORMULA_REGISTRY, FORMULA_VERSION, required_lookback_days
from .teaching_baseline import FACTOR_NAMES


def write_price_volume_factor_specs(spec_dir: str | Path, validation_path: str | Path) -> dict[str, object]:
    """Write the 29 price-volume FactorSpec YAML files and validation summary."""

    spec_path = Path(spec_dir)
    spec_path.mkdir(parents=True, exist_ok=True)
    specs = [_build_factor_spec(factor_name) for factor_name in FACTOR_NAMES]
    for spec in specs:
        path = spec_path / f"{spec['factor_id']}.yaml"
        path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")

    validation = _build_validation_summary(specs)
    output_path = Path(validation_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return validation


def _build_factor_spec(factor_name: str) -> dict[str, object]:
    formula_spec = FACTOR_FORMULA_REGISTRY[factor_name]
    lookback = _lookback_from_factor_name(factor_name)
    family = formula_spec.mechanism_family
    direction = _direction(factor_name)
    required_days = required_lookback_days(factor_name)
    return {
        "schema_version": "factor_spec.v2",
        "factor_id": factor_name,
        "formula_version": FORMULA_VERSION,
        "formula_hash": formula_spec.formula_hash,
        "formula_summary": formula_spec.formula_summary,
        "design_contract": build_design_contract(factor_name, family),
        "design_review_required": True,
        "pre_formula_evidence_required": True,
        "formula_is_measurement_not_thesis": True,
        "raw_value_definition": formula_spec.raw_value_definition,
        "oriented_score_definition": formula_spec.oriented_score_definition,
        "required_inputs": formula_spec.required_inputs,
        "fallback_policy": formula_spec.fallback_policy,
        "fallback_audit_required": True,
        "duplicate_cluster_audit_required": True,
        "mechanism": _mechanism(family),
        "mechanism_family": family,
        "lookback": lookback,
        "required_lookback_days": required_days,
        "skip": 1,
        "direction": direction,
        "signal_timestamp": "month_end_close",
        "visibility_timestamp": "after_month_end_close",
        "tradable_timestamp": "next_rebalance_session",
        "timestamp_contract": {
            "lookback_start": "signal_timestamp - skip_days - required_lookback_days",
            "lookback_end": "last_trading_day <= signal_timestamp - skip_days",
            "signal_timestamp": "month_end_close",
            "visibility_timestamp": "after_month_end_close",
            "tradable_timestamp": "next_rebalance_session",
            "allow_same_close_trading": False,
        },
        "coverage_rule": {
            "minimum_observations": max(required_days + 1, 2),
            "insufficient_coverage": "explicit_abstain",
            "abstain_reason": "insufficient_price_volume_history",
        },
        "no_view_policy": "explicit_abstain",
        "expected_horizon": "next_month_excess_return",
        "known_correlation_family": family,
        "known_failure_mode": _failure_mode(family),
        "no_view_is_not_zero_alpha": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
    }


def _lookback_from_factor_name(factor_name: str) -> int:
    raw = factor_name.rsplit("_", maxsplit=1)[-1]
    if raw.endswith("m"):
        return int(raw[:-1])
    return int(raw)


def _correlation_family(factor_name: str) -> str:
    if "momentum" in factor_name and "volume" not in factor_name and "residual" not in factor_name:
        return "price_momentum"
    if "reversal" in factor_name:
        return "short_reversal"
    if "volatility" in factor_name or "range" in factor_name:
        return "risk_volatility"
    if "liquidity" in factor_name or "turnover" in factor_name or "volume" in factor_name:
        return "liquidity_volume"
    if "high" in factor_name or "drawdown" in factor_name:
        return "price_position"
    if "trend" in factor_name or "ema" in factor_name:
        return "trend_following"
    if "residual" in factor_name:
        return "residual_momentum"
    return "price_volume_other"


def _direction(factor_name: str) -> str:
    if factor_name.startswith("volatility_") or factor_name.startswith("drawdown_") or factor_name.startswith("range_"):
        return "negative"
    return "positive"


def _mechanism(family: str) -> str:
    mechanisms = {
        "price_momentum": "Recent winners may continue if price trend captures persistent demand.",
        "short_reversal": "Short-term overshoot may mean-revert after temporary pressure.",
        "risk_volatility": "Lower realized volatility may proxy for more stable risk-adjusted demand.",
        "liquidity_volume": "Trading activity may proxy for attention, capacity, and liquidity regime.",
        "price_position": "Distance from recent highs and drawdowns may capture trend quality and fragility.",
        "trend_following": "Smoothed price trend may capture persistent directional pressure.",
        "trend_quality": "OLS trend quality separates smooth directional pressure from endpoint return.",
        "path_fragility": "Peak-to-trough drawdown captures path damage rather than only current distance from highs.",
        "overshoot_reversal": "Vol-adjusted reversal only activates after a same-direction prior-trend extension.",
        "liquidity_shock": "Recent dollar-volume shocks may capture attention or liquidity regime changes.",
        "capacity_level": "Persistent dollar-volume level is a capacity proxy rather than a short-term shock.",
        "turnover_shock": "Abnormal turnover z-score captures short-term trading activity shocks.",
        "turnover_trend": "Turnover trend persistence captures steady improvement or deterioration in trading activity.",
        "sector_neutral_residual_momentum": "Same-sector residual momentum removes cross-sectional sector median momentum.",
        "residual_momentum": "Market-adjusted momentum may reduce pure benchmark beta exposure.",
    }
    return mechanisms.get(family, "Price-volume feature family for candidate discovery.")


def _failure_mode(family: str) -> str:
    failures = {
        "price_momentum": "Can collapse in sharp reversals and may be mega-cap growth exposure.",
        "short_reversal": "Can fight persistent trends and raise turnover.",
        "risk_volatility": "Can become defensive beta rather than alpha.",
        "liquidity_volume": "Can be attention or size exposure with high cost drag.",
        "price_position": "Can duplicate momentum windows without marginal value.",
        "trend_following": "Can overfit window choice and lag regime shifts.",
        "trend_quality": "Can still correlate with momentum and may lag sharp regime shifts.",
        "path_fragility": "Can become a volatility proxy or penalize temporary dislocations that recover.",
        "overshoot_reversal": "Can fight persistent trends and can be cost-sensitive.",
        "liquidity_shock": "Can be event attention rather than durable alpha and may raise turnover.",
        "capacity_level": "Can be size/capacity exposure rather than alpha evidence.",
        "turnover_shock": "Can reflect noisy activity or news without predictive content.",
        "turnover_trend": "Can be liquidity regime exposure rather than return prediction.",
        "sector_neutral_residual_momentum": "Can degrade if sector classifications are stale or non-PIT.",
        "residual_momentum": "Can fail if residualization removes the useful exposure.",
    }
    return failures.get(family, "May be redundant with existing price-volume factors.")


def _build_validation_summary(specs: list[dict[str, object]]) -> dict[str, object]:
    required_keys = {
        "lookback",
        "skip",
        "direction",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "coverage_rule",
        "expected_horizon",
        "known_correlation_family",
        "formula_version",
        "formula_hash",
        "mechanism_family",
        "design_contract",
        "design_review_required",
        "pre_formula_evidence_required",
        "formula_is_measurement_not_thesis",
        "timestamp_contract",
        "no_view_is_not_zero_alpha",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    }
    valid_specs = []
    design_results = []
    for spec in specs:
        coverage_rule = spec.get("coverage_rule", {})
        design_result = validate_design_contract(spec)
        design_results.append(design_result)
        valid_specs.append(
            required_keys.issubset(spec)
            and coverage_rule.get("insufficient_coverage") == "explicit_abstain"
            and spec.get("no_view_is_not_zero_alpha") is True
            and spec.get("not_alpha_evidence") is True
            and spec.get("direct_q2_entry_allowed") is False
            and spec.get("formula_version") == FORMULA_VERSION
            and spec.get("visibility_timestamp") != spec.get("tradable_timestamp")
            and design_result["valid"] is True
        )
    return {
        "schema_version": "factor_spec_validation.v1",
        "formula_version": FORMULA_VERSION,
        "factor_count": len(specs),
        "factors": [str(spec["factor_id"]) for spec in specs],
        "all_specs_valid": all(valid_specs),
        "timestamp_contract_complete": all(
            all(key in spec for key in ["signal_timestamp", "visibility_timestamp", "tradable_timestamp"])
            for spec in specs
        ),
        "insufficient_coverage_policy": "explicit_abstain",
        "no_view_is_not_zero_alpha": all(spec.get("no_view_is_not_zero_alpha") is True for spec in specs),
        "design_layer_required_before_formula": True,
        "all_design_contracts_valid": all(result["valid"] is True for result in design_results),
        "formula_is_measurement_not_thesis": all(
            spec.get("formula_is_measurement_not_thesis") is True for spec in specs
        ),
        "production_approval_claimed": False,
    }
