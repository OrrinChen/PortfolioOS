"""FactorSpec generation for the Factor Discovery Sandbox."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

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
    lookback = _lookback_from_factor_name(factor_name)
    family = _correlation_family(factor_name)
    direction = _direction(factor_name)
    return {
        "schema_version": "factor_spec.v1",
        "factor_id": factor_name,
        "mechanism": _mechanism(family),
        "lookback": lookback,
        "skip": 1,
        "direction": direction,
        "signal_timestamp": "month_end_close",
        "visibility_timestamp": "after_month_end_close",
        "tradable_timestamp": "next_rebalance_session",
        "coverage_rule": {
            "minimum_observations": max(lookback + 1, 2),
            "insufficient_coverage": "explicit_abstain",
            "abstain_reason": "insufficient_price_volume_history",
        },
        "expected_horizon": "next_month_excess_return",
        "known_correlation_family": family,
        "known_failure_mode": _failure_mode(family),
        "no_view_is_not_zero_alpha": True,
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
    if factor_name.startswith("volatility_") or factor_name.startswith("drawdown_"):
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
        "no_view_is_not_zero_alpha",
    }
    valid_specs = []
    for spec in specs:
        coverage_rule = spec.get("coverage_rule", {})
        valid_specs.append(
            required_keys.issubset(spec)
            and coverage_rule.get("insufficient_coverage") == "explicit_abstain"
            and spec.get("no_view_is_not_zero_alpha") is True
            and spec.get("visibility_timestamp") != spec.get("tradable_timestamp")
        )
    return {
        "schema_version": "factor_spec_validation.v1",
        "factor_count": len(specs),
        "factors": [str(spec["factor_id"]) for spec in specs],
        "all_specs_valid": all(valid_specs),
        "timestamp_contract_complete": all(
            all(key in spec for key in ["signal_timestamp", "visibility_timestamp", "tradable_timestamp"])
            for spec in specs
        ),
        "insufficient_coverage_policy": "explicit_abstain",
        "no_view_is_not_zero_alpha": all(spec.get("no_view_is_not_zero_alpha") is True for spec in specs),
        "production_approval_claimed": False,
    }
