"""Execution calibration profiles and resolved simulation defaults."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from portfolio_os.domain.errors import InputValidationError


class ExecutionCurveBucket(BaseModel):
    """One intraday liquidity bucket."""

    label: str
    volume_share: float = Field(gt=0.0, le=1.0)
    slippage_multiplier: float = Field(gt=0.0)


class ExecutionMarketCurve(BaseModel):
    """Intraday market-volume curve used by the execution simulator."""

    buckets: list[ExecutionCurveBucket] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_volume_share_sum(self) -> "ExecutionMarketCurve":
        """Require the bucket shares to sum to one."""

        total_share = sum(bucket.volume_share for bucket in self.buckets)
        if abs(total_share - 1.0) > 1e-9:
            raise ValueError(
                f"market_curve volume_share must sum to 1.0, received {total_share:.6f}."
            )
        return self


class CalibrationDefaults(BaseModel):
    """Default simulation toggles bundled with a calibration profile."""

    participation_limit: float | None = Field(default=None, ge=0.0, le=1.0)
    volume_shock_multiplier: float = Field(default=1.0, gt=0.0)
    allow_partial_fill: bool | None = None
    force_completion: bool | None = None


class CalibrationProfile(BaseModel):
    """Declarative execution calibration profile."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    market_curve: ExecutionMarketCurve
    defaults: CalibrationDefaults = Field(default_factory=CalibrationDefaults)


def load_calibration_profile(path: str | Path) -> CalibrationProfile:
    """Load and validate one execution calibration profile."""

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise InputValidationError(f"Expected a YAML mapping in calibration profile {path}.")
    try:
        return CalibrationProfile.model_validate(payload)
    except ValidationError as exc:
        raise InputValidationError(f"Invalid calibration profile: {exc}") from exc


def resolve_optional_path(path_text: str, *, anchors: list[Path]) -> Path:
    """Resolve a possibly relative path against a list of anchors."""

    raw_path = Path(path_text)
    if raw_path.is_absolute():
        return raw_path.resolve()
    candidates = [(anchor / raw_path).resolve() for anchor in anchors]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def default_calibration_profile_path() -> Path:
    """Return the built-in fallback calibration profile path."""

    return (Path.cwd().resolve() / "config" / "calibration_profiles" / "balanced_day.yaml").resolve()


def build_resolved_calibration_payload(
    *,
    execution_profile_defaults: dict[str, Any],
    selected_profile_path: Path,
    selected_profile: CalibrationProfile,
    selected_profile_source: str,
    overridden_fields: list[str],
    resolved_curve: ExecutionMarketCurve,
    resolved_allow_partial_fill: bool,
    resolved_force_completion: bool,
    resolved_default_participation_limit: float,
    resolved_volume_shock_multiplier: float,
) -> dict[str, Any]:
    """Build a JSON-serializable resolved-calibration payload."""

    return {
        "execution_profile_defaults": execution_profile_defaults,
        "selected_profile": {
            "name": selected_profile.name,
            "description": selected_profile.description,
            "path": str(selected_profile_path),
            "source": selected_profile_source,
        },
        "overridden_fields": overridden_fields,
        "resolved_market_curve": resolved_curve.model_dump(mode="json"),
        "resolved_simulation_defaults": {
            "allow_partial_fill": resolved_allow_partial_fill,
            "force_completion": resolved_force_completion,
            "participation_limit": resolved_default_participation_limit,
            "volume_shock_multiplier": resolved_volume_shock_multiplier,
        },
    }
