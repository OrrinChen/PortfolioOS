"""Backtest manifest loading and path resolution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from portfolio_os.data.loaders import read_yaml
from portfolio_os.domain.errors import InputValidationError


class BacktestRebalanceConfig(BaseModel):
    """Backtest rebalance schedule settings."""

    frequency: Literal["monthly"] = "monthly"


class BacktestManifest(BaseModel):
    """Serialized backtest manifest."""

    name: str
    description: str | None = None
    returns_file: str
    market_snapshot: str
    initial_holdings: str
    target_weights: str
    reference: str
    portfolio_state: str
    config: str
    constraints: str
    execution_profile: str
    baselines: list[str] = Field(default_factory=lambda: ["naive_pro_rata", "buy_and_hold"])
    rebalance: BacktestRebalanceConfig = Field(default_factory=BacktestRebalanceConfig)


@dataclass
class LoadedBacktestManifest:
    """Backtest manifest with resolved file paths."""

    manifest_path: Path
    name: str
    description: str | None
    returns_file: Path
    market_snapshot: Path
    initial_holdings: Path
    target_weights: Path
    reference: Path
    portfolio_state: Path
    config: Path
    constraints: Path
    execution_profile: Path
    baselines: list[str]
    rebalance: BacktestRebalanceConfig


def _resolve_manifest_path(raw_path: str, *, manifest_dir: Path, cwd: Path) -> Path:
    """Resolve one manifest path relative to cwd or manifest directory."""

    candidate = Path(raw_path)
    if candidate.is_absolute():
        resolved = candidate.resolve()
        if not resolved.exists():
            raise InputValidationError(f"Backtest manifest path does not exist: {resolved}")
        return resolved

    for anchor in (cwd, manifest_dir):
        resolved = (anchor / candidate).resolve()
        if resolved.exists():
            return resolved
    raise InputValidationError(f"Backtest manifest path does not exist: {raw_path}")


def load_backtest_manifest(path: str | Path) -> LoadedBacktestManifest:
    """Load and validate a backtest manifest."""

    manifest_path = Path(path).resolve()
    payload = read_yaml(manifest_path)
    manifest = BacktestManifest.model_validate(payload)
    manifest_dir = manifest_path.parent
    cwd = Path.cwd().resolve()
    return LoadedBacktestManifest(
        manifest_path=manifest_path,
        name=manifest.name,
        description=manifest.description,
        returns_file=_resolve_manifest_path(manifest.returns_file, manifest_dir=manifest_dir, cwd=cwd),
        market_snapshot=_resolve_manifest_path(manifest.market_snapshot, manifest_dir=manifest_dir, cwd=cwd),
        initial_holdings=_resolve_manifest_path(manifest.initial_holdings, manifest_dir=manifest_dir, cwd=cwd),
        target_weights=_resolve_manifest_path(manifest.target_weights, manifest_dir=manifest_dir, cwd=cwd),
        reference=_resolve_manifest_path(manifest.reference, manifest_dir=manifest_dir, cwd=cwd),
        portfolio_state=_resolve_manifest_path(manifest.portfolio_state, manifest_dir=manifest_dir, cwd=cwd),
        config=_resolve_manifest_path(manifest.config, manifest_dir=manifest_dir, cwd=cwd),
        constraints=_resolve_manifest_path(manifest.constraints, manifest_dir=manifest_dir, cwd=cwd),
        execution_profile=_resolve_manifest_path(manifest.execution_profile, manifest_dir=manifest_dir, cwd=cwd),
        baselines=list(manifest.baselines),
        rebalance=manifest.rebalance,
    )
