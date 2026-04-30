"""Cost-sensitivity planning adapters for Q2."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import LadderResultRow


class CostSensitivityScenario(BaseModel):
    """One non-mutating cost-assumption scenario."""

    cost_bps: int
    config: ExperimentConfig
    portfolioos_overrides: dict[str, float | str]


class CostSensitivityResultRow(LadderResultRow):
    """One cost-sensitivity result row with its cost assumption attached."""

    cost_bps: int
    transaction_cost_objective_mode: str | None = None
    portfolioos_overrides: dict[str, Any] = Field(default_factory=dict)


def _cost_bps_label(cost_bps: int) -> str:
    """Return a stable path label for one cost level."""

    return f"cost_{int(cost_bps)}bps"


def build_portfolioos_cost_overrides(
    *,
    cost_bps: int,
    transaction_cost_objective_mode: str,
) -> dict[str, float | str]:
    """Build explicit PortfolioOS config overrides for one cost assumption.

    The overrides are returned as data, not applied globally. A future execution
    adapter can write these into a derived manifest/config inside a run-specific
    output directory.
    """

    bps = float(cost_bps)
    return {
        "fees.commission_rate": bps / 10000.0,
        "execution.backtest_fixed_half_spread_bps": bps,
        "objective_weights.transaction_cost_objective_mode": transaction_cost_objective_mode,
    }


def build_cost_sensitivity_scenarios(config: ExperimentConfig) -> list[CostSensitivityScenario]:
    """Clone the base experiment config once per cost level."""

    scenarios: list[CostSensitivityScenario] = []
    base_output_dir = Path(config.portfolioos.output_dir)
    for raw_cost_bps in config.cost_sensitivity_bps:
        cost_bps = int(raw_cost_bps)
        scenario_config = config.model_copy(deep=True)
        scenario_config.portfolioos.cost_assumption_bps = float(cost_bps)
        scenario_config.portfolioos.output_dir = str(base_output_dir / _cost_bps_label(cost_bps))
        scenarios.append(
            CostSensitivityScenario(
                cost_bps=cost_bps,
                config=scenario_config,
                portfolioos_overrides=build_portfolioos_cost_overrides(
                    cost_bps=cost_bps,
                    transaction_cost_objective_mode=config.portfolioos.transaction_cost_objective_mode,
                ),
            )
        )
    return scenarios


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _parse_literal(value: object) -> object:
    if _is_missing(value):
        return None
    if not isinstance(value, str):
        return value
    return ast.literal_eval(value)


def _parse_list(value: object) -> list[str]:
    parsed = _parse_literal(value)
    if parsed is None:
        return []
    if not isinstance(parsed, list):
        raise ValueError(f"Expected list value, got {type(parsed).__name__}")
    return [str(item) for item in parsed]


def _parse_dict(value: object) -> dict[str, Any]:
    parsed = _parse_literal(value)
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected dict value, got {type(parsed).__name__}")
    return {str(key): item for key, item in parsed.items()}


def _optional_value(value: object) -> object | None:
    return None if _is_missing(value) else value


def _row_to_cost_result(row: pd.Series) -> CostSensitivityResultRow:
    payload = {
        "layer_name": row["layer_name"],
        "date": _optional_value(row.get("date")),
        "gross_return": _optional_value(row.get("gross_return")),
        "net_return": _optional_value(row.get("net_return")),
        "turnover": _optional_value(row.get("turnover")),
        "estimated_transaction_cost": _optional_value(row.get("estimated_transaction_cost")),
        "realized_transaction_cost": _optional_value(row.get("realized_transaction_cost")),
        "num_positions": _optional_value(row.get("num_positions")),
        "max_position_weight": _optional_value(row.get("max_position_weight")),
        "cash_weight": _optional_value(row.get("cash_weight")),
        "sector_exposure": _parse_dict(row.get("sector_exposure")),
        "risk_exposure": _parse_dict(row.get("risk_exposure")),
        "rejected_symbols": _parse_list(row.get("rejected_symbols")),
        "binding_constraints": _parse_list(row.get("binding_constraints")),
        "infeasibility_reason": _optional_value(row.get("infeasibility_reason")),
        "cost_bps": int(row["cost_bps"]),
        "transaction_cost_objective_mode": _optional_value(row.get("transaction_cost_objective_mode")),
        "portfolioos_overrides": _parse_dict(row.get("portfolioos_overrides")),
    }
    return CostSensitivityResultRow.model_validate(payload)


def load_cost_sensitivity_results(path: str | Path) -> list[CostSensitivityResultRow]:
    """Load cost-sensitivity CSV rows without executing PortfolioOS workflows."""

    input_path = Path(path)
    frame = pd.read_csv(input_path)
    required_columns = {"layer_name", "cost_bps"}
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"Cost-sensitivity CSV missing required columns: {sorted(missing_columns)}")
    return [_row_to_cost_result(row) for _, row in frame.iterrows()]
