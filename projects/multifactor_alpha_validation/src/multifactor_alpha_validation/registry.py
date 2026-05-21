from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.allocator import AllocatorResult
from multifactor_alpha_validation.cost_capacity import SurvivalResult
from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class FactorRegistryResult:
    registry: dict[str, object]
    decision_table: pd.DataFrame


def build_factor_registry(
    specs: list[FactorSpec],
    allocator: AllocatorResult,
    survival: SurvivalResult,
) -> FactorRegistryResult:
    weights = allocator.factor_weights.set_index("factor_id").to_dict("index")
    failure = survival.failure_attribution
    rows: list[dict[str, object]] = []
    for spec in specs:
        weight_row = weights.get(spec.factor_id)
        if spec.status == "disabled":
            final_status = "pit_rejected"
            stop_layer = "pit_unavailable"
            zero_reason = "no_view"
            weight = 0.0
        elif weight_row and float(weight_row["weight"]) > 0:
            final_status = _positive_status(spec.factor_id, survival)
            stop_layer = str(failure[spec.factor_id]["stop_layer"])
            zero_reason = ""
            weight = float(weight_row["weight"])
        else:
            zero_reason = str(weight_row["zero_weight_reason"]) if weight_row else "no_view"
            final_status = _zero_status(zero_reason)
            stop_layer = str(failure.get(spec.factor_id, {}).get("stop_layer", "allocator_zero"))
            weight = 0.0
        rows.append(
            {
                "schema_version": "factor_registry.v1",
                "factor_id": spec.factor_id,
                "family_id": spec.family_id,
                "status": spec.status,
                "final_status": final_status,
                "stop_layer": stop_layer,
                "allocator_weight": round(weight, 8),
                "zero_weight_reason": zero_reason,
                "production_approval": False,
                "live_trading": False,
                "direct_q2_entry": False,
            }
        )
    table = pd.DataFrame(rows)
    registry = {
        "schema_version": "factor_registry.v1",
        "factor_count": int(len(table)),
        "statuses": table[["factor_id", "final_status", "stop_layer"]].to_dict("records"),
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }
    return FactorRegistryResult(registry=registry, decision_table=table)


def write_factor_registry(result: FactorRegistryResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "factor_registry.yaml").write_text(
        yaml.safe_dump(result.registry, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "factor_registry.json").write_text(
        json.dumps(result.registry, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result.decision_table.to_csv(output_dir / "factor_decision_table.csv", index=False)
    return ["factor_registry.yaml", "factor_registry.json", "factor_decision_table.csv"]


def _positive_status(factor_id: str, survival: SurvivalResult) -> str:
    base = survival.cost_stress_matrix[
        (survival.cost_stress_matrix["factor_id"] == factor_id)
        & (survival.cost_stress_matrix["cost_scenario"] == "spread_base")
    ]
    if not base.empty and str(base.iloc[0]["cost_survival_status"]) == "cost_survived":
        return "cost_survived"
    return "cost_killed"


def _zero_status(reason: str) -> str:
    return {
        "high_redundancy": "allocator_weight_zero_redundancy",
        "cluster_dominated": "allocator_weight_zero_redundancy",
        "high_cost_drag": "allocator_weight_zero_cost",
        "high_turnover": "allocator_weight_zero_cost",
        "capacity_limited": "allocator_weight_zero_capacity",
        "low_posterior_alpha": "allocator_weight_zero_low_confidence",
        "insufficient_evidence": "q1_diagnostic_only",
        "no_view": "q1_diagnostic_only",
    }.get(reason, "survival_inconclusive")
