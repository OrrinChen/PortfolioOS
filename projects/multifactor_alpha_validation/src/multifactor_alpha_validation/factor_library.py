from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from multifactor_alpha_validation.data_contract import validate_pit_contract
from multifactor_alpha_validation.schema import FactorSpec


def load_factor_specs(spec_dir: Path) -> list[FactorSpec]:
    specs: list[FactorSpec] = []
    for path in sorted(spec_dir.glob("*.yaml")):
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        specs.append(FactorSpec.model_validate(payload))
    return specs


def validate_factor_spec_directory(spec_dir: Path, output_dir: Path) -> dict[str, Any]:
    specs = load_factor_specs(spec_dir)
    pit_results = [validate_pit_contract(spec) for spec in specs]
    rows = [
        {
            "factor_id": spec.factor_id,
            "family_id": spec.family_id,
            "status": spec.status,
            "data_tier": spec.data_tier,
            "pit_passed": pit.pit_passed,
            "pit_reasons": list(pit.reasons),
            "missing_policy": spec.coverage.missing_policy,
            "reporting_lag_days": spec.pit_contract.reporting_lag_days,
        }
        for spec, pit in zip(specs, pit_results)
    ]
    report = {
        "schema_version": "factor_spec_validation.v1",
        "factor_count": len(specs),
        "enabled_factor_count": sum(spec.status == "enabled" for spec in specs),
        "reference_factor_count": sum(spec.status == "reference" for spec in specs),
        "disabled_factor_count": sum(spec.status == "disabled" for spec in specs),
        "all_specs_valid": all(row["pit_passed"] for row in rows),
        "factor_ids": [spec.factor_id for spec in specs],
        "rows": rows,
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "spec_validation_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report

