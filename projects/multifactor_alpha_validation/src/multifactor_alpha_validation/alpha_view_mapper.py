from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from multifactor_alpha_validation.schema import FactorSpec


def map_signal_panel_to_alpha_view(spec: FactorSpec, panel: pd.DataFrame) -> dict[str, Any]:
    active_rows = panel[panel["coverage_flag"] == True]  # noqa: E712
    abstain_rows = panel[panel["coverage_flag"] == False]  # noqa: E712
    view_type = "event_reference" if spec.horizon.horizon_type == "event_window" else "fixed_horizon"
    return {
        "schema_version": "alpha_view_compatible.v1",
        "factor_id": spec.factor_id,
        "family_id": spec.family_id,
        "view_type": view_type,
        "status": spec.status,
        "horizon": {
            "horizon_type": spec.horizon.horizon_type,
            "holding_days": spec.horizon.holding_days,
            "rebalance_frequency": spec.horizon.rebalance_frequency,
        },
        "timestamp_contract": {
            "signal_timestamp_rule": spec.pit_contract.signal_timestamp_rule,
            "visibility_timestamp_rule": spec.pit_contract.visibility_timestamp_rule,
            "tradable_timestamp_rule": spec.pit_contract.tradable_timestamp_rule,
        },
        "active_view_count": int(len(active_rows)),
        "abstain_count": int(len(abstain_rows)),
        "no_view_is_not_zero_alpha": True,
        "entries": [
            {
                "asset_id": row.asset_id,
                "date": row.date,
                "expected_return_signal": float(row.normalized_signal),
                "signal_timestamp": row.signal_timestamp,
                "visibility_timestamp": row.visibility_timestamp,
                "tradable_timestamp": row.tradable_timestamp,
                "horizon_start": row.horizon_start,
                "horizon_end": row.horizon_end,
                "view_status": "active_view",
            }
            for row in active_rows.itertuples(index=False)
        ],
        "abstains": [
            {
                "asset_id": row.asset_id,
                "date": row.date,
                "reason": row.abstain_reason,
                "view_status": "no_view",
            }
            for row in abstain_rows.itertuples(index=False)
        ],
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }


def write_alpha_view_outputs(specs: list[FactorSpec], signal_panels: dict[str, pd.DataFrame], output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    spec_map = {spec.factor_id: spec for spec in specs}
    for factor_id, panel in sorted(signal_panels.items()):
        view = map_signal_panel_to_alpha_view(spec_map[factor_id], panel)
        filename = f"alpha_view_{factor_id}.json"
        (output_dir / filename).write_text(json.dumps(view, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(filename)
    return written
