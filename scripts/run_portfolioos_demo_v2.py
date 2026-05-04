#!/usr/bin/env python3
"""Run the deterministic local PortfolioOS typed-alpha demo v2."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
for path in (
    REPO_ROOT / "src",
    REPO_ROOT / "projects" / "typed_alpha_pilot" / "src",
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
    REPO_ROOT / "projects" / "execution_aware_optimizer" / "src",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from portfolio_os.dashboard import render_typed_alpha_dashboard  # noqa: E402
from portfolio_os.alpha.schema_versions import (  # noqa: E402
    TYPED_ALPHA_RELEASE_MANIFEST_SCHEMA_VERSION,
    TYPED_ALPHA_SCHEMA_VERSIONS,
)
from portfolio_os.paper.overlay_readiness import (  # noqa: E402
    assess_paper_overlay_readiness,
    write_paper_overlay_readiness_artifacts,
)
from typed_alpha_pilot import run_sue_typed_alpha_pilot  # noqa: E402


SUE_ALPHA_VIEW = REPO_ROOT / "projects" / "alpha_view_contract" / "examples" / "event_sue_pead_view.json"
VALID_BUNDLE = REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local typed-alpha PortfolioOS demo v2.")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "demo_v2"))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    run_sue_typed_alpha_pilot(
        output_dir=output_dir,
        alpha_view_path=SUE_ALPHA_VIEW,
        evidence_bundle_path=VALID_BUNDLE,
    )
    paper_result = assess_paper_overlay_readiness(
        observations=_demo_paper_observations(),
        requested_sample_count=2,
        max_validated_participation_rate=0.001,
    )
    write_paper_overlay_readiness_artifacts(paper_result, output_dir)
    render_typed_alpha_dashboard(artifact_root=output_dir, output_path=output_dir / "dashboard_v2.html")
    _write_release_manifest(output_dir)

    print(f"portfolioos_demo_v2: {output_dir}")


def _demo_paper_observations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "spread_bps": 2.0,
                "half_spread_bps": 1.0,
                "drift_bps": 0.5,
                "drift_vs_half_spread": 0.5,
                "capture_to_submit_latency_seconds": 3.0,
                "time_of_day_bucket": "09:30-10:29",
            },
            {
                "ticker": "SPY",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "spread_bps": 4.0,
                "half_spread_bps": 2.0,
                "drift_bps": -0.4,
                "drift_vs_half_spread": -0.2,
                "capture_to_submit_latency_seconds": 8.0,
                "time_of_day_bucket": "14:30-16:00",
            },
        ]
    )


def _write_release_manifest(output_dir: Path) -> Path:
    artifact_files = sorted(path.name for path in output_dir.iterdir() if path.is_file())
    payload: dict[str, object] = {
        "schema_version": TYPED_ALPHA_RELEASE_MANIFEST_SCHEMA_VERSION,
        "run_id": "demo_v2",
        "status": "release_candidate_local_only",
        "typed_alpha_chain": [
            "AlphaView",
            "Event Evidence",
            "Projection Manifest",
            "Promotion Gate v2",
            "Q2 Typed Matrix",
            "Paper Overlay Readiness",
            "Demo v2 Dashboard",
        ],
        "schema_versions": TYPED_ALPHA_SCHEMA_VERSIONS,
        "artifact_files": artifact_files,
        "production_alpha_approved": False,
        "live_trading_enabled": False,
        "broker_routes_enabled": False,
        "orders_generated": False,
        "realized_alpha_performance_claimed": False,
    }
    payload["content_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    destination = output_dir / "typed_alpha_release_manifest.json"
    destination.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


if __name__ == "__main__":
    main()
