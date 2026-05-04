from __future__ import annotations

import json
from pathlib import Path

from portfolio_os.dashboard import render_typed_alpha_dashboard


def test_typed_alpha_dashboard_has_first_screen_status_and_artifact_links(tmp_path: Path) -> None:
    artifact_root = tmp_path / "demo_v2"
    _write_json(
        artifact_root / "typed_alpha_release_manifest.json",
        {
            "run_id": "demo_v2",
            "status": "release_candidate_local_only",
            "schema_version": "typed_alpha_release_manifest.v1",
            "content_hash": "hash-fixture",
            "production_alpha_approved": False,
            "live_trading_enabled": False,
            "broker_routes_enabled": False,
            "typed_alpha_chain": ["AlphaView", "Event Evidence", "Projection Manifest"],
        },
    )
    _write_json(artifact_root / "us_sue_event_alpha_view.json", {"schema_version": "alpha_view.v1"})
    output_path = tmp_path / "dashboard_v2.html"

    render_typed_alpha_dashboard(artifact_root=artifact_root, output_path=output_path)
    html = output_path.read_text(encoding="utf-8")

    for phrase in (
        "Alpha status: integration benchmark only",
        "Execution status: unavailable or local paper-overlay aggregation only",
        "Trading status: no broker, no orders, no live workflow",
        "Production status: not approved",
        "Typed Alpha Chain",
        "Artifact Links",
        "Manifest Summary",
    ):
        assert phrase in html
    assert 'href="typed_alpha_release_manifest.json"' in html
    assert 'href="us_sue_event_alpha_view.json"' in html
    assert _has_no_workflow_controls(html)


def test_typed_alpha_dashboard_renders_missing_artifacts_as_unavailable(tmp_path: Path) -> None:
    output_path = tmp_path / "dashboard_v2.html"

    render_typed_alpha_dashboard(artifact_root=tmp_path / "empty", output_path=output_path)
    html = output_path.read_text(encoding="utf-8")

    assert "Artifact unavailable" in html
    assert "Missing artifacts are shown as unavailable" in html
    assert _has_no_workflow_controls(html)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _has_no_workflow_controls(html: str) -> bool:
    lowered = html.lower()
    forbidden = ("<form", "method=", "post", "/broker", "/order", "/trade", "/live", "submit")
    return all(term not in lowered for term in forbidden)
