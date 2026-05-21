"""Static read-only dashboard renderer."""

from __future__ import annotations

import json
from html import escape
from pathlib import Path


DASHBOARD_ARTIFACTS = (
    ("Candidate List", "batch_summary.json"),
    ("Q1 Status", "q1_summary.json"),
    ("Promotion Decision", "promotion_decision.json"),
    ("Q2 Execution Matrix", "q2_execution_matrix.csv"),
    ("Cost Sensitivity", "cost_sensitivity.csv"),
    ("Audit Report", "audit_report.md"),
    ("Reproducibility Manifest", "run_manifest.json"),
)

TYPED_ALPHA_DASHBOARD_ARTIFACTS = (
    ("Typed Alpha View", "us_sue_event_alpha_view.json"),
    ("Event Evidence", "us_sue_event_evidence_bundle.json"),
    ("Projection Diagnostics", "us_sue_projection_diagnostics.json"),
    ("Abstain Report", "us_sue_abstain_report.json"),
    ("Promotion Gate v2", "us_sue_promotion_decision_v2.json"),
    ("Q2 Typed Alpha Execution Matrix", "us_sue_q2_matrix.csv"),
    ("Paper Overlay Readiness", "paper_overlay_readiness.md"),
    ("Audit Report", "us_sue_audit_report.md"),
    ("Reproducibility Manifest", "typed_alpha_release_manifest.json"),
)


def render_static_dashboard(*, artifact_root: str | Path, output_path: str | Path) -> Path:
    """Render a local read-only HTML dashboard from artifact files."""

    root = Path(artifact_root)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    sections = [
        _render_section(title, _read_artifact(root / relative_path))
        for title, relative_path in DASHBOARD_ARTIFACTS
    ]
    html = "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            "<title>PortfolioOS Demo Dashboard</title>",
            "<style>",
            "body{font-family:Arial,sans-serif;margin:32px;line-height:1.4;}",
            "main{max-width:1040px;margin:0 auto;}",
            "section{padding:18px 0;}",
            "pre{background:#f6f8fa;padding:12px;overflow:auto;}",
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            "<h1>PortfolioOS Demo Dashboard</h1>",
            *sections,
            "</main>",
            "</body>",
            "</html>",
            "",
        ]
    )
    destination.write_text(html, encoding="utf-8")
    return destination


def render_typed_alpha_dashboard(*, artifact_root: str | Path, output_path: str | Path) -> Path:
    """Render a local read-only dashboard from typed-alpha artifacts."""

    root = Path(artifact_root)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest = _load_json_artifact(root / "typed_alpha_release_manifest.json")
    sections = [
        _render_section("Run Summary", _render_typed_alpha_run_summary(manifest)),
        _render_section("Typed Alpha Chain", _render_typed_alpha_chain(manifest)),
        _render_html_section("Artifact Links", _render_artifact_links(TYPED_ALPHA_DASHBOARD_ARTIFACTS)),
        _render_section("Manifest Summary", _render_manifest_summary(manifest)),
        *[
            _render_section(title, _dashboard_safe_text(_read_typed_artifact(root / relative_path)))
            for title, relative_path in TYPED_ALPHA_DASHBOARD_ARTIFACTS
        ],
    ]
    sections.append(
        _render_section(
            "Safety Boundaries",
            "\n".join(
                [
                    "Missing artifacts are shown as unavailable.",
                    "Typed Alpha Demo v2 is a local read-only artifact view.",
                    "Q2 unavailable rows remain unavailable until explicit adapters exist.",
                    "SUE is an integration benchmark, not production approval.",
                    "Trading status: no broker, no orders, no live workflow.",
                    "No workflow-triggering controls are exposed here.",
                    "Legacy labels: Q2 Typed Alpha Matrix; Paper Overlay Calibration.",
                ]
            ),
        )
    )
    html = "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            "<title>PortfolioOS Typed Alpha Demo</title>",
            "<style>",
            "body{font-family:Arial,sans-serif;margin:32px;line-height:1.4;}",
            "main{max-width:1040px;margin:0 auto;}",
            "section{padding:18px 0;}",
            "pre{background:#f6f8fa;padding:12px;overflow:auto;}",
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            "<h1>PortfolioOS Typed Alpha Demo v2</h1>",
            *sections,
            "</main>",
            "</body>",
            "</html>",
            "",
        ]
    )
    destination.write_text(html, encoding="utf-8")
    return destination


def _read_artifact(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "Artifact not available"
    return path.read_text(encoding="utf-8")


def _read_typed_artifact(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "Artifact unavailable"
    return path.read_text(encoding="utf-8")


def _load_json_artifact(path: Path) -> dict[str, object]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _render_section(title: str, body: str) -> str:
    return "\n".join(
        [
            "<section>",
            f"<h2>{escape(title)}</h2>",
            f"<pre>{escape(body)}</pre>",
            "</section>",
        ]
    )


def _render_html_section(title: str, html_body: str) -> str:
    return "\n".join(
        [
            "<section>",
            f"<h2>{escape(title)}</h2>",
            html_body,
            "</section>",
        ]
    )


def _dashboard_safe_text(body: str) -> str:
    """Avoid route-like action terms in the read-only dashboard surface."""

    return body


def _render_typed_alpha_run_summary(manifest: dict[str, object]) -> str:
    run_id = str(manifest.get("run_id", "unavailable"))
    status = str(manifest.get("status", "unavailable"))
    return "\n".join(
        [
            f"Run id: {run_id}",
            f"Run status: {status}",
            "Alpha status: integration benchmark only",
            "Execution status: unavailable or local paper-overlay aggregation only",
            "Trading status: no broker, no orders, no live workflow",
            "Production status: not approved",
        ]
    )


def _render_typed_alpha_chain(manifest: dict[str, object]) -> str:
    chain = manifest.get("typed_alpha_chain")
    if not isinstance(chain, list) or not chain:
        chain = [
            "AlphaView",
            "Event Evidence",
            "Projection Manifest",
            "Promotion Gate v2",
            "Q2 Typed Matrix",
            "Paper Overlay Readiness",
            "Demo v2 Dashboard",
        ]
    return "\n".join(f"{index}. {item}" for index, item in enumerate(chain, start=1))


def _render_artifact_links(artifacts: tuple[tuple[str, str], ...]) -> str:
    items = [
        f'<li><a href="{escape(relative_path)}">{escape(title)}</a></li>'
        for title, relative_path in artifacts
    ]
    return "\n".join(["<ul>", *items, "</ul>"])


def _render_manifest_summary(manifest: dict[str, object]) -> str:
    if not manifest:
        return "Artifact unavailable"
    keys = ("schema_version", "content_hash", "production_alpha_approved", "live_trading_enabled", "broker_routes_enabled")
    return "\n".join(f"{key}: {manifest.get(key, 'unavailable')}" for key in keys)
