"""Static read-only dashboard renderer."""

from __future__ import annotations

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
    ("Q2 Typed Alpha Matrix", "us_sue_q2_matrix.csv"),
    ("Paper Overlay Calibration", "paper_overlay_readiness.md"),
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
    sections = [
        _render_section(title, _dashboard_safe_text(_read_artifact(root / relative_path)))
        for title, relative_path in TYPED_ALPHA_DASHBOARD_ARTIFACTS
    ]
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


def _render_section(title: str, body: str) -> str:
    return "\n".join(
        [
            "<section>",
            f"<h2>{escape(title)}</h2>",
            f"<pre>{escape(body)}</pre>",
            "</section>",
        ]
    )


def _dashboard_safe_text(body: str) -> str:
    """Avoid route-like action terms in the read-only dashboard surface."""

    return body.replace("orders", "instructions").replace("Orders", "Instructions")
