"""Test path setup for the standalone audit-report project."""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PATHS = [
    REPO_ROOT / "src",
    PROJECT_ROOT / "src",
    REPO_ROOT / "projects" / "agentic_alpha_triage" / "src",
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
    REPO_ROOT / "projects" / "execution_aware_optimizer" / "src",
]

for project_path in PROJECT_PATHS:
    if str(project_path) not in sys.path:
        sys.path.insert(0, str(project_path))
