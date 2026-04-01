from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_step(label: str, command: list[str]) -> int:
    print(f"[engineering-gate] {label}")
    print(f"[engineering-gate] command: {' '.join(command)}")
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if completed.returncode != 0:
        print(f"[engineering-gate] failed: {label} (exit={completed.returncode})")
        return completed.returncode
    print(f"[engineering-gate] passed: {label}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run TDD/SDD engineering setup checks and contract gates."
    )
    parser.add_argument(
        "--full-pytest",
        action="store_true",
        help="Also run full pytest suite after engineering setup gates.",
    )
    args = parser.parse_args()

    steps: list[tuple[str, list[str]]] = [
        (
            "validate engineering setup",
            [sys.executable, str(REPO_ROOT / "scripts" / "devtools" / "validate_engineering_setup.py")],
        ),
        (
            "repository structure contract tests",
            [sys.executable, "-m", "pytest", "-q", "tests/contracts/test_repo_structure.py"],
        ),
    ]

    if args.full_pytest:
        steps.append(("full test suite", [sys.executable, "-m", "pytest", "-q"]))

    for label, command in steps:
        exit_code = _run_step(label, command)
        if exit_code != 0:
            return exit_code

    print("[engineering-gate] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

