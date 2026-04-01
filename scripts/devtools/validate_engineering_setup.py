from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]

REQUIRED_DIRECTORIES = (
    REPO_ROOT / "docs" / "standards",
    REPO_ROOT / "docs" / "templates",
    REPO_ROOT / "scripts" / "devtools",
    REPO_ROOT / "tests" / "contracts",
    REPO_ROOT / "tests" / "templates",
)

REQUIRED_FILES = (
    REPO_ROOT / "docs" / "standards" / "engineering_workflow.md",
    REPO_ROOT / "docs" / "standards" / "testing_policy.md",
    REPO_ROOT / "docs" / "templates" / "task_spec_template.md",
    REPO_ROOT / "docs" / "templates" / "test_design_template.md",
    REPO_ROOT / "docs" / "templates" / "pr_checklist.md",
    REPO_ROOT / "tests" / "templates" / "contract_test_template.py",
    REPO_ROOT / "scripts" / "devtools" / "run_engineering_gate.py",
)

README_REQUIRED_LINKS = (
    "docs/standards/engineering_workflow.md",
    "docs/standards/testing_policy.md",
)


def _missing_directories() -> list[Path]:
    return [path for path in REQUIRED_DIRECTORIES if not path.is_dir()]


def _missing_files() -> list[Path]:
    return [path for path in REQUIRED_FILES if not path.is_file()]


def _missing_readme_links() -> list[str]:
    readme_content = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    return [link for link in README_REQUIRED_LINKS if link not in readme_content]


def main() -> int:
    missing_dirs = _missing_directories()
    missing_files = _missing_files()
    missing_links = _missing_readme_links()
    if not missing_dirs and not missing_files and not missing_links:
        print("Engineering setup validation passed.")
        return 0

    if missing_dirs:
        print("Missing directories:")
        for path in missing_dirs:
            print(f"- {path}")

    if missing_files:
        print("Missing files:")
        for path in missing_files:
            print(f"- {path}")

    if missing_links:
        print("Missing README links:")
        for link in missing_links:
            print(f"- {link}")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
