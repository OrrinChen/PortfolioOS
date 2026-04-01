from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_required_governance_directories_exist() -> None:
    required_dirs = [
        REPO_ROOT / "docs" / "standards",
        REPO_ROOT / "docs" / "templates",
        REPO_ROOT / "scripts" / "devtools",
        REPO_ROOT / "tests" / "contracts",
        REPO_ROOT / "tests" / "templates",
    ]
    for directory in required_dirs:
        assert directory.exists(), f"Missing required directory: {directory}"
        assert directory.is_dir(), f"Expected directory but found file: {directory}"


def test_required_engineering_files_exist() -> None:
    required_files = [
        REPO_ROOT / "docs" / "standards" / "engineering_workflow.md",
        REPO_ROOT / "docs" / "standards" / "testing_policy.md",
        REPO_ROOT / "docs" / "templates" / "task_spec_template.md",
        REPO_ROOT / "docs" / "templates" / "test_design_template.md",
        REPO_ROOT / "docs" / "templates" / "pr_checklist.md",
        REPO_ROOT / "tests" / "templates" / "contract_test_template.py",
        REPO_ROOT / "scripts" / "devtools" / "validate_engineering_setup.py",
        REPO_ROOT / "scripts" / "devtools" / "run_engineering_gate.py",
    ]
    for file_path in required_files:
        assert file_path.exists(), f"Missing required file: {file_path}"
        assert file_path.is_file(), f"Expected file but found directory: {file_path}"


def test_readme_contains_engineering_standards_entry_links() -> None:
    readme_path = REPO_ROOT / "README.md"
    content = readme_path.read_text(encoding="utf-8")
    assert "## Engineering Standards" in content
    assert "docs/standards/engineering_workflow.md" in content
    assert "docs/standards/testing_policy.md" in content
    assert "py -3 scripts/devtools/run_engineering_gate.py" in content
