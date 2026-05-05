"""Build Alpha Registry v2 local artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from portfolio_os.alpha.registry_v2 import (
    build_default_alpha_registry_v2,
    write_alpha_registry_v2_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Alpha Registry v2 artifacts.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "alpha_registry_v2"),
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "alpha_registry_report.md"),
    )
    args = parser.parse_args()

    registry = build_default_alpha_registry_v2()
    artifacts = write_alpha_registry_v2_artifacts(
        registry,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"registry_id={registry.registry_id}")
    print(f"entry_count={len(registry.entries)}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
