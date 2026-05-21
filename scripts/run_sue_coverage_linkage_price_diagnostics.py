"""Run SUE coverage/linkage/price diagnostics from local H1C artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_coverage_diagnostics import (  # noqa: E402
    build_sue_coverage_linkage_price_diagnostics,
    load_sue_coverage_diagnostics_config,
    write_sue_coverage_linkage_price_diagnostics_artifacts,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose SUE expanded-panel coverage, linkage, and price loss.")
    parser.add_argument("--config", default="configs/sue_coverage_linkage_price_diagnostics.yaml")
    args = parser.parse_args()

    config = load_sue_coverage_diagnostics_config(args.config)
    result = build_sue_coverage_linkage_price_diagnostics(config)
    artifacts = write_sue_coverage_linkage_price_diagnostics_artifacts(result)
    summary = result.diagnostic_summary
    print("status=completed")
    print(f"event_count={summary['event_count']}")
    print(f"final_pit_safe_rows={summary['final_pit_safe_rows']}")
    print(f"unlinked_ibes_crsp_rows={summary['unlinked_ibes_crsp_rows']}")
    print(f"missing_price_rows={summary['missing_price_rows']}")
    print(f"missing_return_windows={summary['missing_return_windows']}")
    print(f"recommended_next_action={summary['recommended_next_action']}")
    print(f"diagnostic_summary={artifacts['diagnostic_summary']}")
    print(f"report={artifacts['report']}")


if __name__ == "__main__":
    main()
