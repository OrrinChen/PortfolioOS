"""Rescue SUE IBES/CRSP links with exact-CUSIP CRSP stocknames matches."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_linkage_rescue import (  # noqa: E402
    load_sue_linkage_rescue_config,
    rescue_sue_links_from_crsp_stocknames,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rescue SUE IBES/CRSP links from CRSP stocknames.")
    parser.add_argument("--config", default="configs/wrds_sue_linkage_rescue.yaml")
    args = parser.parse_args()

    import wrds

    config = load_sue_linkage_rescue_config(args.config)
    connection = wrds.Connection()
    try:
        result = rescue_sue_links_from_crsp_stocknames(config, connection=connection)
    finally:
        connection.close()
    print(f"status={result['status']}")
    print(f"failed_event_rows={result['failed_event_rows']}")
    print(f"rescued_event_rows={result['rescued_event_rows']}")
    print(f"rescued_symbols={result['rescued_symbols']}")
    print(f"rescued_permnos={result['rescued_permnos']}")
    print(f"combined_link_rows={result['combined_link_rows']}")
    print("ticker_only_matching_used=False")
    print("production_approval_claimed=False")
    print(f"output_links_path={result['output_links_path']}")
    print(f"rescue_report_path={config.rescue_report_path}")


if __name__ == "__main__":
    main()
