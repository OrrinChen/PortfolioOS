"""Extract CRSP daily prices for the WRDS SUE panel local cache."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_crsp_price_extract import (  # noqa: E402
    SueCrspPriceExtractConfig,
    extract_crsp_prices_for_sue_links,
    load_sue_crsp_price_extract_config,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract resumable CRSP daily prices for SUE linked PERMNOs.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--links-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--chunk-dir", default=None)
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--chunk-size", type=int, default=None)
    parser.add_argument("--max-permnos", type=int, default=None)
    parser.add_argument("--fetched-at", default=None)
    args = parser.parse_args()

    if args.config:
        config = load_sue_crsp_price_extract_config(args.config)
        updates = {
            key: value
            for key, value in {
                "links_path": args.links_path,
                "output_path": args.output_path,
                "chunk_dir": args.chunk_dir,
                "manifest_path": args.manifest_path,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "chunk_size": args.chunk_size,
                "max_permnos": args.max_permnos,
                "fetched_at": args.fetched_at,
            }.items()
            if value is not None
        }
        if updates:
            config = config.model_copy(update=updates)
    else:
        config = SueCrspPriceExtractConfig(
            links_path=args.links_path or SueCrspPriceExtractConfig.model_fields["links_path"].default,
            output_path=args.output_path or SueCrspPriceExtractConfig.model_fields["output_path"].default,
            chunk_dir=args.chunk_dir or SueCrspPriceExtractConfig.model_fields["chunk_dir"].default,
            manifest_path=args.manifest_path or SueCrspPriceExtractConfig.model_fields["manifest_path"].default,
            start_date=args.start_date or "2020-01-01",
            end_date=args.end_date or "2022-03-25",
            chunk_size=args.chunk_size or 1,
            max_permnos=args.max_permnos,
            fetched_at=args.fetched_at,
        )

    import wrds

    connection = wrds.Connection()
    try:
        result = extract_crsp_prices_for_sue_links(config, connection=connection)
    finally:
        connection.close()

    print(f"status={result['status']}")
    print(f"distinct_permnos={result['distinct_permnos']}")
    print(f"chunk_count={result['chunk_count']}")
    print(f"queried_chunks={result['queried_chunks']}")
    print(f"skipped_chunks={result['skipped_chunks']}")
    print(f"row_count={result['row_count']}")
    print("production_approval_claimed=False")
    print(f"output_path={result['output_path']}")
    print(f"manifest_path={config.manifest_path}")


if __name__ == "__main__":
    main()
