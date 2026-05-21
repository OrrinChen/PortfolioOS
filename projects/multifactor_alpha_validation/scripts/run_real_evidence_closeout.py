from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.real_evidence_closeout import run_real_evidence_closeout


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MF-R9 real evidence closeout gate.")
    parser.add_argument(
        "--real-oos-output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/wrds_real_oos_evidence"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/wrds_real_evidence_closeout"),
    )
    args = parser.parse_args()

    result = run_real_evidence_closeout(args.real_oos_output_dir, args.output_dir)
    print(
        "multifactor_real_evidence_closeout "
        f"decision={result.decision} "
        f"production_approval={str(result.production_approval).lower()} "
        f"live_trading={str(result.live_trading).lower()} "
        f"direct_q2_entry={str(result.direct_q2_entry).lower()}"
    )


if __name__ == "__main__":
    main()
