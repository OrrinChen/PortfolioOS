from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.small_emotion_measurement_spec import write_small_emotion_measurement_spec


def main() -> None:
    parser = argparse.ArgumentParser(description="Freeze D4 MeasurementSpec for the small-emotion sharpened candidate.")
    parser.add_argument(
        "--charter",
        type=Path,
        default=Path(
            "outputs/factor_discovery/small_emotion/d3_sharpened_up_shock_reversal_charter/d3_candidate_charter.yaml",
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec"),
    )
    args = parser.parse_args()

    result = write_small_emotion_measurement_spec(
        charter_path=args.charter,
        output_dir=args.output_dir,
    )
    print(json.dumps(result.summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
