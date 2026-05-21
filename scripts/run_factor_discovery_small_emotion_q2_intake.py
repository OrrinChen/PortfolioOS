from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_aware_optimizer.small_emotion_q2_candidate_intake import (
    SmallEmotionQ2CandidateInput,
    run_small_emotion_q2_candidate_intake,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Open Q2-SMALL-EMOTION-01 candidate intake.")
    parser.add_argument(
        "--candidate",
        action="append",
        required=True,
        help=(
            "Candidate bundle as name|measurement_spec_path|q1_output_dir|promotion_gate_dir|measurement_spec_hash. "
            "Repeat for multiple promoted candidates."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/factor_discovery/small_emotion/q2_candidate_intake"),
    )
    args = parser.parse_args()

    candidates = [_parse_candidate(value) for value in args.candidate]
    result = run_small_emotion_q2_candidate_intake(candidates=candidates, output_dir=args.output_dir)
    print(json.dumps(result.summary, indent=2, sort_keys=True))


def _parse_candidate(value: str) -> SmallEmotionQ2CandidateInput:
    parts = value.split("|")
    if len(parts) != 5:
        raise SystemExit(
            "--candidate must be name|measurement_spec_path|q1_output_dir|promotion_gate_dir|measurement_spec_hash"
        )
    name, spec, q1, pg, spec_hash = parts
    return SmallEmotionQ2CandidateInput(
        candidate_name=name,
        measurement_spec_path=Path(spec),
        q1_output_dir=Path(q1),
        promotion_gate_dir=Path(pg),
        required_measurement_spec_hash=spec_hash,
    )


if __name__ == "__main__":
    main()
