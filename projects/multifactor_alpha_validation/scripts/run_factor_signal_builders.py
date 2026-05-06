from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.alpha_view_mapper import write_alpha_view_outputs
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.signal_builders import build_signal_panels, write_signal_outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic multifactor signal panels.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--signal-output-dir", type=Path, default=Path("outputs/factor_signals"))
    parser.add_argument("--alpha-view-output-dir", type=Path, default=Path("outputs/factor_alpha_views"))
    args = parser.parse_args()

    specs = load_factor_specs(args.spec_dir)
    result = build_signal_panels(specs)
    signal_files = write_signal_outputs(result, args.signal_output_dir)
    alpha_view_files = write_alpha_view_outputs(specs, result.signal_panels, args.alpha_view_output_dir)
    print(
        "factor_signals_built "
        f"signal_panel_count={len(result.signal_panels)} "
        f"abstain_count={len(result.abstain_report)} "
        f"disabled_factor_count={len(result.disabled_factors)} "
        f"signal_files={len(signal_files)} "
        f"alpha_view_files={len(alpha_view_files)}"
    )


if __name__ == "__main__":
    main()
