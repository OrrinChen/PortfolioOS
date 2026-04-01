"""CLI entrypoint wrapper for pilot validation runs."""

from __future__ import annotations

from pathlib import Path
import runpy


def main() -> int:
    """Run the scripted pilot validation workflow."""

    script_path = Path(__file__).resolve().parents[3] / "scripts" / "run_pilot_validation.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Pilot validation script not found: {script_path}")
    namespace = runpy.run_path(str(script_path))
    runner = namespace.get("main")
    if runner is None:
        raise RuntimeError("Pilot validation script does not expose a main() function.")
    return int(runner())


if __name__ == "__main__":
    raise SystemExit(main())
