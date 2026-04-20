"""Reviewer-facing aggregation for research promotion bundles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from portfolio_os.alpha.promotion_contract import (
    PROMOTION_CONTRACT_FILENAME,
    PromotionContract,
    load_promotion_contract,
)
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json, write_text


@dataclass
class PromotionRegistryResult:
    registry_csv_path: Path
    manifest_path: Path
    summary_path: Path
    bundle_count: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _discover_bundle_dirs(input_root: Path) -> list[Path]:
    bundle_dirs = {path.parent for path in input_root.rglob(PROMOTION_CONTRACT_FILENAME)}
    return sorted(bundle_dirs)


def _build_registry_frame(contracts: list[PromotionContract]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for contract in contracts:
        rows.append(
            {
                "bundle_id": contract.bundle_id,
                "created_at": contract.created_at,
                "research_line": contract.research_line,
                "candidate_status": contract.candidate_status,
                "thesis_summary": contract.thesis_summary,
                "universe_name": contract.universe_name,
                "signal_names": ", ".join(signal.name for signal in contract.signals),
                "signal_stage_buckets": ", ".join(signal.stage_bucket for signal in contract.signals),
                "combo_eligible_for_stage4": contract.combo.eligible_for_stage4,
                "combo_blocking_reason": contract.combo.blocking_reason,
                "combo_full_sample_ir": contract.combo.full_sample_ir,
                "combo_second_half_ir": contract.combo.second_half_ir,
                "memory_path": str(contract.memory_path),
                "ledger_path": str(contract.ledger_path),
                "bundle_dir": str(contract.bundle_dir),
            }
        )
    return pd.DataFrame(rows)


def _render_summary_markdown(registry: pd.DataFrame, *, input_root: Path) -> str:
    research_line_counts = registry["research_line"].astype(str).value_counts().to_dict()
    candidate_status_counts = registry["candidate_status"].astype(str).value_counts().to_dict()
    lines = [
        "# Promotion Registry Summary",
        "",
        "## Scope",
        f"- Generated at: {_utc_now_iso()}",
        f"- Input root: {input_root.resolve()}",
        f"- Bundle Count: {int(len(registry))}",
        f"- Research-line counts: {research_line_counts}",
        f"- Candidate-status counts: {candidate_status_counts}",
        "",
        "## Bundles",
        "| Bundle ID | Research Line | Candidate Status | Signals | Stage 4 Eligible | Blocking Reason |",
        "|---|---|---|---|---:|---|",
    ]
    for row in registry.to_dict(orient="records"):
        lines.append(
            f"| {row['bundle_id']} | {row['research_line']} | {row['candidate_status']} | "
            f"{row['signal_names']} | {str(bool(row['combo_eligible_for_stage4']))} | {row['combo_blocking_reason']} |"
        )
    return "\n".join(lines)


def run_promotion_registry(*, input_root: Path, output_dir: Path) -> PromotionRegistryResult:
    """Scan recursively for promotion bundles and build a compact reviewer registry."""

    bundle_dirs = _discover_bundle_dirs(input_root)
    if not bundle_dirs:
        raise InputValidationError(f"No promotion bundles found under {input_root}.")

    contracts = [load_promotion_contract(path) for path in bundle_dirs]
    registry = _build_registry_frame(contracts)

    output_dir.mkdir(parents=True, exist_ok=True)
    registry_csv_path = output_dir / "promotion_registry.csv"
    manifest_path = output_dir / "promotion_registry_manifest.json"
    summary_path = output_dir / "promotion_registry_summary.md"

    registry.to_csv(registry_csv_path, index=False)
    write_json(
        manifest_path,
        {
            "generated_at": _utc_now_iso(),
            "input_root": str(input_root.resolve()),
            "bundle_count": int(len(registry)),
            "bundle_dirs": [str(path) for path in bundle_dirs],
            "registry_csv_path": str(registry_csv_path),
            "summary_path": str(summary_path),
        },
    )
    write_text(summary_path, _render_summary_markdown(registry, input_root=input_root))
    return PromotionRegistryResult(
        registry_csv_path=registry_csv_path,
        manifest_path=manifest_path,
        summary_path=summary_path,
        bundle_count=int(len(registry)),
    )
