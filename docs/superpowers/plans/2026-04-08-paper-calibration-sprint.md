# Paper Calibration Sprint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a neutral paper-calibration sprint to PortfolioOS that can generate deterministic trivial targets, dry-run the target contract, collect Alpaca paper fill telemetry, and produce a simulator-vs-paper calibration report without reopening alpha research.

**Architecture:** Build a thin calibration slice on top of existing PortfolioOS execution plumbing. Add a neutral target generator, a paper-calibration workflow service, a report/payload builder, and a light CLI entrypoint. Reuse existing Alpaca adapter, fill collection, storage, and reporting helpers; do not touch optimizer semantics or alpha logic.

**Tech Stack:** Python, pandas, Typer CLI, existing PortfolioOS execution/alpaca/storage modules, pytest

---

## File Structure

- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\neutral_targets.py`
  - Deterministic neutral target frame + manifest helpers for trivial calibration strategies.
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\workflow\paper_calibration.py`
  - Orchestration for dry-run and paper-calibration artifact generation.
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\execution\paper_calibration.py`
  - Report payloads and Markdown rendering for simulator-vs-paper calibration.
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\domain\models.py`
  - Add a `PaperCalibrationArtifacts` model for output paths.
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\storage\runs.py`
  - Add `prepare_paper_calibration_artifacts`.
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
  - Add a `paper-calibration` CLI group/command.
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_neutral_targets.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration_workflow.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py`
  - Add CLI coverage for the new calibration command.

### Task 1: Add deterministic neutral target helpers

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\neutral_targets.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_neutral_targets.py`

- [ ] **Step 1: Write the failing tests**

```python
from __future__ import annotations

import unittest

import pandas as pd

from portfolio_os.alpha.neutral_targets import (
    build_neutral_target_frame,
    build_neutral_target_manifest,
)


class NeutralTargetTests(unittest.TestCase):
    def test_build_neutral_target_frame_is_deterministic_for_spy_buy_and_hold(self) -> None:
        frame_a = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )
        frame_b = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )

        self.assertEqual(frame_a.to_dict(orient="records"), frame_b.to_dict(orient="records"))
        self.assertEqual(frame_a["ticker"].tolist(), ["SPY"])
        self.assertAlmostEqual(float(frame_a["target_weight"].sum()), 1.0)

    def test_build_neutral_target_frame_applies_small_deterministic_perturbation(self) -> None:
        frame = build_neutral_target_frame(
            tickers=["SPY", "IVV"],
            gross_target_weight=1.0,
            perturbation_bps=10.0,
            perturbation_seed=7,
        )

        self.assertEqual(sorted(frame["ticker"].tolist()), ["IVV", "SPY"])
        self.assertAlmostEqual(float(frame["target_weight"].sum()), 1.0, places=9)

    def test_build_neutral_target_manifest_captures_selection_inputs(self) -> None:
        frame = build_neutral_target_frame(
            tickers=["SPY"],
            gross_target_weight=1.0,
        )

        manifest = build_neutral_target_manifest(
            target_frame=frame,
            strategy_name="neutral_buy_and_hold",
            perturbation_bps=0.0,
            perturbation_seed=None,
        )

        self.assertEqual(manifest["strategy_name"], "neutral_buy_and_hold")
        self.assertEqual(manifest["selected_tickers"], ["SPY"])
        self.assertEqual(manifest["target_weight_sum"], 1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_neutral_targets.py -q
```

Expected: FAIL with `ModuleNotFoundError` or import errors for `portfolio_os.alpha.neutral_targets`.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

import random

import pandas as pd


def build_neutral_target_frame(
    *,
    tickers: list[str],
    gross_target_weight: float = 1.0,
    perturbation_bps: float = 0.0,
    perturbation_seed: int | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame({"ticker": [str(t).strip().upper() for t in tickers if str(t).strip()]})
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "target_weight"])
    base_weight = float(gross_target_weight) / float(len(frame))
    frame["target_weight"] = base_weight
    if perturbation_bps > 0 and len(frame) > 1:
        rng = random.Random(perturbation_seed)
        offsets = [rng.uniform(-perturbation_bps, perturbation_bps) / 10000.0 for _ in range(len(frame))]
        offsets = [value - (sum(offsets) / len(offsets)) for value in offsets]
        frame["target_weight"] = frame["target_weight"] + pd.Series(offsets)
        frame["target_weight"] = frame["target_weight"] / float(frame["target_weight"].sum())
    return frame.sort_values("ticker").reset_index(drop=True)


def build_neutral_target_manifest(
    *,
    target_frame: pd.DataFrame,
    strategy_name: str,
    perturbation_bps: float,
    perturbation_seed: int | None,
) -> dict[str, object]:
    return {
        "strategy_name": str(strategy_name),
        "selected_tickers": target_frame["ticker"].astype(str).tolist(),
        "selected_count": int(len(target_frame)),
        "target_weight_sum": float(target_frame["target_weight"].sum()) if not target_frame.empty else 0.0,
        "perturbation_bps": float(perturbation_bps),
        "perturbation_seed": perturbation_seed,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_neutral_targets.py -q
```

Expected: PASS

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\14574\Quant\PortfolioOS add src/portfolio_os/alpha/neutral_targets.py tests/test_neutral_targets.py
git -C C:\Users\14574\Quant\PortfolioOS commit -m "feat: add neutral target helpers"
```

### Task 2: Add paper calibration payloads and report rendering

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\execution\paper_calibration.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration.py`

- [ ] **Step 1: Write the failing tests**

```python
from __future__ import annotations

import unittest

from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord
from portfolio_os.execution.paper_calibration import (
    build_paper_calibration_payload,
    render_paper_calibration_report_markdown,
)


class PaperCalibrationReportTests(unittest.TestCase):
    def _result(self) -> ExecutionResult:
        return ExecutionResult(
            orders=[
                OrderExecutionRecord(
                    ticker="SPY",
                    direction="buy",
                    requested_qty=10,
                    filled_qty=10,
                    avg_fill_price=500.0,
                    status="filled",
                    poll_count=2,
                )
            ],
            submitted_count=1,
            filled_count=1,
            partial_count=0,
            unfilled_count=0,
            rejected_count=0,
            timeout_cancelled_count=0,
        )

    def test_build_payload_includes_required_sections(self) -> None:
        payload = build_paper_calibration_payload(
            strategy_name="neutral_buy_and_hold",
            target_manifest={"selected_tickers": ["SPY"], "selected_count": 1},
            execution_result=self._result(),
            expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
        )

        self.assertEqual(payload["strategy_name"], "neutral_buy_and_hold")
        self.assertIn("realized_summary", payload)
        self.assertIn("expected_assumptions", payload)
        self.assertIn("deviation_summary", payload)

    def test_render_report_mentions_fill_rate_and_slippage(self) -> None:
        payload = build_paper_calibration_payload(
            strategy_name="neutral_buy_and_hold",
            target_manifest={"selected_tickers": ["SPY"], "selected_count": 1},
            execution_result=self._result(),
            expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
        )

        report = render_paper_calibration_report_markdown(payload)
        self.assertIn("fill rate", report.lower())
        self.assertIn("slippage", report.lower())
        self.assertIn("neutral_buy_and_hold", report)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration.py -q
```

Expected: FAIL with import error for `portfolio_os.execution.paper_calibration`.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from typing import Any

from portfolio_os.execution.models import ExecutionResult


def build_paper_calibration_payload(
    *,
    strategy_name: str,
    target_manifest: dict[str, Any],
    execution_result: ExecutionResult,
    expected_assumptions: dict[str, Any],
) -> dict[str, Any]:
    total_orders = max(1, int(execution_result.submitted_count))
    fill_rate = float(execution_result.filled_count + execution_result.partial_count) / float(total_orders)
    partial_fill_frequency = float(execution_result.partial_count) / float(total_orders)
    average_fill_price = None
    if execution_result.orders:
        prices = [order.avg_fill_price for order in execution_result.orders if order.avg_fill_price is not None]
        if prices:
            average_fill_price = float(sum(prices) / len(prices))
    return {
        "strategy_name": str(strategy_name),
        "target_manifest": target_manifest,
        "expected_assumptions": expected_assumptions,
        "realized_summary": {
            "submitted_count": int(execution_result.submitted_count),
            "filled_count": int(execution_result.filled_count),
            "partial_count": int(execution_result.partial_count),
            "unfilled_count": int(execution_result.unfilled_count),
            "rejected_count": int(execution_result.rejected_count),
            "timeout_cancelled_count": int(execution_result.timeout_cancelled_count),
            "fill_rate": fill_rate,
            "partial_fill_frequency": partial_fill_frequency,
            "average_fill_price": average_fill_price,
        },
        "deviation_summary": {
            "fill_rate_vs_full_completion": fill_rate - 1.0,
            "partial_fill_frequency": partial_fill_frequency,
            "timeout_cancelled_count": int(execution_result.timeout_cancelled_count),
        },
    }


def render_paper_calibration_report_markdown(payload: dict[str, Any]) -> str:
    realized = payload["realized_summary"]
    return "\n".join(
        [
            "# Paper Calibration Report",
            "",
            f"## Strategy",
            f"- Name: {payload['strategy_name']}",
            "",
            "## Realized Execution",
            f"- Fill rate: {realized['fill_rate']:.1%}",
            f"- Partial fill frequency: {realized['partial_fill_frequency']:.1%}",
            f"- Average fill price: {realized['average_fill_price']}",
            "",
            "## Slippage / Deviation",
            f"- Assumptions: {payload['expected_assumptions']}",
            f"- Deviation summary: {payload['deviation_summary']}",
        ]
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration.py -q
```

Expected: PASS

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\14574\Quant\PortfolioOS add src/portfolio_os/execution/paper_calibration.py tests/test_paper_calibration.py
git -C C:\Users\14574\Quant\PortfolioOS commit -m "feat: add paper calibration report helpers"
```

### Task 3: Add paper calibration workflow and artifact preparation

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\domain\models.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\storage\runs.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\workflow\paper_calibration.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration_workflow.py`

- [ ] **Step 1: Write the failing tests**

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.workflow.paper_calibration import run_paper_calibration_dry_run


def test_paper_calibration_dry_run_writes_target_manifest_and_report(tmp_path: Path) -> None:
    output_dir = tmp_path / "paper_calibration"

    result = run_paper_calibration_dry_run(
        output_dir=output_dir,
        tickers=["SPY"],
        gross_target_weight=1.0,
        perturbation_bps=0.0,
        perturbation_seed=None,
        expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
    )

    assert Path(result.target_path).exists()
    assert Path(result.manifest_path).exists()
    assert Path(result.report_path).exists()
    frame = pd.read_csv(result.target_path)
    assert frame["ticker"].tolist() == ["SPY"]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration_workflow.py -q
```

Expected: FAIL with import or missing attribute errors.

- [ ] **Step 3: Write minimal implementation**

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_os.alpha.neutral_targets import build_neutral_target_frame, build_neutral_target_manifest
from portfolio_os.execution.models import ExecutionResult
from portfolio_os.execution.paper_calibration import (
    build_paper_calibration_payload,
    render_paper_calibration_report_markdown,
)
from portfolio_os.storage.snapshots import write_json, write_text


@dataclass
class PaperCalibrationDryRunResult:
    target_path: str
    manifest_path: str
    report_path: str


def run_paper_calibration_dry_run(
    *,
    output_dir: Path,
    tickers: list[str],
    gross_target_weight: float,
    perturbation_bps: float,
    perturbation_seed: int | None,
    expected_assumptions: dict[str, Any],
) -> PaperCalibrationDryRunResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    target_frame = build_neutral_target_frame(
        tickers=tickers,
        gross_target_weight=gross_target_weight,
        perturbation_bps=perturbation_bps,
        perturbation_seed=perturbation_seed,
    )
    target_manifest = build_neutral_target_manifest(
        target_frame=target_frame,
        strategy_name="neutral_buy_and_hold",
        perturbation_bps=perturbation_bps,
        perturbation_seed=perturbation_seed,
    )
    target_path = output_dir / "target.csv"
    manifest_path = output_dir / "paper_calibration_manifest.json"
    report_path = output_dir / "paper_calibration_report.md"
    target_frame.to_csv(target_path, index=False)
    payload = build_paper_calibration_payload(
        strategy_name="neutral_buy_and_hold",
        target_manifest=target_manifest,
        execution_result=ExecutionResult(),
        expected_assumptions=expected_assumptions,
    )
    write_json(manifest_path, target_manifest)
    write_text(report_path, render_paper_calibration_report_markdown(payload))
    return PaperCalibrationDryRunResult(
        target_path=str(target_path),
        manifest_path=str(manifest_path),
        report_path=str(report_path),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration_workflow.py -q
```

Expected: PASS

- [ ] **Step 5: Commit**

```powershell
git -C C:\Users\14574\Quant\PortfolioOS add src/portfolio_os/workflow/paper_calibration.py src/portfolio_os/domain/models.py src/portfolio_os/storage/runs.py tests/test_paper_calibration_workflow.py
git -C C:\Users\14574\Quant\PortfolioOS commit -m "feat: add paper calibration workflow"
```

### Task 4: Add CLI entrypoint and end-to-end coverage

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py`

- [ ] **Step 1: Write the failing test**

```python
def test_paper_calibration_cli_produces_expected_artifacts(tmp_path, monkeypatch) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "paper_calibration_cli"

    result = runner.invoke(
        app,
        [
            "paper-calibration",
            "--ticker",
            "SPY",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "target.csv").exists()
    assert (output_dir / "paper_calibration_manifest.json").exists()
    assert (output_dir / "paper_calibration_report.md").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py -q -k paper_calibration
```

Expected: FAIL because the command does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
paper_calibration_app = typer.Typer(add_completion=False, help="PortfolioOS paper calibration CLI.")
app.add_typer(paper_calibration_app, name="paper-calibration")


@paper_calibration_app.command("run")
def run_paper_calibration_command(
    ticker: str = typer.Option("SPY"),
    output_dir: Path = typer.Option(...),
    gross_target_weight: float = typer.Option(1.0),
    perturbation_bps: float = typer.Option(0.0),
    perturbation_seed: int | None = typer.Option(None),
) -> None:
    result = run_paper_calibration_dry_run(
        output_dir=output_dir,
        tickers=[ticker],
        gross_target_weight=gross_target_weight,
        perturbation_bps=perturbation_bps,
        perturbation_seed=perturbation_seed,
        expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
    )
    typer.echo(f"target.csv: {result.target_path}")
    typer.echo(f"paper_calibration_manifest.json: {result.manifest_path}")
    typer.echo(f"paper_calibration_report.md: {result.report_path}")
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py -q -k paper_calibration
```

Expected: PASS

- [ ] **Step 5: Run focused verification**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_neutral_targets.py C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration.py C:\Users\14574\Quant\PortfolioOS\tests\test_paper_calibration_workflow.py C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py -q
python -m py_compile C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\neutral_targets.py C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\execution\paper_calibration.py C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\workflow\paper_calibration.py
```

Expected: PASS with no compilation errors.

- [ ] **Step 6: Commit**

```powershell
git -C C:\Users\14574\Quant\PortfolioOS add src/portfolio_os/api/cli.py tests/test_e2e_cli.py
git -C C:\Users\14574\Quant\PortfolioOS commit -m "feat: add paper calibration cli"
```

## Self-Review

- Spec coverage:
  - neutral target generation: Task 1
  - calibration payload/report: Task 2
  - dry-run workflow/artifacts: Task 3
  - CLI runner / end-to-end contract: Task 4
- Placeholder scan:
  - no `TBD` / `TODO`
  - all tasks include file paths, commands, and concrete code snippets
- Type consistency:
  - `build_neutral_target_frame`, `build_neutral_target_manifest`, `build_paper_calibration_payload`, `render_paper_calibration_report_markdown`, and `run_paper_calibration_dry_run` are defined before later tasks depend on them
