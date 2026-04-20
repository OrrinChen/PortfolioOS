# A-Share State-Transition Panel Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first D2 implementation slice for A-share state-transition mining by creating a deterministic daily panel contract and daily-state taggers for the upper-limit pilot.

**Architecture:** Keep the existing provider and `market.csv` contract unchanged. Add a new alpha-layer module that consumes a richer daily-bar CSV, normalizes it into a `state-transition daily panel`, and derives the first pilot state tags (`sealed_upper_limit`, `failed_upper_limit`) plus next-day return fields needed by `M1/M2/M5`.

**Tech Stack:** Python, pandas, pytest, existing `portfolio_os.alpha` / `portfolio_os.domain.errors` helpers.

---

## File Structure

- Create: `src/portfolio_os/alpha/state_transition_panel.py`
  - Responsible for validating one daily-bar input frame, deriving pilot state tags, and computing next-day returns.
- Create: `tests/test_alpha_state_transition_panel.py`
  - Focused unit tests for input validation, state-tag correctness, and next-day return derivation.
- Modify: `src/portfolio_os/alpha/__init__.py`
  - Export the new panel helpers if the package already uses explicit exports.
- Modify: `docs/strategy/us_alpha_discovery_v2_memory.md`
  - Record that the first D2 implementation slice has started and point to the new plan.

## Task 1: Add Red Tests For The State-Transition Daily Panel Contract

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\tests\test_alpha_state_transition_panel.py`

- [ ] **Step 1: Write the failing test file**

```python
from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_daily_panel,
)
from portfolio_os.domain.errors import InputValidationError


def _daily_bar_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "ticker": "000001",
                "open": 10.00,
                "high": 11.00,
                "low": 9.95,
                "close": 11.00,
                "volume": 1_000_000,
                "amount": 10_500_000,
                "upper_limit_price": 11.00,
                "lower_limit_price": 9.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000001",
                "open": 11.10,
                "high": 11.20,
                "low": 10.80,
                "close": 10.90,
                "volume": 900_000,
                "amount": 9_900_000,
                "upper_limit_price": 12.10,
                "lower_limit_price": 9.90,
                "tradable": True,
            },
            {
                "date": "2026-04-01",
                "ticker": "000002",
                "open": 20.00,
                "high": 22.00,
                "low": 19.80,
                "close": 21.20,
                "volume": 2_000_000,
                "amount": 42_000_000,
                "upper_limit_price": 22.00,
                "lower_limit_price": 18.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000002",
                "open": 21.00,
                "high": 21.10,
                "low": 20.00,
                "close": 20.40,
                "volume": 1_500_000,
                "amount": 31_000_000,
                "upper_limit_price": 23.32,
                "lower_limit_price": 19.08,
                "tradable": True,
            },
        ]
    )


def test_build_state_transition_daily_panel_derives_upper_limit_states() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    same_day = panel.loc[panel["date"] == "2026-04-01"].set_index("ticker")

    assert bool(same_day.loc["000001", "upper_limit_touched"]) is True
    assert bool(same_day.loc["000001", "sealed_upper_limit"]) is True
    assert bool(same_day.loc["000001", "failed_upper_limit"]) is False

    assert bool(same_day.loc["000002", "upper_limit_touched"]) is True
    assert bool(same_day.loc["000002", "sealed_upper_limit"]) is False
    assert bool(same_day.loc["000002", "failed_upper_limit"]) is True


def test_build_state_transition_daily_panel_derives_next_day_returns() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    same_day = panel.loc[
        (panel["date"] == "2026-04-01") & (panel["ticker"] == "000001")
    ].iloc[0]

    assert same_day["next_open_return"] == pytest.approx(11.10 / 11.00 - 1.0)
    assert same_day["next_close_return"] == pytest.approx(10.90 / 11.00 - 1.0)


def test_build_state_transition_daily_panel_rejects_missing_required_columns() -> None:
    bad = _daily_bar_fixture().drop(columns=["upper_limit_price"])

    with pytest.raises(InputValidationError, match="missing required columns"):
        build_state_transition_daily_panel(bad)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_alpha_state_transition_panel.py -q`
Expected: FAIL with import error because `portfolio_os.alpha.state_transition_panel` does not exist yet.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_alpha_state_transition_panel.py
git commit -m "test: add state-transition panel contract coverage"
```

## Task 2: Implement The Daily Panel Builder And State Taggers

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\src\portfolio_os\alpha\state_transition_panel.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\src\portfolio_os\alpha\__init__.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\tests\test_alpha_state_transition_panel.py`

- [ ] **Step 1: Write the minimal implementation**

```python
from __future__ import annotations

import pandas as pd

from portfolio_os.domain.errors import InputValidationError


REQUIRED_STATE_TRANSITION_COLUMNS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "upper_limit_price",
    "lower_limit_price",
    "tradable",
]

_PRICE_TOLERANCE = 1e-6


def build_state_transition_daily_panel(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    missing = sorted(set(REQUIRED_STATE_TRANSITION_COLUMNS) - set(work.columns))
    if missing:
        raise InputValidationError(
            "state-transition daily panel is missing required columns: "
            + ", ".join(missing)
        )

    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["ticker"] = work["ticker"].astype(str).str.strip()
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "upper_limit_price",
        "lower_limit_price",
    ]
    for column in numeric_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["tradable"] = work["tradable"].astype(bool)

    if work["date"].isna().any():
        raise InputValidationError("state-transition daily panel contains invalid dates.")
    if work[["ticker"] + numeric_columns].isna().any().any():
        raise InputValidationError("state-transition daily panel contains invalid numeric values.")
    if work.duplicated(subset=["date", "ticker"]).any():
        raise InputValidationError("state-transition daily panel contains duplicate (date, ticker) rows.")

    work = work.sort_values(["ticker", "date"]).reset_index(drop=True)
    work["upper_limit_touched"] = work["high"] >= (work["upper_limit_price"] - _PRICE_TOLERANCE)
    work["lower_limit_touched"] = work["low"] <= (work["lower_limit_price"] + _PRICE_TOLERANCE)
    work["sealed_upper_limit"] = work["upper_limit_touched"] & (
        work["close"] >= (work["upper_limit_price"] - _PRICE_TOLERANCE)
    )
    work["failed_upper_limit"] = work["upper_limit_touched"] & ~work["sealed_upper_limit"]

    grouped = work.groupby("ticker", sort=False)
    work["next_open"] = grouped["open"].shift(-1)
    work["next_close"] = grouped["close"].shift(-1)
    work["next_open_return"] = work["next_open"] / work["close"] - 1.0
    work["next_close_return"] = work["next_close"] / work["close"] - 1.0
    return work
```

- [ ] **Step 2: Export the helper if needed**

```python
from portfolio_os.alpha.state_transition_panel import build_state_transition_daily_panel

__all__ = [
    "build_state_transition_daily_panel",
]
```

- [ ] **Step 3: Run the focused test file**

Run: `python -m pytest tests/test_alpha_state_transition_panel.py -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/portfolio_os/alpha/state_transition_panel.py src/portfolio_os/alpha/__init__.py tests/test_alpha_state_transition_panel.py
git commit -m "feat: add state-transition daily panel builder"
```

## Task 3: Add Pilot-Specific Extraction Helpers For M1/M2/M5

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\src\portfolio_os\alpha\state_transition_panel.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\tests\test_alpha_state_transition_panel.py`

- [ ] **Step 1: Extend the tests with pilot extraction expectations**

```python
from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_daily_panel,
    extract_upper_limit_daily_state_slice,
)


def test_extract_upper_limit_daily_state_slice_keeps_only_pilot_rows() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    pilot = extract_upper_limit_daily_state_slice(panel)

    assert set(pilot["ticker"]) == {"000001", "000002"}
    assert {"sealed_upper_limit", "failed_upper_limit", "next_open_return", "next_close_return"} <= set(pilot.columns)
    assert bool(pilot.loc[pilot["ticker"] == "000001", "sealed_upper_limit"].iloc[0]) is True
    assert bool(pilot.loc[pilot["ticker"] == "000002", "failed_upper_limit"].iloc[0]) is True
```

- [ ] **Step 2: Add the extraction helper**

```python
def extract_upper_limit_daily_state_slice(panel: pd.DataFrame) -> pd.DataFrame:
    required = {"upper_limit_touched", "sealed_upper_limit", "failed_upper_limit"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise InputValidationError(
            "upper-limit pilot extraction requires derived state columns: "
            + ", ".join(missing)
        )
    work = panel.copy()
    work = work.loc[work["upper_limit_touched"]].copy()
    return work.sort_values(["date", "ticker"]).reset_index(drop=True)
```

- [ ] **Step 3: Run the focused test file again**

Run: `python -m pytest tests/test_alpha_state_transition_panel.py -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/portfolio_os/alpha/state_transition_panel.py tests/test_alpha_state_transition_panel.py
git commit -m "feat: add upper-limit pilot extraction helpers"
```

## Task 4: Record The D2 Slice Start In Branch Memory

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\docs\strategy\us_alpha_discovery_v2_memory.md`

- [ ] **Step 1: Update memory to record the first implementation slice**

```markdown
- D2 implementation has started with the first coding slice:
  - `state-transition daily panel`
  - `sealed / failed upper-limit` daily-state taggers
- active code path:
  - `src/portfolio_os/alpha/state_transition_panel.py`
```

- [ ] **Step 2: Sanity check formatting**

Run: `git -C C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze diff --check`
Expected: no output

- [ ] **Step 3: Commit**

```bash
git add docs/strategy/us_alpha_discovery_v2_memory.md
git commit -m "docs: record state-transition panel implementation start"
```

## Task 5: Full Verification For The Slice

**Files:**
- Verify only; no new files

- [ ] **Step 1: Run the targeted test suite**

Run: `python -m pytest tests/test_alpha_state_transition_panel.py -q`
Expected: PASS

- [ ] **Step 2: Run the adjacent alpha/provider regression tests**

Run: `python -m pytest tests/test_providers.py tests/test_alpha_research.py tests/test_alpha_qualification.py -q`
Expected: PASS

- [ ] **Step 3: Confirm worktree state**

Run: `git -C C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze status --short`
Expected: clean working tree
