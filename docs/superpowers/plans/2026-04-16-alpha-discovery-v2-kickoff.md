# Alpha Discovery v2 Kickoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start the discovery-first program by producing the first two hard assets: the family selection memo and the calibration-family D1 packet.

**Architecture:** This kickoff slice does not implement the full charter. It decomposes the broad discovery program into the first serial stage only: document why `A-share state-transition microstructure` is the primary mining family, then define the `US residual momentum / residual reversal` calibration family tightly enough to run the discovery machine against it. Both deliverables are docs-first and are intended to constrain later code, not to reopen the old qualification sprint.

**Tech Stack:** Markdown strategy docs, existing PortfolioOS research artifacts, git history, PowerShell, pytest-free doc validation via diff/self-review

---

### Task 1: Write The Family Selection Memo

**Files:**
- Create: `docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md`
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Write the first draft of the memo**

Create `docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md` with this structure:

```md
# Alpha Discovery Family Selection Memo (2026-04-16)

## Decision

Calibration family:
- `US residual momentum / residual reversal`

Primary mining family:
- `A-share state-transition microstructure`

## Why This Is Not The Old US Restart

- explain why the old sprint was qualification-first
- explain why the new program is discovery-first
- explain why calibration and primary are now serial

## Candidate Family Comparison

| Family | Expected information density | Crowding | Structural edge | Data liabilities | Decision |
|---|---:|---:|---:|---:|---|
| US residual momentum / residual reversal | ... | ... | ... | ... | calibration only |
| A-share state-transition microstructure | ... | ... | ... | ... | primary |
| A-share generic anti-momentum / reversal | ... | ... | ... | ... | deferred |
| Cross-market connect / dual-list / ADR state effects | ... | ... | ... | ... | deferred |

## Why The Primary Family Wins

- structural edge is objective and market-design based
- discovery target is a mechanism family, not a familiar signal
- this family has a clearer path to adversarial controls than a generic anomaly family

## Why The Calibration Family Stays

- discovery machine needs calibration
- existing data/code path is familiar
- a rough answer is already known, which is useful for validating controls

## Reopen Conditions For Deferred Families

- list what would need to change before revisiting them
```

- [ ] **Step 2: Review prior docs before finalizing the memo**

Run:

```powershell
Get-Content 'docs/strategy/alpha_discovery_charter_v2_2026_04_16.md' -TotalCount 260
Get-Content 'docs/strategy/us_alpha_core_week4_stop_go_note_2026_04_16.md' -TotalCount 200
```

Expected:
- the memo language stays consistent with the charter
- the old US restart remains explicitly closed rather than silently reopened

- [ ] **Step 3: Tighten the memo to remove convenience-driven framing**

Ensure the final draft explicitly says:

```md
- primary family is selected by objective structural edge, not by code convenience
- calibration family exists to validate the discovery machine, not to search for the winner
- the program does not infer informational superiority from personal narrative alone
```

- [ ] **Step 4: Update task memory to reflect the kickoff**

Append or revise the research-governance section in `TASK_MEMORY.md` so it says:

```md
- `alpha_discovery_charter_v2_2026_04_16.md` is now the active discovery design object
- current implementation slice = `Phase 0 + calibration-family D1 kickoff`
- no primary-family mining starts before the calibration-family closeout exists
```

- [ ] **Step 5: Review the diff**

Run:

```powershell
git diff -- 'docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md' 'TASK_MEMORY.md'
```

Expected:
- memo states a clear calibration-vs-primary split
- `TASK_MEMORY.md` points to the kickoff slice rather than immediate primary-family mining

- [ ] **Step 6: Commit**

Run:

```powershell
git add 'docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md' 'TASK_MEMORY.md'
git commit -m "docs: add alpha discovery family selection memo"
```

### Task 2: Write The Calibration Family D1 Packet

**Files:**
- Create: `docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md`
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Draft the calibration D1 packet**

Create `docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md` with this structure:

```md
# US Residual Momentum Calibration Family D1 (2026-04-16)

## Role

- this is the calibration family, not the expected winner family
- its job is to validate the discovery machine

## Mechanism Hypotheses

1. residual continuation
2. residual reversal after overextension
3. volatility-managed residual persistence

## Mechanism Signatures

| Mechanism | Expected horizon | Expected stress sign | Expected baseline overlap | Falsification trigger |
|---|---|---|---|---|
| ... | ... | ... | ... | ... |

## Expression Ledger Admission Rule

- every expression must map to one mechanism
- no free-form transformation cartesian product

## Mandatory Negative Controls

1. shuffled / noise-matched placebo
2. pre-window placebo
3. matched baseline-mimic control

## Intra-family Orthogonality Rule

- top expressions cannot all be near-duplicates

## Calibration Success Read

- controls behave as expected
- bootstrap ranking behaves sensibly
- orthogonality logic does not falsely collapse clearly distinct expressions
- family-level gate produces an interpretable answer

## Calibration Failure Read

- controls leak false positives
- bootstrap ranking is unstable or uninformative
- winner selection collapses under trivial perturbation
```

- [ ] **Step 2: Bind the packet to existing code reality**

Read the current alpha modules and existing runners:

```powershell
Get-ChildItem 'src/portfolio_os/alpha'
Get-ChildItem 'scripts'
```

Expected:
- the packet references existing reusable modules such as `qualification.py`, `long_horizon.py`, and `signal_consistency_diagnostic.py`
- the packet does not assume non-existent calibration infrastructure

- [ ] **Step 3: Add explicit reuse and non-reuse statements**

Ensure the packet explicitly says:

```md
- reuse old residual-momentum code paths only as inputs to the calibration family
- do not reuse the old winner-gate logic as the discovery decision rule
- do not treat the old US restart sample as the primary evidence base
```

- [ ] **Step 4: Update task memory with the next gate**

Revise `TASK_MEMORY.md` to say:

```md
- next document gate after the family selection memo is `us_residual_momentum_calibration_d1_2026_04_16.md`
- only after this calibration packet is reviewed can execution move to calibration-family implementation / experiment setup
```

- [ ] **Step 5: Review the diff**

Run:

```powershell
git diff -- 'docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md' 'TASK_MEMORY.md'
```

Expected:
- the calibration packet is clearly separated from primary-family mining
- `TASK_MEMORY.md` reflects that the next live execution gate is calibration-family setup, not primary-family mining

- [ ] **Step 6: Commit**

Run:

```powershell
git add 'docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md' 'TASK_MEMORY.md'
git commit -m "docs: add calibration family d1 packet"
```

### Task 3: Self-Review The Kickoff Slice

**Files:**
- Modify: `docs/superpowers/plans/2026-04-16-alpha-discovery-v2-kickoff.md`

- [ ] **Step 1: Check spec coverage**

Confirm the kickoff slice covers the immediate charter requirements:

```md
- Phase 0 family selection memo
- calibration family defined before primary-family mining
- no silent reopening of the old US sprint
```

- [ ] **Step 2: Placeholder scan**

Run:

```powershell
Select-String -Path 'docs/superpowers/plans/2026-04-16-alpha-discovery-v2-kickoff.md' -Pattern 'TBD|TODO|later|similar to|appropriate'
```

Expected:
- no placeholder matches

- [ ] **Step 3: Consistency scan**

Run:

```powershell
Select-String -Path 'docs/superpowers/plans/2026-04-16-alpha-discovery-v2-kickoff.md' -Pattern 'primary-family mining|calibration family|family selection memo|calibration-family closeout'
```

Expected:
- no contradiction between serial execution and planned deliverables

- [ ] **Step 4: Commit only if the plan itself changed**

Run:

```powershell
git status --short
```

Expected:
- if only plan edits were made during self-review, commit them
- otherwise leave the plan as already tracked
```
