# Paper Calibration Sprint Design

**Date:** 2026-04-08  
**Workspace:** `C:\Users\14574\Quant\PortfolioOS`  
**Status:** Proposed

## Goal

Build a platform-level paper calibration sprint for PortfolioOS that validates execution and simulator assumptions using a deliberately neutral strategy on Alpaca paper trading.

This sprint is not an alpha-discovery effort. It is a research-validation and platform-calibration effort that should produce reusable execution telemetry, a simulator-vs-paper comparison report, and a hardened dry-run-to-paper workflow.

## Why Now

PortfolioOS has execution infrastructure, Alpaca integration, fill collection, and simulation components already in place. What it does not yet have is a clean, forward-looking calibration loop that compares simulated execution assumptions against real paper fills under a controlled, low-semantic-risk strategy.

This sprint creates that loop without reopening US alpha research and without requiring a production-ready alpha package.

## Roadmap Position

This work aligns with the platform roadmap as a preparation layer between:

- **Phase 2: US Risk And Cost Intelligence**
- **Phase 3: US RL Execution**

It is not a substitute for alpha qualification, and it is not an execution-policy research branch. It is a calibration asset for the platform itself.

## Non-Goals

The sprint explicitly does **not** do any of the following:

- qualify or promote a new alpha
- reopen the US WRDS event-driven alpha branch
- modify optimizer objectives or portfolio construction semantics
- redesign broker abstractions
- introduce RL or new execution-policy learning
- use paper trading as a shortcut for alpha evaluation
- add live automation or scheduling requirements

## Strategy Definition

The paper calibration sprint uses a deliberately neutral strategy so that execution behavior can be studied without entangling it with alpha semantics.

### Neutral Strategy v1

- Instrument universe: start with `SPY`
- Strategy form: deterministic low-turnover target generation
- Default behavior: buy-and-hold or benchmark-tracking style target with optional tiny deterministic perturbation
- Objective: generate realistic but interpretable order flow
- Priority: low semantic complexity, stable cadence, easy attribution

The neutral strategy is not expected to outperform. It is expected to create a controlled stream of orders and fills that can be compared against simulation assumptions.

## Required Deliverables

The sprint must produce the following artifacts:

1. A dry-run contract path from target generation to target/export artifacts
2. A neutral paper calibration runner that uses existing Alpaca paper plumbing
3. A persistent fill telemetry capture path
4. A simulator-vs-paper comparison payload
5. A calibration report in Markdown

The report must be useful even if later alpha work fails.

## Architecture

The implementation should be a thin extension of existing PortfolioOS execution plumbing.

### Existing Components To Reuse

- `src/portfolio_os/workflow/single_run.py`
- `src/portfolio_os/data/providers/alpaca_provider.py`
- `src/portfolio_os/execution/alpaca_adapter.py`
- `src/portfolio_os/execution/fill_collection.py`
- `src/portfolio_os/execution/calibration.py`
- `src/portfolio_os/alpha/event_targets.py`

### New Components

The sprint should add:

- a neutral target generator for trivial paper calibration targets
- a paper calibration workflow runner
- a report builder that compares expected execution assumptions with realized paper outcomes
- fixtures/tests for deterministic calibration behavior

## Phase Structure

### Phase 0: Dry-Run Contract

Purpose:

- prove that a neutral target can flow through the current PortfolioOS target/export path
- freeze the contract shape before real paper trading starts

Required outputs:

- deterministic target file
- run manifest
- execution-ready artifact shape

Success condition:

- target generation and dry-run export succeed without schema hacks

### Phase 1: Neutral Paper Calibration

Purpose:

- run a trivial neutral strategy through Alpaca paper trading
- capture real fill behavior and compare it with simulator expectations

Required outputs:

- realized fill log
- broker-state snapshots
- partial-fill / timeout / lifecycle telemetry
- simulator-vs-paper comparison summary
- `paper_calibration_report.md`

Success condition:

- the system produces a usable calibration report with enough information to explain execution deviations

## Report Scope

The calibration report must include:

- fill rate
- partial-fill frequency
- rejection count and reasons
- timeout/cancel behavior
- realized slippage
- reference-price drift
- realized order lifecycle timing
- simulator-vs-paper deviation summary

The report should answer whether current execution assumptions are directionally reasonable, too optimistic, or too conservative.

## Interfaces

### Neutral Target Generator

Inputs:

- date or run timestamp
- trivial strategy config
- optional deterministic perturbation seed

Outputs:

- target frame compatible with the existing target loader / rebalance workflow

### Paper Calibration Runner

Inputs:

- target artifact or target generation config
- Alpaca paper credentials from existing environment/config handling
- output directory for logs and report artifacts

Outputs:

- normalized fill telemetry artifacts
- calibration payload
- calibration report

### Comparison Builder

Inputs:

- expected execution assumptions
- realized fill telemetry
- optional benchmark/reference prices

Outputs:

- machine-readable comparison summary
- Markdown report section

## Success Criteria

This sprint is successful if all of the following are true:

- a neutral target can be generated deterministically
- the current PortfolioOS workflow can dry-run it cleanly
- Alpaca paper execution can be invoked without changing broker abstractions
- fill telemetry is captured end-to-end
- a calibration report can be generated from actual paper results

The sprint is still considered successful if the neutral strategy itself has no economic edge, as long as the calibration artifacts are complete and interpretable.

## Failure Criteria

This sprint fails if any of the following happen:

- the neutral strategy requires alpha-specific assumptions to function
- target-to-order translation requires ad hoc schema workarounds
- paper execution cannot produce stable, normalized fill artifacts
- the resulting report cannot distinguish simulator assumptions from broker/runtime behavior

## Testing Strategy

The implementation must be test-first and cover three layers.

### 1. Contract Tests

- neutral target generation is deterministic
- target shape is compatible with existing target loaders
- dry-run manifests are stable and complete

### 2. Workflow Tests

- paper calibration runner can execute against mocks/fakes
- fill telemetry is normalized into expected artifact shapes
- comparison payloads contain required fields

### 3. Report Tests

- report builder includes all required calibration sections
- missing/partial telemetry is represented explicitly, not silently dropped

## Implementation Constraints

- prefer additive changes over rewrites
- reuse existing Alpaca and execution plumbing
- do not add alpha semantics
- do not modify optimizer meaning
- do not introduce new scheduler or automation dependencies
- keep the calibration sprint separable from future alpha paper runs

## Timeline

### Sprint Step 1

- write and freeze the calibration sprint spec
- add target generator and dry-run contract tests

### Sprint Step 2

- add paper calibration runner and telemetry/report tests
- wire runner to existing Alpaca paper path

### Sprint Step 3

- perform dry-run verification
- prepare manual paper run instructions and artifact locations

### Sprint Step 4

- run the first paper calibration cycle
- generate the first calibration report

## Assumptions

- Alpaca paper trading remains the current execution validation backend
- a neutral strategy is sufficient for execution calibration
- the current simulator and TCA stack are mature enough to make comparison worthwhile
- this sprint should produce reusable platform assets even if no alpha strategy is attached afterward

## Open Questions Resolved In This Design

- **Should paper account be used to find alpha?** No. It is a platform calibration tool in this sprint.
- **Should this reopen US event-alpha research?** No.
- **Should this wait for a promoted alpha package?** No. Calibration is valuable independently.
- **Should dry-run and paper be separate?** Yes. Dry-run is Phase 0; neutral paper is Phase 1.

## Decision

Proceed with a roadmap-aligned paper calibration sprint in `PortfolioOS`, using a neutral strategy and existing Alpaca execution plumbing, with dry-run validation first and real paper calibration second.
