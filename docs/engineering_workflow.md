# Engineering Workflow Standard (TDD + SDD)

Canonical entry: `docs/standards/engineering_workflow.md`
This file is retained for backward compatibility.

Version: 1.0  
Effective date: 2026-03-25  
Status: active

This document defines the minimum engineering standard for PortfolioOS.
It is designed to improve reliability of optimization logic and multi-script orchestration while keeping iteration speed practical.

## 1. Scope

This standard applies to:

- `src/portfolio_os/**`
- `scripts/**`
- `tests/**`
- any schema-bearing output artifacts (CSV, JSON, Markdown reports)

## 2. Layered Policy

PortfolioOS uses layered strictness instead of one-size-fits-all process.

### L1: Core Logic (strict TDD)

Examples:

- optimizer objective and constraints
- repair and order quantization logic
- risk model activation and risk penalty wiring
- metric definitions (turnover/cost/benchmark)

Rules:

- test must be written before implementation change
- follow red-green-refactor in each change slice
- include at least one edge case and one negative case
- avoid broad refactors in same patch as behavior changes

### L2: Orchestration (strict SDD + contract tests)

Examples:

- `scripts/pilot_ops.py`
- `scripts/pilot_historical_replay.py`
- `scripts/run_pilot_validation.py`
- A/B runner scripts and replay scripts

Rules:

- write a small contract spec first (inputs, outputs, error semantics)
- add parameter passthrough tests for every new CLI flag crossing process boundaries
- assert failure semantics (timeout/retry/partial continue/resume behavior)
- schema changes require explicit compatibility note

### L3: External Integrations (contract tests + smoke)

Examples:

- Tushare, AKShare, Tencent fallback, Alpaca, yfinance

Rules:

- keep default unit test path offline via mocks/stubs
- add focused integration smoke tests only where signal is high
- never make CI correctness depend on unstable third-party APIs

## 3. Required Lifecycle

Every non-trivial change must follow:

1. Spec first (SDD)
2. Failing tests (TDD red)
3. Minimal implementation (green)
4. Refactor without behavior drift
5. Full test pass
6. Docs/spec sync

## 4. Definition Of Ready (DoR)

A task is ready only when all are true:

- problem statement is explicit and testable
- acceptance criteria include:
  - one happy path
  - one failure path
  - one boundary path
- affected schemas/outputs are identified
- cross-script parameter flow is identified when applicable
- risk level is tagged:
  - `low`: local behavior, no schema change
  - `medium`: multi-module change or new output fields
  - `high`: optimizer math, compliance gating, or replay/pilot governance

## 5. Definition Of Done (DoD)

A change is done only when all are true:

- new behavior covered by tests
- existing tests stay green
- command-line passthrough covered if applicable
- no undocumented schema changes
- user-visible failure semantics are deterministic
- documentation updated when contract changes

## 6. Required Test Types By Change Class

- Optimizer math change:
  - deterministic unit test
  - regression test for prior bug
  - boundary test on constraints/weights
- Script orchestration change:
  - subprocess command construction test
  - retry/timeout behavior test
  - resume/progress consistency test (if replay-like)
- Report/summary change:
  - schema compatibility test
  - missing-field behavior test (`N/A` instead of crash)

## 7. Critical Guardrails

- Use `sys.executable` for Python subprocess calls.
- Never hardcode `python3` in cross-platform script orchestration.
- Use `pathlib.Path` for all path composition.
- For optional fields in dashboard/report flows, degrade gracefully.
- For retries and timeouts, encode behavior in tests.

## 8. Pull Request Minimum Checklist

PR must include:

- task spec link or embedded spec section
- list of changed contracts (CLI args/files/fields)
- test evidence:
  - newly added tests
  - updated tests
  - full test run status
- rollout risk note (especially for replay/pilot scripts)

Use template:

- `docs/templates/pr_checklist.md`

## 9. Task Spec Template

Use template:

- `docs/templates/task_spec_template.md`

## 10. Exceptions

Emergency fixes may temporarily skip full cycle, but must include:

- issue containment patch
- follow-up task within 24 hours to restore missing tests/spec updates
- explicit note in PR on what was skipped and why
