# Multi-Factor Alpha Validation Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the standalone Multi-Factor Alpha Validation Engine through Week 1-8 without changing the root PortfolioOS phase sequence.

**Architecture:** Implement a deterministic, local-only research validation engine under `projects/multifactor_alpha_validation/`. Each week adds one layer: contracts, signal panels, evidence, redundancy, shrinkage, allocation, survival, and packaging. Root integrations are limited to make/validation/docs entries and do not route outputs directly into Q2.

**Tech Stack:** Python 3.11, pydantic, pandas, numpy, pyyaml, pytest, static HTML.

---

### Task 1: Week 1 Contracts and Factor Specs

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/schema.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/data_contract.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/factor_library.py`
- Create: `projects/multifactor_alpha_validation/factor_specs/*.yaml`
- Create: `projects/multifactor_alpha_validation/docs/multifactor_alpha_validation_charter.md`
- Test: `projects/multifactor_alpha_validation/tests/test_week1_contracts.py`

- [ ] Write failing tests for FactorSpec validation, PIT timestamp rules, explicit abstain, fundamental reporting lag, disabled analyst revision, and non-claim boundary.
- [ ] Implement pydantic schemas and spec loader.
- [ ] Add 10 MVP factor specs.
- [ ] Add a spec validation script and smoke command.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week1 contracts`.

### Task 2: Week 2 Signal Panels and AlphaView Mapping

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/signal_builders/`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/alpha_view_mapper.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week2_signal_alpha_view.py`

- [ ] Write failing tests for signal panel timestamp fields, abstain behavior, fixed-horizon AlphaView mapping, event-reference SUE mapping, and disabled analyst revision handling.
- [ ] Implement deterministic local signal builders and AlphaView mapping.
- [ ] Add a signal-building smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week2 signals`.

### Task 3: Week 3 Evidence and Neutralization

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/backtest_kernel.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/neutralization.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/q1_evidence.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week3_evidence.py`

- [ ] Write failing tests for raw versus neutralized readouts, coverage, turnover, decay, benchmark separation, and exposure reporting.
- [ ] Implement deterministic cross-sectional evidence generation.
- [ ] Add Q1 evidence smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week3 evidence`.

### Task 4: Week 4 Redundancy and Marginal Value

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/redundancy_gate.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/marginal_value.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week4_redundancy.py`

- [ ] Write failing tests for clustering, residual contribution, cost-adjusted marginal value, and raw-IC-only promotion rejection.
- [ ] Implement correlation, cluster, residual, and decision outputs.
- [ ] Add redundancy smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week4 redundancy gate`.

### Task 5: Week 5 Shrinkage and Covariance

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/shrinkage.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/covariance.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week5_shrinkage_covariance.py`

- [ ] Write failing tests for stronger shrinkage on weak factors, rejected-factor non-revival, covariance condition improvement, duplicate reporting, and preregistered parameters.
- [ ] Implement posterior mean and covariance diagnostics.
- [ ] Add shrinkage smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week5 shrinkage`.

### Task 6: Week 6 Allocator and Zero-Weight Attribution

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/allocator.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/zero_weight_attribution.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week6_allocator.py`

- [ ] Write failing tests for nonnegative normalized weights, zero-weight reasons, sign-flip ranking, scale response, no-view versus zero-alpha, and no security-level output.
- [ ] Implement deterministic factor allocator and attribution.
- [ ] Add allocator smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week6 allocator`.

### Task 7: Week 7 Cost, Capacity, and Benchmark Survival

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/cost_capacity.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/benchmark_attribution.py`
- Create: `projects/multifactor_alpha_validation/tests/test_week7_survival.py`

- [ ] Write failing tests for negative net alpha blocking, capacity frontier bottlenecks, raw versus beta-adjusted benchmark separation, stop-layer attribution, and unavailable handling.
- [ ] Implement cost/capacity/benchmark survival artifacts.
- [ ] Add survival smoke script.
- [ ] Run focused tests, smoke, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week7 survival`.

### Task 8: Week 8 Registry, Report, Dashboard, and Validation

**Files:**
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/registry.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/reports.py`
- Create: `projects/multifactor_alpha_validation/src/multifactor_alpha_validation/dashboard.py`
- Create: `projects/multifactor_alpha_validation/README.md`
- Create/modify: `Makefile`, `VALIDATION.md`, `TASK_MEMORY.md`
- Create: `projects/multifactor_alpha_validation/tests/test_week8_packaging.py`

- [ ] Write failing tests for final statuses, stop layers, non-claims, dashboard read-only behavior, artifact manifest, and `factor-validate` support.
- [ ] Implement registry, report, dashboard, and manifest builders.
- [ ] Add `make factor-validate`.
- [ ] Update docs and memory.
- [ ] Run project tests, all smoke checks, `make factor-validate`, and `git diff --check`.
- [ ] Commit as `feat: add multifactor week8 packaging`.

