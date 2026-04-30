# ROADMAP.md

## Current State

Completed:

- Phase 0: Core PortfolioOS platform and research machinery are implemented through the existing platform phases recorded in `TASK_MEMORY.md`.
- Phase 1: Q1 standalone project shell exists at `projects/agentic_alpha_triage`.
- Phase 2: Q2 execution-aware optimizer project shell exists at `projects/execution_aware_optimizer`.
- Phase 3: Repository-level Codex workflow scaffold is installed with `AGENTS.md`, `ROADMAP.md`, `VALIDATION.md`, and `RUNBOOK.md`.
- Phase 4: Q2 PortfolioOS adapter hardening is complete with fixture-backed attribution mapping, independent alpha fixture, non-mutating cost-sensitivity scenarios, and layer-status documentation.
- Phase 5: Q1 contract examples are installed with schema-backed validation and no agent loops.

Current phase:

- Phase 6: Choose the next narrow project increment.

Deferred:

- autonomous Q1 LLM agent loops
- live SEC/FMP ingestion
- paid API calls
- broker routing
- production deployment
- full optimizer dual-value reporting until PortfolioOS exposes it
- liquidity slack reporting until PortfolioOS exports stable per-name participation diagnostics
- cloud automation setup

## Phase 4: Q2 Adapter Hardening

Goal:
Make the Execution-Aware Portfolio Optimizer produce useful PortfolioOS-backed rows where stable hooks exist, while keeping unavailable layers explicit and honest.

Why now:
Q2 already has a clean shell and schemas. The next useful step is to connect only safe, stable PortfolioOS outputs and keep missing diagnostics marked as gaps.

Tasks:

- [x] Add a fixture-backed Q2 adapter smoke test that maps existing PortfolioOS period attribution into ladder rows.
- [x] Add an alpha-input example fixture that does not depend on Q1.
- [x] Add a cost-sensitivity adapter design that can alter cost assumptions through config without mutating global PortfolioOS behavior.
- [x] Document exactly which Q2 layers are real, partial, or unavailable.

Acceptance criteria:

- Q2 tests pass.
- Relevant PortfolioOS optimizer/backtest tests pass.
- Q2 smoke scripts run with default non-execution config.
- README and `TASK_MEMORY.md` are updated.
- No fabricated performance numbers are introduced.
- No `src/portfolio_os` trading behavior is changed unless explicitly required and tested.

Do not:

- enable arbitrary PortfolioOS workflow execution by default
- require Q1 for Q2
- hide unavailable layers
- change optimizer objective math as part of Q2 project polish

## Phase 5: Q1 Contract Examples

Goal:
Add small example hypothesis, signal, and evaluation contract artifacts for Q1 without implementing agent loops.

Why now:
Q1 needs interview-readable examples that show the triage system is schema-first and leakage-aware.

Tasks:

- [x] Add one valid example hypothesis artifact.
- [x] Add one valid signal contract artifact.
- [x] Add one valid evaluation contract artifact.
- [x] Add a validation script that checks examples against schemas.

Acceptance criteria:

- Q1 tests pass.
- Example validation script passes.
- README explains how Q1 can export alpha scores to Q2 without creating a dependency.
- No live API calls are added.

## Next Phase

Phase 6 should decide whether to:

- build richer Q2 report tables from real PortfolioOS outputs, or
- continue with deeper Q1 evaluator contract examples.
