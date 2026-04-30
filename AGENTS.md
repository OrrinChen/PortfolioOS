# AGENTS.md

You are working on this repository as an autonomous coding agent.

## Project Goal

PortfolioOS is a compliance-aware portfolio rebalance, backtest, scenario, approval, execution-simulation, TCA, and research CLI platform.

Before making changes, read:

1. `README.md`
2. `ROADMAP.md`
3. `TASK_MEMORY.md`
4. `VALIDATION.md`
5. `RUNBOOK.md`

Your job is to advance the next incomplete roadmap phase. Do not invent unrelated features.

## Current Project Boundaries

- PortfolioOS core platform lives under `src/portfolio_os`.
- Q1 lives under `projects/agentic_alpha_triage` and asks: "Is this alpha real?"
- Q2 lives under `projects/execution_aware_optimizer` and asks: "Can this alpha survive execution?"
- Q1 and Q2 must remain separate project stories.
- Q2 may reuse PortfolioOS APIs through explicit adapters.
- Q1 may later export an alpha-score file for Q2, but Q2 must not depend on Q1 agent loops or data collection.

## Hard Constraints

Do not:

- turn this repo into a generic trading bot
- merge Q1 and Q2 into one monorepo narrative
- let agent code directly modify trading logic or run arbitrary PortfolioOS workflows
- change project direction without explicit instruction
- rewrite large modules unnecessarily
- add heavy dependencies unless justified
- hide failures or fabricate backtest results
- generate misleading reports
- commit secrets, credentials, tokens, paid data, or local private data
- hardcode API keys
- run live broker, paid API, or external data workflows unless explicitly requested
- run destructive git or filesystem actions without explicit approval

## Workflow

Before starting:

1. Run `git status`.
2. Read `ROADMAP.md`.
3. Read `TASK_MEMORY.md`.
4. Identify the next incomplete phase.
5. Inspect relevant files before editing.

During work:

1. Make small, testable changes.
2. Keep interfaces modular.
3. Prefer adapters over rewrites.
4. Add or update tests for behavioral changes.
5. Update README when user-facing behavior changes.
6. Update `TASK_MEMORY.md` with what changed and what was verified.

Validation:

1. Run the commands listed in `VALIDATION.md`.
2. Always run relevant unit tests.
3. Run smoke tests for touched CLI, script, or project paths.
4. Run `git diff --check`.

Commit:

- Commit only when the user asks for a commit or the active task explicitly requires it.
- Commit each completed phase separately.
- Use clear commit messages:
  - `feat: ...`
  - `fix: ...`
  - `docs: ...`
  - `test: ...`

## Stopping Rules

Only stop and ask for help if:

- tests cannot pass after reasonable debugging
- product direction is ambiguous
- implementation risks data loss
- credentials, paid APIs, broker access, or private data are required
- a task would violate the hard constraints

When stopping, report:

- what was attempted
- what passed
- what failed
- the exact blocker
- the recommended next step

## Reporting Back

At the end of a task, report:

- files changed
- tests and smoke checks run
- what was intentionally stubbed or deferred
- known limitations
- recommended next roadmap phase
