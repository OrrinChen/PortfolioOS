# ROADMAP.md

## Current State

Completed:

- Phase 0: Core PortfolioOS platform and research machinery are implemented through the existing platform phases recorded in `TASK_MEMORY.md`.
- Phase 1: Q1 standalone project shell exists at `projects/agentic_alpha_triage`.
- Phase 2: Q2 execution-aware optimizer project shell exists at `projects/execution_aware_optimizer`.
- Phase 3: Repository-level Codex workflow scaffold is installed with `AGENTS.md`, `ROADMAP.md`, `VALIDATION.md`, and `RUNBOOK.md`.
- Phase 4: Q2 PortfolioOS adapter hardening is complete with fixture-backed attribution mapping, independent alpha fixture, non-mutating cost-sensitivity scenarios, and layer-status documentation.
- Phase 5: Q1 contract examples are installed with schema-backed validation and no agent loops.
- Phase 6: Q2 report tables summarize observed PortfolioOS-backed ladder rows without fabricating unavailable values.
- Phase 7: Q2 cost-sensitivity CSV rows are parsed into typed records and summarized in markdown reports.
- Phase 8: Q1 evaluator example fixtures demonstrate leakage-safe evaluator expectations and rejected unsafe examples.
- Phase 9: Q1 event-registry examples demonstrate timestamp-safe event representation and rejected unsafe timestamps.
- Phase 10: Q2 executed adapter fixture planning identified a safe local-only PortfolioOS-backed fixture scope.
- Phase 11: Q2 executed adapter fixture verifies local PortfolioOS-backed raw and full execution-aware rows through an explicit library call.
- Phase 12: Q2 executed fixture report polish adds explicit adapter status, layer coverage, and an opt-in local executed report smoke path.
- Phase 13: Q1 evaluator runner design defines the local-only dry-run planner boundary without adding agent loops or live ingestion.

Current phase:

- Phase 14: Q1 Dry-Run Evaluator Planner.

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

## Phase 6: Q2 Real-Output Report Tables

Goal:
Make the Execution-Aware Portfolio Optimizer report more interview-readable by summarizing actual ladder rows into gross/net and alpha-decay tables.

Why now:
Q2 already records partial PortfolioOS-backed rows and explicit unavailable layers. The next useful polish is to make the generated report explain observed decay without inventing missing performance numbers.

Tasks:

- [x] Add gross vs net summary tables grouped by ladder layer.
- [x] Add alpha-decay summary versus `raw_top_alpha_equal_weight`.
- [x] Preserve explicit `Not available` values for unavailable rows.
- [x] Add tests proving report summaries do not fabricate unavailable layer values.

Acceptance criteria:

- Q2 tests pass.
- Q2 smoke scripts pass.
- Generated report includes the new summary tables.
- README and `TASK_MEMORY.md` are updated.
- No PortfolioOS core trading behavior is changed.

## Phase 7: Q2 Cost-Sensitivity Report Reader

Goal:
Let the Q2 markdown report consume cost-sensitivity CSV rows and summarize gross/net, turnover, and cost drag by cost assumption when executed results exist.

Why next:
The cost-sensitivity script already emits planned rows with explicit cost levels. The next useful increment is to make the report understand those rows without running PortfolioOS by default or fabricating unavailable results.

Tasks:

- [x] Add a typed reader for Q2 cost-sensitivity result CSVs.
- [x] Render a cost-sensitivity summary grouped by cost bps and layer.
- [x] Preserve unavailable rows as `Not available`.
- [x] Add tests for executed rows and default non-execution rows.

Acceptance criteria:

- Q2 tests pass.
- Q2 smoke scripts pass.
- Existing default cost-sensitivity CSV remains honest and unavailable until explicit execution is enabled.
- README and `TASK_MEMORY.md` are updated.
- No PortfolioOS core trading behavior is changed.

## Phase 8: Q1 Evaluator Example Fixtures

Goal:
Add small Q1 evaluator fixture examples that demonstrate leakage and placebo-test expectations without implementing agent loops or live data ingestion.

Why next:
Q2 reporting is now interview-readable enough for default non-execution flows. Q1 can next become clearer by showing how evaluator contracts should be represented before any autonomous hypothesis loop exists.

Tasks:

- [x] Add one valid evaluator fixture that references the existing guidance-raise example.
- [x] Add one invalid/leakage-risk fixture for schema or contract-negative testing.
- [x] Add tests that validate the fixture loader rejects unsafe evaluator examples.
- [x] Document how Q1 evaluator examples differ from Q2 execution checks.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- README and `TASK_MEMORY.md` are updated.
- No live API calls, agent loops, or trading workflow execution are added.

## Phase 9: Q1 Event Registry Example Fixtures

Goal:
Add small event-registry examples that show how timestamped Q1 events should be represented before evaluator fixtures consume them.

Why next:
Phase 8 documents evaluator expectations. The next missing Q1 artifact is a concrete event registry example that makes event availability, source timestamps, and tradability anchors explicit.

Tasks:

- [x] Add one valid event-registry example for the guidance-raise story.
- [x] Add one invalid event-registry example that demonstrates a missing or unsafe timestamp.
- [x] Add validation tests for event-registry examples.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- No live API calls, agent loops, or trading workflow execution are added.

## Phase 10: Q2 Executed Adapter Fixture Planning

Goal:
Decide whether Q2 should add a tiny executed PortfolioOS adapter fixture, and if yes, document the exact safe non-live fixture scope before implementation.

Why next:
Q1 now has schema-backed hypothesis, signal, evaluation, evaluator, and event-registry examples. The next useful increment is to determine whether Q2 needs one small executed fixture to demonstrate PortfolioOS-backed rows without opening arbitrary workflow execution.

Tasks:

- [x] Inspect existing Q2 adapter tests and smoke scripts.
- [x] Identify one safe fixture path, or document why executed fixtures remain deferred.
- [x] Update Q2 README and `TASK_MEMORY.md` with the decision.
- [x] Add tests only if the chosen fixture path is implemented.

Acceptance criteria:

- Relevant Q2 tests pass if Q2 code changes.
- No live API calls, paid data, or arbitrary PortfolioOS workflow execution are added.
- No fabricated performance numbers are introduced.

## Phase 11: Q2 Executed Adapter Fixture

Goal:
Implement the selected local-only Q2 executed adapter fixture without changing default non-execution project behavior.

Why next:
Phase 10 confirmed a safe fixture path: a direct library call to `run_alpha_decay_ladder` using the existing local backtest manifest and only PortfolioOS period-attribution-backed layers.

Tasks:

- [x] Add one focused Q2 test that runs the local PortfolioOS backtest adapter through `run_alpha_decay_ladder`.
- [x] Assert raw and full execution-aware rows have observed values.
- [x] Assert intermediate layers remain explicitly unavailable.
- [x] Confirm default Q2 configs still keep `allow_portfolioos_run=false`.
- [x] Update Q2 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q2 tests pass.
- Q2 default smoke scripts pass.
- No live API calls, paid data, broker calls, or arbitrary CLI workflow execution are added.
- No fabricated intermediate layer diagnostics are introduced.

## Phase 12: Q2 Executed Fixture Report Polish

Goal:
Make the Q2 report path clearer for explicitly executed local fixture rows without enabling PortfolioOS execution by default.

Why next:
Phase 11 proves the adapter can map real local PortfolioOS attribution rows for the raw and full execution-aware layers. The next useful increment is to make that evidence easy to inspect in reports while still marking intermediate layers unavailable.

Tasks:

- [x] Add or document a local-only report smoke path for explicit executed fixture rows.
- [x] Ensure report tables clearly distinguish observed rows from unavailable layers.
- [x] Keep default configs and smoke scripts non-execution.
- [x] Update Q2 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q2 tests pass.
- Q2 default smoke scripts pass.
- Any executed report fixture uses only local sample data and explicit opt-in config.
- No fabricated intermediate diagnostics are introduced.

## Phase 13: Q1 Evaluator Runner Design

Goal:
Design the next Q1 evaluator runner boundary without adding autonomous agent loops or live data ingestion.

Why next:
Q2 now has a narrow PortfolioOS-backed executed fixture path. The next useful increment is to make Q1 clearer by defining how schema-backed hypothesis, signal, event, and evaluation fixtures should be assembled into a leakage-safe local evaluator runner.

Tasks:

- [x] Inspect existing Q1 schemas, examples, and validation script.
- [x] Document a local-only evaluator runner contract.
- [x] Add tests only if implementing a narrow fixture loader or dry-run planner.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- No live FMP/SEC calls, LLM agent loops, or trading workflow execution are added.
- Q1 remains independent from Q2.

## Phase 14: Q1 Dry-Run Evaluator Planner

Goal:
Implement a tiny local dry-run planner that assembles validated Q1 fixture artifacts into a non-executing evaluation plan.

Why next:
Phase 13 defined the runner boundary. The next useful step is to prove the boundary with one schema-backed dry-run plan while continuing to reject unsafe fixtures.

Tasks:

- [ ] Add a planner result schema for local dry-run evaluation plans.
- [ ] Load the existing valid guidance-raise fixture family into one plan.
- [ ] Reject plans when fixture, signal, event, or evaluation contracts disagree.
- [ ] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, or trading outputs are added.
- Planner output contains no realized returns or fabricated alpha results.

## Next Phase

After Phase 14, consider a Q1 evaluator CLI dry-run wrapper.
