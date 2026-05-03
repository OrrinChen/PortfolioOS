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
- Phase 14: Q1 dry-run evaluator planner assembles the valid guidance-raise fixture family into a non-executing plan and rejects contract disagreements.
- Phase 15: Q1 evaluator CLI dry-run wrapper prints local evaluator plans as JSON from explicit fixture paths.
- Phase 16: Q1 rejected-plan JSON audit output lets the dry-run CLI emit structured local rejection metadata behind an explicit flag.
- Phase 17: Q1 evaluator-plan fixture manifest lists ready and rejected local dry-run targets without executing them.
- Phase 18: Q1 batch dry-run manifest wrapper emits ordered ready/rejected planner JSON for local manifest entries.
- Phase 19: Q1 manifest summary report counts ready, rejected, and expected-status mismatched entries without evaluator execution.
- Phase 20: Q1 evaluator batch contract note freezes the local dry-run boundary before real evaluator work.
- Phase 21: Evidence bundle schema validates PIT safety, leakage checks, planned tests, and deterministic JSON without trading outputs.
- Phase 22: Promotion gate contract creates a typed Q1-to-Q2 handoff decision without direct Q2 execution.
- Phase 23: Q2 execution evaluation matrix records scenario robustness across cost, participation, liquidity, constraints, and execution modes.
- Phase 24: Decision explainability taxonomy maps rejection, promotion, and Q2 unavailable statuses to structured audit explanations.
- Phase 25: Unified audit report generates a deterministic local demo report across Q1 checks, promotion, Q2 matrix, diagnostics, and reproducibility placeholders.
- Phase 26: Provenance manifests record command, git, config, input, output, environment, seed, schema, and stable content hashes.
- Phase 27: Structured trace events record local evaluation workflow milestones without credentials, orders, or trading instructions.
- Phase 28: CI-style validation targets, no-network guard, schema compatibility tests, and forbidden-output guards harden local regression checks.
- Phase 29: Local batch orchestrator evaluates candidates deterministically with failure isolation, partial reruns, retries, aggregation, and per-run provenance.
- Phase 30: Content-addressed cache keys and store support incremental local reruns with hit/miss status and config/input invalidation.

Current phase:

- Phase 31: Read-Only Service Layer.

Deferred:

- autonomous Q1 LLM agent loops
- live SEC/FMP ingestion
- paid API calls
- broker routing
- production deployment
- full optimizer dual-value reporting until PortfolioOS exposes it
- liquidity slack reporting until PortfolioOS exports stable per-name participation diagnostics
- cloud automation setup

## Strategic Direction: Audit-Ready Decision Evaluation Platform

PortfolioOS should be packaged as:

**PortfolioOS: Audit-Ready Decision Evaluation Platform**

The story is not "a quant strategy." The story is a typed, auditable, reproducible, execution-aware evaluation system for high-risk ML/quant decision workflows.

The product spine is:

```text
Q1 Alpha Triage
  -> Evidence Bundle
  -> Promotion Gate
  -> Q2 Execution-Aware Evaluation
  -> Audit Report
  -> Reproducible Demo
```

Q1 asks: **Is this alpha real?**

Q2 asks: **Can this alpha survive execution?**

The roadmap should prioritize ML evaluation, workflow orchestration, data contracts, governance, simulation, observability, and reliability engineering. It should not reopen alpha mining as the main thread.

Do not:

- continue adding new alpha ideas as the primary roadmap objective
- add live broker or auto-trading paths as a near-term goal
- merge A-share, US WRDS, Qlib, optimizer, Q1, and Q2 into one unfocused story
- add LLM agent loops as trusted evaluators
- describe the project as a production trading system

Preferred framing:

- research-to-execution evaluation platform
- audit-ready simulation platform
- contract-first ML/quant decision evaluation workflow

## Forward Roadmap Overview

| Phase | Focus | Interview Value |
| --- | --- | --- |
| Phase 20-22 | Q1 batch boundary, evidence bundle, promotion contract | Data contracts / governance |
| Phase 23-25 | Q2 evaluation matrix, explainability, unified audit report | ML evaluation pipeline |
| Phase 26-28 | Provenance, structured traces, CI hardening | Production engineering |
| Phase 29-30 | Local orchestration and content-addressed cache | Batch systems / reproducibility |
| Phase 31-32 | Read-only service layer and demo dashboard | System design polish |
| Phase 33-34 | One-command demo, architecture docs, case study | Interview-ready artifact |

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

- [x] Add a planner result schema for local dry-run evaluation plans.
- [x] Load the existing valid guidance-raise fixture family into one plan.
- [x] Reject plans when fixture, signal, event, or evaluation contracts disagree.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, or trading outputs are added.
- Planner output contains no realized returns or fabricated alpha results.

## Phase 15: Q1 Evaluator CLI Dry-Run Wrapper

Goal:
Add a tiny CLI wrapper that prints the Q1 dry-run evaluator plan as local JSON.

Why next:
Phase 14 exposed a library planner. A CLI dry-run wrapper would make the planner easier to inspect without introducing live services, agent loops, or evaluation results.

Tasks:

- [x] Add a script that calls `build_evaluator_plan` for explicit local fixture paths.
- [x] Print the plan JSON without realized returns, alpha performance, or trading outputs.
- [x] Add tests or smoke validation for the wrapper.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- CLI dry-run uses only local files supplied by path.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, or trading outputs are added.

## Phase 16: Q1 Rejected-Plan JSON Audit Output

Goal:
Let the Q1 dry-run CLI emit explicit rejected-plan JSON for local contract disagreements.

Why next:
The Phase 15 CLI prints ready plans. The next useful increment is to make rejection paths auditable without throwing away structured context.

Tasks:

- [x] Add a rejected-plan JSON schema or response wrapper.
- [x] Convert planner `ValueError` failures into CLI JSON when an explicit flag is set.
- [x] Preserve nonzero exit behavior unless the user opts into audit JSON.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- CLI rejected output contains no realized returns, alpha performance, orders, or trading outputs.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, or Q2 exports are added.

## Phase 17: Q1 Evaluator Plan Fixture Manifest

Goal:
Add a tiny local manifest that lists Q1 evaluator-plan fixtures for batch dry-run inspection without adding live ingestion or agent loops.

Why next:
Phase 16 makes individual ready and rejected planner outputs auditable. The next useful increment is a local-only manifest so multiple committed fixtures can be enumerated deterministically.

Tasks:

- [x] Define a minimal manifest schema for local evaluator fixture paths and event-registry directories.
- [x] Add one committed manifest that includes the valid guidance-raise fixture family and at least one rejected audit example.
- [x] Add tests that validate manifest loading without running live services, PortfolioOS workflows, or Q2 exports.
- [x] Update Q1 README and `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- Manifest output remains local planning/audit metadata only.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, trading outputs, or Q2 exports are added.

## Phase 18: Q1 Batch Dry-Run Manifest Wrapper

Goal:
Add a local-only batch wrapper that consumes the evaluator-plan manifest and emits one ready or rejected JSON payload per entry.

Why next:
Phase 17 can enumerate local dry-run targets. The next useful increment is a deterministic batch inspection command that reuses the existing single-entry planner and rejected audit wrapper.

Tasks:

- [x] Add a script or library function that iterates manifest entries deterministically.
- [x] Emit JSON Lines or a list payload containing only ready/rejected planner metadata.
- [x] Preserve explicit local-only behavior and never run live services, agent loops, PortfolioOS workflows, or Q2 exports.
- [x] Add tests and update Q1 README plus `TASK_MEMORY.md`.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- Batch output contains no realized returns, alpha performance, orders, or trading outputs.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, or Q2 exports are added.

## Phase 19: Q1 Manifest Summary Report

Goal:
Add a tiny local summary that counts ready, rejected, and expected-status mismatched manifest entries without adding evaluator execution.

Why next:
Phase 18 emits detailed batch payloads. A concise summary will make local audit output easier to scan while preserving the non-executing Q1 boundary.

Tasks:

- [x] Add a summary builder over `EvaluatorPlanBatchResult`.
- [x] Include manifest id, total entries, ready count, rejected count, and expected-status mismatch count.
- [x] Add tests and update Q1 README plus `TASK_MEMORY.md`.
- [x] Keep the output free of realized returns, alpha performance, orders, trading outputs, PortfolioOS workflow output, and Q2 exports.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- Summary output contains only local manifest/planner audit metadata.
- No live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, trading outputs, or Q2 exports are added.

## Phase 20: Q1 Evaluator Batch Contract Note

Goal:
Document the Q1 batch dry-run contract before any real evaluator implementation is considered.

Why next:
Q1 now has local single-plan, rejected-plan, manifest, batch, and summary audit paths. The next useful increment is to freeze the batch boundary in prose so future evaluator work cannot accidentally become an agent loop or trading workflow.

Tasks:

- [x] Add `projects/agentic_alpha_triage/docs/evaluator_batch_contract.md`.
- [x] Define batch input and output schemas in prose, including manifest path, fixture paths, event-registry directories, ready count, rejected count, mismatch count, rejection reasons, and referenced fixture paths.
- [x] Explicitly state that batch dry-run wrappers may output only planning/audit metadata.
- [x] Explicitly forbid realized returns, alpha performance, orders, trading instructions, PortfolioOS workflow output, and Q2 exports.
- [x] Update Q1 README, `RUNBOOK.md`, `VALIDATION.md`, and `TASK_MEMORY.md`.
- [x] Add or keep tests that verify forbidden fields do not appear in summary JSON.

Acceptance criteria:

- Q1 tests pass.
- Q1 example validation script passes.
- Q1 batch summary smoke passes:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --summary --indent 0
```

- Contract note states no live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, trading outputs, or Q2 exports.
- No evaluator execution or new data ingestion is added.

## Phase 21: Evidence Bundle Schema

Goal:
Turn Q1 planning output into a typed evidence package rather than scattered JSON/YAML/CLI artifacts.

Why next:
After Q1 batch boundaries are documented, the next platform step is a stable evidence object that can be validated, audited, and reviewed before any promotion to execution evaluation.

Tasks:

- [x] Create `projects/evidence_bundle/` as a standalone project area.
- [x] Add schema modules for evidence bundle metadata, PIT-safety checks, leakage checks, planned tests, coverage requirements, rejection reasons, and promotion eligibility.
- [x] Add valid and rejected example bundles, including forward-return leakage and missing/unsafe timestamp cases.
- [x] Add validation and deterministic JSON serialization tests.
- [x] Document that evidence bundles must not contain trading recommendations, orders, broker output, live performance, or hidden Q2 results.

Acceptance criteria:

- Valid bundle validates.
- Forward-return leakage bundle is rejected.
- Missing timestamp bundle is rejected.
- Event anchor before signal visibility is rejected.
- Deterministic JSON output is tested.
- No live services, broker calls, trading instructions, or Q2 execution are added.

## Phase 22: Promotion Gate Contract

Goal:
Separate Q1 research validation from Q2 execution evaluation with an explicit promotion contract.

Why next:
Q1 readiness should be necessary but not sufficient for execution-aware evaluation. A promotion gate prevents unsafe coupling between research validation and trading simulation.

Tasks:

- [x] Create `projects/promotion_gate/` as a standalone project area.
- [x] Define `PromotionDecision` and `Q2InputContract` schemas.
- [x] Implement gate checks for PIT safety, leakage safety, minimum coverage, event timestamp alignment, horizon sanity, and cost-assumption presence.
- [x] Ensure passing decisions generate only Q2 input contracts and do not run Q2.
- [x] Add promoted and rejected examples plus tests.

Acceptance criteria:

- Ready Q1 evidence can produce `promote_to_execution_eval` only when all gate checks pass.
- Unsafe evidence produces `reject` or `needs_more_evidence`.
- Forbidden outputs are checked and recorded.
- Q1 does not import or execute Q2 workflows.

## Phase 23: Q2 Execution Evaluation Matrix

Goal:
Extend Q2 from a single execution-aware report into a scenario matrix across cost, liquidity, participation, and constraint settings.

Why next:
The platform should answer whether a promoted signal survives execution assumptions, not just whether one backtest row looks acceptable.

Tasks:

- [x] Add `execution_matrix.py`, `scenario_grid.py`, and `robustness_summary.py` under `projects/execution_aware_optimizer/src/execution_aware_optimizer/`.
- [x] Support scenario dimensions for cost bps, participation rate, liquidity bucket, constraint level, and execution mode.
- [x] Emit `execution_matrix.csv`, `robustness_summary.json`, and markdown report sections.
- [x] Record source config hash for every scenario.
- [x] Return structured unavailable reasons when a layer or scenario cannot execute.

Acceptance criteria:

- Unavailable layers remain explicit and are never filled with fabricated results.
- Scenario rows include config hashes and status.
- Robustness summary handles observed and unavailable rows.
- Default project behavior remains non-execution unless explicitly opted in.

## Phase 24: Decision Explainability Layer

Goal:
Make every rejection, promotion, unavailable status, and execution-risk decision explainable.

Why next:
Audit-ready systems should turn failed experiments into actionable records rather than silent failures or opaque statuses.

Tasks:

- [x] Add explanation taxonomy modules for leakage, timestamp, coverage, promotion, Q2 availability, cost retention, and execution risk.
- [x] Produce structured explanations with decision, primary reason, severity, human-readable message, and fix hint.
- [x] Integrate explanations with Q1 rejections, promotion decisions, and Q2 unavailable rows where stable hooks exist.
- [x] Add tests for critical rejection categories.

Acceptance criteria:

- Forward-return leakage has a critical explanation.
- Missing timestamp and unsafe anchor cases have deterministic explanations.
- Q2 unavailable rows can carry structured reason metadata.
- Explanation output contains no trading instructions or fabricated performance.

## Phase 25: Unified Audit Report

Goal:
Generate one interview-readable report that follows a candidate from hypothesis to Q1 checks, promotion decision, Q2 execution evaluation, and final audit record.

Why next:
The project needs a single report that communicates the platform in three minutes without relying on scattered artifacts.

Tasks:

- [x] Add a report builder for `reports/demo_audit_report.md`.
- [x] Include sections for hypothesis, signal contract, PIT safety, leakage checks, evaluation plan, promotion decision, execution-aware evaluation, cost sensitivity, constraint diagnostics, final decision, and reproducibility manifest placeholder.
- [x] Add deterministic fixture data for one promoted-like case and one rejected leakage case.
- [x] Add snapshot or golden-output tests.

Acceptance criteria:

- Report generation runs without live services or broker calls.
- Rejected cases do not enter Q2 execution.
- Report does not fabricate numbers.
- Output is deterministic.

## Phase 26: Run Provenance / Reproducibility Manifest

Goal:
Make every evaluation run traceable to code, config, inputs, environment, command, and artifacts.

Why next:
Audit readiness depends on explaining exactly how a result was produced and whether it can be replayed.

Tasks:

- [x] Add provenance modules for manifest writing, hashing, environment capture, and artifact indexing.
- [x] Record git SHA, dirty state, command, config path/hash, input hashes, output hashes, Python version, dependency snapshot, timestamp, runner version, random seed, and schema version.
- [x] Attach manifest summaries to reports where applicable.
- [x] Add tests for stable hashes and invalidation when config or inputs change.

Acceptance criteria:

- Same fixture and config produce stable manifest hash.
- Config changes alter manifest hash.
- Input changes alter manifest hash.
- No secrets, credentials, or paid data payloads are captured.

## Phase 27: Observability / Structured Trace

Goal:
Add structured event traces for evaluation workflows.

Why next:
The platform should be debuggable like a real service, not just a collection of scripts.

Tasks:

- [x] Add structured event, logger, metrics, and trace-writer modules.
- [x] Support trace events such as `bundle_loaded`, `schema_validated`, `leakage_check_failed`, `promotion_decision_created`, `q2_scenario_unavailable`, and `report_written`.
- [x] Add `--trace-jsonl` support to relevant local CLIs.
- [x] Add tests that assert key events appear and forbidden sensitive/trading fields do not.

Acceptance criteria:

- Trace JSONL is deterministic for fixture runs aside from allowed timestamps.
- Critical Q1/Q2 audit events are represented.
- Trace output contains no credentials, orders, or trading instructions.

## Phase 28: CI / Regression Hardening

Goal:
Make the repository validate like a maintainable team project.

Why next:
Before one-command demos and broader packaging, the validation surface should be explicit and repeatable.

Tasks:

- [x] Add `make test`, `make lint`, `make validate-examples`, `make demo`, and `make audit-report` targets where compatible with the repo.
- [x] Add golden-output regression tests for key reports.
- [x] Add schema backward-compatibility tests.
- [x] Add no-network safety checks for local validation.
- [x] Add forbidden-output tests across Q1, promotion, Q2, and reports.

Acceptance criteria:

- `make validate` or documented equivalent runs unit tests, example validation, CLI smoke checks, report generation, diff checks, and no-network guard.
- Golden outputs fail on unintended report drift.
- No-network mode blocks live external service use.

## Phase 29: Batch Scaling / Local Orchestrator

Goal:
Evaluate multiple candidate bundles deterministically with failure isolation and aggregation.

Why next:
This turns the platform from single-candidate demos into a local batch evaluation system without introducing distributed-system overhead.

Tasks:

- [x] Add a local batch runner, scheduler, retry policy, and result store.
- [x] Support deterministic ordering, partial reruns, failure isolation, and aggregation.
- [x] Classify rejected, promoted, unavailable, and failed candidates.
- [x] Write one provenance manifest per run.

Acceptance criteria:

- Batch runs continue after individual candidate failures.
- Partial reruns are deterministic.
- Aggregated summaries include candidate-level statuses and failure reasons.
- No Kubernetes, cloud service, broker path, or live data dependency is added.

## Phase 30: Incremental Rerun / Content-Addressed Cache

Goal:
Avoid rerunning unchanged candidate evaluations while preserving auditability.

Why next:
Reproducible evaluation systems should be efficient without hiding when inputs, configs, or code changed.

Tasks:

- [x] Add content-addressed cache modules for cache key generation, storage, and invalidation.
- [x] Include schema version, code version, input hash, config hash, runner version, and relevant seed in cache keys.
- [x] Surface cache hit/miss in reports and provenance.
- [x] Add tests for miss, hit, config invalidation, and bundle invalidation.

Acceptance criteria:

- First run is a cache miss.
- Identical second run is a cache hit.
- Config changes invalidate cache.
- Bundle/input changes invalidate cache.

## Phase 31: Read-Only Service Layer

Goal:
Expose evaluation artifacts through a read-only API without creating trading or execution endpoints.

Why next:
A service layer helps system-design discussion while preserving compliance boundaries.

Tasks:

- [ ] Add a small read-only service package if dependencies are justified.
- [ ] Provide endpoints for health, runs, bundles, reports, and decisions.
- [ ] Use fixture artifact storage in tests.
- [ ] Document forbidden endpoints such as trade, order, and broker actions.

Acceptance criteria:

- API only reads artifacts.
- API does not trigger live services, brokers, Q2 runs, or trading workflows.
- OpenAPI docs are generated if using FastAPI.
- Tests use local fixture artifacts.

## Phase 32: Demo Dashboard

Goal:
Provide a lightweight UI for browsing evaluation runs and reports.

Why next:
This is optional polish for interviews after the core pipeline and artifacts are stable.

Tasks:

- [ ] Build a small dashboard over local artifacts.
- [ ] Show candidate list, Q1 status, promotion decision, Q2 execution matrix, cost sensitivity, audit report, and reproducibility manifest.
- [ ] Keep the dashboard read-only.

Acceptance criteria:

- Dashboard reads local artifacts only.
- No live services, broker calls, or workflow-triggering actions are exposed.
- Demo fixtures render without manual setup beyond documented commands.

## Phase 33: One-Command PortfolioOS Demo

Goal:
Package the platform into one deterministic local demo command.

Why next:
The final interview artifact should produce all key outputs from one command.

Tasks:

- [ ] Add `make demo` or an equivalent script.
- [ ] Generate Q1 summary, evidence bundle, promotion decision, Q2 execution matrix, audit report, run manifest, and trace JSONL.
- [ ] Include one valid/promoted-like case and one invalid forward-return leakage case.
- [ ] Add smoke and snapshot tests.

Acceptance criteria:

- One command writes outputs under `outputs/demo/`.
- Valid case reaches execution-aware evaluation only through the promotion contract.
- Invalid leakage case is rejected before Q2.
- Demo is deterministic and local-only.

## Phase 34: README / Architecture / Case Study

Goal:
Package the project for review, interviews, and portfolio presentation.

Why next:
After the platform has deterministic outputs, the top-level explanation should make the engineering story obvious.

Tasks:

- [ ] Rewrite top-level README around the audit-ready decision evaluation platform framing.
- [ ] Add architecture diagram for Q1 alpha triage -> evidence bundle -> promotion gate -> Q2 execution evaluation -> audit report.
- [ ] Add quickstart, example outputs, safety boundaries, and validation commands.
- [ ] Add concise case studies for promoted-like and rejected candidates.
- [ ] Preserve clear warnings that this is not a production trading system.

Acceptance criteria:

- README explains the problem, solution, architecture, quickstart, example outputs, safety boundaries, and validation.
- Case studies can be reproduced locally.
- No claims of production trading or alpha discovery success are added.

## Optional Later Phases

Potential follow-on work after Phase 34:

- service hardening
- dashboard polish
- batch scaling beyond local fixtures
- additional read-only artifact APIs
- broader provenance integration across legacy PortfolioOS CLIs
