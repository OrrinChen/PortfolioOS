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
- Phase 31: Framework-neutral read-only artifact service exposes local runs, bundles, reports, and decisions without write or trading routes.
- Phase 32: Static read-only dashboard renderer displays local demo artifacts without service dependencies or workflow triggers.
- Phase 33: One-command local demo writes Q1, promotion, Q2, audit, manifest, trace, and dashboard artifacts under `outputs/demo/`.
- Phase 34: README now packages the audit-ready decision evaluation platform with architecture, quickstart, safety boundaries, and case studies.
- Phase 35: Typed AlphaView contract defines alpha as timestamped, PIT-safe, horizon-aware predictive view with explicit abstain semantics.
- Phase 36: Event-aware evaluation kernel records deterministic evidence-plan artifacts for event-window and to-next-announcement AlphaViews.
- Phase 37: Alpha Projection Bridge v2 projects typed AlphaViews into rebalance-period expected-return panels with diagnostics and explicit abstain reports.
- Phase 38: Promotion Gate v2 emits expected-return-panel Q2 input contracts from EvidenceBundle, AlphaView, and projection manifests without running Q2.
- Phase 39: Q2 typed alpha execution matrix consumes Q2InputContract v2 and projection artifacts while keeping execution rows explicitly unavailable.
- Phase 40: Paper overlay readiness aggregates local paper observations as execution-environment calibration only.
- Phase 41: SUE typed alpha pilot writes the local AlphaView -> Evidence -> Projection -> Promotion v2 -> Q2 matrix artifact chain.
- Phase 42: Typed Alpha Demo v2 adds `make demo-v2` and read-only dashboard sections for typed alpha artifacts.
- Phase 43: Typed Alpha Release Candidate Hardening locks demo-v2 schema versions, release manifest, forbidden-output guard coverage, and release notes.
- Phase 44: Demo v2 Golden Snapshot Tests lock required artifact shapes, release-manifest keys, dashboard sections, and unavailable-row semantics.
- Phase 45: Typed Alpha Closeout Report writes a deterministic closeout memo with explicit proof, non-proof, limitation, and reproducibility sections.
- Phase 46: Dashboard Readability Polish adds first-screen status, typed-alpha chain, artifact links, manifest summary, and unavailable-artifact messaging without workflow controls.
- Phase 47: Typed Q2 Execution Adapter v0 connects typed Q2 input artifacts to the existing local PortfolioOS fixture adapter and emits observed/unavailable rows without live data, orders, brokers, or production approval.
- Phase 48: Typed Expected-Return Injection Fixture proves projected expected-return panels can reach a local optimizer input snapshot without live data, brokers, orders, or production approval.
- Phase 49: Typed Optimizer Response Acceptance Suite proves local optimizer response to positive, scaled, sign-flipped, zero-alpha, and explicit-abstain typed expected-return panels.
- Phase 50: SUE Typed Q2 Survival Matrix v1 proves SUE expected-return reaches the local optimizer input path and maps configured local Q2 rows as observed, including the risk-controlled fixture hook.
- Phase 51: SUE Execution-Survival Attribution Report explains SUE survival layers and records the local fixture result as observed, while keeping Phase 52 as a diagnostic and not production approval.
- Phase 52: Revision Marginal-Value Gate archives revision as a real shadow branch in the local fixture because it does not clear the SUE-adjusted cost-aware marginal threshold.
- Phase 55: Alpha Registry v2 / Decision State Machine freezes alpha statuses and typed-chain stop layers across SUE, revision, composite, old alpha package, Qlib revision, residual momentum, A-share, and leakage fixtures.
- FD-1: Factor Discovery Sandbox teaching baseline replication is complete with
  deterministic local fixture data, QQQ benchmark reporting, 29 price-volume
  factors, IC/ICIR tables, correlation matrix, ICIR weights, and explicit
  survivorship-biased educational-only labels.
- FD-2: FactorSpec conversion is complete with 29 committed price-volume
  FactorSpec YAML files, timestamp contracts, explicit abstain coverage rules,
  `no_view != zero_alpha`, and a validation smoke target.
- FD-3: Rolling ICIR and OOS weighting is complete with prior-history-only
  rolling ICIR weights, OOS factor score panel, no same-close trading, and a
  separate research-mode OOS backtest report.
- FD-4: Redundancy / Marginal-Value Gate is complete with factor clusters,
  residual IC, incremental spread/net/turnover/cost diagnostics, and explicit
  marginal-value decisions.

Current phase:

- Phase 65: PortfolioOS v1 Research-Audit Release.

Deferred:

- autonomous Q1 LLM agent loops
- live SEC/FMP ingestion
- paid API calls
- broker routing
- production deployment
- live paper overlay calibration runs unless explicitly requested
- full optimizer dual-value reporting until PortfolioOS exposes it
- liquidity slack reporting until PortfolioOS exports stable per-name participation diagnostics
- cloud automation setup
- paper canary phases unless explicitly approved by a human
- new alpha research unless imported through a typed research contract
- Factor Discovery Sandbox implementation until an explicit Phase 64 research
  import charter is approved

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

## Next Strategic Direction: Typed Alpha View / Research-to-Paper Closed Loop

The Phase 20-34 platform packaging roadmap is complete. The next roadmap should
not continue horizontal platform polish. The next useful buildout is to make the
alpha layer more realistic by changing alpha from a static score into a typed
predictive claim with mechanism, timestamps, horizon, coverage, abstain,
decay, confidence, capacity, cost sensitivity, PIT safety, and provenance.

New spine:

```text
Mechanism Alpha
  -> Typed Alpha View
  -> Event / State / Fixed Horizon Evidence
  -> Promotion Gate v2
  -> Optimizer Projection
  -> Q2 Execution Survival
  -> Paper Overlay Readiness
  -> Audit Dashboard v2
```

The core rule is:

```text
no_view != zero_alpha
```

Missing alpha coverage must be represented as explicit abstain, not silently
encoded as zero alpha. This preserves the difference between "no predictive
view exists" and "the expected return view is neutral."

Do not:

- keep adding README, dashboard, or generic demo polish as the main work
- rerun near-identical model retries as the primary roadmap
- treat FMP estimate history as PIT-safe analyst revision data
- bring A-share back into the mainline without an explicit new tranche decision
- promote calibrated paper-overlay parameters into production configs
- treat paper calibration as alpha validation
- convert SUE or revision evidence into live trading recommendations
- let typed alpha work call brokers, place orders, or bypass promotion gates

## Forward Roadmap Overview

| Phase | Focus | Interview Value |
| --- | --- | --- |
| Phase 20-22 | Q1 batch boundary, evidence bundle, promotion contract | Data contracts / governance |
| Phase 23-25 | Q2 evaluation matrix, explainability, unified audit report | ML evaluation pipeline |
| Phase 26-28 | Provenance, structured traces, CI hardening | Production engineering |
| Phase 29-30 | Local orchestration and content-addressed cache | Batch systems / reproducibility |
| Phase 31-32 | Read-only service layer and demo dashboard | System design polish |
| Phase 33-34 | One-command demo, architecture docs, case study | Interview-ready artifact |
| Phase 35-38 | Typed alpha view, event evidence, projection, promotion v2 | Alpha realism / contracts |
| Phase 39-42 | Typed Q2 matrix, paper overlay, pilot, dashboard v2 | Research-to-paper closed loop |
| Phase 43-46 | Typed alpha release hardening, golden checks, closeout, dashboard readability | Demo reliability / audit handoff |
| Phase 47 | Typed Q2 execution adapter v0 | Closing the typed-alpha execution-observation gap |

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

- [x] Add a small read-only service package if dependencies are justified.
- [x] Provide endpoints for health, runs, bundles, reports, and decisions.
- [x] Use fixture artifact storage in tests.
- [x] Document forbidden endpoints such as trade, order, and broker actions.

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

- [x] Build a small dashboard over local artifacts.
- [x] Show candidate list, Q1 status, promotion decision, Q2 execution matrix, cost sensitivity, audit report, and reproducibility manifest.
- [x] Keep the dashboard read-only.

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

- [x] Add `make demo` or an equivalent script.
- [x] Generate Q1 summary, evidence bundle, promotion decision, Q2 execution matrix, audit report, run manifest, and trace JSONL.
- [x] Include one valid/promoted-like case and one invalid forward-return leakage case.
- [x] Add smoke and snapshot tests.

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

- [x] Rewrite top-level README around the audit-ready decision evaluation platform framing.
- [x] Add architecture diagram for Q1 alpha triage -> evidence bundle -> promotion gate -> Q2 execution evaluation -> audit report.
- [x] Add quickstart, example outputs, safety boundaries, and validation commands.
- [x] Add concise case studies for promoted-like and rejected candidates.
- [x] Preserve clear warnings that this is not a production trading system.

Acceptance criteria:

- README explains the problem, solution, architecture, quickstart, example outputs, safety boundaries, and validation.
- Case studies can be reproduced locally.
- No claims of production trading or alpha discovery success are added.

## Phase 35: Typed Alpha View Contract

Status:
Next.

Goal:
Define alpha as a typed predictive view, not a static score.

Why next:
Q1, Evidence Bundle, Promotion Gate, Q2, audit reporting, provenance, trace,
batch, cache, service, dashboard, and one-command demo are now packaged. The
weak point is that alpha still enters the platform as `alpha_score`. The next
system boundary should make the alpha expression itself typed, timestamped,
PIT-safe, horizon-aware, and abstain-aware.

Target files:

- `projects/alpha_view_contract/`
- `src/portfolio_os/alpha/view_contract.py`
- `tests/test_alpha_view_contract.py`

Core schema:

```text
AlphaView
  alpha_view_id
  family_id
  mechanism_type: event | state_transition | fixed_horizon | residual_factor
  universe_id
  signal_timestamp
  visibility_timestamp
  tradable_timestamp
  anchor_event_timestamp
  horizon_type: event_window | to_next_event | rebalance_period | state_exit
  holding_window
  decay_policy
  coverage_mask
  abstain_policy
  expected_return_view
  confidence_view
  capacity_view
  cost_sensitivity_view
  pit_safety_report
  provenance
```

Tasks:

- [x] Add `AlphaView` schema and deterministic JSON load/dump helpers.
- [x] Add valid fixtures for event SUE, revision-to-next-announcement, and residual momentum calibration views.
- [x] Add rejected forward-return leakage fixture.
- [x] Represent explicit abstain separately from zero-alpha.
- [x] Forbid orders, broker output, live performance, and trading recommendations.
- [x] Document the contract boundary.

Acceptance criteria:

- AlphaView JSON load/dump is deterministic.
- Valid event, revision, residual, and rejected leakage fixtures are covered.
- `explicit_abstain` cannot be serialized as ordinary zero alpha.
- Forbidden trading/live-output fields are rejected.
- `make validate` passes.

Do not:

- write an evaluator before the AlphaView contract exists
- use FMP estimate history as PIT-safe revision data
- generate trading instructions

## Phase 36: Event-Aware Evaluation Kernel

Goal:
Evaluate event alpha with event-time and to-next-event horizons instead of
forcing event mechanisms through fixed monthly horizon logic.

Why next:
SUE and analyst revision mechanisms need event-aware labels, timestamp
visibility, entry offsets, half-life diagnostics, placebo checks, and overlap
diagnostics. This kernel should answer whether an event mechanism is real before
projection or Q2 execution survival.

Target files:

- `src/portfolio_os/alpha/event_evaluation.py`
- `projects/alpha_view_contract/examples/event_sue_pead_view.json`
- `projects/alpha_view_contract/examples/event_revision_view.json`
- `tests/test_event_alpha_evaluation_contract.py`

Outputs:

- `event_evidence_bundle.json`
- `event_window_grid.csv`
- `event_half_life_summary.json`
- `event_overlap_diagnostics.json`
- `pit_visibility_report.json`
- `placebo_report.json`

Tasks:

- [x] Support `event_window` labels such as SUE `[+2,+2]`, `[+2,+3]`, and `[+2,+22]`.
- [x] Support `to_next_announcement` labels such as revision signal date `statpers` to next trading day to next announcement.
- [x] Record `signal_timestamp`, `visibility_timestamp`, `tradable_timestamp`, and `anchor_event_timestamp`.
- [x] Add PIT visibility checks and placebo/null-test declarations.
- [x] Add overlap diagnostics so revision can be tested as marginal value rather than assumed independent.

Acceptance criteria:

- SUE fixture expresses event-window evaluation.
- Revision fixture expresses `statpers -> next trading day -> to-next-announcement`.
- Event evidence artifacts are deterministic.
- FMP analyst-estimate history is explicitly disallowed as PIT-safe analyst revision source.
- `make validate` passes.

## Phase 37: Alpha Projection Bridge v2

Goal:
Project typed alpha views into rebalance-period `expected_return` panels without
losing mechanism, horizon, decay, coverage, confidence, or abstain semantics.

Why next:
The optimizer can respond when expected return is injected. The missing layer is
the bridge from typed predictive claims to rebalance-period expected returns.

Target files:

- `src/portfolio_os/alpha/projection.py`
- `src/portfolio_os/alpha/projection_diagnostics.py`
- `tests/test_alpha_projection_bridge_v2.py`

Inputs:

- `alpha_views.json`
- `rebalance_calendar.csv`
- `universe_reference.csv`
- `cost_assumptions.json`
- `risk_horizon_days`

Outputs:

- `expected_return_panel.csv`
- `alpha_projection_manifest.json`
- `alpha_projection_diagnostics.json`
- `alpha_abstain_report.json`

Tasks:

- [x] Select active typed alpha views by rebalance date.
- [x] Align event-window, to-next-event, fixed-horizon, and state-exit horizons.
- [x] Apply decay and confidence weighting.
- [x] Emit explicit abstain for missing/stale/low-coverage views.
- [x] Generate projection diagnostics per rebalance date.
- [x] Add synthetic sign-flip fixture that changes optimizer ranking direction.

Acceptance criteria:

- SUE event view projects into rebalance-period expected return.
- Revision to-next-announcement view projects until the next event.
- Explicit abstain is not interpreted as zero alpha.
- Projection diagnostics explain active views, abstained views, coverage count,
  horizon conversion, decay, and expected-return scale.
- `make validate` passes.

## Phase 38: Promotion Gate v2

Goal:
Promote only typed, PIT-safe, bounded-horizon alpha views to Q2 through an
expected-return-panel contract.

Why next:
The existing Promotion Gate separates Q1 from Q2. v2 should keep that boundary
while consuming `EvidenceBundle + AlphaView + ProjectionManifest` instead of a
plain alpha-score contract.

Outputs:

- `promotion_decision_v2.json`
- `q2_input_contract_v2.json`
- `promotion_explanation_v2.md`

Tasks:

- [x] Validate AlphaView type and mechanism.
- [x] Check horizon type against evaluation label.
- [x] Require visibility timestamp no later than tradable timestamp.
- [x] Confirm projection did not change mechanism semantics.
- [x] Require explicit abstain semantics and active coverage reporting.
- [x] Require event overlap / collinearity disclosure.
- [x] Emit Q2InputContract v2 without running Q2.

Acceptance criteria:

- SUE AlphaView can reach `needs_more_evidence` or `promote_to_execution_eval`, but not direct portfolio construction.
- Forward-return leakage fixture is rejected.
- Revision fixture passes schema/horizon checks while requiring marginal-value disclosure.
- Q2InputContract v2 contains no orders, broker output, or live performance.
- `make validate` passes.

## Phase 39: Q2 Typed Alpha Execution Matrix

Goal:
Evaluate whether typed projected alpha survives cost, liquidity, participation,
coverage, abstain, and constraints.

Why next:
Q2 already has a scenario matrix. This phase upgrades the input from generic
`alpha_score` to projected expected-return panels and typed alpha diagnostics.

New dimensions:

```text
projection_policy:
  - event_window_only
  - event_window_decay
  - to_next_event
  - rebalance_period_projection

abstain_policy:
  - explicit_abstain
  - coverage_threshold
  - stale_view_abstain

alpha_family:
  - SUE
  - revision
  - SUE_plus_revision
```

Readouts:

- active rebalance count
- active name count
- gross-to-net retention
- turnover
- expected-return used share
- cost drag
- constraint repair retention
- abstain count
- sign consistency
- view overlap

Acceptance criteria:

- Q2 can consume `q2_input_contract_v2`.
- Unavailable rows remain explicit and never fabricate returns.
- Cost, participation, constraint, and projection scenarios retain source config hashes.
- Q2 report explains where typed alpha is consumed by cost, constraints, coverage, or abstain.
- `make validate` passes.

## Phase 40: Paper Overlay Calibration Lane

Goal:
Calibrate the paper execution environment, not alpha.

Why next:
Paper-stage calibration is useful for execution plumbing and venue behavior,
but it must not be treated as alpha validation or live alpha approval.

Target files:

- `src/portfolio_os/paper/overlay_readiness.py`
- `scripts/run_paper_overlay_calibration_batch.py`
- `tests/test_paper_overlay_readiness.py`

Outputs:

- `paper_overlay_calibration_summary.json`
- `paper_overlay_latency_buckets.csv`
- `paper_overlay_spread_capture.csv`
- `paper_overlay_readiness.md`

Tasks:

- [x] Add paper-overlay readiness schema and local aggregation contract.
- [x] Keep repeated SPY one-share sampling behind explicit user command.
- [x] Separate staleness bias, noise floor, and paper venue quirks.
- [x] Document that paper overlay is execution environment calibration only.

Acceptance criteria:

- Paper overlay calibration does not imply alpha promotion.
- Calibrated parameters are not pushed into production configs.
- Participation scope is not extrapolated above validated paper-overlay range.
- No live alpha orders are generated.
- `make validate` passes.

## Phase 41: First Real Typed Alpha Pilot

Goal:
Run SUE as the canonical event-alpha integration benchmark and revision as a
marginal-value shadow branch.

Why next:
The pilot should test the whole typed-alpha chain on a real mechanism without
turning it into production approval.

Pilot order:

```text
Pilot A: SUE event alpha as canonical integration benchmark
Pilot B: revision as marginal-value shadow branch
Pilot C: residual momentum calibration only after calibration closeout
```

Pilot A outputs:

- `us_sue_event_alpha_view.json`
- `us_sue_event_evidence_bundle.json`
- `us_sue_projection_panel.csv`
- `us_sue_q2_matrix.csv`
- `us_sue_audit_report.md`

Acceptance criteria:

- SUE AlphaView schema passes.
- Event evidence bundle passes.
- Projection Bridge v2 generates expected-return panel.
- Promotion Gate v2 does not reject PIT, horizon, or visibility checks.
- Q2 matrix explains gross-to-net, turnover, coverage, and abstain.
- Audit Report v2 connects discovery to Q2.
- No live trading instruction is generated.

Stop conditions:

- Event alpha projection is too sparse.
- Projection sign consistency breaks.
- Q2 gross-to-net retention is consumed by costs.
- Coverage turns mostly into abstain.
- Event evidence and rebalance projection cannot be reconciled.

## Phase 42: Typed Alpha Demo v2

Goal:
Extend the read-only demo/dashboard for typed alpha artifacts without building a
new app or exposing workflow-triggering actions.

New sections:

- Typed Alpha View
- Event Evidence
- Projection Diagnostics
- Abstain Report
- Q2 Typed Alpha Matrix
- Paper Overlay Calibration

Tasks:

- [x] Add `make demo-v2` or equivalent.
- [x] Generate deterministic local typed-alpha artifacts.
- [x] Render AlphaView -> Evidence -> Projection -> Promotion -> Q2 -> Paper Overlay in the static dashboard.
- [x] Keep read-only service and dashboard free of trade/order/broker routes.

Acceptance criteria:

- `make demo-v2` writes deterministic local artifacts.
- Dashboard shows typed-alpha artifact chain.
- Read-only service remains read-only.
- `make validate` passes.

## Optional Later Phases

These phases are optional hardening and packaging work after the completed
platform and typed-alpha roadmaps. They must not reopen alpha mining, live
trading, broker/order workflows, production alpha approval, or broad optimizer
tuning.

## Phase 43: Typed Alpha Release Candidate Hardening

Status:
Completed.

Goal:
Freeze the completed Phase 35-42 typed-alpha chain into a stable, auditable,
repeatable release surface that cannot be easily misread as production alpha
approval.

Release surface:

```text
AlphaView
  -> Event Evidence
  -> Projection Manifest
  -> Promotion Gate v2
  -> Q2 Typed Matrix
  -> Paper Overlay Readiness
  -> Demo v2 Dashboard
```

Why next:
The typed-alpha workflow now runs as a deterministic local artifact demo. The
next useful work is not another alpha feature; it is to lock the artifact shape,
schema versions, forbidden-output boundaries, and release notes so future
changes cannot silently break or overstate the story.

Target work:

- Add demo-v2 golden artifact contract tests for required local artifacts.
- Lock typed-alpha schema versions in one importable module.
- Extend forbidden-output regression guards across typed-alpha artifacts.
- Clarify demo-v2 dashboard wording only where it prevents misuse or overclaim.
- Add typed-alpha v0.1 release-candidate notes with reproduction commands and
  explicit non-claims.
- Keep generated artifacts local and ignored under `outputs/demo_v2/`.

Acceptance criteria:

- `make demo-v2` passes.
- `make validate` passes.
- `git diff --check` passes.
- Demo-v2 required artifacts are present and carry explicit schema versions.
- Golden contract tests verify the typed-alpha chain remains visible:
  AlphaView -> Evidence -> Projection -> Promotion v2 -> Q2 -> Audit.
- Forbidden-output guards cover AlphaView, projection, promotion v2, Q2 typed
  matrix, paper overlay readiness, audit report, and dashboard artifacts.
- Q2 unavailable rows remain explicitly unavailable and never contain fabricated
  returns, turnover, cost drag, or gross-to-net retention.
- Release candidate notes document what is complete, what is not claimed, how to
  reproduce it, and current limitations.

Do not:

- add new alpha research
- run live SEC/FMP/WRDS/Alpaca workflows
- add broker, order, trade, rebalance, or live execution routes
- promote SUE, revision, residual momentum, or any alpha into production config
- claim production alpha approval, realized alpha performance, or live trading
  readiness
- fill unavailable Q2 rows with synthetic numbers

Suggested commit split:

- `test: add demo v2 golden artifact regression`
- `feat: lock typed alpha schema versions`
- `test: extend forbidden output guards for typed alpha`
- `docs: add typed alpha release candidate notes`

## Phase 44: Demo v2 Golden Snapshot Tests

Status:
Completed.

Goal:
Make `make demo-v2` a stable regression surface by checking artifact structure
and dashboard sections without brittle full-HTML snapshots.

Target work:

- Add golden expected artifact-shape fixtures under `tests/golden/`.
- Snapshot required manifest keys and dashboard section headings.
- Verify required unavailable-row semantics.
- Verify forbidden trading, broker, order, live, and production-approval claims
  are absent.

Acceptance criteria:

- `make demo-v2` generates all expected local artifacts.
- Required dashboard sections are present:
  Run Summary, Typed Alpha View, Event Evidence, Projection Diagnostics,
  Promotion Gate v2, Q2 Typed Alpha Execution Matrix, Paper Overlay Readiness,
  Audit Report, Reproducibility Manifest, and Safety Boundaries.
- Golden tests fail if the AlphaView -> Evidence -> Projection -> Promotion v2
  -> Q2 -> Audit chain is broken.
- Tests avoid full HTML string snapshots unless the output is intentionally
  stable and compact.

Do not:

- treat golden snapshots as permission to fabricate data
- snapshot machine-specific timestamps or local absolute paths
- add new workflow triggers

## Phase 45: Typed Alpha Closeout Report

Status:
Completed.

Goal:
Produce a concise closeout memo explaining what Phase 35-42 proved, what it did
not prove, and why no production alpha approval is claimed.

Target output:

- `reports/typed_alpha_closeout_report.md`
- optional builder script if the report should be regenerated from local
  demo-v2 artifacts
- tests that guard the report's non-claim language

Required sections:

- Scope
- What This Proves
- What This Does Not Prove
- Known Limitations
- Reproducibility Commands
- Next Allowed Work

Acceptance criteria:

- Report builds deterministically from committed fixtures or local demo-v2
  outputs.
- Report explicitly says there is no live alpha approval, production trading
  approval, broker integration approval, order generation, or realized alpha
  performance claim.
- Report states that Q2 typed rows may remain unavailable and that paper overlay
  readiness is environment calibration only.
- Report does not include orders, broker output, live performance, trading
  instructions, or production approval language.

## Phase 46: Dashboard Readability Polish

Status:
Completed.

Goal:
Improve demo-v2 readability without adding functionality or changing business
logic.

Allowed work:

- clearer section ordering
- compact typed-alpha chain diagram
- artifact links
- manifest summary
- better unavailable-row explanations
- first-screen status summary that says:
  - Alpha status: integration benchmark only
  - Execution status: unavailable or local paper-overlay aggregation only
  - Trading status: no broker, no orders, no live workflow
  - Production status: not approved

Acceptance criteria:

- `make demo-v2` still passes.
- Dashboard golden section tests pass.
- Forbidden-output tests pass.
- Missing artifacts render as structured unavailable, not blank or misleading.
- No forms, POST methods, broker routes, order routes, trade routes, live
  execution routes, or workflow-triggering controls are added.

Do not:

- add new alpha logic
- add evaluator logic
- add broker/live routes
- fake Q2 metrics
- add production approval language

## Phase 47: Typed Q2 Execution Adapter v0

Status:
Complete.

Goal:
Connect typed-alpha `Q2InputContract v2` and projected expected-return-panel
artifacts to a local-only PortfolioOS execution-aware fixture so typed Q2 rows
can become observed where local metrics are available.

Why next:
The typed-alpha chain can now express SUE as:

```text
AlphaView
  -> Event Evidence
  -> Projection
  -> Promotion Gate v2
  -> Q2 Input Contract
```

The remaining gap is that the Q2 typed matrix still reports execution rows as
`unavailable`. Phase 47 should answer whether typed-alpha projections can be
consumed by a local Q2 adapter without live data, brokers, order generation, or
production approval.

Non-goals:

- no live Alpaca, SEC, FMP, WRDS, Tushare, or paid/external data workflow
- no broker integration
- no order generation
- no production alpha approval
- no `config/us_expanded.yaml` promotion
- no new SUE, revision, residual momentum, or A-share research
- no fake Q2 metrics
- no placeholder values that make unavailable rows look observed

Target modules:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_adapter_schema.py`
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_portfolioos_adapter.py`
- `projects/execution_aware_optimizer/fixtures/typed_q2/`
- `scripts/run_typed_q2_adapter_fixture.py`
- focused typed-adapter tests under `projects/execution_aware_optimizer/tests/`

Input contract:

```text
TypedQ2AdapterInput
  schema_version
  run_id
  q2_input_contract_v2_path
  expected_return_panel_path
  projection_manifest_path
  local_backtest_manifest_path
  adapter_config
  allow_portfolioos_run
  no_network
  no_broker
```

Output contract:

```text
TypedQ2AdapterResult
  schema_version
  run_id
  adapter_status: observed | partially_observed | unavailable | rejected
  observed_rows
  unavailable_rows
  rejection_reasons
  source_config_hash
  input_artifact_hashes
  no_live_data_confirmed
  no_orders_confirmed
  no_broker_confirmed
```

Required output artifacts:

- `typed_q2_execution_matrix.csv`
- `typed_q2_adapter_result.json`
- `typed_q2_robustness_summary.json`
- `typed_q2_adapter_manifest.json`
- `typed_q2_adapter_trace.jsonl`

Execution semantics:

- `allow_portfolioos_run=false` returns structured unavailable rows.
- `allow_portfolioos_run=true` may produce observed rows only from deterministic
  local fixtures.
- Missing local backtest manifests produce unavailable results.
- Missing expected-return panels produce rejected results.
- Schema mismatches produce rejected results.
- Forbidden output detection produces rejected results.
- Observed rows must map to metrics actually computed from local PortfolioOS
  fixture output.
- Unavailable rows must remain unavailable and must not be encoded as zero
  performance.

Preferred fixture order:

1. Synthetic typed-alpha fixture for contract and mapping safety.
2. Demo-v2 SUE fixture for artifact compatibility.
3. Optional SUE pilot adapter smoke after the synthetic fixture is stable.

Acceptance criteria:

- Adapter consumes `Q2InputContract v2`, expected-return panel, projection
  manifest, and a local backtest manifest.
- `allow_portfolioos_run=false` returns structured unavailable rows.
- `allow_portfolioos_run=true` can produce at least one observed row from a
  deterministic local fixture.
- Observed rows include at least net return or cost/turnover-derived metrics
  from local fixture output.
- Unavailable layers remain explicit and are not fabricated.
- Adapter result confirms no live data, no orders, and no broker path.
- Output artifacts include execution matrix, adapter result, robustness summary,
  manifest, and trace.
- Forbidden-output guards cover order-like payloads, broker output, live
  performance, production approval, credentials, and account identifiers.
- `make validate` passes.
- Optional `make typed-q2-adapter-fixture` passes if the fixture target is added.

Stop conditions:

- Adapter requires live data or broker state.
- Expected-return panel cannot map to PortfolioOS input without semantic loss.
- Observed rows require fake metrics.
- SUE fixture sparsity starts being misread as performance proof.
- Forbidden-output guard catches order-like payloads.

Closeout rule:

If observed local mapping cannot be implemented safely, Phase 47 should close
as `typed Q2 adapter unavailable due to mapping gap`, not force a fake observed
implementation.

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_adapter_schema.py` defines the typed adapter input, matrix row, result, summary, and manifest contracts.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_portfolioos_adapter.py` validates typed artifacts, rejects forbidden output keys, preserves unavailable rows, and maps local PortfolioOS period attribution into observed typed Q2 adapter rows where stable mappings exist.
- `projects/execution_aware_optimizer/fixtures/typed_q2/` provides a synthetic typed-alpha fixture.
- `scripts/run_typed_q2_adapter_fixture.py` and `make typed-q2-adapter-fixture` write local adapter artifacts under `outputs/typed_q2_adapter_fixture/`.
- The adapter v0 validates and records the projected expected-return panel but does not yet inject that panel into a new PortfolioOS optimizer path. It observes only metrics exposed by existing local fixture period attribution.

## Final Typed Alpha to Q2 to Paper-Stage Roadmap

This is the finite Phase 48-66 roadmap. It should not grow automatically. The
main line is no longer platform packaging or new alpha mining. The main line is
to prove where each typed alpha stops:

```text
Typed AlphaView
  -> Expected-Return Injection
  -> Optimizer Response
  -> Q2 Survival
  -> Attribution
  -> Alpha Registry
  -> Optional Paper-Stage Dossier
```

Rules:

- Phase 48-54 are the required typed-alpha to local Q2 closeout path.
- Phase 55 freezes decisions into a machine-readable alpha registry.
- Phase 56-58 are optional paper-stage preparation and governance only.
- Phase 59-61 are locked by default and require explicit human approval.
- Phase 62-64 are research-reopen controls, not automatic research branches.
- Phase 65-66 package and freeze the research-audit release.
- No phase may add live data, broker routes, orders, trading instructions, fake
  Q2 metrics, or production approval language.

Priority order:

1. Phase 48.
2. Phase 49.
3. Phase 50.
4. Phase 51.
5. Phase 52.
6. Phase 55.

Only run Phase 53-54 if Phase 52 promotes revision into composite evaluation.
Only run Phase 56-61 after SUE-only or composite has a clean local Q2 dossier.
Only reopen research through Phase 62-64 after typed mainline closeout.

## Phase 48: Typed Expected-Return Injection Fixture

Status:
Complete.

Goal:
Inject typed `expected_return_panel.csv` artifacts into a local-only PortfolioOS
optimizer fixture input path.

Why next:
Phase 47 validates and records expected-return panels, but it does not prove the
panel reaches the optimizer input. Phase 48 closes that exact gap without
claiming alpha success.

Target modules:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_injection_schema.py`
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_expected_return_injection.py`
- `projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py`
- `scripts/run_typed_expected_return_injection_fixture.py`

Output artifacts:

- `outputs/typed_expected_return_injection_fixture/typed_expected_return_injection_result.json`
- `outputs/typed_expected_return_injection_fixture/optimizer_input_snapshot.csv`
- `outputs/typed_expected_return_injection_fixture/injected_expected_return_panel.csv`
- `outputs/typed_expected_return_injection_fixture/typed_q2_execution_matrix_injected.csv`
- `outputs/typed_expected_return_injection_fixture/typed_q2_injection_robustness_summary.json`
- `outputs/typed_expected_return_injection_fixture/typed_q2_injection_manifest.json`
- `outputs/typed_expected_return_injection_fixture/typed_q2_injection_trace.jsonl`

Acceptance criteria:

- Expected-return panel validates against `Q2InputContract v2` and projection
  manifest.
- `allow_portfolioos_run=false` returns structured unavailable rows.
- `allow_portfolioos_run=true` writes `optimizer_input_snapshot.csv`.
- `optimizer_input_snapshot.csv` proves expected return reached optimizer input.
- Synthetic positive, scaled, and sign-flipped panels are supported.
- Unavailable layers remain unavailable.
- No live data, broker, orders, trading instructions, or production approval
  path is added.
- `make validate` passes.
- `make typed-expected-return-injection-fixture` passes if the target is added.

Stop condition:
If the expected-return panel cannot map into optimizer input without changing
PortfolioOS semantics, close Phase 48 as `typed injection unavailable due to
optimizer input mapping gap`. Do not call that an alpha failure.

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_injection_schema.py` defines typed injection input, result, summary, and manifest contracts.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_expected_return_injection.py` validates Q2InputContract v2 plus projection manifest, transforms positive/scaled/sign-flipped expected-return panels, and builds a local PortfolioOS optimizer input snapshot.
- `projects/execution_aware_optimizer/fixtures/typed_injection/` provides the deterministic Phase 48 fixture aligned to the local monthly backtest schedule.
- `scripts/run_typed_expected_return_injection_fixture.py` and `make typed-expected-return-injection-fixture` write local ignored artifacts under `outputs/typed_expected_return_injection_fixture/`.
- The opt-in smoke path returned `injection_status=injected`, `expected_return_reached_optimizer_input=True`, `optimizer_input_snapshot_rows=33`, and `injected_expected_return_count=2`.
- The fixture proves typed expected-return values reach optimizer input shape; it does not yet prove directional optimizer response or alpha success. Phase 49 is responsible for optimizer response acceptance.

## Phase 49: Typed Optimizer Response Acceptance Suite

Status:
Complete.

Goal:
Prove that injected typed expected-return panels produce directionally coherent
optimizer response diagnostics.

Test panels:

- `positive_panel.csv`
- `scaled_0_5x_panel.csv`
- `scaled_1_0x_panel.csv`
- `scaled_2_0x_panel.csv`
- `sign_flipped_panel.csv`
- `abstain_panel.csv`
- `zero_panel.csv`

Diagnostics:

- `alpha_reward_share`
- `rank_alignment`
- `top_minus_bottom_weight_delta`
- `gross_traded_notional`
- `repair_retention`
- `expected_return_used_share`
- `optimizer_status`

Acceptance criteria:

- Positive panel produces positive rank/weight alignment.
- Scaled panels produce monotonic alpha-reward or expected-return contribution.
- Sign-flipped panel reverses relative ordering.
- Abstain panel is not silently treated as positive alpha.
- Zero alpha and `no_view` are reported separately.
- Repair retention is reported, not hidden.
- Output is deterministic enough for regression tests.

Output artifacts:

- `outputs/typed_optimizer_response_acceptance/optimizer_response_summary.json`
- `outputs/typed_optimizer_response_acceptance/optimizer_response_grid.csv`
- `outputs/typed_optimizer_response_acceptance/sign_flip_diagnostics.json`
- `outputs/typed_optimizer_response_acceptance/abstain_vs_zero_report.json`

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_optimizer_response_schema.py` defines the Phase 49 input, row, summary, and result contracts.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_optimizer_response.py` builds deterministic positive, scaled, sign-flipped, zero-alpha, and explicit-abstain typed expected-return panels and evaluates them through a local PortfolioOS optimizer fixture.
- `scripts/run_typed_optimizer_response_acceptance.py` and `make typed-optimizer-response-acceptance` write local ignored artifacts under `outputs/typed_optimizer_response_acceptance/`.
- The opt-in smoke path returned `response_status=observed`, `optimizer_status=optimal`, `panel_count=7`, and true checks for positive alignment, monotone scaled alpha contribution, sign-flip reversal, `no_view` versus zero-alpha distinction, and repair-retention reporting.
- The fixture proves directional optimizer response to typed expected-return panel variants. It does not prove SUE survival, revision marginal value, paper-stage readiness, or production approval.

## Phase 50: SUE Typed Q2 Survival Matrix v1

Status:
Complete.

Goal:
Run the SUE typed alpha pilot through the injected local optimizer/Q2 path.

Required readout:

- `active_rebalance_count`
- `active_name_count`
- `expected_return_used_share`
- `gross_return`
- `net_return`
- `turnover`
- `cost_drag`
- `gross_to_net_retention`
- `repair_retention`
- `unavailable_reason`
- `projection_policy`
- `abstain_policy`
- `source_config_hash`

Acceptance criteria:

- SUE expected return reaches optimizer input.
- SUE rows are observed or explicitly unavailable.
- No fabricated gross/net/turnover metrics.
- If observed, gross-to-net and cost drag are reported.
- If unavailable, the exact missing layer is reported.
- SUE remains an integration benchmark and Q2 candidate, not production-approved.

Output artifacts:

- `outputs/sue_typed_q2_survival/sue_typed_q2_execution_matrix.csv`
- `outputs/sue_typed_q2_survival/sue_typed_q2_survival_summary.json`
- `outputs/sue_typed_q2_survival/sue_optimizer_input_snapshot.csv`
- `outputs/sue_typed_q2_survival/sue_injection_manifest.json`
- `outputs/sue_typed_q2_survival/sue_q2_trace.jsonl`

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_typed_q2_survival_schema.py` defines the Phase 50 input, row, summary, and result contracts.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_typed_q2_survival.py` aligns the SUE typed projection to the local optimizer fixture date, reuses the Phase 48 injection path, and maps configured local Q2 adapter rows as observed.
- `projects/execution_aware_optimizer/fixtures/sue_survival/` provides the local SUE typed projection fixture.
- `scripts/run_sue_typed_q2_survival.py` and `make sue-typed-q2-survival` write local ignored artifacts under `outputs/sue_typed_q2_survival/`.
- The opt-in smoke path now returns `survival_status=observed`, `injection_status=injected`, `expected_return_reached_optimizer_input=True`, `q2_observed_rows=30`, and `q2_unavailable_rows=0`.
- The SUE projection is still an integration benchmark and Q2 candidate only. This phase does not claim SUE alpha success, revision marginal value, paper-stage readiness, or production approval.

## Phase 51: SUE Execution-Survival Attribution Report

Status:
Complete.

Goal:
Explain exactly where SUE survives or fails.

Attribution layers:

- evidence
- projection
- injection
- optimizer response
- constraint repair
- cost
- turnover
- coverage / abstain
- unavailable local fixture hook

Decision labels:

- `sue_q2_observed_survives`
- `sue_q2_observed_cost_failure`
- `sue_q2_observed_constraint_failure`
- `sue_q2_projection_too_sparse`
- `sue_q2_injection_unavailable`
- `sue_q2_fixture_unavailable`
- `sue_q2_inconclusive`

Acceptance criteria:

- Report states what SUE proves and does not prove.
- Report does not claim production approval.
- Report distinguishes alpha failure from execution failure.
- Report distinguishes projection sparsity from optimizer failure.
- Report states whether Phase 52 revision marginal-value testing should proceed.

Output artifacts:

- `reports/sue_typed_q2_survival_attribution.md`
- `outputs/sue_typed_q2_survival/failure_attribution.json`

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_execution_survival_attribution_schema.py` defines the Phase 51 attribution and layer schemas.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_execution_survival_attribution.py` interprets Phase 50 SUE survival rows into evidence, projection, injection, optimizer response, cost, turnover, coverage/abstain, and unavailable fixture-hook layers.
- `scripts/build_sue_typed_q2_survival_attribution.py` and `make sue-survival-attribution` rebuild the local Phase 50 fixture and write Phase 51 JSON plus Markdown artifacts.
- The local Phase 51 decision is now `sue_q2_observed_survives` because the SUE projection reaches optimizer input and all configured local Q2 fixture rows are observed, including `risk_controlled` through the stable `naive_pro_rata` mapping. Phase 52 may proceed as a marginal-value diagnostic, not as production approval.

## Phase 52: Revision Marginal-Value Gate

Status:
Complete.

Goal:
Decide whether revision adds marginal value beyond SUE.

Required tests:

- SUE-only baseline.
- Revision-only shadow branch.
- SUE + revision equal composite.
- SUE + revision confidence-weighted composite.
- SUE residualized against revision.
- Revision residualized against SUE.
- Event-overlap and coverage-overlap diagnostics.
- Cost-aware marginal contribution.

Gate decisions:

- `revision_promote_to_composite_eval`
- `revision_real_but_no_marginal_value`
- `revision_needs_more_evidence`
- `revision_reject_due_to_pit_or_horizon`

Acceptance criteria:

- WRDS remains the required PIT source for analyst revision research.
- FMP frozen estimate history is not accepted as PIT-safe revision source.
- Revision must beat a SUE-adjusted marginal threshold before composite
  promotion.
- Raw tree importance or feature importance is not accepted as proof.
- Gate produces a clear promote, shadow, archive, or needs-more-evidence
  decision.

Completion note:

- `projects/execution_aware_optimizer/src/execution_aware_optimizer/revision_marginal_value_schema.py` defines the Phase 52 input, test-result, marginal-metric, threshold, overlap-row, summary, and result contracts.
- `projects/execution_aware_optimizer/src/execution_aware_optimizer/revision_marginal_value_gate.py` applies the local gate rules: WRDS is required, FMP frozen estimate history is rejected, raw tree/feature importance is insufficient, and composite promotion requires SUE-adjusted cost-aware marginal improvement.
- `projects/execution_aware_optimizer/fixtures/revision_marginal_value/gate_input.json` is the deterministic local fixture. It keeps revision as a real shadow branch because it passes the required diagnostics but fails the cost-aware marginal threshold.
- `scripts/run_revision_marginal_value_gate.py` and `make revision-marginal-value-gate` write `outputs/revision_marginal_value_gate/` artifacts plus `reports/revision_marginal_value_report.md`.
- The local Phase 52 decision is `revision_real_but_no_marginal_value`, so Phase 53-54 composite work remains closed unless a future explicit marginal-value fixture clears the gate.

## Phase 53: Composite AlphaView Assembler: SUE + Revision

Status:
Conditional on Phase 52 promotion.

Goal:
Build a composite typed AlphaView only if revision passes Phase 52.

Composite policies:

- `equal_weight`
- `confidence_weighted`
- `horizon_priority`
- `event_recency_priority`
- `non_overlapping_only`
- `residualized_revision`

Acceptance criteria:

- `no_view != zero_alpha` remains preserved.
- Conflict resolution is deterministic.
- Horizon semantics are explicit.
- Composite expected-return panel is reproducible.
- Projection diagnostics explain every active and abstained view.

## Phase 54: Composite Typed Q2 Survival Matrix

Status:
Conditional on Phase 53.

Goal:
Evaluate whether the composite survives Q2 better than SUE-only.

Comparison set:

- SUE-only.
- Revision-only.
- SUE + revision equal.
- SUE + revision confidence-weighted.
- SUE + revision residualized.

Decision labels:

- `composite_beats_sue_after_cost`
- `composite_adds_signal_but_costs_too_much`
- `composite_no_marginal_value`
- `composite_inconclusive`

Acceptance criteria:

- Composite is compared against SUE-only, not judged standalone.
- Cost and turnover are included.
- Unavailable rows remain unavailable.
- No production approval is claimed.
- Next phase is blocked unless this produces a clear decision.

## Phase 55: Alpha Registry v2 / Decision State Machine

Status:
Complete.

Goal:
Freeze all alpha statuses in one machine-readable registry.

Required entries:

- SUE / PEAD.
- `revision_1m`.
- SUE + revision composite if built.
- Old real alpha package / Phase 1.5 bridge.
- Qlib fixed-horizon + revision.
- Residual momentum / residual reversal.
- A-share `anti_mom_21_5`.
- Forward-return leakage fixtures.

Registry statuses:

- `canonical_pilot`
- `eligible_for_q2_eval`
- `q2_observed_survives`
- `q2_observed_fails_cost`
- `q2_observed_fails_constraints`
- `real_shadow_branch`
- `needs_marginal_value`
- `diagnostic_only`
- `calibration_only`
- `background_partially_real`
- `rejected_leakage`
- `archived_no_marginal_value`
- `production_not_approved`

Acceptance criteria:

- Every alpha has a typed-chain stop layer.
- No alpha is labeled only pass/fail.
- SUE and revision statuses reflect Phase 50-54 results.
- Residual momentum remains calibration-only unless Phase 62 later changes it.
- A-share remains background unless Phase 63 is explicitly opened.

Completion note:

- `src/portfolio_os/alpha/registry_v2.py` defines the Phase 55 registry and entry contracts plus deterministic default alpha states.
- `scripts/build_alpha_registry_v2.py` and `make alpha-registry-v2` write `outputs/alpha_registry_v2/alpha_registry.yaml`, `outputs/alpha_registry_v2/alpha_registry_decision_table.csv`, and `reports/alpha_registry_report.md`.
- SUE is recorded as `canonical_pilot` with `q2_observed_survives` in its history and stop layer `q2_observed_survives_local_fixture`.
- `revision_1m` is recorded as `real_shadow_branch` with `archived_no_marginal_value` in its status history and stop layer `revision_marginal_value_gate`.
- Residual momentum remains `calibration_only`; A-share `anti_mom_21_5` remains `background_partially_real`; forward-return leakage fixtures remain `rejected_leakage`.
- No alpha is labeled only pass/fail, and the registry does not claim production approval, live trading, paper canary approval, broker workflows, orders, or new research branches.

## Phase 56: Paper Overlay Calibration Round 2

Status:
Optional paper-stage calibration.

Goal:
Calibrate the paper-stage execution environment, not alpha.

Scope:

- SPY x 1 share repeated tranche.
- 30-50 runs.
- Drift aggregation.
- Half-spread scaling.
- Latency buckets.
- Time-of-day buckets.
- Paper venue quirks.

Acceptance criteria:

- Confirms environment calibration only.
- No alpha orders.
- No SUE or revision paper trading.
- No calibrated `k` promotion into `config/us_expanded.yaml`.
- No extrapolation above 0.1 percent participation.

## Phase 57: Paper-Stage Candidate Dossier

Status:
Conditional after local Q2 candidate decision.

Goal:
Create the full paper-stage dossier for SUE-only or SUE + revision composite
without approving live or production trading.

Dossier sections:

- AlphaView contract.
- Event evidence.
- Projection manifest.
- Expected-return injection proof.
- Optimizer response acceptance.
- Typed Q2 survival matrix.
- Cost, turnover, and repair analysis.
- Paper overlay calibration compatibility.
- Failure modes.
- Explicit non-claims.
- Human approval checklist.

Acceptance criteria:

- Candidate status is `paper-stage dossier prepared`, not approved.
- No orders, broker payloads, or trading instructions.
- All artifacts are locally reproducible.
- Limitations are explicit.
- Human approval gate is required for anything beyond local evaluation.

## Phase 58: Human Approval / No-Automation Boundary Pack

Status:
Conditional governance phase.

Goal:
Document the boundary before any paper canary.

Required document:

- `docs/governance/paper_stage_human_approval_boundary.md`

Acceptance criteria:

- PortfolioOS will not automatically place paper alpha orders.
- Paper canary requires explicit human approval.
- Candidate dossier is not approval.
- Paper-stage observation is not production validation.
- Production trading remains out of scope.
- Any broker/order path must be separately authorized and reviewed.
- Forbidden-output guards still pass.

## Phase 59: Conditional Paper Canary Contract

Status:
Locked by default. Requires explicit human approval after Phase 58.

Goal:
Define a tiny paper-stage canary contract before any paper canary is run.

Constraints:

- Paper account only.
- Tiny notional.
- 0-0.1 percent participation.
- Pre-trade reference snapshot required.
- Post-trade reconciliation required.
- Kill switch required.
- Manual approval required.
- No production language.

Acceptance criteria:

- Contract exists before any execution.
- Contract has max notional, max participation, max names, and max attempts.
- No automatic promotion from dossier to canary.
- No production approval.

## Phase 60: Conditional Paper Canary Observation Report

Status:
Locked by default. Requires Phase 59 and explicitly approved run artifacts.

Goal:
Report observed paper-stage behavior if a canary is manually executed.

Report must separate:

- alpha signal behavior
- optimizer intent
- paper fill behavior
- slippage / spread capture
- latency
- broker venue quirks
- unmatched, rejected, or partial fills
- post-trade reconciliation

Acceptance criteria:

- Report does not claim production readiness.
- Report does not generalize from a tiny sample.
- Report records all rejected, partial, and unmatched fills.
- Report explains whether the result is alpha issue, execution issue, or venue
  issue.

## Phase 61: Paper-Stage Closeout Decision

Status:
Conditional after Phase 60.

Goal:
Close the paper-stage path honestly.

Allowed outcomes:

- `paper_stage_continue_observation`
- `paper_stage_pause_due_to_execution_gap`
- `paper_stage_pause_due_to_alpha_decay`
- `paper_stage_close_no_production_case`
- `paper_stage_ready_for_external_review_only`

Forbidden outcomes:

- `production_approved`
- `live_approved`
- `scale_up_approved`

Acceptance criteria:

- Decision is evidence-backed.
- No automatic next trading phase.
- Production remains out of scope.
- Any continuing tranche must be separately approved.

## Phase 62: Residual Momentum Calibration Closeout

Status:
Blocked until typed mainline closeout.

Goal:
Revisit residual momentum only as a calibration closeout, not as alpha
promotion.

Decision labels:

- `calibration_validated`
- `calibration_failed_placebo_dominates`
- `family_archived`
- `needs_new_controls`

Acceptance criteria:

- Placebo no longer dominates live expression, or the family is closed honestly.
- Calibration report states whether the discovery harness is trustworthy.
- No Q2 promotion unless calibration passes.
- No broad factor tournament.

## Phase 63: Conditional A-Share State-Transition Tranche Charter

Status:
Locked by default. Requires explicit new A-share decision.

Goal:
Reopen A-share only through a new typed state-transition tranche charter.

Charter must include:

- mechanism hypothesis
- state definition
- PIT data contract
- short-horizon label
- execution assumption
- capacity assumption
- typed AlphaView shape
- Q1 evidence requirements
- Q2 survival requirements
- kill criteria

Acceptance criteria:

- Old `anti_mom_21_5` is not auto-promoted.
- New tranche has its own preregistered contract.
- Typed state-transition AlphaView is defined before mining.
- Branch-local A-share memory remains canonical for A-share details.

## Phase 64: Research Import Contract for New Typed Alpha Families

Status:
Locked after Phase 62 or Phase 63.

Goal:
Prevent new research from bypassing typed-alpha governance.

Input requirements:

- external research artifact bundle
- mechanism family description
- PIT report
- event / state / fixed-horizon label contract
- coverage report
- cost assumption
- candidate AlphaView

Import decisions:

- `import_rejected`
- `import_needs_more_evidence`
- `import_as_calibration_only`
- `import_as_shadow_branch`
- `import_to_q1_evidence`
- `import_to_q2_eval`

Acceptance criteria:

- No external research enters Q2 directly.
- Import must specify typed horizon.
- Leakage fixtures are rejected.
- Missing PIT source is rejected or marked needs-more-evidence.
- Alpha registry is updated.

### Phase 64 Candidate Charter: Factor Discovery Sandbox

Status:
Proposed future research import candidate. This is not active Phase 65 work and
does not create an automatic Phase 67.

Placement:
`projects/multifactor_alpha_validation/factor_discovery_sandbox/`

Goal:
Create a controlled candidate-generation sandbox for NASDAQ100-style
price-volume factors while preserving the main project story: PortfolioOS is an
audit-ready research-to-execution evaluation platform, not a strategy factory.

Boundary:

- Factor Discovery Sandbox asks: "Which candidate factors deserve typed
  validation?"
- It may generate candidate factor specs, teaching reports, and import bundles.
- It must not claim production alpha, submit orders, call brokers, or enter Q2
  directly.
- It must feed Phase 64 import review before any Q1 evidence, Promotion Gate,
  Q2 execution-survival, or Alpha Registry decision.

Modes:

- `teaching_mode`: current NASDAQ100 constituents, yfinance-style public data,
  QQQ benchmark, 29 price-volume factor baseline, IC/ICIR, correlation filter,
  ICIR weighting, monthly rebalance, and simple cost model. Every artifact must
  set `survivorship_biased=true`, `educational_only=true`, and
  `not_alpha_evidence=true`.
- `research_mode`: historical constituents, universe timestamps, timestamp
  contract, rolling out-of-sample ICIR, train/validation/test split,
  no same-close trading, FactorSpec coverage rules, redundancy and marginal
  value gate, shrinkage, allocator diagnostics, zero-weight attribution, cost
  and capacity stress, and QQQ-relative benchmark attribution.

Candidate sub-roadmap:

- FD-1 Teaching Baseline Replication: complete. Reproduce the seven-step NASDAQ100 factor
  rotation only as a biased educational baseline with QQQ-relative reporting.
- FD-2 FactorSpec Conversion: complete. Convert the factor columns into explicit specs
  with mechanism, lookback, skip, direction, horizon, timestamp contract,
  coverage rule, correlation family, and known failure mode.
- FD-3 Rolling ICIR and OOS Weighting: complete. Estimate IC/ICIR only with history
  available before the rebalance month, form scores at `t`, and trade no earlier
  than `t+1`.
- FD-4 Redundancy / Marginal-Value Gate: complete. Replace shallow correlation filtering
  with cluster, residual IC, incremental spread, incremental net return,
  turnover, cost drag, and marginal-value decisions.
- FD-5 Shrinkage + Allocator + Zero-Weight Attribution: shrink noisy factor
  means, stabilize covariance, cap clusters, and explain every zero factor
  weight.
- FD-6 Cost / Capacity / Benchmark Survival: report raw, QQQ-relative,
  beta-adjusted, cost-adjusted, capacity-adjusted, sector/style/liquidity
  attribution, and survival-funnel results.

Required artifacts before import review:

- `teaching_factor_rotation_report.md`
- `institutional_factor_validation_report.md`
- `factor_specs/price_volume_29/*.yaml`
- `rolling_icir_weights.csv`
- `marginal_value_decision_table.csv`
- `zero_weight_attribution.csv`
- `cost_stress_matrix.csv`
- `capacity_frontier.csv`
- `benchmark_attribution.csv`
- `research_import_bundle.json`

Import decisions:

- `import_rejected`
- `import_needs_more_evidence`
- `import_as_calibration_only`
- `import_as_shadow_branch`
- `import_to_q1_evidence`

Acceptance criteria:

- Teaching-mode output explicitly states survivorship bias, educational-only
  status, and not-alpha-evidence status.
- Research-mode output separates `signal_timestamp`, `visibility_timestamp`,
  and `tradable_timestamp`.
- Full-sample ICIR weights are forbidden for research-mode backtests.
- Missing factor coverage is explicit abstain; `no_view != zero_alpha`.
- High-correlation factors cannot survive on standalone ICIR alone.
- Every promoted candidate has a typed horizon, PIT/universe report,
  benchmark-relative attribution, cost/capacity stress, and marginal-value
  decision.
- Alpha Registry is updated only after Phase 64 import review, never from the
  sandbox directly.

Do not:

- describe the sandbox as a formal alpha strategy.
- cite high teaching-mode annualized returns as success evidence.
- rely on current NASDAQ100 constituents for institutional validation.
- trade at the same close used to form the signal.
- run yfinance, paid data, broker, or live workflows unless explicitly approved.
- merge Q1 and Q2 project stories through the sandbox.

## Phase 65: PortfolioOS v1 Research-Audit Release

Status:
Final packaging.

Goal:
Package the whole system as a stable research-audit release.

Release contents:

- README.
- Architecture.
- RUNBOOK.
- VALIDATION.
- ROADMAP.
- Typed-alpha closeout.
- SUE Q2 report.
- Revision marginal-value report.
- Composite report if applicable.
- Alpha Registry v2.
- Paper-stage dossier if applicable.
- Forbidden-output guard report.

Acceptance criteria:

- Required smoke checks pass.
- Release note lists every non-claim.
- No live, broker, order, or production approval path appears.
- Generated artifacts are local and reproducible.
- `ROADMAP.md` says no automatic next phase.

## Phase 66: Maintenance Freeze / Future-Only Backlog

Status:
Terminal.

Goal:
Stop automatic roadmap growth.

Allowed after Phase 66:

- bug fixes
- schema migration
- test stabilization
- documentation corrections
- artifact readability polish
- new research only through Phase 64 import contract
- paper canary only through Phase 58-61 approval path

Forbidden after Phase 66:

- automatic Phase 67
- new alpha mining without charter
- production approval language
- broker/order workflow by default
- fake Q2 metrics
- dashboard features that imply trading capability

Acceptance criteria:

- `ROADMAP.md` no longer recommends automatic expansion.
- Future work is categorized as bugfix, maintenance, explicit new research
  decision, or explicit paper-stage approval.
- Alpha Registry v2 remains the source of truth.
- All old branches have explicit status.

## Phase 48-66 Decision Tree

After Phase 48-49:

- If typed expected-return injection fails, stop the platform path, document the
  optimizer input mapping gap, and do not blame alpha.
- If injection works but optimizer response fails, stop optimizer path, document
  response failure, and do not run SUE Q2.
- If injection and response work, proceed to SUE Typed Q2 Survival Matrix.

After Phase 50-51:

- If SUE survives local Q2, proceed to revision marginal-value gate and prepare
  SUE candidate dossier.
- If SUE fails due to cost or turnover, document execution survival failure and
  do not reopen alpha research immediately.
- If SUE fails due to projection sparsity, revisit projection bridge, not
  optimizer.
- If SUE is unavailable due to fixture gap, close as local Q2 observability gap.

After Phase 52-54:

- If revision adds marginal value, build composite AlphaView and run composite
  Q2.
- If revision does not add marginal value, archive it as a real shadow branch and
  keep SUE-only.
- If composite improves gross but loses net, keep SUE-only and record
  cost/turnover failure.
- If composite beats SUE net of cost, composite becomes paper-stage dossier
  candidate.

After Phase 56-61:

- If paper calibration is unstable, no paper canary.
- If paper calibration is stable but no human approval exists, stop at dossier.
- If paper canary is approved and observation is clean, mark external review
  only, still no production approval.
- If paper canary shows fill, latency, or slippage issues, pause paper-stage
  path.

After Phase 62-64:

- If residual momentum calibration fails, archive the family.
- If residual momentum calibration passes, import via Phase 64, not direct Q2.
- If A-share is reopened, write a new typed tranche charter first.
- If Factor Discovery Sandbox is opened, treat it as a Phase 64 candidate
  charter and keep teaching-mode output separate from research-mode validation.
- If a new family appears, import via Phase 64 only.
