# VALIDATION.md

Use this file to choose the smallest validation set that proves the touched work.

## Always Run

```bash
git status --short
git diff --check
```

## CI-Style Local Validation

Run the hardened local validation target when touching workflow, report,
schema, provenance, observability, or validation infrastructure:

```bash
make validate
```

No-network guard:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run python scripts/devtools/no_network_guard.py
```

One-command demo smoke:

```bash
make demo
```

Typed alpha demo-v2 smoke:

```bash
make demo-v2
```

Typed alpha release-candidate hardening tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_release_candidate.py -q
```

Demo-v2 golden snapshot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_demo_v2_golden_snapshot.py -q
```

Typed alpha closeout report smoke and tests:

```bash
make typed-alpha-closeout
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_closeout_report.py -q
```

Alpha Registry v2 smoke and tests:

```bash
make alpha-registry-v2
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_registry_v2.py -q
```

PortfolioOS v1 research-audit release hygiene tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_portfolioos_v1_research_audit_release.py -q
```

PortfolioOS v1 maintenance freeze tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_portfolioos_v1_maintenance_freeze.py -q
```

Factor Discovery Sandbox FD-1 teaching baseline smoke and tests:

```bash
make factor-discovery-teaching-baseline
make factor-discovery-factor-specs
make factor-discovery-rolling-oos
make factor-discovery-marginal-value-gate
make factor-discovery-allocator
make factor-discovery-survival
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests -q
```

Formal multifactor research-mode preflight smoke and tests:

```bash
make multifactor-research-mode-preflight
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_week1_contracts.py projects/multifactor_alpha_validation/tests/test_research_mode_preflight.py -q
```

WRDS multifactor ingest config smoke and tests:

```bash
make multifactor-wrds-config-check
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_research_mode_preflight.py projects/multifactor_alpha_validation/tests/test_wrds_ingest.py -q
```

Standalone Multi-Factor Alpha Validation Engine smoke and tests:

```bash
make factor-validate
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests -q
```

Typed alpha dashboard readability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_dashboard_readability.py -q
```

Typed AlphaView contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_view_contract.py -q
```

Event-aware alpha evaluation contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_event_alpha_evaluation_contract.py -q
```

Alpha Projection Bridge v2 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_projection_bridge_v2.py -q
```

Paper overlay readiness tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_paper_overlay_readiness.py -q
```

Typed SUE alpha pilot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/typed_alpha_pilot/tests -q
```

## Q1 Project

Run when touching `projects/agentic_alpha_triage`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py
```

The Q1 example validation script also validates committed evaluator-plan manifests.

CLI dry-run smoke, when touching the Q1 evaluator planner or wrapper:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid
```

Rejected-plan audit JSON smoke, when touching Q1 evaluator rejection handling:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid --emit-rejected-json --indent 0
```

Batch manifest dry-run smoke, when touching Q1 evaluator manifest wrappers:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --indent 0
```

Batch manifest summary smoke, when touching Q1 evaluator manifest summary output:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --summary --indent 0
```

Q1 batch contract note is guarded by:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests/test_evaluator_batch_contract_doc.py -q
```

## Q2 Project

Run when touching `projects/execution_aware_optimizer`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```

Typed Q2 execution matrix contract test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py -q
```

Typed Q2 local adapter contract test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py -q
```

Typed expected-return injection fixture test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py -q
```

Smoke scripts, default non-execution mode:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/alpha_decay_ladder_results.csv --report projects/execution_aware_optimizer/reports/execution_aware_optimizer_report.md
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_cost_sensitivity.py --config projects/execution_aware_optimizer/configs/cost_sensitivity.yaml --output projects/execution_aware_optimizer/reports/cost_sensitivity_results.csv
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_constraint_diagnostics.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/constraint_diagnostics.json
```

Execution matrix smoke, default non-execution mode:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_execution_matrix.py --config projects/execution_aware_optimizer/configs/execution_matrix.yaml --output /tmp/portfolioos_q2_execution_matrix/execution_matrix.csv --summary-output /tmp/portfolioos_q2_execution_matrix/robustness_summary.json --report /tmp/portfolioos_q2_execution_matrix/execution_report.md
```

Explicit local executed fixture report smoke, only when touching that opt-in fixture/report path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/local_executed_fixture_report.yaml --output /tmp/portfolioos_q2_local_executed_fixture/alpha_decay_ladder_results.csv --report /tmp/portfolioos_q2_local_executed_fixture/local_executed_fixture_report.md
```

Typed Q2 local adapter smoke, only when touching Phase 47 adapter paths:

```bash
make typed-q2-adapter-fixture
```

Typed expected-return injection smoke, only when touching Phase 48 injection paths:

```bash
make typed-expected-return-injection-fixture
```

Typed optimizer response acceptance smoke, only when touching Phase 49 optimizer response paths:

```bash
make typed-optimizer-response-acceptance
```

SUE typed Q2 survival smoke, only when touching Phase 50 SUE survival paths:

```bash
make sue-typed-q2-survival
```

SUE execution-survival attribution smoke, only when touching Phase 51 attribution paths:

```bash
make sue-survival-attribution
```

Expanded SUE typed-Q2 candidate smoke and tests, only when touching Phase 56A paths:

```bash
make sue-expanded-typed-q2-survival
PYTHONDONTWRITEBYTECODE=1 poetry run python scripts/build_sue_expanded_q2_attribution.py
make alpha-registry-v2
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_expanded_typed_q2_survival.py -q
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_registry_v2.py -q
```

SUE optimizer input bridge smoke and tests, only when touching explicit Reopen-O1 bridge paths:

```bash
make sue-optimizer-input-bridge-fixture
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_typed_alpha_optimizer_input_bridge.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_optimizer_input_bridge_fixture.py -q
```

Revision marginal-value gate tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py -q
```

Revision marginal-value gate smoke, only when touching Phase 52 gate paths:

```bash
make revision-marginal-value-gate
```

## Evidence Bundle Project

Run when touching `projects/evidence_bundle`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/evidence_bundle/src poetry run pytest projects/evidence_bundle/tests -q
```

## Promotion Gate Project

Run when touching `projects/promotion_gate` or the Q1-to-Q2 handoff contract:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
```

## Audit Report Project

Run when touching `projects/audit_report` or unified report generation:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/audit_report/tests -q
```

Demo audit report smoke:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json
```

Demo audit report smoke with structured trace:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl
```

## PortfolioOS Core

Decision explainability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_decision_explainability.py -q
```

Provenance manifest tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_provenance_manifest.py -q
```

Structured trace tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_observability_trace.py -q
```

Local batch orchestrator tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_local_batch_orchestrator.py -q
```

Content-addressed cache tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_content_addressed_cache.py -q
```

Read-only artifact service tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_read_only_service.py -q
```

Static dashboard tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_static_dashboard.py -q
```

One-command demo tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_one_command_demo.py -q
```

README packaging tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_readme_packaging.py -q
```

Run a focused subset when touching optimizer, cost, execution, backtest, replay, scenario, or alpha bridge code:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_optimizer.py tests/test_cost.py tests/test_execution_simulation.py tests/test_backtest.py tests/test_alpha_backtest_bridge.py tests/test_event_targets.py tests/test_replay.py tests/test_scenarios.py -q
```

Run the full suite before a broad platform change or release-style handoff:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest -q
```

## Docs-Only Changes

For docs-only changes, run:

```bash
git diff --check
```

If the docs update changes workflow instructions, also run the relevant project tests for the affected project area.

## External Services

Do not run live Alpaca, FMP, WRDS, Tushare, or other paid/external-service workflows unless the user explicitly asks and required credentials are present in the environment.
