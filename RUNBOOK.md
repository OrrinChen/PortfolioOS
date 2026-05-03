# RUNBOOK.md

## Setup

Install dependencies from the repository root:

```bash
poetry install
```

Check the active interpreter:

```bash
poetry run python -V
```

The expected development runtime is Python 3.11.

## Common Commands

Run all tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest -q
```

Run the CI-style local validation target:

```bash
make validate
```

Run the no-network guard self-test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run python scripts/devtools/no_network_guard.py
```

Run Q1 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
```

Validate Q1 contract examples:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py
```

This command also validates the committed Q1 evaluator-plan manifest.

Print Q1 dry-run evaluator plan JSON:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid
```

Print Q1 audit-only rejected evaluator-plan JSON:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid --emit-rejected-json
```

Print Q1 batch evaluator-plan manifest JSON:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml
```

Print Q1 batch evaluator-plan manifest summary:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --summary
```

Read the Q1 batch dry-run contract:

```bash
sed -n '1,220p' projects/agentic_alpha_triage/docs/evaluator_batch_contract.md
```

Run Q2 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```

Run Evidence Bundle tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/evidence_bundle/src poetry run pytest projects/evidence_bundle/tests -q
```

Run Promotion Gate tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
```

Run Q2 report smoke path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml
```

Run Q2 local executed fixture report smoke path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/local_executed_fixture_report.yaml --output /tmp/portfolioos_q2_local_executed_fixture/alpha_decay_ladder_results.csv --report /tmp/portfolioos_q2_local_executed_fixture/local_executed_fixture_report.md
```

Run Q2 execution matrix smoke path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_execution_matrix.py --config projects/execution_aware_optimizer/configs/execution_matrix.yaml --output /tmp/portfolioos_q2_execution_matrix/execution_matrix.csv --summary-output /tmp/portfolioos_q2_execution_matrix/robustness_summary.json --report /tmp/portfolioos_q2_execution_matrix/execution_report.md
```

Run decision explainability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_decision_explainability.py -q
```

Run unified demo audit report tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/audit_report/tests -q
```

Build the unified demo audit report:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json
```

Build the unified demo audit report with structured trace:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl
```

Run provenance manifest tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_provenance_manifest.py -q
```

Run structured trace tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_observability_trace.py -q
```

Run local batch orchestrator tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_local_batch_orchestrator.py -q
```

Run content-addressed cache tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_content_addressed_cache.py -q
```

Run read-only artifact service tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_read_only_service.py -q
```

Run PortfolioOS backtest CLI:

```bash
poetry run portfolio-os-backtest --manifest data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml --output-dir outputs/backtest_smoke
```

Run static replay:

```bash
poetry run portfolio-os-replay --manifest data/replay_samples/manifest.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/replay_smoke
```

## Q1 / Q2 Operating Rules

Q1:

- schema-first hypothesis triage only
- no autonomous trading behavior
- no live FMP/SEC ingestion by default
- may later export `alpha_score.csv`

Q2:

- accepts independent alpha scores
- may consume Q1 exports as plain files only
- uses PortfolioOS through explicit adapters
- records unavailable layers honestly
- does not fabricate backtest, net-performance, or constraint numbers

## Troubleshooting

If `python` is not found:

```bash
poetry run python -V
```

Use `poetry run python` or `python3` instead of bare `python`.

If imports fail for project shells:

```bash
PYTHONPATH=src:projects/execution_aware_optimizer/src:projects/agentic_alpha_triage/src poetry run python -c "import execution_aware_optimizer, agentic_alpha_triage"
```

If tests create cache files in project shells:

```bash
find projects -type d -name __pycache__ -prune -exec rm -rf {} +
```

Prefer setting:

```bash
PYTHONDONTWRITEBYTECODE=1
```

If a Q2 script outputs unavailable rows:

- check `portfolioos.allow_portfolioos_run`
- confirm a manifest is configured
- confirm the adapter actually supports that layer
- do not treat unavailable rows as failed performance results

If external-service credentials are missing:

- stop unless the user explicitly asked for live service work
- report the missing environment variable
- do not hardcode credentials

## Handoff Checklist

Before ending a phase:

1. Run relevant validation from `VALIDATION.md`.
2. Run `git diff --check`.
3. Update `TASK_MEMORY.md`.
4. Summarize files changed, tests run, known limitations, and next phase.
