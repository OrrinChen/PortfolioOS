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

Run Q1 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
```

Validate Q1 contract examples:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py
```

Run Q2 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```

Run Q2 report smoke path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml
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
