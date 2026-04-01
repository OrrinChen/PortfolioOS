# Pilot Runbook

This runbook is the minimum operating guide for a static cross-sectional pilot run.

For day-by-day pilot governance, KPI thresholds, and go/no-go policy, see:

- `docs/pilot_operations_plan.md`

## 1. Input Preparation

Prepare one sample directory with:

- `holdings.csv`
- `target.csv`
- `portfolio_state.yaml`
- `tickers.txt`

Then build feed files:

- `portfolio-os-build-market`
- `portfolio-os-build-reference`

Optional:

- `portfolio-os-build-target` when provider permissions allow index weights

If `target` permissions are limited, continue with client-provided `target.csv`.

## 2. One-Command Validation

Run nightly validation (provisional output allowed):

```bash
py -3.11 -m poetry run portfolio-os-validate-pilot --mode nightly
```

Run US nightly validation:

```bash
py -3.11 -m poetry run portfolio-os-validate-pilot --mode nightly --market us
```

Run release validation (strict gate, exits non-zero when not releasable):

```bash
py -3.11 -m poetry run portfolio-os-validate-pilot ^
  --mode release ^
  --reviewer-input outputs/pilot_validation_reviewer.csv ^
  --real-sample
```

Outputs land under `outputs/pilot_validation_<timestamp>/`:

- `evaluation/sample_assessment.csv`
- `pilot_validation_summary.md`
- `evaluation/provider_capability_report.json`
- per-sample logs and artifacts

Pilot operations helper commands:

```bash
py -3.11 scripts/pilot_ops.py init
py -3.11 scripts/pilot_ops.py nightly --phase phase_1 --real-sample --as-of-date 2026-03-24
py -3.11 scripts/pilot_ops.py weekly --phase phase_2 --reviewer-input outputs/reviewer_release.csv --real-sample
py -3.11 scripts/pilot_ops.py go-nogo --window-trading-days 20 --as-of-date 2026-03-24
```

US + Alpaca paper cycle:

```bash
py -3.11 scripts/pilot_ops.py nightly --phase phase_1 --market us --real-sample --broker alpaca --notes "us_pilot_smoke_test"
```

## 3. Failure Triage

Use per-sample logs under `samples/<sample_id>/logs/`:

- `01_*`, `02_*`: builder failures
- `03_*`: main rebalance failures
- `04_*`: scenario failures
- `05_*`: approval failures
- `06_*`: execution failures

Typical triage order:

1. Confirm builder manifests and input hashes.
2. Check precheck and optimizer errors in `03_main.log`.
3. Check approval status and blocking/warning codes in `approval_record.json`.
4. Check execution residual risk in `execution_report.json` and `execution_report.md`.
5. Check real-feed capability blockers in `evaluation/provider_capability_report.json`.

## 4. Approval Responsibilities

Approval request requires:

- decision owner (`decision_maker`, `decision_role`)
- rationale
- warning acknowledgement by code

When blocking findings exist and continuation is required:

- provide controlled `override` fields (`reason`, `approver`, `approved_at`)
- ensure `approval_status` becomes `approved_with_override`
- verify override metadata appears in:
  - `approval_record.json`
  - `freeze_manifest.json`
  - `handoff_checklist.md`

## 5. Re-run Cadence

Recommended cadence:

- code change: run full unit/integration tests
- data/constraint profile change: run `portfolio-os-validate-pilot`
- pre-demo or partner checkpoint: run pilot validation twice and compare summary deltas
- release candidate: run release mode with complete reviewer CSV and real sample enabled

Pilot-ready gate:

- static full-chain success `= 5/5`
- real full-chain success `= 1/1`
- static cost improvement vs naive `>= 70%`
- static override usage `<= 2/5`
- static scenario `score_gap >= 0.01` in at least `4/5`
- mean reviewer order/explainability/execution scores `>= 4/5`
- real market/reference capability must be non-degraded (`build_status=success`)

US variant gate:

- static full-chain success `= 3/3`
- real full-chain success `= 1/1`
- static override usage `<= 2/5` (absolute threshold kept)
- Alpaca execution outputs include `execution_result.json` and `reconciliation_report.json`
