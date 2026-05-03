# PortfolioOS Demo Audit Report

This standalone project builds the interview-readable audit report for the
current platform spine:

```text
Q1 Alpha Triage -> Evidence Bundle -> Promotion Gate -> Q2 Execution Matrix -> Audit Report
```

It uses only committed local fixtures. It does not call live services, run
broker workflows, create trading instructions, or fabricate performance
numbers.

## Demo Cases

- Promoted-like case: valid guidance-raise evidence is promoted only to a Q2
  input contract, then evaluated through the default non-execution Q2 matrix.
- Rejected leakage case: forward-return leakage is rejected before Q2 execution
  evaluation, and the report records that Q2 was skipped.

## Run

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json
```

The script writes a provenance sidecar for the report. Use an output path under
`/tmp` during validation to avoid committing machine-specific timestamps.

Optional structured trace sidecar:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl
```

The trace records local workflow events such as bundle loading, schema
validation, promotion decisions, Q2 unavailable rows, and report writes. It
does not record credentials, broker output, orders, trading instructions, or
live performance.

## Test

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/audit_report/tests -q
```
