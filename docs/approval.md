# Approval Workflow

## Purpose

The approval workflow is a lightweight local-file mechanism that turns a scenario comparison into a frozen execution package.

It is intentionally simple:

- no database
- no permissions system
- no digital signature service
- no live OMS integration

Its purpose is to make the following chain explicit:

- compare scenarios
- choose one
- record who chose it and why
- acknowledge warnings
- freeze the handoff package

## Approval Request Format

Example file: `data/approval_samples/approval_request_example.yaml`

Required fields:

- `name`
- `scenario_output_dir`
- `decision_maker`
- `decision_role`
- `rationale`

Optional fields:

- `description`
- `selected_scenario`
- `acknowledged_warning_codes`
- `override`
- `handoff`
- `tags`

Behavior:

- if `selected_scenario` is omitted, the workflow uses the `recommended_scenario` from `scenario_comparison.json`
- `acknowledged_warning_codes` must cover all warning codes present in the selected scenario if the package is to be approved

## Approval Status Rules

Current statuses:

- `approved`
- `approved_with_override`
- `rejected`
- `incomplete_request`

Current rule set:

- if the selected scenario contains blocking findings and no valid override is provided, status is `rejected`
- if blocking findings exist and a complete override payload is provided, status is `approved_with_override`
- if warnings exist and at least one warning code is not acknowledged, status is `incomplete_request`
- otherwise, status is `approved`

Override payload fields (required when `override.enabled=true`):

- `override.reason`
- `override.approver`
- `override.approved_at`

This is a workflow rule only. It is not a legal approval system or an investment approval framework.

## Final Execution Package

When the request is approved, the workflow freezes:

- `final_orders.csv`
- `final_orders_oms.csv`
- `final_audit.json`
- `final_summary.md`

It also writes:

- `approval_record.json`
- `approval_summary.md`
- `freeze_manifest.json`

## Freeze Consistency

The freeze step is implemented by copying the selected scenario artifacts and recording hashes for:

- the approval request
- the source scenario comparison file
- the selected scenario artifacts
- the copied final artifacts

This helps show that the files reviewed at approval time are the same files handed off after freeze.

## Warning Acknowledgement

Warnings are acknowledged by code, not by free-text matching.

Example:

```yaml
acknowledged_warning_codes:
  - no_order_due_to_constraint
  - manager_aggregate_limit
```

This keeps the mechanism simple and auditable.

Example override block:

```yaml
override:
  enabled: true
  reason: "Controlled pilot override for blocking findings."
  approver: "risk_head_demo"
  approved_at: "2026-03-24T09:30:00+00:00"
```
