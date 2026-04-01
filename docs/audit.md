# Audit JSON Structure

The audit package is a local JSON file that records what went into the run and what came out of it.

## Top-Level Keys

- `run_id`: unique local identifier for the run
- `created_at`: ISO timestamp
- `disclaimer`: risk statement
- `inputs`: source file paths, sizes, and SHA256 hashes
- `parameters`: merged runtime configuration
- `constraints_snapshot`: resolved constraint template, including the effective single-name limit
- `findings`: pre-trade findings after repair
- `orders`: exported order basket as structured records
- `summary`: portfolio-level summary metrics
- `export_readiness`: OMS/export release status derived from blocking findings

## Structured Finding Fields

Each finding now includes:

- `code`: stable rule or event code such as `trade_blocked` or `single_name_limit`
- `category`: one of `tradability`, `regulatory`, `risk`, `cash`, `data_quality`
- `severity`: `INFO`, `WARNING`, or `BREACH`
- `ticker`: optional security code
- `message`: human-readable explanation
- `rule_source`: source rule path such as `constraints.participation_limit`
- `blocking`: whether the finding should prevent a clean release into an OMS-style workflow
- `repair_status`: `not_needed`, `repaired`, `partially_repaired`, or `unresolved`
- `details`: structured numeric or contextual payload

### Example Finding

```json
{
  "code": "trade_blocked",
  "category": "tradability",
  "severity": "BREACH",
  "ticker": "000858",
  "message": "Desired trade was removed because security is at a price limit and cannot trade.",
  "rule_source": "constraints.blocked_trade_policy",
  "blocking": true,
  "repair_status": "repaired",
  "details": {
    "requested_quantity": -500.0
  }
}
```

### Data Quality Example

```json
{
  "code": "benchmark_weight_total_anomaly",
  "category": "data_quality",
  "severity": "WARNING",
  "ticker": null,
  "message": "Reference benchmark weights look abnormal for the covered universe and should be reviewed before using benchmark-relative interpretation.",
  "rule_source": "reference.csv",
  "blocking": false,
  "repair_status": "not_needed",
  "details": {
    "benchmark_weight_total": 1.75,
    "covered_ticker_count": 7,
    "benchmark_weight_count": 7
  }
}
```

### Blocked / Repaired Explanation Example

```json
{
  "code": "small_order_removed",
  "category": "tradability",
  "severity": "INFO",
  "ticker": "601012",
  "message": "Residual order was removed because it fell below the minimum notional threshold.",
  "rule_source": "constraints.min_order_notional",
  "blocking": false,
  "repair_status": "repaired",
  "details": {
    "quantity": 100.0,
    "notional": 2500.0,
    "reason_label": "dust_order_below_min_notional",
    "repair_action": "removed_dust_order"
  }
}
```

## Purpose

The JSON package is meant for:

- reproducibility
- compliance review
- PM and trader handoff
- local run cataloging

The MVP stores the package on disk only. No database is required.
