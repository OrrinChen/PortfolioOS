# Expanded SUE Typed-Q2 Survival Report

This is an expanded typed-Q2 candidate benchmark, not production approval.
production approval: not claimed

## Evidence Scope

- deterministic fixture evidence: expanded local SUE event panel
- real historical evidence: not claimed
- paper-ready status: not claimed

## Summary

- event_count: `120`
- rebalance_date_count: `12`
- active_rebalance_count: `12`
- active_name_count: `12`
- median_active_names_per_active_date: `10.00`
- expected_return_used_share: `0.833333`
- abstain_count: `1608`
- coverage_loss_count: `24`
- q2_observed_rows: `30`
- q2_unavailable_rows: `0`

## Attribution

| layer | status | details |
|---|---|---|
| evidence | observed | 120 deterministic fixture event-name rows validate PIT sequencing. |
| projection | observed | active_rebalance_count=12; active_name_count=12; coverage_loss_count=24 |
| injection | observed | injection_status=injected |
| optimizer_response | observed | Phase 49 validates local optimizer response; Phase 56A uses a representative expanded SUE date. |
| constraint_repair | observed | observed_rows=30 |
| cost | observed | observed_rows=30 |
| turnover | observed | observed_rows=30 |
| coverage / abstain | observed | abstain_count=1608; no_view is not encoded as zero alpha. |

## Safety Boundaries

- no live data workflow
- no broker workflow
- no orders or trading instructions
- no production alpha approval
- missing coverage is explicit abstain; no_view != zero_alpha
