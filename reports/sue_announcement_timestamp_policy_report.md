# SUE Announcement Timestamp Source / Anchor Policy Audit

-5/-10 windows cannot be used as tradable SUE unless actual EPS availability is proven earlier.
If no earlier timestamp source exists, SUE remains blocked before typed projection/Q2.
This phase does not approve paper/live/broker/order/production workflows.
It does not select a production SUE score.
It does not run Q2 or optimizer-path evaluation.

## Decision

- schema_version: `sue_announcement_timestamp_policy.v1`
- decision_label: `no_auditable_earlier_timestamp_sue_blocked`
- event_count: `17027`
- repaired_event_count: `0`
- blind_shift_policy_allowed: `False`
- selected_score: `None`
- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- production_approval_claimed: `False`

## Anchor Policies

| Policy | Eligible Events | Description |
| --- | ---: | --- |
| `current_policy` | 17027 | Use the existing IBES/WRDS event_available_timestamp and next tradable timestamp. |
| `conservative_date_only_next_open` | 0 | Use date-only source dates only at next market open. |
| `after_close_next_open` | 0 | Use after-close source timestamps at next market open. |
| `before_open_same_day_or_next_open` | 0 | Use before-open source timestamps at same-day open when still after source visibility. |
| `source_repaired_announcement_timestamp` | 0 | Use an earlier auditable source timestamp, never a blind shifted placebo. |
| `blocked_if_no_auditable_timestamp` | 17027 | Block timing repair when no earlier auditable actual-EPS source exists. |

## H1E Rerun Status

- rerun_attempted: `False`
- rerun_required: `False`
- blocked_reason: `no_auditable_earlier_timestamp_source`
- h1e_selected_score: `None`
- h1e_interpretation: `None`

## Boundaries

- A shifted placebo result is not an auditable timestamp source.
- A repaired policy must keep tradability after actual EPS public availability.
- No Alpha Registry promotion is performed by this audit.
