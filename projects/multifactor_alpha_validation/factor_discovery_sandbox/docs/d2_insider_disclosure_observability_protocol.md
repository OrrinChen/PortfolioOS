# D2-INSIDER-01: Insider Disclosure Regime Observability Protocol

Status: D2 protocol draft.

Coupling group: `insider_disclosure_regime_2023`.

Source ledger rows: `PP-01`, `PP-22`, `PP-23`, `PP-24` in
`alpha_pain_point_ledger.md`.

This protocol defines no formula, no score, no composite, no Q1 input, no Q2
input, no optimizer path, no Alpha Registry update, no paper workflow, no
broker/order workflow, no live workflow, and no production approval. It only
defines how to observe whether the insider-disclosure regime pain point has an
auditable, falsifiable event-time footprint.

## Purpose

The D2 question is not "does this alpha work?" The D2 question is:

```text
Do post-2023 insider disclosure subsets show event-local behavior that is
visible only after EDGAR filing availability, consistent with the stated
mechanism, and not explained by timestamp leakage, non-event controls,
transaction-type controls, role randomization, shifted dates, or simple
sector/size/liquidity exposure?
```

Only if this protocol returns `observable` or a clearly narrowed `mixed`
decision may a later D3 charter freeze a MeasurementSpec.

## Regulatory And Source Anchors

The implementation should record source references in data lineage. The D2
protocol relies on these public source classes:

- EDGAR accepted datetime for Form 4 / Form 5 ownership filings.
- Official Form 4 transaction-code semantics. Code `P` is an open-market or
  private purchase; code `S` is an open-market or private sale; codes such as
  `A`, `M`, `F`, and `G` are controls or exclusions, not primary event classes.
- Post-2023 Rule 10b5-1 related Form 4 / Form 5 fields, including whether a
  transaction was intended to satisfy Rule 10b5-1 and the plan adoption date
  when present.

Exact regulatory dates and parser field names must be preserved in the future
data lineage manifest. This protocol does not hardcode a tradable inference
from transaction date.

## Coupled Hypotheses

The insider-disclosure rows are not independent candidates. They form a single
triangulation:

| Hypothesis | D2 readout | Expected D2 footprint |
| --- | --- | --- |
| `PP-22 open_market_insider_buying_post_2023` | Event subset | Post-filing returns for qualifying open-market purchases should be positive after auditable EDGAR visibility. |
| `PP-23 discretionary_sell_vs_planned_sell` | Contrast subset | Discretionary sells should have more negative information content than planned sells if the regime split matters. |
| `PP-24 planned_sell_information_compression` | Regime diagnostic | Planned sells should have weaker post-filing information content than discretionary sells if planned execution is lower signal. |
| `PP-01 post_2023_10b5_1_plan_events` | Supporting event class | Plan-event disclosures may help explain the regime, but direction must not be inferred without auditable event timing and event type. |

The coupled group is stronger than any one row. A lucky sample can make one
subset look interesting; the D2 target is a coherent pattern across buys,
discretionary sells, planned sells, and timing controls.

## Hard Boundary

D2 may define:

- event extraction requirements
- timestamp contracts
- event subsets
- return windows
- matched controls
- placebo tests
- coverage gates
- D2 decision labels

D2 may not define:

- role weights
- cluster weights
- holding-change weights
- cross-sectional z-scores
- composite scores
- optimizer inputs
- portfolio weights
- Q1 or Q2 handoff artifacts
- Alpha Registry decisions
- paper, broker, order, live, or production workflows

## Event Registry Contract

Each extracted event row must carry these fields before any event-time read:

```text
event_id
issuer_cik
issuer_ticker
issuer_permno_or_security_id
accession_number
form_type
is_amendment
filing_accepted_timestamp
visibility_timestamp
tradable_timestamp
reporting_person_id
reporting_person_name_hash
is_director
is_officer
officer_title
is_10pct_owner
transaction_code
transaction_date
transaction_shares
transaction_price
transaction_dollar_value
acquired_or_disposed
security_title
is_derivative_security
ownership_form_direct_or_indirect
post_transaction_holding
rule_10b5_1_indicator
plan_adoption_date
event_subset
event_cluster_id
market_cap_at_visibility
adv_20d_at_visibility
spread_proxy_at_visibility
sector
industry
price_coverage_state
return_coverage_state
coverage_state
diagnostic_only
no_view_reason
```

`transaction_date` is a disclosure lag diagnostic only. It is never the event
anchor for tradable returns.

## Timestamp Contract

The canonical visibility and tradability policy is:

```text
visibility_timestamp = EDGAR filing accepted datetime
tradable_timestamp = first regular market session open after visibility_timestamp
```

Rules:

- If accepted datetime is missing, the row is `diagnostic_only=true`.
- If only a filing date is available, the row is `diagnostic_only=true` unless
  a later implementation defines a conservative date-only policy in a separate
  charter.
- If `tradable_timestamp <= visibility_timestamp` cannot be proven, the row is
  blocked.
- Return windows must start after `tradable_timestamp`.
- Same-close trading is not allowed.
- Transaction date can be used for filing-lag reports, but cannot be used to
  start event returns.
- Missing return coverage is `no_view`, not zero.

## Event Subsets

Future D2 execution should build these subsets from the same event stream:

| Subset | Inclusion | D2 role |
| --- | --- | --- |
| `open_market_buy` | Form 4, transaction code `P`, non-derivative common stock, acquired shares, auditable filing timestamp | Primary observability subset, not a formula. |
| `discretionary_sell` | Form 4, transaction code `S`, non-derivative common stock, disposed shares, post-2023 10b5-1 indicator false | Contrast subset, not a short-alpha formula. |
| `planned_sell` | Form 4, transaction code `S`, non-derivative common stock, disposed shares, post-2023 10b5-1 indicator true | Compression diagnostic subset. |
| `sell_unknown_plan_flag` | Code `S` with missing or ambiguous post-2023 plan flag | Audit exception / no-view, not merged into either sell subset. |
| `compensation_controls` | Codes `A`, `M`, `F`, `G`, derivative-only rows, grants, exercises, tax withholding, gifts | Negative controls and contamination checks. |
| `plan_event_disclosure` | Plan adoption, modification, or termination disclosures with auditable accepted timestamp | Timing / regime diagnostic only unless direction is later chartered. |

Form amendments (`4/A`) may repair metadata for an earlier event only if the
original event can be linked. Unlinked amendments are `diagnostic_only=true`.

Issuer-day or issuer-window clusters should be created only to avoid
double-counting closely related filings. Cluster construction must not assign
weights in D2.

## Return Windows

Let `T0` be the first tradable session after `tradable_timestamp`.

Preferred event returns use entry at the first executable open at or after
`tradable_timestamp`. If only daily close data is available, D2 must use a
conservative close-entry fallback and label the window set as
`daily_close_conservative`.

Required windows:

| Window | Purpose |
| --- | --- |
| `[-20,-1]` | Long pre-event drift audit. |
| `[-10,-1]` | Main pre-event drift audit. |
| `[-5,-1]` | Late pre-event leakage / anticipation audit. |
| `[0,+1]` | Immediate post-visibility diagnostic. |
| `[+1,+5]` | Short event-local read. |
| `[+1,+10]` | Short-to-medium event-local read. |
| `[+1,+22]` | Primary D2 observability read. |
| `[+1,+44]` | Decay diagnostic only. |

Required return versions:

- raw event return
- market-adjusted event return
- sector-adjusted event return when sector returns are available

No window can be selected after seeing results. D2 reports all required
windows and highlights `[+1,+22]` only as the preregistered primary
observability window.

## Matched Controls

Controls must be defined before looking at event-window results:

1. `issuer_matched_non_event`
   - Same issuer.
   - Non-event dates with no Form 4 event in the exclusion window.
   - Matched by calendar month where possible.

2. `sector_size_liquidity_matched_non_event`
   - Same sector when available.
   - Similar market-cap bucket.
   - Similar ADV / spread bucket.
   - Same calendar month or quarter.

3. `transaction_type_controls`
   - Codes `A`, `M`, `F`, `G`, grants, exercises, tax withholding, gifts, and
     derivative-only transactions.

4. `pre_2023_baseline_controls`
   - Pre-regime Form 4 event classes used only to contextualize reporting
     changes.
   - Must not be merged into post-2023 event subsets.

5. `issuer_cluster_controls`
   - Issuers with multiple filings in a short window should be checked against
     issuer-clustered controls to avoid over-counting serial reports.

## Placebo Suite

D2 requires these placebos before any D3 charter:

| Placebo | Required behavior |
| --- | --- |
| `shifted_filing_dates` | Shift accepted timestamps by `-10`, `-5`, `+5`, and `+10` trading days. Shifted windows must not beat the live window for the same subset. |
| `issuer_matched_non_event` | Same-issuer non-event dates must not reproduce the live event footprint. |
| `role_randomized` | Reporting-person role labels randomized within event month and subset must not outperform true roles if role-based diagnostics are reported. |
| `plan_flag_randomized` | Post-2023 planned/discretionary flags randomized within sell events must not reproduce the planned-vs-discretionary contrast. |
| `transaction_type_control` | Compensation / derivative / tax / gift control transactions must not match or beat the open-market purchase subset. |
| `pre_event_drift` | Pre-event windows must not dominate post-filing tradable windows. |
| `same_coverage_random` | Random event labels preserving the coverage mask must not beat live event subsets. |

If a placebo beats the live read, D2 must classify the result as `mixed`,
`not_observable`, or a specific blocker. It must not proceed by changing
windows or event definitions.

## Observability Metrics

Each subset/control/window report should include:

```text
event_count
issuer_count
event_month_count
median_events_per_month
coverage_share
diagnostic_only_count
no_view_count
mean_raw_return
mean_market_adjusted_return
mean_sector_adjusted_return
t_stat
monthly_sign_consistency
pre_event_to_post_event_ratio
placebo_advantage
control_advantage
```

The coupled insider regime summary must additionally report:

```text
buy_post_filing_direction
discretionary_sell_vs_planned_sell_direction
planned_sell_compression_read
plan_event_timing_read
timestamp_blocker_count
unknown_plan_flag_count
event_subset_with_minimum_sample_count
```

`planned_sell_compression_read` is a diagnostic ratio computed from event-path
readouts, not a trading formula. It exists only to decide whether the planned
sell compression thesis is observable enough to charter later.

## Minimum Sample Contract

The D2 run is sample-admitted only if:

- at least `24` event months exist for the coupled group
- at least `300` qualifying open-market buy events exist
- at least `300` qualifying sell events exist across planned and
  discretionary sell diagnostics
- at least `50` issuer-level event clusters exist for the open-market buy
  subset
- at least `70%` of sample-admitted rows have price and return coverage
- unknown post-2023 sell plan flags are reported separately and not silently
  assigned

If any requirement fails, D2 returns `hold_insufficient_sample` or
`blocked_data_coverage`. It must not widen the hypothesis after seeing results.

## Hard Blockers

D2 must block or demote to diagnostic-only when:

- accepted filing timestamp is missing
- transaction date is used as the return anchor
- return window starts before tradable timestamp
- post-2023 sell plan flag is missing and the row is merged into either sell
  subset
- derivative-only / compensation / tax / gift events enter the open-market buy
  or sell contrast subsets
- missing coverage is encoded as zero
- shifted-date or same-coverage random placebo beats the live subset
- pre-event drift dominates the post-filing tradable window
- matched controls fully explain the live event path
- report text claims validation, Q1/Q2 readiness, portfolio readiness, paper
  readiness, live readiness, broker/order readiness, or production approval

## D2 Decision Labels

| Decision | Meaning | Next action |
| --- | --- | --- |
| `observable` | Coupled group passes timestamp, sample, coverage, control, and placebo gates with the preregistered direction pattern. | Write D3 charter or explicitly opened D3 MeasurementSpec for the named subset only. |
| `mixed_narrow_scope` | One subset is promising, but at least one coupled readout or placebo is weak. | Narrow the mechanism and rerun D2 once; do not write formula. |
| `not_observable` | Expected event footprints are absent or controls/placebos dominate. | Archive coupled group with reason; promote next shadow-queue family. |
| `blocked_timestamp` | Event visibility or tradability cannot be audited. | Fix timestamp source or stop. |
| `blocked_data_coverage` | Price, return, sector, liquidity, or mapping coverage is insufficient. | Fix data coverage or stop. |
| `hold_insufficient_sample` | Minimum event-month or event-count contract fails. | Hold until more data exists; do not relax gates. |
| `blocked_placebo_dominance` | Shifted-date, same-coverage random, or control placebos beat live subsets. | Stop before D3. |

## D2 To D3 Boundary

An `observable` D2 decision does not create a formula implementation. It only
permits a separate D3 charter or explicitly opened D3 MeasurementSpec to be
drafted for the named subset. That D3 artifact must freeze, before any formula
implementation:

- the event subset to measure
- the event direction
- the primary horizon
- the timestamp policy
- the no-view / abstain policy
- the allowed diagnostic variants
- the hard falsifiers inherited from this protocol

The D3 charter may still decide that the mechanism is too narrow, sparse, or
costly to measure. No D2 decision can bypass D3, Q1, Promotion Gate, or Q2.

## Future D2 Artifact Contract

A later D2 runner, if explicitly opened, should write:

```text
outputs/factor_discovery/design_layer/d2_insider/
  insider_event_registry_audit.csv
  insider_event_subset_summary.csv
  insider_car_window_grid.csv
  insider_matched_control_report.csv
  insider_placebo_report.json
  insider_timestamp_audit.json
  insider_coverage_report.json
  insider_observability_decision.json
reports/factor_discovery_insider_d2_observability_report.md
```

Those future artifacts remain D2 diagnostics only. They must not include a
signal panel, formula output, optimizer input, portfolio return, Q1 promotion,
Q2 input, Alpha Registry update, paper workflow, broker/order workflow, live
workflow, or production approval.

## References To Current Track A Boundary

This protocol depends on the Track A rules already recorded in
`alpha_pain_point_ledger.md` and `factor_design_layer_spec.md`:

- formula is measurement, not thesis
- no-view is not zero alpha
- D0/D1/D2 cannot run Q1, Q2, optimizer, allocator, Alpha Registry, paper,
  broker/order, live, or production workflows
- hard guard failures must stop before attractive evidence is produced

## External Regulatory References

Future implementation should verify parser details against current official SEC
materials before coding:

- SEC final-rule / small-entity guidance for Rule 10b5-1 insider trading
  arrangements and related disclosure requirements.
- SEC Form 4 instructions and transaction-code tables.
- EDGAR ownership XML technical field names for Rule 10b5-1 indicators and plan
  adoption date fields.

These references constrain timestamp and event parsing only. They do not
promote this protocol into Q1, Q2, portfolio construction, paper trading,
broker/order workflows, live workflows, or production use.
