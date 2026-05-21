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

Validate the two-track research boundary registry:

```bash
make research-track-boundaries
```

This reads `configs/research_tracks.yaml` and verifies that single-alpha
research, Factor Discovery candidate design, formal multi-factor portfolio
validation, and shared Q1/Q2 governance remain separated. It does not run
research, Q2, optimizer, broker, paper, or production workflows.

Run all tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest -q
```

Run the CI-style local validation target:

```bash
make validate
```

Run the one-command local demo:

```bash
make demo
```

Run the typed alpha local demo v2:

```bash
make demo-v2
```

Run the PortfolioOS portfolio quant walk-forward smoke:

```bash
make portfolio-quant-walk-forward
```

This writes no-lookahead monthly historical portfolio evaluation artifacts under
`outputs/portfolio_quant_walk_forward/`, including NAV, drawdown, turnover
distribution, equal-weight / mean-variance / risk-parity / cost-unaware /
PortfolioOS cost-aware comparisons, CVaR, exposure drift, policy-gate rows, and
a markdown report. It is a portfolio construction / execution-aware evaluation
smoke only and does not open alpha research, Q1/Q2, broker/order/live, or
production workflows.

Run typed alpha release-candidate hardening tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_release_candidate.py -q
```

Run demo-v2 golden snapshot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_demo_v2_golden_snapshot.py -q
```

Build and test the typed alpha closeout report:

```bash
make typed-alpha-closeout
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_closeout_report.py -q
```

Build and test Alpha Registry v2:

```bash
make alpha-registry-v2
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_registry_v2.py -q
```

Run the formal multifactor research-mode preflight:

```bash
make multifactor-research-mode-preflight
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_week1_contracts.py projects/multifactor_alpha_validation/tests/test_research_mode_preflight.py -q
```

Check and run the WRDS option B ingest after local WRDS credentials are
configured outside the repo:

```bash
make multifactor-wrds-config-check
make multifactor-external-source-check
WRDS_USERNAME=<your_wrds_username> PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run python projects/multifactor_alpha_validation/scripts/run_wrds_multifactor_ingest.py --config projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml --require-ready
make multifactor-real-dataset-dry-run
```

The real dataset dry run reads the local WRDS monthly PIT bundle only; it does
not reconnect to WRDS. It writes MF-R7 coverage, timestamp, universe,
benchmark, delisting, signal-availability, and daily-long-task artifacts under
`outputs/multifactor_alpha_validation/wrds_real_dataset_dry_run/`. Daily
price-volume validation remains a separate explicit long task tracked by
`projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_price_volume_long_task.yaml`.

Build local PIT-style universe snapshots from the synthetic onboarding fixture:

```bash
make multifactor-research-universe
make multifactor-research-panels
make multifactor-research-delistings
make multifactor-first-research-dry-run
make multifactor-rolling-oos-validation
```

Run the FD real-data validation admission/universe/return audit line after the
WRDS monthly PIT bundle exists locally:

```bash
make factor-discovery-design-layer
make factor-discovery-design-d1
make factor-discovery-design-d2
make factor-discovery-insider-d2-observability
make factor-discovery-insider-d2-real-observability
make factor-discovery-fd-wide-design-audit
make factor-discovery-factor-specs
make factor-discovery-real-data-validation
make factor-discovery-real-data-validation-daily
make factor-discovery-real-factor-replay
make factor-discovery-real-rolling-oos
make factor-discovery-real-placebo-robustness
make factor-discovery-real-failure-diagnosis
make factor-discovery-formula-mechanism-audit
make factor-discovery-weighting-reliability
make factor-discovery-momentum-low-vol-candidate
make factor-discovery-revision-confirmed-alpha
make factor-discovery-small-cap-wrds-pull
make factor-discovery-small-cap-quality-pull
make factor-discovery-small-cap-data-admission
make factor-discovery-small-cap-quality-residual-momentum
make factor-discovery-small-cap-dominance-diagnosis
make factor-discovery-small-cap-s4-2
make factor-discovery-small-cap-s4-3
make factor-discovery-small-emotion-d2
```

This writes FD-R0/R1/R2 artifacts under `outputs/factor_discovery/real_data/`.
It validates data admission, PIT universe construction, symbol mapping,
survivorship bias, adjusted returns, QQQ benchmark alignment, and
corporate-action audit readiness. It does not rank factors, run allocator
weights, claim alpha success, approve production, or enter Q2.

The FD design-layer target writes
`outputs/factor_discovery/design_layer/factor_design_contract_validation.json`
and `reports/factor_discovery_design_layer_report.md`. It is the required
pre-formula gate: every future factor design must document the market pain
point, mechanism, investor constraint or flow, expected universe/regime,
observable diagnostics, placebo design, cost/capacity risks, and failure modes.
The formula is measurement, not thesis.

The FD-wide design audit scans existing candidate output directories:

```bash
make factor-discovery-fd-wide-design-audit
```

It writes `outputs/factor_discovery/design_audit/fd_wide_design_manifest_audit.csv`,
`fd_wide_design_manifest_audit.json`, and
`reports/factor_discovery_fd_wide_design_audit.md`. Any candidate or family
decision output directory missing a valid same-directory
`candidate_design_manifest.json` is a blocker for further FD validation. This
audit does not approve allocator, Q1/Q2, Alpha Registry, broker/order/live, or
production paths.

The FD-D1 target writes the mechanism-first market pain-point map:

```bash
make factor-discovery-design-d1
```

It writes `factor_pain_point_map.md`, `factor_design_ledger.csv`,
`candidate_family_backlog.json`, and `factor_design_d1_summary.json` under
`outputs/factor_discovery/design_layer/d1/`. The ledger preserves existing
candidate diagnostics as prior history only, then separates market pain point,
mechanism, pre-formula diagnostics, placebo design, and cost/capacity risks
before any future formula validation. It does not run backtests, Q1/Q2,
allocator, Alpha Registry, broker/order/live, or production approval paths.

The FD-D2 target writes pre-formula diagnostics:

```bash
make factor-discovery-design-d2
```

It reads the FD-D1 ledger when available and writes
`pre_formula_diagnostics.csv`, `candidate_family_d2_decisions.json`,
`pre_formula_diagnostic_summary.json`, and
`pre_formula_diagnostic_report.md` under
`outputs/factor_discovery/design_layer/d2/`. FD-D2 checks coverage,
PIT/timestamp readiness, placebo design, exposure-contamination risk, and
cost/capacity before any formula validation. It can mark a family ready for a
D3 charter, but it cannot allow formula validation, Q1/Q2 entry, allocator,
Alpha Registry, broker/order/live, or production approval paths.

Run the D2-INSIDER-01 no-formula observability fixture:

```bash
make factor-discovery-insider-d2-observability
```

It writes `insider_event_registry.csv`, event subset counts,
timestamp/tradability audits, CAR-window diagnostics, matched controls,
placebo diagnostics, `d2_observability_summary.json`, and
`d2_insider_disclosure_observability_report.md` under
`outputs/factor_discovery/insider_disclosure/d2/`. This target only checks
whether the active insider-disclosure pain-point group has a clean observable
D2 footprint. It writes no formula score or MeasurementSpec and cannot enter
Q1/Q2, allocator, optimizer, Alpha Registry, paper, broker/order/live, or
production approval paths.

Run the D2-INSIDER-01R local Form 4 extraction and replay path:

```bash
make factor-discovery-insider-d2-real-observability
```

By default it reads local SEC-style ownership XML and `form4_source_index.csv`
from `data/cache/sec_form4_insider_disclosure/` and writes artifacts under
`outputs/factor_discovery/insider_disclosure/d2_real/`. If the local archive is
missing, the target exits cleanly with `missing_inputs_report.json` and
`real_data_status=unavailable_missing_source`; it does not fall back to fixture
events or fabricate historical evidence. With local inputs present, it writes
`form4_source_manifest.json`, XML parse coverage, issuer/timestamp audits,
`insider_event_registry_real.csv`, market-join/no-view reports, and the same
no-formula D2 replay artifacts with `_real` suffixes. It uses no network fetch
and cannot write formula scores, MeasurementSpec files, Q1/Q2 handoffs,
optimizer inputs, Alpha Registry updates, paper workflows, broker/order/live
paths, or production approval.

Run the D2-INSIDER-02 planned-vs-discretionary sell contrast:

```bash
make factor-discovery-insider-d2-sell-contrast
```

It reads the real Form 4 aggregate event registry and local label price panel,
then writes S-code sell subset counts, plan-flag coverage, no-view reasons,
CAR-window diagnostics, matched controls, placebo diagnostics, a D2 summary,
and a report under
`outputs/factor_discovery/insider_disclosure/d2_sell_contrast/`. This is
no-formula observability only. If the parsed S-code stream cannot split planned
and discretionary sells, the target must stop with
`blocked_plan_flag_coverage` and must not write a MeasurementSpec, formula
score, expected-return panel, Q1/Q2 handoff, optimizer input, Alpha Registry
update, paper workflow, broker/order/live path, or production approval.

Run the D2-INSIDER-02A plan-flag parser/source audit:

```bash
make factor-discovery-insider-plan-flag-audit
```

It samples post-2023 S-code Form 4 filings from the local aggregate registry
and available raw source roots, inventories raw fields containing 10b5, plan,
adoption, transaction, and checkbox terms, checks footnote / explanation text,
and distinguishes explicit false from missing plan flags. It writes
`sample_manifest.csv`, `raw_field_inventory.csv`,
`footnote_plan_flag_candidates.csv`, source coverage JSON, summary JSON, and a
report under `outputs/factor_discovery/insider_disclosure/d2_plan_flag_audit/`.
This target is parser/source audit only and cannot write formulas,
MeasurementSpec files, expected-return panels, Q1/Q2 handoffs, optimizer
inputs, Alpha Registry updates, or downstream execution artifacts.

Run the D2-INSIDER-02B final source/locator/parser repair gate:

```bash
make factor-discovery-insider-plan-flag-repair
```

It audits accession-number to raw Form 4 XML resolution, reports missing raw
paths, parses structured 10b5-1 checkbox state as explicit true / explicit
false / missing, writes parser before/after classification, and stops before
rerunning sell contrast unless all source and plan-flag thresholds pass. If
`raw_file_found_share < 0.80`, `planned_sell_event_count < 300`,
`planned_sell_month_count < 24`, or `known_plan_flag_share < 0.60`, the target
keeps D2-INSIDER-02 blocked and recommends
`switch_to_D2_8K_01_subtype_underreaction`. Missing plan flags remain
unknown/no-view, never discretionary false.

Run the D2-8K-01 subtype underreaction observability fixture:

```bash
make factor-discovery-8k-d2-observability
```

It prioritizes auditor change, CFO departure, CEO departure, material agreement
termination, and restatement/amendment-related 8-K rows. The default run writes
a deterministic fixture plus `eightk_event_registry.csv`,
`eightk_subtype_counts.csv`, timestamp and coverage/no-view audits,
`car_window_panel.csv`, matched controls, `placebo_report.csv`, a D2 summary,
and a report under `outputs/factor_discovery/8k_subtype/d2/`. This is
no-formula observability only and not real EDGAR evidence. It cannot write
formula scores, MeasurementSpec files, expected-return panels, Q1/Q2 handoffs,
optimizer inputs, Alpha Registry updates, paper workflows, broker/order/live
paths, or production approval.

Run the D2-8K-01R real local EDGAR 8-K source-admission replay:

```bash
make factor-discovery-8k-d2-real-observability
```

It reads a local SEC filing archive (`request_specs.json` plus cached 8-K /
8-K/A documents) when available, audits raw-document locator coverage, accepted
timestamp coverage, document types, item-header parsing, and issuer/ticker /
market join coverage, then replays the no-formula D2 subtype CAR/control/
placebo protocol. The default runner checks the in-repo cache first and then
the repo-external archive under the workspace parent. It prefers same-accession
primary 8-K documents over exhibit-only links when both are cached, then filters
and combines available local price panels into a replay-specific market panel.
A D3 charter is allowed for at most one subtype, and only when source,
timestamp, market coverage, and placebo gates pass. It writes no formula,
MeasurementSpec, expected-return panel, Q1/Q2 handoff, optimizer input, Alpha
Registry update, paper workflow, broker/order/live path, or production
approval.

Run the bounded WRDS/CRSP market rescue for D2-8K-01R priority events:

```bash
WRDS_USERNAME=<wrds_username> make factor-discovery-8k-wrds-market-rescue
```

This target writes only a local CRSP daily price cache and manifest for priority
8-K events that are within the available WRDS max date. It does not write
formula, MeasurementSpec, expected-return panel, Q1/Q2 handoff, optimizer input,
Alpha Registry update, paper workflow, broker/order/live path, or production
approval.

Run the D3-INSIDER-02 open-market insider buying SignalBuilder:

```bash
make factor-discovery-insider-d3-signal-builder
```

It reads the frozen
`open_market_insider_buying_post_2023_v0` MeasurementSpec and the real D2
aggregate event registry, then writes signal, component, coverage/abstain,
lineage, timestamp, normalization, no-view, hard-falsifier, summary, and report
artifacts under
`outputs/factor_discovery/insider_disclosure/d3_open_market_buying_v0/`.
This target does not run Q1/Q2, does not write expected returns, does not call
optimizer or portfolio paths, and does not update Alpha Registry, paper,
broker/order/live, or production approval paths.

Run the Q1-INSIDER-01 evidence review:

```bash
make factor-discovery-insider-q1-evidence
```

It reads the D3 `signal_panel.csv` and local insider replay market cache,
clusters active P-code buy signals by issuer/tradable date, computes forward
return labels from the tradable signal date, and writes Q1 event-treatment,
ranking, placebo, pre-filing dominance, concentration, and liquidity/cost
artifacts under
`outputs/factor_discovery/insider_disclosure/q1_open_market_buying_v0/`.
The target is evidence review only. It writes no expected-return panel, does
not run Q2, optimizer, portfolio construction, Alpha Registry, paper,
broker/order/live, or production approval paths. If the local price cache does
not support enough forward labels, the correct result is a blocked or hold
decision rather than a fabricated Q1 pass.

Run the Q1-INSIDER-01A label coverage rescue:

```bash
make factor-discovery-insider-q1-label-rescue
```

This target merges local label-only price panels and reruns the same frozen Q1
evidence path under
`outputs/factor_discovery/insider_disclosure/q1_label_coverage_rescue/`. It
does not modify D3 signals, MeasurementSpec files, formulas, role weights,
cluster rules, holding windows, expected-return panels, Q2, optimizers,
portfolio construction, Alpha Registry, paper, broker/order/live, or production
paths. To explicitly use WRDS for label coverage only after credentials are
configured outside the repo:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_insider_disclosure_q1_label_rescue.py --allow-wrds --wrds-username <wrds_username>
```

The current WRDS `crsp.dsf_v2` rescue lifted observed primary labels to 429 of
654 active clusters, but the source ends at 2025-12-31 and remains below the
pre-registered label coverage threshold.

Run the Track A forensic workflow fixture:

```bash
make track-a-forensic-workflow-fixture
```

This writes the local `sector_neutral_residual_momentum` Track A fixture under
`outputs/track_a/sector_neutral_residual_momentum/fixture/`. It verifies
CandidateCharter / MeasurementSpec separation, PIT/as-of joins, explicit
coverage/abstain/tradability panels, evidence/placebo records, deterministic
manifest hashes, and report/boundary guards. It is a research harness only and
does not run Q2, optimizers, portfolio construction, Alpha Registry, paper,
broker/order/live, or production workflows.

The daily target reads the locally pulled WRDS daily PIT bundle under
`data/cache/wrds_multifactor/nasdaq100_daily_full10/` and writes
`outputs/factor_discovery/real_data_daily/`. The cache is ignored by git and
must not be committed.

The FD-R3 replay target reads the same local daily PIT bundle and writes
`outputs/factor_discovery/real_data_daily/fd_r3/`. It expands the 29 price-volume
FactorSpecs into a monthly signal panel, coverage report, and timestamp audit
with explicit abstain rows. The current FactorSpec v2 replay also records
`formula_version=price_volume_29_mechanism_v2`, formula hashes, mechanism
families, raw values, oriented scores, cross-sectional ranks, fallback audit
fields, and research evidence quality. It does not allocate weights, claim
alpha success, approve production, enter Q2, or update Alpha Registry.

The FD-R4 target reads the FD-R3 panel and writes
`outputs/factor_discovery/real_data_daily/fd_r4/`. It uses prior-visible
history only to estimate rolling ICIR weights, then reports 1m/3m QQQ-relative
OOS scores and decile spreads. It forbids full-sample ICIR, future universe
membership, future normalization, and post-period factor selection. It does not
run an allocator, claim alpha success, approve production, or enter Q2.

The FD-R5 target reads the FD-R4 OOS score panel and writes
`outputs/factor_discovery/real_data_daily/fd_r5/`. It runs shuffled,
lagged-signal, random same-coverage, sector-neutral, rebalance-shifted, and
future-return leakage-control diagnostics. The current real-data result is a
failed placebo gate with `recommended_next_action=stop_before_allocator`, so do
not proceed to allocator, Q1/Q2, or Alpha Registry import from this FD run.

The FD-R5.1 target reads the existing FD-R3/R4/R5 artifacts and writes
`outputs/factor_discovery/real_data_daily/fd_r5_1/`. It diagnoses standalone
factor OOS, family composites, rolling-weight failure attribution, redundancy
clusters, and candidate revision recommendations. It does not rewrite
FactorSpecs or rerun FD-R3/R4/R5; it only records why this candidate set should
stop before allocator.

The formula mechanism audit target reads the FD-R3 panel and writes
`outputs/factor_discovery/research_mode/formula_mechanism_audit.csv`,
`duplicate_cluster_audit.csv`, `rank_identity_audit.csv`, and
`reports/factor_formula_mechanism_v2_audit.md`. It is the FD-R5.1 mechanism
separation audit for FactorSpec v2. Exact value duplicates, rank duplicates,
and pure sign-flip duplicates are hard failures; high correlation alone is
review-only. This target does not run allocator, Q1, Q2, Alpha Registry,
broker/order, paper/live trading, or production approval paths.

The weighting reliability target reads the FD-R3 panel, FD-R4 rolling ICIR
weights and OOS score panel, and FD-R5 placebo report. It writes
`outputs/factor_discovery/research_mode/weighting_estimator_comparison.csv`,
`weight_stability_diagnostics.csv`, `weighting_placebo_comparison.csv`,
`weighting_failure_diagnosis.json`, and
`reports/factor_discovery_weighting_reliability_report.md`. It compares current
rolling ICIR against equal-weight, family-equal, shrunk ICIR, signed shrunk
ICIR, and rolling ridge estimators to separate weak signal from noisy weighting.
It does not run allocator, Q1, Q2, Alpha Registry, broker/order, paper/live
trading, or production approval paths.

The momentum low-vol candidate target reads the local WRDS daily PIT bundle and
writes `outputs/factor_discovery/research_mode/momentum_low_vol_candidate/`.
It evaluates the fixed user-supplied 12-1 momentum minus 3m volatility formula,
then writes live 1m/3m Rank IC, top-bottom spread, and placebo controls for
raw, industry-neutral, and 63d dollar-volume capacity-filtered variants. The
capacity filter excludes the bottom 30% capacity-rank names from that variant
instead of encoding filtered names as zero alpha. This is a standalone FD
diagnostic only; it does not add the candidate to the 29 FactorSpecs, run
allocator, enter Q1/Q2, update Alpha Registry, or approve paper/live trading.
The target writes `candidate_design_manifest.json` before validation; the
manifest records the market pain point, mechanism, pre-formula diagnostics,
placebo design, and cost/capacity risks.

The revision-confirmed alpha target runs FD-S6 inside the Factor Discovery
Sandbox:

```bash
make factor-discovery-revision-confirmed-alpha
```

It reads the local expanded SUE event panel, PIT IBES estimate snapshots, the
WRDS Nasdaq100 daily price-volume bundle, and historical membership
classifications. It writes the fixed FD-S6 artifact set under
`outputs/factor_discovery/research_mode/revision_confirmed_earnings_underreaction/`.
It writes `candidate_design_manifest.json` before scoring so the revision
formula remains tied to an explicit pain-point and mechanism thesis.
The gate is preregistered: positive 20d and to-next-announcement test Rank IC,
meaningfully positive t-stat, positive top-bottom spread after estimated costs,
shifted-event and random same-coverage placebo survival, capacity survival,
adjacent-horizon survival, non-fragility across variants, PIT audit pass, and
coverage diagnostics must all pass before the result can be labeled
`promotable_to_Q1_candidate_review`. Otherwise the target writes a conservative
blocking label such as `insufficient_support`, `placebo_blocked`,
`capacity_blocked`, or `timestamp_blocked`. It does not write Q1/Q2 inputs,
typed projections, Alpha Registry state, broker/order/live artifacts, or
production approval.

The small-cap WRDS pull target writes a local ignored CRSP daily bundle under
`data/cache/wrds_multifactor/small_cap_us_daily/` plus
`outputs/factor_discovery/small_cap/wrds_small_cap_pull_summary.json`.
Configure WRDS credentials outside the repo, for example with `.pgpass` and
`WRDS_USERNAME`. The target pulls common-share / major-exchange CRSP daily
prices, PIT market cap, shares outstanding, spread proxy fields, IWM benchmark
returns, historical name intervals, and delisting rows. It is a data pull only,
not alpha evidence, Q1/Q2 entry, Alpha Registry update, or production approval.

The small-cap quality pull target writes a local ignored CCM/Compustat PIT
quality bundle under `data/cache/wrds_multifactor/small_cap_quality/` plus
`outputs/factor_discovery/small_cap/wrds_small_cap_quality_pull_summary.json`.
It reads the CRSP small-cap research manifest, pulls `crsp_a_ccm.ccmxpf_lnkhist`
and `comp.fundq`, builds `quality_score_panel.csv`, writes
`quality_manifest.yaml`, and attaches the quality section to the local ignored
research manifest. Credentials must remain outside the repo. This is a
quality-control data pull only, not alpha evidence or an approval path.

The small-cap data admission target writes
`outputs/factor_discovery/small_cap/data_admission_report.json`,
`data_quality_summary.csv`, and
`reports/factor_discovery_small_cap_data_admission.md`. It is a data gate for a
new candidate family line, not a continuation of `price_volume_29`. Missing
delisting handling blocks the family. Missing PIT market cap, shares/float, or
exchange/share-class filters also blocks the family run. With the local WRDS
small-cap bundle, the current data admission passes.

The small-cap quality residual momentum target writes artifacts under
`outputs/factor_discovery/small_cap/family_candidates/quality_residual_momentum/`
plus `reports/factor_discovery_small_cap_quality_residual_momentum.md`. It uses
fixed single-signal scoring for
`small_cap_quality_residual_momentum_6m_ex1m`; rolling ICIR, ridge, shrunk ICIR,
learned weighting, allocator, Q1, Q2, Alpha Registry, broker/order, paper/live
trading, and production approval paths are all blocked.
It writes `candidate_design_manifest.json` before validation and also embeds
the same design contract in `family_manifest.json`.
If the bundle lacks PIT Compustat quality scores, the family may run only as a
clearly degraded no-quality variant and must be downgraded to
`calibration_only`, not Phase 64 candidate status.
With PIT quality scores available, the current run remains blocked because the
realized-return placebo gate fails and the leg-level cost/capacity pre-gate
reports negative cost-adjusted spread.
The runner also writes `monthly_signal_panel_cache.csv`,
`forward_target_panel_cache.csv`, and `panel_cache_manifest.json`; unchanged
reruns should report `signal_panel_cache_status=hit` and
`target_panel_cache_status=hit`. `placebo_dominance_diagnosis.csv` explains the
lagged-signal and value-weight controls before any candidate rewrite.

The small-cap dominance diagnosis target reads the cached monthly signal and
forward-target panels and writes FD-S4.1 lag/capacity diagnostics under the
same family output directory plus
`reports/factor_discovery_small_cap_dominance_diagnosis.md`. It compares live,
lagged, smoothed, stale, and update-component signals; monthly/quarterly
holding-period variants; market-cap, ADV, spread, and price buckets; fixed
equal/value/ADV/capacity weighting schemes; and leg-level cost drag. It is a
diagnostic-only gate: allocator, Q1, Q2, Alpha Registry, broker/order,
paper/live trading, and production approval paths remain blocked.

The small-cap S4.2 target writes a dedicated 1m/3m/6m target cache under
`outputs/factor_discovery/small_cap/target_cache/`, preregisters the fixed
high-capacity filter under
`outputs/factor_discovery/small_cap/family_candidates/quality_residual_momentum_s4_2/`,
and evaluates only `live_signal`, `lag_1m_signal`,
`rolling_3m_mean_signal`, and `rolling_3m_median_signal` with fixed
capacity-aware weighting schemes. It is a diagnostic rerun only. It does not
rewrite the factor formula, use learned weighting, promote a candidate, enter
Q1/Q2, update Alpha Registry, or open broker/order/live workflows.

The small-cap S4.3 target closes the slow/lagged branch and preregisters the
capacity-filtered live-signal observation from S4.2 before confirmation. It
evaluates only `live_signal / 3m / quarterly` with
`adv_weight_within_bucket` and `capacity_capped_equal_weight`, then writes
confirmation, placebo, cost-survival, exposure, shortability-boundary, and
decision artifacts under
`outputs/factor_discovery/small_cap/family_candidates/capacity_filtered_live_s4_3/`
plus `reports/factor_discovery_small_cap_capacity_filtered_live_s4_3.md`.
The current run returns `reject_capacity_filter_hypothesis` because matched
placebos beat the primary confirmation row.

Run the D2-SMALL-EMOTION-01 small-cap shock-conditioned observability pass:

```bash
make factor-discovery-small-emotion-d2
```

It reads the local PIT small-cap daily price-volume panel, IWM benchmark panel,
and delisting file, then builds no-formula D2 subsets for
`panic_overreaction_candidate`, `fomo_continuation_candidate`, and
`liquidity_vacuum_reversal_candidate`. It writes event counts, stale-price
guards, ADV/capacity guards, cost/spread pre-gate, delisting audit, CAR
windows, continuation/reversal diagnostics, matched controls, placebo report,
no-view coverage report, summary JSON, and markdown report under
`outputs/factor_discovery/small_emotion/d2_observability/`. The default target
uses a controlled row cap for local smoke replay; pass `--max-rows 0` to the
script for a full-file replay when the workstation has enough memory. This is
observability only: it writes no formula, MeasurementSpec, Q1/Q2 handoff,
expected-return panel, optimizer input, portfolio artifact, Alpha Registry
update, paper workflow, broker/order/live path, or production approval.

Run the D2-SMALL-EMOTION-01A chunked/full replay with subset-level guards:

```bash
make factor-discovery-small-emotion-full-replay
```

The full replay reads the local annual WRDS price chunks under
`data/cache/wrds_multifactor/small_cap_us_daily/raw/_chunks/adjusted_price_volume_panel/`,
runs the existing D2 pass per chunk, writes per-chunk artifacts under
`outputs/factor_discovery/small_emotion/d2_full_replay/chunks/`, and aggregates
`chunk_manifest.csv`, `subset_guard_aggregate.csv`,
`full_replay_decision.json`, and `full_replay_report.md`. Reruns are resumable:
completed chunk summaries are reused unless the runner is invoked with
`--refresh`. This is still no-formula observability only and does not write a
MeasurementSpec, Q1/Q2 handoff, expected-return panel, optimizer input,
portfolio artifact, Alpha Registry update, paper workflow, broker/order/live
path, or production approval.

Run the D2-SMALL-EMOTION-01B shock-direction remap audit over existing D2
artifacts:

```bash
make factor-discovery-small-emotion-direction-remap
```

The remap audit evaluates `up_shock_continuation`, `up_shock_reversal`,
`down_shock_reversal`, and `down_shock_continuation` as separate no-formula
mechanisms. It writes `shock_direction_remap_grid.csv`,
`shock_direction_placebo_audit.csv`, `shock_direction_remap_decision.json`, and
`shock_direction_remap_report.md` under
`outputs/factor_discovery/small_emotion/d2_direction_remap/`. It reads existing
D2 artifacts only and does not create a formula, MeasurementSpec, Q1/Q2 handoff,
expected-return panel, optimizer input, portfolio artifact, Alpha Registry
update, paper workflow, broker/order/live path, or production approval.

Run the E0-SMALL-EMOTION-02 exploratory parameter sweep:

```bash
make factor-discovery-small-emotion-exploratory-sweep
```

This stage intentionally allows in-sample parameter search over shock
thresholds, abnormal-volume thresholds, market-cap buckets, liquidity filters,
stale-price filters, ADV gates, direction hypotheses, and post-shock windows.
It writes `parameter_sweep_grid.csv`, `best_in_sample_candidates.csv`,
`overfit_risk_report.json`, `candidate_to_freeze_next.json`,
`exploratory_sweep_summary.json`, and `exploratory_sweep_report.md` under
`outputs/factor_discovery/small_emotion/e0_exploratory_sweep/`. The default
target uses a 750,000-row smoke cap; invoke the script with `--max-rows 0` for
a full single-file replay if the workstation has enough memory. This is
exploratory search only: it does not write a formula score, MeasurementSpec,
Q1/Q2 handoff, expected-return panel, optimizer input, portfolio artifact,
Alpha Registry update, paper workflow, broker/order/live path, or production
approval. Any candidate found here must be frozen into a later explicit D3
charter before validation.

Replay the selected E0 top pocket over chunked local WRDS price panels:

```bash
make factor-discovery-small-emotion-top-pocket-replay
```

The default focused pocket is `up_shock_reversal` with `shock_threshold=0.05`,
`volume_spike_threshold=1.5`, `market_cap_bucket=all_small_cap`,
`liquidity_filter=all`, `stale_filter=medium`, `adv_min_dollars=250000`, and
`window=post_1_22`. The replay writes `top_pocket_chunk_manifest.csv`,
`top_pocket_chunk_metrics.csv`, `top_pocket_replay_summary.json`,
`candidate_freeze_review.json`, and `top_pocket_replay_report.md` under
`outputs/factor_discovery/small_emotion/e0_top_pocket_replay/`.

If the focused replay remains stable enough for manual D3 review, freeze the
candidate charter:

```bash
make factor-discovery-small-emotion-d3-charter
```

This writes `d3_candidate_charter.yaml`, `d3_charter_manifest.json`,
`d3_charter_summary.json`, and `d3_charter_report.md` under
`outputs/factor_discovery/small_emotion/d3_up_shock_reversal_charter/`. This is
only a charter freeze. It does not write a MeasurementSpec, signal panel,
Q1/Q2 handoff, expected-return panel, optimizer input, portfolio artifact,
Alpha Registry update, paper workflow, broker/order/live path, or production
approval.

Run the aggressive E0-SMALL-EMOTION-04 sharpening sweep:

```bash
make factor-discovery-small-emotion-sharpening-sweep
```

This is intentionally overfit-oriented in-sample search. It writes
`sharpening_sweep_grid.csv`, `best_explosive_candidates.csv`,
`overfit_disclosure.json`, `candidate_to_freeze_next.json`, and report markdown
under `outputs/factor_discovery/small_emotion/e0_sharpening_sweep/`. It does
not write a MeasurementSpec, signal panel, expected-return panel, Q1/Q2 handoff,
optimizer input, portfolio artifact, Alpha Registry update, paper workflow,
broker/order/live path, or production approval.

Replay the sharpened top pocket over chunked local WRDS price panels:

```bash
make factor-discovery-small-emotion-sharpened-top-pocket-replay
```

The current default sharpened pocket is `up_shock_reversal` with
`shock_threshold=0.05`, `volume_spike_threshold=1.5`,
`prior_5d_min_return=0.20`, `market_cap_bucket=micro`,
`regime_filter=market_up_20d`, `adv_min_dollars=250000`, and
`window=post_1_22`. If the replay remains stable enough for manual freeze
review, write the D3 charter:

```bash
make factor-discovery-small-emotion-sharpened-d3-charter
```

The sharpened charter is a candidate freeze only. It writes no MeasurementSpec,
signal panel, expected-return panel, Q1/Q2 handoff, optimizer input, portfolio
artifact, Alpha Registry update, paper workflow, broker/order/live path, or
production approval.

Run the E0-SMALL-EMOTION-05 greedy leaf search:

```bash
make factor-discovery-small-emotion-leaf-search
```

This searches multiple shock directions and post-shock windows, then greedily
adds one filter at a time while preserving minimum event and event-month
requirements. It writes `leaf_search_tree.csv`, `best_leaf_candidates.csv`,
`leaf_overfit_disclosure.json`, `leaf_candidate_to_freeze_next.json`,
`leaf_search_summary.json`, and report markdown under
`outputs/factor_discovery/small_emotion/e0_leaf_search/`. This is aggressive
in-sample overfit discovery only and does not write a MeasurementSpec, signal
panel, expected-return panel, Q1/Q2 handoff, optimizer input, portfolio
artifact, Alpha Registry update, paper workflow, broker/order/live path, or
production approval.

Run the E1 full-market exploratory overfit lab:

```bash
make factor-discovery-small-emotion-full-market-overfit-lab
```

This widens the research universe to full-market common-stock coverage and
searches shock-conditioned leaves across direction, window, size, liquidity,
spread, market regime, price, prior return, and close-location predicates. It
writes `full_market_overfit_grid.csv`, `top_50_overfit_pockets.csv`,
`tail_concentration_audit.csv`, `cost_liquidity_audit.csv`,
`best_pocket_spec_draft.json`, summary JSON, and report markdown under
`outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab/`. Use
`scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py
--max-rows 0` for a full no-cap single-file replay. This is exploratory
overfit discovery only and does not write a MeasurementSpec, signal panel,
expected-return panel, Q1/Q2 handoff, optimizer input, portfolio artifact,
Alpha Registry update, paper workflow, broker/order/live path, or production
approval.

For full no-cap replay, prefer the cached path:

```bash
make factor-discovery-small-emotion-full-market-feature-cache
make factor-discovery-small-emotion-full-market-cached-replay
```

The feature-cache target shards the full price panel by asset and writes cached
event-label files under
`data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full/`.
The cached replay target reads that cache and writes the full-sample pocket
leaderboard under
`outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached/`.
This is still E1 exploratory overfit search only.

Run the cost-clean full cached replay:

```bash
make factor-discovery-small-emotion-full-market-cost-clean-cached-replay
```

This uses the same full feature cache but excludes `spread_wide`,
`price_under_5`, `weak_liquidity`, and `liquidity_low` from the greedy leaf
search. It writes artifacts under
`outputs/factor_discovery/small_emotion/e1_full_market_cost_clean_cached_replay/`.
The current top pocket is
`up_shock_reversal / post_1_22 / prior5_ge_20pct & open_to_close_le_minus_5pct`
with 238 events, 56 months, 202 issuers, 20.44% in-sample directional return,
and passing E1 cost/liquidity audit. This is still exploratory overfit search
only.

Run the stricter cost+stale-clean full cached replay:

```bash
make factor-discovery-small-emotion-full-market-cost-stale-clean-cached-replay
```

This keeps the same cost-clean predicate exclusions and additionally removes
stale-price rows (`stale_roll_5 >= 1`) and zero-volume rows from the candidate
event pool before leaf search. It writes artifacts under
`outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/`.
The current top pocket remains
`up_shock_reversal / post_1_22 / prior5_ge_20pct & open_to_close_le_minus_5pct`
with 225 events, 55 months, 193 issuers, and 20.61% in-sample directional
return. This is still exploratory overfit search only.

Run the locked validation for the full cached E1 top pocket:

```bash
make factor-discovery-small-emotion-freeze-validation
```

This runs SMALL-EMOTION-FREEZE-02 on
`up_shock_reversal / post_1_22 / spread_wide & shock_ge_20pct`. It does not
search for new pockets or modify thresholds after freezing. It reconciles the
pocket against the prior frozen small-cap MeasurementSpec hash
`eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976`; because
the full-market pocket differs, it writes a new D3 charter and D4
MeasurementSpec for this exact pocket before reporting locked validation. The
target writes temporal split metrics, sweep-adjusted placebo-selected pocket
audit, conservative cost/liquidity gate, capacity frontier, top-contributor
data-anomaly audit, decision summary, and markdown report under
`outputs/factor_discovery/small_emotion/freeze_02_full_market_locked_validation/`.

The current locked run is not promoted. It returns
`decision=stale_or_bad_print_failed`: temporal train/validation/test reads are
positive, but top contributors include three bad-print proxy events, the best
shifted-date placebo-selected pocket beats the selected live pocket, and
conservative spread/slippage stress makes post-cost directional return
negative. The phase writes no expected-return panel, Q2 handoff, optimizer
input, portfolio artifact, Alpha Registry update, paper workflow,
broker/order/live path, or production approval.

For the cost-clean top pocket, run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_freeze_validation.py \
  --top-pockets outputs/factor_discovery/small_emotion/e1_full_market_cost_clean_cached_replay/top_50_overfit_pockets.csv \
  --search-grid outputs/factor_discovery/small_emotion/e1_full_market_cost_clean_cached_replay/full_market_overfit_grid.csv \
  --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full \
  --output-dir outputs/factor_discovery/small_emotion/freeze_02_cost_clean_top_locked_validation_20260518 \
  --exclude-predicates spread_wide,price_under_5,weak_liquidity,liquidity_low
```

The current locked result is `selection_bias_failed`. Temporal split, anomaly,
cost, and capacity gates pass, but the best stale-price-matched
placebo-selected pocket beats the selected live pocket. No Q1/Q2, optimizer,
portfolio, Alpha Registry, paper/live/broker/order, or production path is
opened.

For the cost+stale-clean top pocket, run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_freeze_validation.py \
  --top-pockets outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/top_50_overfit_pockets.csv \
  --search-grid outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/full_market_overfit_grid.csv \
  --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full \
  --output-dir outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_top_locked_validation_20260518 \
  --exclude-predicates spread_wide,price_under_5,weak_liquidity,liquidity_low \
  --exclude-stale-price-events
```

The current locked result is still `selection_bias_failed`: the selected live
pocket passes anomaly, cost, capacity, and temporal split checks, but the
same-coverage-random placebo-selected pocket dominates on the current
mean-return sweep gate. No downstream Q1/Q2, optimizer, portfolio, Alpha
Registry, paper/live/broker/order, or production path is opened.

Run the pre-registered profile-gate rerun:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_freeze_validation.py \
  --top-pockets outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/top_50_overfit_pockets.csv \
  --search-grid outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/full_market_overfit_grid.csv \
  --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full \
  --output-dir outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_top_profile_gate_validation_20260518 \
  --exclude-predicates spread_wide,price_under_5,weak_liquidity,liquidity_low \
  --exclude-stale-price-events
```

The profile gate uses
`max(mean,0) * max(t_stat,0) * max(hit_rate - 0.5,0) * sqrt(event_month_count)`.
The current rerun returns `decision=promote_to_q2_candidate` with
`live_profile_score=2.909918` and best placebo profile score `1.254377`, while
leaving `q1_entry_allowed=false`, `q2_entry_allowed=false`,
`optimizer_entry_allowed=false`, `portfolio_construction_allowed=false`,
`alpha_registry_update_allowed=false`, and all paper/live/broker/order/
production flags false.

Freeze the sharpened small-cap emotion MeasurementSpec and run Q1
falsifier/OOS:

```bash
make factor-discovery-small-emotion-measurement-spec
make factor-discovery-small-emotion-q1-oos
make factor-discovery-small-emotion-promotion-gate
```

The MeasurementSpec target writes
`outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec/`. The
Q1 target writes
`outputs/factor_discovery/small_emotion/q1_sharpened_up_shock_reversal_oos/`
and runs a bounded 750k-row smoke by default. For an explicit full single-file
Q1 replay, run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_q1_oos.py --max-rows 0
```

Run the broader cost+stale-clean profile-rank-2 pocket Q1/OOS replay:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_q1_oos.py \
  --measurement-spec outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank2_validation_20260518/measurement_spec.yaml \
  --output-dir outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518 \
  --max-rows 0 \
  --minimum-event-count 200 \
  --minimum-event-month-count 24 \
  --minimum-oos-event-count 50 \
  --max-falsifier-events 5000 \
  --exclude-stale-price-events
```

The current full no-cap replay returns
`q1_decision=passed_q1_research_review`, with 336 active events, 313 observed
primary labels across 62 event months, 19.12% mean primary directional return,
26.01% test mean directional return, zero falsifier dominance, and zero policy
breaches. This only opens a research-stage Promotion Gate possibility; Q2,
optimizer, portfolio construction, Alpha Registry, paper/live/broker/order, and
production paths remain closed unless explicitly reopened.

Run the matching rank-2 Promotion Gate from that existing full Q1 output:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_promotion_gate.py \
  --measurement-spec outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank2_validation_20260518/measurement_spec.yaml \
  --q1-output-dir outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518 \
  --output-dir outputs/factor_discovery/small_emotion/pg_profile_rank2_broad_post_1_22_20260519 \
  --search-grid outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/full_market_overfit_grid.csv \
  --required-measurement-spec-hash 21cbf8277ed0778a8b0aa1ef473d65bcfa14c202830f4d7d391923dcdd0fd9b9 \
  --skip-q1-replay
```

The current gate returns `promotion_decision=promote_to_q2_candidate`,
`stop_reason=promotion_gate_passed_no_q2_run`, and keeps
`q2_entry_allowed=false`. Tail concentration, anomaly, cost/liquidity, and time
breadth pass; search burden is a warning because the source grid has 13,440
searched rows.

Open Q2 candidate intake for the three promoted cost+stale-clean pockets after
their full Q1 and PG artifacts exist:

```bash
make factor-discovery-small-emotion-q2-intake
```

This writes `outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519/`
with `small_emotion_q2_candidate_matrix.csv`,
`small_emotion_q2_expected_return_panel.csv`,
`small_emotion_q2_execution_risk_gate.csv`,
`small_emotion_q2_input_contract.json`, and a markdown report. It opens Q2
candidate intake only; it does not run optimizer, portfolio construction,
Alpha Registry, paper/live/broker/order, or production workflows.

Run Q2 execution-survival diagnostics for the opened candidate intake:

```bash
make factor-discovery-small-emotion-q2-survival
```

This writes
`outputs/factor_discovery/small_emotion/q2_execution_survival_20260519/` with
`small_emotion_q2_execution_survival_matrix.csv`,
`small_emotion_q2_cost_capacity_report.csv`,
`small_emotion_q2_holding_path.csv`,
`small_emotion_q2_optimizer_input_probe.csv`, summary JSON, and a markdown
report. It checks participation, cost pressure, holding path, and optimizer
input staging only. The target does not run an optimizer, build a portfolio,
update Alpha Registry, or open paper/live/broker/order/production workflows.

Run the Q2 optimizer adapter dry-run after execution-survival passes:

```bash
make factor-discovery-small-emotion-q2-optimizer-dry-run
```

This writes
`outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520/` with
`small_emotion_q2_optimizer_response_matrix.csv`,
`small_emotion_q2_optimizer_constraint_response.csv`,
`small_emotion_q2_optimizer_input_snapshot.csv`, summary JSON, manifest, and a
markdown report. It connects the staged probe panel to the local PortfolioOS
optimizer input shape and observes constraint response for live, sign-flipped,
and zero-alpha panels. It writes no orders, does not build a portfolio
construction artifact, and does not open Alpha Registry, paper/live,
broker/order, or production workflows.

Run the Q2 execution-survival closeout after the optimizer adapter dry-run:

```bash
make factor-discovery-small-emotion-q2-complete
```

This writes
`outputs/factor_discovery/small_emotion/q2_complete_20260520/` with
`small_emotion_q2_complete_matrix.csv`, summary JSON, manifest, and a markdown
report. It closes the local Q2 execution-survival chain by verifying candidate
intake, cost/capacity survival, optimizer response, sign-flip response, and
constraint status. It writes no orders, does not build a portfolio construction
artifact, and does not open Alpha Registry, paper/live, broker/order, or
production workflows.

Run the Q2 portfolio quant replay after Q2 closeout:

```bash
make factor-discovery-small-emotion-q2-portfolio-replay
```

This writes
`outputs/factor_discovery/small_emotion/q2_portfolio_replay_20260520/` with an
event replay panel, monthly returns, NAV/drawdown curve, cost attribution,
policy gate, summary JSON, manifest, and markdown report. It uses completed Q2
candidates plus their observed Q1 primary-window event returns and Q2
cost/capacity fields. The current replay completes rank1/rank2 and blocks rank3
on the spread/cost-liquidity policy gate. It writes no orders, does not build
production portfolio construction artifacts, and does not open Alpha Registry,
paper/live, broker/order, or production workflows.

Run the Q2 factor exposure / beta residual audit after Q2 closeout:

```bash
make factor-discovery-small-emotion-q2-factor-exposure-audit
```

This writes
`outputs/factor_discovery/small_emotion/q2_factor_exposure_audit_20260520/`
with event-level factor exposures, trailing beta / volatility estimates,
factor loadings, beta-residual matrix, policy gate, summary JSON, manifest, and
markdown report. It checks benchmark, beta, size, liquidity, volatility,
reversal, shock, and attention proxy controls only. It writes no orders, does
not build production portfolio construction artifacts, and does not open Alpha
Registry, paper/live, broker/order, or production workflows.

Run the Q2 robustness profile audit after Q2 closeout:

```bash
make factor-discovery-small-emotion-q2-robustness-audit
```

This writes `outputs/factor_discovery/small_emotion/q2_robustness_audit_20260520/`
with robustness, horizon-decay, concentration, overlap, bootstrap, summary,
manifest, and markdown report artifacts. It ignores drawdown as a hard gate for
this audit by explicit request, but still keeps orders, production portfolio
construction artifacts, Alpha Registry, paper/live, and broker/order workflows
closed.

The Promotion Gate target writes
`outputs/factor_discovery/small_emotion/pg_sharpened_up_shock_reversal/`. It
requires the frozen D4 MeasurementSpec hash and runs full no-cap Q1 replay
before the PG audits. To run the same path manually:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_promotion_gate.py --q1-max-rows 0
```

The current full replay rejects the pocket as
`reject_overfit_or_data_artifact` because three placebos dominate the live
full-sample read, even though time breadth and cost/liquidity audits now pass.
The gate does not run Q2, optimizer, portfolio construction, Alpha Registry,
paper, broker/order/live, or production workflows.

Q1 is research evidence only. It does not write an expected-return panel, enter
Q2, call an optimizer, build a portfolio, update Alpha Registry state, or open
paper/live/broker/order/production workflows.

Run the standalone Multi-Factor Alpha Validation Engine:

```bash
make factor-validate
```

Run the MF-R13 strict residual evidence closeout and fixed failure diagnosis
report after the MF-R12 waterfall artifacts exist under
`outputs/multifactor_alpha_validation/risk_model/`:

```bash
make multifactor-strict-residual-closeout
make multifactor-failure-diagnosis-report
make multifactor-portfolio-component-gate
make multifactor-candidate-filter-audit
make multifactor-component-oos-observations
make multifactor-portfolio-validation
make multifactor-portfolio-assembly-audit
make multifactor-component-oos-availability
make multifactor-portfolio-contribution
```

The candidate filter audit writes the R15 input pool at
`outputs/multifactor_alpha_validation/risk_model/soft_resurrected_component_pool.csv`.
It keeps hard PIT/timestamp/lookahead/survivorship failures blocked, but restores
soft standalone-evidence failures as documented portfolio components.

The portfolio validation target writes MF-R15 artifacts under
`outputs/multifactor_alpha_validation/portfolio_validation/`. It runs diagnostic
ensembles and placebo baselines only. It does not run an OR optimizer, build
security-level target weights, enter Q2, or approve production use.

The component OOS observation target writes expanded component observations
under `outputs/multifactor_alpha_validation/component_oos_observations/`. It
uses the local WRDS daily price-volume bundle plus lagged Compustat
fundamentals to add `liquidity_turnover`, `value_bm`,
`profitability_quality`, `investment_asset_growth`, and `accruals` observations
when those local manifests exist. It keeps `sue_event_reference` unavailable
until a PIT event visibility timestamp path exists. Raw WRDS data stays in
ignored local cache and must not be committed.

The portfolio assembly audit target writes MF-R15.5 artifacts under the same
portfolio validation directory. It separates low-coverage observed-subset
failure from current observed component-pool diagnostics, checks component
availability, direction conventions, role-aware ensemble mixes, gross-to-net
cost drag, and benchmark exposure conflict. After the observation expansion the
current audit reclassifies the R15 state to `component_pool_fails_gross`, with
`sue_event_reference` still unavailable. It remains diagnostic only and does
not open OR optimization, security-level construction, Q2, or production use.

The component OOS availability target writes MF-R15.6 artifacts under
`outputs/multifactor_alpha_validation/portfolio_validation/`. It classifies why
each resurrected component is observed or unavailable, verifies that fundamental
components retain reporting lag, flags event components that still need
visibility timestamps, and enforces the 60% minimum observation-coverage
threshold before any full-pool decision is allowed. It writes no fabricated
returns and does not open OR optimization, security-level construction, Q2, or
production use.

The portfolio contribution target writes MF-R16 diagnostic ablation artifacts
under `outputs/multifactor_alpha_validation/portfolio_contribution/`. It runs
leave-one-factor-out, drop-cluster, role contribution, and QQQ up/down regime
diagnostics on the observed component pool. It is post-portfolio attribution
only; it does not run an OR optimizer, generate security-level target weights,
enter Q2, or approve production use.

Run bounded multifactor cost/capacity attribution diagnostics:

```bash
make multifactor-portfolio-cost-capacity
```

The cost/capacity target writes MF-R17 component cost-drag, cost-stress, and
component-proxy capacity-frontier artifacts under
`outputs/multifactor_alpha_validation/portfolio_cost_capacity/`. If
security-level ADV is unavailable, capacity is explicitly proxy-only. It does
not fabricate capacity, run OR optimization, generate security-level target
weights, enter Q2, or approve production use.

Run the full-market multifactor E0 overfit/discovery sweep:

```bash
make multifactor-full-market-sweep
```

The sweep target writes feature-cache, leaf-pocket grid, template grid, placebo
top-pocket, summary, and report artifacts under
`outputs/multifactor_alpha_validation/full_market_sweep/`. It is a discovery
lab only: it records search burden and top diagnostic pockets, but it does not
write a D3 charter, MeasurementSpec, Q1/Q2 input, OR optimizer output, Alpha
Registry update, paper/live workflow, broker/order path, or production approval.

Run the full-market multifactor E0 supervisor retry loop:

```bash
make multifactor-full-market-supervisor
```

The supervisor target writes sweep, locked validation, frozen candidate, attempt
log, summary, and markdown artifacts under
`outputs/multifactor_alpha_validation/full_market_supervisor/`. It freezes each
attempted candidate before locked diagnostics and retries the next candidate on
failure. The default retry budget is 100 candidates. It remains diagnostic-only and does not write a D3 charter,
MeasurementSpec, Q1/Q2 input, OR optimizer output, Alpha Registry update,
paper/live workflow, broker/order path, or production approval.

Run the full-market candidate full audit:

```bash
make multifactor-full-market-candidate-audit
```

The candidate full audit writes temporal breadth, tail concentration, data
anomaly, cost/capacity, benchmark residual, summary, and report artifacts under
`outputs/multifactor_alpha_validation/full_market_candidate_audit/`. It keeps
cost/capacity pending when executable cost inputs remain incomplete. The current
default reads `data/universe/us_universe_reference.csv` and
`data/universe/us_universe_market_2026-03-27.csv` for static ADV/market-cap
capacity proxies; real bid-ask spread is still marked unavailable rather than
fabricated. The runner does not write D3, MeasurementSpec, Q1/Q2, OR optimizer,
Alpha Registry, paper/live, broker/order, or production artifacts.

The diagnosis writes `factor_failure_diagnosis.csv`,
`qqq_relative_guard_review.json`, and `factor_failure_diagnosis_report.md`.
It records each factor's stop layer and reviews whether the QQQ-relative guard
is too strict for long-short factor spreads. The review is diagnostic only and
does not promote factors into redundancy, allocator, Q2, paper/live, broker, or
production workflows.

The component gate writes `component_candidate_table.csv`,
`portfolio_component_gate_summary.json`, and `portfolio_component_gate_report.md`.
It separates standalone alpha from documented portfolio components. Current
component eligibility allows only `diagnostic_ensemble_only` validation, not
unrestricted allocator entry or alpha approval.

Run typed alpha dashboard readability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_dashboard_readability.py -q
```

Run AlphaView contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_view_contract.py -q
```

Run event-aware alpha evaluation contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_event_alpha_evaluation_contract.py -q
```

Run Alpha Projection Bridge v2 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_projection_bridge_v2.py -q
```

Run paper overlay readiness tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_paper_overlay_readiness.py -q
```

Build paper overlay readiness artifacts from local observations only:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run python scripts/run_paper_overlay_calibration_batch.py --observations outputs/paper_calibration_aggregate/drift_observations.csv --output-dir /tmp/portfolioos_paper_overlay_readiness
```

Run the typed SUE alpha pilot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/typed_alpha_pilot/tests -q
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

Run Q2 typed execution matrix tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py -q
```

Run Q2 typed PortfolioOS adapter tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py -q
```

Run Q2 typed expected-return injection tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py -q
```

Run Q2 typed optimizer response acceptance tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_optimizer_response.py -q
```

Run SUE typed Q2 survival tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_typed_q2_survival.py -q
```

Run SUE execution-survival attribution tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_execution_survival_attribution.py -q
```

Run revision marginal-value gate tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py -q
```

Run the local-only typed Q2 adapter fixture:

```bash
make typed-q2-adapter-fixture
```

Run the local-only typed expected-return injection fixture:

```bash
make typed-expected-return-injection-fixture
```

Run the local-only typed optimizer response acceptance fixture:

```bash
make typed-optimizer-response-acceptance
```

Run the local-only SUE typed Q2 survival fixture:

```bash
make sue-typed-q2-survival
```

Build the local SUE execution-survival attribution report:

```bash
make sue-survival-attribution
```

Run the explicit local SUE optimizer input bridge fixture:

```bash
make sue-optimizer-input-bridge-fixture
```

Build the WRDS PIT-labeled historical SUE event panel smoke artifacts:

```bash
make sue-historical-event-panel-smoke
```

Run the local WRDS full-mode SUE panel audit path:

```bash
make sue-historical-event-panel-full-audit
```

If the configured local IBES/CRSP extracts in
`configs/wrds_sue_event_panel_full.yaml` are missing, the command writes a
structured unavailable report instead of generating a fake panel or substituting
smoke data.

Expand the local CRSP daily price cache for the historical SUE panel:

```bash
PGUSER=<wrds_username> make sue-historical-crsp-price-extract
make sue-historical-event-panel-full-audit
```

The extractor is resumable through
`data/cache/wrds_sue_event_panel/crsp_daily_chunks/`; reruns skip completed
chunks and rewrite the merged ignored `crsp_daily.csv` cache.

Rescue expanded SUE linkage failures with exact-CUSIP CRSP stocknames matches:

```bash
PGUSER=<wrds_username> make sue-historical-linkage-rescue
PGUSER=<wrds_username> make sue-historical-crsp-price-extract
```

The rescue writes `data/cache/wrds_sue_event_panel/ibes_links_rescued.csv` and
`outputs/sue_coverage_linkage_price_diagnostics/linkage_rescue_report.json`.
The follow-up CRSP extraction should use the rescued link cache. Ticker-only
matching is not used.

Build the expanded SUE historical panel and coverage rescue diagnostics:

```bash
PGUSER=<wrds_username> make sue-historical-event-panel-expanded
```

This writes expanded panel artifacts under
`outputs/sue_historical_event_panel_expanded/` and
`reports/sue_historical_event_panel_expansion_report.md`. The configured WRDS
refresh writes actual/estimate cache files under ignored
`data/cache/wrds_sue_event_panel_expanded/` and reuses the ignored CRSP daily
cache.

Run the historical SUE event evidence grid after H1A/H1C artifacts exist
locally:

```bash
make sue-historical-event-evidence-grid
```

This writes H1B/H1C evidence artifacts under
`outputs/sue_historical_event_evidence/` and
`reports/sue_historical_event_evidence_report.md`. It evaluates event-window
evidence only and does not run Q2, optimizer-path evaluation, broker/order
workflows, paper trading, or production approval.

Diagnose coverage, linkage, and price-window loss after the expanded SUE panel
and evidence grid exist locally:

```bash
make sue-coverage-linkage-price-diagnostics
```

This writes diagnostic artifacts under
`outputs/sue_coverage_linkage_price_diagnostics/` plus
`reports/sue_coverage_linkage_price_diagnostics_report.md`. It is a data
coverage diagnostic only and does not run Q2, optimizer-path evaluation, Alpha
Registry promotion, broker/order workflows, paper trading, or production
approval.

Run score-definition diagnostics after the expanded SUE panel and CRSP cache
exist locally:

```bash
make sue-score-definition-diagnostics
```

This writes H1D artifacts under `outputs/sue_score_definition_diagnostics/`
plus `reports/sue_score_definition_diagnostics_report.md`. It compares raw EPS
difference with scale-aware SUE score definitions and explains raw-tail spread
failures without running Q2, optimizer-path evaluation, paper workflows, or
production approval.

Run the pre-registered scale-aware SUE score-definition gate:

```bash
make sue-score-definition-gate
```

This writes H1E artifacts under `outputs/sue_score_definition_gate/` plus
`reports/sue_score_definition_gate_report.md`. It downgrades raw EPS difference
to diagnostic-only, evaluates scale-aware SUE definitions with denominator,
winsorization, placebo, and tail guards, and keeps downstream typed projection
and Q2 locked behind separate explicit reopen work.

Run the SUE event-date-shift placebo failure attribution diagnostic:

```bash
make sue-placebo-failure-attribution
```

This writes H1E.1 artifacts under `outputs/sue_placebo_failure_attribution/`
plus `reports/sue_placebo_failure_attribution_report.md`. It decomposes the
event-date-shift placebo advantage across timing shifts, return-window overlap,
market/regime concentration, denominator and tail buckets, and unavailable
sector/size/liquidity hooks. It diagnoses the H1E placebo failure only; it does
not select a score, run Q2, run optimizer-path evaluation, promote Alpha
Registry state, open paper/live/broker/order workflows, or approve production
use.

Run the SUE market-regime placebo filter check:

```bash
make sue-regime-filter-placebo-check
```

This writes H1E.2 artifacts under
`outputs/sue_regime_filter_placebo_check/` plus
`reports/sue_regime_filter_placebo_check_report.md`. It reruns filtered
score-gate summaries and placebo curves after excluding March 2020,
high-volatility weeks, and low-liquidity weeks. It validates the H1E.1
market-regime attribution only; it does not select a score, run Q2, run
optimizer-path evaluation, promote Alpha Registry state, open
paper/live/broker/order workflows, or approve production use.

Run the SUE event timing / anchor definition audit:

```bash
make sue-event-timing-anchor-audit
```

This writes H1E.3 artifacts under `outputs/sue_event_timing_anchor_audit/`
plus `reports/sue_event_timing_anchor_audit_report.md`. It compares current
tradable, announcement-date, and shifted anchors; audits pre-event drift
windows; records timing-quality buckets; and checks live/shifted window
overlap. It is an event-timing diagnostic only and does not select a score,
run Q2, run optimizer-path evaluation, promote Alpha Registry state, open
paper/live/broker/order workflows, or approve production use.

Current local result: `anchor_definition_likely_late`. The strongest primary
`[+2,+22]` anchor is `shift_minus_5_td`, and the strongest pre-event drift
window is `minus_5_minus_1`. Treat that as an anchor-policy diagnostic, not a
SUE selection or Q2 approval.

Run the SUE announcement timestamp policy audit:

```bash
make sue-announcement-timestamp-policy-audit
```

This writes H1E.4 artifacts under
`outputs/sue_announcement_timestamp_policy/` plus
`reports/sue_announcement_timestamp_policy_report.md`. It compares current
IBES/WRDS announcement timing against optional auditable source timestamps such
as actual-EPS source timestamps, report dates, Compustat RDQ, and announcement
time fields. It blocks blind -5/-10 anchor shifts unless actual EPS
availability is proven earlier.

Current local result: `no_auditable_earlier_timestamp_sue_blocked` with
`auditable_source_event_count=0` and `repaired_event_count=0`. Do not rerun H1E,
typed projection, or Q2 until a new extract provides an earlier auditable actual
EPS availability timestamp.

Run the SUE timestamp-source enrichment layer:

```bash
make sue-timestamp-source-extract
make sue-timestamp-enrichment
```

The source-extract target connects to WRDS using credentials configured outside
the repo and writes ignored local cache files under
`data/cache/wrds_sue_timestamp_sources/`. It extracts IBES actual announcement
dates/times and Compustat quarterly RDQ fields for the expanded SUE event panel.

This writes H1E.5 artifacts under `outputs/sue_timestamp_enrichment/` plus
`reports/sue_timestamp_enrichment_report.md`. Local source files can provide
IBES `anndats_act`, Compustat `rdq`, exact earnings-release timestamps, or SEC
filing timestamps. Date-only sources remain audit-only; exact release timestamps
may create repair candidates for later review, but H1E is not rerun by this
target.

Current local result after WRDS source extraction:
`timestamp_enrichment_no_repair_sue_blocked`, `repairable_event_count=0`,
`ibes_anndats_act_count=17027`, and `compustat_rdq_count=12987`.

Run the local-only revision marginal-value gate:

```bash
make revision-marginal-value-gate
```

Run Evidence Bundle tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/evidence_bundle/src poetry run pytest projects/evidence_bundle/tests -q
```

Run Promotion Gate tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
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

Run static dashboard tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_static_dashboard.py -q
```

Run one-command demo tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_one_command_demo.py -q
```

Run README packaging tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_readme_packaging.py -q
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
