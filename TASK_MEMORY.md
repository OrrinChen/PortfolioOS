# TASK_MEMORY

This file is the short handoff note for continuing PortfolioOS. It keeps only the current state, stable conclusions, and the next useful branch. Detailed artifacts remain in `docs/`, `outputs/`, and the external research workspaces.

## Current Snapshot

- PortfolioOS is a compliance-aware portfolio rebalance, scenario, approval, execution-simulation, backtest, TCA, and research CLI platform.
- Repository-level Codex workflow scaffold is now installed:
  - `AGENTS.md` defines autonomous-agent rules, hard constraints, validation expectations, and stopping rules.
  - `ROADMAP.md` defines the next incomplete phase and acceptance criteria.
  - `VALIDATION.md` records test, smoke, and diff-check commands.
  - `RUNBOOK.md` records common commands and troubleshooting.
- Current standalone project shells:
  - Q1 = `projects/agentic_alpha_triage`, asks "Is this alpha real?", and contains schemas/contracts plus validated example artifacts.
  - Q2 = `projects/execution_aware_optimizer`, asks "Can this alpha survive execution?", and contains a PortfolioOS-aware project shell with explicit unavailable-layer reporting.
- Research track boundaries are now explicit:
  - `configs/research_tracks.yaml` is the source of truth.
  - Track A = Single-Alpha Research Factory for SUE, revision-confirmed earnings, Factor Discovery candidate design, and other individual factor/event candidates.
  - Track B = Multi-Factor Portfolio Validation Engine for formal factor/component pools, risk attribution, ensemble OOS, ablation, and portfolio assembly diagnostics.
  - Shared governance = Q1/Q2, Evidence Bundle, Promotion Gate, Alpha Registry, audit, provenance, and validation.
  - `make research-track-boundaries` validates that Factor Discovery cannot enter Q2 directly, multi-factor diagnostics cannot be treated as single-alpha proof, and no track claims broker/live/order/production approval.
- PortfolioOS portfolio quant walk-forward v1 is now the active core-platform
  pivot after freezing recent alpha research lines:
  - `configs/research_freeze_before_portfolio_quant.yaml` records SUE as
    blocked, insider buy v0 as retired before Promotion Gate, insider sell
    contrast as stopped before D3, 8-K subtype work as hold-pending data
    coverage, and small-cap emotion/liquidity as D2-only hold-insufficient
    sample. The freeze is governance-only: it does not delete, revert, or
    commit prior research artifacts.
  - `make portfolio-quant-walk-forward` runs the PortfolioOS historical
    walk-forward smoke and writes portfolio quant artifacts under
    `outputs/portfolio_quant_walk_forward/`.
  - `src/portfolio_os/backtest/walk_forward.py` and
    `src/portfolio_os/cli/walk_forward.py` provide the dedicated historical
    weekly/monthly walk-forward layer. The walk-forward surface compares
    equal-weight, mean-variance, risk-parity, `cost_unaware_rebalance`, and
    `portfolio_os_cost_aware_rebalance`, validates that each rebalance uses
    only prior-window estimates, and exports NAV, drawdown, turnover
    distribution, CVaR, exposure drift, strategy comparison, policy-breach
    rows, no-lookahead report, and markdown report artifacts. It is portfolio
    construction / execution-aware evaluation only, not new alpha research,
    Q1/Q2 entry, Alpha Registry promotion, paper/live trading, broker/order
    workflow, or production approval.
- Track A Forensic Research Workflow v1 is implemented:
  - `factor_discovery_sandbox.track_a_forensic_workflow` separates
    `CandidateCharter`, frozen `MeasurementSpec`, and `SignalBuilder`.
  - The local fixture uses `sector_neutral_residual_momentum` to verify PIT
    as-of joins, explicit signal/label/coverage/abstain/tradability panels,
    hard guard blocking, evidence/placebo diagnostics, decision records,
    manifest hashes, and report language guards.
  - `make track-a-forensic-workflow-fixture` writes ignored artifacts under
    `outputs/track_a/sector_neutral_residual_momentum/fixture/`.
  - This is workflow discipline only, not alpha validation, not Q2 entry, not
    optimizer or portfolio construction, not Alpha Registry promotion, and not
    paper/live/broker/order/production approval.
- Track A D0 Alpha Pain Point Ledger is drafted at
  `projects/multifactor_alpha_validation/factor_discovery_sandbox/docs/alpha_pain_point_ledger.md`:
  - The ledger lists mechanism-first market pain points, including insider /
    EDGAR disclosure, 8-K and amendment complexity, event timing, residual
    underreaction, small-cap attention/capacity/stale-price risks,
    emotion-amplified event response, forced-flow / market-structure pressure,
    and 0DTE gamma-flow microstructure.
  - D0.1 triage preserves every candidate instead of deleting weakly prepared
    ideas. The statuses are `active_mainline`, `shadow_queue`,
    `hold_pending_data`, `guard_only`, and `archive_prior_work`.
  - The active mainline is now the coupled
    `insider_disclosure_regime_2023` group: 10b5-1 plan events,
    post-2023 open-market insider buys, discretionary-sell vs planned-sell
    contrast, and planned-sell information compression. Its next allowed work
    is a no-formula D2 observability protocol, not a MeasurementSpec or score.
  - `projects/multifactor_alpha_validation/factor_discovery_sandbox/docs/d2_insider_disclosure_observability_protocol.md`
    now drafts that no-formula D2 protocol. It defines Form 4 / 10b5-1 event
    extraction requirements, SEC accepted timestamp and tradable timestamp
    policy, event subsets, CAR windows, matched controls, placebos, sample
    gates, hard blockers, decision labels, and future artifact names. It
    explicitly forbids role weights, cluster weights, formulas, composite
    scores, Q1/Q2 handoffs, optimizer inputs, Alpha Registry updates,
    paper/live/broker/order workflows, and production approval.
  - The shadow queue preserves 8-K subtype / amendment complexity, index
    boundary pressure, and residual underreaction. The 0DTE gamma-flow idea is
    preserved as `hold_pending_data` until options-chain / expiry / intraday
    data readiness is defined.
  - It is not a formula backlog, alpha evidence, Q1/Q2 input, optimizer path,
    allocator path, Alpha Registry promotion, paper/live/broker/order workflow,
    or production approval.
- D2-INSIDER-01 no-formula observability runner is implemented:
  - `factor_discovery_sandbox.insider_disclosure_d2`,
    `scripts/run_insider_disclosure_d2_observability.py`, and
    `make factor-discovery-insider-d2-observability` write artifacts under
    `outputs/factor_discovery/insider_disclosure/d2/`.
  - Artifacts: `insider_event_registry.csv`, `event_subset_counts.csv`,
    `timestamp_audit.csv`, `tradability_audit.csv`, `car_window_panel.csv`,
    `matched_control_panel.csv`, `placebo_report.csv`,
    `d2_observability_summary.json`, and
    `d2_insider_disclosure_observability_report.md`.
  - The deterministic D2 fixture currently records `event_count=824`,
    `event_month_count=24`, `overall_decision=observable`, and
    `allow_d3_charter_for=["open_market_insider_buying_post_2023"]`.
    Subset decisions are `open_market_buy=observable`,
    `discretionary_sell=observable`, `planned_sell=compression_observable`,
    and `compensation_controls=control_clean`.
  - It is observability only: `formula_score_written=false`,
    `measurement_spec_written=false`, `q1_entry_allowed=false`,
    `q2_entry_allowed=false`, `alpha_registry_update_allowed=false`, and
    `production_approval_claimed=false`.
  - Validation: focused D2-INSIDER tests passed; `make
    factor-discovery-insider-d2-observability` passed. This does not prove an
    insider alpha, does not write a MeasurementSpec, and does not open Q1/Q2,
    optimizer, allocator, Alpha Registry, paper/live/broker/order, or
    production approval paths.
- D2-INSIDER-01R real local Form 4 extraction path is implemented:
  - `factor_discovery_sandbox.insider_disclosure_d2_real`,
    `scripts/run_insider_disclosure_d2_real_observability.py`, and
    `make factor-discovery-insider-d2-real-observability` read only local
    SEC-style ownership XML inputs from
    `data/cache/sec_form4_insider_disclosure/` and write artifacts under
    `outputs/factor_discovery/insider_disclosure/d2_real/`.
  - The parser now also supports repo-external SEC filing archives with
    `request_specs.json` plus nested `documents/{ticker}/{form_type}/...`
    rendered SEC Form 4 HTML files. It uses `acceptedDate` from
    `request_specs.json` as the SEC accepted timestamp, parses local rendered
    Table I non-derivative Form 4 rows, and keeps parse failures / missing
    market joins as explicit no-view or audit rows rather than fixture fallback.
  - Source-admission artifacts include `form4_source_manifest.json`,
    `form4_download_or_cache_audit.csv`, `form4_xml_parse_coverage.csv`,
    `issuer_mapping_audit.csv`, and `timestamp_source_audit.csv`.
  - Real replay artifacts include `insider_event_registry_real.csv`,
    `insider_event_market_join.csv`, `market_join_audit.csv`, and `_real`
    D2 replay files for subset counts, timestamp/tradability audit, CAR-window
    diagnostics, matched controls, placebo, summary, and report.
  - If the local archive is absent, the target writes
    `missing_inputs_report.json` and `real_data_status=unavailable_missing_source`
    without falling back to fixture events or fabricating historical evidence.
  - A local archive probe using copied files from the repo-external SEC filing
    archive parsed real rendered Form 4 files without network access. A
    50-file probe returned `event_count=68`, `parsed_file_count=49`, and
    `overall_decision=hold_insufficient_sample`; a P/S-focused 30-file probe
    returned `event_count=48` with observed `open_market_buy`,
    `discretionary_sell`, `compensation_control`, and explicit
    `unknown_no_view` subsets. These are source-admission/protocol smoke checks
    only, not alpha evidence.
  - It uses no network fetch and remains no-formula: `formula_score_written=false`,
    `measurement_spec_written=false`, `q1_entry_allowed=false`,
    `q2_entry_allowed=false`, `alpha_registry_update_allowed=false`, and
    `production_approval_claimed=false`.
  - Validation: focused D2-INSIDER-01R tests passed; `make
    factor-discovery-insider-d2-real-observability` passes in unavailable mode
    when the default in-repo source path is absent; use `--source-dir` to replay
    a repo-external SEC filing archive explicitly.
  - The real replay now supports controlled archive slices with
    `--start-offset` and `--max-files`, and date-sensitive market joins from
    local daily price/volume panels. Missing market joins and missing
    price/volume controls remain explicit no-view rows; no missing coverage is
    encoded as zero.
  - Repo-external SEC archive replay from
    `/Users/orynwilder/Documents/New project 2/sec_filing_archive/20260501T145601Z`
    found `source_index_total_count=63288` local Form 4 entries. Four controlled
    batches were run and aggregated under
    `outputs/factor_discovery/insider_disclosure/d2_real_archive_batched_aggregate/`.
    Aggregate readout:
    - `event_count=84442`
    - `event_month_count=51`
    - `overall_decision=observable`
    - `allow_d3_charter_for=["open_market_insider_buying_post_2023"]`
    - `open_market_buy event_count=1798`
    - `open_market_buy covered_count=1458`
    - `open_market_buy coverage_share=0.810901`
    - `open_market_buy covered_event_month_count=39`
    - `open_market_buy covered_cluster_count=820`
  - The first archive batch alone failed coverage, so the D3 decision is based
    on the aggregate replay, not a cherry-picked passing segment.
  - D3 is now opened only for
    `open_market_insider_buying_post_2023_v0`. The frozen MeasurementSpec lives
    at
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/factor_specs/insider_disclosure_2023/open_market_insider_buying_post_2023_v0.yaml`,
    with a boundary note at
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/docs/d3_insider_open_market_buying_measurement_spec_v0.md`.
  - This is still Track A measurement definition only: no formula score has
    been produced, no Q1/Q2 handoff is allowed, no optimizer/portfolio path is
    opened, and no Alpha Registry, paper/live/broker/order, or production
    approval path is opened.
  - D3-INSIDER-02 SignalBuilder is now implemented for the frozen
    `open_market_insider_buying_post_2023_v0` MeasurementSpec:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_d3_signal_builder.py`
    - runner: `scripts/run_insider_disclosure_d3_signal_builder.py`
    - smoke target: `make factor-discovery-insider-d3-signal-builder`
    - default output:
      `outputs/factor_discovery/insider_disclosure/d3_open_market_buying_v0/`
  - The D3 builder writes `signal_panel.csv`, `signal_component_panel.csv`,
    `coverage_abstain_panel.csv`, `event_to_signal_lineage.csv`,
    `signal_timestamp_audit.csv`, `signal_normalization_audit.csv`,
    `no_view_reason_report.csv`, `hard_falsifier_input_pack.json`,
    `d3_signal_builder_summary.json`, and
    `d3_open_market_buying_signal_builder_report.md`.
  - Current real D3 smoke on the aggregate event registry:
    - `event_count=84442`
    - `signal_row_count=84442`
    - `active_signal_count=1362`
    - `no_view_count=83080`
    - `transaction_code_scope=open_market_or_private_purchase`
    - `private_purchase_filter_status=unavailable_from_form4_code_only`
    - `expected_return_panel_written=false`
    - `q1_entry_allowed=false`
    - `q2_entry_allowed=false`
    - `alpha_registry_update_allowed=false`
    - `production_approval_claimed=false`
  - The active signal count is lower than the D2 covered count because D3 also
    blocks rows with missing holding baseline or unsupported role parsing. These
    rows remain no-view / abstain and are not encoded as zero.
  - D3 rejects event registries that contain forward-return, CAR, Q1, Q2,
    optimizer, portfolio, or expected-return columns. Form 4 code `P` is audited
    as "open-market or private purchase"; the builder does not claim every
    `P` transaction is exchange open-market.
  - Q1-INSIDER-01 Evidence Review is now implemented for the frozen D3
    `open_market_insider_buying_post_2023_v0` signal panel:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_q1_evidence.py`
    - runner: `scripts/run_insider_disclosure_q1_evidence.py`
    - smoke target: `make factor-discovery-insider-q1-evidence`
    - default output:
      `outputs/factor_discovery/insider_disclosure/q1_open_market_buying_v0/`
  - The Q1 runner builds issuer-event clusters as the primary analysis unit,
    keeps transaction-level rows diagnostic, creates forward return labels from
    local price/benchmark panels, separates event-treatment evidence from
    within-event score-ranking evidence, and writes shifted-date,
    same-coverage, role-label, issuer-non-event, compensation-control,
    pre-filing, concentration, and liquidity/cost pre-gate artifacts.
  - Current real Q1 smoke on the D3 signal panel and local market cache:
    - `q1_decision=hold_insufficient_sample`
    - `q1_result_interpretation=insufficient_q1_label_sample_or_price_coverage`
    - `active_event_clusters=654`
    - `observed_primary_label_clusters=140`
    - `observed_event_month_count=24`
    - `label_coverage_share=0.214067`
    - `primary_mean_abnormal_return=0.001553292744801939`
    - `rank_ic_mean=-0.024980025134823915`
    - `top_bottom_spread_mean=-0.0014979928952522242`
    - `promotion_gate_allowed=false`
    - `q2_entry_allowed=false`
    - `optimizer_entry_allowed=false`
    - `alpha_registry_update_allowed=false`
    - `production_approval_claimed=false`
  - Q1 does not reuse D2 aggregate CAR values as labels, does not encode
    no-view rows as zero, does not write an expected-return panel, and does not
    open Promotion Gate unless Q1 passes. The current blocker is forward-return
    label coverage / market-cache breadth, with additional negative diagnostics
    from shifted-date, compensation-control, pre-filing dominance, and
    score-ranking reads.
  - Q1-INSIDER-01A Label Coverage Rescue is now implemented as a narrow label
    coverage repair:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_q1_label_rescue.py`
    - runner: `scripts/run_insider_disclosure_q1_label_rescue.py`
    - smoke target: `make factor-discovery-insider-q1-label-rescue`
    - default output:
      `outputs/factor_discovery/insider_disclosure/q1_label_coverage_rescue/`
  - The rescue preserves the frozen D3 signal hash, MeasurementSpec, formula,
    role weights, cluster logic, and holding windows. It only merges
    label-only price panels and reruns the existing Q1 evidence code. It
    rejects price panels carrying expected-return, forward-return, optimizer,
    portfolio, Q2, broker/order/live, or production-like fields.
  - Local-only rescue did not improve coverage (`140 / 654` remained
    observed). Explicit WRDS `crsp.dsf_v2` rescue wrote
    `wrds_dsf_v2_label_rescue_price_panel.csv` with `176644` rows for `181`
    active D3 tickers and lifted observed primary labels to `429 / 654`
    (`label_coverage_share=0.655963`) across `34` observed event months.
    WRDS `crsp.dsf_v2` source max date is `2025-12-31`, while requested label
    coverage would need prices into 2026 for later active events.
  - Q1 remains `hold_insufficient_sample` with
    `coverage_rescue_status=blocked_data_coverage_after_rescue`. On the
    rescued observed slice, `primary_mean_abnormal_return=-0.011148452950123393`,
    `rank_ic_mean=0.022255095442329544`, and
    `top_bottom_spread_mean=0.0109813263656504`; however, pre-filing dominance,
    shifted-date placebo, and liquidity/cost pre-gate still fail. Because the
    pre-registered label coverage threshold was not met, this remains a data
    coverage blocker rather than alpha approval or final promotion evidence.
  - `open_market_insider_buying_post_2023_v0` is retired before Promotion Gate
    as observed-negative-after-rescue / not worth further rescue. This is a
    minimal stop marker for that v0 buy-conviction measurement only; it does
    not retire the broader insider-disclosure mechanism family.
  - D2-INSIDER-02 planned-vs-discretionary sell contrast is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_d2_sell_contrast.py`
    - runner: `scripts/run_insider_disclosure_d2_sell_contrast.py`
    - smoke target: `make factor-discovery-insider-d2-sell-contrast`
    - default output:
      `outputs/factor_discovery/insider_disclosure/d2_sell_contrast/`
  - D2-INSIDER-02 is no-formula observability only. It writes sell subset
    counts, plan-flag coverage, no-view reasons, CAR diagnostics, controls,
    placebos, and a D2 summary; it does not write a MeasurementSpec, formula
    score, expected-return panel, Q1/Q2 handoff, optimizer input, Alpha
    Registry update, paper workflow, broker/order/live path, or production
    approval.
  - The real aggregate run returns `overall_decision=blocked_plan_flag_coverage`
    because the parsed S-code stream has `discretionary_sell_event_count=7862`,
    `planned_sell_event_count=0`, and
    `unknown_plan_flag_event_count=16429`. D3 remains blocked for
    `planned_vs_discretionary_sell_contrast_post_2023` unless the Form 4
    10b5-1 plan-flag source/parser is repaired or a richer auditable source is
    added.
  - D2-INSIDER-02A plan-flag parser/source audit is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_plan_flag_audit.py`
    - runner: `scripts/run_insider_disclosure_plan_flag_audit.py`
    - smoke target: `make factor-discovery-insider-plan-flag-audit`
    - default output:
      `outputs/factor_discovery/insider_disclosure/d2_plan_flag_audit/`
  - D2-INSIDER-02A samples post-2023 S-code filings from the local aggregate
    registry and available raw source roots, inventories raw 10b5/plan/adoption
    fields, checks footnote/explanation text, and distinguishes explicit false
    from missing plan flags. The current local audit returns
    `overall_decision=hold_pending_clean_plan_flag_source`,
    `structured_true_count=10`, `structured_false_count=12`,
    `structured_missing_count=178`, `footnote_10b5_candidate_count=1`,
    `footnote_adoption_date_candidate_count=0`,
    `false_without_structured_source_count=99`, and
    `known_plan_flag_share=0.326743`. This suggests a parser/source coverage
    problem exists, but it is not yet a clean repaired plan-flag source. D2
    sell contrast remains blocked and no D3, Q1, Q2, optimizer, portfolio,
    Alpha Registry, paper, broker/order/live, or production path is opened.
  - D2-INSIDER-02B plan-flag source locator / parser repair is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/insider_disclosure_plan_flag_repair.py`
    - runner: `scripts/run_insider_disclosure_plan_flag_repair.py`
    - smoke target: `make factor-discovery-insider-plan-flag-repair`
    - default output:
      `outputs/factor_discovery/insider_disclosure/d2_plan_flag_repair/`
  - D2-INSIDER-02B audits accession-to-raw-XML resolution, missing raw paths,
    structured 10b5-1 explicit true/false/missing parser results, and
    before/after sell-subset classification. The current local run returns
    `overall_decision=source_locator_repair_failed_switch_to_8k`,
    `raw_file_found_share=0.002241`, `known_plan_flag_share=0.001582`,
    `structured_or_high_confidence_source_share=0.001582`,
    `repaired_planned_sell_event_count=25`,
    `repaired_planned_sell_month_count=6`,
    `repaired_discretionary_sell_event_count=13`, and
    `repaired_unknown_plan_flag_event_count=23987`. D2-INSIDER-02 remains
    `blocked_plan_flag_coverage`; no D3/Q1/Q2 or downstream path is opened.
    Per the stop rule, the recommended next alpha-discovery action is
    `D2-8K-01 subtype_underreaction_observability`.
  - D2-8K-01 subtype underreaction observability is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/eightk_subtype_d2.py`
    - runner: `scripts/run_factor_discovery_8k_subtype_d2.py`
    - smoke target: `make factor-discovery-8k-d2-observability`
    - default output:
      `outputs/factor_discovery/8k_subtype/d2/`
  - D2-8K-01 prioritizes auditor change, CFO departure, CEO departure,
    material agreement termination, and restatement/amendment-related 8-K
    rows. It writes `eightk_event_registry.csv`,
    `eightk_subtype_counts.csv`, `timestamp_audit.csv`,
    `coverage_report.csv`, `no_view_reason_report.csv`,
    `car_window_panel.csv`, `matched_control_panel.csv`,
    `placebo_report.csv`, `d2_8k_subtype_summary.json`, and
    `d2_8k_subtype_report.md`.
  - The current default run is a deterministic fixture path, not real EDGAR
    evidence. It returns `overall_decision=observable`, `event_count=25`,
    `priority_event_count=20`, `routine_control_event_count=4`,
    `unknown_no_view_event_count=1`, and `allow_d3_charter_for` limited to
    `auditor_change`, `cfo_departure`, `ceo_departure`,
    `material_agreement_termination`, and `restatement_amendment`.
  - D2-8K-01 writes no formula score, MeasurementSpec, expected-return panel,
    Q1/Q2 handoff, optimizer input, Alpha Registry update, paper workflow,
    broker/order/live path, or production approval. Missing subtype/coverage
    remains explicit no_view/abstain, not zero alpha. The recommended next
    alpha-discovery action is a real local EDGAR 8-K archive source-admission
    and replay path before any D3 MeasurementSpec.
  - D2-8K-01R real EDGAR 8-K archive source admission + subtype replay is
    implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/eightk_subtype_d2_real.py`
    - runner: `scripts/run_factor_discovery_8k_subtype_d2_real.py`
    - smoke target: `make factor-discovery-8k-d2-real-observability`
    - default output:
      `outputs/factor_discovery/8k_subtype/d2_real/`
  - D2-8K-01R reads only local EDGAR 8-K / 8-K/A archive sources when
    available (`request_specs.json` plus cached documents), audits accession to
    raw-document locator coverage, accepted timestamp coverage, document type,
    item-header parsing, issuer/ticker/market join coverage, and then replays
    the no-formula D2 CAR/control/placebo protocol.
  - D2-8K-01R now rescues source and market admission by preferring same-accession
    primary 8-K documents over exhibit-only links when a cached primary document
    is available, and by filtering/combining multiple local price panels into a
    replay-specific market panel before running the D2 protocol.
  - Online SEC source rescue was probed with a declared local user-agent string,
    but SEC still returned the undeclared automated tool block. A real reachable
    contact email is required before bulk SEC primary-document supplementing can
    be run responsibly.
  - WRDS/CRSP market rescue is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/eightk_wrds_market_rescue.py`
    - runner: `scripts/rescue_factor_discovery_8k_wrds_market_coverage.py`
    - smoke target: `make factor-discovery-8k-wrds-market-rescue`
    - output:
      `data/cache/factor_discovery_8k/wrds_priority_8k_price_rescue.csv`
    - manifest:
      `outputs/factor_discovery/8k_subtype/d2_real/wrds_market_rescue_manifest.json`
  - The current WRDS rescue run with username `chenhe44` completed with
    `eligible_event_count=98`, `skipped_after_wrds_max_date=235`,
    `linked_permno_count=39`, and `row_count=27183`. It writes a local CRSP
    market cache only; it is not alpha evidence and opens no Q1/Q2/downstream
    path. The rescue cannot cover 2025/2026 events because the current WRDS
    CRSP daily table available to this workspace reports max date
    `2024-12-31`.
  - The current controlled local archive replay reads 2,000 indexed files from
    `/Users/orynwilder/Documents/New project 2/sec_filing_archive/20260501T145601Z`.
    It returns `raw_file_found_share=1.0`,
    `accepted_timestamp_coverage_share=1.0`,
    `item_header_parse_coverage_share=0.439`,
    `market_coverage_share=0.07`,
    `priority_market_coverage_share=0.396396`,
    `price_panel_count=5`, `filtered_price_row_count=144858`,
    `filtered_price_ticker_count=75`, `event_count=2000`,
    `priority_event_count=333`, and
    `overall_decision=blocked_market_coverage` with
    `decision_reason=priority_real_8k_market_join_coverage_below_threshold`.
    `allow_d3_charter_for=[]`, so no real 8-K subtype enters D3.
  - D2-8K-01R writes `source_admission_report.json`,
    `raw_locator_coverage_report.csv`, `accepted_timestamp_coverage.csv`,
    `document_type_audit.csv`, `item_header_parse_audit.csv`,
    `issuer_market_join_coverage.csv`, `eightk_event_registry_real.csv`,
    `eightk_subtype_counts_real.csv`, `timestamp_audit_real.csv`,
    `coverage_report_real.csv`, `no_view_reason_report_real.csv`,
    `car_window_panel_real.csv`, `matched_control_panel_real.csv`,
    `placebo_report_real.csv`, `d2_8k_subtype_summary_real.json`, and
    `d2_8k_subtype_report_real.md`.
  - D2-8K-01R writes no formula score, MeasurementSpec, expected-return panel,
    Q1/Q2 handoff, optimizer input, Alpha Registry update, paper workflow,
    broker/order/live path, or production approval. Missing market/source
    coverage remains explicit no_view/abstain, not zero alpha. The recommended
    next alpha-discovery action is to improve real 8-K market coverage and
    primary-document/item-header source admission before rerunning real D2.
- D2-SMALL-EMOTION-01 Small-Cap Shock-Conditioned Emotion / Liquidity
  Observability is now the active Factor Discovery mainline:
  - D2-INSIDER-02 sell contrast is `stopped_before_d3` due to
    `plan_flag_source_locator_repair_failed`.
  - D2-8K-01 is held for data coverage and no D3 is open.
  - module:
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_d2.py`
  - runner: `scripts/run_factor_discovery_small_emotion_d2.py`
  - smoke target: `make factor-discovery-small-emotion-d2`
  - default output:
    `outputs/factor_discovery/small_emotion/d2_observability/`
  - The runner reads the local PIT small-cap daily price-volume panel, IWM
    benchmark panel, and delisting file, then builds no-formula D2 subsets for
    `panic_overreaction_candidate`, `fomo_continuation_candidate`, and
    `liquidity_vacuum_reversal_candidate`.
  - It writes subset counts, stale-price guard, ADV/capacity guard,
    cost/spread pre-gate, delisting retention audit, CAR windows,
    continuation/reversal diagnostics, matched controls, placebo report,
    no-view coverage report, summary JSON, and report markdown.
  - Current controlled smoke uses the default 750,000-row local replay cap and
    returns `overall_decision=hold_insufficient_sample`, `event_count=5696`,
    `active_event_count=5196`, `no_view_count=500`, `data_status=available`,
    and `allow_d3_charter_for=[]`. Subset counts show only 8 event months in
    the capped replay, so no D3 charter is opened.
  - D2-SMALL-EMOTION-01A chunked/full replay is implemented with subset-level
    stale/placebo guards:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_full_replay.py`
    - runner: `scripts/run_factor_discovery_small_emotion_full_replay.py`
    - smoke target: `make factor-discovery-small-emotion-full-replay`
    - default output:
      `outputs/factor_discovery/small_emotion/d2_full_replay/`
  - The full replay runs the existing D2 pass over local chunked annual price
    panels, writes per-chunk D2 artifacts under `chunks/chunk_*/`, and records
    `chunk_manifest.csv`, `subset_guard_aggregate.csv`,
    `full_replay_decision.json`, and `full_replay_report.md`. It is
    resumable: reruns load completed chunk summaries unless `--refresh` is
    used.
  - Current full replay covers 6 local WRDS chunks and `70002` active events.
    It returns `overall_decision=blocked_placebo_dominance`,
    `allow_d3_charter_for=[]`, and `minimum_observable_chunks=2`. The
    liquidity-vacuum subset is the strongest read but passes only 1 chunk,
    with 2 placebo-dominated chunks and 1 stale-placebo-dominated chunk; panic
    and fomo pass 0 chunks.
  - D2-SMALL-EMOTION-01B shock-direction remap audit is implemented as a
    no-formula diagnostic over existing D2 artifacts:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_direction_remap.py`
    - runner: `scripts/run_factor_discovery_small_emotion_direction_remap.py`
    - smoke target: `make factor-discovery-small-emotion-direction-remap`
    - default output:
      `outputs/factor_discovery/small_emotion/d2_direction_remap/`
  - The remap evaluates `up_shock_continuation`, `up_shock_reversal`,
    `down_shock_reversal`, and `down_shock_continuation` without changing the
    source D2 artifacts, writing a remap grid, transformed placebo audit,
    decision JSON, and report. The default 750,000-row remap remains
    `hold_insufficient_sample`; a 1,200,000-row probe reaches 14 event months
    but returns `blocked_placebo_dominance`, so flipping the direction does not
    currently allow D3.
  - E0-SMALL-EMOTION-02 exploratory parameter sweep is implemented as the
    explicit overfit/search layer before any D3 freeze:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_exploratory_sweep.py`
    - runner: `scripts/run_factor_discovery_small_emotion_exploratory_sweep.py`
    - smoke target: `make factor-discovery-small-emotion-exploratory-sweep`
    - default output:
      `outputs/factor_discovery/small_emotion/e0_exploratory_sweep/`
  - The sweep searches shock thresholds, abnormal-volume thresholds, direction
    hypotheses, market-cap buckets, liquidity filters, stale filters, ADV
    gates, and post-shock windows, then writes `parameter_sweep_grid.csv`,
    `best_in_sample_candidates.csv`, `overfit_risk_report.json`,
    `candidate_to_freeze_next.json`, `exploratory_sweep_summary.json`, and
    report markdown. It is explicitly allowed to overfit in-sample, but all
    artifacts state that the output is not alpha evidence and must be frozen
    before Q1/OOS/placebo validation.
  - Current E0 controlled smoke uses the default 750,000-row replay cap and
    returns `candidate_found_for_possible_freeze=true` with top in-sample
    pocket `up_shock_reversal`: `shock_threshold=0.05`,
    `volume_spike_threshold=1.5`, `market_cap_bucket=all_small_cap`,
    `liquidity_filter=all`, `stale_filter=medium`, `adv_min_dollars=250000`,
    `window=post_1_22`, `active_event_count=4296`, `event_month_count=7`,
    `mean_directional_return=0.033745`, and `hit_rate=0.599395`. This is a
    search result only; it does not open D3 automatically.
  - E0-SMALL-EMOTION-02A top-pocket chunked replay is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_top_pocket_replay.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_top_pocket_replay.py`
    - smoke target:
      `make factor-discovery-small-emotion-top-pocket-replay`
    - default output:
      `outputs/factor_discovery/small_emotion/e0_top_pocket_replay/`
  - Current top-pocket replay covers 6 local WRDS chunks and confirms the
    pocket is not only a 750k-row cap artifact: `aggregate_active_event_count=52562`,
    `observed_chunk_count=6`, `positive_chunk_count=4`,
    `weighted_mean_directional_return=0.004788`, and
    `weighted_hit_rate=0.579278`. The effect shrinks materially from the
    capped smoke read, but remains positive enough for manual D3 charter
    review. It is still exploratory and not alpha evidence.
  - D3-SMALL-EMOTION-03 candidate charter is implemented for
    `small_cap_up_shock_reversal_post_1_22_v0`:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_d3_charter.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_d3_charter.py`
    - smoke target:
      `make factor-discovery-small-emotion-d3-charter`
    - default output:
      `outputs/factor_discovery/small_emotion/d3_up_shock_reversal_charter/`
  - The charter freezes the mechanism, thresholds, primary window,
    timestamp/no-view policy, replay evidence, and hard falsifiers. It writes
    no MeasurementSpec, signal panel, expected-return panel, Q1/Q2 handoff,
    optimizer input, portfolio artifact, Alpha Registry update, paper workflow,
    broker/order/live path, or production approval.
  - E0-SMALL-EMOTION-04 mechanism sharpening sweep is implemented as an
    explicit aggressive overfit/search layer:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_sharpening_sweep.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_sharpening_sweep.py`
    - smoke target:
      `make factor-discovery-small-emotion-sharpening-sweep`
    - default output:
      `outputs/factor_discovery/small_emotion/e0_sharpening_sweep/`
  - Current E0 sharpening sweep tests a bounded aggressive grid of 15,552 rows
    and finds top in-sample pocket:
    `mechanism=up_shock_reversal`, `shock_threshold=0.05`,
    `volume_spike_threshold=1.5`, `prior_5d_min_return=0.20`,
    `market_cap_bucket=micro`, `regime_filter=market_up_20d`,
    `adv_min_dollars=250000`, `window=post_1_22`,
    `active_event_count=54`, `event_month_count=6`,
    `mean_directional_return=0.219921`, and `hit_rate=0.870370`.
    This is intentional in-sample overfit discovery, not alpha evidence.
  - E0-SMALL-EMOTION-04A sharpened top-pocket chunked replay is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_sharpened_top_pocket_replay.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_sharpened_top_pocket_replay.py`
    - smoke target:
      `make factor-discovery-small-emotion-sharpened-top-pocket-replay`
    - default output:
      `outputs/factor_discovery/small_emotion/e0_sharpened_top_pocket_replay/`
  - Current sharpened replay preserves the sharpening filters across 6 local
    WRDS chunks and returns `overall_decision=candidate_stable_enough_for_manual_d3_freeze_review`,
    `aggregate_active_event_count=859`, `observed_chunk_count=6`,
    `positive_chunk_count=5`, `weighted_mean_directional_return=0.068215`,
    and `weighted_hit_rate=0.681024`.
  - D3 sharpened candidate charter is frozen for
    `small_cap_sharpened_up_shock_reversal_post_1_22_v0`:
    - smoke target:
      `make factor-discovery-small-emotion-sharpened-d3-charter`
    - default output:
      `outputs/factor_discovery/small_emotion/d3_sharpened_up_shock_reversal_charter/`
    - candidate charter hash:
      `1e8c8c85aee6067ca056ae19d6d38c4aba19d66fe9019e5213dce8cb11d5729e`
  - The sharpened charter is still not alpha evidence. It writes no
    MeasurementSpec, signal panel, expected-return panel, Q1/Q2 handoff,
    optimizer input, portfolio artifact, Alpha Registry update, paper workflow,
    broker/order/live path, or production approval.
  - E0-SMALL-EMOTION-05 greedy leaf search is implemented:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_leaf_search.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_leaf_search.py`
    - smoke target:
      `make factor-discovery-small-emotion-leaf-search`
    - default output:
      `outputs/factor_discovery/small_emotion/e0_leaf_search/`
  - Current E0.5 leaf search explores 760 tree nodes across shock directions,
    windows, and greedy predicates. It selects the same strongest leaf found by
    the sharpening sweep:
    `mechanism=up_shock_reversal`, `window=post_1_22`,
    `path_predicates=prior5_ge_20pct & regime_market_up & mcap_micro`,
    `active_event_count=54`, `event_month_count=6`,
    `mean_directional_return=0.219921`, `hit_rate=0.870370`.
    This corroborates the pocket selection inside the E0 search space, but
    remains aggressive in-sample overfit discovery and not alpha evidence.
  - E1-SMALL-EMOTION-FULL-MARKET-OVERFIT is implemented as a separate
    exploratory overfit lab after the frozen small-cap pocket failed full
    Promotion Gate falsifiers:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_full_market_overfit_lab.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py`
    - smoke target:
      `make factor-discovery-small-emotion-full-market-overfit-lab`
    - default output:
      `outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab/`
  - E1 widens the search to full-market common-stock research coverage and
    searches shock direction, holding window, liquidity, spread, market regime,
    size, price, prior-return, and close-location leaves. It writes a
    full-market overfit grid, top-pocket leaderboard, tail-concentration audit,
    cost/liquidity audit, and best-pocket spec draft. It remains exploratory
    overfit discovery only: no MeasurementSpec, signal panel, expected-return
    panel, Q1/Q2 handoff, optimizer input, portfolio artifact, Alpha Registry
    update, paper/live/broker/order workflow, or production approval is opened.
  - Current bounded E1 smoke (`max_rows=750000`) writes 4,128 searched rows and
    finds an in-sample `up_shock_reversal / post_1_22` pocket with
    `path_predicates=size_micro & open_to_close_le_minus_5pct`,
    `active_event_count=69`, `event_month_count=7`,
    `mean_directional_return=0.197366`, `t_stat=5.332608`, and
    `hit_rate=0.753623`. Tail concentration remains `review_tail_concentration`.
    Cost/liquidity audit artifacts are written.
  - E1 full no-cap replay now has an asset-sharded feature-cache path:
    `make factor-discovery-small-emotion-full-market-feature-cache` writes
    cached full-sample event labels under
    `data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full/`,
    and `make factor-discovery-small-emotion-full-market-cached-replay` reads
    that cache for the pocket search. The full cache build read
    `price_row_count=5977605` and wrote `event_label_row_count=1324572` across
    four event-label windows.
  - Current full cached E1 replay writes 4,128 searched rows under
    `outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached/`.
    The strongest full-sample exploratory pocket is
    `mechanism=up_shock_reversal`, `window=post_1_22`,
    `path_predicates=spread_wide & shock_ge_20pct`,
    `active_event_count=178`, `event_month_count=53`, `issuer_count=148`,
    `mean_directional_return=0.259144`, `t_stat=8.389154`, and
    `hit_rate=0.848315`. Tail concentration passes, but the cost/liquidity
    audit is `review_cost_liquidity` with very wide spread / slippage stress,
    so this remains an in-sample overfit pocket, not alpha evidence and not
    D3/Q1/Q2-ready.
  - SMALL-EMOTION-FREEZE-02 is implemented for the full cached E1 top pocket:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_freeze_validation.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_freeze_validation.py`
    - smoke/full target:
      `make factor-discovery-small-emotion-freeze-validation`
    - default output:
      `outputs/factor_discovery/small_emotion/freeze_02_full_market_locked_validation/`
    - The selected pocket is locked as `mechanism=up_shock_reversal`,
      `window=post_1_22`, `path_predicates=spread_wide & shock_ge_20pct`.
      It is not identical to the prior frozen small-cap MeasurementSpec hash
      `eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976`,
      so the phase writes a new D3 charter and D4 MeasurementSpec only for this
      exact full-market pocket. The new measurement spec hash is
      `6120ea8b20a6d04814be39b3999449f3cc10a5c0a0fb8754f436e92351db22a9`.
    - Locked temporal validation keeps positive reads across splits: train
      `mean_directional_return=0.207393`, validation
      `mean_directional_return=0.333014`, and test
      `mean_directional_return=0.337654`, with `locked_event_count=178`,
      `locked_event_month_count=53`, and `locked_issuer_count=148`.
    - The locked gate still fails with `decision=stale_or_bad_print_failed`
      and `decision_reason=top_contributor_data_anomaly`. Top-contributor audit
      flags three bad-print proxy events; sweep-adjusted audit has
      `selected_beats_best_placebo=false` because the best shifted-date
      placebo-selected pocket has `mean_directional_return=0.367531` versus the
      live pocket's `0.259144`; conservative cost/liquidity stress also fails,
      with next-close post-cost mean `-0.017643` and next-open proxy post-cost
      mean `-0.127327`.
    - All downstream flags remain false: no expected-return panel, Q2 handoff,
      optimizer input, portfolio artifact, Alpha Registry update, paper
      workflow, broker/order/live path, or production approval.
  - E1 full-market continuation scan was run on the existing full feature cache
    with wider in-sample search burden (`grid_row_count=15264`,
    `max_depth=5`, `beam_width=64`, shock thresholds
    `0.05/0.08/0.10/0.15/0.20`, volume thresholds `1.0/1.5/2.0/3.0/5.0`).
    The output is
    `outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached_wide_scan_20260518/`.
    The top pocket is still in the same illiquid wide-spread family:
    `mechanism=up_shock_reversal`, `window=post_1_22`,
    `path_predicates=close_top_quartile & spread_wide`,
    `shock_threshold=0.20`, `active_event_count=128`,
    `event_month_count=46`, `issuer_count=112`,
    `mean_directional_return=0.273375`, `t_stat=7.290920`, and
    `hit_rate=0.875000`.
  - FREEZE-02 path locking was corrected so D3/D4 locked validation applies the
    selected pocket's full path predicates instead of only spread/shock filters;
    the focused regression test now covers
    `close_top_quartile & spread_wide & shock_ge_20pct`. The corrected locked
    validation for the wide-scan top pocket writes
    `outputs/factor_discovery/small_emotion/freeze_02_wide_scan_top_locked_validation_20260518/`
    with candidate id
    `small_emotion_full_market_close_top_quartile_spread_wide_shock_ge20_up_reversal_post_1_22_v0`
    and `new_measurement_spec_hash=e20be6f89770baaefdeccaceabf156c622636a30d6634aeb71603ccb732143ec`.
    Temporal split reads remain positive (train `0.219465`, validation
    `0.329115`, test `0.375221`), but the locked decision is still
    `stale_or_bad_print_failed`: `anomaly_count=5`, `selected_beats_best_placebo=false`
    because shifted-date placebo selection has mean `0.367531` versus live
    `0.273375`, and conservative cost/liquidity stress fails with next-close
    post-cost mean `-0.001780` and next-open proxy post-cost mean `-0.112638`.
    Q1/Q2, optimizer, portfolio, Alpha Registry, paper/live/broker/order, and
    production flags remain false.
  - A cost-clean E1 full-market cached replay was run after excluding the
    known cost-toxic predicates `spread_wide`, `price_under_5`,
    `weak_liquidity`, and `liquidity_low` from the greedy leaf search:
    - target:
      `make factor-discovery-small-emotion-full-market-cost-clean-cached-replay`
    - output:
      `outputs/factor_discovery/small_emotion/e1_full_market_cost_clean_cached_replay/`
    - `grid_row_count=13440`, `top_pocket_count=100`,
      `excluded_predicates=["liquidity_low","price_under_5","spread_wide","weak_liquidity"]`.
    - The top cost-clean pocket is `mechanism=up_shock_reversal`,
      `window=post_1_22`,
      `path_predicates=prior5_ge_20pct & open_to_close_le_minus_5pct`,
      `shock_threshold=0.20`, `active_event_count=238`,
      `event_month_count=56`, `issuer_count=202`,
      `mean_directional_return=0.204386`, `t_stat=5.861639`, and
      `hit_rate=0.831933`. Top-20 pocket paths contain none of the excluded
      predicates, and E1 cost/liquidity audit passes for the top pockets
      (`spread_proxy_p95` around 2%-3% on the top rows).
    - The corrected locked validation for the cost-clean top pocket writes
      `outputs/factor_discovery/small_emotion/freeze_02_cost_clean_top_locked_validation_20260518/`
      with candidate id
      `small_emotion_full_market_prior5_ge_20pct_open_to_close_le_minus_5pct_shock_ge20_up_reversal_post_1_22_v0`
      and `new_measurement_spec_hash=a088335d5ebd69420b195b0aafa3c8c68ddbd47db5089056e98ca00ba930337d`.
      It passes anomaly, cost, and capacity gates (`anomaly_count=0`,
      `cost_gate_failed=false`, `capacity_gate_failed=false`) and keeps
      positive temporal splits (train `0.163576`, validation `0.268872`, test
      `0.260630`). It still fails locked validation with
      `decision=selection_bias_failed` because the best stale-price-matched
      placebo-selected pocket has `mean_directional_return=0.219018`, above
      the live pocket's `0.204386`. Downstream Q1/Q2, optimizer, portfolio,
      Alpha Registry, paper/live/broker/order, and production flags remain
      false.
  - A stricter cost+stale-clean E1 full-market cached replay was added and
    run after excluding the known cost-toxic predicates plus candidate events
    with `stale_roll_5 >= 1` or `zero_volume=true`:
    - target:
      `make factor-discovery-small-emotion-full-market-cost-stale-clean-cached-replay`
    - output:
      `outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/`
    - The replay removes `75140` candidate event rows (`1324572` before,
      `1249432` after), keeps `grid_row_count=13440`, and finds the same top
      pocket shape:
      `mechanism=up_shock_reversal`, `window=post_1_22`,
      `path_predicates=prior5_ge_20pct & open_to_close_le_minus_5pct`,
      `shock_threshold=0.20`, `active_event_count=225`,
      `event_month_count=55`, `issuer_count=193`,
      `mean_directional_return=0.206120`, `t_stat=5.749171`, and
      `hit_rate=0.831111`.
    - The stale-clean locked validation writes
      `outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_top_locked_validation_20260518/`.
      It removes `18785` stale/zero-volume rows from the cached
      `post_1_22` validation panel, keeps positive temporal splits (train
      `0.169302`, validation `0.263687`, test `0.259009`), and passes anomaly,
      cost, and capacity gates (`anomaly_count=0`,
      `cost_gate_failed=false`, `capacity_gate_failed=false`). It still fails
      with `decision=selection_bias_failed` because the same-coverage-random
      placebo-selected pocket has `mean_directional_return=0.346686`, above
      the live pocket's `0.206120`; that placebo has weak supporting profile
      (`t_stat=0.978498`, `hit_rate=0.456410`), but the current sweep gate is
      mean-return based and therefore blocks promotion. Downstream Q1/Q2,
      optimizer, portfolio, Alpha Registry, paper/live/broker/order, and
      production flags remain false.
    - An additional robustness-oriented locked validation was run on the
      overfit-score top pocket
      `down_shock_continuation / post_1_22 / close_top_quartile` using the
      same cost+stale-clean filters. It has broad coverage (`28746` events,
      `69` months, `2093` issuers) and positive temporal splits, but fails as
      `stale_or_bad_print_failed` due to top-contributor bad-print proxies and
      also does not beat the same-coverage-random placebo on the current
      mean-return sweep gate. It is not a Q1/Q2 candidate.
    - The freeze/promotion sweep gate was pre-registered from mean-only
      placebo dominance to a profile gate:
      `profile_score=max(mean,0)*max(t_stat,0)*max(hit_rate-0.5,0)*sqrt(event_month_count)`.
      This preserves the best-mean placebo fields for audit but blocks only
      placebo-selected pockets that also have credible t-stat, hit-rate, and
      time-breadth support. Two focused tests now cover (a) high-mean but weak
      random placebo no longer dominating and (b) stable high-profile placebo
      still blocking the live pocket.
    - The cost+stale-clean top pocket was rerun under the pre-registered
      profile gate:
      `outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_top_profile_gate_validation_20260518/`.
      It keeps `locked_event_count=225`, `locked_event_month_count=55`,
      `locked_mean_directional_return=0.206120`, `anomaly_count=0`,
      `cost_gate_failed=false`, and `capacity_gate_failed=false`. The old
      mean-only comparison remains transparent:
      `selected_mean_beats_best_placebo_mean=false` because the
      same-coverage-random placebo mean is `0.346686`; however that random
      placebo has weak profile (`t_stat=0.978498`, `hit_rate=0.456410`). Under
      the profile gate, the live profile score is `2.909918` versus best
      placebo profile score `1.254377` (`adv_capacity_matched`), so the locked
      run returns `decision=promote_to_q2_candidate` with
      `decision_reason=locked_validation_passed_no_downstream_opened`. Despite
      the decision label, downstream Q1/Q2, optimizer, portfolio, Alpha
      Registry, paper/live/broker/order, and production flags remain false
      until an explicit reopen.
    - The cost+stale-clean grid was re-ranked by the same profile score used
      in the freeze gate, producing
      `outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay/top_100_profile_score_pockets.csv`.
      Profile-score rank 1 is
      `open_to_close_le_minus_5pct & size_micro & prior5_ge_20pct`,
      `shock_threshold=0.15`, `post_1_22`, with `264` events, `60` months,
      `222` issuers, `mean_directional_return=0.202469`, `t_stat=6.514561`,
      `hit_rate=0.810606`, and profile score `3.173429`.
      Its locked validation writes
      `outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_score_top_validation_20260518/`
      and returns `decision=promote_to_q2_candidate`,
      `live_profile_score=3.173429`, best placebo profile score `1.484325`,
      `anomaly_count=0`, `cost_gate_failed=false`, and
      `capacity_gate_failed=false`.
    - Profile-score rank 2 removes the explicit `size_micro` filter:
      `open_to_close_le_minus_5pct & prior5_ge_20pct`, `shock_threshold=0.15`,
      `post_1_22`, with `313` events, `62` months, `257` issuers,
      `mean_directional_return=0.191242`, `t_stat=6.712406`,
      `hit_rate=0.808307`, and profile score `3.116300`. Its locked validation
      writes
      `outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank2_validation_20260518/`
      and also returns `decision=promote_to_q2_candidate`,
      `anomaly_count=0`, `cost_gate_failed=false`, and
      `capacity_gate_failed=false`. This broader rank-2 variant is the cleaner
      practical candidate for the next explicit Q1/OOS reopen if avoiding a
      micro-only thesis is preferred.
    - Q1/OOS was explicitly opened for that broader profile-score rank-2
      variant without changing the frozen MeasurementSpec, formula, window,
      threshold, or path predicates:
      `outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518/`.
      The Q1 runner now supports full-market MeasurementSpecs
      (`market_cap_bucket=all_full_market`), locked `path_predicates`, and
      `--exclude-stale-price-events` so the replay matches the cost+stale-clean
      freeze path instead of silently falling back to the small-cap universe.
      The full no-cap replay returns
      `q1_decision=passed_q1_research_review`, `active_event_count=336`,
      `observed_primary_label_count=313`, `event_month_count=62`,
      `mean_primary_directional_return=0.191242`,
      `oos_test_mean_directional_return=0.260129`,
      `falsifier_dominance_count=0`, and `policy_breach_count=0`.
      It sets `promotion_gate_allowed=true` as a Q1 research-stage gate only;
      `q2_entry_allowed=false`, `optimizer_entry_allowed=false`,
      `portfolio_construction_allowed=false`,
      `alpha_registry_update_allowed=false`, and all paper/live/broker/order/
      production flags remain false. No expected-return panel is written.
    - PG-SMALL-EMOTION-01 was then run for the same broader rank-2 candidate
      using the full no-cap Q1 output and required MeasurementSpec hash
      `21cbf8277ed0778a8b0aa1ef473d65bcfa14c202830f4d7d391923dcdd0fd9b9`.
      The output directory is
      `outputs/factor_discovery/small_emotion/pg_profile_rank2_broad_post_1_22_20260519/`.
      The gate returns `promotion_decision=promote_to_q2_candidate` and
      `stop_reason=promotion_gate_passed_no_q2_run`, with
      `full_no_cap_q1_observed=true`, `anomaly_status=pass`,
      `tail_status=pass`, `cost_liquidity_status=pass`, and
      `time_breadth_status=pass`. Search burden is still a warning
      (`search_grid_row_count=13440`), not a hard fail. The gate leaves
      `q2_entry_allowed=false`, `optimizer_entry_allowed=false`,
      `portfolio_construction_allowed=false`,
      `alpha_registry_update_allowed=false`, and all paper/live/broker/order/
      production flags false; explicit Q2 reopen is still required.
    - Profile-score rank 3 is the shorter `post_1_10` version of the rank-2
      pocket with `321` events, `63` months, `265` issuers,
      `mean_directional_return=0.162177`, `t_stat=8.138467`, and profile score
      `2.986192`. Its locked validation writes
      `outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank3_validation_20260518/`
      and passes the profile gate, but its margin over the best placebo
      profile is narrower (`2.986192` vs `2.581237`). It remains a secondary
      candidate only.
    - Explicit Q2 reopen was then applied to the three cost+stale-clean
      promoted pockets only after full no-cap Q1 and PG checks:
      rank1 micro `post_1_22`, rank2 broader `post_1_22`, and rank3 broader
      `post_1_10`. Q1/PG additions are:
      `q1_profile_rank1_micro_post_1_22_oos_20260519/`,
      `pg_profile_rank1_micro_post_1_22_20260519/`,
      `q1_profile_rank3_broad_post_1_10_oos_20260519/`, and
      `pg_profile_rank3_broad_post_1_10_20260519/`. Both rank1 and rank3
      return `promotion_decision=promote_to_q2_candidate`; rank1 has
      `observed_primary_label_count=264`,
      `mean_primary_directional_return=0.202469`, and
      `oos_test_mean_directional_return=0.279462`; rank3 has
      `observed_primary_label_count=321`,
      `mean_primary_directional_return=0.162177`, and
      `oos_test_mean_directional_return=0.194286`. The Q2 intake output is
      `outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519/`,
      with `opened_q2_candidate_count=3`,
      `expected_return_panel_row_count=955`, and `q2_entry_allowed=true`.
      This is Q2 candidate intake / diagnostic expected-return input only:
      `optimizer_entry_allowed=false`, `portfolio_construction_allowed=false`,
      `alpha_registry_update_allowed=false`, and all paper/live/broker/order/
      production flags remain false.
    - Q2-SMALL-EMOTION-02 execution-survival diagnostics are implemented for
      the three opened candidates:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_execution_survival.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_survival.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-survival`
      - output:
        `outputs/factor_discovery/small_emotion/q2_execution_survival_20260519/`
      - artifacts include `small_emotion_q2_execution_survival_matrix.csv`,
        `small_emotion_q2_cost_capacity_report.csv`,
        `small_emotion_q2_holding_path.csv`,
        `small_emotion_q2_optimizer_input_probe.csv`, summary JSON, and a
        markdown report.
      - Current run returns `survival_passed_count=3`,
        `cost_capacity_failed_count=0`, and
        `optimizer_input_unavailable_count=0`. Rank1/rank2/rank3 all return
        `survival_decision=execution_survival_passed` with optimizer input
        staged for inspection only. The step does not run an optimizer, build
        a portfolio, update Alpha Registry, or open paper/live/broker/order/
        production workflows; all downstream flags remain false.
    - Q2-SMALL-EMOTION-03 optimizer adapter dry-run is implemented:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_optimizer_dry_run.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_optimizer_dry_run.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-optimizer-dry-run`
      - output:
        `outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520/`
      - artifacts include `small_emotion_q2_optimizer_response_matrix.csv`,
        `small_emotion_q2_optimizer_constraint_response.csv`,
        `small_emotion_q2_optimizer_input_snapshot.csv`, summary JSON,
        manifest, and a markdown report.
      - Current run returns `optimizer_observed_candidate_count=3` and
        `response_row_count=9`; all rank1/rank2/rank3 live, sign-flipped, and
        zero-alpha panels solve with local optimizer status `optimal`.
        Turnover, participation, and single-name constraints pass. The live
        negative-return panels reduce net weight, sign-flipped panels increase
        net weight, and zero-alpha panels act as controls. This is optimizer
        input / constraint-response dry-run only: it writes no orders, does
        not build a portfolio construction artifact, does not update Alpha
        Registry, and keeps paper/live/broker/order/production flags false.
    - Q2-SMALL-EMOTION-04 execution-survival closeout is implemented:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_complete.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_complete.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-complete`
      - output:
        `outputs/factor_discovery/small_emotion/q2_complete_20260520/`
      - artifacts include `small_emotion_q2_complete_matrix.csv`, summary
        JSON, manifest, and a markdown report.
      - Current run returns `candidate_count=3`,
        `q2_complete_passed_count=3`, and `q2_complete_failed_count=0`.
        Rank1, rank2, and rank3 all have
        `q2_complete_decision=completed_q2_execution_survival`, three observed
        optimizer panels, zero constraint failures, and sign-response status
        `pass`.
      - This closes the local Q2 execution-survival chain by checking intake,
        cost/capacity survival, optimizer response, sign-flip response, and
        constraint status. It does not write orders, build a portfolio
        construction artifact, update Alpha Registry, or open
        paper/live/broker/order/production workflows.
    - Q2-SMALL-EMOTION-05 portfolio quant replay is implemented:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_portfolio_replay.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_portfolio_replay.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-portfolio-replay`
      - output:
        `outputs/factor_discovery/small_emotion/q2_portfolio_replay_20260520/`
      - artifacts include `small_emotion_q2_portfolio_replay_matrix.csv`,
        event panel, monthly returns, NAV/drawdown curve, cost attribution,
        policy gate, summary JSON, manifest, and markdown report.
      - Current run returns `candidate_count=3`,
        `portfolio_replay_completed_count=2`, and
        `portfolio_replay_blocked_count=1`. Rank1 and rank2 complete the
      portfolio quant replay; rank3 is blocked by the spread/cost-liquidity
      policy gate. This is Q2 portfolio quant replay only: it writes no
      orders, does not build production portfolio construction artifacts,
      does not update Alpha Registry, and keeps paper/live/broker/order/
      production workflows closed.
    - Q2-SMALL-EMOTION-06 factor exposure / beta residual audit is
      implemented:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_factor_exposure_audit.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_factor_exposure_audit.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-factor-exposure-audit`
      - output:
        `outputs/factor_discovery/small_emotion/q2_factor_exposure_audit_20260520/`
      - artifacts include `small_emotion_q2_factor_exposure_panel.csv`,
        `small_emotion_q2_beta_residual_matrix.csv`,
        `small_emotion_q2_factor_loading_matrix.csv`,
        factor exposure summary, policy gate, summary JSON, manifest, and a
        markdown report.
      - Current run returns `candidate_count=3` and
        `beta_residual_passed_count=3` using local benchmark/beta, size,
        liquidity, volatility, reversal, shock, and attention proxy controls.
        Rank1 has factor-adjusted alpha `0.202469` with t-stat `6.596028`;
        rank2 has factor-adjusted alpha `0.191242` with t-stat `6.833515`;
        rank3 has factor-adjusted alpha `0.162177` with t-stat `8.350597`.
        This audit supports that the current reads are not explained by the
        simple benchmark/beta proxy controls, but it does not override the
        Q2 portfolio replay warning: rank1/rank2 still have severe drawdowns,
        and rank3 remains blocked by cost/liquidity. It writes no orders, does
        not build production portfolio construction artifacts, does not update
        Alpha Registry, and keeps paper/live/broker/order/production workflows
        closed.
    - Q2-SMALL-EMOTION-07 robustness profile audit is implemented:
      - module:
        `projects/execution_aware_optimizer/src/execution_aware_optimizer/small_emotion_q2_robustness_audit.py`
      - runner:
        `scripts/run_factor_discovery_small_emotion_q2_robustness_audit.py`
      - smoke target:
        `make factor-discovery-small-emotion-q2-robustness-audit`
      - output:
        `outputs/factor_discovery/small_emotion/q2_robustness_audit_20260520/`
      - artifacts include `small_emotion_q2_robustness_matrix.csv`,
        `small_emotion_q2_horizon_decay_matrix.csv`,
        `small_emotion_q2_concentration_matrix.csv`,
        `small_emotion_q2_overlap_matrix.csv`,
        `small_emotion_q2_bootstrap_matrix.csv`, summary JSON, manifest, and a
        markdown report.
      - Current run returns `candidate_count=3`,
        `robustness_passed_count=3`, and `redundant_pair_count=3` while
        explicitly ignoring drawdown as a hard gate by user request. Rank1,
        rank2, and rank3 all pass temporal breadth, hit-rate, t-stat, bootstrap,
        and tail concentration checks. Candidate overlap is high:
        rank1/rank2 event Jaccard `0.820189`, rank1/rank3 `0.811146`, and
        rank2/rank3 `0.975078`, so the three should be treated as near-neighbor
        variants of the same up-shock reversal mechanism rather than three
        independent alphas. It writes no orders, does not build production
        portfolio construction artifacts, does not update Alpha Registry, and
        keeps paper/live/broker/order/production workflows closed.
  - D4-SMALL-EMOTION-04 MeasurementSpec freeze is implemented for
    `small_cap_sharpened_up_shock_reversal_post_1_22_v0`:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_measurement_spec.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_measurement_spec.py`
    - smoke target:
      `make factor-discovery-small-emotion-measurement-spec`
    - default output:
      `outputs/factor_discovery/small_emotion/d4_sharpened_measurement_spec/`
    - `measurement_spec_hash=eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976`
    - source charter hash:
      `1e8c8c85aee6067ca056ae19d6d38c4aba19d66fe9019e5213dce8cb11d5729e`
  - Q1-SMALL-EMOTION-01 falsifier/OOS review is implemented for that frozen
    MeasurementSpec:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_q1_oos.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_q1_oos.py`
    - smoke target:
      `make factor-discovery-small-emotion-q1-oos`
    - default output:
      `outputs/factor_discovery/small_emotion/q1_sharpened_up_shock_reversal_oos/`
  - Current bounded Q1 smoke uses the same default 750,000-row local replay cap
    as the E0 search path and keeps full single-file replay explicit via
    `--max-rows 0`. It returns
    `q1_decision=passed_q1_research_review`,
    `active_event_count=59`, `observed_primary_label_count=54`,
    `event_month_count=6`,
    `mean_primary_directional_return=0.219921`,
    `oos_test_mean_directional_return=0.221275`,
    `falsifier_dominance_count=0`, and `policy_breach_count=0`.
    Shifted-date, same-coverage random, large-cap matched shock,
    stale-price matched, and ADV/capacity matched falsifiers do not dominate
    the live bounded read.
  - The Q1 result is a research-stage read for a heavily searched and frozen
    pocket. It opens no expected-return panel, Q2 handoff, optimizer input,
    portfolio artifact, Alpha Registry update, paper workflow,
    broker/order/live path, or production approval. Missing coverage remains
    explicit no_view / abstain and is not encoded as zero alpha. A full
    single-file Q1 run must be requested explicitly with `--max-rows 0`.
  - PG-SMALL-EMOTION-01 Promotion Gate is implemented for
    `small_cap_sharpened_up_shock_reversal_post_1_22_v0`:
    - module:
      `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/small_emotion_promotion_gate.py`
    - runner:
      `scripts/run_factor_discovery_small_emotion_promotion_gate.py`
    - smoke/full target:
      `make factor-discovery-small-emotion-promotion-gate`
    - default output:
      `outputs/factor_discovery/small_emotion/pg_sharpened_up_shock_reversal/`
    - The gate requires
      `measurement_spec_hash=eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976`,
      audits search burden, tail concentration, data anomalies,
      cost/liquidity implementability, and time breadth, and keeps all Q2,
      optimizer, portfolio, Alpha Registry, paper/live/broker/order, and
      production flags false.
    - The full no-cap Q1 replay now completes with `--max-rows 0` on the
      5,977,605-row local WRDS small-cap daily panel. Full Q1 returns
      `q1_decision=blocked_placebo_dominance`, `active_event_count=1214`,
      `observed_primary_label_count=1182`, `event_month_count=45`,
      `mean_primary_directional_return=0.003136`,
      `oos_test_mean_directional_return=0.097206`, and
      `falsifier_dominance_count=3`.
    - PG decision is `reject_overfit_or_data_artifact` with
      `stop_reason=q1_failed_full_replay`, `full_no_cap_q1_observed=true`,
      `promotion_gate_allowed=false`, and `q2_entry_allowed=false`.
      Same-coverage random, large-cap matched shock, and ADV/capacity matched
      placebos beat the live full replay. Tail concentration, anomaly, time
      breadth, and cost/liquidity audits pass; cost now uses available
      bid/ask-spread proxy and slippage-stress rows.
  - The D2/E0 observability and search layers before the D4 freeze wrote no
    formula score, MeasurementSpec, expected-return panel, Q1/Q2 handoff,
    optimizer input, portfolio artifact, Alpha Registry update, paper
    workflow, broker/order/live path, or production approval. Missing coverage
    remains explicit no_view / abstain and is not encoded as zero alpha.
- New Phase 35-42 roadmap has been added:
  - the completed Phase 20-34 platform packaging work remains closed.
  - the new active direction is `Typed Alpha View / Research-to-Paper Closed Loop`.
  - Phase 35 Typed Alpha View Contract is now complete.
  - Phase 36 Event-Aware Evaluation Kernel is now complete.
  - Phase 37 Alpha Projection Bridge v2 is now complete.
  - Phase 38 Promotion Gate v2 is now complete.
  - Phase 39 Q2 Typed Alpha Execution Matrix is now complete.
  - Phase 40 Paper Overlay Calibration Lane is now complete.
  - Phase 41 First Real Typed Alpha Pilot is now complete for the local SUE integration benchmark.
  - Phase 42 Typed Alpha Demo v2 is now complete.
  - core rule: `no_view != zero_alpha`; missing alpha coverage must be explicit abstain, not silently encoded as zero alpha.
  - the new roadmap keeps Q1, Evidence Bundle, Promotion Gate, Q2, paper overlay, and dashboard boundaries separate.
  - live paper overlay calibration, live data refreshes, and broker paths remain explicit opt-in work only.
- Optional Phase 43-46 roadmap has been added:
  - Phase 43 Typed Alpha Release Candidate Hardening is now complete.
  - Phase 43 locks demo-v2 artifact contracts, typed-alpha schema versions, forbidden-output guard coverage, and release-candidate notes.
  - Phase 44 Demo v2 Golden Snapshot Tests is now complete.
  - Phase 45 Typed Alpha Closeout Report is now complete.
  - Phase 46 Dashboard Readability Polish is now complete.
  - Current release-candidate hardening status is complete.
  - forbidden next work remains new alpha research, live data refreshes, broker/order paths, production approval language, fake Q2 metrics, and optimizer retuning.
- Phase 47 Typed Q2 Execution Adapter v0 is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_adapter_schema.py` defines local adapter input, matrix row, result, robustness summary, and manifest contracts.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_portfolioos_adapter.py` validates typed Q2 input artifacts, rejects forbidden output keys, preserves unavailable rows, and maps stable local PortfolioOS period-attribution rows into observed typed Q2 adapter rows.
  - `projects/execution_aware_optimizer/fixtures/typed_q2/` provides the synthetic typed-alpha fixture.
  - `scripts/run_typed_q2_adapter_fixture.py` and `make typed-q2-adapter-fixture` write local ignored artifacts under `outputs/typed_q2_adapter_fixture/`.
  - The opt-in smoke path now returns `adapter_status=observed`, `observed_rows=30`, and `unavailable_rows=0` against the existing local backtest fixture after mapping the Q2 `risk_controlled` layer to the stable PortfolioOS `naive_pro_rata` strategy.
  - The adapter confirms no live data, no orders, and no broker path; it does not claim production alpha approval.
  - Known limitation: adapter v0 validates and records the projected expected-return panel, but it does not inject that panel into a new PortfolioOS optimizer path. Observed rows are only the metrics exposed by existing local fixture period attribution.
  - validation: Q2 project tests `58 passed`; typed Q2 adapter smoke passed with `adapter_status=observed`; `make typed-q2-adapter-fixture` passed with `observed_rows=30`.
- Final finite Phase 48-66 roadmap has been added:
  - Phase 48 Typed Expected-Return Injection Fixture is now complete.
  - Phase 49 Typed Optimizer Response Acceptance Suite is now complete.
  - Phase 50 SUE Typed Q2 Survival Matrix v1 is now complete.
  - Phase 51 SUE Execution-Survival Attribution Report is now complete.
  - Phase 52 Revision Marginal-Value Gate is now complete.
  - Phase 55 Alpha Registry v2 / Decision State Machine is now complete.
  - Phase 65 PortfolioOS v1 Research-Audit Release Hygiene is now complete. Phase 56-58 paper-stage preparation is technically unblocked by a clean local SUE Q2 fixture, but it remains optional and must not start without an explicit decision.
  - Phase 66 Maintenance Freeze / Future-Only Backlog is now complete.
  - Phase 66 is the final freeze phase. No automatic roadmap expansion after Phase 66.
  - New work requires explicit reopen decision. Existing unrelated Multifactor / Factor Discovery working-tree changes are not part of the v1 freeze.
  - Phase 48-54 are the required typed-alpha to local Q2 closeout path.
  - Phase 55 freezes alpha decisions into Alpha Registry v2.
  - Phase 56-58 are optional paper-stage preparation and governance only.
  - Phase 59-61 are locked by default and require explicit human approval.
  - Phase 62-64 are research-reopen controls, not automatic research branches.
  - Phase 65-66 package and freeze the research-audit release.
  - No automatic roadmap expansion is allowed after Phase 66.
  - Reopen-O1 Typed Alpha Optimizer Input Bridge v1 is now complete as an explicit reopen task, not an automatic Phase 67:
    - `src/portfolio_os/alpha/optimizer_input_bridge.py` injects typed expected-return panels into optimizer universe frames behind `allow_typed_alpha_optimizer_injection`, keeps no_view rows explicitly labeled, and uses objective-neutral solver fill only as a recorded optimizer API adaptation.
    - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_optimizer_input_bridge.py` runs the expanded deterministic SUE panel through actual local `run_rebalance` calls and emits Q2 rows based on optimizer outputs, not existing adapter-hook attribution mapping.
    - `make sue-optimizer-input-bridge-fixture` returns `bridge_status=observed`, `expected_return_reached_actual_optimizer_input=True`, `optimizer_decision_used_typed_expected_return=True`, `sign_flip_reversal_observed=True`, `scaled_alpha_monotonicity_observed=True`, `no_view_not_encoded_as_zero=True`, `actual_optimizer_output_rows=7`, and `adapter_hook_only=False`.
    - Known limitation: this proves local optimizer-path integration only. It does not prove real historical SUE alpha, paper readiness, live readiness, or production approval.
  - Reopen-H1A WRDS PIT-Safe SUE Event Panel Builder is now complete as an explicit historical-evidence panel-building reopen task:
    - `src/portfolio_os/alpha/sue_historical_schema.py` defines PIT-labeled SUE event rows and guards against timestamp violations, missing estimates being encoded as zero SUE, forward-return feature leakage, FMP frozen estimate history as PIT-safe substitute, and misleading approval language.
    - `src/portfolio_os/alpha/sue_historical_panel.py` builds smoke-mode WRDS/IBES/CRSP-style artifacts and supports full mode from local earnings, estimate, security-link, and CRSP daily extracts with PIT estimate filtering and sdates-aware link validity.
    - `scripts/build_wrds_sue_event_panel.py --mode smoke` writes `events.csv`, `sue_values.csv`, PIT visibility, linkage, coverage, lineage, and report artifacts under `outputs/sue_historical_event_panel/` plus `reports/sue_historical_event_panel_report.md`.
    - Smoke validation returned `event_count=60`, `rebalance_date_count=60`, `linked_rows=57`, `unlinked_rows=3`, `missing_estimates=3`, `missing_actuals=3`, `missing_prices=2`, and `diagnostic_only_rows=11`.
    - Known limitation: H1A builds and audits the panel only. It does not prove SUE alpha success, run typed event evidence/Q2/optimizer evaluation, approve paper/live trading, or update Alpha Registry.
  - Reopen-H1A.1 WRDS Full Extract Run + Panel Audit is now complete as a full-mode local extract audit path:
    - `configs/wrds_sue_event_panel_full.yaml` defines local IBES actuals, IBES estimate snapshot, IBES link, and CRSP daily extract paths plus full-mode output/report destinations.
    - `scripts/build_wrds_sue_event_panel.py --config configs/wrds_sue_event_panel_full.yaml` writes a structured unavailable report when required local WRDS extracts are absent, and writes full panel artifacts when the local cache is present.
    - Current workspace WRDS cache status: bounded 2020-2021 local extracts produced `event_count=5000`, `rebalance_date_count=77`, `linked_rows=3210`, `unlinked_rows=1790`, `missing_estimates=700`, `missing_actuals=730`, `missing_prices=4990`, `diagnostic_only_rows=4990`, and `final_pit_safe_rows=10`.
    - Known limitation: the current real WRDS-backed panel is a bounded audit run with intentionally sampled CRSP price coverage. It does not prove SUE alpha success, run typed event evidence/Q2/optimizer evaluation, approve paper/live trading, or update Alpha Registry.
  - Reopen-H1A.2 CRSP Price Coverage Expansion / Resumable Extract is now complete as a narrow coverage-expansion path:
    - `scripts/extract_wrds_sue_crsp_prices.py` and `configs/wrds_sue_crsp_price_extract.yaml` extract CRSP daily rows for linked SUE PERMNOs into ignored local cache chunks and merge `data/cache/wrds_sue_event_panel/crsp_daily.csv`.
    - The staged WRDS runs advanced coverage from 120 to 500, then 1,000, then 1,250 linked PERMNO chunks; resumable reruns skipped completed chunks and only queried missing chunks.
    - The current merged ignored CRSP daily cache has 665,875 rows for 1,250 linked PERMNO chunks.
    - Re-running the full panel audit returned `missing_prices=3772`, `diagnostic_only_rows=3831`, and `final_pit_safe_rows=1169`, up from 10. The PIT-safe rows cover 70 rebalance dates with median 11 safe names per date.
    - Known limitation: this is now broad enough for a bounded H1B historical SUE event evidence grid, but it remains a coverage audit only; it is not Q2 evaluation, paper readiness, live readiness, or production approval.
  - Reopen-H1B Bounded Historical SUE Event Evidence Grid is now complete:
    - `src/portfolio_os/alpha/sue_historical_event_evidence.py`, `scripts/run_sue_historical_event_evidence_grid.py`, and `configs/sue_historical_event_evidence_grid.yaml` compute event-window evidence on the bounded WRDS/PIT-safe sample.
    - `make sue-historical-event-evidence-grid` writes `event_window_grid.csv`, `rank_ic_by_date.csv`, `top_bottom_spread_by_date.csv`, `placebo_report.json`, monthly/yearly coverage, PIT/leakage audit, summary JSON, and `reports/sue_historical_event_evidence_report.md`.
    - Current result: `interpretation=sue_bounded_evidence_mixed`, `pit_safe_rows=1169`, `safe_rebalance_dates=70`, best window `plus_2_plus_3`, mean Rank IC `0.067034`, Rank IC t-stat `1.468260`, mean top-bottom spread `0.019819`.
    - Placebo diagnostics are generated for event-date shift, sign-flip SUE, and randomized SUE. Missing coverage remains explicit unavailable/no_view and is not encoded as zero alpha.
    - Known limitation: this is bounded historical event-window evidence only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove full historical SUE alpha.
  - Reopen-H1C WRDS SUE Panel Expansion and Coverage Rescue is now complete:
    - `configs/wrds_sue_event_panel_expanded.yaml` and `scripts/build_wrds_sue_event_panel_expanded.py` refresh expanded WRDS `statsum`-derived actual/estimate inputs into ignored local cache and write expanded panel artifacts under `outputs/sue_historical_event_panel_expanded/`.
    - The expanded panel uses `2020-01-01` to `2020-12-31`, `max_events=30000`, existing IBES/CRSP links, and the ignored CRSP daily cache.
    - Current expanded panel metrics: `event_count=17027`, `rebalance_date_count=255`, `linked_rows=11250`, `unlinked_rows=5777`, `missing_expected_eps=0`, `missing_actual_eps=0`, `missing_prices=6815`, `diagnostic_only_rows=12592`, and `final_pit_safe_rows=4435`.
    - Coverage rescue artifacts include `coverage_rescue_report.json`, `linkage_failure_report.csv`, `missing_price_report.csv`, PIT visibility, and `reports/sue_historical_event_panel_expansion_report.md`.
    - Re-running the evidence grid on expanded outputs returned `interpretation=sue_expanded_evidence_mixed`, `pit_safe_rows=4435`, `safe_rebalance_dates=226`, best window `plus_2_plus_22`, mean Rank IC `0.004475`, Rank IC t-stat `0.147701`, and mean top-bottom spread `0.000665`.
    - Interpretation: the bounded early `[+2,+3]` result did not strengthen under broader 2020 coverage. The correct next action is coverage/linkage/price diagnosis and possibly more data expansion, not Q2 or optimizer-path evaluation.
    - Known limitation: H1C is expanded historical event evidence and coverage rescue only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove SUE alpha.
  - Post-H1C coverage/linkage/price diagnostics are now available:
    - `src/portfolio_os/alpha/sue_coverage_diagnostics.py`, `scripts/run_sue_coverage_linkage_price_diagnostics.py`, `configs/sue_coverage_linkage_price_diagnostics.yaml`, and `make sue-coverage-linkage-price-diagnostics` read the expanded SUE panel, coverage rescue report, linkage failure report, missing price report, and ignored CRSP daily cache.
    - Current diagnostic result: `event_count=17027`, `final_pit_safe_rows=4435`, `unlinked_ibes_crsp_rows=5777`, `missing_price_rows=6815`, `missing_return_windows=12592`, `crsp_cache_rows=665875`, `crsp_cache_permnos=1244`, and `pit_safe_share=0.26046867`.
    - Price gap classification is dominated by `permno_absent_from_crsp_cache=6814`, with `return_window_after_crsp_cache_end=1`; the recommended next action is `rescue_linkage_and_price_coverage_before_q2`.
    - This is data coverage diagnosis only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove SUE alpha.
  - Post-H1C data coverage rescue has now expanded both price and linkage coverage:
    - CRSP price extraction now uses `chunk_size=25`, `max_permnos=null`, and the rescued link cache, with ignored chunks under `data/cache/wrds_sue_event_panel/crsp_daily_chunks_linkage_rescue/`.
    - `make sue-historical-linkage-rescue` exact-CUSIP matched expanded linkage failures against `crsp.stocknames`, rescued `4098` failed event rows across `1302` symbols and `1303` PERMNOs, and wrote `data/cache/wrds_sue_event_panel/ibes_links_rescued.csv`. The target is now idempotent: after the panel is rebuilt, rerunning it preserves `1391` exact-CUSIP rescue link rows and reports `rescued_event_rows=0` for the remaining unresolved linkage failures.
    - The refreshed CRSP cache now has `distinct_permnos=4373` and `row_count=2325833`.
    - The rebuilt expanded panel now has `event_count=17027`, `final_pit_safe_rows=15365`, `unlinked_ibes_crsp_rows=1661`, `missing_price_rows=1`, `missing_return_windows=1662`, and `pit_safe_share=0.90239032`.
    - The rerun evidence grid remains `sue_expanded_evidence_mixed`; best window is `plus_2_plus_22` with `mean_rank_ic=0.039434`, `rank_ic_t_stat=1.948866`, and `mean_top_bottom_spread=-0.006599`.
    - This is coverage rescue and event evidence only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove SUE alpha.
  - Reopen-H1D SUE Score Definition Diagnostics is complete as an explicit diagnostic reopen task:
    - `src/portfolio_os/alpha/sue_score_definition_diagnostics.py`, `scripts/run_sue_score_definition_diagnostics.py`, `configs/sue_score_definition_diagnostics.yaml`, and `make sue-score-definition-diagnostics` compare raw EPS difference against expected-EPS, actual-EPS, winsorized, and price-scaled score definitions.
    - The current H1D run returns `preferred_diagnostic_score=surprise_pct_actual_eps`, `raw_eps_diff_scale_warning=true`, and `diagnostic_interpretation=sue_raw_eps_diff_not_preferred`. For `[+2,+22]`, raw EPS diff has `mean_rank_ic=0.039434` but `mean_top_bottom_spread=-0.006599`; `surprise_pct_actual_eps` has `mean_rank_ic=0.057052` and `mean_top_bottom_spread=0.022334`.
    - The diagnostic explains why current expanded evidence can show positive Rank IC but negative raw top-bottom spread: raw EPS differences are scale-sensitive, while scale-aware SUE definitions are the appropriate next diagnostic candidate.
    - Missing SUE, price, or return coverage remains unavailable/no_view and is not encoded as zero alpha.
    - Known limitation: H1D is score-definition diagnosis only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove SUE alpha.
  - Reopen-H1E Scale-Aware SUE Score Definition Gate is complete as an explicit score-definition gate:
    - `src/portfolio_os/alpha/sue_score_definition_gate.py`, `scripts/run_sue_score_definition_gate.py`, `configs/sue_score_definition_gate.yaml`, and `make sue-score-definition-gate` preregister candidate SUE definitions, set raw EPS difference to diagnostic-only, and evaluate denominator, winsorization, placebo, breadth, and tail-concentration guards.
    - The current run returns `interpretation=scale_aware_sue_mixed`, `provisional_score=surprise_pct_actual_eps`, `selected_score=null`, `placebo_passed=false`, and `event_date_shift_passed=false`. `surprise_pct_actual_eps` improves the primary `[+2,+22]` live read (`mean_rank_ic=0.053752`, `mean_top_bottom_spread=0.020775`) but is not selected because the event-date-shift placebo is stronger.
    - Required artifacts are written under `outputs/sue_score_definition_gate/`: `score_registry.yaml`, `score_grid.csv`, `window_metrics.csv`, `placebo_report.json`, `denominator_guard_report.json`, `tail_concentration_report.json`, size/liquidity and sector exposure reports, and `score_selection_summary.json`; the report lives at `reports/sue_score_definition_gate_report.md`.
    - Missing denominator/price/return coverage remains unavailable/no_view and is not encoded as zero alpha.
    - Known limitation: H1E is score-definition gating only. It does not run Q2, optimizer-path evaluation, Alpha Registry promotion, paper/live trading, broker/order workflows, or production approval, and it does not prove SUE alpha.
  - Reopen-H1E.1 Event-Date-Shift Placebo Failure Attribution is available as a diagnostic-only reopen task:
    - `src/portfolio_os/alpha/sue_placebo_failure_attribution.py`, `scripts/run_sue_placebo_failure_attribution.py`, `configs/sue_placebo_failure_attribution.yaml`, and `make sue-placebo-failure-attribution` decompose the H1E event-date-shift placebo failure.
    - The diagnostic compares live and shifted anchors across `[+2,+2]`, `[+2,+3]`, and `[+2,+22]`; writes timing-shift, month, sector, size/liquidity, return-window overlap, market-adjustment, denominator/tail, regime concentration, and attribution-summary artifacts under `outputs/sue_placebo_failure_attribution/`; and writes `reports/sue_placebo_failure_attribution_report.md`.
    - It does not select a score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, or approve production use.
    - Missing denominator/price/return coverage remains unavailable/no_view and is not encoded as zero alpha.
  - Reopen-H1E.2 Market-Regime Placebo Filter Check is available as a narrow follow-up diagnostic:
    - `src/portfolio_os/alpha/sue_regime_filter_placebo_check.py`, `scripts/run_sue_regime_filter_placebo_check.py`, `configs/sue_regime_filter_placebo_check.yaml`, and `make sue-regime-filter-placebo-check` rerun filtered score-gate summaries and placebo curves after excluding March 2020, high-volatility weeks, low-liquidity weeks, and their union.
    - The diagnostic writes `score_gate_summary.csv`, `filtered_placebo_shift_curve.csv`, `regime_week_classification.csv`, `regime_filter_summary.json`, and `reports/sue_regime_filter_placebo_check_report.md`.
    - Current run returns `interpretation=market_regime_filter_reduces_but_does_not_resolve_placebo_failure`. Baseline placebo advantage on `[+2,+22]` is `0.0689167`; the best reduction comes from excluding high-volatility weeks, reducing the advantage by only `0.0059384`, and every filtered slice still has `event_date_shift_passed=false`.
    - The local CRSP cache lacks volume, so low-liquidity filtering uses `price_observation_count_proxy_missing_volume` and excludes only `9` events in the current sample.
    - It validates the H1E.1 market-regime attribution only. It does not select a score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, or approve production use.
  - Reopen-H1E.3 SUE Event Timing / Anchor Definition Audit is complete as a narrow timing diagnostic:
    - `src/portfolio_os/alpha/sue_event_timing_anchor_audit.py`, `scripts/run_sue_event_timing_anchor_audit.py`, `configs/sue_event_timing_anchor_audit.yaml`, and `make sue-event-timing-anchor-audit` compare current tradable, announcement-date, shifted, and pre-event drift windows.
    - The diagnostic writes `anchor_grid.csv`, `pre_event_drift_grid.csv`, `timing_quality_breakdown.csv`, `window_overlap_audit.json`, `market_timing_audit.csv`, `anchor_selection_diagnostic.json`, and `reports/sue_event_timing_anchor_audit_report.md`.
    - Current run returns `interpretation=anchor_definition_likely_late`, `best_anchor_definition=shift_minus_5_td`, and `best_pre_event_window=minus_5_minus_1`. For `[+2,+22]`, current-tradable mean Rank IC is `0.055597` and mean top-bottom spread is `0.020484`; shifted-minus-5 mean Rank IC is `0.191015` and mean top-bottom spread is `0.089401`.
    - Window construction checks pass: shifted anchors change return windows, no event-available-after-tradable violations are present, no estimate-after-event-available violations are present, and missing coverage remains unavailable/no_view rather than zero alpha.
    - It is an event-timing audit only. It does not select a score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, or approve production use.
  - Reopen-H1E.4 SUE Announcement Timestamp Source / Anchor Policy Audit is complete as a source-proof gate:
    - `src/portfolio_os/alpha/sue_announcement_timestamp_policy.py`, `scripts/run_sue_announcement_timestamp_policy_audit.py`, `configs/sue_announcement_timestamp_policy.yaml`, and `make sue-announcement-timestamp-policy-audit` compare current IBES/WRDS announcement timing against optional actual-EPS source timestamps, report dates, Compustat RDQ, and announcement-time fields.
    - The diagnostic writes `timestamp_source_comparison.csv`, `timing_repair_eligibility.csv`, `anchor_policy_grid.csv`, `repaired_h1e_summary.json`, `timing_policy_decision.json`, and `reports/sue_announcement_timestamp_policy_report.md`.
    - Current run returns `decision_label=no_auditable_earlier_timestamp_sue_blocked`, `event_count=17027`, `auditable_source_event_count=0`, `repaired_event_count=0`, and `selected_score=null`. H1E was not rerun because no source-repaired event set exists.
    - It blocks -5/-10 anchor shifts unless actual EPS availability is proven earlier and keeps SUE blocked before typed projection/Q2 if no auditable earlier source exists.
    - It does not select a score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, implement Factor Discovery, or approve production use.
  - Reopen-H1E.5 SUE Timestamp Data Acquisition / Source Enrichment is complete as a local source-enrichment layer:
    - `src/portfolio_os/alpha/sue_timestamp_source_extract.py`, `scripts/extract_wrds_sue_timestamp_sources.py`, `configs/wrds_sue_timestamp_sources.yaml`, and `make sue-timestamp-source-extract` pull local WRDS timestamp-source cache files for H1E.5.
    - `src/portfolio_os/alpha/sue_timestamp_enrichment.py`, `scripts/run_sue_timestamp_enrichment.py`, `configs/sue_timestamp_enrichment.yaml`, and `make sue-timestamp-enrichment` enrich SUE events with local IBES `anndats_act`, Compustat `rdq`, exact release timestamps, and SEC filing timestamps.
    - The diagnostic writes `timestamp_source_comparison.csv`, `source_coverage_report.json`, `date_disagreement_report.csv`, `repairable_event_candidates.csv`, `nonrepairable_event_report.csv`, `timestamp_enrichment_decision.json`, and `reports/sue_timestamp_enrichment_report.md`.
    - Current WRDS source extract returns `event_count=17027`, `ibes_actuals_matched_events=17027`, `compustat_rdq_matched_events=12987`, and no Q2/optimizer/production flags.
    - Current enrichment run returns `decision_label=timestamp_enrichment_no_repair_sue_blocked`, `event_count=17027`, `repairable_event_count=0`, `ibes_anndats_act_count=17027`, `compustat_rdq_count=12987`, `exact_release_timestamp_count=0`, and `sec_filing_timestamp_count=0`.
    - Date-only IBES/Compustat fields are audit evidence but not tradable timestamp repairs. Exact release timestamps can create repair candidates for later review, but H1E is not rerun in this phase.
    - It does not select a score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, implement Factor Discovery, or approve production use.
- Factor Discovery Sandbox has been added only as a Phase 64 candidate charter,
  not as active Phase 65 implementation:
  - Proposed future path:
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/`.
  - It asks "Which candidate factors deserve typed validation?", not "What is
    the formal alpha strategy?"
  - Teaching mode may replicate the NASDAQ100 29-factor IC/ICIR rotation
    baseline with QQQ benchmark, but every artifact must be labeled
    `survivorship_biased=true`, `educational_only=true`, and
    `not_alpha_evidence=true`.
  - Research mode requires historical universe handling, timestamp contracts,
    rolling out-of-sample ICIR, train/validation/test boundaries, FactorSpecs,
    redundancy/marginal-value gates, shrinkage, zero-weight attribution,
    cost/capacity stress, and QQQ-relative benchmark attribution.
  - Sandbox outputs must enter through Phase 64 import review before Q1
    evidence, Promotion Gate, Q2 execution survival, or Alpha Registry updates.
  - It must not run live broker, paid API, external data refresh, or yfinance
    workflows unless explicitly approved for an opt-in sandbox run.
- FD-1 Teaching Baseline Replication is complete:
  - `projects/multifactor_alpha_validation/factor_discovery_sandbox/src/factor_discovery_sandbox/teaching_baseline.py`
    builds a deterministic current-constituent style teaching fixture with 29
    price-volume factors.
  - `scripts/run_factor_discovery_teaching_baseline.py` and
    `make factor-discovery-teaching-baseline` write ignored artifacts under
    `outputs/factor_discovery/teaching_mode/`.
  - Artifacts include `nasdaq100_factor_table.csv`,
    `qqq_benchmark_report.csv`, `factor_ic_table.csv`,
    `factor_correlation_matrix.csv`, `icir_weight_table.csv`, and
    `teaching_backtest_report.md`.
  - The report and result summary explicitly set survivorship-biased,
    educational-only, not-alpha-evidence, no-network, and no-production-approval
    status.
  - Validation: the FD-1 focused test passed; `make
    factor-discovery-teaching-baseline` passed.
- FD-2 FactorSpec Conversion is complete:
  - `factor_discovery_sandbox/factor_specs.py` writes the 29 price-volume specs
    from the same factor list used by FD-1.
  - FD-D0 Factor Design Layer is now installed before formula validation:
    `factor_discovery_sandbox/factor_design.py`,
    `scripts/run_factor_discovery_design_layer.py`, `make
    factor-discovery-design-layer`, and
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/docs/factor_design_layer_spec.md`
    define the mandatory design contract. Every candidate must state the
    market pain point, mechanism hypothesis, investor constraint or behavior,
    expected universe/regime, why the pattern is not arbitraged away,
    observable pre-formula diagnostics, formula measurement role, placebo
    design, cost/capacity risks, and expected failure modes before FD
    validation. Current artifacts are
    `outputs/factor_discovery/design_layer/factor_design_contract_validation.json`
    and `reports/factor_discovery_design_layer_report.md`.
  - Committed specs live under
    `projects/multifactor_alpha_validation/factor_discovery_sandbox/factor_specs/price_volume_29/`.
  - `scripts/run_factor_discovery_factor_specs.py` and
    `make factor-discovery-factor-specs` write specs plus ignored
    `outputs/factor_discovery/factor_spec_validation.json`.
  - Every spec records mechanism, lookback, skip, direction, timestamp contract,
    expected horizon, correlation family, failure mode, explicit abstain
    coverage rule, FD-D0 design contract, and `no_view_is_not_zero_alpha=true`.
  - Validation: FD-2 focused test passed; `make
    factor-discovery-design-layer` passed; `make factor-discovery-factor-specs`
    passed with `factor_count=29` and `all_specs_valid=true`.
  - FD-D0 is also enforced for standalone candidate-family runners:
    `write_candidate_design_manifest` now writes
    `candidate_design_manifest.json` before validation for
    `small_cap_quality_residual_momentum_6m_ex1m`,
    `momentum_12m_ex1m_low_vol_3m`, and
    `revision_confirmed_earnings_underreaction`. These manifests record the
    market pain point, mechanism hypothesis, investor constraint or behavior,
    expected universe/regime, observable pre-formula diagnostics, placebo
    design, cost/capacity risks, and expected failure modes. Missing or invalid
    candidate design contracts block validation rather than allowing formula
    mining.
  - FD-D0 now has an FD-wide output audit:
    `factor_discovery_sandbox/fd_wide_design_audit.py`,
    `scripts/run_factor_discovery_fd_wide_design_audit.py`, and
    `make factor-discovery-fd-wide-design-audit` scan candidate output
    directories for candidate/family summary or decision artifacts and require
    a valid same-directory `candidate_design_manifest.json`.
    Current local scan found `candidate_directory_count=3`,
    `valid_manifest_count=3`, `blocker_count=0`, and
    `decision=all_candidate_design_manifests_valid`. The audit remains
    not-alpha-evidence and blocks allocator, Q1/Q2, Alpha Registry, broker/live
    trading, and production approval paths.
    Validation: focused FD-wide audit tests passed (`4 passed`); full FD
    sandbox tests passed (`99 passed`); `make
    factor-discovery-fd-wide-design-audit` passed; `git diff --check` passed;
    `make validate` passed.
- FD-D1 Factor Pain-Point Map is complete:
  - `factor_discovery_sandbox/factor_design_d1.py`,
    `scripts/run_factor_discovery_design_d1.py`, and
    `make factor-discovery-design-d1` write the mechanism-first planning
    artifacts under `outputs/factor_discovery/design_layer/d1/`.
  - Artifacts: `factor_pain_point_map.md`, `factor_design_ledger.csv`,
    `candidate_family_backlog.json`, and `factor_design_d1_summary.json`.
  - Current FD-D1 run: `ledger_row_count=6`, `candidate_family_count=6`,
    `ledger_valid=true`, `design_layer_required_before_formula=true`,
    `formula_first_candidates_blocked=true`, `not_alpha_evidence=true`, and
    `direct_q2_entry_allowed=false`.
  - The ledger reframes existing formula-first diagnostics as prior history:
    `momentum_12m_ex1m_low_vol_3m` remains
    `mixed_initial_diagnostic_gate`,
    `small_cap_quality_residual_momentum_v1` remains
    `reject_placebo_failure`,
    `revision_confirmed_earnings_underreaction` remains
    `insufficient_support`, and SUE timing remains blocked by timestamp-source
    observability. None are Q1 candidate-review eligible from FD-D1.
  - New design-backlog rows include within-sector residual underreaction and
    liquidity/activity attention-vs-capacity confusion. These are design
    backlog entries only, not validated alpha candidates.
  - Validation: focused FD-D1/D0/FactorSpec tests passed; `make
    factor-discovery-design-d1` passed; `git diff --check` passed.
- FD-D2 Pre-Formula Diagnostics is complete:
  - `factor_discovery_sandbox/factor_design_d2.py`,
    `scripts/run_factor_discovery_design_d2.py`, and
    `make factor-discovery-design-d2` write pre-formula diagnostics under
    `outputs/factor_discovery/design_layer/d2/`.
  - Artifacts: `pre_formula_diagnostics.csv`,
    `candidate_family_d2_decisions.json`,
    `pre_formula_diagnostic_summary.json`, and
    `pre_formula_diagnostic_report.md`.
  - Current FD-D2 run: `diagnostic_row_count=6`, `ready_for_d3_count=1`,
    `formula_validation_allowed_count=0`, `diagnostics_valid=true`,
    `formula_validation_ran=false`, `not_alpha_evidence=true`, and
    `direct_q2_entry_allowed=false`.
  - D2 decisions:
    `revision_confirmed_earnings_underreaction=blocked_coverage_alignment`,
    `sue_event_timing_and_timestamp_repair=blocked_timestamp_observability`,
    `small_cap_quality_residual_momentum_v1=blocked_placebo_prior`,
    `momentum_12m_ex1m_low_vol_3m=diagnostic_only_prior_mixed`,
    `liquidity_activity_shock=needs_pre_formula_data_diagnostics`, and
    `sector_neutral_residual_momentum=ready_for_d3_charter`.
  - No FD-D2 row allows formula validation, Q1 candidate review, allocator,
    Q2, Alpha Registry, broker/order/live, or production approval. The only
    allowed next step is a D3 charter for the sector-neutral residual momentum
    pain point.
  - Validation: focused FD-D2 tests passed; `make
    factor-discovery-design-d2` passed; `git diff --check` passed.
- FD-3 Rolling ICIR and OOS Weighting is complete:
  - `factor_discovery_sandbox/rolling_oos.py` estimates factor ICIR weights for
    each rebalance date using only dates before that rebalance.
  - `scripts/run_factor_discovery_rolling_oos.py` and
    `make factor-discovery-rolling-oos` write `rolling_icir_weights.csv`,
    `oos_factor_score_panel.csv`, and `oos_backtest_report.md` under ignored
    `outputs/factor_discovery/research_mode/`.
  - OOS score rows carry signal, visibility, and tradable timestamps, with
    tradability after signal formation.
  - The report states full-sample ICIR is forbidden and keeps teaching-mode
    results separate from research-mode OOS results.
  - Validation: FD-3 focused test passed; `make
    factor-discovery-rolling-oos` passed with `uses_full_sample_icir=false`.
- FD-4 Redundancy / Marginal-Value Gate is complete:
  - `factor_discovery_sandbox/marginal_value.py` writes
    `factor_cluster_report.csv`, `residual_ic_report.csv`, and
    `marginal_value_decision_table.csv`.
  - Decisions are selected from `promote_to_allocator`, `real_but_redundant`,
    `archive_no_marginal_value`, `needs_more_evidence`, and `diagnostic_only`.
  - The gate records residual contribution, incremental spread, incremental net
    return, incremental turnover, and cost drag for every factor.
  - High-correlation factors cannot be kept by standalone ICIR alone.
  - Validation: FD-4 focused test passed; `make
    factor-discovery-marginal-value-gate` passed with
    `high_correlation_kept_by_icir_only=false`.
- FD-5 Shrinkage + Allocator + Zero-Weight Attribution is complete:
  - `factor_discovery_sandbox/allocator.py` writes `posterior_factor_mu.csv`,
    `factor_covariance_shrunk.csv`, `allocator_weights.csv`, and
    `zero_weight_attribution.csv`.
  - Posterior means are shrunk toward zero; covariance is stabilized from the
    local factor correlation panel; allocator weights are nonnegative and
    normalized.
  - Every zero-weight factor receives one of the allowed reasons:
    `low_posterior_alpha`, `high_redundancy`, `cluster_dominated`,
    `high_turnover`, `high_cost_drag`, `capacity_limited`, `no_view`, or
    `insufficient_evidence`.
  - The allocator report path records sign-flip and scale-response sanity
    checks and does not claim production strategy status.
  - Validation: FD-5 focused test passed; `make factor-discovery-allocator`
    passed with sign-flip and scale-response checks true.
- FD-6 Cost / Capacity / Benchmark Survival is complete:
  - `factor_discovery_sandbox/survival.py` writes `cost_stress_matrix.csv`,
    `capacity_frontier.csv`, `benchmark_attribution.csv`,
    `survival_funnel.csv`, `final_factor_discovery_report.md`, and
    `research_import_bundle.json` under ignored
    `outputs/factor_discovery/research_mode/`.
  - The report compares raw, QQQ-relative, beta-adjusted, cost-adjusted, and
    capacity-adjusted results and explicitly attributes sector, style, and
    liquidity exposure.
  - The local fixture flags tech concentration risk and keeps the recommended
    import decision at `import_as_calibration_only`.
  - The research import bundle sets `direct_q2_entry_allowed=false` and does
    not claim production approval.
  - Validation: FD-6 focused test passed; `make factor-discovery-survival`
    passed with `recommended_import_decision=import_as_calibration_only`.
- FD-R0/R1/R2 Real-Data Validation Line is now started under the FD sandbox:
  - `factor_discovery_sandbox/real_data_validation.py` consumes the local WRDS
    monthly PIT manifest and writes FD data admission, PIT universe, symbol
    mapping, survivorship, returns, benchmark, and corporate-action audit
    artifacts under ignored `outputs/factor_discovery/real_data/`.
  - `scripts/run_factor_discovery_real_data_validation.py` and
    `make factor-discovery-real-data-validation` run the new FD real-data
    admission/universe/return audit path.
  - `factor_discovery_sandbox/docs/real_data_validation_roadmap.md` records the
    new chain:
    `FD sandbox fixture line -> FD real-data validation line -> Phase 64 import
    review -> Q1 / Promotion Gate / Q2`.
  - Current WRDS monthly PIT run returned
    `admission_status=admitted_for_monthly_pit_r0_r2`,
    `full_daily_price_volume_ready=false`, `factor_ranking_ran=false`,
    `allocator_ran=false`, and `alpha_success_claimed=false`.
  - The data quality summary passes historical constituents, adjusted prices,
    volume, QQQ benchmark, delisting records, and timestamp policy, but warns
    that raw prices, sector/style exposures, and daily price-volume are absent.
  - This is not alpha evidence, not factor ranking, not an allocator output, not
    production approval, and not Q2 entry.
  - Missing daily/raw/sector data was then pulled directly from WRDS into the
    ignored local cache
    `data/cache/wrds_multifactor/nasdaq100_daily_full10/`. The completed daily
    bundle has 827,439 adjusted daily price-volume rows with raw open/close,
    4,529 QQQ benchmark rows, 272 historical membership rows with Compustat
    sector/industry metadata, and 55 delisting rows. Preflight returned
    `blocker_count=0`.
  - `make factor-discovery-real-data-validation-daily` writes
    `outputs/factor_discovery/real_data_daily/` and returns
    `admission_status=admitted_for_daily_pit_r0_r2`,
    `full_daily_price_volume_ready=true`, `warning_count=0`,
    `factor_ranking_ran=false`, `allocator_ran=false`, and
    `alpha_success_claimed=false`.
  - `projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_price_volume_long_task.yaml`
    now records `status=completed_local_wrds_pull` and points to the completed
    daily manifest. The data cache is ignored and must not be committed.
- FD-R3 Real Factor Replay is complete on the admitted WRDS daily PIT bundle
  and has been rerun with `price_volume_29_mechanism_v2`:
  - `factor_discovery_sandbox/factor_formulas_v2.py` and
    `factor_discovery_sandbox/factor_formula_registry.py` define the stable v2
    formula registry while preserving all 29 factor IDs.
  - The duplicate-prone factors now use distinct mechanisms:
    sector-neutral `residual_momentum_6m`, OLS trend-quality
    `trend_slope_3m/6m`, path max-drawdown severity, vol-adjusted overshoot
    reversal, liquidity shock versus capacity level, and turnover shock versus
    turnover trend persistence.
  - `factor_discovery_sandbox/real_factor_replay.py` writes raw values,
    oriented scores, cross-sectional ranks, formula hashes, mechanism families,
    fallback audit fields, factor coverage report, timestamp audit, replay
    report, summary JSON, and parquet-status JSON.
  - `scripts/run_factor_discovery_real_factor_replay.py` and
    `make factor-discovery-real-factor-replay` run the FD-R3 path against
    `data/cache/wrds_multifactor/nasdaq100_daily_full10/standardized/research_mode_dataset_manifest.yaml`.
  - The v2 real smoke produced
    `outputs/factor_discovery/real_data_daily/fd_r3/real_factor_panel.csv`
    with `row_count=615090`, `active_view_rows=600674`,
    `explicit_abstain_rows=14416`, `factor_count=29`,
    `formula_version=price_volume_29_mechanism_v2`, and
    `signal_date_count=215`.
  - Timestamp audit passed all 215 signal dates with
    `same_close_trading_used=false` and `timestamp_contract_status=passed`.
  - Explicit abstain semantics are preserved: abstain rows keep
    `normalized_value` missing, all no-view rows are marked
    `no_view_is_not_zero_alpha=true`, and FD-R3 does not convert missing
    coverage into zero alpha.
  - Parquet output is intentionally not written in this environment because
    `pyarrow`/`fastparquet` is not installed; the CSV panel is the source
    artifact and `parquet_status.json` records the reason.
  - FD-R3 remains not alpha evidence. It does not run rolling OOS, factor
    ranking, allocator weights, production approval, Q2 entry, or Alpha
    Registry import.
  - Validation: v2 formula tests, FactorSpec tests, focused FD-R3 tests, and
    `make factor-discovery-factor-specs` passed; `make
    factor-discovery-real-factor-replay` passed on the real daily PIT bundle.
- FD-R4 True Rolling OOS Validation is complete on the FD-R3 real-data panel:
  - `factor_discovery_sandbox/oos_splitter.py` defines rolling
    train/validation/test splits without future dates.
  - `factor_discovery_sandbox/real_rolling_oos.py` builds 1m/3m forward
    QQQ-relative targets, computes cross-sectional rank IC, estimates rolling
    ICIR weights using only prior returns whose target window is already
    visible, scores each rebalance, and writes OOS decile diagnostics.
  - `scripts/run_factor_discovery_real_rolling_oos.py` and
    `make factor-discovery-real-rolling-oos` run the FD-R4 path against the
    FD-R3 panel.
  - The real smoke wrote
    `outputs/factor_discovery/real_data_daily/fd_r4/rolling_icir_real.csv`,
    `oos_factor_score_panel_real.csv`, `oos_decile_spread_real.csv`,
    `oos_validation_report.md`, and `oos_validation_summary.json`.
  - Real smoke summary: `rebalance_count=176`,
    `validation_rebalance_count=12`, `test_rebalance_count=164`,
    `score_row_count=35322`, `decile_row_count=352`, `factor_count=29`,
    `horizons_months=[1,3]`.
  - Leakage checks passed in smoke: `uses_full_sample_icir=false`,
    `future_universe_used=false`, `future_normalization_used=false`,
    `post_period_factor_selection_used=false`; non-empty estimation windows and
    return visibility cutoffs are all before each rebalance date.
  - Current v2 real-data diagnostics remain weak / negative on average:
    test 1m mean Rank IC `-0.012462`, test 1m mean top-bottom spread
    `-0.003923`, test 3m mean Rank IC `-0.021133`, and test 3m mean
    top-bottom spread `-0.020784`.
  - FD-R4 remains not alpha evidence. It does not run allocator weights,
    production approval, Q2 entry, or Alpha Registry import.
  - Validation: focused FD-R4 test passed; `make
    factor-discovery-real-rolling-oos` passed on the real daily PIT bundle.
- FD-R5 Placebo / Robustness / Family Test is complete on the FD-R4 score
  panel:
  - `factor_discovery_sandbox/factor_placebo.py` runs shuffled cross-section,
    lagged-signal, random same-coverage, sector-neutral, rebalance-date-shifted,
    and future-return leakage negative-control diagnostics.
  - `factor_discovery_sandbox/robustness_suite.py` builds period and benchmark
    regime robustness summaries.
  - `factor_discovery_sandbox/family_diagnostics.py` writes factor-family
    coverage diagnostics from the FD-R3 panel.
  - `scripts/run_factor_discovery_real_placebo_robustness.py` and
    `make factor-discovery-real-placebo-robustness` run the FD-R5 path.
  - Real smoke output lives under
    `outputs/factor_discovery/real_data_daily/fd_r5/` and includes
    `placebo_report.csv`, `robustness_by_period.csv`,
    `robustness_by_regime.csv`, `factor_family_diagnostics.md`, and
    `placebo_robustness_summary.json`.
  - Current real result: `placebo_test_count=7`,
    `placebo_status=failed_placebo_gate`,
    `recommended_next_action=stop_before_allocator`,
    `allocator_entry_allowed=false`, `direct_q2_entry_allowed=false`, and
    `alpha_success_claimed=false`.
  - Placebo comparison confirms the live OOS composite is not strong enough:
    live test 1m mean top-bottom spread `-0.003923` and test 3m
    `-0.020784`, while shuffled/random controls are not consistently worse.
    The future-return leakage negative control returns rank IC near `1.0`,
    confirming the diagnostic can identify an impossible leakage signal.
  - FD-R5 remains not alpha evidence and explicitly blocks allocator, Q1/Q2,
    production approval, and Alpha Registry import for this candidate set.
  - Validation: focused FD-R5 test passed; `make
    factor-discovery-real-placebo-robustness` passed on the real daily PIT
    bundle.
  - Recommended next FD action is to stop before FD-R6 for this 29-factor
    composite and revise candidate definitions/families before replaying
    FD-R3/FD-R4, not to proceed to allocator.
- FD-R5.1 Candidate Failure Diagnosis is complete on existing FD-R3/R4/R5
  artifacts and has been rerun after the v2 formula mechanism rewrite:
  - `factor_discovery_sandbox/failure_diagnosis.py` reads the FD-R3 factor
    panel, FD-R4 rolling ICIR weights and OOS score panel, and FD-R5 placebo
    report. It does not rewrite FactorSpecs, pull WRDS data, or rerun
    FD-R3/R4/R5.
  - `scripts/run_factor_discovery_real_failure_diagnosis.py` and
    `make factor-discovery-real-failure-diagnosis` write
    `outputs/factor_discovery/real_data_daily/fd_r5_1/`.
  - Artifacts: `standalone_factor_oos_diagnostics.csv`,
    `family_composite_diagnostics.csv`,
    `rolling_weight_failure_attribution.csv`,
    `real_factor_redundancy_clusters.csv`,
    `candidate_revision_recommendations.json`, and
    `factor_failure_diagnosis_report.md`.
  - V2 real smoke diagnosis flags:
    `data_timestamp_failure=false`, `coverage_failure=false`,
    `factor_definition_failure=false`, `redundancy_failure=false`,
    `rolling_icir_overfit_noise_failure=true`,
    `sector_regime_contribution=partial`, and `allocator_entry=blocked`.
  - V2 real smoke confirms the live rolling ICIR composite is still worse than
    simple diagnostics: live test spread average `-0.012354`, all-29 equal test
    spread `0.012808`, and price-momentum equal test spread `0.020044`.
  - Redundancy diagnosis now emits no archive factors and no rewrite-required
    factors after the v2 replay. The remaining blocker is not duplicate
    formula definition; it is rolling ICIR noise and failed placebo survival.
  - Candidate recommendations keep core families at price momentum,
    trend-quality, and sector-neutral residual momentum; risk/volatility,
    reversal, liquidity, capacity, and turnover families remain diagnostic
    until the placebo/weighting problem is solved.
  - FD-R5.1 preserves `recommended_next_action=stop_before_allocator`,
    `allocator_entry_allowed=false`, `direct_q2_entry_allowed=false`, and
    `alpha_success_claimed=false`.
  - Validation: focused FD-R5.1 test passed; `make
    factor-discovery-real-failure-diagnosis` passed on existing real outputs.
  - Next FD work is candidate-family selection or weighting redesign followed
    by FD-R3/R4/R5 replay, not FD-R6 allocator escalation.
- FD-R5.1 Formula Mechanism Separation Audit is complete for FactorSpec v2:
  - `factor_discovery_sandbox/formula_mechanism_audit.py`,
    `scripts/run_factor_discovery_formula_mechanism_audit.py`, and
    `make factor-discovery-formula-mechanism-audit` read the FD-R3 panel and
    write `formula_mechanism_audit.csv`, `duplicate_cluster_audit.csv`,
    `rank_identity_audit.csv`, and
    `reports/factor_formula_mechanism_v2_audit.md`.
  - The audit treats exact oriented-score duplicates, cross-sectional rank
    duplicates, and pure sign-flip rank duplicates as hard failures. High
    absolute correlation alone is review-only and does not fail mechanism
    separation.
  - Current real audit result:
    `formula_version=price_volume_29_mechanism_v2`, `factor_count=29`,
    `pair_count=406`, `hard_fail_pair_count=0`, and
    `high_correlation_review_pair_count=0`.
  - The audit remains formula-mechanism validation only. It does not open
    allocator, Q1, Q2, production approval, or Alpha Registry paths.
- FD-R4.1 / FD-R5.2 Rolling Weighting Reliability Gate is complete:
  - `factor_discovery_sandbox/factor_weighting_estimators.py`,
    `factor_discovery_sandbox/weighting_reliability.py`,
    `scripts/run_factor_discovery_weighting_reliability.py`, and
    `make factor-discovery-weighting-reliability` compare the current rolling
    ICIR method against equal-weight, family-equal, shrunk ICIR, signed shrunk
    ICIR, and rolling ridge estimators using only prior-visible history.
  - The gate writes
    `outputs/factor_discovery/research_mode/weighting_estimator_comparison.csv`,
    `weight_stability_diagnostics.csv`, `weighting_placebo_comparison.csv`,
    `weighting_failure_diagnosis.json`, and
    `reports/factor_discovery_weighting_reliability_report.md`.
  - Current real result: `decision=close`, `best_estimator=equal_weight_all`,
    `rolling_icir_overfit_noise_failure=true`, and `estimator_count=11`.
    Equal/family baselines are stronger than learned rolling ICIR variants, and
    learned estimators do not clear the placebo/baseline survival requirement.
  - Hard guards remain false/blocked:
    `allocator_entry_allowed=false`, `q1_entry_allowed=false`,
    `q2_entry_allowed=false`, `alpha_registry_update_allowed=false`,
    `production_approval_claimed=false`, `direct_q2_entry_allowed=false`, and
    `not_alpha_evidence=true`.
  - Validation: FD sandbox tests passed; the FD-R3 replay, FD-R4 rolling OOS,
    FD-R5 placebo robustness, formula mechanism audit, and weighting
    reliability smoke targets passed on the local WRDS daily PIT bundle.
- User-supplied Momentum 12-1 Low-Vol Candidate Diagnostic is available as a
  standalone FD diagnostic, not a new roadmap phase or Q1/Q2 import:
  - `factor_discovery_sandbox/momentum_low_vol_candidate.py`,
    `scripts/run_factor_discovery_momentum_low_vol_candidate.py`, and
    `make factor-discovery-momentum-low-vol-candidate` compute
    `rank(ts_sum(returns,252)-ts_sum(returns,21))-0.5*rank(ts_std_dev(returns,63))`
    directly from the local WRDS daily PIT bundle.
  - The diagnostic writes `candidate_signal_panel.csv`,
    `candidate_validation_by_rebalance.csv`, `candidate_placebo_report.csv`,
    `candidate_design_manifest.json`, `candidate_summary.json`, and
    `candidate_report.md` under
    `outputs/factor_discovery/research_mode/momentum_low_vol_candidate/`.
  - The current implementation retests raw, same-industry-neutralized, and 63d
    dollar-volume capacity-filtered variants. The capacity filter excludes the
    bottom 30% capacity-rank names from that variant while preserving
    `no_view != zero_alpha`; the current run filters out 5,913 of 20,229 active
    signal rows for the capacity-filtered score.
  - Current real run: `signal_date_count=215`, `active_signal_rows=20229`,
    `explicit_abstain_rows=981`, `validation_row_count=1056`,
    `placebo_test_count=6`, and top-level
    `candidate_validation_status=mixed_initial_diagnostic_gate`.
  - Variant statuses under the strict strongest-placebo gate:
    `raw_candidate_score=mixed_initial_diagnostic_gate`,
    `industry_neutral_score=mixed_initial_diagnostic_gate`, and
    `capacity_filtered_score=failed_initial_diagnostic_gate`.
  - Test-period raw diagnostics remain mixed: 1m mean Rank IC `0.028145` and
    top-bottom spread `0.004025` are positive, while 3m mean Rank IC
    `0.027307` is positive but 3m top-bottom spread is negative at
    `-0.000311`; the 3m random same-coverage placebo spread is stronger at
    `0.003848`.
  - Test-period industry-neutral diagnostics improve 3m spread
    (`mean_rank_ic=0.034653`, `top_bottom_spread=0.011041`) but remain mixed
    because 1m shifted-placebo spread `0.004077` is slightly stronger than live
    1m spread `0.004004`. Capacity-filtered diagnostics weaken and fail, with
    3m live spread `-0.002310` versus random same-coverage placebo `0.009544`.
  - The candidate remains `not_alpha_evidence=true`,
    `direct_q2_entry_allowed=false`, `allocator_ran=false`, and
    `alpha_success_claimed=false`; it must not enter allocator, Q1, Q2, Alpha
    Registry, broker/order/live, or production approval paths without a
    separate Phase 64 import decision.
  - Validation: focused momentum-low-vol tests passed; `make
    factor-discovery-momentum-low-vol-candidate` passed on the local WRDS daily
    PIT bundle; `git diff --check` passed.
- FD-S6 Resume-Grade Revision-Confirmed Earnings Underreaction Alpha is
  implemented as a sandbox-only candidate diagnostic:
  - `factor_discovery_sandbox/revision_confirmed_earnings_underreaction.py`,
    `scripts/run_factor_discovery_revision_confirmed_earnings_underreaction.py`,
    and `make factor-discovery-revision-confirmed-alpha` build
    `revision_confirmed_earnings_underreaction` without allocator, Q1, Q2,
    typed projection, Alpha Registry, broker/order/live, or production approval
    writes.
  - The fixed signal combines PIT consensus EPS `revision_20d`,
    `revision_acceleration`, SUE/CAR3 event confirmation observed only after
    announcement date + 3 trading days, a 5-trading-day post-earnings
    tradability rule, next-earnings exclusion, same-industry neutralization, and
    no-view-as-abstain handling. Missing SUE/CAR3 is neutralized rather than
    forward-filled into a fake event signal.
  - The runner writes exactly the FD-S6 research artifact set under
    `outputs/factor_discovery/research_mode/revision_confirmed_earnings_underreaction/`:
    `candidate_design_manifest.json`,
    `revision_confirmed_alpha_results.csv`,
    `revision_confirmed_alpha_summary.json`,
    `revision_confirmed_alpha_report.md`, `placebo_comparison.csv`,
    `signal_decay.csv`, `capacity_diagnostics.csv`, and
    `pit_timestamp_audit.json`.
  - Current local result:
    `decision_label=insufficient_support`, `promotion_gate_passed=false`,
    `q1_candidate_review_eligible=false`, `active_row_count=368`,
    `active_date_count=35`, and `explicit_abstain_rows=14997`.
    PIT timestamp audit passes with `future_estimate_timestamp_violations=0`,
    `broken_pit_timestamp_rows=0`, and `tradability_rule_violations=0`.
  - Blocking reasons are explicit: the expanded SUE panel is broad while the
    default daily price-volume bundle is Nasdaq100-scoped, producing
    `missing_price_rows=14606`; 20d and to-next-announcement test Rank IC/spread
    gates fail for the primary industry-neutral score; shifted-event, random
    same-coverage, and other placebo diagnostics remain stronger in places; and
    the effect is not non-fragile across variants.
  - The result is not production alpha, not formal alpha evidence, not Q1/Q2
    input, and not an Alpha Registry update. If future work revisits this
    family, it needs a broader price-volume/ADV panel aligned to the SUE event
    coverage before any candidate-review claim.
  - Validation: focused FD-S6 tests passed; `make
    factor-discovery-revision-confirmed-alpha` passed on local artifacts; `git
    diff --check` passed.
- FD-S0/S1/S3 Small-Cap Candidate Family v1 is now started as a separate FD
  research-family line:
  - `wrds_small_cap_pull.py`, `scripts/run_factor_discovery_small_cap_wrds_pull.py`,
    and `make factor-discovery-small-cap-wrds-pull` pull the local WRDS CRSP
    small-cap daily bundle into ignored cache under
    `data/cache/wrds_multifactor/small_cap_us_daily/`.
  - Current pulled bundle: `dataset_id=wrds_us_small_cap_daily_v1`,
    `research_start=2020-01-01`, `research_end=2024-12-31`,
    `price_start=2019-01-01`, `universe_rows=11337`,
    `price_rows=5977605`, `benchmark_rows=1510`, and
    `delisting_rows=1452`. It uses common-share / major-exchange CRSP filters,
    PIT market cap, shares outstanding, spread proxy fields, IWM benchmark,
    historical name intervals, and explicit CRSP delisting rows. Credentials
    stay outside the repo.
  - `small_cap_data_admission.py` validates PIT market cap, shares outstanding
    or float, adjusted/raw prices, volume, corporate-action handling, delisting
    returns/events, exchange/share-class filters, sector/industry, benchmark
    returns, ADV, and spread/proxy diagnostics.
  - `small_cap_universe.py` creates `large_cap_control`,
    `small_cap_investable`, and `microcap_quarantine` tiers. Microcap rows are
    diagnostic-only and have `candidate_decision_allowed=false`.
  - `small_cap_quality_family.py` implements
    `small_cap_quality_residual_momentum_v1` with primary signal
    `small_cap_quality_residual_momentum_6m_ex1m`: skip-1m 6m momentum,
    same-date residualization against sector, beta, log market cap, log ADV,
    and quality where available, then residual divided by realized 6m
    volatility.
  - The family writes `candidate_design_manifest.json` before validation and
    embeds the same FD-D0 design contract in `family_manifest.json`.
  - The family is fixed single-signal only. It explicitly records
    `rolling_icir_used=false`, `ridge_weighting_used=false`, and
    `learned_weighting_used=false`.
  - The WRDS small-cap data admission now passes with
    `small_cap_research_admitted=true`, `candidate_family_run_allowed=true`,
    `delisting_handling_status=pass`, and `liquidity_cost_data_status=pass`.
  - `wrds_small_cap_quality_pull.py`,
    `scripts/run_factor_discovery_small_cap_quality_pull.py`, and `make
    factor-discovery-small-cap-quality-pull` pull the local WRDS
    CCM/Compustat PIT quality bundle into ignored cache under
    `data/cache/wrds_multifactor/small_cap_quality/`. Current pulled bundle:
    `dataset_id=wrds_small_cap_quality_v1`, `ccm_link_rows=13513`,
    `fundamental_rows=246031`, `quality_score_rows=265119`, and
    `quality_covered_assets=4858`. It uses `crsp_a_ccm.ccmxpf_lnkhist` for CCM
    links and `comp.fundq` for quarterly quality inputs; credentials stay
    outside the repo.
  - The quality score is a PIT same-date robust z-score of
    `profitability_roa + gross_profitability - leverage`, visible at `rdq` when
    available and otherwise after a 90-day fallback lag. The puller attaches
    the `quality` section to the local ignored small-cap research manifest.
  - The quality-controlled WRDS-backed family rerun writes
    `outputs/factor_discovery/small_cap/` and
    `reports/factor_discovery_small_cap_quality_residual_momentum.md` with
    `decision_label=reject_placebo_failure`.
    `active_signal_rows=387161`, the primary active rows are all
    `quality_controlled` / `standard`, and residualization controls include
    `sector,beta,log_market_cap,log_adv_3m,quality_score`.
  - Primary OOS test metrics are positive before costs
    (`test_mean_rank_ic≈0.01867`, `test_mean_spread≈0.00778`; 1m spread
    ≈0.00473 and 3m spread ≈0.01083), but the realized-return placebo gate
    fails: the asset rebalance lag control and value-weighted live control beat
    the live equal-weight signal.
  - The cost/capacity pre-gate now uses leg-level spread/ADV proxy costs rather
    than a constant return haircut. It reports
    `pre_gate_status=fail_cost_adjusted_spread`, gross mean spread
    ≈0.00778, cost-adjusted mean spread ≈-0.00407, estimated cost drag
    ≈0.01185, median 1% ADV capacity ≈$29.7k, and median spread proxy
    ≈0.00282. This blocks Phase 64 candidate status even before any formal
    import review.
  - The family runner now caches monthly signal and forward-target panels under
    `outputs/factor_discovery/small_cap/family_candidates/quality_residual_momentum/`.
    Current cache artifacts are `monthly_signal_panel_cache.csv` (~206MB),
    `forward_target_panel_cache.csv` (~104MB), and `panel_cache_manifest.json`;
    the latest rerun reports `signal_panel_cache_status=hit` and
    `target_panel_cache_status=hit`.
  - `placebo_dominance_diagnosis.csv` diagnoses why the two live-beating
    controls dominate. Current real diagnostics: lagged signal dominance is
    consistent with signal persistence / monthly update noise
    (`score_lag_rank_corr≈0.76094`, top-quintile lag overlap ≈0.65891), and
    value-weight dominance is consistent with payoff concentration in larger
    / more capacity-friendly small-cap names (`score_market_cap_rank_corr≈0.08868`,
    top/bottom market-cap ratio ≈1.36113).
  - FD-S4.1 Small-Cap Lag / Capacity Dominance Gate is complete:
    `small_cap_temporal_diagnostics.py`,
    `small_cap_capacity_diagnostics.py`,
    `small_cap_lag_capacity_diagnosis.py`, and `make
    factor-discovery-small-cap-dominance-diagnosis` read the cached monthly
    signal/target panels and write `lag_decay_grid.csv`,
    `holding_period_sensitivity.csv`, `capacity_bucket_diagnostics.csv`,
    `weighting_scheme_comparison.csv`, `cost_drag_decomposition.csv`,
    `lag_construction_audit.csv`,
    `temporal_update_component_diagnostics.csv`,
    `small_cap_dominance_decision.json`, and
    `reports/factor_discovery_small_cap_dominance_diagnosis.md`.
  - Current FD-S4.1 result: `decision_label=diagnostic_only_cost_blocked`,
    `temporal_noise_detected=true`, `slow_signal_condition_passed=false`, and
    `capacity_filter_condition_passed=false`. Live 1m spread is ≈0.00272 and
    3m spread is ≈0.01054, but cost-adjusted live spread is ≈-0.00868; lag-1m,
    lag-3m, and rolling-median variants have stronger gross reads but still
    negative cost-adjusted spreads. The live-minus-lag update component has
    negative Rank IC (≈-0.00577) and negative 3m spread (≈-0.00803), consistent
    with noisy monthly updates rather than a clean faster signal.
  - Capacity diagnostics show larger / more liquid / tighter-spread buckets
    survive better (`market_cap=high` net spread ≈0.00348, `adv=high` net
    spread ≈0.00190, `spread=low` net spread ≈0.00122), but the broad fixed
    weighting schemes remain net negative after cost drag. The cost
    decomposition reports gross spread ≈0.00272, half-spread cost ≈0.00608,
    turnover cost ≈0.00053, ADV capacity penalty ≈0.00028, impact cost
    ≈0.00004, and net spread ≈-0.00421. The 6m holding rows are present but
    unavailable until a 6m target cache is generated.
  - FD-S4.2 Small-Cap Slow / Capacity-Filtered Diagnostic Rerun is complete:
    `small_cap_target_cache.py`, `small_cap_s4_2_diagnostic.py`,
    `scripts/run_factor_discovery_small_cap_s4_2.py`, and `make
    factor-discovery-small-cap-s4-2` write
    `outputs/factor_discovery/small_cap/target_cache/forward_returns_1m_3m_6m.csv`,
    `target_cache_audit.json`,
    `quality_residual_momentum_s4_2/pre_registered_filter_manifest.json`,
    `slow_signal_validation_grid.csv`, `capacity_filtered_oos.csv`,
    `placebo_comparison.csv`, `cost_adjusted_survival.csv`,
    `subperiod_survival.csv`, `s4_2_decision.json`, and
    `reports/factor_discovery_small_cap_s4_2_slow_capacity_filtered_diagnostic.md`.
  - Current FD-S4.2 target cache has all required horizons available:
    1m available rows `274025`, 3m available rows `263199`, and 6m available
    rows `247196`; it records corporate-action adjusted and delisting-adjusted
    targets with forward-return visibility only after the exit timestamp.
  - Current FD-S4.2 decision is
    `decision_label=reject_temporal_noise_confirmed`,
    `six_month_target_available=true`, `lag_or_smoothed_beats_live=false`,
    `holding_horizon_improves_cost_adjusted=true`, and
    `best_cost_adjusted_spread≈0.10115`. The best held-out filtered row is
    `live_signal / 3m / quarterly / adv_weight_within_bucket`; the best slow
    row is `lag_1m_signal / 3m / quarterly / adv_weight_within_bucket` with
    cost-adjusted spread ≈0.07138. This rejects the slow-signal hypothesis but
    leaves a possible future capacity-filter-only diagnostic, not a promoted
    candidate.
  - Hard guards remain blocked:
    `allocator_entry_allowed=false`, `q1_entry_allowed=false`,
    `q2_entry_allowed=false`, `alpha_registry_update_allowed=false`,
    `production_approval_claimed=false`, `direct_q2_entry_allowed=false`, and
    `not_alpha_evidence=true`.
  - FD-S4.3 Small-Cap Capacity-Filtered Live Signal Preregistration Gate is
    complete:
    `small_cap_s4_3_preregistration.py`,
    `scripts/run_factor_discovery_small_cap_s4_3.py`, and `make
    factor-discovery-small-cap-s4-3` write
    `outputs/factor_discovery/small_cap/family_candidates/capacity_filtered_live_s4_3/slow_signal_closeout.json`,
    `capacity_filter_preregistration_manifest.json`,
    `confirmation_split_manifest.json`, `capacity_filtered_live_signal_oos.csv`,
    `fixed_weighting_confirmation.csv`, `placebo_comparison.csv`,
    `cost_adjusted_survival.csv`, `subperiod_survival.csv`,
    `exposure_attribution.csv`, `shortability_boundary_report.json`,
    `s4_3_decision.json`, and
    `reports/factor_discovery_small_cap_capacity_filtered_live_s4_3.md`.
  - Current FD-S4.3 closes the slow/lagged branch and locks only
    `live_signal / 3m / quarterly / adv_weight_within_bucket` against
    `capacity_capped_equal_weight` control. A nested confirmation split exists,
    and the primary confirmation row has gross spread ≈0.08462 and
    cost-adjusted spread ≈0.08216, but the same-coverage, capacity-matched, and
    rebalance-shifted placebos beat it. The decision is
    `decision_label=reject_capacity_filter_hypothesis`; shortability remains
    unknown, no long-short tradability claim is made, and the family is not
    promoted.
  - FD-S4.3 hard guards remain blocked:
    `allocator_entry_allowed=false`, `q1_entry_allowed=false`,
    `q2_entry_allowed=false`, `alpha_registry_update_allowed=false`,
    `production_approval_claimed=false`, `direct_q2_entry_allowed=false`, and
    `not_alpha_evidence=true`.
  - Validation: focused small-cap/quality tests passed; focused FD-S4.1 tests
    passed (`11 passed`); `make
    factor-discovery-small-cap-quality-pull`, `make
    factor-discovery-small-cap-data-admission`, and `make
    factor-discovery-small-cap-quality-residual-momentum` passed; `make
    factor-discovery-small-cap-dominance-diagnosis` passed; `make
    factor-discovery-small-cap-s4-2` passed; focused FD-S4.3 tests passed
    (`10 passed`); `make factor-discovery-small-cap-s4-3` passed.
    Full FD sandbox tests passed (`88 passed`), `git diff --check` passed, and
    repo `make validate` passed after this update.
- Formal multifactor research-mode preflight is installed:
  - `multifactor_alpha_validation.data_contract.run_research_mode_preflight`
    validates actual dataset manifests before formal factor validation can
    proceed.
  - `make multifactor-research-mode-preflight` writes
    `outputs/multifactor_alpha_validation/research_mode_preflight/`.
  - The default local proxy manifest is expected to be blocked because current
    constituent/yfinance-style local files do not provide PIT historical
    membership, adjusted price-volume coverage, explicit delisting handling,
    formal use-mode metadata, source provenance, content hash, and trading
    calendar coverage.
  - This preflight is not alpha evidence; it prevents sandbox or teaching-mode
    artifacts from being promoted into real research mode without the required
    PIT data contract.
- MF-R6/R7/R8/R9/R10/R11/R12 real PIT dataset onboarding and risk-attribution
  setup is complete through the first factor attribution waterfall:
  - The monthly WRDS PIT bundle path remains complete for MF-R7 data-readiness
    dry checks and no factor claims.
  - `projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_research_mode.yaml`
    defines the local daily WRDS pull without embedded credentials and writes
    ignored cache artifacts under `data/cache/wrds_multifactor/nasdaq100_daily/`.
  - The refreshed size-aware local daily bundle lives under
    `data/cache/wrds_multifactor/nasdaq100_daily_size/` and contains historical
    Nasdaq100 membership, daily adjusted price-volume, QQQ benchmark coverage,
    delisting records, and non-null `dlycap`, `shrout`, and `dlyprcvol` fields.
  - `multifactor_alpha_validation.real_rolling_oos.run_first_real_rolling_oos_evidence`
    runs MF-R8 for `momentum_12_1`, `reversal_5_1`, and `low_vol_60d` only.
    It writes `real_oos_factor_evidence.csv`,
    `real_oos_neutralization_report.csv`,
    `real_oos_benchmark_attribution.csv`, `real_oos_survival_funnel.csv`,
    `real_oos_summary.json`, `real_oos_exposure_panel.csv`, and
    `real_oos_readiness.md` under
    `outputs/multifactor_alpha_validation/wrds_real_oos_evidence_size/`.
  - Current MF-R8 run returned `oos_status=evidence_ready`,
    `dataset_frequency=daily`, `observation_count=612`,
    `full_sample_icir_used=false`, `prior_history_only=true`,
    `allocator_ran=false`, `alpha_success_claimed=false`, and
    `not_alpha_evidence=true`.
  - `multifactor_alpha_validation.real_evidence_closeout.run_real_evidence_closeout`
    runs MF-R9 and writes `real_evidence_closeout_decision.json`,
    `real_evidence_conflict_diagnostics.csv`, and
    `real_evidence_closeout_report.md` under
    `outputs/multifactor_alpha_validation/wrds_real_evidence_closeout_size/`.
  - Sector attribution is now observed from the historical membership panel.
    Style attribution is now observed as size/liquidity/volatility proxy
    attribution using `market_cap`, trailing liquidity, and trailing volatility.
  - Current MF-R9 decision is `diagnostic_only` with reasons
    `style_proxy_only` and `benchmark_beta_style_conflict`. The stricter
    closeout explicitly flags `momentum_12_1`: QQQ-relative and beta-adjusted
    spreads are negative while the style-adjusted proxy net spread is positive.
    The positive style-proxy residual is diagnostic only and does not override
    benchmark/beta failure. Allocator entry, redundancy-gate entry, direct Q2
    entry, paper canary, live trading, broker/order paths, and production
    approval all remain false.
  - `multifactor_alpha_validation.risk_exposure_store.run_pit_exposure_store`
    runs MF-R10 and writes `exposure_panel.csv`,
    `exposure_coverage_report.json`, and `exposure_manifest.yaml` under
    `outputs/multifactor_alpha_validation/risk_model/`.
  - Current MF-R10 smoke built `223960` PIT exposure rows across `204` month-end
    dates, `226` assets, and `11` exposure names: sector, industry, trailing
    market beta, log market cap, liquidity ADV, residual volatility,
    short-term reversal, medium-term momentum, book-to-market, profitability,
    and asset growth.
  - R10 uses the local WRDS daily price-volume bundle plus the ignored WRDS
    Compustat/CCM fundamentals cache under
    `data/cache/wrds_multifactor/nasdaq100_fundamentals/`. Quarterly
    fundamentals are used only when `visibility_timestamp <= signal_date`; rows
    without visible or usable exposures become explicit abstain rows.
  - R10 timestamp checks pass: `exposure_date <= visibility_timestamp <=
    tradable_timestamp`, and same-close trading is false. The exposure store is
    `risk_attribution_input_only`, not a factor signal, alpha evidence,
    allocator input, Q2 input, paper canary, live workflow, or production
    approval.
  - `multifactor_alpha_validation.cross_sectional_risk_model.run_cross_sectional_risk_model`
    runs MF-R11 and writes `risk_model_returns_by_period.csv`,
    `risk_model_exposure_coefficients.csv`,
    `risk_model_residual_returns.csv`, and
    `risk_model_fit_diagnostics.json` under
    `outputs/multifactor_alpha_validation/risk_model/`.
  - Current MF-R11 smoke built `203` period rows, `37152` residual-return rows,
    and `8622` coefficient rows. The component groups are intercept, market
    beta, industry, configured style proxy, fitted return, and residual return.
  - R11 rows with missing required exposures become explicit abstain rows. The
    diagnostics set `model_use=ex_post_attribution_only`; residuals are
    attribution artifacts only, not tradeable predictions, factor signals,
    allocator input, Q2 input, or alpha evidence.
  - `multifactor_alpha_validation.factor_attribution_waterfall.run_factor_attribution_waterfall`
    runs MF-R12 and writes `factor_attribution_waterfall.csv`,
    `factor_attribution_waterfall_by_period.csv`,
    `factor_attribution_waterfall_{factor_id}.json`,
    `factor_attribution_diagnostics.json`, and
    `factor_attribution_report.md` under
    `outputs/multifactor_alpha_validation/risk_model/`.
  - Current MF-R12 smoke built `3` factor rows and `609` factor-period rows.
    `momentum_12_1`, `reversal_5_1`, and `low_vol_60d` are all marked
    `style_proxy_conflict`: positive configured proxy residuals do not override
    negative benchmark/beta readouts.
  - R12 reports gross, QQQ-relative, beta-adjusted, industry-adjusted,
    style-proxy-adjusted, and full-residual spread readouts. It remains
    diagnostic attribution only and does not allow redundancy-gate, allocator,
    Q2, paper/live, broker/order, or production-approval entry.
  - `multifactor_alpha_validation.strict_residual_closeout.run_strict_residual_closeout`
    runs MF-R13 and writes `strict_residual_closeout_decision_table.csv`,
    `strict_residual_closeout_diagnostics.json`,
    `factor_registry_risk_model_update.yaml`, and
    `strict_residual_closeout_report.md` under
    `outputs/multifactor_alpha_validation/risk_model/`.
  - Current MF-R13 smoke built `3` factor decisions with
    `ready_for_redundancy_count=0`: `momentum_12_1` is
    `insufficient_residual_evidence`, while `reversal_5_1` and `low_vol_60d`
    are `style_proxy_conflict`.
  - MF-R13 explicitly blocks redundancy and allocator entry when a configured
    proxy residual is positive but benchmark/beta readouts are negative, or when
    full-residual evidence is unstable. Proxy residuals are not style-neutral
    alpha and are not tradeable predictions.
  - `multifactor_alpha_validation.failure_diagnosis_report.run_failure_diagnosis_report`
    writes a fixed failure diagnosis report after MF-R13:
    `factor_failure_diagnosis.csv`, `qqq_relative_guard_review.json`, and
    `factor_failure_diagnosis_report.md`.
  - Current diagnosis: `momentum_12_1` stops at `residual_stability`,
    `reversal_5_1` stops at `residual_stability`, and `low_vol_60d` stops at
    `beta_exposure`. The QQQ-relative guard is marked over-strict as a hard gate
    for long-short factor spreads, but softening it rescues `0` current factors,
    so redundancy and allocator entry remain blocked.
  - `multifactor_alpha_validation.portfolio_component_gate.run_portfolio_component_gate`
    runs MF-R14 and writes `component_candidate_table.csv`,
    `portfolio_component_gate_summary.json`, and
    `portfolio_component_gate_report.md`.
  - Current MF-R14 smoke returns `standalone_clean_alpha_count=0`,
    `component_candidate_count=3`, and
    `portfolio_validation_mode=diagnostic_ensemble_only`. `momentum_12_1` and
    `reversal_5_1` are `eligible_benchmark_premia_component`; `low_vol_60d` is
    `eligible_hedge_component`.
  - R14 corrects the R13 interpretation: strict residual attribution is not a
    death gate for multi-factor research. It is a role classifier feeding only
    portfolio-level diagnostic ensemble validation. It does not approve
    standalone alpha, unrestricted allocator entry, Q2, paper/live, or
    production workflows.
  - `multifactor_alpha_validation.candidate_filter_audit.run_candidate_filter_audit`
    runs MF-R14.5 and writes `candidate_filter_audit.csv`,
    `hard_excluded_candidates.csv`, `soft_resurrected_component_pool.csv`,
    `component_pool_manifest.json`, and `filter_audit_report.md`.
  - Current MF-R14.5 smoke audits all `10` formal FactorSpecs:
    `component_pool_count=9`, `hard_excluded_count=1`. The only hard exclusion is
    `analyst_revision_disabled` because the PIT estimate source is missing. The
    soft-resurrected component pool includes the three R14 component rows plus
    enabled formal specs not yet risk-attributed and `sue_event_reference` as a
    reference component.
  - R14.5 fixes the early-filtering bias before R15: PIT/timestamp/lookahead /
    survivorship / same-close / missing required data / forward-return leakage
    remain hard blockers, but weak standalone IC, insufficient residual evidence,
    benchmark exposure, style-proxy conflict, high correlation, high turnover
    warning, and unstable standalone evidence are labels, not pre-portfolio kill
    rules. R15 must use `soft_resurrected_component_pool.csv`.
  - `multifactor_alpha_validation.portfolio_validation.run_portfolio_ensemble_validation`
    runs MF-R15 and writes `portfolio_ensemble_oos_report.csv`,
    `ensemble_vs_baselines.csv`, `ensemble_validation_summary.json`,
    `random_weight_placebo_report.csv`, `permuted_signal_placebo_report.csv`,
    and `portfolio_validation_report.md` under
    `outputs/multifactor_alpha_validation/portfolio_validation/`.
  - Current MF-R15 smoke returns `validation_status=evaluated`,
    `decision_state=portfolio_component_pool_fails_cost`,
    `input_component_count=9`, `available_component_count=8`,
    `unavailable_component_count=1`, and `hard_blocked_component_count=0`
    after component OOS observation expansion. The available components are
    `momentum_12_1`, `reversal_5_1`, `low_vol_60d`, `liquidity_turnover`,
    `value_bm`, `profitability_quality`, `investment_asset_growth`, and
    `accruals`; `sue_event_reference` remains unavailable rather than receiving
    fabricated event returns.
  - R15 compares equal-weight, cluster-equal, inverse-vol,
    simple-shrinkage, current-component, best-single, QQQ, random-weight
    placebo, and permuted-signal placebo ensembles. It keeps
    `full_sample_weights_used=false`, `or_optimizer_used=false`, and
    `security_level_portfolio_construction_used=false`. The primary equal-weight
    ensemble has negative gross, negative cost-adjusted annualized return, and
    negative QQQ-relative return, so OR optimization remains blocked.
  - `multifactor_alpha_validation.portfolio_assembly_audit.run_portfolio_assembly_audit`
    runs MF-R15.5 and writes `portfolio_assembly_audit.json`,
    `observed_subset_coverage_report.csv`, `component_direction_audit.csv`,
    `gross_to_net_waterfall.csv`, `role_aware_ensemble_report.csv`, and
    `decision_state_reclassification.md`.
  - Current MF-R15.5 smoke reclassifies
    `portfolio_component_pool_fails_cost` to `component_pool_fails_gross`
    after observation expansion. The audit records
    `eligible_component_count=9`, `observed_component_count=8`,
    `unavailable_component_count=1`, `coverage_ratio=0.8888888889`, and
    `benchmark_exposure_conflict=true`.
  - R15.5 conclusion: the current observed component pool fails gross and
    cost-adjusted OOS under the primary diagnostic construction. The remaining
    unavailable component is `sue_event_reference`, which still needs an
    auditable event visibility timestamp path. OR optimization remains blocked.
  - `multifactor_alpha_validation.component_oos_observation_expansion.run_component_oos_observation_expansion`
    builds expanded component OOS observations from the local WRDS daily
    price-volume manifest and lagged Compustat fundamentals manifest. It writes
    `real_oos_observations.csv`,
    `component_oos_observation_expansion_summary.json`, and
    `component_oos_observation_enablement_report.csv` under
    `outputs/multifactor_alpha_validation/component_oos_observations/`.
  - Current component OOS observation expansion generated observations for
    `accruals`, `investment_asset_growth`, `liquidity_turnover`,
    `profitability_quality`, and `value_bm`, lifting observed component
    coverage from 3 of 9 to 8 of 9. It fixes manifest-path resolution for
    repo-relative cache paths and normalizes WRDS/Compustat identifiers such as
    `12142.0` to `12142` before gvkey joins. It keeps
    `sue_event_reference` unavailable, writes no raw WRDS data into git,
    fabricates no returns, uses no full-sample ICIR, and does not open OR, Q2,
    broker/order, live, or production workflows.
  - `multifactor_alpha_validation.component_oos_availability.run_component_oos_availability_expansion`
    runs MF-R15.6 and writes `component_oos_availability_report.csv`,
    `component_oos_availability_summary.json`, and
    `component_enablement_plan.md`.
  - Current MF-R15.6 smoke returns `eligible_component_count=9`,
    `observed_component_count=8`, `unavailable_component_count=1`,
    `coverage_ratio=0.8888888889`,
    `component_pool_validation_state=component_pool_observation_coverage_sufficient`,
    and `full_pool_decision_allowed=true`. The only remaining unavailable
    reason is `missing_event_timestamp` for `sue_event_reference`.
  - R15.6 keeps unavailable event components unavailable rather than
    fabricating returns. Fundamental components retain `reporting_lag_days=90`.
  - Correct next multifactor step is diagnostic post-portfolio contribution /
    ablation only if kept scoped to the observed component pool. Do not open OR
    factor-sleeve optimization, security-level construction, or unrestricted
    allocator work until portfolio-level OOS evidence improves rather than
    failing gross/cost diagnostics.
  - `multifactor_alpha_validation.portfolio_contribution.run_post_portfolio_contribution`
    runs MF-R16 and writes `factor_ablation_report.csv`,
    `cluster_ablation_report.csv`, `factor_role_contribution.csv`,
    `contribution_by_regime.csv`, `portfolio_contribution_summary.json`, and
    `post_portfolio_contribution_report.md` under
    `outputs/multifactor_alpha_validation/portfolio_contribution/`.
  - Current MF-R16 smoke returns `validation_status=evaluated`,
    `decision_state=portfolio_contribution_diagnostic_only`, and
    `observed_component_count=8`. The baseline diagnostic equal-weight
    construction remains weak: gross annualized return `-0.0260405898`,
    cost-adjusted return `-0.0376911544`, and Sharpe `-0.6257279985`.
  - Current MF-R16 attribution says `fundamental_premia_component` and
    `style_premia_return_driver` have positive role contribution,
    `hedge_or_diversifier_component` is negative as a role, `low_vol_60d` has
    positive QQQ-down hedge contribution but is negative overall, and
    `liquidity_turnover` is a negative post-portfolio contribution. This is
    attribution only; it does not create weights or revive the failed R15 pool.
  - MF-R17 bounded cost/capacity attribution is installed via
    `multifactor_alpha_validation.portfolio_cost_capacity.run_portfolio_cost_capacity_attribution`
    and `make multifactor-portfolio-cost-capacity`. It writes
    `component_cost_capacity_attribution.csv`, `cost_stress_report.csv`,
    `capacity_frontier.csv`, `portfolio_cost_capacity_summary.json`, and
    `portfolio_cost_capacity_report.md` under
    `outputs/multifactor_alpha_validation/portfolio_cost_capacity/`.
  - MF-R17 is component-proxy diagnostic only. If security-level ADV is
    unavailable, capacity rows are marked
    `proxy_only_missing_security_level_adv`; no executable capacity is
    fabricated. It keeps `or_optimizer_used=false`,
    `security_level_portfolio_construction_used=false`, and
    `direct_q2_entry=false`.
  - MF-E0 full-market multifactor sweep is opened via
    `multifactor_alpha_validation.full_market_sweep.run_full_market_multifactor_sweep`
    and `make multifactor-full-market-sweep`. It writes
    `full_market_feature_cache.csv`, `full_market_pocket_grid.csv`,
    `full_market_template_grid.csv`, `full_market_placebo_top_pockets.csv`,
    `full_market_sweep_summary.json`, and `full_market_sweep_report.md` under
    `outputs/multifactor_alpha_validation/full_market_sweep/`.
  - MF-E0 is an overfit/discovery lab only. It scans leaf pockets and fixed
    multi-factor templates from a returns-long panel, records search burden, and
    keeps `d3_charter_allowed=false`, `measurement_spec_written=false`,
    `q1_entry_allowed=false`, `q2_entry_allowed=false`, and
    `or_optimizer_used=false`.
  - MF-E0 supervisor retry loop is installed via
    `multifactor_alpha_validation.full_market_supervisor.run_full_market_multifactor_supervisor`
    and `make multifactor-full-market-supervisor`. It runs the E0 sweep,
    freezes each attempted candidate, performs locked train/validation/test
    diagnostics through `full_market_locked_validation`, retries up to 100
    candidates by default if the locked gate fails, and writes
    `supervisor_run_summary.json`, `supervisor_attempt_log.csv`,
    `frozen_candidate_manifest.json`, and `full_market_supervisor_report.md`.
  - The supervisor is freeze-only diagnostic infrastructure. It keeps
    `d3_charter_allowed=false`, `measurement_spec_written=false`,
    `q1_entry_allowed=false`, `q2_entry_allowed=false`, `or_optimizer_used=false`,
    `expected_return_panel_written=false`, and
    `alpha_registry_update_allowed=false`.
  - Current `make multifactor-full-market-supervisor` smoke uses
    `data/risk_inputs_us_expanded/returns_long.csv`, searches 1160 E0
    candidates, and finds a freeze-only locked-validation pass on attempt 76:
    `low_vol_momentum`, template, `side=top`, `quantile=0.8`, `window=post_1_44`.
    Locked test metrics are mean return `0.0325262359`, t-stat
    `4.0578996726`, hit rate `0.5846153846`, month breadth `3`, and issuer
    breadth `40`; same-coverage random is negative and shifted-date profile is
    below live. This is not D3, not Q1, not Q2, and not alpha approval.
  - `make multifactor-full-market-candidate-audit` runs a full audit on the
    freeze-only candidate and writes `candidate_full_audit_summary.json`,
    `candidate_temporal_breadth.csv`, `candidate_tail_concentration.csv`,
    `candidate_data_anomaly_audit.json`, `candidate_cost_capacity_audit.json`,
    `candidate_benchmark_residual_audit.csv`, and
    `candidate_full_audit_report.md` under
    `outputs/multifactor_alpha_validation/full_market_candidate_audit/`.
  - Current full audit returns
    `decision_label=full_audit_passed_cost_capacity_pending`. Tail concentration
    is not dominant (`top10_abs_share=0.0339767369`), but two selected-path
    extreme daily-return rows remain explicit data-anomaly watch items. Test
    benchmark residual mean is `0.0357569672`. The audit now joins static US
    universe ADV/market inputs with `market_input_coverage_share=1.0`; proxy
    cost/capacity is evaluated, but real bid-ask spread remains unavailable, so
    cost/capacity remains pending. All D3/Q1/Q2/OR/Alpha Registry and
    production flags remain false.
  - Known limitation: R8/R9/R10/R11/R12/R13/R14/R14.5/R15/R15.5/R15.6 prove the real-data workflow shape,
    timestamped evidence plumbing, PIT exposure-store readiness,
    cross-sectional attribution plumbing, factor waterfall reporting, and strict
    residual/component closeout, candidate-pool audit, and diagnostic
    portfolio-ensemble validation, assembly audit, component OOS observation
    expansion, availability expansion, and post-portfolio contribution
    attribution only. They do not prove real alpha, approve Q2, or unlock OR
    optimization, unrestricted allocator, or redundancy promotion.
  - Validation: research-mode preflight focused tests passed; smoke returned
    `status=blocked` with thirteen blockers on the local proxy manifest.
- WRDS option B ingest is installed for formal multifactor research mode:
  - `multifactor_alpha_validation.wrds_ingest.run_wrds_multifactor_ingest`
    uses a locally configured WRDS connection, rejects configs containing
    credential-like keys, writes raw and standardized extracts under
    `data/cache/wrds_multifactor/`, builds a research-mode manifest, and runs
    the PIT preflight.
  - `projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml`
    uses WRDS Compustat `idxcst_his` plus CRSP/Compustat CCM for PIT Nasdaq100
    membership, CRSP monthly CIZ security data for adjusted monthly
    price-volume coverage, CRSP monthly QQQ benchmark data, and explicit CRSP
    delisting records. The current local bundle is monthly, not daily.
  - Current workspace WRDS monthly PIT bundle status: ready. The standardized
    manifest at
    `data/cache/wrds_multifactor/nasdaq100/standardized/research_mode_dataset_manifest.yaml`
    passed preflight with 272 historical membership rows, 39,470 adjusted
    monthly price-volume rows, 216 QQQ benchmark rows, and 55 delisting rows.
  - `make multifactor-wrds-config-check` validates the query config without
    opening a WRDS connection.
  - Validation: WRDS ingest focused tests passed; config-check smoke passed
    with `credentials_in_config=false`; the actual local ingest passed with
    `research_mode_ready=true` and no preflight blockers.
- Standalone Multi-Factor Alpha Validation Engine Week 1-8 is implemented:
  - `projects/multifactor_alpha_validation/ROADMAP.md` remains independent from
    the root PortfolioOS phase sequence and does not create an automatic Phase
    67.
  - Formal MVP specs, PIT contracts, signal panels, AlphaView-compatible
    mapping, Q1-style evidence, neutralization, redundancy/marginal-value
    gates, shrinkage, covariance stabilization, allocator diagnostics,
    zero-weight attribution, cost/capacity/benchmark survival, registry, final
    report, dashboard, release manifest, project README, and interview talking
    points are installed under `projects/multifactor_alpha_validation/`.
  - `make factor-validate` runs the standalone local pipeline and project tests.
  - The engine remains local-only, no-production-approval, no-live-trading,
    no-security-output, and no-direct-Q2-entry.
  - Current project state is infrastructure complete plus MF-R7 monthly WRDS
    PIT dry-run complete. This is still not research-grade alpha evidence; the
    active blocker is daily price-volume validation / MF-R8 evidence scope.
  - MF-R0 Dataset Manifest Contract is complete:
    current-constituent/yfinance-style manifests fail closed, synthetic PIT
    fixture manifests can pass, missing historical membership / price /
    benchmark / delisting / trading-calendar / provenance / content-hash /
    allowed-use metadata is blocked, same-close trading is blocked, and WRDS
    query config validation rejects embedded credentials without opening a WRDS
    connection.
  - MF-R1 Historical Universe Membership Loader is complete:
    `multifactor_alpha_validation.research_dataset.load_historical_universe_membership`
    validates historical membership fields, rejects current-constituent
    backfill, writes rebalance-date universe snapshots, keeps exited names in
    their valid historical windows, and emits
    `historical_membership_validation.json`.
  - MF-R2 Adjusted Price/Volume and QQQ Benchmark Panel is complete:
    `validate_adjusted_price_volume_and_benchmark` requires adjusted open,
    adjusted close, and volume, checks price coverage against PIT universe
    snapshots, checks QQQ coverage across rebalance dates, records the adjusted
    price convention, and writes missing name coverage to an abstain report
    instead of zero-filling.
  - MF-R3 Delisting and Inactive Asset Handling is complete:
    `validate_delisting_inactive_handling` identifies historical membership
    rows with exit dates, requires explicit delisting or terminal-return
    policy fields, reports inactive-name coverage, blocks missing inactive
    coverage, and records the delisting policy artifact.
  - MF-R4 First Research Dry Run is complete:
    `run_first_research_dry_run` requires a ready research-mode preflight,
    runs only `momentum_12_1`, `reversal_5_1`, and `low_vol_60d` through signal
    panels, AlphaView mapping, Q1 evidence, and QQQ/beta benchmark attribution,
    blocks same-close trading, does not run the allocator, and writes explicit
    no-alpha-success language.
  - MF-R5 Rolling OOS Factor Validation is complete:
    `run_rolling_oos_factor_validation` forbids full-sample ICIR weighting,
    records prior-history-only cutoffs before every rebalance date, makes
    train/validation/test windows explicit, separates raw, neutralized, and
    cost-adjusted readouts, writes a survival funnel, and records an honest
    null when costs collapse the fixture result.
  - MF-R6 External PIT Dataset Source Adapter is complete:
    `validate_external_pit_dataset_source` validates the WRDS Nasdaq100 source
    config without opening a WRDS connection, writes
    `dataset_source_manifest.yaml`, `source_field_mapping.yaml`,
    `dataset_ingest_validation.json`, and `dataset_readiness.md`, blocks
    current-constituent/yfinance-style source configs, rejects embedded
    credentials, requires local data/cache paths, and records no-alpha-evidence
    non-claims.
  - MF-R7 Real Dataset Dry Run, No Factor Claims is complete for the local WRDS
    monthly PIT bundle:
    `multifactor_alpha_validation.real_dataset_dry_run.run_real_dataset_dry_run`
    reads the real manifest and CSVs, reruns preflight, writes
    `real_dataset_summary.json`, `real_dataset_coverage.csv`,
    `timestamp_alignment.csv`, `universe_snapshot_summary.csv`,
    `benchmark_alignment.csv`, `delisting_coverage.csv`,
    `signal_availability.csv`, `daily_price_volume_long_task.md`, and
    `real_dataset_dry_run_report.md` under ignored
    `outputs/multifactor_alpha_validation/wrds_real_dataset_dry_run/`.
  - The MF-R7 smoke returned `dataset_frequency=monthly`,
    `daily_price_volume_validation_status=separate_long_task_not_started`,
    `allocator_ran=false`, `factor_ranking_ran=false`,
    `strategy_return_claimed=false`, and `alpha_conclusion_claimed=false`.
  - Daily price-volume validation is explicitly separated in
    `projects/multifactor_alpha_validation/configs/wrds_nasdaq100_daily_price_volume_long_task.yaml`
    with `status=not_started` and `requires_explicit_run=true`; it must be run
    before daily `reversal_5_1`, `low_vol_60d`, or next-session tradability
    claims.
  - Next recommended multifactor phase is the daily price-volume long task or a
    tightly scoped MF-R8 that only uses evidence the monthly bundle can prove;
    do not add factors, tune returns, or open allocator/ML polish before that.
  - Do not add factors, tune allocator logic, add ML models, or polish returns
    before the PIT dataset gate is ready.
  - Validation: Week 1-8 focused tests passed; `make factor-validate` is the
    required smoke for this project.
- Phase 48 Typed Expected-Return Injection Fixture is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_injection_schema.py` defines injection input/result/summary/manifest contracts.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_expected_return_injection.py` validates Q2InputContract v2 plus projection manifest, supports positive/scaled/sign-flipped expected-return panels, and writes optimizer input snapshots.
  - `projects/execution_aware_optimizer/fixtures/typed_injection/` provides a deterministic fixture aligned to the local 2026-02-27 rebalance date.
  - `scripts/run_typed_expected_return_injection_fixture.py` and `make typed-expected-return-injection-fixture` write local ignored artifacts under `outputs/typed_expected_return_injection_fixture/`.
  - The opt-in smoke path returned `injection_status=injected`, `expected_return_reached_optimizer_input=True`, `optimizer_input_snapshot_rows=33`, `injected_expected_return_count=2`, and `q2_adapter_status=observed`.
  - Known limitation: Phase 48 proves optimizer-input reachability only. It does not prove directional optimizer response, SUE survival, production alpha approval, or paper-stage readiness.
  - validation: Phase 48 focused tests `6 passed`; typed Q2 + Phase 48 focused tests `15 passed`; Q2 project tests `39 passed`; `make typed-expected-return-injection-fixture` passed with `injection_status=injected`; `make validate` passed.
- Phase 49 Typed Optimizer Response Acceptance Suite is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_optimizer_response_schema.py` defines response input, row, summary, and result contracts.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_optimizer_response.py` builds deterministic positive, scaled, sign-flipped, zero-alpha, and explicit-abstain typed expected-return panels and evaluates aggregate optimizer response diagnostics through a local PortfolioOS fixture.
  - `scripts/run_typed_optimizer_response_acceptance.py` and `make typed-optimizer-response-acceptance` write local ignored artifacts under `outputs/typed_optimizer_response_acceptance/`.
  - The opt-in smoke path returned `response_status=observed`, `optimizer_status=optimal`, `panel_count=7`, and true checks for positive rank/weight alignment, monotone scaled alpha contribution, sign-flip reversal, `no_view` versus zero-alpha distinction, and repair-retention reporting.
  - Known limitation: Phase 49 proves deterministic local optimizer response to typed expected-return panel variants. It does not prove SUE survival, revision marginal value, paper-stage readiness, or production approval.
  - validation: Phase 49 focused tests `4 passed`; `make typed-optimizer-response-acceptance` passed; Q2 typed focused tests `19 passed`; Q2 project tests `43 passed`; `make validate` passed.
- Phase 50 SUE Typed Q2 Survival Matrix v1 is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_typed_q2_survival_schema.py` defines SUE survival input, row, summary, and result contracts.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_typed_q2_survival.py` aligns the SUE typed projection to the local 2026-02-27 optimizer fixture date, reuses the Phase 48 injection path, and maps configured local Q2 rows as observed.
  - `projects/execution_aware_optimizer/fixtures/sue_survival/` provides the local SUE typed projection fixture.
  - `scripts/run_sue_typed_q2_survival.py` and `make sue-typed-q2-survival` write local ignored artifacts under `outputs/sue_typed_q2_survival/`.
  - The opt-in smoke path now returns `survival_status=observed`, `injection_status=injected`, `expected_return_reached_optimizer_input=True`, `q2_observed_rows=30`, and `q2_unavailable_rows=0`.
  - Known limitation: Phase 50 proves SUE expected-return reaches a local optimizer input snapshot and maps existing local Q2 adapter rows. It does not claim SUE alpha success, revision marginal value, paper-stage readiness, or production approval.
  - validation: Phase 50 focused tests passed; `make sue-typed-q2-survival` passed with `survival_status=observed`; Q2 project tests `58 passed`.
- Phase 51 SUE Execution-Survival Attribution Report is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_execution_survival_attribution_schema.py` defines attribution and layer schemas.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_execution_survival_attribution.py` interprets Phase 50 rows across evidence, projection, injection, optimizer response, constraint repair, cost, turnover, coverage/abstain, and unavailable fixture-hook layers.
  - `scripts/build_sue_typed_q2_survival_attribution.py` and `make sue-survival-attribution` rebuild the local Phase 50 fixture and write `outputs/sue_typed_q2_survival/failure_attribution.json` plus `reports/sue_typed_q2_survival_attribution.md`.
  - The local decision is now `sue_q2_observed_survives`: SUE reaches optimizer input and all configured local Q2 fixture rows are observed, including `risk_controlled` through the `naive_pro_rata` mapping.
  - Phase 52 should proceed only as a revision marginal-value diagnostic, not as SUE alpha success or production approval.
  - validation: Phase 51 focused tests passed; `make sue-survival-attribution` passed with `decision_label=sue_q2_observed_survives`; Q2 project tests `58 passed`.
- Phase 52 Revision Marginal-Value Gate is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/revision_marginal_value_schema.py` defines the gate input, required-test, marginal-metric, threshold, overlap-row, summary, and result contracts.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/revision_marginal_value_gate.py` enforces WRDS-only PIT source acceptance for analyst revision research, rejects FMP frozen estimate history as PIT-safe input, rejects raw tree or feature importance as proof, and requires SUE-adjusted cost-aware marginal improvement before composite promotion.
  - `projects/execution_aware_optimizer/fixtures/revision_marginal_value/gate_input.json` is the deterministic local fixture; it passes the required diagnostics but fails the cost-aware marginal threshold.
  - `scripts/run_revision_marginal_value_gate.py` and `make revision-marginal-value-gate` write `outputs/revision_marginal_value_gate/` artifacts plus `reports/revision_marginal_value_report.md`.
  - The local decision is `revision_real_but_no_marginal_value`: revision remains a real shadow branch, Phase 53-54 composite work is not opened by the default fixture, and production approval is not claimed.
  - validation: Phase 52 focused tests `5 passed`; Phase 51-52 focused tests `9 passed`; Q2 project tests `56 passed`; `make revision-marginal-value-gate` passed with `gate_decision=revision_real_but_no_marginal_value`; `make validate` passed.
- Phase 55 Alpha Registry v2 / Decision State Machine is complete:
  - `src/portfolio_os/alpha/registry_v2.py` defines machine-readable alpha registry and entry contracts, allowed decision statuses, pass/fail label rejection, stop-layer requirements, and no-production/no-live guards.
  - `scripts/build_alpha_registry_v2.py` and `make alpha-registry-v2` write `outputs/alpha_registry_v2/alpha_registry.yaml`, `outputs/alpha_registry_v2/alpha_registry_decision_table.csv`, and `reports/alpha_registry_report.md`.
  - Registry entries freeze SUE as `canonical_pilot` with `q2_observed_survives` in its history, revision as `real_shadow_branch`, SUE + revision composite as `archived_no_marginal_value`, old Phase 1.5 bridge and Qlib revision as `diagnostic_only`, residual momentum as `calibration_only`, A-share `anti_mom_21_5` as `background_partially_real`, and forward-return leakage fixtures as `rejected_leakage`.
  - Every registry entry has a typed-chain stop layer, and no entry claims production approval, live trading, paper canary approval, broker workflows, orders, or new research branches.
  - validation: Alpha Registry v2 focused tests `3 passed`; `make alpha-registry-v2` passed with `entry_count=8`; `make validate` passed.
- Phase 56A Expanded SUE Typed Q2 Candidate v1 is complete:
  - `projects/typed_alpha_pilot/fixtures/sue_expanded/fixture_config.json` defines the deterministic expanded SUE fixture.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_expanded_survival_schema.py` validates event rows with `event_timestamp`, `event_available_timestamp`, `tradable_timestamp`, and `rebalance_date` PIT ordering.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/sue_expanded_typed_q2_survival.py` generates 120 deterministic SUE event-name rows across 12 rebalance dates, projects them through the existing Alpha Projection Bridge, runs representative local expected-return injection, and maps Q2 fixture rows separately as observed/unavailable.
  - `scripts/run_sue_expanded_typed_q2_survival.py` and `make sue-expanded-typed-q2-survival` write local artifacts under `outputs/sue_expanded_typed_q2_survival/` plus `reports/sue_expanded_typed_q2_survival_report.md`.
  - The smoke path returned `survival_status=observed`, `injection_status=injected`, `event_count=120`, `rebalance_date_count=12`, `active_rebalance_count=12`, `median_active_names_per_active_date=10.00`, `q2_observed_rows=30`, and `q2_unavailable_rows=0`.
  - The report states this is expanded deterministic fixture evidence, not real historical evidence, paper-ready status, or production approval.
  - `src/portfolio_os/alpha/registry_v2.py` now records a structured SUE decision-history entry for `sue_expanded_fixture_q2_observed_survives` with `evidence_type=deterministic_expanded_fixture`, `event_count=120`, `rebalance_date_count=12`, `active_rebalance_count=12`, `median_active_names_per_active_date=10.0`, `expected_return_used_share=0.833333`, `coverage_loss_count=24`, `q2_observed_rows=30`, `q2_unavailable_rows=0`, and `production_approval_claimed=false`.
  - `reports/sue_expanded_typed_q2_closeout.md` closes Phase 56A as deterministic expanded fixture breadth only; missing coverage remains explicit abstain/no_view, not zero alpha.
- Phase 65 PortfolioOS v1 Research-Audit Release Hygiene is complete:
  - `docs/releases/portfolioos_v1_research_audit_release.md` summarizes the release boundaries across Q1, Evidence Bundle / Promotion Gate, Typed AlphaView, SUE local typed-Q2 pilot, expanded deterministic SUE benchmark, Alpha Registry v2, dashboard, audit, provenance, and no-network safeguards.
  - `tests/test_portfolioos_v1_research_audit_release.py` verifies the release note, Alpha Registry v2 SUE status, Phase 56A deterministic fixture metrics, `outputs/alpha_registry_v2/` consistency when generated, and misleading-claim rejection for production approval, paper readiness, live alpha orders, broker execution, real historical SUE proof, guaranteed tradable alpha, auto trading, and investment recommendations.
  - Release hygiene does not add alpha research, optimizer changes, broker/order paths, paper canary approval, live workflows, or Factor Discovery implementation.
- Phase 66 PortfolioOS v1 Maintenance Freeze / Future-Only Backlog is complete:
  - `docs/releases/portfolioos_v1_maintenance_freeze.md` freezes the v1 research-audit release after Phase 56A and Phase 65.
  - `docs/strategy/portfolioos_future_backlog.md` records locked future-only categories for real historical SUE, paper-overlay calibration, Factor Discovery import review, live/broker/order work, and production approval.
  - `tests/test_portfolioos_v1_maintenance_freeze.py` rejects misleading freeze claims and verifies that new work is backlog-only unless explicitly reopened.
  - SUE remains an expanded deterministic typed-Q2 candidate benchmark, not production-approved, not paper-ready, not live-ready, and not historically proven.
  - Existing unrelated Multifactor / Factor Discovery working-tree changes are not part of the v1 freeze.
- Phase 46 Dashboard Readability Polish is complete:
  - `src/portfolio_os/dashboard/static_dashboard.py` now renders a first-screen typed-alpha status summary, typed-alpha chain, artifact links, manifest summary, clearer safety boundaries, and missing-artifact unavailable messaging.
  - Dashboard wording explicitly distinguishes integration benchmark, unavailable/local paper-overlay execution state, no broker/orders/live workflow, and not-approved production state.
  - The dashboard remains static read-only and exposes no forms, POST methods, route-like broker/order/trade/live paths, submit controls, or workflow triggers.
  - `tests/test_typed_alpha_dashboard_readability.py` guards the readability surface and missing-artifact behavior.
  - validation: typed alpha dashboard readability tests `2 passed`; demo-v2/static/golden dashboard focused tests `8 passed`.
- Phase 45 Typed Alpha Closeout Report is complete:
  - `scripts/build_typed_alpha_closeout_report.py` writes a deterministic local closeout memo.
  - `reports/typed_alpha_closeout_report.md` records Phase 35-42 scope, what the typed-alpha work proves, what it does not prove, known limitations, reproducibility commands, and next allowed work.
  - `tests/test_typed_alpha_closeout_report.py` checks builder output, committed-report determinism, and non-approval language.
  - `make typed-alpha-closeout` rebuilds the committed closeout report.
  - validation: typed alpha closeout report tests `3 passed`.
- Phase 44 Demo v2 Golden Snapshot Tests is complete:
  - `tests/golden/demo_v2_expected_manifest.json` records required demo-v2 artifact names, release-manifest keys, typed-alpha chain, and dashboard section headings without snapshotting full HTML.
  - `tests/test_demo_v2_golden_snapshot.py` runs `scripts/run_portfolioos_demo_v2.py` against a temp output directory and verifies artifact shape, manifest structure, required dashboard sections, read-only controls, and explicit Q2 unavailable-row semantics.
  - `src/portfolio_os/dashboard/static_dashboard.py` now renders the demo-v2 sections required by the golden contract while remaining static and read-only.
  - validation: demo-v2 golden snapshot tests `2 passed`; demo-v2/static dashboard focused tests `6 passed`.
- Phase 43 Typed Alpha Release Candidate Hardening is complete:
  - `src/portfolio_os/alpha/schema_versions.py` centralizes typed-alpha artifact schema versions.
  - AlphaView, PromotionDecisionV2, Q2InputContractV2, Q2 typed matrix rows, and paper overlay readiness summary now carry explicit schema versions.
  - `scripts/run_portfolioos_demo_v2.py` now writes `typed_alpha_release_manifest.json` with local-only status, typed-alpha chain, schema versions, explicit non-approval flags, and a deterministic content hash.
  - `docs/releases/typed_alpha_v0_1_release_candidate.md` documents what the typed-alpha release candidate completes and what it does not claim.
  - `tests/test_typed_alpha_release_candidate.py` guards schema version locks, demo-v2 release artifacts, unavailable Q2 row semantics, release manifest non-approval flags, and release-note non-claim language.
  - validation: typed alpha release candidate tests `4 passed`; AlphaView tests `6 passed`; Promotion Gate tests `12 passed`; Q2 typed matrix tests `3 passed`; paper overlay tests `3 passed`; demo-v2 tests `2 passed`; typed SUE pilot tests `2 passed`.
- Phase 42 Typed Alpha Demo v2 is complete:
  - `scripts/run_portfolioos_demo_v2.py` writes deterministic typed-alpha artifacts under `outputs/demo_v2/` by default.
  - `make demo-v2` runs the local SUE typed pilot, paper overlay readiness fixture, and read-only typed alpha dashboard renderer.
  - `src/portfolio_os/dashboard/static_dashboard.py` now includes a typed-alpha dashboard renderer with sections for Typed Alpha View, Event Evidence, Projection Diagnostics, Abstain Report, Q2 Typed Alpha Matrix, and Paper Overlay Calibration.
  - dashboard v2 remains static read-only and exposes no forms, POST methods, trade routes, order routes, broker routes, or workflow triggers.
  - validation: demo-v2/static dashboard tests `4 passed`; `make demo-v2` smoke passed; `make validate` passed.
- Phase 41 First Real Typed Alpha Pilot is complete:
  - `projects/typed_alpha_pilot/src/typed_alpha_pilot/pilot.py` builds deterministic local SUE artifacts across AlphaView, Event Evidence, Projection Bridge v2, Promotion Gate v2, and Q2 Typed Alpha Matrix.
  - output names include `us_sue_event_alpha_view.json`, `us_sue_event_evidence_bundle.json`, `us_sue_projection_panel.csv`, `us_sue_q2_matrix.csv`, and `us_sue_audit_report.md`.
  - the pilot uses SUE as an integration benchmark only and writes an audit report that states it is not production approval.
  - Q2 typed matrix rows remain unavailable and do not fabricate gross-to-net retention, turnover, or cost drag.
  - no live trading instruction, broker output, or live performance artifact is generated.
  - validation: typed SUE alpha pilot tests `2 passed`; `make validate` passed.
- Phase 40 Paper Overlay Calibration Lane is complete:
  - `src/portfolio_os/paper/overlay_readiness.py` aggregates local paper drift observations into readiness summary, latency-bucket, spread-capture, and markdown artifacts.
  - `scripts/run_paper_overlay_calibration_batch.py` is a local-only CLI over an observations CSV; it does not connect to Alpaca, submit orders, or run live paper sampling.
  - readiness output explicitly sets `alpha_promotion_allowed=false`, `production_config_update_allowed=false`, and `live_alpha_orders_allowed=false`.
  - the summary separates staleness bias, noise floor, and paper venue quirk notes and preserves max validated participation scope.
  - validation: paper overlay readiness tests `3 passed`; `make validate` passed.
- Phase 39 Q2 Typed Alpha Execution Matrix is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/typed_execution_matrix.py` consumes Q2InputContract v2 shape plus projection manifest, expected-return panel, projection diagnostics, and abstain report artifacts.
  - typed scenario rows cross existing cost, participation, liquidity, constraint, and execution-mode scenarios with projection policy, abstain policy, and alpha family dimensions.
  - rows retain deterministic `source_config_hash` values that include projection manifest hash and typed dimensions.
  - default rows remain `unavailable`; gross-to-net retention, turnover, and cost drag stay `None` until a typed Q2 execution adapter exists.
  - reports explain cost assumptions, constraint level, expected-return used share, active name count, abstain count, sign consistency, and view overlap without fabricating execution results.
  - validation: Q2 typed execution matrix tests `3 passed`; `make validate` passed.
- Phase 38 Promotion Gate v2 is complete:
  - `projects/promotion_gate/src/promotion_gate/schema.py` now defines `Q2InputContractV2` and `PromotionDecisionV2`.
  - `projects/promotion_gate/src/promotion_gate/gate.py` adds typed evaluation over `EvidenceBundle + AlphaView + ProjectionManifest` plus local artifact writing for `promotion_decision_v2.json`, `q2_input_contract_v2.json`, and `promotion_explanation_v2.md`.
  - v2 checks typed event AlphaView semantics, horizon fields, visibility before tradability, explicit abstain, projection manifest schema/content hash/active rows, abstain reporting, and analyst revision marginal-value disclosure.
  - SUE can promote to a projected expected-return-panel Q2 input contract when overlap disclosure is present; revision reaches `needs_more_evidence` without marginal-value disclosure.
  - forward-return AlphaView fixtures are rejected before any Q2 contract is emitted.
  - v1 Promotion Gate behavior is preserved; v1 tests still pass without `src` on `PYTHONPATH`.
  - validation: Promotion Gate tests `12 passed`; legacy v1-only Promotion Gate tests `7 passed`; `make validate` passed.
- Phase 37 Alpha Projection Bridge v2 is complete:
  - `src/portfolio_os/alpha/projection.py` projects typed AlphaViews into rebalance-period expected-return rows with horizon scaling, decay multipliers, confidence weights, and deterministic artifact writing.
  - `src/portfolio_os/alpha/projection_diagnostics.py` ranks projected optimizer-input expected returns and builds per-date diagnostics.
  - projection outputs include `expected_return_panel.csv`, `alpha_projection_manifest.json`, `alpha_projection_diagnostics.json`, and `alpha_abstain_report.json`.
  - SUE event views project only inside the event window; revision views project until `holding_window.next_event_timestamp`.
  - explicit abstain is preserved: `no_view`, missing coverage, and inactive windows are recorded in `alpha_abstain_report` instead of synthetic zero expected-return rows.
  - a synthetic sign-flip test verifies that projected expected-return signs change the optimizer-input ranking direction.
  - validation: Alpha Projection Bridge v2 tests `6 passed`; AlphaView/Event/Projection focused tests `16 passed`; `make validate` passed.
- Phase 36 Event-Aware Evaluation Kernel is complete:
  - `src/portfolio_os/alpha/event_evaluation.py` defines event-window and to-next-announcement label contracts plus deterministic event-evidence bundle/artifact writers.
  - `projects/alpha_view_contract/examples/event_sue_pead_view.json` expresses SUE event windows such as `[+2,+2]`, `[+2,+3]`, and `[+2,+22]`.
  - `projects/alpha_view_contract/examples/event_revision_view.json` expresses analyst revision from `statpers` to next trading day to next announcement with WRDS as the PIT source.
  - event artifacts include `event_evidence_bundle.json`, `event_window_grid.csv`, `event_half_life_summary.json`, `event_overlap_diagnostics.json`, `pit_visibility_report.json`, and `placebo_report.json`.
  - the kernel records planned half-life, placebo, PIT, and overlap diagnostics only; it does not compute realized alpha performance, orders, broker output, or trading instructions.
  - FMP analyst-estimate history is explicitly rejected as a PIT-safe analyst revision source.
  - validation: event-aware alpha evaluation contract tests `4 passed`; `make validate` passed.
- Phase 35 Typed Alpha View Contract is complete:
  - `src/portfolio_os/alpha/view_contract.py` defines `AlphaView`, PIT safety, coverage mask, abstain policy, expected-return entries, deterministic JSON load/dump helpers, and contract-level validation errors.
  - `projects/alpha_view_contract/` contains valid fixtures for SUE event view, revision-to-next-announcement view, and residual momentum calibration view plus a rejected forward-return leakage fixture.
  - `no_view` entries must carry a reason and cannot carry a value; zero expected-return values are allowed only as explicit `active_view` entries.
  - AlphaView rejects forbidden trading/live-output fields including orders, broker output, live performance, trading recommendations, trading instructions, and hidden Q2 results.
  - validation: AlphaView contract tests `6 passed`; `make validate` passed.
- Phase 34 README / Architecture / Case Study is complete:
  - top-level `README.md` now frames PortfolioOS as an audit-ready ML/quant decision evaluation platform.
  - README includes Problem, Solution, Architecture, Quickstart, Example Outputs, Safety Boundaries, Case Studies, and Validation sections.
  - architecture explicitly preserves Q1 -> Evidence Bundle -> Promotion Gate -> Q2 -> Audit Report boundaries.
  - case studies cover the promoted-like guidance-raise path and the rejected forward-return leakage path without claiming alpha success.
  - validation: README packaging tests `3 passed`; `make validate` passed.
- Phase 33 One-Command PortfolioOS Demo is complete:
  - `scripts/run_portfolioos_demo.py` now writes the deterministic local demo artifact set under `outputs/demo/`.
  - `make demo` generates `q1_summary.json`, `evidence_bundle.json`, `promotion_decision.json`, `q2_execution_matrix.csv`, `audit_report.md`, `run_manifest.json`, `trace.jsonl`, `cost_sensitivity.csv`, and `dashboard.html`.
  - the valid guidance-raise case reaches Q2 only through the promotion contract; the forward-return leakage case is rejected before Q2 and recorded in the audit report.
  - generated demo artifacts are under ignored `outputs/` and are not committed.
  - validation: one-command demo tests `2 passed`; `make demo` passed; `make validate` passed.
- Phase 32 Demo Dashboard is complete:
  - `src/portfolio_os/dashboard/` now renders a static read-only HTML dashboard from local artifact files.
  - dashboard sections cover candidate list, Q1 status, promotion decision, Q2 execution matrix, cost sensitivity, audit report, and reproducibility manifest.
  - missing artifacts are shown as unavailable, and the generated HTML intentionally contains no forms, POST routes, trade routes, broker routes, or order routes.
  - no Streamlit, React, server, or live workflow dependency was added.
  - validation: static dashboard tests `2 passed`; `make validate` passed.
- Phase 31 Read-Only Service Layer is complete:
  - `src/portfolio_os/service/` now provides a framework-neutral `ReadOnlyArtifactService` over local artifact directories.
  - supported routes are `GET /health`, `GET /runs`, `GET /runs/{run_id}`, `GET /bundles/{bundle_id}`, `GET /reports/{run_id}`, and `GET /decisions/{bundle_id}`.
  - non-GET requests return a read-only error; trade/order/broker routes are not exposed.
  - no FastAPI or server dependency was added because the current requirement is a local artifact access boundary rather than a network service.
  - validation: read-only service tests `4 passed`; `make validate` passed.
- Phase 30 Incremental Rerun / Content-Addressed Cache is complete:
  - `src/portfolio_os/cache/` now provides cache-key construction, JSON content-addressed storage, and field-level invalidation explanations.
  - cache keys include schema version, code version, input hash, optional config hash, runner version, seed, and optional extra fields.
  - `run_local_batch` can use an optional `ContentAddressedStore`; candidate results now surface `cache_status` and `cache_key`, and cached hits skip runner execution.
  - cache artifacts remain local JSON only and do not trigger Q1/Q2 workflows, brokers, live data, or trading outputs.
  - validation: content-addressed cache tests `5 passed`; local batch orchestrator tests `4 passed`; `make validate` passed.
- Phase 29 Batch Scaling / Local Orchestrator is complete:
  - `src/portfolio_os/orchestration/` now provides a deterministic local batch scheduler, bounded retry policy, JSON result store, and injected-runner batch executor.
  - batch runs sort candidate ids, support deterministic partial reruns, isolate per-candidate failures, classify `promoted`, `rejected`, `unavailable`, and `failed`, and aggregate status counts.
  - each candidate result writes a local JSON artifact plus a provenance manifest using the Phase 26 provenance machinery.
  - the orchestrator is generic and uses an injected runner, so it does not import Q1/Q2 workflows, call live services, trigger broker paths, or create trading instructions.
  - validation: local batch orchestrator tests `4 passed`; `make validate` passed.
- Phase 28 CI / Regression Hardening is complete:
  - `Makefile` now defines `test`, `lint`, `validate-examples`, `audit-report`, `demo`, `no-network`, and `validate` targets.
  - `src/portfolio_os/validation/no_network.py` blocks socket connection attempts during local validation; `scripts/devtools/no_network_guard.py` self-tests the guard without live service calls.
  - regression tests cover Makefile target presence, audit report golden-output coverage, schema compatibility for committed Evidence Bundle and Promotion Gate examples, no-network blocking, and forbidden-output guards across Q1 summary, promotion decisions, Q2 matrix rows, and the audit report.
  - validation: CI hardening tests `8 passed`; `make validate` passed.
- Phase 27 Observability / Structured Trace is complete:
  - `src/portfolio_os/observability/` now provides `TraceEvent`, `TraceWriter`, `StructuredTraceLogger`, and trace-event metrics.
  - trace payload sanitization drops secret-like keys and trading-output keys such as API keys, tokens, passwords, broker output, orders, live performance, and trading instructions.
  - `projects/audit_report/scripts/build_demo_audit_report.py` accepts `--trace-jsonl` and records local workflow events for schema validation, bundle loading, promotion decisions, Q2 unavailable scenarios, and report writes.
  - trace output is JSONL with deterministic key ordering; fixture determinism is preserved aside from runtime timestamps.
  - validation: observability trace tests `4 passed`; audit report trace CLI test `1 passed`; full audit report tests `5 passed`; demo audit report smoke with trace sidecar passed.
- Phase 26 Run Provenance / Reproducibility Manifest is complete:
  - `src/portfolio_os/provenance/` now provides hashing, artifact indexing, environment capture, manifest construction, command redaction, and sorted JSON writing.
  - provenance manifests record schema version, run id, runner version, timestamp, sanitized command, git SHA/dirty state, Python/platform environment, dependency snapshot placeholder, random seed, config artifact, input artifacts, output artifacts, and stable `content_hash`.
  - `content_hash` excludes runtime timestamp and uses sanitized commands so repeated fixture runs stay stable while config/input changes invalidate the hash.
  - `projects/audit_report/scripts/build_demo_audit_report.py` writes a provenance sidecar for the demo audit report; validation writes that sidecar under `/tmp` to avoid committing machine-specific timestamps.
  - validation: provenance manifest tests `4 passed`; audit report tests `4 passed`; demo audit report smoke with provenance sidecar passed.
- Phase 25 Unified Audit Report is complete:
  - `projects/audit_report/` builds the deterministic local demo report for the current Q1 -> Evidence Bundle -> Promotion Gate -> Q2 matrix pipeline.
  - `projects/audit_report/examples/demo_audit_manifest.yaml` defines one promoted-like guidance-raise case and one rejected forward-return-leakage case.
  - `projects/audit_report/src/audit_report/builder.py` loads local Q1 schemas, evidence bundles, promotion decisions, Q2 execution matrix rows, constraint diagnostics, and explanation tables into one markdown report.
  - rejected leakage cases are recorded as skipped before Q2 execution evaluation; default Q2 output remains structured unavailable rows without fabricated returns.
  - `reports/demo_audit_report.md` is generated from the committed manifest.
  - validation: audit report tests `4 passed`; demo audit report smoke passed.
- Phase 24 Decision Explainability layer is complete:
  - `src/portfolio_os/explain/rejection_taxonomy.py` defines `DecisionExplanation` plus deterministic explanations for forward-return leakage, timestamp/PIT failures, unsafe anchors, missing coverage, missing costs, unbounded horizons, Q2 adapter gaps, cost retention, and execution risk.
  - `src/portfolio_os/explain/promotion_explainer.py` explains promotion-gate decisions without importing project-specific schemas.
  - `src/portfolio_os/explain/optimizer_explainer.py` explains Q2 unavailable rows.
  - `src/portfolio_os/explain/report_sections.py` renders deterministic markdown explanation tables.
  - Q2 execution matrix rows now carry structured explanation metadata for unavailable rows.
  - validation: decision explainability tests `5 passed`; Q2 execution matrix tests `4 passed`.
- Phase 23 Q2 execution evaluation matrix is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/scenario_grid.py` builds deterministic scenario ids and source config hashes across cost, participation, liquidity, constraint, and execution-mode dimensions.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/execution_matrix.py` delegates scenario/layer runs to the existing ladder adapter and records observed vs unavailable matrix rows.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/robustness_summary.py` summarizes total scenarios, rows, observed rows, unavailable rows, unique config hashes, and unavailable reasons.
  - `projects/execution_aware_optimizer/scripts/run_execution_matrix.py` writes `execution_matrix.csv`, `robustness_summary.json`, and an execution-matrix markdown report.
  - default matrix config stays non-execution; the smoke produced 270 scenarios and 1,890 structured unavailable layer rows under `/tmp`, with no fabricated returns.
  - validation: Q2 tests `24 passed`; Q2 execution matrix smoke passed.
- Phase 22 Promotion Gate contract is complete:
  - `projects/promotion_gate/` is now a standalone contract layer between Evidence Bundle validation and Q2 execution-aware evaluation.
  - `projects/promotion_gate/src/promotion_gate/schema.py` defines `PromotionDecision` and `Q2InputContract`.
  - `projects/promotion_gate/src/promotion_gate/gate.py` evaluates local evidence bundles for reject, needs-more-evidence, or promote-to-execution-eval decisions.
  - the gate checks Evidence Bundle PIT/leakage validation, coverage requirements, cost-assumption presence, bounded evaluation horizon, forbidden output keys, and the no-direct-Q2-execution boundary.
  - passing decisions produce only a Q2 input contract with alpha-score columns; they do not import Q2 workflows, run PortfolioOS, create orders, or generate trading output.
  - validation: Promotion Gate tests `7 passed`; Evidence Bundle tests `4 passed`; Q1 tests `30 passed`; Q1 example validation script passed; Q1 batch manifest summary CLI smoke passed.
- Phase 21 Evidence Bundle schema is complete:
  - `projects/evidence_bundle/` is now a standalone project area for typed Q1 evidence packages.
  - `projects/evidence_bundle/src/evidence_bundle/schema.py` defines `EvidenceBundle`, `PitSafetyReport`, `LeakageCheck`, and `PlannedTest`.
  - `projects/evidence_bundle/src/evidence_bundle/validation.py` provides `load_evidence_bundle` and deterministic JSON serialization.
  - committed examples cover one valid guidance-raise bundle plus rejected forward-return leakage, missing timestamp, and anchor-before-signal-timestamp cases.
  - valid evidence bundles now include promotion-review cost assumptions and a bounded evaluation horizon for the Phase 22 gate.
  - evidence bundles explicitly exclude trading recommendations, orders, broker output, live performance, hidden Q2 results, and direct Q2 execution output.
  - validation: Evidence Bundle tests `4 passed`; Q1 tests `30 passed`; Q1 example validation script passed; Q1 batch manifest summary CLI smoke passed.
- Phase 20 Q1 evaluator batch contract note is complete:
  - `projects/agentic_alpha_triage/docs/evaluator_batch_contract.md` defines the local batch dry-run purpose, allowed schema-backed inputs, allowed detailed output, allowed summary output, forbidden output, forbidden behavior, real-evaluation boundary, and Q2 boundary.
  - the contract explicitly allows only planning/audit metadata such as ready count, rejected count, mismatch count, rejection reasons, and referenced fixture paths.
  - the contract explicitly forbids realized return, alpha performance, orders, trading instructions, PortfolioOS workflow output, Q2 exports, live FMP/SEC calls, LLM agent loops, PortfolioOS workflows, evaluator execution, and new data ingestion.
  - `projects/agentic_alpha_triage/tests/test_evaluator_batch_contract_doc.py` guards the key allowed/forbidden contract terms.
  - validation: Q1 tests `30 passed`; Q1 example validation script passed with `evaluator_plan_manifests=1`; Q1 batch manifest summary CLI smoke passed; Q1 batch manifest detail CLI smoke passed.
- Phase 19 Q1 manifest summary report is complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_plan_batch.py` now defines `EvaluatorPlanBatchSummary` plus `summarize_evaluator_plan_batch`.
  - `projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py` accepts `--summary` to print manifest id, total entries, ready count, rejected count, expected-status mismatch count, and mismatched entry ids.
  - summary output intentionally excludes detailed planner payloads, realized returns, alpha performance, orders, trading instructions, PortfolioOS workflow output, and Q2 exports.
  - validation: Q1 tests `29 passed`; Q1 example validation script passed with `evaluator_plan_manifests=1`; Q1 ready-plan CLI smoke passed; Q1 rejected-plan audit JSON smoke passed; Q1 batch manifest CLI smoke passed; Q1 batch manifest summary CLI smoke passed.
- Phase 18 Q1 batch dry-run manifest wrapper is complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_plan_batch.py` iterates committed evaluator-plan manifest entries deterministically and emits ordered ready/rejected planner payloads.
  - `projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py` prints the batch payload as JSON for an explicit local manifest path.
  - each batch entry records entry id, expected status, observed status, whether the expected status matched, manifest-relative fixture path, manifest-relative event-registry directory, and the underlying ready/rejected planner payload.
  - the batch wrapper reuses local schema-backed planner and rejection handling only; it does not call live SEC/FMP services, run LLM agent loops, run PortfolioOS workflows, produce trading outputs, or export to Q2.
  - validation: Q1 tests `27 passed`; Q1 example validation script passed with `evaluator_plan_manifests=1`; Q1 ready-plan CLI smoke passed; Q1 rejected-plan audit JSON smoke passed; Q1 batch manifest CLI smoke passed.
- Phase 17 Q1 evaluator-plan fixture manifest is complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_plan_manifest.py` defines a local-only manifest schema and loader for evaluator fixture paths, event-registry directories, expected statuses, and descriptions.
  - `projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml` lists one ready guidance-raise fixture target and one rejected forward-return-leakage target.
  - manifest loading validates schema, unique entry ids, and local referenced paths only; it does not execute evaluations, call live SEC/FMP services, run LLM agent loops, run PortfolioOS workflows, generate trading results, or export to Q2.
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/example_validation.py` now includes committed evaluator-plan manifests in example validation output.
  - validation: Q1 tests `25 passed`; Q1 example validation script passed with `evaluator_plan_manifests=1`; Q1 ready-plan CLI smoke passed; Q1 rejected-plan audit JSON smoke passed.
- Phase 16 Q1 rejected-plan JSON audit output is complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_planner.py` now defines `RejectedEvaluatorPlan` as an audit-only rejected response wrapper.
  - `projects/agentic_alpha_triage/scripts/plan_evaluator.py` accepts `--emit-rejected-json` and converts local schema/contract failures into structured rejected-plan JSON only when the flag is explicit.
  - default planner CLI rejection behavior remains nonzero without the flag.
  - rejected-plan JSON contains fixture path, event-registry path, status, and rejection reasons only; it does not contain realized returns, alpha performance, orders, trading instructions, PortfolioOS workflow output, or Q2 exports.
  - `projects/agentic_alpha_triage/tests/test_evaluator_plan_cli.py` covers ready output, default nonzero rejection, and opt-in rejected audit JSON.
  - validation: Q1 tests `22 passed`; Q1 example validation script passed with `evaluator_fixtures=1`, `rejected_evaluator_fixtures=1`, `event_registry_examples=1`, and `rejected_event_registry_examples=2`; Q1 ready-plan CLI smoke passed; Q1 rejected-plan audit JSON smoke passed.
- Phase 15 Q1 evaluator CLI dry-run wrapper is complete:
  - `projects/agentic_alpha_triage/scripts/plan_evaluator.py` prints a local `EvaluatorPlan` JSON payload from explicit fixture and event-registry paths.
  - the CLI delegates to `build_evaluator_plan`; it does not call live services, run agent loops, execute PortfolioOS workflows, compute returns, produce orders, or export to Q2.
  - `projects/agentic_alpha_triage/tests/test_evaluator_plan_cli.py` validates the CLI output and confirms it omits realized returns, alpha performance, and orders.
  - `VALIDATION.md`, `RUNBOOK.md`, and Q1 README document the CLI dry-run smoke command.
  - validation: Q1 tests `20 passed`; Q1 example validation script passed with `evaluator_fixtures=1`, `rejected_evaluator_fixtures=1`, `event_registry_examples=1`, and `rejected_event_registry_examples=2`; Q1 evaluator CLI dry-run smoke passed.
- Phase 14 Q1 dry-run evaluator planner is complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_planner.py` defines `EvaluatorPlan` and `build_evaluator_plan`.
  - the planner loads the valid guidance-raise evaluator fixture, referenced hypothesis, signal, evaluation contract, and compatible event registry examples into one non-executing plan.
  - planner checks reject signal-name mismatches, event-registry hypothesis mismatches, and evaluation timestamp-field mismatches before any evaluation can run.
  - planner output contains only local planning metadata, required columns, feature columns, holding windows, benchmark, cost assumptions, leakage checks, and placebo tests; it contains no realized returns, performance, orders, or Q2 export.
  - validation: Q1 tests `19 passed`; Q1 example validation script passed with `evaluator_fixtures=1`, `rejected_evaluator_fixtures=1`, `event_registry_examples=1`, and `rejected_event_registry_examples=2`.
- Phase 13 Q1 evaluator runner design is complete:
  - `projects/agentic_alpha_triage/docs/evaluator_runner_contract.md` defines the local-only dry-run planner boundary.
  - the contract specifies allowed local inputs, required assembly checks, planned output fields, and explicit non-responsibilities.
  - Q1 README now points to the runner contract and reiterates that the runner must not call live SEC/FMP services, run LLM agent loops, execute PortfolioOS workflows, or export directly to Q2.
  - no code runner was added in this phase; implementation is deferred to Phase 14 so it can be test-first.
  - validation: Q1 tests `15 passed`; Q1 example validation script passed with `evaluator_fixtures=1`, `rejected_evaluator_fixtures=1`, `event_registry_examples=1`, and `rejected_event_registry_examples=2`.
- Phase 12 Q2 executed fixture report polish is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/reports.py` now renders PortfolioOS adapter execution status and layer coverage before the detailed ladder table.
  - `projects/execution_aware_optimizer/configs/local_executed_fixture_report.yaml` is an explicit opt-in local fixture config for the executed report smoke path.
  - `projects/execution_aware_optimizer/README.md`, `RUNBOOK.md`, and `VALIDATION.md` document the local-only executed report command with outputs under `/tmp`.
  - default Q2 configs remain non-execution, and the default Q2 smoke scripts still produce unavailable rows rather than fabricated results.
  - validation: Q2 tests `20 passed`; default Q2 alpha-decay, cost-sensitivity, and constraint-diagnostics smoke scripts passed; explicit local executed fixture report smoke passed.
- Phase 11 Q2 executed adapter fixture is complete:
  - `projects/execution_aware_optimizer/tests/test_portfolioos_adapter.py` now includes a local PortfolioOS-backed fixture that calls `run_alpha_decay_ladder` with `portfolio_os.backtest.engine.run_backtest`.
  - the fixture uses `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml` and requires explicit `allow_portfolioos_run=true` inside the test config.
  - raw and full execution-aware layers produce observed `net_return` rows; the representative intermediate layer remains explicitly unavailable.
  - default Q2 configs still keep `allow_portfolioos_run=false`, and the fixture writes no report artifacts.
  - validation: Q2 tests `18 passed`; default Q2 alpha-decay, cost-sensitivity, and constraint-diagnostics smoke scripts passed.
- Phase 10 Q2 executed adapter fixture planning is complete:
  - decision note: `projects/execution_aware_optimizer/docs/executed_adapter_fixture_plan.md`.
  - inspection confirmed Q2 maps only PortfolioOS `period_attribution` strategies `alpha_only_top_quintile` and `optimizer` into stable ladder rows.
  - a read-only local probe using `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml` returned observed rows through `run_alpha_decay_ladder` without live services or report writes.
  - selected next fixture scope: direct library call, local manifest, raw and full execution-aware mapped layers, and explicit unavailable intermediate layer.
  - default Q2 configs remain `allow_portfolioos_run=false`.
- Phase 9 Q1 event-registry example fixtures are complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/event_registry_schema.py` now includes `EventRegistryExample` plus YAML loaders for static examples.
  - `projects/agentic_alpha_triage/examples/event_registry/valid/guidance_raise_event.yaml` records a timestamp-safe guidance-raise event for the existing Q1 story.
  - `projects/agentic_alpha_triage/examples/event_registry/invalid/` contains missing-timestamp and anchor-before-event negative examples that must be rejected.
  - Q1 example validation now reports `event_registry_examples=1` and `rejected_event_registry_examples=2`.
  - validation: Q1 tests `15 passed`; Q1 example validation script passed.
- Phase 8 Q1 evaluator example fixtures are complete:
  - `projects/agentic_alpha_triage/src/agentic_alpha_triage/evaluator_fixture.py` defines a schema-backed evaluator fixture loader.
  - `projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml` references the existing guidance-raise hypothesis, signal, and evaluation examples.
  - `projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml` is a committed negative fixture that must be rejected because it uses forward returns as features.
  - Q1 README now distinguishes Q1 evaluator fixtures from Q2 execution checks.
  - validation: Q1 tests `11 passed`; Q1 example validation script passed with `evaluator_fixtures=1` and `rejected_evaluator_fixtures=1`.
- Phase 7 Q2 cost-sensitivity report reader is complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/cost_sensitivity.py` loads cost-sensitivity CSV rows into typed `CostSensitivityResultRow` records.
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/reports.py` renders cost-bps/layer summaries for supplied cost-sensitivity rows.
  - default non-execution rows remain unavailable in both CSV and report output.
  - validation: Q2 tests `16 passed`; default Q2 smoke scripts passed.
- Phase 6 Q2 real-output report tables are complete:
  - `projects/execution_aware_optimizer/src/execution_aware_optimizer/reports.py` renders gross/net summary tables from observed ladder rows.
  - alpha-decay summary is computed versus `raw_top_alpha_equal_weight` only when raw net-return observations exist.
  - unavailable layers remain `Not available` and count as unavailable rows instead of receiving synthetic numbers.
  - validation: Q2 tests `13 passed`; default Q2 smoke scripts passed.
- Phase 5 Q1 contract examples are complete:
  - one valid example hypothesis lives at `projects/agentic_alpha_triage/examples/hypothesis_guidance_raise_drift.yaml`.
  - one valid example signal contract lives at `projects/agentic_alpha_triage/examples/signal_guidance_raise_drift.yaml`.
  - one valid example evaluation contract lives at `projects/agentic_alpha_triage/examples/evaluation_guidance_raise_drift.yaml`.
  - schema-backed validation lives in `projects/agentic_alpha_triage/src/agentic_alpha_triage/example_validation.py`.
  - smoke command: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py`.
  - validation: Q1 tests `8 passed`; example validation script passed.
- Phase 4 Q2 adapter hardening is complete:
  - fixture-backed adapter test maps existing PortfolioOS `alpha_only_top_quintile` attribution into `raw_top_alpha_equal_weight` rows and `optimizer` attribution into `full_execution_aware_cost_adjusted` rows.
  - independent Q2 alpha fixture lives at `projects/execution_aware_optimizer/fixtures/example_alpha_scores.csv`.
  - non-mutating cost-sensitivity scenarios live in `projects/execution_aware_optimizer/src/execution_aware_optimizer/cost_sensitivity.py`.
  - Q2 README documents each ladder layer as partial or unavailable.
  - validation: Q2 tests `11 passed`; relevant PortfolioOS subset `64 passed, 36 warnings`; default Q2 smoke scripts passed without enabling PortfolioOS execution.
- Next recommended repo workflow phase: Phase 65 PortfolioOS v1 Research-Audit
  Release. Phase 56-58 are technically unblocked by the clean local SUE Q2
  fixture but remain optional governance/paper-environment work; Phase 59-61
  remain locked without explicit human approval. Do not open new alpha research,
  broker/order workflows, or paper canary approval paths. Factor Discovery
  Sandbox is only a Phase 64 candidate charter until explicitly approved as a
  research import task.
- Core platform buildout through Phase 12 is implemented and stable.
- The current project-wide meta stage is `research convergence + promotion contract`, not repo merge.
- Project operating mode is now `paper-stage only`.
- Default rule: freeze new research, new integrations, and new optimization work unless they are directly required by the live paper-stage path.
- Canonical orchestration path: `src/portfolio_os/workflow/single_run.py`.
- Historical backtests should run through library calls, not CLI subprocess chains.
- Default execution simulation mode: `impact_aware`.
- `participation_twap` is intentionally preserved in sample execution requests as the baseline comparison mode.
- Latest full regression on this machine: `python -m pytest -q` -> `310 passed, 38 warnings`.

## Active Worktree Topology

- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01` main research repo = home of the closed US WRDS method asset; read `docs/us_wrds_alpha_roadmap.md` and `docs/us_wrds_memory.md` there instead of treating US as an active worktree.
- `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1` = the only active A-share research line.
- `C:\Users\14574\Quant\PortfolioOS` main repo = shared platform changes only; do not leave branch-local research copies here.
- `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze` = current strategy-doc worktree for closing the old US qualification sprint and drafting the next discovery charter; no active implementation should begin here before the new charter is reviewed.

## Stable Platform State

### Research Governance

- The narrow `US alpha core restart` qualification sprint is formally closed with `no winner`:
  - see `docs/strategy/us_alpha_core_week4_stop_go_note_2026_04_16.md`
- The next research move is no longer "expand the old frozen candidate field".
- The next active design object is a discovery-first charter:
  - `docs/strategy/alpha_discovery_charter_v2_2026_04_16.md`
- The new proposed order is:
  - calibration family first = `US residual momentum / residual reversal`
  - then a primary mining family = `A-share state-transition microstructure`
- Current implementation slice = `Phase 0 + calibration-family D1 kickoff`
- `docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md` now records why the primary family is chosen by structural edge rather than convenience
- `docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md` is now the next document gate before any calibration-family implementation or experiment setup
- No primary-family mining should begin before a calibration-family closeout exists
- First executable calibration slice is now live:
  - module = `src/portfolio_os/alpha/discovery_calibration.py`
  - runner = `scripts/run_us_residual_momentum_calibration.py`
  - tests = `tests/test_alpha_discovery_calibration.py`
  - first artifacts = `outputs/us_residual_momentum_calibration/2026-04-16/`
- Current calibration read:
  - harness is operational
  - but calibration is **not yet validated**
  - strongest control (`CTRL1_SHUFFLED_PLACEBO`) currently reads stronger than the best live expression (`RM3_VOL_MANAGED`)
  - therefore the discovery machine should be treated as still under calibration, not yet trusted for family-winner promotion
- Primary-family mining remains blocked until calibration produces a credible closeout
- Important methodological change:
  - discovery is now organized around mechanism-bearing families, not frozen single-factor tournaments
  - qualification becomes a downstream handoff phase rather than the discovery admission filter
  - the primary family is selected by structural edge, not by code convenience or continuity with the failed US restart sprint

### Data And Inputs

- Provider/builder infrastructure for `market.csv`, `reference.csv`, `target.csv`, and snapshot bundles is complete.
- Tushare permission-aware fallbacks are implemented.
- Because current Tushare `index_weight` access is still limited, client-provided `target.csv` remains the official fallback when live snapshot generation is incomplete.
- Frozen expanded-US research assets are the canonical US fundamentals workspace:
  - `data/universe/us_equity_expanded_tickers.txt`
  - `data/universe/us_universe_reference.csv`
  - `data/universe/us_universe_market_2026-03-27.csv`
  - `data/risk_inputs_us_expanded/`
  - `C:\Users\14574\Quant\fmp_data_freeze`
- Use the frozen FMP workspace for resumed US fundamentals or transcript work; do not reopen the old `yfinance` path.
- Important FMP caveat: frozen analyst-estimate payloads are not PIT-safe for analyst-revision research because they do not include historical snapshot metadata.
- WRDS bootstrap research infrastructure is now live in the external workspace under:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\run_w1.py`
  - `...\scripts\wrds_ibes_w3.py`
  - `...\scripts\wrds_eval_adapter.py`
  - `...\outputs\wrds_bootstrap\`
- Stable WRDS research notes:
  - `W1` dynamic universes were validated cleanly: `top_500_dynamic` stays at exactly `500` names and `rank_500_1500_dynamic` stays at exactly `1000`
  - `wrds.iclink` is not available on this account; the working IBES-CRSP link uses local `ibes.idsum` CUSIP matching with `sdates`-aware validity handling
  - for analyst and event research, WRDS is now the canonical PIT source; do not use the old FMP estimate history as a substitute

### Execution, TCA, And Costs

- Expanded-US replay/sample validation for `sample_us_04` to `sample_us_06` is already complete; the stack is healthy enough for further research work.
- Fill collection preserves `reference_price` end to end.
- BOM-tolerant JSON handling is required for some calibration and broker-state artifacts.
- The constrained fill batch generator already supports broker positions, buying-power limits, and audit manifests.
- Current account scale only supports realistic participation around `0-0.1%` on the frozen 50-name US universe.
- Paper calibration sprint is now live as a platform-validation lane:
  - dry-run contract path is implemented via `portfolio_os.api.cli paper-calibration`
  - first neutral Alpaca paper run completed on `2026-04-15`
  - canonical live calibration read is:
    - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v3`
    - `fill_rate = 100%`, `partial_count = 0`, `rejected_count = 0`
    - dedicated pre-trade reference snapshot is now captured:
      - `captured_ticker_count = 1`
      - `fallback_reference_count = 0`
      - `quoted_mid_price = 697.33`
      - `quoted_spread_bps = 0.86`
    - `requested_notional = 697.33`
    - `filled_notional = 697.36`
    - `reconciliation matched_count = 12`, `mismatched_count = 0`
  - important scope rule:
    - treat this as platform calibration, not alpha validation
    - current sample is still tiny and not yet a stress / slippage distribution sample
  - repeated-sampling tooling now exists:
    - `paper-calibration --repeat N --interval-seconds X`
    - `paper-calibration-aggregate --input-root ... --output-dir ...`
  - current recommended next step on this lane:
    - collect a small repeated `SPY x 1 share` tranche (`~30-50` runs)
    - aggregate drift, half-spread scaling, latency slope, and time-of-day buckets
    - then decide whether the result is:
      - systematic staleness bias
      - microstructure noise floor
      - or paper-venue-specific quirk
- Low-participation TCA closure is done:
  - `overlay_readiness = sufficient`
  - `candidate_k = 3.498400399110418`
  - scope = paper overlay only
- Do not extrapolate the calibration above `0.1%` participation.
- Do not promote calibrated `k` into `config/us_expanded.yaml` yet.
- Calibrated research config exists at `config/us_expanded_tca_calibrated.yaml`.
- The main estimator follow-up is to include negative-signal eligible fills; useful, but not blocking the current low-participation closure.

### Optimizer And Risk Conclusions

- Cost-model sweeps show a structural problem, not just `k` tuning:
  - without a real alpha / expected-return term, the optimizer mainly differentiates itself by suppressing trades
  - under realistic costs, that suppression gives up more holding return than it saves
- Risk-aversion tooling is implemented (`portfolio-os-risk-sweep`), but no Sharpe-improving sweet spot was found.
- Conclusion: better alpha is the main bottleneck, not more optimizer tuning or a risk-aware default objective.
- Project-level structural decision is now frozen in:
  - `docs/strategy/single_period_vs_multi_period_structure_decision_memo_2026_04_15.md`
- Current read from that memo:
  - immediate full multi-period portfolio-construction work is **not** approved
  - the present issue is a mix of alpha weakness, objective-shape limitations, and missing intertemporal allocation logic
  - reopen multi-period only if a real alpha package later proves that single-period myopia is the next binding loss source
- Immediate optimizer-interface design reference is now:
  - `docs/strategy/objective_function_units_spec_2026_04_15.md`
- Current implementation order from that spec:
  - align alpha, cost, and covariance to one rebalance-period horizon
  - remove `target_deviation` from the core economic objective
  - treat cash deployment as a constraint design problem, not as a hidden objective anchor
  - rerun continuous-solution ablations before reopening signal research or multi-period work
- As of `2026-04-15`, the first objective-units implementation pass is now in code:
  - default `transaction_cost_objective_mode` is `nav_fraction`
  - risk-enabled objective paths now use the economic core only:
    - `risk_term`
    - `tracking_error`
    - `transaction_cost`
    - `alpha_reward`
  - legacy `target_deviation` / fee / turnover / slippage penalties no longer re-enter the risk-enabled objective through `augment`
  - walk-forward alpha snapshots now deannualize `annualized_top_bottom_spread` into a rebalance-period `expected_return`
  - backtest alpha panels now carry:
    - `period_top_bottom_spread`
    - `decision_horizon_days`
  - risk covariance is now scaled from annualized space to the same decision horizon when `decision_horizon_days` is supplied in-universe
- First post-fix local sanity checks:
  - on the frozen US expanded alpha sample at rebalance date `2025-10-31` with next rebalance `2025-11-28` (`decision_horizon_days = 19`):
    - objective decomposition shares were approximately:
      - `transaction_cost = 45.4%`
      - `risk_term = 31.0%`
      - `alpha_reward = 16.5%`
      - `tracking_error = 7.2%`
    - solver continuous gross traded notional was about `$452k`
    - repair preserved `20` executable instructions
  - practical read:
    - the optimizer is no longer collapsing into near-zero continuous trades solely because of raw-currency cost vs annualized-alpha mismatch
    - any remaining quality issues should now be treated as genuine portfolio-construction or signal problems, not as the old objective-unit bug
- Optimizer-isolated synthetic-alpha proof is now also live:
  - runner = `scripts/run_optimizer_isolated_acceptance.py`
  - module = `src/portfolio_os/optimizer/acceptance_proof.py`
  - artifacts live under:
    - `outputs/optimizer_isolated_acceptance_2026-04-16/`
  - current realistic-context proof read on rebalance date `2025-10-31`:
    - positive synthetic scales `0.5x -> 1.0x -> 2.0x` all solved `optimal`
    - `alpha_share_abs_weighted` rose monotonically from `0.1136 -> 0.2883 -> 0.4921`
    - base-alpha alignment stayed strongly positive (`~0.96`, `~0.94`, `~0.86`)
    - sign-flip case reversed relative to the same base-alpha ordering:
      - `base_alignment_spearman ~ -0.93`
      - `base_top_minus_bottom_weight_delta ~ -0.0300`
    - repair retained most of the continuous solution gross notional (`~88%` at `0.5x`, `~96%` at `1.0x`, `~98%` at `2.0x`)
  - practical interpretation:
    - this is strong evidence that the optimizer can structurally receive alpha once alpha is injected directly into `expected_return`
    - it is **not** yet proof of full time-series reception under the real bridge, because the real bottleneck still includes sparse alpha activation upstream
- Alpha-bridge gating diagnosis is now also frozen in:
  - note = `docs/strategy/alpha_bridge_spread_floor_investigation_note_2026_04_15.md`
  - current `spread floor` is an intentional one-sided non-reversal guard from the original Phase 1.5 design, not an accidental implementation leftover
  - on the corrected walk-forward sample:
    - `alpha_ready_months = 4`
    - `nonzero_alpha_months = 1`
    - `spread_floor_to_zero_months = 3`
  - practical implication:
    - do not treat `risk_term = 0.3` time-series confirmation as the next irreversible step until spread-floor semantics are either kept explicitly or replaced explicitly
- Alpha-bridge semantic closeout is now also frozen in:
  - contract = `docs/strategy/alpha_bridge_semantic_contract.md`
  - offline guard-event comparison artifacts live under:
    - `outputs/alpha_bridge_semantic_comparison_2026-04-15/`
  - three-event read:
    - `floor_to_zero` and `explicit_abstain` were behaviorally identical under the current objective
    - `signed_spread` was the only protocol that changed optimizer behavior materially
    - it raised mean guard-event turnover from about `1.15%` to about `2.17%`
  - semantic decision:
    - selected contract = `explicit_abstain`
    - rationale = cleaner "no alpha view" semantics with no current behavioral cost versus `floor_to_zero`
  - implementation read:
    - runtime can remain on `floor_to_zero` for now because current behavior matches `explicit_abstain`
    - if later downstream consumers distinguish zero-valued alpha vectors from missing alpha coverage, align implementation to the contract explicitly before promotion
- Real alpha package audit is now also live:
  - runner = `scripts/run_real_alpha_package_audit.py`
  - helper = `src/portfolio_os/alpha/package_audit.py`
  - artifacts live under:
    - `outputs/real_alpha_package_audit_2026-04-16/`
  - current read on `manifest_us_expanded_alpha_phase_1_5.yaml`:
    - `rebalance_count = 12`
    - `alpha_ready_count = 6`
    - `alpha_active_count = 2`
    - corrected terminal-state taxonomy:
      - `cold_start_count = 6`
      - `insufficient_history_count = 1`
      - `spread_floor_to_zero_count = 3`
      - `guard_zero_count = 3`
    - among active months, realized mapping is currently wrong on average:
      - `mean_rank_ic ~ -0.1206`
      - `positive_rank_ic_ratio = 0`
      - `mean_realized_top_bottom_spread ~ -7.85%`
      - `spread_sign_match_ratio = 0`
    - thickness is not the first failure mode:
      - gross active trading pnl `~ 562.35`
      - trading cost pnl `~ -172.52`
      - net active pnl `~ 389.82`
      - gross-to-net retention `~ 69.3%`
  - practical interpretation:
    - the optimizer is now largely cleared as the first-order bottleneck
    - the current real alpha package fails mainly because activation is sparse and the few active months currently map the wrong way
    - however the wrong-way read remains low-confidence at `N=2`; treat it as an observation, not yet a structural conclusion
    - the next justified work is alpha-package / signal-side diagnosis, not renewed optimizer tuning
  - diagnostic-only counterfactual spread-floor audit is now also live:
    - artifacts live under:
      - `outputs/real_alpha_package_audit_counterfactual_signed_2026-04-16/`
    - method:
      - keep the same backtest and optimizer
      - only replace `spread_floor_to_zero` months with a diagnostic `signed_spread` counterfactual
      - do **not** treat this as a production recommendation
    - current read:
      - `counterfactual_promoted_count = 3`
      - active sample expands from `2` to `5`
      - realized mapping improves only modestly:
        - `mean_rank_ic` moves from `~ -0.1206` to `~ -0.0686`
        - `positive_rank_ic_ratio = 20%`
        - `spread_sign_match_ratio = 20%`
      - thickness worsens materially:
        - gross active trading pnl drops to `~ 53.46`
        - net active pnl turns negative at `~ -176.29`
    - practical interpretation:
      - opening the spread floor is useful diagnostically because it expands the sample
      - but it does **not** rescue the package economically
      - current evidence is therefore:
      - sparse activation is definitely one bottleneck
      - removing the floor does not reveal a hidden good package underneath
      - any next step should stay diagnostic rather than promote a looser production guard
  - pointwise production-vs-canonical consistency diagnostic is now also live:
    - runner = `scripts/run_signal_consistency_diagnostic.py`
    - module = `src/portfolio_os/alpha/signal_consistency_diagnostic.py`
    - artifacts live under:
      - `outputs/signal_consistency_diagnostic_2026-04-16/`
    - provenance is now frozen per run:
      - `portfolioos_head_sha` with dirty-state suffix
      - canonical builder path/name
      - canonical signal spec (`21/84/21`, momentum-only, `21d` forward)
      - pooled method = `concat_then_correlate`
    - current read:
      - baseline production view preserves the canonical signal almost exactly on its two active months:
        - pooled `alpha_score` vs canonical Spearman `~ 0.9982`
        - pooled `expected_return` vs canonical Spearman `~ 0.9161`
        - top/bottom overlap remains high
      - signed-spread counterfactual preserves `alpha_score` ordering but breaks expected-return mapping on the three promoted months:
        - pooled `alpha_score` vs canonical Spearman `~ 0.9983`
        - pooled `expected_return` vs canonical Spearman `~ -0.0444`
        - `2025-11-28`, `2025-12-31`, and `2026-01-30` each flip to near `-1.0` month-level expected-vs-canonical Spearman
    - practical interpretation:
      - baseline production does **not** appear to be mechanically distorting the signal on the months where it is active
      - the signed-spread expansion mainly adds months whose expected-return mapping is misaligned with the canonical ordering
      - this supports the current read that spread floor can be locally useful as a small-sample filter even though long-horizon work does **not** support a broader crash-protection interpretation
      - next justified step is to accumulate more production sample before reopening signal re-engineering
- Narrow US long-horizon signal extension is now also live:
  - runner = `scripts/run_us_long_horizon_signal_extension.py`
  - helper = `src/portfolio_os/alpha/long_horizon.py`
  - artifacts live under:
    - `outputs/us_long_horizon_signal_extension_2026-04-16/`
  - important implementation correction:
    - factor attribution is now period-aligned by calendar month, not by exact date equality between trading month-end and calendar month-end
    - this materially weakens the old over-strong read that the operational package itself was just a standard momentum proxy
  - current proxy read on the same frozen 50-name expanded-US universe:
    - yfinance-adjusted close history covers all `50/50` names from roughly `2006-05-01` to `2026-04-15`
    - native-horizon (`5d`) monthly top-bottom spread remains momentum-adjacent, but only moderately:
      - `Mom beta ~ +0.2377`, `t ~ +2.69`
      - post-2010 robustness drops materially:
        - `Mom beta ~ +0.0633`, `t ~ +0.60`
      - native spread still also carries non-MOM style tilt:
        - `HML beta ~ +0.2848`
        - `QMJ beta ~ -0.1764`
    - deployable operational-horizon (`21d`) spread is **not** materially explained by MOM:
      - full-sample `Mom beta ~ +0.0158`, `t ~ +0.13`
      - post-2010 `Mom beta ~ -0.0206`, `t ~ -0.12`
    - Layer B Stage 2 bad-month cohort decomposition is now also live on the raw operational `21d` spread:
      - worst-quintile cohort size = `47` months
      - split = `outer_half 23`, `inner_half 24`, `non_bad 187`
      - temporal distribution is not identical:
        - `outer_half median_year ~ 2019`
        - `inner_half median_year ~ 2014.5`
        - so the read still carries a real time-mix caveat
      - pre-2010 historical size coverage is effectively unavailable from the current yfinance shares proxy:
        - `historical_shares_coverage_ratio = 0` for all pre-2010 cohort rows
        - treat size-bucket evidence as post-2010-heavy and only directional
      - relative to non-bad bootstrap nulls, `outer_half` and `inner_half` look materially closer on:
        - `pre_vol_bucket`
        - `leg_hhi`
        - `leg_effective_n`
        - `long_short_attribution`
      - sector and industry mixes also look more similar than random non-bad comparisons, but with the static-label caveat:
        - labels are current-as-of-analysis-date, not historical classifications
      - `pre_return_bucket` does **not** show the same clean same-type read:
        - `outer_inner` distance is roughly at the non-bad null median
        - this weakens any claim that left-tail months share one uniform winner/loser composition story
      - practical interpretation:
        - the current best framing is `broad left-tail vulnerability with partial structural consistency`, not `identified independent crash mode`
        - B3 macro conditioning should stay deferred until there is either a cleaner same-type read or better data coverage
    - operational-horizon (`21d`) monthly stress view still includes clear negative windows:
      - `2009-03-31 spread ~ -19.7%`
      - `2025-10-31 spread ~ -10.4%`
      - worst observed month in this proxy sample was `2022-12-30` at `~ -35.5%`
      - `2026-02-27` remains a weaker negative active month at roughly `-3.5%`
    - conditional decomposition continues to weaken the old self-gating intuition:
      - high trailing `12m` market state mean spread is roughly flat-to-negative (`~ -0.03%`)
      - low trailing `12m` market state mean spread stays modestly positive (`~ +0.43%`)
      - positive trailing signal-spread state actually underperforms nonpositive state on average (`~ -0.40%` vs `~ +0.88%`)
    - MOM residual analysis on the three key crash windows (`2009-03`, `2022-12`, `2025-10`) now says:
      - native `5d` mean matching absorption share is only `~ 15.1%`
      - operational `21d` mean matching absorption share is only `~ 0.3%`
      - both classify as `independent_residual`
      - concrete examples:
        - native `2009-03` is only partially momentum-absorbed (`~45%`)
        - native `2022-12` and `2025-10` are not absorbed; `2022-12` is actually offset by positive MOM
        - operational `2009-03`, `2022-12`, and `2025-10` are all almost entirely residual to MOM
  - practical interpretation:
    - this is now a two-layer read, not a one-layer label:
      - the signal is momentum-adjacent at its native `5d` horizon
      - but the current deployable `21d` package losses are mostly **not** explained away by standard MOM exposure
    - the current `spread floor` should be treated as a weak semantic filter, not as real crash protection
    - standard momentum crash protection is therefore **not** yet the default next template for this package
    - if this line continues, the next research question is custom residual crash mode / regime handling, not direct BSC/DM porting
    - this remains a fast proxy study until rerun on CRSP-grade history; do not over-upgrade it into a final research verdict

## US Research State

- Status: reopened as a tightly scoped US factor-layer qualification sprint under an 8-week charter; this is a new line, not a reopen of Branch A.
- Last sync: `2026-04-16`
- Branch-local references:
  - roadmap = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_alpha_roadmap.md`
  - memory = `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\docs\us_wrds_memory.md`
  - charter = `C:\Users\14574\Quant\PortfolioOS\docs\strategy\us_alpha_core_restart_charter_2026_04_16.md`
  - week1 checklist = `C:\Users\14574\Quant\PortfolioOS\docs\superpowers\plans\2026-04-16-us-alpha-core-restart-week1.md`
  - week1 baseline registry = `C:\Users\14574\Quant\PortfolioOS\docs\strategy\us_alpha_core_week1_baseline_registry_2026_04_16.md`
  - candidate definition sheet = `C:\Users\14574\Quant\PortfolioOS\docs\strategy\us_alpha_core_candidate_definition_sheet_2026_04_16.md`
  - qualification artifact contract = `C:\Users\14574\Quant\PortfolioOS\docs\strategy\us_alpha_core_qualification_artifact_contract_2026_04_16.md`
- Stable read:
  - data quality and evaluation-horizon mismatch were both real bottlenecks
  - `announcement-timed SUE` is the main US event alpha
  - finalized event-aware `revision` is real, but fixed-horizon mainline ingestion does not recover most of its edge
  - naive hybrid-v1 fallback is already rejected
  - same-event package qualification also failed:
    - same-event `SUE x revision` correlation stayed low
    - but the simple `SUE + revision` package still underperformed pure `SUE`
    - redesigned event-aligned mainline delta was not admitted from that branch
  - package-local diagnosis is now closed:
    - no package correctness bug was found
    - `signed_spread` is a by-design abstain case, not a repair direction
    - the only plausible package-side tweak is the small `min_evaluation_dates: 20 -> 19` coverage sidecar
  - Week 1 freeze is now materially defined:
    - the baseline registry explicitly separates:
      - current platform-native comparable baselines
      - external WRDS methodological benchmarks
      - frozen family references without current-platform scorecards
    - the eight-candidate field is frozen with exact formulas, direction, cadence, and PIT assumptions
    - the qualification artifact bundle is frozen so Week 2-3 can run under one report shape
  - Week 4 stop/go closeout is now also written:
    - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\docs\strategy\us_alpha_core_week4_stop_go_note_2026_04_16.md`
- Current active branch:
  - US factor-layer restart has entered Week 2 under a new platform-native qualification engine:
    - implementation path:
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\src\portfolio_os\alpha\qualification.py`
    - first-pass Family A artifacts now exist under:
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\A1\2026-04-16\`
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\A2\2026-04-16\`
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\A3\2026-04-16\`
    - first-pass Family C artifacts now also exist under:
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\C1\2026-04-16\`
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\C2\2026-04-16\`
    - Family B has now been unblocked on the checked-in sample via a local daily dollar-volume panel:
      - generated liquidity input:
        - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\liquidity_inputs\2026-04-16\liquidity_long.csv`
      - first-pass Family B artifacts now exist under:
        - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\B1\2026-04-16\`
        - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\B2\2026-04-16\`
        - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\B3\2026-04-16\`
    - cross-family first-pass synthesis artifact:
      - `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\outputs\us_alpha_core_restart\week2_interim_synthesis\2026-04-16\week2_interim_synthesis.md`
  - First-pass read on the checked-in platform-native sample (`50` expanded-US names, not full `rank_500_1500`):
    - `A1`:
      - `oos_mean_rank_ic ~ 0.0078`
      - `oos_alpha_only_tstat ~ -0.07`
      - fails admission, subperiod, and orthogonality
    - `A2`:
      - `oos_mean_rank_ic ~ 0.0358`
      - `oos_alpha_only_tstat ~ 1.10`
      - best Family A candidate so far
      - passes orthogonality and winner-increment reads
      - still fails admission (`rank_ic_tstat < 2`, `alpha_only_tstat < 2`, `gross_to_net_retention < 0.50`)
      - narrowly misses subperiod gate because weakest slice is slightly negative
    - `A3`:
      - `oos_mean_rank_ic ~ 0.0240`
      - `oos_alpha_only_tstat ~ -0.05`
      - passes orthogonality and winner-increment reads
      - fails admission and subperiod
    - `C1`:
      - `oos_mean_rank_ic ~ 0.1209`
      - `oos_rank_ic_tstat ~ 1.45`
      - `oos_alpha_only_tstat ~ 0.58`
      - `gross_to_net_retention ~ 0.64`
      - `spread_corr_vs_baseline ~ -0.28`
      - strongest raw rank-IC read so far, but coverage is too thin (`coverage_median ~ 0.40`) and it still fails admission / subperiod
    - `C2`:
      - `oos_mean_rank_ic ~ -0.0784`
      - `oos_rank_ic_tstat ~ -1.14`
      - `oos_alpha_only_tstat ~ -1.22`
      - `spread_corr_vs_baseline ~ 0.51`
      - direction is wrong on the current sample and it fails admission / subperiod
    - `B1`:
      - `oos_mean_rank_ic ~ -0.0395`
      - `oos_rank_ic_tstat ~ -0.49`
      - `oos_alpha_only_tstat ~ -0.69`
      - fails economically despite clean coverage (`coverage_median ~ 0.80`)
    - `B2`:
      - `oos_mean_rank_ic ~ 0.0215`
      - `oos_rank_ic_tstat ~ 0.31`
      - `oos_alpha_only_tstat ~ 1.20`
      - `gross_to_net_retention ~ 0.45`
      - best Family B candidate so far, but still misses admission / subperiod and does not clear the 50% retention line
    - `B3`:
      - `oos_mean_rank_ic ~ 0.0398`
      - `oos_rank_ic_tstat ~ 0.58`
      - `oos_alpha_only_tstat ~ -0.13`
      - orthogonal enough, but economics stay too weak
  - Interpretation boundary:
    - Week 2 has genuinely started and produced comparable contract-shaped bundles
    - and all three planned family directions (`A`, `B`, `C`) now have first-pass reads on the current 50-name platform-native sample
    - but there is still no Family A / B / C winner yet
    - current first-pass leaderboard is:
      - raw rank-IC: `C1`
      - alpha-only t-stat: `B2`
      - closest all-around platform-native candidate: `A2`
    - current results must be read as platform-native narrow-sample evidence, not full mid-cap qualification
  - Week 4 stop/go decision on the current evidence base:
    - `STOP`
    - no candidate clears the chartered winner definition
    - Weeks 5-8 do not continue under the frozen sprint
- Next decision node:
  - decide whether to:
    - leave the US alpha core restart closed on the current platform-native evidence base
    - or explicitly reopen under a new decision, for example:
      - a full `rank_500_1500` rerun under the same contract
      - a new charter with a different candidate field or data layer
- Mainline constraints for this sprint:
  - US only
  - `rank_500_1500` mid-cap universe
  - no Branch A reopen
  - no label redesign
  - no optimizer / multi-period redesign
  - 3 family directions, at most 8 total candidates
  - Week 4 is a hard stop/go gate

## A-Share Research State

- Status:
  - frozen as a branch-local archive; main-repo memory is index-only for A-share
- Last sync:
  - `2026-04-08`
- Branch-local canonical docs:
  - memory:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\ASHARE_MEMORY.md`
  - roadmap:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\docs\ashare_alpha_roadmap.md`
  - ledger:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\docs\superpowers\notes\2026-04-07-ashare-inefficiency-hypothesis-ledger.md`
- Stable read:
  - `anti_mom_21_5` remains the only audited A-share lead and is currently `partially_real`
  - `pivot 1a` is already triggered
  - A5 is closed as an execution / alpha-translation repair result, not as an alpha winner
  - the current A-share line is sequencing-driven, with multiple honest negatives, several inconclusives, and no nearby-family reopen path
  - the current tranche has now exported its first promotion-contract example bundle instead of trying to merge research code into PortfolioOS
- Current active branch:
  - none; frozen while paper stage is the only active project lane
- Next decision node:
  - none by default
  - only reopen by explicit decision to start a new A-share tranche
- Main-repo rule:
  - do not use this section as the operating note for A-share work
  - use the branch-local memory above

## Recommended Next Steps

1. Complete the Week 1 freeze for the US alpha core restart charter:
   - frozen baseline registry
   - frozen candidate definition sheet
   - frozen artifact contract
2. Run Week 2 on the highest-priority family first:
   - residual momentum / residual reversal
3. Treat `min_evaluation_dates: 20 -> 19` only as a sidecar backlog item, not as the mainline objective.
4. Keep A-share as background maintenance; do not pull it into this 8-week US sprint.
5. If Week 4 produces no winner, close the sprint honestly rather than broadening the search space.

### Current SUE PEAD Read

- Announcement-timed `SUE` PEAD mapping on `rank_500_1500_dynamic` is now live across:
  - `[+2,+2]`
  - `[+2,+3]`
  - `[+2,+4]`
  - `[+2,+5]`
  - `[+2,+7]`
  - `[+2,+10]`
  - `[+2,+15]`
  - `[+2,+22]`
- Best clean / risk-adjusted ranking window is:
  - `[+2,+2]`
  - `rank_ic_t ~ 22.7`
- Best alpha-only t-stat window is:
  - `[+2,+3]`
  - `alpha_only_t ~ 12.3`
- Best mean alpha-only spread window is:
  - `[+2,+22]`
  - `mean_alpha_only_spread ~ 1.49%`
- Rank-IC decays as the window lengthens, but mean alpha-only spread remains economically meaningful through roughly the 2-to-3 week range:
  - practical read: the drift is front-loaded in purity / ranking power, but not fully exhausted immediately
- Breadth remains stable across the dense grid:
  - active months stay around `251-252`
  - total event count stays around `78k`
  - mean events per month stays around `312`
- Half-sample split stays directionally positive:
  - the result looks real
  - later-half t-stats are generally lower, so the strongest window numbers should be treated as strong empirical reads rather than fixed constants
- This reinforces the current US WRDS boundary:
  - the main edge is announcement-timed `SUE`
  - the CAR3 overlay is secondary and should stay closed as a modest delayed-entry confirmation result
  - the next distinct branch after SUE mapping remains event-aware `revision`

### Current Revision Event-Aware Read

- FY1 `revision_1m` now has its own narrow Phase C runner:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\run_revision_event_mapping.py`
- Phase C is now closed with a final representative spec:
  - signal date = `statpers`
  - entry = next trading day after `statpers`
  - label = **pure `to-next-announcement`**
  - representation = **full panel**, not latest-snapshot-per-event
  - aggregation = monthly cross-sectional evaluation by signal month on `rank_500_1500_dynamic`
- Final representative result:
  - `monthly_signal_count = 250`
  - `total_signal_count = 218,766`
  - `mean_signals_per_month = 875.1`
  - `mean_rank_ic ~ 0.0166`
  - `rank_ic_t ~ 3.61`
  - `mean_alpha_only_spread ~ 0.42%`
  - `alpha_only_t ~ 4.03`
- Shared-event overlap with announcement-timed `SUE` is low:
  - using the last revision snapshot before each earnings event, pooled rank correlation with `SUE` is only `~0.11`
  - practical read: revision is not just a noisy SUE shadow
- Closeout interpretation:
  - the main problem in early Phase C was **horizon misspecification**, not snapshot dilution
  - `last snapshot per event` did not win even after the horizon was corrected:
    - pure full panel: `rank_ic_t ~ 3.61`, `alpha_only_t ~ 4.03`
    - pure last snapshot: `rank_ic_t ~ 2.37`, `alpha_only_t ~ 0.79`
  - final representative spec is therefore **full-panel pure `to-next-announcement` revision**
  - announcement-timed `SUE` remains the primary event-driven alpha benchmark, but finalized revision is strong enough to treat as a real separate branch rather than a weak afterthought
  - if the US WRDS line continues, the next question is whether this finalized revision branch adds enough marginal value relative to announcement-timed `SUE`; Phase C itself should not be reopened

### Phase 3 Mainline Integration Pre-Registration

- Next cheap discriminating experiment:
  - wire finalized revision back into the Phase 3 Qlib / LightGBM mainline as a **feature**
  - keep the current fixed-horizon training label unchanged on the first pass
  - run a matching simple linear baseline in the same round
- Revision feature spec for the first-pass mainline test:
  - use **full-panel pure `to-next-announcement` revision** as the source signal
  - feature timestamp is still `statpers`, not `announcement_date`
  - align to the **next trading day** before the feature becomes visible in the daily model table
  - use **latest-value carry-forward** between revision snapshots until a newer snapshot arrives
  - practical caveat: this repeated carry-forward makes revision look more persistent in the training table than the number of truly independent revision events; do not over-interpret raw tree split counts
- Mainline comparison must stay locked across runs:
  - same universe
  - same label
  - same train/validation split
  - same random seeds
  - same feature-standardization fit window
  - same early-stopping criterion
  - compare **delta versus baseline**, not absolute one-off scores
- Seed discipline:
  - run at least `3` fixed seeds for baseline and `+revision`
  - judge the result on mean delta and dispersion, not on a single lucky seed
- Simple baseline definition for this round:
  - use **signal-level** cross-sectional equal-weight combination, not portfolio-level strategy ensembling
  - standardize features by **same-day cross-sectional z-score**
  - then average the standardized signals into one composite score
  - this is the sanity-check baseline that should be compared directly against LightGBM's score output
- Importance/readout discipline:
  - if revision appears important in LightGBM, cross-check with permutation importance or SHAP-style diagnostics
  - do not treat raw split / gain importance alone as decisive because carry-forward can overstate visual prominence
  - Pre-registered interpretation bands for the first-pass mainline test:
    - **Clearly positive**:
      - overall IC improves by `>= 15%`, or revision ranks stably in the top `5` features
      - interpretation: fixed-horizon mainline is already able to absorb most of the revision edge; keep stacking validated features before touching labels
  - **Intermediate**:
    - overall IC improves by `5%` to `< 15%`, or revision lands in the top `10` but not stably
    - interpretation: fixed-horizon mainline captures part of the edge; record the gain and keep label redesign as a later optimization path
    - **Near-zero**:
      - overall IC improves by `< 5%`
      - interpretation: fixed-horizon labeling is likely leaving most of the revision value on the table; event-aligned label redesign becomes the next justified engineering step

### Phase 3 Mainline Integration First-Pass Result

- The cheap discriminating integration test is now partially complete:
  - finalized WRDS revision was wired into the external Phase 3 Qlib mainline as `revision_1m_wrds`
  - implementation lives in:
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\staging_to_qlib.py`
    - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\scripts\run_qlib_lgbm_walkforward.py`
  - feature contract used:
    - source = finalized full-panel FY1 `revision_1m`
    - feature timestamp = `statpers`
    - visibility = next trading day
    - daily representation = latest-value carry-forward
- Fixed-horizon mainline read on `expanded_liquid_core`:
  - baseline seeds were all weak/negative:
    - seed `7`: `mean_rank_ic ~ 0.0010`, `rank_ic_t ~ 0.07`, `alpha_only_t ~ -1.42`
    - seed `17`: `mean_rank_ic ~ -0.0011`, `rank_ic_t ~ -0.08`, `alpha_only_t ~ -1.86`
    - seed `29`: `mean_rank_ic ~ -0.0015`, `rank_ic_t ~ -0.11`, `alpha_only_t ~ -2.48`
  - `+revision` seed `7` improved slightly but remained economically weak:
    - `mean_rank_ic ~ 0.0021`
    - `rank_ic_t ~ 0.15`
    - `alpha_only_t ~ -1.22`
  - practical read:
    - fixed-horizon Qlib does appear to ingest **some** revision information
    - but the realized lift is tiny relative to the size of the event-aware revision edge
    - this did **not** turn the mainline model into a live signal
- Simple linear sanity check reached the same conclusion:
  - same-day cross-sectional z-score equal-weight custom-feature composite without revision:
    - `rank_ic_t ~ -0.46`
    - `alpha_only_t ~ 0.57`
  - with revision added:
    - `rank_ic_t ~ -0.46`
    - `alpha_only_t ~ 0.60`
  - practical read:
    - adding revision helps only trivially in the fixed-horizon signal-level baseline too
- Important model-usage nuance:
  - on the completed `+revision` seed `7` run, `REVISION_1M_WRDS` ranked very high in raw tree usage:
    - gain rank = `2`
    - split rank = `1`
  - do **not** over-read that as proof the mainline solved revision
  - the carry-forward representation makes revision visually prominent in tree splits, but the realized holdout lift stayed small
- Updated decision boundary:
  - do **not** reopen a broad multi-seed fixed-horizon mainline sweep just for revision
  - do **not** promote the fixed-horizon Phase 3 Qlib branch on the back of this result
  - current best interpretation is:
    - fixed-horizon mainline absorbs a little of revision
    - most of the value remains tied to event-aware labeling
  - if the US WRDS line returns to mainline model work, the justified next step is **event-aligned or hybrid label design**, not more feature-only stacking on the old fixed-horizon target

## Key Paths And Docs

- Main docs:
  - `docs/execution_mode_decision_note.md`
  - `docs/cost_model_decision_note.md`
  - `docs/platform_ml_rl_roadmap.md`
  - `docs/phase_1_alpha_closeout_note.md`
  - `docs/strategy/single_period_vs_multi_period_structure_decision_memo_2026_04_15.md`
  - `docs/strategy/objective_function_units_spec_2026_04_15.md`
  - `docs/phase_1_5_alpha_decision_note.md`
  - `docs/paper_calibration_runbook.md`
  - `docs/paper_calibration_live_2026_04_15.md`
- Core alpha implementation:
  - `src/portfolio_os/alpha/`
- Alpha CLIs:
  - `portfolio-os-alpha-research`
  - `portfolio-os-alpha-acceptance`
- Canonical US freeze summary:
  - `C:\Users\14574\Quant\fmp_data_freeze\summary\fmp_coverage_summary.json`
- External research workspace:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01`
- Latest WRDS US research artifacts:
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\outputs\wrds_bootstrap\`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-06-car3-confirmation-closeout.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-06-sue-pead-mapping-closeout.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-07-revision-event-aware-initial.md`
  - `C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\phase3-qlib-ml-alpha\docs\superpowers\notes\2026-04-07-revision-event-aware-closeout.md`

## Workflow Notes

- Commit every substantive change.
- Default full regression on this machine: `python -m pytest -q`.
- Generated artifacts under `outputs/` stay out of version control.
