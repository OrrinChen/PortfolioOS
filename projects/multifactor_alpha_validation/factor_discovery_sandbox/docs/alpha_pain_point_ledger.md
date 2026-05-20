# Alpha Pain Point Ledger

This ledger is the Track A / Factor Discovery D0 pain-point inventory. It is
not a formula backlog, alpha evidence, Q1 promotion, Q2 input, portfolio
construction, paper workflow, or production approval surface.

The purpose is to start from market frictions, constraints, information
diffusion failures, and behavioral pressure before any formula is written. A
candidate can only move from this ledger into a D3 charter after pre-formula
diagnostics show that the market pain point has observable, falsifiable traces.

## Ledger Rules

- Formula is measurement, not thesis.
- A row is a pain-point hypothesis, not an alpha.
- Every row must identify who is constrained or slow, how the constraint can
  leave a price footprint, why the footprint may not be fully arbitraged, how
  it can be observed, and what placebo would defeat it.
- Missing coverage must remain no-view / abstain, never zero alpha.
- D0 and D1 do not run Q1, Q2, optimizer paths, allocator paths, Alpha
  Registry promotion, paper workflows, broker/order workflows, live workflows,
  or production approval.
- D0.1 triage preserves candidates instead of deleting them. Only one mechanism
  can be `active_mainline` at a time; the rest must be assigned a status and
  blocking reason.

## D0.1 Triage Taxonomy

`thesis_role` separates sources of potential alpha from the controls that keep
research honest:

- `alpha_mechanism`: a candidate market mechanism that could create a
  measurable price footprint.
- `research_confounder`: a timestamp, coverage, mapping, stale-price, or data
  issue that can make false alpha look real.
- `execution_falsifier`: a cost, capacity, borrow, spread, turnover, or
  microstructure condition that can kill an otherwise real gross effect.
- `data_dependency`: a potentially valid mechanism that is blocked until a
  specific data source or extraction plan exists.

`D1_decision` preserves candidates without letting all of them compete for
attention:

- `active_mainline`: the one mechanism family currently allowed to receive D2
  protocol work.
- `shadow_queue`: mechanism is plausible and preserved, but waits behind the
  active mainline.
- `hold_pending_data`: mechanism is plausible, but data readiness is not high
  enough for D1/D2 work.
- `guard_only`: row is a required blocker, falsifier, or confounder, not an
  alpha-source candidate.
- `archive_prior_work`: row overlaps prior work enough that it should not be
  restarted unless a new charter explains the novelty.

## D0.1 Candidate Triage

| ID | Thesis role | D1 decision | Coupling group | Primary blocker / reason |
| --- | --- | --- | --- | --- |
| PP-01 | alpha_mechanism | active_mainline | insider_disclosure_regime_2023 | Highest fit with EDGAR-style infrastructure and post-2023 disclosure regime; must start with no-formula D2 event observability. |
| PP-02 | alpha_mechanism | shadow_queue | disclosure_complexity | Strong candidate, but waits behind insider mainline to avoid splitting EDGAR work. |
| PP-03 | alpha_mechanism | shadow_queue | disclosure_complexity | Strong candidate, but sample severity taxonomy must be defined before D2. |
| PP-04 | alpha_mechanism | shadow_queue | forced_holder_turnover | Mechanism is forced-flow, not disclosure complexity; waits for index/mandate holder data plan. |
| PP-05 | research_confounder | guard_only | event_timing_policy | Timestamp gate for event research, not an alpha source. |
| PP-06 | alpha_mechanism | archive_prior_work | analyst_revision_event_timing | Overlaps prior revision work; reopen only with a clearly novel event-timing charter. |
| PP-07 | alpha_mechanism | hold_pending_data | peer_diffusion | Needs a concrete peer-link data source before D2. |
| PP-08 | alpha_mechanism | shadow_queue | residual_underreaction | Preserved, but prior residual / small-cap work lowers immediate priority. |
| PP-09 | alpha_mechanism | hold_pending_data | emotion_amplified_shock | WHO and emotion proxy must be narrowed before D2. |
| PP-10 | alpha_mechanism | hold_pending_data | squeeze_asymmetry | Needs observable short-interest / borrow / option-flow proxy. |
| PP-11 | alpha_mechanism | hold_pending_data | liquidity_cascade | Needs shock taxonomy and liquidity-vacuum proxy. |
| PP-12 | alpha_mechanism | shadow_queue | index_boundary_pressure | Clean forced-flow candidate; waits behind insider mainline. |
| PP-13 | alpha_mechanism | shadow_queue | index_boundary_pressure | Preserve as less-watched-index extension of PP-12. |
| PP-14 | data_dependency | hold_pending_data | etf_basket_transmission | Requires ETF basket / creation-redemption / premium-discount data plan. |
| PP-15 | data_dependency | hold_pending_data | auction_microstructure | Requires auction / intraday imbalance data plan. |
| PP-16 | data_dependency | hold_pending_data | settlement_regime_shift | Short post-regime sample; diagnostic-only until sample and proxy are defined. |
| PP-17 | alpha_mechanism | shadow_queue | residual_underreaction | Small-cap attention hypothesis preserved, but tied to PP-18/PP-19 guards. |
| PP-18 | execution_falsifier | guard_only | small_cap_execution_gate | Cost/capacity blocker, not an alpha source. |
| PP-19 | research_confounder | guard_only | small_cap_microstructure_gate | Stale-price confounder, not an alpha source. |
| PP-20 | alpha_mechanism | hold_pending_data | small_cap_liquidity_shock | Plausible, but needs source-of-pressure proxy and cost guard. |
| PP-21 | alpha_mechanism | hold_pending_data | market_emotion_regime | Too broad until tied to a specific observable participant or flow. |
| PP-22 | alpha_mechanism | active_mainline | insider_disclosure_regime_2023 | Open-market insider buys are the sharpest post-2023 insider disclosure mechanism, but must first pass no-formula D2 event observability. |
| PP-23 | alpha_mechanism | active_mainline | insider_disclosure_regime_2023 | Discretionary sell vs planned sell contrast is part of the same triangulation, not a separate short-alpha candidate. |
| PP-24 | research_confounder | active_mainline | insider_disclosure_regime_2023 | Planned-sell information compression is a regime diagnostic for PP-22/PP-23, not a primary alpha source. |
| PP-25 | data_dependency | hold_pending_data | odte_gamma_flow | Dealer hedging / gamma-flow mechanism is plausible but needs options-chain, expiry, volume/open-interest, and intraday data plan before D1/D2. |

## Pain Point Inventory

| ID | Pain point | Who is constrained or slow | Expected price footprint | Why not fully arbitraged | Observable diagnostics | Placebo / falsifier |
| --- | --- | --- | --- | --- | --- | --- |
| PP-01 | 10b5-1 plan regime change | Insiders and compliance-bound executives operating under post-2023 rule changes. | Plan adoption, modification, termination, or clustered planned sales may have changed information content after the cooling-off / disclosure regime shift. | Sparse events, low capacity, disclosure parsing cost, changing legal constraints, and event heterogeneity reduce straightforward arbitrage. | Event-time return drift by plan event type, pre/post 2023 split, insider role, plan modification vs adoption, and issuer liquidity bucket. | Non-trading insider forms, shifted filing dates, randomized insider role labels, and pre-2023 behavior matching post-2023 by chance. |
| PP-02 | 8-K item subtype underreaction | Fundamental investors and screens that process 8-K filings too coarsely. | Auditor changes, CFO departures, CEO departures, material agreements, and acquisition completions may have different post-filing paths. | Item-level parsing and subtype classification are noisy; sample sizes are uneven; some subtypes require qualitative reading. | Event drift by item subtype, officer role, before/after market filing timing, issuer size, and analyst coverage. | Randomized item labels, routine 8-K controls, shifted filing dates, and matched issuers with non-material filings. |
| PP-03 | Restatement / amendment complexity drift | Investors under-process complex accounting amendments and restatements. | 10-K/A, 10-Q/A, and restatement-related filings may produce slow repricing when the amendment reveals deeper accounting quality issues. | Accounting interpretation is slow, sample sizes are limited, and short-side capacity can be constrained. | Amendment severity buckets, repeat amendments, auditor-change overlap, and post-amendment return / volume drift. | Routine amendment controls, randomized amendment severity, and shifted amendment dates. |
| PP-04 | Spinoff forced selling | Index, mandate, and size-style holders forced to own or not own parent / spun assets. | Spinoff children or parents may face non-fundamental selling pressure around distribution and eligibility windows. | Small samples, operational complexity, low capacity, and mandate-specific pressure limit arbitrage scale. | Parent and child price pressure by index membership, size bucket, ownership base, and first eligible trading windows. | Non-index spinoff controls, pseudo-event dates, and matched corporate actions without forced holder turnover. |
| PP-05 | Earnings timestamp ambiguity | Researchers and investors using vendor announcement dates without verifying public availability. | Apparent pre-event drift can dominate post-event drift when the event anchor is late or date-only. | Timestamp source quality is uneven; exact public release time can require separate data acquisition. | Compare announcement date, event-available timestamp, tradable timestamp, IBES actual date, Compustat RDQ, and exact release timestamp if available. | Shifted -5/-10 trading-day windows beating live windows blocks tradable interpretation unless earlier public availability is proven. |
| PP-06 | Guidance / revision confirmation lag | Analysts and PMs wait for confirmation before updating estimates or positions. | Estimate revisions confirmed by guidance or later filings may diffuse slowly across names and sectors. | PIT analyst data is difficult; revision context matters; signals can overlap with earnings surprises. | Revision timing, forecast-period alignment, confirmation event overlap, and post-confirmation drift by coverage bucket. | Stale revisions, wrong forecast period, randomized revision dates, and non-PIT estimate snapshots. |
| PP-07 | Peer information diffusion | Investors react first to visible leaders and later to smaller or less-covered peers. | A leader event can move same-industry peers with a delay if the market slowly maps implications across comparable firms. | Peer mapping is ambiguous; common factor contamination is high; sample design can leak sector beta. | Leader event response followed by peer drift, controlling for sector, size, beta, liquidity, and shared supplier/customer hints when available. | Random peer assignment, sector shuffle, shifted leader events, and peer groups matched only on sector beta. |
| PP-08 | Industry residual underreaction | Investors over-anchor on sector moves and under-process idiosyncratic residual moves. | Within-sector residual moves can continue if price discovery is incomplete after common-factor effects are removed. | Gross edge may be small; exposure contamination and costs can dominate; signal can collapse under same-coverage placebo. | Residual return drift after sector / market controls, by liquidity, size, and coverage buckets. | Sector / size / liquidity matched random residuals, shifted dates, and raw momentum controls matching the residual pattern. |
| PP-09 | Emotion-amplified event response | Retail, trend followers, stop-loss users, short-covering flows, and discretionary PMs responding emotionally to salient shocks. | The same event or shock may produce larger continuation or later reversal in high-attention, high-volatility, low-liquidity, or crowded names. | Timing risk, path dependency, shorting frictions, and crash / squeeze risk make clean arbitrage hard. | Conditional event response by abnormal volume, intraday range, gap size, news intensity, option activity where available, liquidity, and short-interest proxy. | Same-return shock with randomized emotion state, shifted event dates, sector-size-liquidity matched shocks, and same-coverage random emotion buckets. |
| PP-10 | Upside FOMO / short-squeeze overshoot | Retail buyers, call buyers, short sellers covering, and trend followers. | Large positive shocks can overshoot through attention and squeeze mechanics, with possible short continuation and medium-horizon decay. | Borrow constraints, squeeze risk, and uncertain timing make short-side arbitrage risky. | Up-shock path by short-interest proxy, option-volume proxy, abnormal volume, and retail-attention proxy. | Low-short-interest winners, randomized squeeze labels, and same-size / same-volatility winners without attention shock. |
| PP-11 | Downside panic / deleveraging cascade | Stop-loss users, risk-budget funds, levered holders, and liquidity-constrained sellers. | Large negative shocks can extend when forced selling or liquidity vacuum dominates fundamental repricing. | Catching falling knives is risky; liquidity can disappear exactly when the edge appears. | Down-shock continuation / reversal by liquidity, volatility, ownership proxy, and market-stress regime. | Beta-adjusted random losers, high-liquidity losers, and shifted selloff windows. |
| PP-12 | Russell / index rebalance boundary pressure | Passive funds and benchmark trackers with tight implementation windows. | Additions, deletions, and boundary candidates can experience predictable flow pressure around announcement and effective windows. | Crowded and partially arbitraged, but capacity, borrow, and boundary uncertainty can leave residual effects. | Boundary candidate pressure by size rank, announcement stage, effective date, and liquidity bucket. | Non-boundary same-size stocks, pseudo-boundary ranks, and shifted rebalance dates. |
| PP-13 | Less-watched index rebalance pressure | Niche passive vehicles and small-index mandates. | Microcap, small-cap, or less-followed index changes may create localized flow pressure. | Lower capacity and fragmented data make it unattractive for large funds. | Add/delete pressure in less-followed index events by ADV and ownership proxy. | Announced but non-effective changes, same-size non-index controls, and randomized effective dates. |
| PP-14 | ETF basket creation/redemption pressure | Authorized participants, ETF market makers, and underlying basket liquidity providers. | ETF primary-market flows may transmit pressure to basket constituents, especially thinner names. | Basket composition, timing, and inventory management are noisy; the effect may be short-lived. | Basket flow proxies, constituent pressure, ETF premium/discount, and basket liquidity split. | Same-sector non-basket names, pseudo-basket assignments, and shifted flow dates. |
| PP-15 | Closing auction imbalance pressure | Benchmark MOC traders, indexers, and funds minimizing benchmark slippage. | Predictable closing pressure or next-open reversal can appear around flow-heavy sessions. | Very short horizon, high competition, microstructure costs, and data availability limit scale. | MOC imbalance proxy, close-to-next-open path, flow calendar, and liquidity bucket. | Non-MOC days, randomized imbalance sign, and matched volume days without benchmark flow. |
| PP-16 | T+1 settlement liquidity management | Funds, brokers, and operations teams managing cash and settlement constraints after the regime change. | Settlement-cycle changes can alter timing of liquidity demand, fails management, and end-of-day cash behavior. | Recent regime change limits historical samples; operational behavior is hard to observe. | Pre/post T+1 flow timing, cash-like names, high-turnover names, and settlement-sensitive days. | Unaffected buckets, pre-change pseudo-regime splits, and non-settlement calendar days. |
| PP-17 | Small-cap attention neglect | Institutions under-cover smaller investable names; sell-side and media attention is thinner. | Information may diffuse slowly in small-cap names that are investable but below large-cap attention thresholds. | Capacity is limited, spreads are wider, and data quality / coverage are weaker. | Event or residual drift by market-cap tier, analyst coverage, media intensity, ADV, and spread proxy. | Large-cap control names, same-ADV random small caps, and coverage-matched random signals. |
| PP-18 | Small-cap capacity trap | Researchers mistake gross small-cap spreads for exploitable alpha while costs and capacity dominate. | Apparent positive spreads disappear after spread, ADV, rebalance lag, and realistic participation constraints. | Large funds cannot deploy much capital; small names can move against implementation; turnover costs can dominate. | Gross vs capacity-adjusted diagnostic spread by ADV bucket, spread proxy, holding horizon, and rebalance frequency. | Same-coverage random small-cap signals that produce similar gross but fail net/capacity diagnostics. |
| PP-19 | Small-cap stale-price / delayed-reaction ambiguity | Thin trading can make stale prints look like delayed alpha. | Delayed price adjustment may appear after news or market moves, but it may be stale trading rather than exploitable underreaction. | Distinguishing stale prints from real delayed reaction needs daily/intraday liquidity evidence. | Non-trading days, zero-return streaks, bid-ask proxy, first trade after event, and close-to-next-trade behavior. | Names with similar stale-price patterns but randomized events, high-liquidity small-cap controls, and excluding zero-volume / stale intervals. |
| PP-20 | Small-cap local liquidity shock | Small funds, insiders, tax-loss sellers, and forced small-cap holders can create idiosyncratic pressure. | Localized selling or buying pressure can dominate fundamentals in investable small caps, then mean-revert when liquidity normalizes. | Capacity is low; timing is uncertain; identifying pressure source is difficult. | Abnormal volume, spread widening, market-cap tier, tax-loss season, fund-flow calendar, and event-free pressure episodes. | Same-season random small caps, high-liquidity controls, and pressure episodes without abnormal flow / spread widening. |
| PP-21 | Market-emotion convexity | Market participants extrapolate recent gains or losses nonlinearly during euphoric or fearful regimes. | Up markets can overshoot beyond fundamentals, while drawdowns can exceed expected risk moves before partial normalization. | Regime timing is hard, crowding changes quickly, and hedging / de-risking flows are path-dependent. | Conditional response of event shocks or residual moves under high market return, high realized volatility, high dispersion, or high drawdown regimes. | Same signal in neutral regimes, shuffled market-regime labels, and market-beta / volatility matched controls. |
| PP-22 | Open-market insider buying after the post-2023 disclosure regime | Officers, directors, and large holders who choose to buy common stock in open-market or private purchase transactions after clearer 10b5-1-related reporting. | If discretionary insider conviction is informative, post-filing returns should be positive after auditable EDGAR filing visibility, especially when purchases cluster across insiders or senior roles. | Capacity is limited, signals are event-sparse, insiders may buy for non-information reasons, and public filing lag makes entry later than transaction date. | No-formula event study for Form 4 code `P` common-stock purchases after EDGAR acceptance timestamp, split by role, cluster, issuer size, and post-2023 reporting status. | Shifted filing dates, issuer-matched non-event filings, randomized role labels, non-open-market transaction controls, and pre-event drift dominating post-filing drift. |
| PP-23 | Discretionary sell vs planned sell information contrast | Insiders who sell outside a 10b5-1 plan may differ from insiders executing planned sales. | Discretionary sells may have more negative information content than planned sells, but sell-side motives are heterogeneous and should be contrast-only at D2. | Tax, diversification, compensation, liquidity, estate planning, and preplanned execution dilute sell informativeness. | Compare post-filing event paths for Form 4 code `S` with post-2023 10b5-1 flag false vs true, without constructing a short alpha score. | Randomized 10b5-1 flags, compensation-related transaction controls, shifted filing dates, and planned sells beating discretionary sells in absolute information content. |
| PP-24 | Planned sell information compression | Post-2023 10b5-1 planned-sell disclosure may make planned sells easier to identify as low-signal mechanical execution. | Planned sells should show weaker post-filing return slope than discretionary sells if the compression thesis is true. | Planned sales can still contain information, plan adoption dates may be stale, and quarterly plan disclosures may lack precise event timing. | Compression diagnostic comparing planned-sell and discretionary-sell event paths pre/post reporting change, using only auditable filing visibility. | Planned sells retain similar or stronger information content than discretionary sells, or randomized plan flags match the observed contrast. |
| PP-25 | 0DTE options gamma / charm flow | Options market makers and liquidity providers hedging same-day expiry convexity under gamma and charm exposure. | Under certain dealer positioning states, hedging can create procyclical or stabilizing equity-index flow around expiry windows. | Competition is high, data requirements are heavier, intraday timing matters, and observed proxies can be vendor-dependent. | Data-readiness study for option chain, expiry calendar, volume/open interest, dealer-gamma proxy, intraday SPX/SPY returns, and market liquidity state. | Randomized expiry windows, non-0DTE option controls, same-volatility non-expiry days, and gamma proxy failing to explain any incremental flow beyond realized volatility. |

## Mechanism Families

| Family | Included pain points | Rationale | First diagnostic question |
| --- | --- | --- | --- |
| Disclosure complexity barrier | PP-02, PP-03 | Uses public disclosure structure and existing EDGAR-style infrastructure; mechanism is concrete and falsifiable. PP-01 is handled by the active insider-disclosure regime group. | Do subtype / amendment events have event-local behavior not explained by issuer size, sector, liquidity, or shifted dates? |
| Information diffusion / event timing | PP-05, PP-06, PP-07 | Separates true information lag from timestamp artifacts and peer-mapping errors. | Does drift start only after auditable public availability, and does it survive shifted-date and peer-randomization placebos? |
| Residual underreaction / peer-relative price discovery | PP-08, PP-17 | Tests whether within-sector or within-peer residual moves carry information after common factors and attention gaps are controlled. | Does residual drift survive sector / size / liquidity / coverage matched placebo without relying on missing coverage? |
| Emotion / attention / liquidity amplification | PP-09, PP-10, PP-11, PP-20, PP-21 | Captures nonlinear reactions to shocks, but must be event- or shock-conditioned to avoid generic sentiment mining and guarded by PP-18/PP-19 where small caps are involved. | Does emotion / attention state add incremental information beyond the original shock, volatility, liquidity, sector, size, and short-term reversal? |
| Forced-flow / market-structure pressure | PP-04, PP-12, PP-13, PP-14, PP-15, PP-16 | Focuses on mandate, benchmark, settlement, auction, and holder-turnover constraints with clear non-discretionary flow mechanics. | Does pressure concentrate around the specified forced-flow calendar and disappear under pseudo-event / non-boundary controls? |
| Insider disclosure regime 2023 | PP-01, PP-22, PP-23, PP-24 | Treats 10b5-1 plan events, open-market buys, discretionary sells, and planned-sell compression as a coupled triangulation rather than independent alpha candidates. | Do post-2023 insider disclosure subsets show consistent, auditable post-filing footprints without relying on transaction-date leakage or formula weights? |
| 0DTE gamma-flow microstructure | PP-25 | Preserves a major options-flow mechanism as a data-readiness branch without pulling Track A away from EDGAR/disclosure work. | Is there enough PIT-safe options and intraday data to define a no-formula D1/D2 protocol? |

## D0.1 Work Queue

### Active Mainline

1. **Insider disclosure regime 2023**
   - Coupled rows: PP-01, PP-22, PP-23, PP-24.
   - Next step: D2 no-formula observability protocol.
   - Current protocol draft:
     `d2_insider_disclosure_observability_protocol.md`.
   - Completion signal: a D2 report classifies the coupled group as
     `observable`, `mixed`, or `not_observable` using event-study diagnostics
     and preregistered placebos.

### Shadow Queue

2. **Disclosure complexity barrier**
   - Rows: PP-02, PP-03.
   - Keep for later D1/D2 work after the insider mainline closes.

3. **Index boundary / forced-flow pressure**
   - Rows: PP-12, PP-13.
   - Clean mechanism, but waits behind the active EDGAR/disclosure mainline.

4. **Residual underreaction / peer-relative price discovery**
   - Rows: PP-08, PP-17, guarded by PP-18 and PP-19.
   - Preserved, but prior residual and small-cap diagnostics lower immediate
     priority.

### Hold Pending Data

- PP-07 peer diffusion, PP-09 to PP-11 emotion/shock mechanisms, PP-14 ETF
  basket transmission, PP-15 auction microstructure, PP-16 T+1 settlement,
  PP-20 small-cap liquidity shock, PP-21 market-emotion convexity, and PP-25
  0DTE gamma-flow microstructure.

### Guard Only

- PP-05 event timestamp ambiguity.
- PP-18 small-cap capacity trap.
- PP-19 small-cap stale-price ambiguity.

## Explicit Non-Goals

- This ledger does not select a formula.
- This ledger does not validate a signal.
- This ledger does not run Q1 or Q2.
- This ledger does not approve portfolio construction, Alpha Registry entry,
  paper workflows, broker/order workflows, live workflows, or production use.
- This ledger does not treat historical fixture success, local optimizer-path
  integration, or diagnostic spread as alpha evidence.
