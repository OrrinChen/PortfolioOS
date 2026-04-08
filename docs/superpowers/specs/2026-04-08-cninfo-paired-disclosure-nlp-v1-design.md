# CNInfo Paired Disclosure NLP v1 Design

**Date:** 2026-04-08  
**Workspace:** `C:\Users\14574\Quant\PortfolioOS`  
**Status:** Proposed

## Goal

Design a narrowly scoped Chinese financial text research program that tests whether language changes between paired A-share disclosures can predict formal disclosure surprise and, secondarily, post-disclosure market reaction.

This v1 is not a broad Chinese finance NLP platform and not a portfolio-ready alpha program. It is a controlled, point-in-time-clean research design for one thesis:

`forecast -> formal report` language delta may contain incremental information beyond simple metadata.

## Why This Slice

This slice is intentionally narrow because the main research risk is not model complexity. The main research risk is label, timestamp, and corpus contamination.

Constraining the first version to CNInfo paired disclosures provides:

- stronger point-in-time discipline than mixed text sources
- a natural paired-text object for delta analysis
- cleaner control-variable experiments
- a high-value Chinese finance NLP pipeline asset even if the alpha thesis is weak

## Roadmap Position

This design governs a new sibling research branch under the broader Quant workspace. It does not reopen the existing US event-driven line and does not change PortfolioOS alpha production semantics.

If v1 succeeds, its outputs may later feed a separate A-share text research package or upstream artifact pipeline. v1 itself remains a research qualification project.

## Core Thesis

The thesis under test is deliberately narrow:

- the anchor corpus is CNInfo listed-company disclosures only
- the main paired object is `performance forecast -> formal periodic report`
- the secondary paired object is `express report -> formal periodic report`
- the primary prediction target is formal disclosure surprise relative to prior guidance
- post-disclosure return prediction is a secondary economic validation layer, not the primary success criterion

## Frozen Corpus Contract

### Included Source

- CNInfo listed-company disclosures only

### Included Document Families

- performance forecasts
- express reports
- formal periodic reports
  - annual reports
  - semiannual reports
  - quarterly reports

### Explicitly Excluded In v1

- investor Q&A / interactive platforms
- exchange inquiry-response letters
- broker research reports
- news
- social media
- earnings calls or transcripts
- any non-CNInfo auxiliary source for signal generation

The exclusion list is a hard scope boundary. v1 may not add these sources opportunistically.

## Timestamp And Visibility Contract

Because CNInfo disclosures do not provide universally reliable minute-level timestamps for all relevant filings, v1 uses a deliberately conservative visibility rule.

### Main Visibility Rule

- information becomes tradable at `next trading day open`

### Consequences

- all primary evaluation aligns to `next-open`
- same-day price reactions are not used for main claims
- same-day close alignment is allowed only as a leakage probe

This sacrifices some possible signal strength in exchange for cleaner point-in-time semantics.

## Sample Window Contract

### Start Date

- `2018-01-01`

### End Date

- `latest sufficiently matured paired cutoff`

This means the usable sample ends at the most recent date where paired disclosure completion is judged mature enough for evaluation.

### Holdout Design

- no walk-forward in v1
- holdout is the trailing `12 months`, split by formal report disclosure date

The holdout is intentionally longer than six months so that the evaluation spans a full disclosure cycle rather than a partial seasonal slice.

## Universe Contract

### Research Universe

- point-in-time CSI 300 plus CSI 500 membership

### Executable Slice

- point-in-time CSI 300 plus CSI 500 after tradability filters
- filters include at minimum:
  - ST status exclusions
  - long suspension exclusions
  - clearly non-tradable samples

The research universe and executable slice must remain distinct objects. Tradability filters may not silently redefine the research universe.

## Pairing Contract

### Main Pair Type

- `forecast -> report`

### Secondary Robustness Pair Type

- `express -> report`

### Pair Identity

- `pair_id = ticker + fiscal_period + pair_type`

### Pair Selection Rule

For each pair identity:

- keep only the last eligible forecast or express report before the formal report disclosure
- retain earlier revisions in raw registries for audit, but exclude them from the v1 primary paired sample

This prevents repeated reuse of the same future report path in the main learning sample.

## Report View Contract

Formal reports are much longer and structurally different from forecasts or express reports. v1 therefore does not compare raw full-document text against short filings.

### Primary Report View

A standardized report text view assembled from the most comparable high-signal sections, such as:

- summary or highlights
- major financial data summary
- early management discussion or operating overview text

### Secondary Report View

- a broader management discussion slice for robustness only

The exact section extraction rules must be deterministic and frozen before modeling.

## Labels

v1 uses a two-layer label structure.

### Primary Label: Formal Disclosure Surprise

The primary label measures whether the formal report outcome lands below, within, or above the prior guidance implied by the paired forecast.

Preferred first implementation:

- parse numeric guidance from the forecast or express report
- align to realized metrics in the formal report
- assign a ternary label:
  - below prior guidance
  - within prior guidance
  - above prior guidance

Secondary continuous residual labels may be added only for the subset where numeric parsing is stable.

### Secondary Label: Post-Disclosure Market Reaction

This label is a secondary economic relevance layer only.

Suggested windows:

- `next-open -> same-day close`
- `next-open -> +3 trading days`
- `next-open -> +5 trading days`

Failure on the market-reaction layer does not automatically kill the pipeline if the primary disclosure-surprise task is still informative.

## Non-Goals

v1 explicitly does not do any of the following:

- build a general Chinese finance document platform
- mix in broker reports, news, or Q&A data
- qualify a portfolio-ready alpha package
- run walk-forward model selection
- use minute-level event timing assumptions
- introduce retrieval-agent loops or autonomous browsing agents
- treat prompt search as a primary optimization surface
- reopen unrelated A-share or US factor branches

## Phase Structure

### Phase 1: Corpus Freeze And PIT Validation

Purpose:

- prove that the corpus boundary, timestamps, pair construction, and labels are trustworthy

Required outputs:

- CNInfo disclosure registry
- paired-sample manifest
- deterministic report-view construction rules
- numeric label parser outputs
- manual audit sheet and error notes

Phase 1 gate:

- pair precision must pass threshold
- numeric label coverage must pass threshold
- no obvious point-in-time or boilerplate contamination

### Phase 2: Baseline Signal Qualification

Purpose:

- test whether simple text-delta representations contain information beyond metadata

Frozen baseline ladder:

1. metadata-only
2. lexicon delta
3. embedding delta

Frozen negative controls:

- same-day close leakage probe
- shuffled pair control
- length-only or boilerplate-only control

Phase 2 gate:

- at least one text baseline must stably beat metadata-only on the primary holdout
- the result must not depend mainly on same-day-close alignment, a single subperiod, or a single reporting slice

### Phase 3: Structured Extraction And Incremental Lift

Purpose:

- test whether structured event extraction provides information beyond embedding-based delta

Frozen extraction posture:

- use a fixed extraction schema
- use a fixed model family
- do not perform open-ended prompt search
- compare on exactly the same pairs, labels, holdout, and evaluation contract as Phase 2

Phase 3 gate:

- extraction must be at least competitive with embedding on the primary metric
- extraction must not show gains that are mainly explained by memorization propensity or highly templated documents

### Phase 4: Attribution Closeout

Purpose:

- close the thesis cleanly rather than continue open-ended optimization

Required comparisons:

- `forecast -> report` main sample versus `express -> report` robustness
- CSI 300 versus CSI 500 slices
- numeric surprise versus market reaction readout

This phase includes one deliberately lightweight combination test for attribution only.

## Representation Ladder

The representation ladder is frozen so that only one major variable changes at a time.

### Metadata-Only

Allowed inputs:

- report type markers
- industry markers
- size bucket
- fiscal-period markers
- document length and similar non-semantic controls

Purpose:

- establish the non-text baseline

### Lexicon Delta

Allowed additions over metadata:

- uncertainty terms
- risk terms
- degree modifiers
- negation and revision cues
- numeric-expression density changes

Purpose:

- provide a cheap sanity baseline for language change

### Embedding Delta

Allowed additions over the same paired sample:

- paired text representations using `BGE-M3`
- first pass uses fixed-window paired views
- retrieval-aligned section comparison is allowed only after the fixed-window version is evaluated

Purpose:

- test whether richer representation alone provides incremental value

### Structured Extraction Delta

Allowed additions over the same paired sample:

- fixed-schema extraction using a Chinese-capable open model such as `Qwen`
- event or attribute deltas computed from extracted structured fields

Possible schema fields include:

- guidance polarity
- uncertainty
- operational headwinds and tailwinds
- revision cues
- risk acknowledgement
- management confidence or conservatism

Purpose:

- test whether explicit structured interpretation adds value beyond embeddings

## Simple Combo Contract

The simple combo is preserved in v1, but its role is strictly limited.

### Role

- attribution tool for closeout

### Not Allowed

- not a fourth candidate signal family
- not a rescue path for weak single-signal results
- not an independent pass or fail gate for v1

### Scope Contract

- structure frozen:
  - equal-weight
  - rank-IC weight computed once on the training window
- membership frozen:
  - combo inputs may only be baseline signals that have already passed earlier gates
- evaluation frozen:
  - same holdout
  - same primary metrics
  - same `next-open` alignment
  - no special combo-only metric changes

The combo is used only to answer:

- whether the text representations are mostly redundant
- whether extraction contributes marginal information beyond simpler representations

## Negative Controls And Leakage Checks

v1 must pre-register and execute the following controls.

### 1. Same-Day Close Leakage Probe

- compare `next-open` main results with a same-day-close alignment
- same-day close may not be used in the main claim

Interpretation:

- if the same-day-close result is much stronger while `next-open` collapses, treat that as a warning rather than a positive surprise

### 2. Shuffled Pair Control

- shuffle matched documents within constrained company or season buckets

Interpretation:

- if performance survives aggressive pair shuffling, the model is likely learning document type or seasonal structure rather than real paired delta

### 3. Length And Boilerplate Control

- test whether length, template phrases, or highly standardized sections explain most of the signal

Interpretation:

- if so, the result does not count as a meaningful language-delta success

### 4. Memorization-Propensity Check

Inspired by recent lookahead-bias work in LLM forecasting, add a lightweight proxy check for whether extraction gains concentrate in highly repeated or highly templated documents.

Interpretation:

- if extraction gains are strongly tied to memorization-prone documents, do not promote extraction as genuine incremental prediction

## Pre-Registered Expected Results

The design freezes the expected directional ordering before modeling.

### Phase 1 Expectations

- `forecast -> report` should be the main usable sample
- `express -> report` should be a thinner but still usable robustness sample
- pair precision should be very high if the pairing logic is correct
- numeric label coverage, not modeling, should be the first hard bottleneck

### Phase 2 Expectations

Expected ordering on the primary task:

`embedding delta > lexicon delta > metadata-only`

Additional expectations:

- `forecast -> report` should outperform `express -> report`
- CSI 500 should show stronger effects than CSI 300
- numeric surprise prediction should be easier than market-reaction prediction

### Phase 3 Expectations

- structured extraction may improve interpretability before it improves headline metrics
- the healthiest extraction success pattern is:
  - competitive single-signal performance
  - non-zero marginal contribution in closeout attribution

### Phase 4 Expectations

- simple combo should provide only small to moderate lift
- very large combo lift should trigger extra suspicion about leakage, redundancy mismeasurement, or evaluation drift
- the most likely overall closeout is:
  - `pipeline asset survives, alpha thesis weak`

## Hard Gates

### Phase 1 Hard Gates

- pair precision must be `>= 98%`
- `forecast -> report` numeric label coverage should target `45%-65%`
- `forecast -> report` numeric label coverage below `40%` stops the alpha claim
- `express -> report` numeric label coverage is expected to be higher, ideally `>= 60%`
- any clear use of non-point-in-time membership or non-conservative timestamp alignment invalidates the study design

### Phase 2 Hard Gates

- if all text baselines fail to beat metadata-only on the trailing `12m` holdout, stop the alpha claim
- if the apparent success depends mainly on same-day-close alignment, do not count it as a pass
- if the result is supported mainly by one reporting season or one narrow slice, do not count it as a general pass

### Phase 3 Hard Gates

- if extraction is not at least competitive with embedding and shows no marginal contribution in closeout attribution, do not promote it
- if extraction requires repeated prompt search to work, it does not pass v1
- if extraction gains are tightly coupled to memorization-prone document subsets, do not promote it

### Phase 4 Hard Gates

- the simple combo cannot rescue a signal family that failed earlier gates
- if only one signal family survives, report that no meaningful combination attribution was available

## Stop Rules

The project should stop expanding complexity under the following conditions:

- if Phase 1 fails, freeze the work as a high-quality paired-disclosure pipeline asset and do not broaden the corpus
- if Phase 2 fails, do not use extraction complexity to rescue the alpha thesis
- if Phase 3 fails, do not continue with prompt tuning, agent loops, or additional retrieval tricks
- once Phase 4 starts, do not introduce new features or new text sources

These stop rules are part of the design, not post-hoc discretion.

## Success Conditions

v1 is successful if it produces one of the following clean outcomes with evidence:

1. `alpha thesis survives`
2. `pipeline asset survives, alpha thesis weak`
3. `v1 thesis killed`

The project succeeds if it reaches one of these closeout states honestly and with a reproducible evidence trail. It fails only if the study drifts in scope, loses point-in-time discipline, or cannot support a credible closeout.

## Feasibility Assessment

### High-Confidence Deliverables

- CNInfo paired disclosure registry
- point-in-time-clean paired text pipeline
- deterministic report-view construction
- basic numeric surprise label builder
- auditable text-delta baselines

### Medium-Confidence Deliverables

- stable embedding-delta lift over metadata-only
- usable structured extraction baseline with interpretable schema outputs

### Lower-Confidence Deliverables

- strong market-reaction alpha with immediate portfolio-readiness

The most realistic v1 value is a differentiated Chinese finance NLP research asset with a credible answer to whether paired disclosure delta contains measurable information.

## Tooling Direction

The design recommends a simple, explicit stack rather than orchestration-heavy abstractions.

- text parsing: native extraction first, OCR fallback only when needed
- OCR fallback: `PaddleOCR`
- embedding baseline: `BGE-M3`
- extraction baseline: `Qwen`
- retrieval store only if needed after fixed-window embedding tests

No LangChain-style abstraction layer is required in v1.

## Evidence And Reporting Outputs

The final closeout package should include:

- corpus contract summary
- pair manifest summary
- label coverage summary
- manual audit summary
- baseline comparison table
- negative-control table
- slice comparison table
- attribution combo summary
- final closeout decision

## External References

The design direction is informed by recent benchmark and methodology references, including:

- `CFLUE`
- `CFinBench`
- `Golden Touchstone`
- structured event representation work for stock return prediction
- recent lookahead-bias work for LLM forecasting
- official documentation for `BGE-M3`, `Qwen`, `PaddleOCR`, and Hugging Face Text Embeddings Inference

These references guide the design posture, but v1 remains a custom research task rather than a benchmark reproduction.
