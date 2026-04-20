# PortfolioOS Pilot Operations Plan

**Version:** 1.0  
**Effective date:** 2026-03-25  
**Status:** pilot-ready (release gate passed on 2026-03-24)  
**Classification:** Internal - Trading Desk Operations

## 1. Pilot Scope and Objective

Objective: validate PortfolioOS as a production-grade rebalance decision engine under live market conditions, with enough coverage to support a go/no-go decision.

What is being tested:

- Rebalance recommendation quality under multiple market regimes.
- Data-feed resilience under a multi-source fallback chain (`tushare -> akshare -> tencent`).
- Compliance pathway integrity (`blocking -> disposition -> approval -> freeze -> execution simulation`).
- Operational cadence sustainability for nightly, weekly, and rebalance cycles.

What is not being tested:

- Live broker connectivity or order routing.
- Alpha generation or strategy research.
- Multi-portfolio concurrent operations.

Portfolio scope:

- Single A-share equity portfolio.
- CSI 300 universe.
- AUM notional range for simulation: CNY 50M to CNY 200M equivalent.

US pilot variant:

- Single US equity paper portfolio.
- Large-cap plus low-liquidity stress sample sets.
- Optional Alpaca paper trading execution/reconciliation loop via `scripts/pilot_ops.py --market us --broker alpaca`.

## 2. Duration and Phase Gates

| Phase | Duration | Entry | Exit |
|---|---|---|---|
| Phase 1 - Burn-in | T+1 to T+5 (5 trading days) | Release gate passed | 0 critical failures, nightly completes 5/5 |
| Phase 2 - Steady state | T+6 to T+20 (15 trading days) | Phase 1 exit | KPIs met and >=2 rebalance cycles |
| Phase 3 - Stress validation | T+21 to T+30 (10 trading days) | Phase 2 exit | >=1 stress event handled without pipeline failure |
| Go/No-go | T+31 | Phase 3 exit | Formal sign-off or remediation plan |

Minimum duration: 30 trading days.

If no natural stress event occurs by T+30, extend up to T+40 and run synthetic stress validation (Section 7).

## 3. Operational Cadence

### 3.1 Daily (Trading days)

Pre-market (before 09:15 CST):

1. Run nightly validation (`--mode nightly`).
2. Check provider capability report. If any feed is degraded or failed, create an incident entry.
3. Review `data_source_mix` and note any fallback activation by source and field.

Post-close (after 15:30 CST):

4. Capture end-of-day market snapshot for next builder run.
5. If rebalance triggered, verify the full artifact chain exists:
   `orders -> orders_oms -> audit -> summary -> scenario_comparison -> decision_pack -> approval_record -> freeze_manifest -> execution_report -> handoff`.

Recommended manual time budget: under 10 minutes when no incident occurs.

### 3.2 Weekly (Friday post-close)

1. Run release validation (`--mode release --reviewer-input ... --real-sample`).
2. Update weekly trends for override usage, cost ratio, fallback frequency, and solver fallback count.
3. Update dashboard and save weekly summary:
   `outputs/pilot_tracking/weekly/week_NN_summary.md`.
4. Build gate-status report from dashboard + incidents:
   `py -3.11 scripts/pilot_ops.py go-nogo --window-trading-days 20 --as-of-date YYYY-MM-DD`.

Reviewer policy:

- Weekly release scoring should be cross-reviewed by at least two reviewers (PM + Risk).
- Keep final score either as averaged value or resolved with documented arbitration.

### 3.3 Rebalance Trigger

Recommended fixed schedule: bi-weekly, or on demand when one of these holds:

- Total active-weight drift >= 3%.
- Corporate action materially changes position weight.
- PM manual trigger with documented rationale.

Every rebalance must run full chain; no partial shortcuts.

## 4. Data Feed Monitoring

### 4.1 Feed Health Matrix

| Feed | Primary | Fallback 1 | Fallback 2 | Critical fields | SLA |
|---|---|---|---|---|---|
| Market daily | Tushare `daily` | AKShare | Tencent | close, pre_close, vol, amount | close+30m |
| Limit prices | Tushare `stk_limit` | AKShare | Tencent | upper/lower limit and hit flags | close+30m |
| Reference industry | Tushare `stock_basic` | AKShare EM | AKShare XQ | industry | stale <= 1 week |
| Reference shares | Tushare `daily_basic` | AKShare EM | AKShare XQ | total shares | stale <= 1 day |

US variant feed chain:

| Feed | Primary | Fallback | Critical fields |
|---|---|---|---|
| Market daily | Alpaca bars | local sample fallback | close, pre_close, volume, amount, adv |
| Reference profile | yfinance | local reference overlay | industry, shares outstanding |
| Trading state | Alpaca assets | local tradable assumptions | tradable/status |

US pilot note:

- Alpaca credentials are required in environment (`ALPACA_API_KEY`, `ALPACA_SECRET_KEY`) for real-sample checks.
- US mode treats limit-up/limit-down checks as not applicable and relies on tradable/status plus blacklist controls.

### 4.2 Degradation Protocol

Level 1 (single fallback active):

- Log only.

Level 2 (primary + fallback1 both fail, fallback2 active):

- Log and flag in daily check.
- If >3 consecutive days, investigate root cause.

Level 3 (all sources fail for critical field):

- Pipeline must fail-fast for that sample.
- Block same-day rebalance.
- Escalate as incident.

### 4.3 Retry Baseline

Default retry policy for provider/builder transient errors:

- Max retries: 2.
- Backoff: 30s then 60s.
- Still failing after retries: classify as incident (P1 or P2 based on impact).

## 5. Quantitative KPIs

### 5.1 Hard Gates (must pass)

| KPI | Threshold | Window |
|---|---|---|
| Nightly completion rate | >=95% (<=1 fail per 20 days) | Full pilot |
| Weekly release pass rate | 100% | Phase 2 + 3 |
| Override rate per rebalance | <=2/5 samples | Every rebalance |
| Critical data integrity incidents | 0 | Full pilot |
| Artifact chain completeness | 100% | Every rebalance |

Solver reliability gate:

- `SCS` fallback usage <= 5% of validation samples.
- Any `SCS` fallback must include audit trace (`solver_fallback_used=true` and residual recorded).

### 5.2 Soft Metrics (directional)

| Metric | Target direction |
|---|---|
| Cost advantage vs naive | >=80% rebalance cycles better |
| Fallback frequency | Stable or down |
| Execution residual risk | Matches liquidity profile expectations |
| Reviewer score average | >=3.5/5, stable or improving |

## 6. Pilot Tracking Dashboard

Use:

- `outputs/pilot_tracking/pilot_dashboard.csv`

Columns:

```csv
date,phase,mode,run_root,as_of_date,nightly_status,release_status,release_gate_passed,rebalance_triggered,artifact_chain_complete,override_count,cost_better_ratio,primary_feed_success,fallback_activated,solver_primary,blocked_untradeable_count,static_count,real_count,full_chain_success_static,full_chain_success_real,override_used_static,score_gap_ge_001_static,cost_better_ratio_static,solver_fallback_used_static,solver_sample_count_static,mean_order_reasonableness_static,mean_findings_explainability_static,mean_execution_credibility_static,execution_residual_risk_consistent,provider_blockers_count,incident_id,notes
```

This dashboard is the single source of truth for go/no-go review.

## 7. Stress Testing Protocol

### 7.1 Natural Stress Events

Qualifying event examples:

- CSI300 single-day move >=2%.
- >=3 holdings hitting limit-up/down in same day.
- Trading halt on holding with weight >=3%.
- Universe median volume drops >=50% vs 20-day baseline.

If triggered, mark in dashboard and run post-close validation against the latest stress execution report.

### 7.2 Synthetic Stress (if no natural event)

Scenario A - Liquidity shock:

- Reduce ADV estimates for top-5 names to 20%.
- Verify execution residual risk and participation alerts increase.

Scenario B - Mass limit-lock:

- Mark 30% holdings as limit-locked.
- Verify `blocked_untradeable` fires correctly and no phantom orders are created.

Scenario C - Data blackout:

- Disable Tushare and AKShare.
- Verify fallback/report behavior is explicit and non-silent.
- Passing behavior allows fail-fast on missing critical reference fields, but stale prior-day data reuse without trace is not allowed.

## 8. Incident Register

Use:

- `outputs/pilot_tracking/incident_register.csv`

Columns:

```csv
incident_id,date,severity,category,description,root_cause,resolution,time_to_resolve_hours,recurrence,linked_kpi_impact
```

Severity:

- P1: pipeline failure or data integrity breach.
- P2: degraded but completed operation.
- P3: notable observation.

## 9. Escalation for blocked_untradeable

| Consecutive days | Action |
|---|---|
| Day 1 | Log only |
| Day 2 | Add estimated resumption date |
| Day 3 | Escalate in weekly review |
| Day 5+ | Formal PM memo with compensating-action options |

This remains a human decision; system provides visibility and traceability.

## 10. Go/No-go Framework

GO:

- All hard gates pass.
- No unresolved P1 incident.
- Soft metrics stable or improving.

Conditional GO:

- Hard gates pass.
- Limited soft-metric drift or one resolved P1 with validated fix.
- Remediation owners and deadlines documented.

No-GO:

- Any hard gate fails.
- >=2 P1 incidents.
- Systemic feed instability.

Decision output:

- `outputs/pilot_tracking/go_nogo_decision.md`
- `outputs/pilot_tracking/go_nogo_status.md` (automated status aggregation; dual windows: rolling 20 and pilot-to-date)

## 11. Artifact Retention

During pilot:

- Retain all nightly and release outputs.
- Retain full rebalance artifact chains.
- Retain all provider capability reports.
- Retain dashboard and incident snapshots.

Estimated footprint: under 5 GB for full pilot.

## 12. Out-of-Scope (deferred)

- Multi-portfolio support.
- Broker integration.
- Intraday real-time monitoring.
- Full RBAC and permissioning.
- DR/failover architecture.
- Runtime optimization unless nightly >30 minutes.
