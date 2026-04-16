# US Alpha Core Candidate Definition Sheet (2026-04-16)

## Common Evaluation Conventions

These conventions are frozen for all Week 2-3 candidate runs.

### Universe And Decision Grid

- Primary research target: US mid-cap style restart on a `rank_500_1500`-style monthly decision grid
- Current platform comparator object: expanded-US Phase 1 momentum seed in PortfolioOS
- Decision date `m`: final trading day of each monthly rebalance period
- Signal timestamp: close of `m`
- Trading assumption: signals are consumed on the next rebalance cycle after date `m`

### Shared Input Conventions

- `P_i(t)` = split-adjusted close for stock `i` on trading day `t`
- `P_M(t)` = split-adjusted close for the market proxy on trading day `t`
- `V_i(t)` = raw traded shares on day `t`
- `DV_i(t) = P_i(t) * V_i(t)` = dollar volume on day `t`
- `r_i(t) = P_i(t) / P_i(t-1) - 1`
- `r_M(t)` = market return using the frozen market proxy consumed by the current platform research stack
- `sector(i)` = static sector label from [`data/universe/us_universe_reference.csv`](/C:/Users/14574/Quant/PortfolioOS/.worktrees/codex-us-alpha-week1-freeze/data/universe/us_universe_reference.csv)

### PIT / Data-Lag Rule

All candidates in this sheet use only end-of-day prices, volumes, and static universe metadata available through the close of decision date `m`.

No candidate in Week 2-3 may depend on:

- announcement-timed labels
- analyst event timestamps
- point-in-time fundamentals
- post-close revisions after decision date `m`

### Direction Convention

- Every raw factor is frozen in a `high-is-good` orientation.
- If the economic story is negative (for example illiquidity or lottery demand), the definition below already carries the required negative sign.

## Family A: Residual Momentum / Residual Reversal

### `A1` Market-Residual `84/21` Momentum

**Economic idea**

Keep the old momentum family but strip out simple market-direction exposure before ranking names.

**Formula**

1. Raw skip momentum:
   - `M84_21_i(m) = P_i(m-21) / P_i(m-84) - 1`
2. Rolling market beta:
   - estimate `beta_i(m)` from OLS on trailing daily returns over the `126` trading days ending at `m-21`
   - regression: `r_i(t) = alpha_i + beta_i * r_M(t) + eps_i(t)`
   - require at least `63` valid daily pairs
3. Market-residual momentum:
   - `A1_i(m) = M84_21_i(m) - beta_i(m) * (P_M(m-21) / P_M(m-84) - 1)`

**Direction**

- high `A1` = better expected forward return

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history for stock and market proxy

### `A2` Sector-Residual `84/21` Momentum

**Economic idea**

Keep within-sector winners and avoid simply buying the sector that already rallied.

**Formula**

1. Raw skip momentum:
   - `M84_21_i(m) = P_i(m-21) / P_i(m-84) - 1`
2. Sector median raw momentum on date `m`:
   - `SectorMed_s(m) = median(M84_21_j(m))` for all eligible `j` with `sector(j) = s`
3. Sector-residual momentum:
   - `A2_i(m) = M84_21_i(m) - SectorMed_sector(i)(m)`

**Direction**

- high `A2` = stronger stock-specific momentum relative to sector peers

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- static sector labels

### `A3` Vol-Managed Residual Momentum

**Economic idea**

Keep residual momentum exposure but downweight names whose own recent volatility is too high.

**Formula**

1. Start from `A1_i(m)`
2. Realized volatility:
   - `sigma63_i(m) = annualized stdev(r_i(t))` over the trailing `63` trading days ending at `m`
3. Floor:
   - `sigma_floor = 20%` annualized
4. Vol-managed residual momentum:
   - `A3_i(m) = A1_i(m) / max(sigma63_i(m), sigma_floor)`

**Direction**

- high `A3` = attractive residual momentum after volatility scaling

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- market proxy

## Family B: Low-Frequency Microstructure / Liquidity

### `B1` Amihud Illiquidity Level

**Economic idea**

Illiquid names should underperform after cost and implementation frictions; the tradable long signal is the liquid side of that cross-section.

**Formula**

1. Daily Amihud proxy:
   - `ILLIQ_i(t) = |r_i(t)| / max(DV_i(t), 1)`
2. Monthly factor:
   - `B1_i(m) = - median(ILLIQ_i(t))` over the trailing `63` trading days ending at `m`

**Direction**

- high `B1` = more liquid = better expected forward return

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- daily volume

### `B2` Illiquidity Shock / Change

**Economic idea**

Names whose illiquidity is suddenly worsening should be penalized even if their long-run liquidity level looks acceptable.

**Formula**

1. `ILLIQ21_i(m) = median(ILLIQ_i(t))` over the trailing `21` trading days ending at `m`
2. `ILLIQ63_i(m) = median(ILLIQ_i(t))` over the trailing `63` trading days ending at `m`
3. Shock factor:
   - `B2_i(m) = - (ILLIQ21_i(m) / max(ILLIQ63_i(m), 1e-12) - 1)`

**Direction**

- high `B2` = recent liquidity improved or at least did not deteriorate sharply

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- daily volume

### `B3` Abnormal-Turnover-Conditioned Short-Term Reversal

**Economic idea**

A sharp short-term selloff accompanied by unusually heavy trading can leave temporary inventory pressure that mean-reverts over the next monthly window.

**Formula**

1. Five-day reversal leg:
   - `REV5_i(m) = - (P_i(m) / P_i(m-5) - 1)`
2. Abnormal dollar-volume ratio:
   - `ADV5_i(m) = mean(DV_i(t))` over the trailing `5` trading days ending at `m`
   - `ADV63_i(m) = mean(DV_i(t))` over the trailing `63` trading days ending at `m`
   - `AVOL_i(m) = max(ADV5_i(m) / max(ADV63_i(m), 1), 1) - 1`
3. Conditioned reversal:
   - `B3_i(m) = REV5_i(m) * AVOL_i(m)`

**Direction**

- high `B3` = recent loser under abnormal trading pressure, expected to mean-revert

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- daily volume

## Family C: Idiosyncratic Risk / Lottery

### `C1` Idiosyncratic Volatility

**Economic idea**

High idiosyncratic volatility is often associated with overpricing and unstable speculative demand; the tradable long side is low idiosyncratic volatility.

**Formula**

1. Estimate the daily market model over the trailing `63` trading days ending at `m`:
   - `r_i(t) = alpha_i + beta_i * r_M(t) + eps_i(t)`
2. Idiosyncratic volatility:
   - `IVOL63_i(m) = annualized stdev(eps_i(t))`
3. Long-side orientation:
   - `C1_i(m) = - IVOL63_i(m)`

**Direction**

- high `C1` = lower idiosyncratic volatility = better expected forward return

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history
- market proxy

### `C2` MAX Effect / Lottery Proxy

**Economic idea**

Names that recently printed extremely large daily upside moves often become lottery-like and subsequently underperform on a cross-sectional basis.

**Formula**

1. `MAX21_i(m) = max(r_i(t))` over the trailing `21` trading days ending at `m`
2. Long-side orientation:
   - `C2_i(m) = - MAX21_i(m)`

**Direction**

- high `C2` = lower recent lottery-like upside spike = better expected forward return

**PIT lag**

- available at the close of `m`

**Cadence**

- monthly / `21d` decision grid

**Required inputs**

- adjusted close history

## What Is Frozen By This Sheet

- exactly `8` candidate definitions
- no parameter sweeps during Week 2-3
- no new family can enter after this sheet is frozen
- no candidate may borrow event-driven labels or WRDS-only PIT fields during the qualification sprint

## Out Of Scope For These Candidates

The following are explicitly deferred and cannot enter Weeks 2-4 through the back door:

- analyst disagreement / forecast-error variants
- short-interest / borrow fee signals
- event-driven SUE or revision relaunches
- optimizer redesign or label redesign disguised as factor work
