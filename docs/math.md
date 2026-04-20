# PortfolioOS Math Notes

## Objective

The MVP solves a single-period convex rebalance problem. The objective combines four terms:

1. `target_deviation`: squared deviation between post-trade approximate weights and target weights.
2. `transaction_fee`: commission, transfer fee, and sell-side stamp duty.
3. `turnover_penalty`: penalty on `gross_traded_notional / pre_trade_nav`.
4. `slippage_penalty`: simple market-impact term based on `k * |q| * sqrt(|q| / ADV)`.

The implementation intentionally uses the names `target_deviation` and `portfolio_deviation`. It does not call the objective a formal tracking-error model because the MVP does not include a full covariance or risk model.

## Hard Constraints

The optimizer enforces:

- no negative post-trade shares
- sell quantity cannot exceed current holdings
- cash must remain above `min_cash_buffer`
- non-tradable, limit-hit, and blacklisted directions cannot trade
- participation per order cannot exceed `participation_limit * ADV`
- effective single-name cap uses the stricter of the generic and double-ten limits
- industry exposure must stay within configured bounds
- turnover cannot exceed the configured maximum

Market-specific behavior:

- `market=cn`: blocked mask includes non-tradable and limit-hit flags; effective single-name limit can include double-ten tightening.
- `market=us`: limit-hit checks are skipped; only tradable/status and blacklist constraints block trades.
- `market=us`: double-ten controls are treated as out-of-scope in pretrade checks and effective single-name limits.

## Why Repair Exists

The optimizer works with continuous share quantities because that keeps the problem convex and stable. The repair stage converts the optimizer output into executable lot-aware orders by:

- zeroing blocked trades
- clipping sells and participation
- rounding toward zero to market lot size (`100` for CN default, `1` for US default)
- removing small residual tickets
- reducing buys until cash is valid under the exact same fee and slippage formulas

This separation keeps the optimizer simple while producing realistic order baskets.
