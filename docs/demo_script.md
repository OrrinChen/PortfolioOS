# Demo Script

## Opening

Today we are not pitching another research model. We are showing a portfolio operations tool for A-share rebalance workflows.

You give PortfolioOS your current holdings, target weights, market snapshot, reference data, account cash state, and the mandate constraints you already work with. In one run, it produces an executable basket, tells you what got blocked, estimates the trading friction, and leaves a clean audit record.

## Input Story

The first message to the client team is simple:

"Give us your holdings, target, and constraints. We do not need you to learn a new UI or re-key the mandate into a spreadsheet."

The inputs are ordinary files the team already owns:

- current holdings
- target weights
- market snapshot
- reference and blacklist data
- account cash state
- a mandate template

That means less manual copy-paste between PM, compliance, and trading.

## Order Basket Story

The next step is to show the output basket.

"This is the order list you can actually review. It already reflects board-lot trading, cash limits, basic liquidity limits, and the trading blocks we know about from the input snapshot."

This is where the team sees:

- what to buy
- what to sell
- what estimated cost we attach to each ticket
- why each order exists in plain language

Then show the second export right after it:

"This second file is not for PM review. It is the OMS-friendly handoff file. It carries account, strategy tag, basket ID, and whether blocking checks are cleared for each order."

That helps the client see the bridge between analysis and operations.

## Blocked And Filtered Trades

Then move to the friction story.

"PortfolioOS also tells you which trades were not executable or were stopped by policy."

Show examples such as:

- suspended names
- limit-hit names
- buy blacklist names
- orders trimmed or deleted during repair

This matters because many teams still discover these issues late, after a naive basket is already circulated.

At this point, explain the distinction clearly:

- hard block: the order cannot be treated as cleanly releasable
- warning: the order can still be reviewed, but someone should look at the rule hit

PortfolioOS now carries that difference as structured fields, not only free-text messages.

## Benchmark Value Story

This is the Phase 2 business punchline.

"We do not just export a basket. We also compare it against a naive rebalance and a cost-unaware optimization."

For the audience, keep it practical:

- compared with a naive target chase, how much estimated cost did we avoid
- how many blocked or non-executable tickets did we avoid
- compared with a cost-unaware optimizer, did we spend less to reach a similar target

The point is not that PortfolioOS replaces judgement. The point is that it gives the desk a cleaner first draft with less execution friction.

## Second-Layer Demo: Replay Suite

After the audience understands the single-snapshot workflow, move to the second layer.

"The next question is usually: does this only look good on one example? So we also run a small replay suite of static snapshots."

Frame it carefully:

- this is not a historical backtest platform
- this is not a market simulator
- it is a batch replay of multiple realistic static operating snapshots

The replay story should focus on repeatability:

- across several samples, how often does PortfolioOS save estimated cost versus naive rebalance
- how often does it reduce blocked tickets
- how often does it reduce turnover
- how stable is the finding profile across samples

The right artifact to show is `suite_summary.md`, not raw JSON.

## How To Narrate The Replay Results

Use simple business language:

"Here is the single-snapshot story, and here is the distribution story. We are not asking you to trust one curated example. We are showing the same process across several different operating conditions: more cash, tighter cash, more blocked names, and more constraint pressure."

Then point to:

- median cost savings versus naive
- median turnover reduction versus naive
- median cost savings versus cost-unaware optimization
- best and worst samples, so the audience can see the range rather than only the average

## How To Tailor The Story By Client Type

For a public-fund audience:

- lead with mandate discipline
- show template switching and stricter blocked-trade treatment
- emphasize audit trail, hard blocks, and structured findings

For a private-fund or quant audience:

- lead with execution efficiency
- show lower estimated cost and cleaner OMS handoff
- emphasize that the same workflow still keeps warnings and hard blocks visible without slowing the desk down

## If The Client Asks: Can You Give Me More Than One Answer?

The right answer is yes, but keep it framed as decision support rather than prediction:

"We can run a scenario pack on the same snapshot. That means we keep holdings, target, market, and reference data fixed, then compare several policy choices side by side: for example a stricter public-fund style, a more execution-friendly variant, or a higher cash-buffer variant."

"The output is not just a list of numbers. It gives a recommended scenario under a transparent workflow score, and it also labels the cheapest scenario, the lowest-turnover scenario, the scenario with the fewest blocked trades, and the best target-fit scenario."

"So instead of forcing one answer, we can support a PM / trader / risk conversation about trade-offs."

## How To Show Recommended vs Alternative Scenarios

Use the scenario story in this order:

1. show the scenario table
2. point to the recommended scenario
3. compare it with the cheapest alternative
4. compare it with the lowest-turnover alternative
5. compare it with the best target-fit alternative

The key message is:

"We are not saying this is the best investment answer. We are saying this is the recommended operational scenario under the current workflow scoring rule, and here are the alternatives if the team wants to prioritize a different trade-off."

Use the trade-off language explicitly:

- if the PM wants tighter target fit, point to the best target-fit scenario
- if trading wants lower friction, point to the lowest-cost or lowest-turnover scenario
- if risk wants fewer hard issues, point to the fewest-blocked-trades scenario

That way the tool looks like a decision aid for a real meeting, not a machine that insists on one unquestionable answer.

## If The Client Asks: Can You Also Show Me How It Gets Signed Off?

The answer should stay grounded:

"Yes. After the scenario pack, we can create an approval request that names the selected scenario, records who made the decision, states the rationale, acknowledges any remaining warnings, and freezes a final execution package for handoff."

Walk them through the chain:

- recommended scenario
- human decision maker
- explicit warning acknowledgement
- frozen final orders and OMS-ready basket
- trader / risk / compliance handoff contacts

The important message is:

"The tool does not replace the decision maker. It makes the decision explicit, auditable, and easier to hand off."

## If The Client Asks: You Gave Me A Final Basket, But What About Execution?

The answer should stay practical:

"Yes. We can now run a local execution simulation on the frozen final basket before any real trading integration exists."

Then explain the value in business language:

- it estimates how the basket may fill under a simple participation / TWAP-style schedule
- it surfaces partial fills and names that may remain unfilled
- it estimates execution price, fee, slippage, and total cost
- it gives trading and risk a residual-risk view before live handoff

The positioning should be explicit:

"This is not a broker connection and not a high-frequency simulator. It is a local preflight layer between the frozen basket and any future live execution interface."

If the audience asks why that matters, the short answer is:

"Because the final basket is not the end of the workflow. Before a real trader or API touches it, we still want to know whether the basket looks broadly fillable, where liquidity pressure sits, and which tickets are likely to carry residual execution risk."

## If The Client Asks: Our Export Files Do Not Match Your Schema

The right answer is:

"That is expected in a pilot. We now support declarative import profiles so we can map your current holdings, target, market, and reference CSV exports into the PortfolioOS standard schema without building a separate platform."

Keep the message practical:

- we map columns, defaults, booleans, and simple percentage scaling
- we do not ask the client to reformat everything by hand
- we still convert into one transparent internal schema before optimization
- we fail fast if a required mapped field is missing

The positioning should stay conservative:

"This is not a hidden ETL engine. It is a lightweight file-mapping layer so the pilot can work with the client team's current exports."

## If The Client Asks: Can The Execution-Simulation Parameters Be Tuned To Our Style

The answer should be yes, but framed carefully:

"Yes. We now separate execution calibration profiles from the core simulator logic. That means we can switch between templates such as a balanced day, a more liquid midday profile, or a tighter-liquidity profile, and we can still let the request override specific fields when needed."

The important follow-up line is:

"We are not fitting an ML model here. We are making the execution assumptions explicit, reviewable, and easy to tune during a pilot."

## If The Client Asks: What Does The Trader Actually Receive At Handoff

The answer should connect the artifacts:

"The trader still receives the OMS-friendly basket, but now the freeze and execution-preflight steps also produce a handoff checklist."

Explain what that checklist adds:

- selected scenario or frozen package source
- whether blocking findings are zero
- whether warnings were acknowledged
- whether execution simulation shows partial-fill or unfilled risk
- trader, reviewer, and compliance contacts
- explicit human checkboxes before downstream handoff

The key message is:

"We are not turning this into a workflow platform. We are giving the desk a cleaner file-based handoff package that is easier to review before anything moves into a live downstream process."

## Audit Story

Close the workflow with the control story.

"Everything that drove the run is saved locally: inputs, parameters, findings, orders, summary, and benchmark comparison."

That gives the team:

- a reproducible run ID
- a documented constraint snapshot
- an audit JSON package for internal review
- a Markdown summary that is easy to circulate

## If The Client Asks: Why Should I Trust This?

Keep the answer operational, not academic:

"First, the inputs are checked before the engine is allowed to run. Unsafe issues such as missing market coverage, missing industry labels, duplicate tickers, or invalid prices fail fast. Softer issues, such as suspicious target concentration or abnormal benchmark metadata, are still surfaced as structured data-quality findings."

"Second, the findings are layered. We separate hard blocks from warnings, and we keep repair status so the team can see what was fixed automatically and what still needs attention."

"Third, we explain not only why an order exists, but also why an order did not exist. If a name is suspended, limit-hit, blacklisted, clipped for participation, or removed as dust, that reason is retained."

"Fourth, the replay suite means we are not asking you to trust one handpicked example. We can show how the same process behaved across several static snapshots."

"Finally, everything is auditable. Inputs, findings, repair actions, benchmark results, and OMS-ready exports are all persisted locally."

## Closing Message

The final positioning line should stay plain and conservative:

"PortfolioOS is an auxiliary decision-support tool. It helps PM, compliance, and trading teams move from target portfolio to reviewable basket with less manual friction, clearer blocked-trade visibility, lower estimated execution cost, and a complete audit trail. It does not constitute investment advice."
