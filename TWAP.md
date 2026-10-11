# Adaptive TWAP with trajectory tolerance and opportunistic execution

## 1. Recommended approach and research basis

Build a **linear TWAP scheduler with separate controllers for urgency, aggressive opportunities, and passive placement**. Express tolerance as seconds ahead or behind schedule. Prioritize completion within the parent price limit, and use bounded exponential passive layering initially; evaluate truncated Lévy and other distributions experimentally.

The repository currently contains a Java 21 container proposal, so these are proposed strategy interfaces rather than changes to existing implementation.

| Approach | What the research supports | Design choice |
|---|---|---|
| Joint passive/aggressive allocation | Order placement depends on execution uncertainty, fees, order flow, and venue characteristics. [Cont–Kukanov](https://arxiv.org/abs/1210.1625) | Allocate between resting orders and liquidity-taking orders using expected cost and fill probability. |
| Deadline-aware passive pricing | Passive execution models couple placement distance with remaining quantity, time, and non-execution risk. [Guéant–Lehalle–Fernandez-Tapia](https://arxiv.org/abs/1106.3279) | Move placement toward the touch as urgency increases; withdraw deep orders near completion. |
| Queue-aware modelling | Queue-reactive models condition order flow on book state and support execution simulation. [Huang–Lehalle–Rosenbaum](https://arxiv.org/abs/1312.0563) | Add this when depth data becomes available. Quotes and own fills do not identify queue position. |
| Heavy-tailed placement | Empirical studies found power-law incoming limit-order distances in particular equity markets. [Bouchaud–Mézard–Potters](https://arxiv.org/abs/cond-mat/0203511) | Treat heavy tails as candidate placement shapes; those observations do not establish an optimal execution policy. |

For FX adapters, distinguish firm liquidity from dealer streams and account for rejection and latency. BIS documents fragmented liquidity, limited trade visibility, and execution risk borne by algorithm users. These are reasons to avoid assuming a consolidated public volume or a universally executable book. [BIS execution-algorithm report](https://www.bis.org/publications/fx-execution-algorithms-and-market-functioning)

The formulas and parameter choices below are proposed engineering choices informed by this research, not published optimality results.

## 2. Trajectory and urgency controller

Let:

- \(Q\): parent quantity; \(T\): duration; \(t\): elapsed time.
- \(r=Q/T\): planned execution rate.
- \(x(t)=r\,t\): planned cumulative execution.
- \(y(t)\): confirmed cumulative fills.
- \(a,b\): permitted seconds ahead and behind.

Define the execution corridor:

\[
L(t)=\max(0,x(t)-rb), \qquad
U(t)=\min(Q,x(t)+ra).
\]

The measured deviation in seconds is:

\[
e(t)=\frac{y(t)-x(t)}{r}.
\]

For the README’s 100 million over one hour, 30 seconds ahead permits approximately 833,333 extra units; 60 seconds behind permits approximately 1.67 million fewer units than scheduled.

**Make the upper corridor an exposure constraint.** With \(W(t)\) representing the maximum additional fills possible from all outstanding children:

\[
y(t)+W(t)\le U(t).
\]

Include pending submissions, unresolved aggressive requests, pending cancels, and potentially fillable replacement orders in \(W\). Release reservations only after authoritative order-state updates. Expected fill probabilities must never weaken this constraint.

The lower corridor is a recovery objective. Liquidity shortages or the price limit can make it impossible to maintain.

Use the following operating behaviour:

| Condition | Execution behaviour |
|---|---|
| Ahead, approaching upper boundary | Reduce new orders; preserve only useful passive exposure within available capacity. |
| Inside corridor | Maintain base rate; choose passive or aggressive execution according to cost. |
| Behind schedule | Increase planned rate and concentrate passive placement nearer the touch. |
| Below lower boundary | Use limit-protected aggressive execution without waiting for a cheap-spread signal. |
| Terminal window | Cancel passive exposure, release confirmed reservations, and pursue remaining quantity aggressively within limits. |

For v1, use a proportional catch-up rate:

\[
v_{\text{desired}}
=\operatorname{clip}\left(
r+\frac{x(t)-y(t)}{H(t)},\,0,\,v_{\max}
\right),
\]

where \(H(t)=\max(h,\min(H_0,T-t))\), \(h\) is the planning interval, and \(H_0\) the recovery horizon. Subtract conservative expected fills from existing children when calculating additional planned quantity, then apply the hard exposure reservation.

Start terminal behaviour during the final \(\min(60\text{ seconds},0.1T)\). Taper the lag allowance to zero over that window. A terminal transition overrides spread preference, but never the parent limit or container risk constraints.

At the deadline, stop new submissions and cancel remaining children. Keep reconciling late execution reports before marking the parent terminal. Report residual quantity and the reason completion was constrained.

## 3. Opportunistic aggression and passive layering

**Aggressive opportunities**

Measure opportunities using side-specific execution cost for the intended child size:

\[
C_{\text{take}}(q)
=s\,[P_{\text{executable}}(q)-m_t]
+\text{fees}
+\text{estimated retry/slippage cost},
\]

where \(s=+1\) for buys and \(-1\) for sells, and \(m_t\) is a fresh reference midpoint. Express all terms in consistent price or basis-point units.

Maintain historical cost distributions by instrument, venue, side, size bucket, and session. Compare current cost against a baseline calculated before incorporating the current observation.

For v1, consider an opportunity when:

- Cost is at or below its historical 20th percentile.
- Savings versus the historical median exceed an uncertainty buffer.
- Quote size covers the child quantity and quote freshness passes the adapter’s threshold.
- The child satisfies the parent limit and has reserved exposure capacity.

Execute a bounded opportunity clip, then re-evaluate after its result. Borrow from future execution only within the ahead allowance. Use a cooldown and separate enter/exit thresholds to prevent repeated reactions to a flickering quote.

A narrow spread is therefore a useful signal, but the decision uses executable cost. With quote-only data, do not extrapolate top-of-book prices to larger sizes or infer hidden depth.

Use **marketable limit IOC orders** where supported. Reserve FOK for explicitly indivisible clips or venue-specific requirements. Ordinary catch-up execution does not require the opportunity threshold.

**Passive placement**

Keep passive children on the parent side. For a buy, anchor at the venue bid; for a sell, at the ask. Start with offsets of 0, 1, and 2 ticks, subject to parent-limit and venue rules. Use post-only when available.

Separate three quantities:

1. Total passive exposure budget.
2. Eligible price levels.
3. Quantity weights across those levels.

The exposure budget must fit inside available lead capacity and the parent residual. Passive orders placed on several venues share that budget; each venue must not receive the full residual independently.

Start with bounded exponential weights:

\[
w_k=\frac{e^{-k/\ell}}
{\sum_{j=0}^{K}e^{-j/\ell}}.
\]

Use \(\ell=1\) initially. As urgency increases, reduce the maximum distance and concentrate quantity at the touch. Round to venue quantity increments and leave unusable small residuals unposted.

| Distribution | Useful property | Main limitation |
|---|---|---|
| Exponential/geometric | Simple, concentrates exposure near the touch | May underrepresent useful distant opportunities |
| Truncated power law | Adjustable tail thickness | Deep exposure can contribute little before the deadline |
| Truncated Lévy | Heavy tail and adjustable placement scale | Can allocate too much exposure far from the touch |
| Empirical fill-adjusted weights | Responds to observed venue performance | Requires representative placement history |

For the Lévy experiment, use the one-sided Lévy distribution, not an unspecified Lévy process. Its standardized density has an \(x^{-3/2}\) tail, implying an infinite untruncated mean. [SciPy’s mathematical definition](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.levy.html)

Give the touch a separate weight, and allocate the remaining quantity using **CDF differences over tick bins**, conditioned on a finite maximum distance. Do not sample unbounded offsets and clamp them afterward; that would accumulate artificial weight at the boundary.

Use deterministic weights initially. Evaluate every distribution with identical exposure budgets, maximum distance, and urgency rules.

Preserve useful resting orders where possible. Reprice only when expected improvement exceeds replacement cost and loss of priority; cancel when exposure, price-limit, stale-data, or terminal conditions require it. On venues without passive-order support, use aggressive execution only.

## 4. Models and Java integration

Package the TWAP as an independently discovered strategy JAR. Keep scheduling and placement logic inside the strategy; keep connectivity, enforced limits, and audit logging in the container.

Introduce these minimum contracts:

- **Parent/config:** instrument, quantity, side, limit, start/end, lead/lag seconds, child/rate limits, opportunity settings, and placement model.
- **Market/venue capabilities:** timestamped quotes and sizes, tick/lot rules, supported order types, passive support, firmness, and optional depth/volume.
- **Execution events:** submission acknowledgements, fills, cancels, rejects, status reconciliation, and amendments with stable identifiers.
- **Decision output:** desired child actions, required quantity reservations, model version, and reason codes.

Use serialized events per parent for state transitions, fixed-point prices and quantities, and explicit monotonic logical-time messages on the input queue for pacing and deterministic replay. Audit each decision with trajectory position, outstanding exposure, market snapshot, opportunity score, and governing constraint.

Fit venue-specific passive models from own order histories:

- Probability of any fill over the planning horizon.
- Expected fill fraction, conditional on order size.
- Features: distance from touch, spread, quote volatility, order age, venue, side, size, and time remaining.
- Separate rejection/acceptance models for aggressive requests.

Start with pooled, regularized empirical estimates; introduce a discrete-time survival model when data supports it. Treat cancellation and deadline expiry as censoring events. Do not count cancelled orders as proof they could never fill, or assume a price touch guarantees execution.

Own fills describe the placement policy that generated them. Predictions for rarely used deeper levels remain uncertain and require controlled pilots.

Initial **replay defaults**, to be calibrated before live use:

- Ahead/behind allowances: 30/60 seconds.
- Recovery horizon: 60 seconds.
- Planning interval: 1 second, also triggered by execution and material quote events.
- Opportunity clip cap: 5 seconds of planned quantity.
- Passive exposure cap: 10 seconds of planned quantity.
- Exponential placement at 0/1/2 ticks.
- No automatic extension past the deadline.
- No participation percentage unless a reliable volume feed exists.

Venue-specific freshness, size, throughput, and maximum execution-rate limits must be supplied by the adapter/container configuration.

## 5. Validation and acceptance

Compare equal-slice aggressive TWAP, corridor TWAP, corridor plus opportunistic aggression, and each passive placement distribution under matched constraints.

Measure:

- Signed execution cost against full-interval midpoint TWAP.
- Arrival-price shortfall as a secondary metric.
- Completion ratio, residual quantity, and terminal execution cost.
- Lead/lag excursions, passive fill fractions, rejection rates, and cancellation traffic.
- Post-fill markouts and results by venue, size, volatility, and spread regime.

Report residual exposure alongside filled-quantity costs so incomplete strategies cannot appear superior simply by avoiding expensive fills.

Quote replay can assess opportunity signals and constraints, but cannot establish counterfactual passive fills or market impact. Use pessimistic/base/optimistic fill scenarios, followed by shadow evaluation and small controlled live pilots. Add queue-based replay when depth data becomes available.

Acceptance scenarios must cover partial fills, simultaneous venue fills, cancel/fill races, duplicate events, disconnect/recovery, stale quotes, amendments, unavailable passive support, and a binding parent limit.

Require:

- No overfill, limit violation, or exposure-reservation breach in race simulations.
- Verified completion where the simulated liquidity and constraints make completion feasible.
- Explicit residual reasons when completion is infeasible.
- Out-of-sample cost improvement without materially worse completion or tail cost before promoting an experimental layering model.

Assumptions: asset-neutral controller; quotes plus own fills initially; linear TWAP remains the reference trajectory; completion is pursued within a hard parent limit. Numerical defaults are research starting points, not claims of optimal calibration.
