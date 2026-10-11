# Implement IS and Passive execution strategies

## Summary

Add independently deployable `is` and `passive` strategy plugins using the existing engine’s logical clock, reservations, constraints and replay infrastructure.

| Behaviour | IS | Passive |
|---|---|---|
| Objective | Reduce shortfall against inception mid | Execute a paced parent without taking liquidity |
| Trajectory | Configurable front-loaded curve | Linear trajectory with lead/lag tolerance |
| Aggressive execution | Allowed within parent limit | Prohibited |
| Benchmark | Composite midpoint fixed at scheduled start | No benchmark required |
| Deadline | Increase urgency, then cancel unresolved children | Cancel remaining children; report incomplete quantity |

IS will use configured urgency initially. Its design follows the execution-cost versus delay-risk trade-off described by [Almgren–Chriss](https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf), without claiming a calibrated optimal execution model. Passive pricing will respond to remaining time and quantity, consistent with the considerations in [Guéant–Lehalle–Fernandez-Tapia](https://arxiv.org/abs/1106.3279).

## Shared engine and API changes

- Add a default `StrategyFactory.executionProfile(ParentSpec)` describing aggression permission and benchmark requirements. Existing TWAP defaults preserve its current behaviour.
- Enforce `PASSIVE_ONLY` inside the engine, including manual child submissions, replacements and final dispatch authorization. Require LIMIT orders, DAY/GTC and genuine venue post-only protection. Reject unsupported venues; provide no aggressive fallback.
- Add borrowed `Views.Child` access and a parent-owned child visitor for inspecting leaves, reservations, prices, pending replacements and terminal states. Strategies use these to rebuild layer ownership after restart and avoid replacing unresolved children.
- Add borrowed effective order bounds: minimum/maximum child quantity, remaining exposure capacity, available child slots and minimum replacement interval. These intersect instrument, destination and applicable engine policies; command acceptance remains authoritative.
- Add borrowed current-reference and frozen-benchmark views. The engine owns the benchmark, preserving it through amendments, pause/resume, strategy restart and journal recovery.
- Extend execution reports with `LiquidityRole {UNKNOWN, MAKER, TAKER}` while retaining a compatibility constructor defaulting to UNKNOWN. Record a reported taker fill on a Passive parent as a financial fact, then raise a breach and suspend it. UNKNOWN relies on the connector’s documented post-only guarantee.
- Extend parent snapshots with benchmark, strategy state and execution-cost diagnostics. Use existing management endpoints and strategy parameter discovery; add UI forms for both strategies.

All market processing uses preallocated state and borrowed views. Configuration and management may allocate. No additional runtime libraries are required.

## IS implementation

### Inception composite

Require an explicit `referenceDestinations` list; execution uses one independently configured `destination`.

- Eligible constituents are online, firm, instrument-compatible quotes with positive sizes, valid bid/ask and age within `referenceAgeMillis`, default 1,000ms.
- Composite bid is the highest eligible bid; composite ask is the lowest eligible ask. Ties use destination ID for deterministic provenance.
- At least **one** eligible constituent is sufficient, as selected. Expose constituent count so degraded references are visible.
- Locked or crossed composites are invalid. Do not silently substitute a single venue.
- Capture on the first ordered input at or after scheduled start that supplies a valid composite. For a parent accepted after its start, use its first valid observation after acceptance; never fabricate a historical midpoint.
- Retain bid and ask separately so half-tick midpoints remain exact. Store capture sequence, logical time, source destinations and observation timestamps.
- Until capture, submit no children. After `benchmarkWaitMillis`, default 5,000ms, pause with an explicit missing-benchmark diagnostic. Expiry takes precedence.
- Benchmark/reference configuration becomes immutable after capture. Other permitted amendments preserve inception mid.
- Recovery adoption accepts original benchmark metadata. An already-started IS parent missing both journal and recovery metadata remains blocked; do not establish a replacement benchmark.

Share cached composite calculations between parents with identical reference configuration. Emit and hash a deterministic `BENCHMARK_CAPTURED` outcome.

### Trajectory and decisions

Let \(u\) be elapsed fraction of the execution window. Planned cumulative execution is:

\[
A(u)=Q\left[1-\frac{\sinh(k(1-u))}{\sinh(k)}\right]
\]

Use `urgency=2` by default, with supported range 0–8; zero gives linear TWAP. Compute using `StrictMath`, clamp endpoints, and round child quantities down to lots.

- Replan every `planningMillis`, default 1,000ms, with quote-driven opportunity checks limited to that frequency.
- Apply lead/lag corridors using \(A(t+\text{ahead})\) and \(A(t-\text{behind})\). Defaults: 30 seconds ahead and 60 seconds behind. Confirmed fills plus reservations must remain inside the upper release boundary.
- Normally release the next planning slice. Use passive layers when on schedule without an opportunity.
- An opportunity exists when the executable touch is at least as favourable as inception mid, or spread is below its trailing median and within its lowest 20th percentile. Require a valid current composite and execution quote.
- Sample spreads once per planning interval using a bounded rolling histogram; require 32 samples before using spread quantiles.
- Opportunities may release five seconds of trajectory, bounded by the lead corridor, visible liquidity, effective child limits and residual capacity.
- When behind, cancel passive exposure and use IOC for the released deficit after reservations permit. Prefer IOC; use FOK only if IOC is unsupported.
- In the final 10% of the window, capped at 60 seconds, progressively remove tolerated lag and pursue the residual within the upper corridor and parent limit.
- Inception mid is never a hard execution limit. A worse price may be accepted for urgency; the parent limit always governs.
- Stale execution/reference data prevents new exposure. Do not spin on constraint failures; wait for logical-time or order-state changes.

Report signed filled shortfall, average execution price and residual opportunity cost against inception mid. Show venue and wash contributions separately. Report price-only costs because fees are currently unavailable. Missing or stale marks produce unavailable opportunity-cost metrics, not zero.

## Passive implementation

Use linear pacing with the same default planning interval and lead/lag tolerances as TWAP.

- Require a configured post-only execution destination. Submit only LIMIT DAY/GTC children with `postOnly=true`.
- Release the next planning slice, bounded by the upper corridor. Cap outstanding exposure at `workingFraction=0.10` of parent quantity, rounded to lots and respecting effective child bounds.
- Default to three layers with offsets 0, 1 and 2 ticks and normalized exponential weights \(e^{-j}\). Support one to three layers; redistribute quantities that would fall below minimum child size.
- Normally anchor buys at bid and sells at ask.
- When confirmed fills fall below the lower corridor, allow `improvementTicks=1` inside the spread. Buy prices must remain strictly below ask; sell prices strictly above bid. Parent limits still apply.
- Preserve queue position: ordinary repricing requires at least a two-tick anchor change and `minimumRestingMillis=1,000`, additionally respecting engine replacement frequency.
- Use atomic replacement when supported. Otherwise cancel and wait for confirmed release before submitting a replacement.
- Track each layer through partial fills and pending actions. Replenish confirmed reductions without assuming cancellation released exposure.
- Cancel stale-data orders and children invalidated by amendments. Refresh plans when valid data returns.
- In the terminal window, consolidate into one layer at the nearest permitted passive price. Never switch to IOC/FOK or remove post-only.
- At expiry, cancel remaining children and expose the unfilled residual. No automatic extension or internal booking; existing explicit operator booking remains separately authorized.

## Validation and delivery

- Test buy/sell symmetry, half-tick benchmarks, degraded composites, tied sources, stale/locked/crossed markets, delayed capture and benchmark timeout.
- Verify inception mid survives amendment, restart and recovery; conflicting or missing recovery metadata blocks execution appropriately.
- Test IS curve endpoints, zero urgency, increasing front-loading, corridor reservations, favourable prices, cheap spreads, behind-schedule aggression and binding limits.
- Test Passive inside-spread improvement, one-tick spreads, minimum sizes, partial fills, queue-preserving updates, unsupported post-only, replacement races and incomplete expiry.
- Attempt aggressive commands through both plugin and management paths; verify the engine rejects them for Passive. Test reported taker-fill breaches.
- Verify identical decisions and hashes through replay, including capture, corrections, busts, pause/resume and recovery.
- Add deterministic scripted fill scenarios for strategy tests. Keep simulated passive fills explicitly labelled; quote replay cannot establish queue position or market impact.
- Version the journal schema for new report fields. Retain the old binary for existing schema-1 recordings; do not silently reinterpret them.
- Repeat the 100k-update benchmark with mixed TWAP/IS/Passive parents and shared composites. Report allocation, latency and journal health; retain the existing latency qualification requirement.
- Deliver two Gradle plugin modules, API/engine extensions, parameter documentation, UI support and reproducible smoke scenarios. No calibrated impact model, multi-venue execution routing or Lévy placement model in this iteration.
