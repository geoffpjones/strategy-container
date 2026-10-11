# Strategy container

The Java 21 implementation uses Gradle and consists of `strategy-api`, `engine`, and the separately loaded `example-twap`, `example-is`, and `example-passive` strategies. It includes a Disruptor event loop, reservation ledger, scoped constraints, connector SPI, asynchronous journal/replay, authenticated HTTP management and a web UI.

```sh
./gradlew test :engine:installDist
```

See [running and operating the engine](docs/OPERATIONS.md) for the demo, endpoints, recovery and performance results; see [architecture and extension contracts](docs/ARCHITECTURE.md) for plugin development. The [TWAP design](TWAP.md) remains the research plan. See [IS and Passive usage](docs/STRATEGIES.md) and their [implementation plan](IS_PASSIVE.MD). The requirements below remain the intended acceptance specification; the implementation's outstanding qualification and scope limits are explicit in the runbook. The latest mixed throughput smoke sustained 113k updates/s with all 100 parents active and a healthy journal, but its p99 reached the 4,095µs histogram ceiling; full production qualification remains outstanding.

## Design requirements

## 1. Design goals and architecture

This document specifies a Java 21 container hosting independently deployed strategy JARs. Each front-office team can run its own engine instance for equities, FX, fixed income, or other products, supplying asset-class-specific OMS and market-data connectors. The container discovers strategy implementations at startup without depending on their implementation classes; reflection and metadata discovery stay outside the hot path.

The container owns connectivity, order state, constraints, operator controls, and observability. Strategies own execution decisions, including TWAP trajectory tolerance and passive layering. See [TWAP.md](TWAP.md) for the execution-strategy design; its clock and replay integration must follow the contracts below.

**Performance requirements**

- Sustain **100k incoming price updates/second per engine**, including sequencing, journal enqueue, subscription fan-out, strategy callbacks, and constraint evaluation.
- Target **p99 ingress-to-decision latency below 100µs** on declared benchmark hardware. Measure connector dispatch, disk persistence, and external acknowledgements separately.
- Aim for allocation-free steady-state price processing in the engine core. Measure strategy and connector allocation separately; do not promise a GC-free JVM.
- Use bounded queues, explicit capacity limits, and controlled behaviour under overload.

Use one serialized execution core per engine initially. This core owns mutable order state, counters, timers, and strategy callbacks. Connector, journal, and HTTP/UI work runs outside it. Strategies must not block callbacks or perform network/file I/O.

Use **LMAX Disruptor** for preallocated event transport, behind a small internal abstraction. Its preallocation and sequencing support low-allocation processing, but deterministic results require additional application rules. [Disruptor documentation](https://lmax-exchange.github.io/disruptor/user-guide/)

Start with a configurable bounded ring and a yielding wait strategy; provide a busy-spin deployment profile for dedicated cores. Benchmark both. Avoid sharding the order ledger in v1; introduce partitions only after measurements justify the added cross-partition constraint coordination.

Assume trusted strategy JARs. Providing only container services is an architectural boundary, not a security sandbox.

## 2. Interfaces, event ordering, and memory ownership

### Required interfaces

Strategies use asynchronous Java command/event APIs; operators use authenticated HTTP commands and streamed updates.

| Service | Required capabilities |
|---|---|
| Market data | Subscribe/unsubscribe; timestamped quotes, sizes, source and freshness; optional depth; gap and availability events. |
| Instruments | Quantity units, fixed-point scales, tick/lot increments, minimum tradable size, sessions, and contract multipliers where needed. |
| Destinations | Session health, routing eligibility, supported instruments, size limits, firmness, and supported order-type/time-in-force combinations. |
| Parent context | Side, instrument, quantity, price limit, execution window, parameters, lifecycle, and configuration version. |
| Execution | Submit, amend/replace, cancel, query, and reconcile; stable command identifiers and asynchronous outcomes. |
| Ledger and constraints | Fills, residual, children, reservations, unresolved requests, effective policies, and available message budgets. |
| Timers | Register/cancel deadlines against engine logical time; receive serialized timer callbacks. |
| Strategy management | Discover JARs and parameter definitions; validate configurations; publish progress, diagnostics, and errors. |
| Internal booking | Preview and submit wash-book residual fills through connectors that support OMS-confirmed booking. |

Expose operator commands to create/amend parents, pause/resume/restart/cancel, manage sessions and policies, inspect order state/audits, and confirm wash-book completion. Commands carry identity, authorization, idempotency keys, and expected versions.

Separate limit/pegged order types from IOC/FOK/DAY/GTC time-in-force instructions. Describe replacement semantics and enforceable peg protection explicitly.

### Deterministic event model

All decision-affecting inputs enter the same ordered stream:

- Market updates and connectivity changes.
- Execution reports, corrections, rejects, and reconciliation results.
- Operator commands and configuration changes.
- Explicit `TimeAdvance` messages.
- Connector dispatch outcomes and other external results affecting state.

Assign each input an engine sequence number. Record that order and replay it unchanged; do not reconstruct ordering by sorting timestamps.

Only `TimeAdvance` updates engine logical time. A live clock publisher reads system clocks outside the deterministic core and emits advances at a configurable cadence, initially 1ms. Record monotonic elapsed time and UTC where calendar rules require it.

Strategies, timers, quote-age checks, throughput windows, and reject breakers use logical time. They must not call `System.currentTimeMillis()`, `System.nanoTime()`, or read unrecorded external state for decisions. System clocks remain available outside the core for operational latency measurement.

Expire timers deterministically, ordered by deadline and registration sequence. Generate command identifiers from recorded engine identity and deterministic counters. Record random seeds; prohibit unseeded randomness and unordered collection traversal where these affect decisions.

### Allocation and ownership

Use fixed-point primitive values, numeric instrument/destination identifiers, reusable event slots, and bounded order-state storage. Resolve strings and reflective metadata outside the hot path.

Prefer immutable, validated **builder-based configuration and management objects**, without public setters. Allow narrowly scoped mutable transport slots and reusable command composers where allocation matters.

Callback event views are borrowed and valid only during the callback. A strategy that retains information must copy it into its own state. Journal/UI consumers must finish reading a slot before reuse or copy it into separately owned bounded storage.

Avoid per-price futures, streams, boxing, formatted log strings, and repeated collection construction. Use targeted reuse rather than general object pools with unclear ownership.

## 3. Order constraints and operator behaviour

Every exposure-creating request passes through the container gate immediately before dispatch. Apply the intersection of engine, account/team, strategy, parent, instrument, and destination policies.

| Constraint | Enforcement |
|---|---|
| Parent size | Positive quantity, configured maximum quantity/notional, valid units and lot rules. |
| Parent exposure | Confirmed client fills plus maximum potentially fillable outstanding quantity cannot exceed parent quantity. |
| Child size | Minimum/maximum submitted size, lot increments, and optional notional limits; validate replacements using normalized quantity semantics. |
| Working exposure | Maximum concurrent children and aggregate working quantity/notional, with optional destination/price-level limits. |
| Parent limit | Buy protection at or below the limit; sell protection at or above it. Default interpretation excludes fees. |
| Child limits | Valid tick-aligned prices, fresh-reference collars, allowed order combinations, and enforceable worst prices for pegs. |
| Update frequency | Sustained and burst budgets for submissions/replacements; optional minimum replacement interval per child. |
| Throughput | Parent, strategy, account, session, and engine budgets; count actual connector messages generated by logical commands. |
| Reject frequency | Separate local validation failures, venue new/replace/cancel rejects, technical errors, and liquidity/last-look outcomes. |
| Lifecycle | Enforce ownership, side/instrument, execution window, eligible destinations, and active parent state. |

Let \(F\) be confirmed client fills and \(R\) maximum additional possible fills:

\[
F+R\le Q_{\text{parent}}.
\]

Reserve quantity before dispatch. Pending submissions, cancels, replacements, and uncertain requests retain reservations until authoritative resolution. An atomic replacement reserves the larger possible old/new remaining exposure; independently live replacements reserve both.

Actual fills and corrections remain authoritative even when they reveal a breach. Record them, alert, and stop additional exposure.

Rate-limited commands receive structured outcomes rather than entering an unbounded delayed queue. Maintain cancellation capacity and connector-aware dispatch priority. Track repeated local attempts separately to detect retry loops.

Reject breakers use configured consecutive counts and rolling rates with minimum sample counts. Parent trips pause and cancel that parent; destination trips block new exposure there and initiate cancellation/reconciliation. Require explicit reset.

**Pause cancels working children.** Resume/restart requires reconciliation. Tightened policies block new violations immediately and cancel violating children; expose transitional breaches until resolved. Reject parent quantity reductions below confirmed fills and drain outstanding exposure before committing a smaller quantity.

TWAP remains responsible for trajectory lead/lag checks.

**Wash-book force-fill**

For the 100m parent with 99.95m venue fills:

- Stop execution and resolve outstanding external exposure.
- Preview the 50k residual at a fresh ask for a buy or bid for a sell.
- Require authorized wash book, operator reason, parent version, and freshness validation.
- Preserve the parent price limit and internal-booking constraints; external minimum routing size does not apply.
- Submit an idempotent booking through the asset-class OMS connector.
- Complete the client parent only after confirmation.

Track venue and wash-book fills separately. An uncertain booking retains its reservation. Never manufacture a venue execution report.

## 4. Journaling, replay, and production operations

Use an **asynchronous, versioned binary journal**, implemented with JDK file/channel facilities and reusable encoding buffers.

Record every decision-affecting input, emitted command, constraint outcome, internal booking, and configuration transition. Include sequence numbers, correlation identifiers, logical time, schema version, checksums, strategy/connector artifact hashes, configuration, and random seeds.

Record normalized inputs sufficient to reproduce callbacks. Raw protocol capture is an optional connector facility.

Persist asynchronously using bounded buffers, batched writes, rotation, and periodic flush/sync. Publish written and durable sequence watermarks, backlog, oldest pending age, disk usage, and the possible loss window.

Orders may leave before journal persistence, as selected. Consequently:

- Replay guarantees cover complete recorded prefixes.
- Host failure may leave external orders absent from the durable journal.
- Recovery must reconcile with the OMS before enabling execution.
- Stable command identifiers support deduplication where the connector permits it; do not claim universal exactly-once delivery.

Do not silently drop journal events. Journal failure or exhausted capacity stops new exposure and raises an alert while cancellation/reconciliation remains available. Record overload/coalescing decisions if a connector supports lossy market-data recovery; default core processing is lossless.

Provide two replay modes:

- **Verification:** feed recorded inputs, suppress external dispatch, and compare generated commands, constraint decisions, and state hashes against recorded outputs.
- **Simulation:** replace execution connectors with deterministic, seeded fill models; identify results as simulated rather than reproduction of historical fills.

Replay runs without wall-clock sleeps, at arbitrary speed. Report the first divergence with its sequence, relevant state, and configuration.

Separate the binary replay/audit journal from structured operational logs. Operational logs include lifecycle changes, errors, constraint trips, and recovery progress—not a formatted line for every price update.

Expose queue occupancy, throughput, processing latency, callback duration, allocation/GC behaviour, journal lag, connector latency, reject counts, and reservation discrepancies. Slow or failing callbacks stop the offending strategy; exceptions must not escape into shared event processing.

Use JFR and GC logs for qualification. Evaluate G1 and Java 21 generational ZGC under the full workload rather than assuming one collector is best. [Java 21 GC guidance](https://docs.oracle.com/en/java/javase/21/gctuning/available-collectors.html)

## 5. Code organization, documentation, and acceptance

Use clear modules/packages for:

- `api`: strategy/connector contracts and configuration definitions.
- `core`: sequencing, logical time, lifecycle, and scheduling.
- `orders` and `risk`: ledger, reservations, policies, and counters.
- `journal` and `replay`: codecs, persistence, verification, and simulation.
- `connectors`, `management`, and `bootstrap`: external adapters, operator services, and deployment.

Keep strategy implementations outside the container modules.

Prefer JDK facilities and explicit wiring. Allow Disruptor as a runtime dependency; require a documented reason for others. Avoid a broad application framework, reflection in hot paths, Lombok, and automatic getter/setter generation. Build/test dependencies are assessed separately.

Document public/protected methods and non-obvious internal methods: ownership, callback lifetime, thread affinity, units, errors, lifecycle, and replay behaviour. Include architectural decisions, connector examples, operational procedures, journal compatibility rules, and performance reproduction instructions.

**Acceptance tests**

- Verify layered orders, simultaneous fills across destinations, fill/cancel and fill/replace races, uncertain sends, duplicate execution events, corrections, and trade busts.
- Exercise parent/child size boundaries, partial-fill residuals, lot rounding, buy/sell limits, price collars, stale references, and bounded pegged orders.
- Verify nested throughput budgets, bursts, repeated local failures, cancellation priority, reject breaker classification, suspension, and explicit reset.
- Cover parent amendments, policy tightening, pause/resume/restart reconciliation, and wash-book completion, including stale previews, duplicate requests, rejected bookings, and uncertain confirmations.
- Verify identical commands and state hashes across repeated replay runs and different replay speeds.
- Exercise equal-deadline timers, clock advances, seeded randomness, configuration changes, and journal truncation/corruption.
- Stress slot reuse, asynchronous readers, queue exhaustion, journal slowdown, callback failure, and crash/OMS recovery.

**Performance qualification**

Benchmark 100k updates/second across 1,000 instruments and 100 active representative TWAP parents, then increase subscription fan-out and active-parent counts to identify capacity.

After warm-up, run for 30 minutes with journaling and management telemetry enabled. Require p99 ingress-to-decision below 100µs, bounded memory/backlogs, and no event loss. Include recurring one-second 200k-update bursts and measure recovery.

Profile engine allocation separately from strategies, connectors, and management. Require no per-price engine-object allocation on the steady-state path; report measured bytes/event, GC activity, and all latency percentiles.

Publish CPU, storage, JDK build, heap/collector settings, wait strategy, journal sync policy, workload, and test results. The target applies to qualified deployments and representative strategies; arbitrary third-party strategy code cannot inherit an unconditional performance guarantee.
