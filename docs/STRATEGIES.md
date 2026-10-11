# IS and Passive plugins

Build with `./gradlew test :engine:installDist`. Copy these three separately deployable JARs into the plugin directory:

```sh
mkdir -p build/plugins
cp example-twap/build/libs/example-twap-1.0-SNAPSHOT.jar build/plugins/
cp example-is/build/libs/example-is-1.0-SNAPSHOT.jar build/plugins/
cp example-passive/build/libs/example-passive-1.0-SNAPSHOT.jar build/plugins/
```

IS and Passive bundle the internal `execution-support` classes in their own JARs; no extra runtime dependency or support JAR is needed. Both depend on the shared API, not engine implementation classes. They are available as `is` and `passive` in the UI. `/api/strategies/{name}` exposes parameter descriptions. Existing parent create/amend/lifecycle endpoints apply unchanged. Use `param.<name>` in form-encoded requests.

## Configuration

| Parameter | Default | Behaviour |
| --- | --- | --- |
| `destination` | 1 | One execution venue |
| `planningMillis` | 1000 | Logical planning interval |
| `aheadSeconds` / `behindSeconds` | 30 / 60 | Release lead and tolerated lag |
| `quoteAgeMillis` | 1000 | Strategy execution quote freshness; engine policy may be stricter |
| `layers` | 3 | One to three exponentially weighted passive layers |
| `workingFraction` | 0.10 | Maximum planned passive reservation as a fraction of parent quantity |
| `minimumRestingMillis` | 1000 | Minimum resting time before ordinary resize/reprice, also subject to policy |
| `improvementTicks` | 1 | Passive inside-spread improvement when behind or terminal; never crosses |
| `urgency` (IS) | 2 | Front-loading coefficient, 0–8; zero gives linear pacing |
| `referenceDestinations` (IS) | Required | Comma-separated firm reference venues; may include execution venue |
| `referenceAgeMillis` (IS) | 1000 | Maximum reference age |
| `benchmarkWaitMillis` (IS) | 5000 | Acquisition wait before pausing for review |

These are development defaults, not calibrated instrument/venue settings. Parent quantity, price limits, tick/lot rules and engine risk policies remain authoritative. Layers smaller than the effective minimum child quantity consolidate into the nearest level; child caps can reduce total released exposure. A working fraction smaller than one admissible child results in no passive order rather than rounding exposure upward.

## IS behaviour and objective

The engine captures its composite at the first valid ordered observation at or after scheduled start. It uses the highest fresh firm bid and lowest fresh firm ask across the configured list; a single eligible constituent is allowed. It excludes invalid, stale, indicative, offline and zero-size quotes, and rejects a locked/crossed resulting composite without selecting a different fallback market. Equal-price sources use the lowest destination ID.

Capture is immutable. The snapshot records both legs, source IDs, source observation times, constituent count, capture time and input sequence. Midpoints retain half ticks. No trading occurs before capture. Missing data pauses the parent after the wait timeout; resume can capture when fresh data returns. Expiry takes precedence. A scheduled start or reference configuration amendment never resets an already captured benchmark; reference changes after capture are rejected.

Configured urgency uses a deterministic hyperbolic front-loaded curve; it is not a calibrated market-impact optimizer. On schedule, IS works passive layers. Favourable executable prices versus inception mid or a narrow spread can release up to five seconds of trajectory within the lead corridor. The spread estimator samples once per planning interval, retains 256 observations in 128 buckets, requires 32 samples, and compares against the lowest 20th percentile and median. Very wide spreads saturate the overflow bucket and are not falsely classified as cheap.

Behind-schedule and terminal execution may take liquidity at worse-than-inception prices, within the parent limit. IS cancels passive exposure first and continues only when terminal facts release its reservations. It uses IOC, falling back to FOK where necessary. Stale reference/execution data blocks new exposure, including at final dispatch authorization. Completion is not guaranteed against missing liquidity, uncertainty or a binding parent limit.

## Passive behaviour

The engine enforces the passive permission independently of the plugin. Every child, including manual submissions and replacements, must be LIMIT DAY/GTC with post-only protection on a compatible venue. New and replacement orders are also checked against the current opposing quote at the final dispatch gate. There is no IOC/FOK fallback or automatic wash booking.

Normal layers anchor at bid for buys and ask for sells. Ordinary repricing requires a two-tick movement and the minimum resting period. Behind/terminal improvement permits a one-tick move inside the spread, strictly short of the opposing quote. Useful existing prices remain when a one-tick ordinary movement would not justify replacement. A resting price that would now cross drains rather than being resized at that price.

Atomic replacement preserves maximum potentially fillable old/new exposure. Other venues use cancel-and-confirm before replacement. Pending cancellations still count against the trajectory and working-fraction budgets. Amendments restore layers in price-priority order and cancel children on obsolete execution venues. Stale quotes cancel passive exposure. At expiry, the engine cancels children and reports `EXPIRED_INCOMPLETE` with the residual; incoming maker fills can complete a parent earlier.

Connectors should classify fills as MAKER or TAKER. UNKNOWN retains backward compatibility and relies on their documented post-only guarantee. A reported TAKER fill is recorded, then the parent enters ERROR and outstanding orders are cancelled. The engine cannot prove maker status if the external adapter supplies neither a trustworthy classification nor post-only semantics.

## Cost reporting, recovery and replay

Snapshots contain strategy phase, inception benchmark, average venue/wash execution price, signed venue and wash shortfall separately, and residual opportunity cost. Positive shortfall is adverse for either side. Costs exclude fees and use the instrument price scale and contract multiplier; average price remains in quoted price units. Opportunity cost marks the residual to a fresh current composite. Expiry freezes a valid fresh mark when available; missing marks are unavailable, never zero-filled. Reserved quantity is shown separately and is not counted as a confirmed execution.

Pause/resume, amendments and strategy restart preserve the benchmark. OMS adoption via `RecoveredOrder` can supply original benchmark metadata; conflicting metadata is rejected. A recovered IS parent lacking its original benchmark stays blocked for operator review. Cumulative fills recovered without execution-price history make average price and shortfall unavailable, while maintaining authoritative filled/reserved quantities. Supply execution identity/history through reconciliation rather than treating unknown history as free execution.

The journal is schema **2**. New normalized fields include liquidity classification and recovery benchmark metadata. Existing schema-1 journals require the retained schema-1 binary and original artifacts; no automatic migration is supplied. Replay uses identical artifacts, re-derives composite/capture decisions and compares all output commands, status transitions and checkpoints.

## Validation and performance

```sh
python3 scripts/smoke.py --mixed
engine/build/install/engine/bin/engine benchmark-mixed NEW_JOURNAL build/plugins 1800 120
```

The mixed smoke exercises discovery of the packaged JARs, HTTP parameter discovery, TWAP/IS simulated IOC fills, Passive cancellation at expiry, graceful shutdown and replay. It can explicitly amend/resume a drained parent after the development callback-latency guard pauses it, and records those recovery actions. Passive orders intentionally never fill in the demo simulator. Unit scenarios inject deterministic maker fills, partial fills, corrections, busts and rejection/cancellation races; they do not claim calibrated counterfactual fill modelling.

The mixed benchmark uses 1,000 instruments, 100 parents split across TWAP/IS/Passive, two reference venues and recurring 200k-update bursts. It reports latency, throughput, journal status, active-parent count, whole-core allocation and JVM GC collection count. Allocation includes order/timer/control work, not solely the steady-state price path. Qualification requires all 100 parents still active, throughput >=100k/s and p99 <100µs; a short smoke cannot establish the full 30-minute requirement. Use JFR and representative real adapters before deployment.

### Recorded short mixed run

After the ledger iteration change and journal sizing update, a 15-second measurement after five seconds of warm-up on this container (4 available CPUs, JDK 21.0.10, Linux, default heap/collector, yielding wait strategy) produced:

| Metric | Result |
| --- | --- |
| Price updates | 1,700,000 |
| Throughput | 113,333 updates/s |
| Active parents | 100 / 100 |
| p50 ingress-to-decision | 97µs |
| p99 / p99.9 | At the 4,095µs histogram ceiling; actual tail may be higher |
| Whole-core allocated bytes during measurement | 1,847,656, including orders/timers/control work |
| JVM GC collections during measurement | 0 |
| End journal backlog / producer backpressure | 0 / 0 |
| Journal health | Healthy |
| Qualification | FAIL: p99 requirement not met; full 30-minute qualification not run |

An earlier 32,768-slot run exhausted journal capacity during the burst workload. The launcher and benchmark now use 131,072 preallocated slots (about 128MiB of initial payload buffers, plus object/direct-buffer overhead), 256MiB segments and a 100ms sync interval. Capacity remains bounded; exhaustion still fails closed for new exposure. The successful short run does not guarantee survival of longer disk stalls. Financial/risk iteration now visits recorded children in stable ID order rather than scanning unused configured slots, while retaining the array for constant-time child lookup.

The final mixed HTTP smoke verifies packaged discovery, TWAP/IS completion, Passive incomplete expiry and complete journal replay. Unit/integration coverage totals 72 passing tests; Javadoc generation and UI JavaScript syntax checks also pass.
