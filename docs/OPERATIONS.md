# Running and operating the engine

Requires JDK 21. Gradle's wrapper downloads Gradle and the authorized build dependencies. Disruptor is the only third-party runtime dependency; JUnit and google-java-format are build tools.

```sh
./gradlew test :engine:installDist
mkdir -p build/plugins
cp example-twap/build/libs/example-twap-1.0-SNAPSHOT.jar build/plugins/
export STRATEGY_ENGINE_TOKEN="$(openssl rand -hex 24)"
engine/build/install/engine/bin/engine demo build/demo-journal build/plugins 8080
```

Use a new, empty journal directory for every run. Open http://localhost:8080 and paste the token into the UI. Demo quotes and executions are simulated; IOC/FOK fill at the command limit, passive orders rest until cancelled. The reference TWAP can be selected from the create-parent form. Quantities are integer instrument units; prices are integer ticks scaled by `priceScale`. Parent `startNanos` and `endNanos` are logical monotonic times, obtained from `/api/state`, not Unix timestamps.

The UI refreshes parent state and health once per second after connecting; use the auto-refresh checkbox to pause polling or Refresh for a manual update. Select a parent in the Plan and execution chart (or use its Chart button) to compare the current planned trajectory and tolerance band with venue fills, total fills including wash, and total fills plus reserved exposure. TWAP/Passive plans are linear; IS uses the configured urgency curve. Chart values are indicative browser floating-point quantities before engine lot rounding, and the tolerance band is a pacing guide. Execution observations use engine logical time and begin when the page connects; they are sampled snapshots, not a complete trade history, and older samples are thinned to bound browser memory. Reopening the page loses that history. Amendments redraw the plan from the current specification; engine-time regression clears observations. `/api/state` includes each parent's immutable current `specification` for this purpose.

The UI and API return asynchronous acceptance. HTTP 202 means queued; follow `/api/events` or `/api/stream` for outcomes. Every POST requires a bearer token, `Idempotency-Key` and form-encoded body. Parent actions require the current `version`. Repeating the same key/body is harmless; changing the body with the same key is rejected. Read `/api/state` before retrying a stale version.

| Endpoint | Purpose |
| --- | --- |
| GET `/api/state`, `/api/health`, `/api/strategies` | Snapshots, health, available plugins |
| GET `/api/events`, `/api/stream` | Recent outcomes or bounded SSE subscription |
| POST `/api/parents` | Create using id, account, instrument, side, quantity, limit, startNanos, endNanos, strategy and `param.*` |
| POST `/api/parents/{id}/pause`, `/resume`, `/restart`, `/cancel` | Lifecycle control |
| POST `/api/parents/{id}/amend` | Complete replacement specification plus version |
| POST `/api/parents/{id}/submit`, `/replace`, `/cancel-child`, `/query` | Child management |
| POST `/api/parents/{id}/preview`, `/book` | Versioned internal residual booking preview and confirmation |
| POST `/api/instruments`, `/api/destinations`, `/api/policies` | Admin configuration |
| POST `/api/session`, `/api/reconciled`, `/api/ready`, `/api/policies/reset` | Admin readiness and explicit scoped breaker reset |

`ManagementServer.command` specifies exact form fields. VIEWER is read-only; TRADER handles parents; ADMIN handles configuration. `Principal.washBooks` controls internal booking permissions. The CLI supplies one admin token; embed `ManagementServer` to configure multiple principals. It binds loopback: use an authenticated TLS proxy for remote access.

Run `python3 scripts/smoke.py` after building to exercise HTTP TWAP execution, graceful shutdown and deterministic replay in an isolated temporary directory. If the development callback-latency guard pauses a drained parent, the smoke test performs up to three explicit versioned amendments/resumes; it prints each recovery and verifies those inputs through replay. The final smoke run exercised one such recovery and matched 4,974 inputs and 83 outputs.

For IS and Passive plugin installation, controls, benchmark reporting and schema-2 compatibility, see [strategy operations](STRATEGIES.md). Use `python3 scripts/smoke.py --mixed` to include both new plugins.

## Recovery

```sh
engine/build/install/engine/bin/engine replay build/demo-journal build/plugins
engine/build/install/engine/bin/engine serve NEW_JOURNAL PLUGINS PROFILE.properties 8080 OLD_JOURNAL
```

Replay requires identical artifact fingerprints and engine capacities. Verification streams the journal, compares emitted decisions and commands, and reports the first divergence. Recovery accepts only a complete transaction prefix with valid checksums, verifies it, and restores it into a new journal. A truncated final transaction is excluded; corruption is an error.

After recovery the engine remains unready, parents require operator review, and potentially live children retain uncertain reservations. Every connector must reconcile the OMS, including orders missing from the journal's asynchronous tail. `ConnectorContext.recover(RecoveredOrder)` adopts those orders; normalized reconciled reports resolve recorded orders. Only signal `reconciled(destination)` after that process completes. Bring sessions online and submit readiness. Restart or resume reviewed parents explicitly; do not assume external exactly-once delivery.

Pause cancels children but does not release reservations until confirmed terminal. Throttled or rejected cancellations retry on subsequent logical clock advances as cancellation budget replenishes. Cancels and status requests remain possible after journal failure. Broader circuit breakers require an explicit scoped reset. Internal booking requires a paused, drained parent, a fresh preview and authorized WASH book; only an OMS BOOK acknowledgement creates a wash fill. Never use a booking request to conceal an uncertain venue child.

SIGTERM stops prices, pauses parents, allows up to five seconds for cancellation drain, closes connectors/core, and flushes the journal. A failed or timed-out drain requires OMS reconciliation. Host failure can lose asynchronously buffered records. Monitor `journalHealthy`, backlog, written/durable sequence watermarks, queue remaining capacity, backpressure and callback durations. Journal capacity exhaustion prevents new exposure. Preserve binary journals and deployment artifacts alongside operational logs.

## Performance qualification and current limits

```sh
engine/build/install/engine/bin/engine benchmark NEW_JOURNAL build/plugins 1800 120
```

The workload includes 1,000 instruments, 100 parents, journalling, simulated execution and recurring 200k/s bursts. Record CPU/storage/JDK/heap/collector settings; use JFR and GC logs to measure allocations and pauses. Repeat with representative real connectors, strategy fan-out and management traffic.

A 15-second measurement after five seconds of warm-up on this container (4 available CPUs, JDK 21.0.10, Linux) processed 113,347 price updates/s, p50 45µs, p99 812µs, p99.9 at the histogram ceiling of 4095µs; the journal remained healthy with zero end backlog. This **fails** the <100µs p99 requirement. It is a smoke result, not a 30-minute qualification or an allocation measurement. A previous smaller journal/write implementation exhausted its capacity; batching writes and increasing bounded capacity resolved that failure in this short run.

Parent/child identifiers and idempotency records use bounded lifetime capacity and are not recycled. Trade deduplication/correction history grows with executions; long sessions need controlled restart/recovery or a retention design before production sizing. Working-quantity limits use raw instrument units; child notional limits use scaled integer price × quantity × multiplier, not a cross-currency portfolio valuation. Instrument/destination IDs are bounded indexes. There is no external broker connector, durable snapshot compaction, calibrated queue-position model, calibrated Lévy placement model, or production-grade fill simulator in this repository. Those are deployment or further development work; the reference TWAP uses bounded exponential layering and a spread histogram.

Run dependency-free UI chart/polling checks with `node scripts/test-ui.cjs`.
