package io.strategycontainer.bootstrap;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.connectors.*;
import io.strategycontainer.core.*;
import io.strategycontainer.journal.*;
import io.strategycontainer.risk.*;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Paced full-pipeline harness; production qualification must run on declared production-class
 * hardware.
 */
public final class Benchmark {
  private Benchmark() {}

  private static long allocated(com.sun.management.ThreadMXBean bean, long id) {
    return bean != null && bean.isThreadAllocatedMemorySupported() && id > 0
        ? bean.getThreadAllocatedBytes(id)
        : -1;
  }

  private static long gcCount() {
    long total = 0;
    for (var bean : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, bean.getCollectionCount());
    return total;
  }

  public static void run(
      Path directory, PluginRegistry plugins, int seconds, int warmup, boolean mixed)
      throws Exception {
    if (seconds <= 0 || warmup < 0)
      throw new IllegalArgumentException("Invalid benchmark duration");
    if (mixed) {
      plugins.require("is");
      plugins.require("passive");
    }
    EngineConfig config = EngineConfig.builder().build();
    AtomicReference<OutputSink> routing = new AtomicReference<>(o -> {});
    try (AsyncJournal journal =
        new AsyncJournal(
            directory, plugins.manifest() + config, 131072, 256L * 1024 * 1024, 100_000_000)) {
      EngineCore core =
          new EngineCore(config, plugins, journal, o -> {}, o -> routing.get().accept(o));
      long sequence = 0;
      for (int i = 1; i <= 1000; i++) {
        EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.INSTRUMENT);
        e.data = Instrument.builder().id(i).symbol("BENCH-" + i).build();
        core.process(++sequence, e);
      }
      for (int dest = 1; dest <= (mixed ? 2 : 1); dest++) {
        EngineEvent d = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
        d.data =
            Destination.builder()
                .id(dest)
                .name("BENCH-" + dest)
                .instrumentId(0)
                .supports(OrderType.LIMIT, TimeInForce.DAY)
                .supports(OrderType.LIMIT, TimeInForce.IOC)
                .postOnly(true)
                .build();
        core.process(++sequence, d);
        EngineEvent session = new EngineEvent().reset(EngineEvent.Kind.SESSION);
        session.destinationId = dest;
        session.a = 1;
        core.process(++sequence, session);
      }
      EngineEvent policy = new EngineEvent().reset(EngineEvent.Kind.POLICY);
      policy.data =
          Policy.engine(
              Limits.builder()
                  .maxWorking(1_000_000_000)
                  .maxChildren(10000)
                  .messages(10000, 10000)
                  .quoteAgeNanos(10_000_000_000L)
                  .build());
      core.process(++sequence, policy);
      for (int dest = 1; dest <= (mixed ? 2 : 1); dest++) {
        EngineEvent reconciled = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
        reconciled.destinationId = dest;
        core.process(++sequence, reconciled);
      }
      core.process(++sequence, new EngineEvent().reset(EngineEvent.Kind.READY));
      for (int i = 1; i <= 100; i++)
        core.process(
            ++sequence,
            EngineEvent.parent(
                ParentSpec.builder()
                    .id(i)
                    .instrumentId(i)
                    .quantity(100_000_000)
                    .limit(140000)
                    .window(0, (seconds + warmup + 3600L) * 1_000_000_000)
                    .strategy(mixed ? i % 3 == 0 ? "is" : i % 3 == 1 ? "passive" : "twap" : "twap")
                    .parameters(
                        mixed && i % 3 == 0
                            ? java.util.Map.of("referenceDestinations", "1,2")
                            : java.util.Map.of())
                    .build()));
      try (EngineRuntime runtime = new EngineRuntime(config, core, false);
          ConnectorDispatcher dispatcher = new ConnectorDispatcher(4096)) {
        dispatcher.register(1, new SimulatedConnector(runtime));
        if (mixed) dispatcher.register(2, new SimulatedConnector(runtime));
        dispatcher.start(runtime);
        routing.set(dispatcher);
        long start = System.nanoTime(), lastTime = 0, total = 0, measured = 0, nextBatch = start;
        int instrument = 0;
        com.sun.management.ThreadMXBean allocation =
            java.lang.management.ManagementFactory.getPlatformMXBean(
                com.sun.management.ThreadMXBean.class);
        if (allocation != null
            && allocation.isThreadAllocatedMemorySupported()
            && !allocation.isThreadAllocatedMemoryEnabled())
          allocation.setThreadAllocatedMemoryEnabled(true);
        long allocatedStart = -1;
        long gcStart = gcCount();
        boolean warmed = warmup == 0;
        if (warmed) runtime.metrics().resetLatency();
        while (System.nanoTime() - start < (seconds + warmup) * 1_000_000_000L) {
          long elapsed = System.nanoTime() - start;
          if (warmed && allocatedStart < 0 && runtime.coreThreadId() > 0)
            allocatedStart = allocated(allocation, runtime.coreThreadId());
          if (!warmed && elapsed >= warmup * 1_000_000_000L) {
            runtime.awaitIdle(5000);
            runtime.metrics().resetLatency();
            warmed = true;
            measured = total;
            allocatedStart = allocated(allocation, runtime.coreThreadId());
            gcStart = gcCount();
          }
          int rate = elapsed / 1_000_000_000L % 10 == 9 ? 200000 : 100000;
          long targetNanos = rate == 200000 ? 5000 : 10000;
          if (elapsed - lastTime >= 1_000_000) {
            runtime.publish(EngineEvent.time(elapsed, elapsed / 1_000_000), 1000);
            lastTime = elapsed;
          }
          int batch = 100;
          for (int j = 0; j < batch; j++) {
            instrument = instrument % 1000 + 1;
            while (!runtime.quote(
                instrument,
                mixed ? (int) ((total / 1000) % 2) + 1 : 1,
                110000,
                110002,
                10_000_000,
                10_000_000,
                lastTime)) Thread.onSpinWait();
            total++;
          }
          nextBatch += batch * targetNanos;
          long until = nextBatch;
          while (System.nanoTime() < until) {
            long remaining = until - System.nanoTime();
            if (remaining > 100_000) LockSupport.parkNanos(remaining - 50_000);
            else Thread.onSpinWait();
          }
        }
        runtime.awaitIdle(10000);
        long allocatedEnd = allocated(allocation, runtime.coreThreadId());
        System.out.println(
            "workload="
                + (mixed ? "mixed" : "twap")
                + " coreAllocatedBytes="
                + (allocatedStart < 0 || allocatedEnd < 0 ? -1 : allocatedEnd - allocatedStart)
                + " gcCollections="
                + (gcCount() - gcStart)
                + " allocationIncludesOrdersAndTimers=true");
        EngineCore.Snapshot finalSnapshot =
            runtime.snapshot().get(5, java.util.concurrent.TimeUnit.SECONDS);
        long activeParents =
            finalSnapshot.parents().stream().filter(p -> p.state().equals("ACTIVE")).count();
        System.out.println("activeParents=" + activeParents + " expectedParents=100");
        double rate = (total - measured) / (double) seconds;
        long p99 = runtime.metrics().percentileMicros(.99);
        System.out.println(
            "updates="
                + (total - measured)
                + " updates/s="
                + rate
                + " p50us="
                + runtime.metrics().percentileMicros(.5)
                + " p99us="
                + p99
                + " p999us="
                + runtime.metrics().percentileMicros(.999)
                + " journalBacklog="
                + journal.backlog()
                + " "
                + runtime.metrics().text());
        System.out.println(
            "qualification="
                + (rate >= 100000 && p99 < 100 && journal.healthy() && activeParents == 100
                    ? "PASS"
                    : "FAIL")
                + " CPU="
                + Runtime.getRuntime().availableProcessors()
                + " JDK="
                + System.getProperty("java.version")
                + " OS="
                + System.getProperty("os.name")
                + " warmupSeconds="
                + warmup
                + " measurementSeconds="
                + seconds);
      }
    }
  }
}
