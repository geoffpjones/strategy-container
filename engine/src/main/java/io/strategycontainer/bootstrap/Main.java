package io.strategycontainer.bootstrap;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.connectors.*;
import io.strategycontainer.core.*;
import io.strategycontainer.journal.*;
import io.strategycontainer.management.*;
import io.strategycontainer.replay.*;
import io.strategycontainer.risk.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** CLI bootstrap. Real connectors are supplied by asset-class JARs. */
public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println(
          "Usage: demo <journal-dir> <plugins-dir> [port] | replay <journal-dir> <plugins-dir> |"
              + " benchmark[-mixed] <journal-dir> <plugins-dir> [seconds] [warmup-seconds] | serve"
              + " <journal-dir> <plugins-dir> <profile> [port] [recovery-journal]");
      return;
    }
    EngineConfig config = EngineConfig.builder().build();
    Path journalPath = Path.of(args[1]);
    try (PluginRegistry plugins = new PluginRegistry().discover(Path.of(args[2]))) {
      switch (args[0]) {
        case "replay" -> System.out.println(ReplayVerifier.verify(journalPath, config, plugins));
        case "benchmark", "benchmark-mixed" ->
            Benchmark.run(
                journalPath,
                plugins,
                args.length > 3 ? Integer.parseInt(args[3]) : 1800,
                args.length > 4 ? Integer.parseInt(args[4]) : 120,
                args[0].equals("benchmark-mixed"));
        case "demo", "serve" -> serve(args, config, plugins);
        default -> throw new IllegalArgumentException("Unknown mode");
      }
    }
  }

  private static void serve(String[] args, EngineConfig config, PluginRegistry plugins)
      throws Exception {
    boolean demo = args[0].equals("demo");
    String token = System.getenv("STRATEGY_ENGINE_TOKEN");
    if (token == null || token.length() < 16)
      throw new IllegalArgumentException("Set STRATEGY_ENGINE_TOKEN (16+ characters)");
    if (!demo && args.length < 4) throw new IllegalArgumentException("Production profile required");
    Deployment deployment = demo ? null : Deployment.load(Path.of(args[3]));
    List<Instrument> instruments =
        demo
            ? List.of(
                Instrument.builder()
                    .id(1)
                    .symbol("EUR/USD")
                    .quantityUnit("EUR")
                    .priceScale(100000)
                    .build())
            : deployment.instruments();
    List<Destination> destinations =
        demo
            ? List.of(
                Destination.builder()
                    .id(1)
                    .name("SIMULATED-FX")
                    .instrumentId(0)
                    .sizes(1, 10_000_000)
                    .supports(OrderType.LIMIT, TimeInForce.IOC)
                    .supports(OrderType.LIMIT, TimeInForce.FOK)
                    .supports(OrderType.LIMIT, TimeInForce.DAY)
                    .postOnly(true)
                    .internalBooking(true)
                    .build())
            : deployment.destinations();
    Limits limits = demo ? Limits.builder().build() : deployment.limits();
    CountDownLatch shutdownComplete = new CountDownLatch(1);
    EventFeed feed = new EventFeed();
    AtomicReference<OutputSink> route = new AtomicReference<>(o -> {});
    try (AsyncJournal journal =
        new AsyncJournal(
            Path.of(args[1]),
            plugins.manifest() + config,
            32768,
            256L * 1024 * 1024,
            100_000_000)) {
      EngineCore core = new EngineCore(config, plugins, journal, feed, o -> route.get().accept(o));
      if (!demo && args.length > 5) {
        Path recovery = Path.of(args[5]);
        String manifest = plugins.manifest() + config;
        long prefix = JournalReader.completePrefix(recovery, manifest);
        ReplayVerifier.verifyPrefix(recovery, config, plugins, prefix);
        core.replayMode();
        JournalReader.visitPrefix(
            recovery,
            manifest,
            prefix,
            (type, sequence, payload) -> {
              if (type == 1) {
                while (journal.backlog() > 2048)
                  java.util.concurrent.locks.LockSupport.parkNanos(100_000);
                core.process(sequence, BinaryCodec.input(payload));
              }
            });
        core.process(core.sequence() + 1, new EngineEvent().reset(EngineEvent.Kind.RECOVER));
        core.liveMode();
      } else {
        long sequence = 0;
        for (Instrument i : instruments) {
          EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.INSTRUMENT);
          e.data = i;
          core.process(++sequence, e);
        }
        for (Destination d : destinations) {
          EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
          e.data = d;
          core.process(++sequence, e);
        }
        EngineEvent policy = new EngineEvent().reset(EngineEvent.Kind.POLICY);
        policy.data = Policy.engine(limits);
        core.process(++sequence, policy);
      }
      try (EngineRuntime runtime = new EngineRuntime(config, core, true);
          ConnectorDispatcher dispatcher = new ConnectorDispatcher(4096)) {
        ConnectorContext context =
            new ConnectorContext() {
              public boolean quote(
                  int i, int d, long bid, long ask, long bs, long as, long observed) {
                return runtime.quote(i, d, bid, ask, bs, as, observed);
              }

              public boolean report(ExecutionReport r) {
                EngineEvent e =
                    EngineEvent.report(
                        r.childId(),
                        r.destinationId(),
                        r.type(),
                        r.quantity(),
                        r.price(),
                        r.executionId());
                e.parentId = r.parentId();
                e.id = r.commandId();
                e.c = r.rejectClass().ordinal();
                e.liquidityRole = r.liquidityRole().ordinal();
                return runtime.offer(e);
              }

              public boolean session(int id, boolean online) {
                EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.SESSION);
                e.destinationId = id;
                e.a = online ? 1 : 0;
                return runtime.offer(e);
              }

              public boolean reconciled(int id) {
                EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
                e.destinationId = id;
                return runtime.offer(e);
              }

              public boolean recover(RecoveredOrder order) {
                EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.RECOVER_CHILD);
                e.data = order;
                return runtime.offer(e);
              }

              public long logicalNanos() {
                return runtime.metrics().logicalNanos;
              }
            };
        for (Destination d : destinations)
          dispatcher.register(
              d.id(),
              demo
                  ? new SimulatedConnector(runtime)
                  : plugins
                      .connector(deployment.connector(d.id()))
                      .create(d, deployment.connectorSettings(d.id()), context));
        dispatcher.start(runtime);
        route.set(dispatcher);
        int port = args.length > (demo ? 3 : 4) ? Integer.parseInt(args[demo ? 3 : 4]) : 8080;
        try (ManagementServer management =
            new ManagementServer(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
                runtime,
                plugins,
                feed,
                Map.of(
                    token,
                    new ManagementServer.Principal(
                        "operator", ManagementServer.Role.ADMIN, Set.of("WASH"))))) {
          management.start();
          CountDownLatch stop = new CountDownLatch(1);
          Thread hook =
              new Thread(
                  () -> {
                    stop.countDown();
                    try {
                      shutdownComplete.await();
                    } catch (InterruptedException ex) {
                      Thread.currentThread().interrupt();
                    }
                  },
                  "engine-shutdown");
          Runtime.getRuntime().addShutdownHook(hook);
          ScheduledExecutorService prices = Executors.newSingleThreadScheduledExecutor();
          try {
            if (demo) {
              EngineEvent session = new EngineEvent().reset(EngineEvent.Kind.SESSION);
              session.destinationId = 1;
              session.a = 1;
              runtime.publish(session, 1000);
              EngineEvent reconciled =
                  new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
              reconciled.destinationId = 1;
              runtime.publish(reconciled, 1000);
              runtime.publish(new EngineEvent().reset(EngineEvent.Kind.READY), 1000);
              prices.scheduleAtFixedRate(
                  () ->
                      runtime.quote(
                          1,
                          1,
                          110000,
                          110002,
                          10_000_000,
                          10_000_000,
                          runtime.metrics().logicalNanos),
                  0,
                  10,
                  TimeUnit.MILLISECONDS);
            }
            System.out.println(
                (demo ? "SIMULATED demo" : "Engine awaiting session/reconciliation readiness")
                    + " at http://localhost:"
                    + management.port()
                    + "; journal="
                    + args[1]);
            stop.await();
          } finally {
            prices.shutdownNow();
            // Cancel while the connector and core are still alive; unresolved orders require
            // recovery.
            EngineCore.Snapshot snapshot = runtime.snapshot().get(2, TimeUnit.SECONDS);
            for (EngineCore.ParentSnapshot parent : snapshot.parents()) {
              EngineEvent pause = new EngineEvent().reset(EngineEvent.Kind.PAUSE);
              pause.parentId = parent.id();
              runtime.publish(pause, 1000);
            }
            long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < drainDeadline
                && runtime.snapshot().get(2, TimeUnit.SECONDS).children().stream()
                    .anyMatch(c -> c.reserved() > 0))
              java.util.concurrent.locks.LockSupport.parkNanos(10_000_000);
            try {
              Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
            }
          }
        }
      }
    } finally {
      shutdownComplete.countDown();
    }
  }
}
