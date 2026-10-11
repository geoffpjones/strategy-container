package io.strategycontainer;

import static org.junit.jupiter.api.Assertions.*;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.bootstrap.*;
import io.strategycontainer.connectors.*;
import io.strategycontainer.core.*;
import io.strategycontainer.journal.*;
import io.strategycontainer.management.*;
import io.strategycontainer.replay.*;
import io.strategycontainer.risk.*;
import io.strategycontainer.twap.TwapFactory;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class RuntimeIntegrationTest {
  @TempDir Path temporary;

  @Test
  void twapPluginTradesThroughFinalGateAndReplaysExactly() throws Exception {
    EngineConfig config =
        EngineConfig.builder()
            .ringSize(1024)
            .instruments(4)
            .destinations(4)
            .parents(8)
            .children(256)
            .build();
    PluginRegistry plugins = new PluginRegistry().register(new TwapFactory());
    Path directory = temporary.resolve("live");
    AtomicReference<OutputSink> routing = new AtomicReference<>(o -> {});
    EventFeed feed = new EventFeed();
    try (AsyncJournal journal =
        new AsyncJournal(directory, plugins.manifest() + config, 1024, 100_000_000, 1_000_000)) {
      EngineCore core =
          new EngineCore(config, plugins, journal, feed, o -> routing.get().accept(o));
      try (EngineRuntime runtime = new EngineRuntime(config, core, false);
          ConnectorDispatcher dispatcher = new ConnectorDispatcher(64)) {
        dispatcher.register(1, new SimulatedConnector(runtime));
        dispatcher.start(runtime);
        routing.set(dispatcher);
        configure(runtime);
        runtime.publish(
            EngineEvent.parent(
                ParentSpec.builder()
                    .id(1)
                    .instrumentId(1)
                    .quantity(100)
                    .limit(120)
                    .window(0, 10_000_000_000L)
                    .strategy("twap")
                    .parameters(Map.of("mode", "aggressive"))
                    .build()),
            1000);
        runtime.publish(EngineEvent.time(1_000_000_000L, 1000), 1000);
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        EngineCore.Snapshot snapshot;
        do {
          snapshot = runtime.snapshot().get(1, TimeUnit.SECONDS);
          if (snapshot.parents().getFirst().venueFilled() > 0) break;
          Thread.sleep(10);
        } while (System.nanoTime() < until);
        assertTrue(
            snapshot.parents().getFirst().venueFilled() > 0, "IOC should fill through dispatcher");
        assertTrue(
            feed.recent().stream()
                .anyMatch(
                    o ->
                        o.type().equals("ROUTE")
                            && o.command() != null
                            && o.command().type() == CommandType.NEW));
        runtime.publish(EngineEvent.time(10_000_000_000L, 10000), 1000);
        runtime.awaitIdle(1000);
      }
    }
    ReplayVerifier.Result result = ReplayVerifier.verify(directory, config, plugins);
    assertTrue(result.inputs() > 10);
    assertEquals(result, ReplayVerifier.verify(directory, config, plugins));
  }

  @Test
  void managementRequiresAuthenticationAndProvidesOwnedSnapshots() throws Exception {
    EngineConfig config = EngineConfig.builder().ringSize(1024).build();
    PluginRegistry plugins = new PluginRegistry().register(new EngineCoreTest.IdleFactory());
    EventFeed feed = new EventFeed();
    EngineCore core = new EngineCore(config, plugins, Journal.none(), feed, o -> {});
    try (EngineRuntime runtime = new EngineRuntime(config, core, false);
        ManagementServer server =
            new ManagementServer(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                runtime,
                plugins,
                feed,
                Map.of(
                    "test-admin-token-12345",
                    new ManagementServer.Principal(
                        "test", ManagementServer.Role.ADMIN, Set.of("WASH")),
                    "test-read-token-12345",
                    new ManagementServer.Principal(
                        "read", ManagementServer.Role.VIEWER, Set.of())))) {
      server.start();
      HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      URI state = URI.create("http://localhost:" + server.port() + "/api/state");
      assertEquals(
          401,
          http.send(
                  HttpRequest.newBuilder(state).GET().build(), HttpResponse.BodyHandlers.ofString())
              .statusCode());
      HttpResponse<String> response =
          http.send(
              HttpRequest.newBuilder(state)
                  .header("Authorization", "Bearer test-admin-token-12345")
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(200, response.statusCode());
      assertTrue(response.body().contains("parents"));
      URI parents = URI.create("http://localhost:" + server.port() + "/api/parents");
      response =
          http.send(
              HttpRequest.newBuilder(parents)
                  .header("Authorization", "Bearer test-read-token-12345")
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .header("Idempotency-Key", "R")
                  .POST(HttpRequest.BodyPublishers.ofString("id=1"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(403, response.statusCode());
      response =
          http.send(
              HttpRequest.newBuilder(parents)
                  .header("Authorization", "Bearer test-admin-token-12345")
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .POST(HttpRequest.BodyPublishers.ofString("id=1"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(400, response.statusCode());
    }
  }

  @Test
  void concurrentProducersNeverCorruptReusablePriceSlots() throws Exception {
    EngineConfig config =
        EngineConfig.builder().ringSize(256).instruments(4).destinations(4).build();
    PluginRegistry plugins = new PluginRegistry();
    AtomicInteger seen = new AtomicInteger();
    plugins.register(
        new StrategyFactory() {
          public String name() {
            return "check";
          }

          public Strategy create(ParentSpec p) {
            return new Strategy() {
              public void onStart(StrategyContext c) {
                c.subscribe(1);
              }

              public void onQuote(StrategyContext c, Views.Quote q) {
                if (q.ask() != q.bid() + 1) throw new AssertionError("Torn quote");
                seen.incrementAndGet();
              }
            };
          }
        });
    EngineCore core = new EngineCore(config, plugins, Journal.none(), o -> {}, o -> {});
    try (EngineRuntime runtime = new EngineRuntime(config, core, false)) {
      configure(runtime);
      runtime.publish(
          EngineEvent.parent(
              ParentSpec.builder()
                  .id(1)
                  .instrumentId(1)
                  .quantity(100)
                  .limit(120)
                  .window(0, 100_000_000_000L)
                  .strategy("check")
                  .build()),
          1000);
      runtime.awaitIdle(1000);
      try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 4; t++)
          futures.add(
              pool.submit(
                  () -> {
                    for (int i = 0; i < 2000; i++)
                      while (!runtime.quote(1, 1, 100, 101, 1000, 1000, 0)) Thread.onSpinWait();
                  }));
        for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
      }
      runtime.awaitIdle(5000);
      assertEquals(8000, seen.get());
    }
  }

  @Test
  void timersWithEqualDeadlinesHaveStableRegistrationOrder() {
    List<Long> timers = new ArrayList<>();
    PluginRegistry registry =
        new PluginRegistry()
            .register(
                new StrategyFactory() {
                  public String name() {
                    return "timers";
                  }

                  public Strategy create(ParentSpec p) {
                    return new Strategy() {
                      public void onStart(StrategyContext c) {
                        c.schedule(2, 100);
                        c.schedule(1, 100);
                      }

                      public void onTimer(StrategyContext c, long id) {
                        timers.add(id);
                      }
                    };
                  }
                });
    EngineCore core =
        new EngineCore(EngineConfig.builder().build(), registry, Journal.none(), o -> {}, o -> {});
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.INSTRUMENT);
    e.data = Instrument.builder().id(1).symbol("T").build();
    core.process(1, e);
    e = new EngineEvent().reset(EngineEvent.Kind.POLICY);
    e.data = Policy.engine(Limits.builder().build());
    core.process(2, e);
    core.process(
        3,
        EngineEvent.parent(
            ParentSpec.builder()
                .id(1)
                .instrumentId(1)
                .quantity(10)
                .limit(100)
                .window(0, 1000)
                .strategy("timers")
                .build()));
    core.process(4, EngineEvent.time(100, 0));
    assertEquals(List.of(2L, 1L), timers);
  }

  @Test
  void slowEventClientsCannotBlockCore() {
    EventFeed feed = new EventFeed();
    try (EventFeed.Subscription subscription = feed.subscribe()) {
      for (int i = 0; i < 300; i++) feed.accept(new Output(i, "TEST", 0, 0, "", null));
      assertFalse(subscription.alive());
    }
  }

  private static void configure(EngineRuntime runtime) {
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.INSTRUMENT);
    e.data = Instrument.builder().id(1).symbol("TEST").priceScale(100).build();
    runtime.publish(e, 1000);
    e = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
    e.data =
        Destination.builder()
            .id(1)
            .name("SIM")
            .instrumentId(1)
            .supports(OrderType.LIMIT, TimeInForce.IOC)
            .supports(OrderType.LIMIT, TimeInForce.DAY)
            .postOnly(true)
            .internalBooking(true)
            .build();
    runtime.publish(e, 1000);
    e = new EngineEvent().reset(EngineEvent.Kind.POLICY);
    e.data = Policy.engine(Limits.builder().build());
    runtime.publish(e, 1000);
    e = new EngineEvent().reset(EngineEvent.Kind.SESSION);
    e.destinationId = 1;
    e.a = 1;
    runtime.publish(e, 1000);
    EngineEvent reconciled = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
    reconciled.destinationId = 1;
    runtime.publish(reconciled, 1000);
    runtime.publish(new EngineEvent().reset(EngineEvent.Kind.READY), 1000);
    runtime.publish(EngineEvent.quote(1, 1, 100, 101, 1000, 1000, 0), 1000);
    runtime.awaitIdle(1000);
  }

  @Test
  void managementCannotBypassPassiveProfileAndExposesParameters() throws Exception {
    EngineConfig config = EngineConfig.builder().ringSize(1024).build();
    PluginRegistry plugins =
        new PluginRegistry().register(new io.strategycontainer.passive.PassiveFactory());
    EventFeed feed = new EventFeed();
    EngineCore core = new EngineCore(config, plugins, Journal.none(), feed, o -> {});
    try (EngineRuntime runtime = new EngineRuntime(config, core, false);
        ManagementServer server =
            new ManagementServer(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                runtime,
                plugins,
                feed,
                Map.of(
                    "passive-admin-token-12345",
                    new ManagementServer.Principal(
                        "test", ManagementServer.Role.ADMIN, Set.of())))) {
      server.start();
      configure(runtime);
      HttpClient http = HttpClient.newHttpClient();
      String base = "http://localhost:" + server.port();
      var descriptions =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/strategies/passive"))
                  .header("Authorization", "Bearer passive-admin-token-12345")
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(200, descriptions.statusCode());
      assertTrue(descriptions.body().contains("improvementTicks"));
      String form =
          "id=1&account=1&instrument=1&side=BUY&quantity=1000&limit=120&startNanos=0&endNanos=100000000000&strategy=passive";
      var creation =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/parents"))
                  .header("Authorization", "Bearer passive-admin-token-12345")
                  .header("Idempotency-Key", "passive-create")
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .POST(HttpRequest.BodyPublishers.ofString(form))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(202, creation.statusCode());
      runtime.awaitIdle(1000);
      var snapshot = runtime.snapshot().get(1, TimeUnit.SECONDS);
      assertEquals(100_000_000_000L, snapshot.parents().getFirst().specification().endNanos());
      var stateResponse =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/state"))
                  .header("Authorization", "Bearer passive-admin-token-12345")
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(200, stateResponse.statusCode());
      assertTrue(stateResponse.body().contains("\"specification\""));
      assertTrue(stateResponse.body().contains("\"endNanos\":100000000000"));
      long version = snapshot.parents().getFirst().version();
      var aggressive =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/api/parents/1/submit"))
                  .header("Authorization", "Bearer passive-admin-token-12345")
                  .header("Idempotency-Key", "passive-attack")
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "version="
                              + version
                              + "&destination=1&quantity=10&price=101&type=LIMIT&tif=IOC&postOnly=false"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertEquals(202, aggressive.statusCode());
      runtime.awaitIdle(1000);
      assertTrue(
          feed.recent().stream()
              .anyMatch(o -> o.type().equals("CONSTRAINT") && o.detail().equals("UNSUPPORTED")));
      assertTrue(
          feed.recent().stream()
              .filter(o -> o.type().equals("COMMAND") && o.command().type() == CommandType.NEW)
              .allMatch(o -> o.command().postOnly()));
    }
  }
}
