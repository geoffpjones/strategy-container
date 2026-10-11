package io.strategycontainer;

import static org.junit.jupiter.api.Assertions.*;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.bootstrap.*;
import io.strategycontainer.core.*;
import io.strategycontainer.execution.*;
import io.strategycontainer.is.IsFactory;
import io.strategycontainer.journal.*;
import io.strategycontainer.passive.PassiveFactory;
import io.strategycontainer.replay.*;
import io.strategycontainer.risk.*;
import java.math.BigDecimal;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class IsPassiveTest {
  @TempDir Path directory;

  private EngineCoreTest.Fixture fixture() {
    return fixture(Journal.none());
  }

  private EngineCoreTest.Fixture fixture(Journal journal) {
    var f = new EngineCoreTest.Fixture(journal);
    f.core.replayMode();
    f.plugins.register(new IsFactory()).register(new PassiveFactory());
    f.policy(
        Limits.builder()
            .quoteAgeNanos(300_000_000_000L)
            .maxChildren(100)
            .maxWorking(100000)
            .maxParent(100000)
            .childSizes(1, 100000)
            .messages(10000, 10000)
            .cancellations(10000, 10000)
            .rejects(100, 100, 100, 300_000_000_000L)
            .build());
    return f;
  }

  private ParentSpec spec(String strategy, Side side, Map<String, String> settings) {
    Map<String, String> p =
        new HashMap<>(
            Map.of(
                "referenceDestinations",
                "1,2",
                "referenceAgeMillis",
                "300000",
                "quoteAgeMillis",
                "300000"));
    p.putAll(settings);
    return ParentSpec.builder()
        .id(2)
        .instrumentId(1)
        .quantity(1000)
        .limit(side == Side.BUY ? 120 : 80)
        .side(side)
        .window(0, 100_000_000_000L)
        .strategy(strategy)
        .parameters(p)
        .build();
  }

  private void create(EngineCoreTest.Fixture f, String strategy, Map<String, String> p) {
    f.send(EngineEvent.parent(spec(strategy, Side.BUY, p)));
  }

  private EngineCore.ParentSnapshot parent(EngineCoreTest.Fixture f) {
    return f.core.snapshot().parents().stream().filter(p -> p.id() == 2).findFirst().orElseThrow();
  }

  private List<EngineCore.ChildSnapshot> children(EngineCoreTest.Fixture f) {
    return f.core.snapshot().children().stream().filter(c -> c.parentId() == 2).toList();
  }

  private List<ExecutionCommand> commands(EngineCoreTest.Fixture f, CommandType type) {
    return f.outputs.stream()
        .filter(o -> o.type().equals("COMMAND") && o.parentId() == 2 && o.command().type() == type)
        .map(Output::command)
        .toList();
  }

  private void ack(EngineCoreTest.Fixture f) {
    for (var c : children(f))
      if (c.state().equals("PENDING_NEW"))
        f.report(c.id(), c.destination(), ReportType.ACK, 0, 0, "ack" + c.id());
  }

  private void time(EngineCoreTest.Fixture f, long seconds) {
    f.send(EngineEvent.time(seconds * 1_000_000_000L, seconds * 1000));
  }

  private void quote(EngineCoreTest.Fixture f, int dest, long bid, long ask, long observed) {
    f.send(EngineEvent.quote(1, dest, bid, ask, 10000, 10000, observed));
  }

  private void cancelReports(EngineCoreTest.Fixture f) {
    for (var c : children(f))
      if (c.state().equals("PENDING_CANCEL"))
        f.report(c.id(), c.destination(), ReportType.CANCELLED, 0, 0, "cancel" + c.id());
  }

  @Test
  void trajectoryIsMonotonicAndZeroUrgencyIsLinear() {
    assertEquals(0, Trajectory.quantity(1000, 0, 100, 2));
    assertEquals(1000, Trajectory.quantity(1000, 100, 100, 8));
    assertEquals(250, Trajectory.quantity(1000, 25, 100, 0));
    assertTrue(Trajectory.quantity(1000, 25, 100, 2) > 250);
    assertTrue(Trajectory.quantity(1000, 25, 100, 8) > Trajectory.quantity(1000, 25, 100, 2));
    long last = 0;
    for (int i = 0; i <= 100; i++) {
      long q = Trajectory.quantity(1000, i, 100, 2);
      assertTrue(q >= last);
      last = q;
    }
  }

  @Test
  void compositeCapturesExactHalfTickAndDeterministicSources() {
    var f = fixture();
    quote(f, 1, 101, 105, 0);
    quote(f, 2, 100, 104, 0);
    create(f, "is", Map.of());
    var b = parent(f).benchmark();
    assertEquals(102.5, b.mid());
    assertEquals(1, b.bidDestination());
    assertEquals(2, b.askDestination());
    assertEquals(2, b.constituents());
    quote(f, 1, 110, 111, 0);
    assertEquals(b, parent(f).benchmark());
  }

  @Test
  void tiedSourcesUseLowestDestinationAndOneConstituentIsValid() {
    var f = fixture();
    create(f, "is", Map.of());
    assertEquals(1, parent(f).benchmark().bidDestination());
    var g = fixture();
    EngineEvent offline = new EngineEvent().reset(EngineEvent.Kind.SESSION);
    offline.destinationId = 2;
    g.send(offline);
    create(g, "is", Map.of());
    assertEquals(1, parent(g).benchmark().constituents());
  }

  @Test
  void crossedAndLockedCompositesWaitWithoutTrading() {
    for (long bid : new long[] {103, 104}) {
      var f = fixture();
      quote(f, 1, 100, 103, 0);
      quote(f, 2, bid, 105, 0);
      create(f, "is", Map.of());
      assertNull(parent(f).benchmark());
      assertTrue(commands(f, CommandType.NEW).isEmpty());
    }
  }

  @Test
  void nonfirmAndZeroSizeSourcesCannotCapture() {
    var f = fixture();
    f.send(EngineEvent.quote(1, 1, 100, 101, 0, 100, 0));
    f.send(EngineEvent.quote(1, 2, 100, 101, 100, 0, 0));
    create(f, "is", Map.of());
    assertNull(parent(f).benchmark());
    EngineEvent d = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
    d.data =
        Destination.builder()
            .id(3)
            .name("INDICATIVE")
            .instrumentId(1)
            .firm(false)
            .supports(OrderType.LIMIT, TimeInForce.IOC)
            .build();
    f.send(d);
    EngineEvent online = new EngineEvent().reset(EngineEvent.Kind.SESSION);
    online.destinationId = 3;
    online.a = 1;
    f.send(online);
    quote(f, 3, 100, 101, 0);
    var s = spec("is", Side.BUY, Map.of("referenceDestinations", "3"));
    f.send(EngineEvent.parent(s));
    assertTrue(f.saw("INPUT_ERROR", "Duplicate parent"));
    var g = fixture();
    g.send(d);
    g.send(online);
    quote(g, 3, 100, 101, 0);
    g.send(EngineEvent.parent(s));
    assertNull(parent(g).benchmark());
  }

  @Test
  void scheduledStartUsesFreshQuoteAndTimeoutPauses() {
    var f = fixture();
    var p = spec("is", Side.BUY, Map.of("referenceAgeMillis", "1000"));
    p =
        new ParentSpec(
            p.id(),
            p.accountId(),
            p.instrumentId(),
            p.side(),
            p.quantity(),
            p.limit(),
            2_000_000_000L,
            p.endNanos(),
            p.strategy(),
            p.parameters());
    f.send(EngineEvent.parent(p));
    assertNull(parent(f).benchmark());
    time(f, 1);
    quote(f, 1, 100, 103, 1_000_000_000L);
    time(f, 2);
    assertEquals(2_000_000_000L, parent(f).benchmark().captureNanos());
    var g = fixture();
    time(g, 2);
    create(g, "is", Map.of("referenceAgeMillis", "1000"));
    assertNull(parent(g).benchmark());
    time(g, 7);
    assertEquals("PAUSED", parent(g).state());
    assertTrue(commands(g, CommandType.NEW).isEmpty());
    quote(g, 1, 100, 101, 7_000_000_000L);
    g.action(2, EngineEvent.Kind.RESUME);
    assertNotNull(parent(g).benchmark());
  }

  @Test
  void expiryTakesPrecedenceOverBenchmarkTimeout() {
    var f = fixture();
    quote(f, 1, 100, 101, 0);
    quote(f, 2, 100, 101, 0);
    time(f, 2);
    var p = spec("is", Side.BUY, Map.of("referenceAgeMillis", "1"));
    p =
        new ParentSpec(
            p.id(), 1, 1, p.side(), 1000, p.limit(), 0, 3_000_000_000L, "is", p.parameters());
    f.send(EngineEvent.parent(p));
    time(f, 3);
    assertEquals("CANCELLED", parent(f).state());
    assertEquals("EXPIRED_INCOMPLETE", parent(f).strategyState());
    assertNull(parent(f).benchmark());
  }

  @Test
  void inceptionSurvivesPauseRestartAndAmendment() {
    var f = fixture();
    create(f, "is", Map.of());
    var b = parent(f).benchmark();
    f.action(2, EngineEvent.Kind.PAUSE);
    cancelReports(f);
    quote(f, 1, 101, 102, 0);
    f.action(2, EngineEvent.Kind.RESTART);
    assertEquals(b, parent(f).benchmark());
    EngineEvent amend = new EngineEvent().reset(EngineEvent.Kind.AMEND);
    amend.parentId = 2;
    amend.a = parent(f).version();
    amend.data = spec("is", Side.BUY, Map.of("urgency", "4"));
    f.send(amend);
    assertEquals(b, parent(f).benchmark());
    amend = new EngineEvent().reset(EngineEvent.Kind.AMEND);
    amend.parentId = 2;
    amend.a = parent(f).version();
    amend.data = spec("is", Side.BUY, Map.of("referenceDestinations", "1"));
    f.send(amend);
    assertTrue(f.saw("INPUT_ERROR", "immutable after capture"));
    assertEquals(b, parent(f).benchmark());
  }

  @Test
  void favourableTouchCancelsPassiveThenTakesWithoutExceedingCorridor() {
    var f = fixture();
    create(f, "is", Map.of("aheadSeconds", "5"));
    ack(f);
    quote(f, 1, 99, 100, 0);
    quote(f, 2, 99, 100, 0);
    time(f, 1);
    assertTrue(
        commands(f, CommandType.NEW).stream().noneMatch(c -> c.timeInForce() == TimeInForce.IOC));
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream().anyMatch(c -> c.timeInForce() == TimeInForce.IOC));
    long upper = Trajectory.quantity(1000, 6_000_000_000L, 100_000_000_000L, 2);
    assertTrue(parent(f).reserved() <= upper);
  }

  @Test
  void behindScheduleCanTradeWorseThanInceptionWithinLimit() {
    var f = fixture();
    create(f, "is", Map.of("behindSeconds", "0"));
    ack(f);
    quote(f, 1, 100, 105, 0);
    quote(f, 2, 100, 105, 0);
    time(f, 2);
    cancelReports(f);
    var orders = commands(f, CommandType.NEW);
    assertTrue(
        orders.stream()
            .anyMatch(
                c ->
                    c.timeInForce() == TimeInForce.IOC && c.price() > parent(f).benchmark().mid()));
    assertTrue(orders.stream().allMatch(c -> c.price() <= 120));
  }

  @Test
  void parentLimitBlocksUrgentTaking() {
    var f = fixture();
    create(f, "is", Map.of("behindSeconds", "0"));
    quote(f, 1, 121, 122, 0);
    quote(f, 2, 121, 122, 0);
    time(f, 2);
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream().noneMatch(c -> c.timeInForce() == TimeInForce.IOC));
  }

  @Test
  void terminalDrainContinuesImmediatelyAfterCancellation() {
    var f = fixture();
    var p = spec("is", Side.BUY, Map.of());
    p = new ParentSpec(2, 1, 1, Side.BUY, 1000, 120, 0, 10_000_000_000L, "is", p.parameters());
    f.send(EngineEvent.parent(p));
    ack(f);
    time(f, 9);
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream().anyMatch(c -> c.timeInForce() == TimeInForce.IOC));
  }

  @Test
  void shortfallTracksCorrectionsAndWashSeparately() {
    var f = fixture();
    create(f, "is", Map.of("behindSeconds", "0"));
    ack(f);
    var child = children(f).getFirst();
    f.report(child.id(), 1, ReportType.FILL, 1, 101, "t1");
    assertEquals(0, parent(f).executionCosts().venueShortfall().compareTo(new BigDecimal("0.005")));
    f.report(child.id(), 1, ReportType.CORRECT, 1, 102, "t1");
    assertEquals(0, parent(f).executionCosts().venueShortfall().compareTo(new BigDecimal("0.015")));
    f.report(child.id(), 1, ReportType.BUST, 0, 0, "t1");
    assertEquals(0, parent(f).executionCosts().venueShortfall().signum());
    quote(f, 1, 104, 105, 0);
    quote(f, 2, 104, 105, 0);
    assertTrue(parent(f).executionCosts().residualOpportunityCost().signum() > 0);
    time(f, 301);
    assertNull(parent(f).executionCosts().residualOpportunityCost());
  }

  @Test
  void passiveHasOnlyPostOnlyDayOrdersAndConservativeExposure() {
    var f = fixture();
    create(f, "passive", Map.of());
    var orders = commands(f, CommandType.NEW);
    assertFalse(orders.isEmpty());
    assertTrue(
        orders.stream()
            .allMatch(c -> c.postOnly() && c.timeInForce() == TimeInForce.DAY && c.price() <= 100));
    assertTrue(parent(f).reserved() <= 100);
  }

  @Test
  void passiveImprovesInsideSpreadWhenBehindBuyAndSell() {
    for (Side side : Side.values()) {
      var f = fixture();
      quote(f, 1, 100, 104, 0);
      quote(f, 2, 100, 104, 0);
      f.send(
          EngineEvent.parent(spec("passive", side, Map.of("behindSeconds", "0", "layers", "1"))));
      ack(f);
      time(f, 2);
      assertTrue(
          commands(f, CommandType.REPLACE).stream()
              .anyMatch(c -> c.price() == (side == Side.BUY ? 101 : 103)));
      assertTrue(commands(f, CommandType.REPLACE).stream().allMatch(ExecutionCommand::postOnly));
    }
  }

  @Test
  void passiveNeverCrossesOneTickSpreadOrChangesForOneTickAnchor() {
    var f = fixture();
    create(f, "passive", Map.of("behindSeconds", "0", "layers", "1"));
    ack(f);
    time(f, 2);
    assertTrue(commands(f, CommandType.REPLACE).stream().allMatch(c -> c.price() < 101));
    var g = fixture();
    create(g, "passive", Map.of("layers", "1"));
    ack(g);
    quote(g, 1, 101, 102, 0);
    time(g, 1);
    assertTrue(commands(g, CommandType.REPLACE).stream().allMatch(c -> c.price() == 100));
  }

  @Test
  void passiveManualAggressionAndCrossingAreRejected() {
    var f = fixture();
    create(f, "passive", Map.of());
    f.submit(2, 1, 10, 101);
    assertTrue(f.saw("CONSTRAINT", "UNSUPPORTED"));
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.SUBMIT);
    e.parentId = 2;
    e.destinationId = 1;
    e.a = 10;
    e.b = 101;
    e.flags = (TimeInForce.IOC.ordinal() << 8) | (1 << 16);
    f.send(e);
    assertTrue(f.saw("CONSTRAINT", "UNSUPPORTED"));
    e.flags = (TimeInForce.DAY.ordinal() << 8) | (1 << 16);
    f.send(e);
    assertTrue(f.saw("CONSTRAINT", "PRICE"));
  }

  @Test
  void passiveFinalGateRechecksCrossingAfterAcceptedIntent() {
    var f = fixture();
    create(f, "passive", Map.of());
    var order = commands(f, CommandType.NEW).getFirst();
    quote(f, 1, 98, 99, 0);
    EngineEvent gate = new EngineEvent().reset(EngineEvent.Kind.DISPATCH);
    gate.id = order.id();
    f.send(gate);
    assertTrue(
        f.outputs.stream()
            .anyMatch(
                o ->
                    o.type().equals("ROUTE_SKIP") && o.detail().equals(Long.toString(order.id()))));
  }

  @Test
  void unsupportedPassiveDestinationFailsAdmission() {
    var f = fixture();
    EngineEvent d = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
    d.data =
        Destination.builder()
            .id(3)
            .name("NO-POST")
            .instrumentId(1)
            .supports(OrderType.LIMIT, TimeInForce.DAY)
            .build();
    f.send(d);
    create(f, "passive", Map.of("destination", "3"));
    assertTrue(f.saw("INPUT_ERROR", "post-only"));
    assertEquals(1, f.core.snapshot().parents().size());
  }

  @Test
  void takerFillIsRecordedThenBreachesPassiveContract() {
    var f = fixture();
    create(f, "passive", Map.of());
    var child = children(f).getFirst();
    EngineEvent fill =
        EngineEvent.report(child.id(), 1, ReportType.FILL, 1, child.price(), "taker");
    fill.liquidityRole = LiquidityRole.TAKER.ordinal();
    f.send(fill);
    assertEquals(1, parent(f).venueFilled());
    assertEquals("ERROR", parent(f).state());
    assertTrue(f.saw("BREACH", "passive-only"));
  }

  @Test
  void passiveStalenessCancelsAndExpiryRemainsIncomplete() {
    var f = fixture();
    create(f, "passive", Map.of("quoteAgeMillis", "1000"));
    ack(f);
    time(f, 2);
    assertFalse(commands(f, CommandType.CANCEL).isEmpty());
    cancelReports(f);
    time(f, 100);
    assertEquals("CANCELLED", parent(f).state());
    assertEquals(1000, parent(f).executionCosts().residual());
    assertTrue(
        commands(f, CommandType.NEW).stream().noneMatch(c -> c.timeInForce() == TimeInForce.IOC));
  }

  @Test
  void partialFillsKeepReservationsAndLayerOwnership() {
    var f = fixture();
    create(f, "passive", Map.of());
    ack(f);
    var child = children(f).getFirst();
    f.report(child.id(), 1, ReportType.FILL, 1, child.price(), "maker");
    assertEquals(1, parent(f).venueFilled());
    assertEquals(9, parent(f).reserved());
    time(f, 1);
    assertTrue(parent(f).reserved() <= 100);
    assertTrue(commands(f, CommandType.REPLACE).stream().allMatch(ExecutionCommand::postOnly));
  }

  @Test
  void nonAtomicReplacementWaitsForCancellation() {
    var f = fixture();
    EngineEvent d = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
    d.data =
        Destination.builder()
            .id(3)
            .name("NONATOMIC")
            .instrumentId(1)
            .atomicReplace(false)
            .postOnly(true)
            .supports(OrderType.LIMIT, TimeInForce.DAY)
            .build();
    f.send(d);
    EngineEvent session = new EngineEvent().reset(EngineEvent.Kind.SESSION);
    session.destinationId = 3;
    session.a = 1;
    f.send(session);
    quote(f, 3, 100, 101, 0);
    create(f, "passive", Map.of("destination", "3", "layers", "1"));
    ack(f);
    quote(f, 3, 104, 105, 0);
    time(f, 1);
    assertTrue(commands(f, CommandType.REPLACE).isEmpty());
    assertEquals(1, commands(f, CommandType.NEW).size());
    assertFalse(commands(f, CommandType.CANCEL).isEmpty());
    cancelReports(f);
    time(f, 2);
    assertEquals(2, commands(f, CommandType.NEW).size());
  }

  @Test
  void recoveryRequiresOriginalBenchmarkAndRejectsConflicts() {
    var f = fixture();
    f.send(new EngineEvent().reset(EngineEvent.Kind.RECOVER));
    var p = spec("is", Side.BUY, Map.of());
    EngineEvent recover = new EngineEvent().reset(EngineEvent.Kind.RECOVER_CHILD);
    recover.data =
        new RecoveredOrder(p, 10, 20, 1, 0, 0, 100, OrderType.LIMIT, TimeInForce.DAY, true);
    f.send(recover);
    assertEquals("BENCHMARK_RECOVERY_BLOCKED", parent(f).strategyState());
    f.action(2, EngineEvent.Kind.RESTART);
    assertTrue(f.saw("INPUT_ERROR", "Original inception"));
    var b = new InceptionBenchmark(100, 101, 1, 2, 0, 0, 2, 1, 0);
    recover.data =
        new RecoveredOrder(p, 10, 20, 1, 0, 0, 100, OrderType.LIMIT, TimeInForce.DAY, true, b);
    f.send(recover);
    assertEquals(b, parent(f).benchmark());
    var conflict = new InceptionBenchmark(100, 102, 1, 2, 0, 0, 2, 1, 0);
    recover.data =
        new RecoveredOrder(
            p, 10, 20, 1, 0, 0, 100, OrderType.LIMIT, TimeInForce.DAY, true, conflict);
    f.send(recover);
    assertTrue(f.saw("INPUT_ERROR", "Conflicting inception"));
    assertEquals(b, parent(f).benchmark());
  }

  @Test
  void schemaTwoPreservesLiquidityRoleAndBenchmarkRecoveryMetadata() {
    var b = new InceptionBenchmark(100, 101, 1, 2, 0, 0, 2, 1, 0);
    EngineEvent event = new EngineEvent().reset(EngineEvent.Kind.RECOVER_CHILD);
    event.data =
        new RecoveredOrder(
            spec("is", Side.BUY, Map.of()),
            10,
            20,
            1,
            5,
            2,
            100,
            OrderType.LIMIT,
            TimeInForce.DAY,
            true,
            b);
    event.liquidityRole = LiquidityRole.MAKER.ordinal();
    ByteBuffer buffer = ByteBuffer.allocate(4096);
    BinaryCodec.input(buffer, 1, event);
    buffer.flip();
    buffer.get();
    buffer.getLong();
    EngineEvent decoded = BinaryCodec.input(buffer);
    assertEquals(event.data, decoded.data);
    assertEquals(event.liquidityRole, decoded.liquidityRole);
    assertEquals(2, BinaryCodec.SCHEMA);
  }

  @Test
  void mixedStrategyJournalReplaysIncludingBenchmarkCorrectionsAndRestart() throws Exception {
    var seed = fixture();
    Path path = directory.resolve("mixed");
    try (var journal =
        new AsyncJournal(path, seed.plugins.manifest() + seed.config, 1024, 10000000, 1000000)) {
      var f = fixture(journal);
      create(f, "is", Map.of());
      ack(f);
      var child = children(f).getFirst();
      f.report(child.id(), 1, ReportType.FILL, 1, child.price(), "fill");
      f.report(child.id(), 1, ReportType.CORRECT, 1, child.price() + 1, "fill");
      f.report(child.id(), 1, ReportType.BUST, 0, 0, "fill");
      f.action(2, EngineEvent.Kind.PAUSE);
      cancelReports(f);
      f.action(2, EngineEvent.Kind.RESTART);
      var p = spec("passive", Side.SELL, Map.of());
      p =
          new ParentSpec(
              3, 1, 1, p.side(), 1000, p.limit(), 0, p.endNanos(), p.strategy(), p.parameters());
      f.send(EngineEvent.parent(p));
      time(f, 99);
      f.send(new EngineEvent().reset(EngineEvent.Kind.CHECKPOINT));
      time(f, 100);
    }
    var result = ReplayVerifier.verify(path, seed.config, seed.plugins);
    assertEquals(result, ReplayVerifier.verify(path, seed.config, seed.plugins));
    assertTrue(result.outputs() > 20);
  }

  @Test
  void rollingSpreadEstimatorNeedsWarmupAndDetectsNarrowing() {
    SpreadWindow w = new SpreadWindow();
    for (int i = 0; i < 32; i++) assertFalse(w.observe(i * 1000L, 1000, 10));
    assertTrue(w.observe(32000, 1000, 1));
    assertTrue(w.observe(32001, 1000, 1));
  }

  @Test
  void invalidParametersFailBeforeCallback() {
    var f = new IsFactory();
    assertThrows(
        IllegalArgumentException.class,
        () -> f.validate(spec("is", Side.BUY, Map.of("urgency", "NaN"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PassiveFactory()
                .validate(spec("passive", Side.BUY, Map.of("workingFraction", "0"))));
    assertThrows(
        IllegalArgumentException.class,
        () -> f.validate(spec("is", Side.BUY, Map.of("referenceDestinations", ""))));
  }

  @Test
  void cheapSpreadSignalSurvivesCancellationDrainWithoutFavourableBenchmarkPrice() {
    var f = fixture();
    quote(f, 1, 100, 110, 0);
    create(f, "is", Map.of("behindSeconds", "1000", "urgency", "0"));
    ack(f);
    for (int i = 1; i <= 32; i++) time(f, i);
    quote(f, 1, 100, 104, 32_000_000_000L);
    time(f, 33);
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream()
            .anyMatch(
                c ->
                    c.timeInForce() == TimeInForce.IOC && c.price() > parent(f).benchmark().mid()));
  }

  @Test
  void sellShortfallHasCorrectSignAndFavourableBidCanTriggerTaking() {
    var f = fixture();
    f.send(EngineEvent.parent(spec("is", Side.SELL, Map.of())));
    ack(f);
    var child = children(f).getFirst();
    f.report(child.id(), 1, ReportType.FILL, 1, 101, "sale");
    assertEquals(
        0, parent(f).executionCosts().venueShortfall().compareTo(new BigDecimal("-0.005")));
    quote(f, 1, 101, 102, 0);
    quote(f, 2, 101, 102, 0);
    time(f, 1);
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream()
            .anyMatch(c -> c.timeInForce() == TimeInForce.IOC && c.side() == Side.SELL));
  }

  @Test
  void passiveDustUsesMinimumChildSizeAndTerminalLayersRespectWorkingCapDuringCancel() {
    var f = fixture();
    f.policy(
        Limits.builder()
            .maxParent(100000)
            .childSizes(5, 10000)
            .maxWorking(10000)
            .quoteAgeNanos(300_000_000_000L)
            .build());
    create(f, "passive", Map.of());
    assertTrue(commands(f, CommandType.NEW).stream().allMatch(c -> c.quantity() >= 5));
    assertEquals(10, parent(f).reserved());
    ack(f);
    time(f, 90);
    assertTrue(parent(f).reserved() <= 100);
    var g = fixture();
    create(g, "passive", Map.of());
    ack(g);
    time(g, 90);
    assertTrue(parent(g).reserved() <= 100);
    assertFalse(commands(g, CommandType.CANCEL).isEmpty());
  }

  @Test
  void minimumRestingTimePreventsReplacementAndEffectivePolicyBoundsLimitChildren() {
    var f = fixture();
    create(f, "passive", Map.of("minimumRestingMillis", "5000", "layers", "1"));
    ack(f);
    quote(f, 1, 104, 105, 0);
    time(f, 1);
    assertTrue(commands(f, CommandType.REPLACE).isEmpty());
    time(f, 5);
    assertFalse(commands(f, CommandType.REPLACE).isEmpty());
    var g = fixture();
    g.policy(
        Limits.builder()
            .maxParent(100000)
            .childSizes(1, 3)
            .quoteAgeNanos(300_000_000_000L)
            .build());
    create(g, "passive", Map.of());
    assertTrue(commands(g, CommandType.NEW).stream().allMatch(c -> c.quantity() <= 3));
  }

  @Test
  void unknownRecoveredFillHistoryIsNotReportedAsZeroShortfall() {
    var f = fixture();
    f.send(new EngineEvent().reset(EngineEvent.Kind.RECOVER));
    var b = new InceptionBenchmark(100, 101, 1, 2, 0, 0, 2, 1, 0);
    EngineEvent r = new EngineEvent().reset(EngineEvent.Kind.RECOVER_CHILD);
    r.data =
        new RecoveredOrder(
            spec("is", Side.BUY, Map.of()),
            10,
            20,
            1,
            5,
            2,
            100,
            OrderType.LIMIT,
            TimeInForce.DAY,
            true,
            b);
    f.send(r);
    assertFalse(parent(f).executionCosts().historyComplete());
    assertNull(parent(f).executionCosts().venueShortfall());
    assertNull(parent(f).executionCosts().averagePrice());
    assertEquals(2, parent(f).venueFilled());
  }

  @Test
  void isUsesFokWhenIocIsUnavailable() {
    var f = fixture();
    EngineEvent d = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
    d.data =
        Destination.builder()
            .id(3)
            .name("FOK")
            .instrumentId(1)
            .postOnly(true)
            .supports(OrderType.LIMIT, TimeInForce.FOK)
            .supports(OrderType.LIMIT, TimeInForce.DAY)
            .build();
    f.send(d);
    EngineEvent session = new EngineEvent().reset(EngineEvent.Kind.SESSION);
    session.destinationId = 3;
    session.a = 1;
    f.send(session);
    quote(f, 3, 100, 101, 0);
    create(f, "is", Map.of("destination", "3", "behindSeconds", "0"));
    ack(f);
    time(f, 1);
    cancelReports(f);
    assertTrue(
        commands(f, CommandType.NEW).stream().anyMatch(c -> c.timeInForce() == TimeInForce.FOK));
  }

  @Test
  void borrowedChildViewsEnforceOwnershipAndTraverseTerminalChildren() {
    var f = fixture();
    StrategyContext[] captured = new StrategyContext[1];
    f.plugins.register(
        new StrategyFactory() {
          public String name() {
            return "inspect";
          }

          public Strategy create(ParentSpec p) {
            return new Strategy() {
              public void onStart(StrategyContext c) {
                captured[0] = c;
              }
            };
          }
        });
    f.send(EngineEvent.parent(spec("inspect", Side.BUY, Map.of())));
    f.submit(1, 1, 10, 101);
    assertThrows(IllegalArgumentException.class, () -> captured[0].child(1));
    f.submit(2, 1, 10, 101);
    assertEquals(10, captured[0].child(2).reserved());
    f.report(2, 1, ReportType.CANCELLED, 0, 0, "done");
    List<Views.Child> visited = new ArrayList<>();
    captured[0].visitChildren(visited::add);
    assertEquals(1, visited.size());
    assertTrue(visited.getFirst().terminal());
    assertTrue(captured[0].bounds(1).maximumQuantity() > 0);
  }

  @Test
  void benchmarkPriceComparisonIsExactEvenNearLongMaximum() {
    long bid = Long.MAX_VALUE - 4, ask = Long.MAX_VALUE - 1;
    var b = new InceptionBenchmark(bid, ask, 1, 1, 0, 0, 1, 1, 0);
    assertTrue(b.favourable(bid + 1, Side.BUY));
    assertFalse(b.favourable(bid + 2, Side.BUY));
    assertTrue(b.favourable(bid + 2, Side.SELL));
    assertFalse(b.favourable(bid + 1, Side.SELL));
  }
}
