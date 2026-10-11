package io.strategycontainer;

import static org.junit.jupiter.api.Assertions.*;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.bootstrap.*;
import io.strategycontainer.core.*;
import io.strategycontainer.journal.*;
import io.strategycontainer.replay.*;
import io.strategycontainer.risk.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class EngineCoreTest {
  @TempDir Path temporary;

  static final class IdleFactory implements StrategyFactory {
    public String name() {
      return "idle";
    }

    public Strategy create(ParentSpec p) {
      return new Strategy() {};
    }
  }

  static final class Fixture {
    final EngineConfig config =
        EngineConfig.builder().instruments(4).destinations(4).parents(8).children(256).build();
    final PluginRegistry plugins = new PluginRegistry().register(new IdleFactory());
    final List<Output> outputs = new ArrayList<>();
    final EngineCore core;
    long seq;

    Fixture() {
      this(Journal.none());
    }

    Fixture(Journal journal) {
      core = new EngineCore(config, plugins, journal, outputs::add, o -> {});
      setup();
    }

    void send(EngineEvent e) {
      core.process(++seq, e);
    }

    void setup() {
      EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.INSTRUMENT);
      e.data = Instrument.builder().id(1).symbol("TEST").priceScale(100).build();
      send(e);
      for (int d = 1; d <= 2; d++) {
        e = new EngineEvent().reset(EngineEvent.Kind.DESTINATION);
        e.data =
            Destination.builder()
                .id(d)
                .name("D" + d)
                .instrumentId(1)
                .supports(OrderType.LIMIT, TimeInForce.DAY)
                .supports(OrderType.LIMIT, TimeInForce.IOC)
                .postOnly(true)
                .internalBooking(true)
                .build();
        send(e);
        e = new EngineEvent().reset(EngineEvent.Kind.SESSION);
        e.destinationId = d;
        e.a = 1;
        send(e);
      }
      policy(
          Limits.builder()
              .maxParent(1000)
              .childSizes(1, 1000)
              .maxWorking(10000)
              .maxChildren(100)
              .collarTicks(100)
              .rejects(100, 100, 100, 10_000_000_000L)
              .build());
      for (int id = 1; id <= 2; id++) {
        EngineEvent reconciled = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
        reconciled.destinationId = id;
        send(reconciled);
      }
      send(new EngineEvent().reset(EngineEvent.Kind.READY));
      send(EngineEvent.quote(1, 1, 100, 101, 1000, 1000, 0));
      send(EngineEvent.quote(1, 2, 100, 101, 1000, 1000, 0));
      create(1, 100);
    }

    void create(long id, long quantity) {
      send(
          EngineEvent.parent(
              ParentSpec.builder()
                  .id(id)
                  .instrumentId(1)
                  .quantity(quantity)
                  .limit(120)
                  .window(0, 100_000_000_000L)
                  .strategy("idle")
                  .build()));
    }

    void policy(Limits limits) {
      EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.POLICY);
      e.data = Policy.engine(limits);
      send(e);
    }

    void submit(long parent, int dest, long quantity, long price) {
      EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.SUBMIT);
      e.parentId = parent;
      e.destinationId = dest;
      e.a = quantity;
      e.b = price;
      e.flags = TimeInForce.DAY.ordinal() << 8;
      send(e);
    }

    void action(long parent, EngineEvent.Kind kind) {
      EngineEvent e = new EngineEvent().reset(kind);
      e.parentId = parent;
      send(e);
    }

    void childAction(long parent, long child, EngineEvent.Kind kind) {
      EngineEvent e = new EngineEvent().reset(kind);
      e.parentId = parent;
      e.childId = child;
      send(e);
    }

    void report(long child, int dest, ReportType type, long quantity, long price, String id) {
      send(EngineEvent.report(child, dest, type, quantity, price, id));
    }

    void replace(long child, long quantity, long price) {
      EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.REPLACE);
      e.parentId = 1;
      e.childId = child;
      e.a = quantity;
      e.b = price;
      send(e);
    }

    EngineCore.ParentSnapshot parent() {
      return core.snapshot().parents().getFirst();
    }

    long lastChild() {
      return outputs.stream()
          .filter(o -> o.command() != null && o.command().type() == CommandType.NEW)
          .mapToLong(Output::childId)
          .max()
          .orElse(0);
    }

    boolean saw(String type, String detail) {
      return outputs.stream().anyMatch(o -> o.type().equals(type) && o.detail().contains(detail));
    }
  }

  @Test
  void layeredOrdersAcrossVenuesCannotOverfill() {
    Fixture f = new Fixture();
    f.submit(1, 1, 60, 101);
    f.submit(1, 2, 40, 101);
    f.submit(1, 2, 1, 101);
    assertEquals(100, f.parent().reserved());
    assertTrue(f.saw("CONSTRAINT", "EXPOSURE"));
    f.report(1, 1, ReportType.FILL, 60, 101, "A");
    f.report(2, 2, ReportType.FILL, 40, 101, "B");
    assertEquals("FILLED", f.parent().state());
    assertEquals(0, f.parent().reserved());
  }

  @Test
  void cancelFillRaceRetainsReservationUntilConfirmation() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.childAction(1, 1, EngineEvent.Kind.CANCEL_CHILD);
    assertEquals(100, f.parent().reserved());
    f.report(1, 1, ReportType.FILL, 30, 101, "A");
    assertEquals(70, f.parent().reserved());
    f.submit(1, 2, 1, 101);
    assertTrue(f.saw("CONSTRAINT", "EXPOSURE"));
    f.report(1, 1, ReportType.CANCELLED, 0, 0, "C");
    assertEquals(0, f.parent().reserved());
    assertEquals(30, f.parent().venueFilled());
  }

  @Test
  void duplicateFillsDoNotChangeQuantityOrVersion() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.report(1, 1, ReportType.FILL, 25, 101, "A");
    long version = f.parent().version();
    f.report(1, 1, ReportType.FILL, 25, 101, "A");
    assertEquals(25, f.parent().venueFilled());
    assertEquals(version, f.parent().version());
  }

  @Test
  void replaceFillRaceUsesMaximumOldNewExposure() {
    Fixture f = new Fixture();
    f.submit(1, 1, 80, 101);
    f.report(1, 1, ReportType.ACK, 0, 0, "ack");
    f.replace(1, 100, 102);
    assertEquals(100, f.parent().reserved());
    f.report(1, 1, ReportType.FILL, 20, 101, "A");
    assertEquals(80, f.parent().reserved());
    f.report(1, 1, ReportType.REPLACED, 0, 0, "R");
    assertEquals(80, f.parent().reserved());
    f.submit(1, 2, 1, 101);
    assertTrue(f.saw("CONSTRAINT", "EXPOSURE"));
  }

  @Test
  void replaceRejectionRestoresOriginalReservation() {
    Fixture f = new Fixture();
    f.submit(1, 1, 60, 101);
    f.report(1, 1, ReportType.ACK, 0, 0, "ack");
    f.replace(1, 90, 102);
    f.report(1, 1, ReportType.FILL, 10, 101, "A");
    f.report(1, 1, ReportType.REPLACE_REJECTED, 0, 0, "reject");
    assertEquals(50, f.parent().reserved());
  }

  @Test
  void uncertainStatusNeverReleasesExposure() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.report(1, 1, ReportType.UNCERTAIN, 0, 0, "timeout");
    assertEquals(100, f.parent().reserved());
    assertEquals("ERROR", f.parent().state());
    f.action(1, EngineEvent.Kind.RESUME);
    assertTrue(f.saw("INPUT_ERROR", "Reconciliation"));
    f.report(1, 1, ReportType.RECONCILED, 0, 0, "gone");
    assertEquals(0, f.parent().reserved());
    f.action(1, EngineEvent.Kind.RESUME);
    assertEquals("ACTIVE", f.parent().state());
  }

  @Test
  void priceLimitAndStaleQuoteBlockOutboundExposure() {
    Fixture f = new Fixture();
    f.submit(1, 1, 10, 121);
    assertTrue(f.saw("CONSTRAINT", "LIMIT"));
    f.send(EngineEvent.time(2_000_000_000L, 1000));
    f.submit(1, 1, 10, 101);
    assertTrue(f.saw("CONSTRAINT", "STALE"));
    assertEquals(0, f.parent().reserved());
  }

  @Test
  void childMinMaxAndParentMaximumEnforced() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().maxParent(100).childSizes(10, 20).maxWorking(100).build());
    f.submit(1, 1, 9, 101);
    f.submit(1, 1, 21, 101);
    assertEquals(0, f.parent().reserved());
    f.create(2, 101);
    assertTrue(f.saw("INPUT_ERROR", "Parent size"));
  }

  @Test
  void cancellationHasIndependentBudget() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().messages(1, 1).cancellations(10, 10).build());
    f.submit(1, 1, 10, 101);
    f.submit(1, 1, 10, 101);
    assertTrue(f.saw("CONSTRAINT", "THROTTLED"));
    f.childAction(1, 1, EngineEvent.Kind.CANCEL_CHILD);
    assertTrue(
        f.outputs.stream()
            .anyMatch(o -> o.command() != null && o.command().type() == CommandType.CANCEL));
  }

  @Test
  void scopedPolicyUsesIntersection() {
    Fixture f = new Fixture();
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.POLICY);
    e.data = new Policy(Scope.PARENT, 1, "", Limits.builder().childSizes(1, 5).build());
    f.send(e);
    f.submit(1, 1, 6, 101);
    assertTrue(f.saw("CONSTRAINT", "SIZE"));
  }

  @Test
  void venueRejectBreakerPausesAndCancels() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().rejects(2, 20, 100, 10_000_000_000L).build());
    f.submit(1, 1, 10, 101);
    f.report(1, 1, ReportType.REJECTED, 0, 0, "R1");
    f.submit(1, 1, 10, 101);
    f.report(2, 1, ReportType.REJECTED, 0, 0, "R2");
    assertEquals("PAUSED", f.parent().state());
  }

  @Test
  void liquidityRejectDoesNotTripTechnicalBreaker() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().rejects(1, 20, 100, 10_000_000_000L).build());
    f.submit(1, 1, 10, 101);
    EngineEvent e = EngineEvent.report(1, 1, ReportType.REJECTED, 0, 0, "last-look");
    e.c = RejectClass.LIQUIDITY.ordinal();
    f.send(e);
    assertEquals("ACTIVE", f.parent().state());
  }

  @Test
  void localFailureBreakerStopsRepeatedInvalidRequests() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().rejects(2, 20, 100, 10_000_000_000L).build());
    f.submit(1, 1, 0, 101);
    f.submit(1, 1, 0, 101);
    assertEquals("PAUSED", f.parent().state());
  }

  @Test
  void pauseAndDeadlineCancelWorkingOrders() {
    Fixture f = new Fixture();
    f.submit(1, 1, 10, 101);
    f.action(1, EngineEvent.Kind.PAUSE);
    assertEquals("PAUSED", f.parent().state());
    assertEquals(10, f.parent().reserved());
    f.report(1, 1, ReportType.CANCELLED, 0, 0, "C");
    f.action(1, EngineEvent.Kind.RESUME);
    f.send(EngineEvent.time(100_000_000_000L, 100_000));
    assertEquals("CANCELLED", f.parent().state());
  }

  @Test
  void correctedTradeReopensResidualWithoutReopeningDeadChild() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.report(1, 1, ReportType.FILL, 100, 101, "A");
    f.report(1, 1, ReportType.CORRECT, 90, 101, "A");
    assertEquals(90, f.parent().venueFilled());
    assertEquals(0, f.parent().reserved());
    assertEquals("PAUSED", f.parent().state());
    f.report(1, 1, ReportType.BUST, 0, 0, "A");
    assertEquals(0, f.parent().venueFilled());
  }

  @Test
  void authoritativeOverfillIsRecordedAndRaisesBreach() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.report(1, 1, ReportType.FILL, 101, 101, "A");
    assertEquals(101, f.parent().venueFilled());
    assertEquals("ERROR", f.parent().state());
    assertTrue(f.saw("BREACH", ""));
  }

  @Test
  void executionAboveLimitCannotBeSilentlyDiscarded() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    f.report(1, 1, ReportType.FILL, 100, 125, "A");
    assertEquals(100, f.parent().venueFilled());
    assertEquals("ERROR", f.parent().state());
    assertTrue(f.saw("BREACH", "price"));
  }

  @Test
  void finalDispatchGateBlocksOrderAfterPause() {
    Fixture f = new Fixture();
    f.submit(1, 1, 100, 101);
    long id =
        f.outputs.stream()
            .filter(o -> o.command() != null && o.command().type() == CommandType.NEW)
            .findFirst()
            .orElseThrow()
            .command()
            .id();
    f.action(1, EngineEvent.Kind.PAUSE);
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.DISPATCH);
    e.id = id;
    f.send(e);
    assertTrue(f.saw("ROUTE_SKIP", Long.toString(id)));
    assertEquals(0, f.parent().reserved());
  }

  @Test
  void amendmentsRequireVersionAndDrainExposure() {
    Fixture f = new Fixture();
    f.submit(1, 1, 80, 101);
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.AMEND);
    e.parentId = 1;
    e.a = f.parent().version();
    e.data =
        ParentSpec.builder()
            .id(1)
            .instrumentId(1)
            .quantity(50)
            .limit(120)
            .window(0, 100_000_000_000L)
            .strategy("idle")
            .build();
    f.send(e);
    assertEquals(100, f.parent().quantity());
    assertTrue(f.saw("AMEND_DEFERRED", ""));
  }

  @Test
  void operatorCommandsAreIdempotent() {
    Fixture f = new Fixture();
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.SUBMIT);
    e.parentId = 1;
    e.destinationId = 1;
    e.a = 10;
    e.b = 101;
    e.flags = TimeInForce.DAY.ordinal() << 8;
    e.requestKey = "operator:1";
    f.send(e);
    f.send(e);
    assertEquals(10, f.parent().reserved());
    assertTrue(f.saw("DUPLICATE_REQUEST", ""));
    e.a = 20;
    f.send(e);
    assertTrue(f.saw("IDEMPOTENCY_CONFLICT", ""));
    assertEquals(10, f.parent().reserved());
  }

  @Test
  void washFillRequiresResolvedExposureAndOmsConfirmation() {
    Fixture f = new Fixture();
    f.submit(1, 1, 95, 101);
    f.report(1, 1, ReportType.FILL, 95, 101, "A");
    f.action(1, EngineEvent.Kind.PAUSE);
    EngineEvent preview = new EngineEvent().reset(EngineEvent.Kind.PREVIEW);
    preview.parentId = 1;
    preview.destinationId = 1;
    preview.a = f.parent().version();
    preview.key = "WASH";
    preview.text = "untradeable residual";
    f.send(preview);
    long previewId =
        Long.parseLong(
            f.outputs.stream()
                .filter(o -> o.type().equals("BOOK_PREVIEW"))
                .reduce((a, b) -> b)
                .orElseThrow()
                .detail()
                .split(":")[0]);
    EngineEvent confirm = new EngineEvent().reset(EngineEvent.Kind.CONFIRM_BOOK);
    confirm.parentId = 1;
    confirm.id = previewId;
    confirm.key = "book-1";
    f.send(confirm);
    assertEquals("BOOKING", f.parent().state());
    assertEquals(5, f.parent().reserved());
    ExecutionCommand booking =
        f.outputs.stream()
            .filter(o -> o.command() != null && o.command().type() == CommandType.BOOK)
            .findFirst()
            .orElseThrow()
            .command();
    EngineEvent report = EngineEvent.report(0, 1, ReportType.BOOKED, 5, 101, "OMS-1");
    report.parentId = 1;
    report.id = booking.id();
    f.send(report);
    assertEquals("FILLED", f.parent().state());
    assertEquals(95, f.parent().venueFilled());
    assertEquals(5, f.parent().washFilled());
    assertEquals(0, f.parent().reserved());
  }

  @Test
  void staleBookingPreviewCannotBeConfirmed() {
    Fixture f = new Fixture();
    f.action(1, EngineEvent.Kind.PAUSE);
    EngineEvent preview = new EngineEvent().reset(EngineEvent.Kind.PREVIEW);
    preview.parentId = 1;
    preview.destinationId = 1;
    preview.a = f.parent().version();
    preview.key = "WASH";
    preview.text = "residual";
    f.send(preview);
    long previewId =
        Long.parseLong(
            f.outputs.stream()
                .filter(o -> o.type().equals("BOOK_PREVIEW"))
                .findFirst()
                .orElseThrow()
                .detail()
                .split(":")[0]);
    f.send(EngineEvent.time(2_000_000_000L, 2000));
    EngineEvent confirm = new EngineEvent().reset(EngineEvent.Kind.CONFIRM_BOOK);
    confirm.parentId = 1;
    confirm.id = previewId;
    confirm.key = "B";
    f.send(confirm);
    assertEquals(0, f.parent().washFilled());
    assertTrue(f.saw("INPUT_ERROR", "expired"));
  }

  @Test
  void journalReplayProducesIdenticalCommandsAndState() throws Exception {
    Fixture template = new Fixture();
    Path dir = temporary.resolve("recording");
    AsyncJournal journal =
        new AsyncJournal(dir, template.plugins.manifest() + template.config, 1024, 4096, 1_000_000);
    Fixture f = new Fixture(journal);
    f.submit(1, 1, 60, 101);
    f.report(1, 1, ReportType.FILL, 60, 101, "A");
    f.submit(1, 2, 40, 101);
    f.report(2, 2, ReportType.FILL, 40, 101, "B");
    EngineEvent checkpoint = new EngineEvent().reset(EngineEvent.Kind.CHECKPOINT);
    f.send(checkpoint);
    String hash = f.core.stateHash();
    journal.close();
    ReplayVerifier.Result result = ReplayVerifier.verify(dir, f.config, f.plugins);
    assertEquals(hash, result.stateHash());
    assertEquals(f.seq, result.inputs());
    assertEquals(result, ReplayVerifier.verify(dir, f.config, f.plugins));
  }

  @Test
  void truncatedAndCorruptJournalAreRejected() throws Exception {
    Fixture template = new Fixture();
    Path dir = temporary.resolve("bad");
    AsyncJournal journal =
        new AsyncJournal(
            dir, template.plugins.manifest() + template.config, 1024, 10_000_000, 1_000_000);
    Fixture f = new Fixture(journal);
    journal.close();
    Path file = dir.resolve("000000.sej");
    byte[] bytes = Files.readAllBytes(file);
    Files.write(file, Arrays.copyOf(bytes, bytes.length - 1));
    assertThrows(
        java.io.IOException.class,
        () ->
            JournalReader.visit(
                dir, template.plugins.manifest() + template.config, (t, seq, b) -> {}));
  }

  @Test
  void failedJournalBlocksNewExposureButAllowsCancellation() {
    class ToggleJournal implements Journal {
      boolean ok = true;

      public boolean input(long seq, EngineEvent e) {
        return ok;
      }

      public boolean output(Output o) {
        return ok;
      }

      public boolean healthy() {
        return ok;
      }

      public long writtenSequence() {
        return 0;
      }

      public long durableSequence() {
        return 0;
      }
    }
    ToggleJournal journal = new ToggleJournal();
    Fixture f = new Fixture(journal);
    f.submit(1, 1, 20, 101);
    journal.ok = false;
    f.submit(1, 1, 10, 101);
    assertTrue(f.saw("CONSTRAINT", "LIFECYCLE"));
    f.childAction(1, 1, EngineEvent.Kind.CANCEL_CHILD);
    ExecutionCommand cancel =
        f.outputs.stream()
            .filter(o -> o.command() != null && o.command().type() == CommandType.CANCEL)
            .findFirst()
            .orElseThrow()
            .command();
    EngineEvent gate = new EngineEvent().reset(EngineEvent.Kind.DISPATCH);
    gate.id = cancel.id();
    f.send(gate);
    assertTrue(
        f.outputs.stream()
            .anyMatch(
                o ->
                    o.type().equals("ROUTE")
                        && o.command() != null
                        && o.command().type() == CommandType.CANCEL));
    assertEquals(20, f.parent().reserved());
  }

  @Test
  void crashRecoveryAdoptsUnjournalledChildrenAndRequiresReconciliation() {
    Fixture f = new Fixture();
    f.send(new EngineEvent().reset(EngineEvent.Kind.RECOVER));
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.RECOVER_CHILD);
    e.data =
        new RecoveredOrder(
            ParentSpec.builder()
                .id(1)
                .instrumentId(1)
                .quantity(100)
                .limit(120)
                .window(0, 100_000_000_000L)
                .strategy("idle")
                .build(),
            5,
            50,
            1,
            80,
            20,
            101,
            OrderType.LIMIT,
            TimeInForce.DAY,
            false);
    f.send(e);
    assertEquals(20, f.parent().venueFilled());
    assertEquals(80, f.parent().reserved());
    f.send(new EngineEvent().reset(EngineEvent.Kind.READY));
    assertFalse(f.core.snapshot().ready());
    e = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
    e.destinationId = 1;
    f.send(e);
    e = new EngineEvent().reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
    e.destinationId = 2;
    f.send(e);
    f.send(new EngineEvent().reset(EngineEvent.Kind.READY));
    assertTrue(f.core.snapshot().ready());
    assertEquals("ERROR", f.parent().state());
  }

  @Test
  void explicitPrefixRecoveryDiscardsIncompleteFinalTransaction() throws Exception {
    Fixture template = new Fixture();
    Path directory = temporary.resolve("prefix");
    AsyncJournal journal =
        new AsyncJournal(
            directory, template.plugins.manifest() + template.config, 1024, 10_000_000, 1_000_000);
    Fixture f = new Fixture(journal);
    journal.close();
    Path file = directory.resolve("000000.sej");
    byte[] bytes = Files.readAllBytes(file);
    Files.write(file, Arrays.copyOf(bytes, bytes.length - 1));
    long prefix =
        JournalReader.completePrefix(directory, template.plugins.manifest() + template.config);
    assertEquals(f.seq - 1, prefix);
    assertEquals(
        prefix,
        ReplayVerifier.verifyPrefix(directory, template.config, template.plugins, prefix).inputs());
  }

  @Test
  void operatorVersionConflictDoesNotChangeState() {
    Fixture f = new Fixture();
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.PAUSE);
    e.parentId = 1;
    e.expectedVersion = 99;
    f.send(e);
    assertEquals("ACTIVE", f.parent().state());
    assertTrue(f.saw("INPUT_ERROR", "Stale parent version"));
  }

  @Test
  void sharedSubscriptionsReleaseOnlyAfterLastTerminalParentAndContextIsConfined()
      throws Exception {
    Fixture f = new Fixture();
    StrategyContext[] captured = new StrategyContext[1];
    int[] stops = new int[1];
    f.plugins.register(
        new StrategyFactory() {
          public String name() {
            return "subscribed";
          }

          public Strategy create(ParentSpec parent) {
            return new Strategy() {
              public void onStart(StrategyContext c) {
                captured[0] = c;
                c.subscribe(1);
              }

              public void onStop(StrategyContext c) {
                stops[0]++;
              }
            };
          }
        });
    for (long id = 2; id <= 3; id++)
      f.send(
          EngineEvent.parent(
              ParentSpec.builder()
                  .id(id)
                  .instrumentId(1)
                  .quantity(100)
                  .limit(120)
                  .window(0, 100_000_000_000L)
                  .strategy("subscribed")
                  .build()));
    assertEquals(
        1,
        f.outputs.stream()
            .filter(
                o ->
                    o.command() != null
                        && o.type().equals("COMMAND")
                        && o.command().type() == CommandType.SUBSCRIBE)
            .count());
    java.util.concurrent.atomic.AtomicReference<Throwable> failure =
        new java.util.concurrent.atomic.AtomicReference<>();
    Thread other =
        new Thread(
            () -> {
              try {
                captured[0].status("illegal");
              } catch (Throwable ex) {
                failure.set(ex);
              }
            });
    other.start();
    other.join();
    assertInstanceOf(IllegalStateException.class, failure.get());
    for (long id = 2; id <= 3; id++) {
      EngineEvent cancel = new EngineEvent().reset(EngineEvent.Kind.CANCEL_PARENT);
      cancel.parentId = id;
      f.send(cancel);
      assertEquals(
          id - 2,
          f.outputs.stream()
              .filter(
                  o ->
                      o.command() != null
                          && o.type().equals("COMMAND")
                          && o.command().type() == CommandType.UNSUBSCRIBE)
              .count());
    }
    assertEquals(2, stops[0]);
  }

  @Test
  void throttledPauseCancellationsRetryOnLogicalTimeWithoutReleasingExposure() {
    Fixture f = new Fixture();
    f.policy(Limits.builder().cancellations(1, 1).build());
    f.submit(1, 1, 10, 101);
    f.submit(1, 1, 10, 101);
    f.action(1, EngineEvent.Kind.PAUSE);
    assertEquals(20, f.parent().reserved());
    long before =
        f.outputs.stream()
            .filter(o -> o.type().equals("COMMAND") && o.command().type() == CommandType.CANCEL)
            .count();
    assertEquals(1, before);
    f.send(EngineEvent.time(1_000_000_000L, 1000));
    assertEquals(
        2,
        f.outputs.stream()
            .filter(o -> o.type().equals("COMMAND") && o.command().type() == CommandType.CANCEL)
            .count());
    assertEquals(20, f.parent().reserved());
  }
}
