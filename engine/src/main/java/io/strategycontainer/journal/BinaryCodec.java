package io.strategycontainer.journal;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.core.*;
import io.strategycontainer.risk.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Explicit schema-2 codec. Price-event encoding uses only caller-owned buffers and no temporary
 * objects.
 */
public final class BinaryCodec {
  public static final int SCHEMA = 2;

  private BinaryCodec() {}

  private static void string(ByteBuffer b, String s) {
    b.putInt(s.length());
    for (int i = 0; i < s.length(); i++) b.putChar(s.charAt(i));
  }

  private static String string(ByteBuffer b) {
    int n = b.getInt();
    if (n < 0 || n > b.remaining() / 2) throw new IllegalArgumentException("Invalid string length");
    char[] c = new char[n];
    for (int i = 0; i < n; i++) c[i] = b.getChar();
    return new String(c);
  }

  private static void instrument(ByteBuffer b, Instrument i) {
    b.putInt(i.id());
    string(b, i.symbol());
    string(b, i.quantityUnit());
    b.putLong(i.priceScale())
        .putLong(i.tick())
        .putLong(i.lot())
        .putLong(i.minimumQuantity())
        .putLong(i.multiplier());
  }

  private static Instrument instrument(ByteBuffer b) {
    return new Instrument(
        b.getInt(),
        string(b),
        string(b),
        b.getLong(),
        b.getLong(),
        b.getLong(),
        b.getLong(),
        b.getLong());
  }

  private static void destination(ByteBuffer b, Destination d) {
    b.putInt(d.id());
    string(b, d.name());
    b.putInt(d.instrumentId())
        .putLong(d.minimumQuantity())
        .putLong(d.maximumQuantity())
        .putInt(d.combinations());
    b.put((byte) (d.postOnly() ? 1 : 0))
        .put((byte) (d.protectedPeg() ? 1 : 0))
        .put((byte) (d.atomicReplace() ? 1 : 0))
        .put((byte) (d.internalBooking() ? 1 : 0))
        .put((byte) (d.firm() ? 1 : 0))
        .putInt(d.replaceMessageCost());
  }

  private static Destination destination(ByteBuffer b) {
    return new Destination(
        b.getInt(),
        string(b),
        b.getInt(),
        b.getLong(),
        b.getLong(),
        b.getInt(),
        b.get() != 0,
        b.get() != 0,
        b.get() != 0,
        b.get() != 0,
        b.get() != 0,
        b.getInt());
  }

  private static void parent(ByteBuffer b, ParentSpec p) {
    b.putLong(p.id())
        .putInt(p.accountId())
        .putInt(p.instrumentId())
        .putInt(p.side().ordinal())
        .putLong(p.quantity())
        .putLong(p.limit())
        .putLong(p.startNanos())
        .putLong(p.endNanos());
    string(b, p.strategy());
    TreeMap<String, String> params = new TreeMap<>(p.parameters());
    b.putInt(params.size());
    params.forEach(
        (k, v) -> {
          string(b, k);
          string(b, v);
        });
  }

  private static ParentSpec parent(ByteBuffer b) {
    long id = b.getLong();
    int account = b.getInt(), instrument = b.getInt();
    Side side = Side.values()[b.getInt()];
    long quantity = b.getLong(), limit = b.getLong(), start = b.getLong(), end = b.getLong();
    String strategy = string(b);
    int n = b.getInt();
    if (n < 0 || n > 1024) throw new IllegalArgumentException("Invalid parameters");
    Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < n; i++) map.put(string(b), string(b));
    return new ParentSpec(
        id, account, instrument, side, quantity, limit, start, end, strategy, map);
  }

  private static void policy(ByteBuffer b, Policy p) {
    b.putInt(p.scope().ordinal()).putLong(p.key());
    string(b, p.strategyName());
    Limits l = p.limits();
    b.putLong(l.maxParent())
        .putLong(l.minChild())
        .putLong(l.maxChild())
        .putLong(l.maxWorking())
        .putInt(l.maxChildren())
        .putLong(l.maxNotional())
        .putLong(l.quoteAgeNanos())
        .putLong(l.collarTicks())
        .putLong(l.minimumReplaceNanos())
        .putInt(l.messagesPerSecond())
        .putInt(l.burst())
        .putInt(l.cancelPerSecond())
        .putInt(l.cancelBurst())
        .putInt(l.consecutiveRejects())
        .putInt(l.minimumRejectSamples())
        .putInt(l.rejectPercent())
        .putLong(l.rejectWindowNanos());
  }

  private static Policy policy(ByteBuffer b) {
    Scope s = Scope.values()[b.getInt()];
    long k = b.getLong();
    String n = string(b);
    return new Policy(
        s,
        k,
        n,
        new Limits(
            b.getLong(),
            b.getLong(),
            b.getLong(),
            b.getLong(),
            b.getInt(),
            b.getLong(),
            b.getLong(),
            b.getLong(),
            b.getLong(),
            b.getInt(),
            b.getInt(),
            b.getInt(),
            b.getInt(),
            b.getInt(),
            b.getInt(),
            b.getInt(),
            b.getLong()));
  }

  /** Encode one normalized input. Caller clears the buffer and owns it throughout the call. */
  public static void input(ByteBuffer b, long sequence, EngineEvent e) {
    b.put((byte) 1)
        .putLong(sequence)
        .putInt(e.kind.ordinal())
        .putLong(e.parentId)
        .putLong(e.childId)
        .putLong(e.id)
        .putLong(e.a)
        .putLong(e.b)
        .putLong(e.c)
        .putLong(e.d)
        .putLong(e.e)
        .putLong(e.f)
        .putInt(e.instrumentId)
        .putInt(e.destinationId)
        .putInt(e.flags)
        .putInt(e.liquidityRole);
    string(b, e.text);
    string(b, e.key);
    string(b, e.actor);
    string(b, e.requestKey);
    b.putLong(e.expectedVersion);
    switch (e.kind) {
      case INSTRUMENT -> instrument(b, (Instrument) e.data);
      case DESTINATION -> destination(b, (Destination) e.data);
      case POLICY -> policy(b, (Policy) e.data);
      case CREATE, AMEND -> parent(b, (ParentSpec) e.data);
      case RECOVER_CHILD -> {
        RecoveredOrder r = (RecoveredOrder) e.data;
        parent(b, r.parent());
        b.putLong(r.childId())
            .putLong(r.commandId())
            .putInt(r.destinationId())
            .putLong(r.remainingQuantity())
            .putLong(r.cumulativeFilled())
            .putLong(r.price())
            .putInt(r.type().ordinal())
            .putInt(r.tif().ordinal())
            .put((byte) (r.postOnly() ? 1 : 0));
        benchmark(b, r.benchmark());
      }
      default -> {}
    }
  }

  public static EngineEvent input(ByteBuffer b) {
    EngineEvent e = new EngineEvent().reset(EngineEvent.Kind.values()[b.getInt()]);
    e.parentId = b.getLong();
    e.childId = b.getLong();
    e.id = b.getLong();
    e.a = b.getLong();
    e.b = b.getLong();
    e.c = b.getLong();
    e.d = b.getLong();
    e.e = b.getLong();
    e.f = b.getLong();
    e.instrumentId = b.getInt();
    e.destinationId = b.getInt();
    e.flags = b.getInt();
    e.liquidityRole = b.getInt();
    e.text = string(b);
    e.key = string(b);
    e.actor = string(b);
    e.requestKey = string(b);
    e.expectedVersion = b.getLong();
    e.data =
        switch (e.kind) {
          case INSTRUMENT -> instrument(b);
          case DESTINATION -> destination(b);
          case POLICY -> policy(b);
          case CREATE, AMEND -> parent(b);
          case RECOVER_CHILD ->
              new RecoveredOrder(
                  parent(b),
                  b.getLong(),
                  b.getLong(),
                  b.getInt(),
                  b.getLong(),
                  b.getLong(),
                  b.getLong(),
                  OrderType.values()[b.getInt()],
                  TimeInForce.values()[b.getInt()],
                  b.get() != 0,
                  benchmark(b));
          default -> null;
        };
    return e;
  }

  private static void benchmark(ByteBuffer b, InceptionBenchmark v) {
    b.put((byte) (v == null ? 0 : 1));
    if (v == null) return;
    b.putLong(v.bid())
        .putLong(v.ask())
        .putInt(v.bidDestination())
        .putInt(v.askDestination())
        .putLong(v.bidObservedNanos())
        .putLong(v.askObservedNanos())
        .putInt(v.constituents())
        .putLong(v.captureSequence())
        .putLong(v.captureNanos());
  }

  private static InceptionBenchmark benchmark(ByteBuffer b) {
    return b.get() == 0
        ? null
        : new InceptionBenchmark(
            b.getLong(),
            b.getLong(),
            b.getInt(),
            b.getInt(),
            b.getLong(),
            b.getLong(),
            b.getInt(),
            b.getLong(),
            b.getLong());
  }

  private static void command(ByteBuffer b, ExecutionCommand c) {
    b.putLong(c.id())
        .putLong(c.parentId())
        .putLong(c.childId())
        .putInt(c.destinationId())
        .putInt(c.instrumentId())
        .putInt(c.side().ordinal())
        .putInt(c.type().ordinal())
        .putLong(c.quantity())
        .putLong(c.price())
        .putInt(c.orderType().ordinal())
        .putInt(c.timeInForce().ordinal())
        .put((byte) (c.postOnly() ? 1 : 0));
    string(b, c.reference());
    string(b, c.reason());
  }

  private static ExecutionCommand command(ByteBuffer b) {
    return new ExecutionCommand(
        b.getLong(),
        b.getLong(),
        b.getLong(),
        b.getInt(),
        b.getInt(),
        Side.values()[b.getInt()],
        CommandType.values()[b.getInt()],
        b.getLong(),
        b.getLong(),
        OrderType.values()[b.getInt()],
        TimeInForce.values()[b.getInt()],
        b.get() != 0,
        string(b),
        string(b));
  }

  public static void output(ByteBuffer b, Output o) {
    b.put((byte) 2).putLong(o.sequence());
    string(b, o.type());
    b.putLong(o.parentId()).putLong(o.childId());
    string(b, o.detail());
    b.put((byte) (o.command() == null ? 0 : 1));
    if (o.command() != null) command(b, o.command());
  }

  public static Output output(ByteBuffer b, long sequence) {
    String t = string(b);
    long p = b.getLong(), c = b.getLong();
    String d = string(b);
    return new Output(sequence, t, p, c, d, b.get() == 0 ? null : command(b));
  }
}
