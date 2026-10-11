package io.strategycontainer.core;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;

/** Reusable ring slot and borrowed input. Only producers may initialize unpublished slots. */
public final class EngineEvent {
  public enum Kind {
    TIME,
    QUOTE,
    INSTRUMENT,
    DESTINATION,
    POLICY,
    CREATE,
    AMEND,
    PAUSE,
    RESUME,
    CANCEL_PARENT,
    RESTART,
    SUBMIT,
    REPLACE,
    CANCEL_CHILD,
    QUERY,
    REPORT,
    SESSION,
    READY,
    PREVIEW,
    CONFIRM_BOOK,
    CHECKPOINT,
    DISPATCH,
    RECOVER,
    OVERRUN,
    RECONCILIATION_COMPLETE,
    RECOVER_CHILD,
    RESET_POLICY
  }

  public Kind kind;
  public long parentId, childId, id, a, b, c, d, e, f;
  public int instrumentId, destinationId, flags;
  public int liquidityRole;
  public String text = "", key = "", actor = "system", requestKey = "";
  public Object data;
  public long ingressNanos;
  public long expectedVersion = -1;

  public EngineEvent reset(Kind k) {
    kind = k;
    parentId = childId = id = a = b = c = d = e = f = 0;
    instrumentId = destinationId = flags = liquidityRole = 0;
    text = key = "";
    actor = "system";
    requestKey = "";
    data = null;
    ingressNanos = 0;
    expectedVersion = -1;
    return this;
  }

  public EngineEvent copyFrom(EngineEvent v) {
    kind = v.kind;
    parentId = v.parentId;
    childId = v.childId;
    id = v.id;
    a = v.a;
    b = v.b;
    c = v.c;
    d = v.d;
    e = v.e;
    f = v.f;
    instrumentId = v.instrumentId;
    destinationId = v.destinationId;
    flags = v.flags;
    liquidityRole = v.liquidityRole;
    text = v.text;
    key = v.key;
    actor = v.actor;
    requestKey = v.requestKey;
    data = v.data;
    ingressNanos = v.ingressNanos;
    expectedVersion = v.expectedVersion;
    return this;
  }

  public static EngineEvent time(long nanos, long utcMillis) {
    EngineEvent e = new EngineEvent().reset(Kind.TIME);
    e.a = nanos;
    e.b = utcMillis;
    return e;
  }

  public static EngineEvent quote(
      int instrument,
      int destination,
      long bid,
      long ask,
      long bidSize,
      long askSize,
      long observed) {
    EngineEvent e = new EngineEvent().reset(Kind.QUOTE);
    e.instrumentId = instrument;
    e.destinationId = destination;
    e.a = bid;
    e.b = ask;
    e.c = bidSize;
    e.d = askSize;
    e.e = observed;
    return e;
  }

  public static EngineEvent parent(ParentSpec spec) {
    EngineEvent e = new EngineEvent().reset(Kind.CREATE);
    e.parentId = spec.id();
    e.data = spec;
    return e;
  }

  public static EngineEvent report(
      long child, int destination, ReportType type, long quantity, long price, String executionId) {
    EngineEvent e = new EngineEvent().reset(Kind.REPORT);
    e.childId = child;
    e.destinationId = destination;
    e.flags = type.ordinal();
    e.a = quantity;
    e.b = price;
    e.text = executionId;
    return e;
  }
}
