package io.strategycontainer.connectors;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.core.*;
import java.util.*;

/**
 * Deterministic synthetic venue for demos/tests. IOC/FOK fill at their limit; passive orders rest
 * until cancelled.
 */
public final class SimulatedConnector implements Connector {
  private final InputSink input;
  private final Map<Long, ExecutionCommand> working = new HashMap<>();
  private final Set<Long> commands = new HashSet<>();

  public SimulatedConnector(InputSink input) {
    this.input = input;
  }

  public String name() {
    return "deterministic-simulator-v1";
  }

  public void send(ExecutionCommand c) {
    if (!commands.add(c.id())) return;
    switch (c.type()) {
      case NEW -> {
        working.put(c.childId(), c);
        emit(c, ReportType.ACK, 0, 0, "ack-" + c.id());
        if (c.timeInForce() == TimeInForce.IOC || c.timeInForce() == TimeInForce.FOK) {
          working.remove(c.childId());
          emit(c, ReportType.FILL, c.quantity(), c.price(), "fill-" + c.id());
        }
      }
      case REPLACE -> {
        working.put(c.childId(), c);
        emit(c, ReportType.REPLACED, 0, 0, "replace-" + c.id());
      }
      case CANCEL -> {
        working.remove(c.childId());
        emit(c, ReportType.CANCELLED, 0, 0, "cancel-" + c.id());
      }
      case STATUS -> {
        ExecutionCommand active = working.get(c.childId());
        emit(
            c,
            ReportType.RECONCILED,
            active == null ? 0 : active.quantity(),
            0,
            "status-" + c.id());
      }
      case BOOK -> emit(c, ReportType.BOOKED, c.quantity(), c.price(), "internal-" + c.id());
      case SUBSCRIBE, UNSUBSCRIBE -> {}
    }
  }

  private void emit(ExecutionCommand c, ReportType type, long q, long price, String ref) {
    EngineEvent e = EngineEvent.report(c.childId(), c.destinationId(), type, q, price, ref);
    e.parentId = c.parentId();
    e.id = c.id();
    e.liquidityRole =
        type == ReportType.FILL
            ? (c.postOnly() ? LiquidityRole.MAKER : LiquidityRole.TAKER).ordinal()
            : LiquidityRole.UNKNOWN.ordinal();
    while (!input.offer(e)) {
      if (Thread.currentThread().isInterrupted())
        throw new IllegalStateException("Interrupted simulator");
      java.util.concurrent.locks.LockSupport.parkNanos(100_000);
    }
  }
}
