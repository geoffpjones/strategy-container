package io.strategycontainer.connectors;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.core.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Bounded connector worker. Every queued intent returns to the core for a final gate before
 * external I/O.
 */
public final class ConnectorDispatcher implements OutputSink, AutoCloseable {
  private final Map<Integer, Connector> connectors = new HashMap<>();
  private final ArrayBlockingQueue<ExecutionCommand> normal, priority;
  private final ConcurrentHashMap<Long, CompletableFuture<ExecutionCommand>> authorizations =
      new ConcurrentHashMap<>();
  private final Thread worker;
  private volatile boolean running = true;
  private volatile InputSink input;

  public ConnectorDispatcher(int capacity) {
    normal = new ArrayBlockingQueue<>(capacity);
    priority = new ArrayBlockingQueue<>(capacity);
    worker = new Thread(this::loop, "connector-dispatch");
  }

  public void register(int destination, Connector connector) {
    if (worker.isAlive()) throw new IllegalStateException("Register before startup");
    if (connectors.putIfAbsent(destination, connector) != null)
      throw new IllegalArgumentException("Duplicate connector");
  }

  public void start(InputSink input) {
    this.input = input;
    worker.start();
  }

  public void accept(Output output) {
    if (output.type().equals("ROUTE") || output.type().equals("ROUTE_SKIP")) {
      CompletableFuture<ExecutionCommand> f =
          authorizations.remove(Long.parseLong(output.detail()));
      if (f != null) f.complete(output.command());
      return;
    }
    ExecutionCommand c = output.command();
    if (c == null) return;
    boolean urgent = c.type() == CommandType.CANCEL || c.type() == CommandType.STATUS;
    if (!(urgent ? priority : normal).offer(c))
      throw new IllegalStateException(
          "Connector dispatch capacity exhausted; reservation retained");
  }

  private void loop() {
    while (running || !normal.isEmpty() || !priority.isEmpty()) {
      try {
        ExecutionCommand c = priority.poll();
        if (c == null) c = normal.poll(10, TimeUnit.MILLISECONDS);
        if (c == null) continue;
        CompletableFuture<ExecutionCommand> approval = new CompletableFuture<>();
        authorizations.put(c.id(), approval);
        EngineEvent gate = new EngineEvent().reset(EngineEvent.Kind.DISPATCH);
        gate.id = c.id();
        publish(gate);
        ExecutionCommand routed;
        try {
          routed = approval.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException ex) {
          authorizations.remove(c.id());
          uncertain(c, "dispatch gate timeout");
          continue;
        }
        if (routed == null) continue;
        Connector connector = connectors.get(c.destinationId());
        try {
          if (connector == null) throw new IllegalStateException("Missing connector");
          if (routed.type() == CommandType.SUBSCRIBE) connector.subscribe(routed.instrumentId());
          else if (routed.type() == CommandType.UNSUBSCRIBE)
            connector.unsubscribe(routed.instrumentId());
          else connector.send(routed);
        } catch (Exception ex) {
          uncertain(c, ex.getClass().getSimpleName());
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void uncertain(ExecutionCommand c, String reason) {
    EngineEvent e =
        EngineEvent.report(c.childId(), c.destinationId(), ReportType.UNCERTAIN, 0, 0, reason);
    e.parentId = c.parentId();
    e.id = c.id();
    publish(e);
  }

  private void publish(EngineEvent event) {
    while (!input.offer(event)) {
      if (!running) throw new IllegalStateException("Ingress unavailable during shutdown");
      java.util.concurrent.locks.LockSupport.parkNanos(100_000);
    }
  }

  public void close() {
    running = false;
    try {
      worker.join(5000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    if (worker.isAlive()) {
      worker.interrupt();
      throw new IllegalStateException("Connector did not stop");
    }
    for (Connector c : new HashSet<>(connectors.values())) c.close();
  }
}
