package io.strategycontainer.core;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Live bounded ingress. Producers never retain ring slots; only the consumer mutates core state.
 */
public final class EngineRuntime implements InputSink, AutoCloseable {
  private final long sequenceOffset;
  private final EngineCore core;
  private final Disruptor<EngineEvent> disruptor;
  private final RingBuffer<EngineEvent> ring;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final ConcurrentLinkedQueue<CompletableFuture<EngineCore.Snapshot>> snapshots =
      new ConcurrentLinkedQueue<>();
  private final AtomicInteger snapshotCount = new AtomicInteger();
  private final Thread clock;
  private volatile long completed = -1;
  private volatile Throwable failure;
  private volatile long coreThreadId;

  public EngineRuntime(EngineConfig config, EngineCore core, boolean liveClock) {
    this.core = core;
    sequenceOffset = core.sequence();
    core.transferOwnership();
    WaitStrategy wait = config.busySpin() ? new BusySpinWaitStrategy() : new YieldingWaitStrategy();
    disruptor =
        new Disruptor<>(
            EngineEvent::new,
            config.ringSize(),
            r -> new Thread(r, "engine-core"),
            ProducerType.MULTI,
            wait);
    disruptor.setDefaultExceptionHandler(
        new ExceptionHandler<>() {
          public void handleEventException(Throwable ex, long seq, EngineEvent event) {
            failure = ex;
            running.set(false);
          }

          public void handleOnStartException(Throwable ex) {
            failure = ex;
            running.set(false);
          }

          public void handleOnShutdownException(Throwable ex) {
            failure = ex;
          }
        });
    disruptor.handleEventsWith(
        (event, sequence, endOfBatch) -> {
          if (coreThreadId == 0) coreThreadId = Thread.currentThread().threadId();
          core.process(sequenceOffset + sequence + 1, event);
          if (event.kind == EngineEvent.Kind.QUOTE && event.ingressNanos != 0)
            core.metrics.latency(System.nanoTime() - event.ingressNanos);
          completed = sequence;
          if (core.metrics.slowParent != 0) {
            EngineEvent overrun = new EngineEvent().reset(EngineEvent.Kind.OVERRUN);
            overrun.parentId = core.metrics.slowParent;
            core.metrics.slowParent = 0;
            offer(overrun);
          }
          if (snapshotCount.get() > 0) {
            CompletableFuture<EngineCore.Snapshot> f;
            EngineCore.Snapshot s = core.snapshot();
            while ((f = snapshots.poll()) != null) {
              snapshotCount.decrementAndGet();
              f.complete(s);
            }
          }
          event.data = null;
        });
    ring = disruptor.start();
    if (liveClock) {
      clock = new Thread(this::clockLoop, "engine-clock");
      clock.setDaemon(true);
      clock.start();
    } else clock = null;
  }

  /** Allocation-free ingress for normalized price updates. Caller retries on backpressure. */
  public boolean quote(
      int instrument,
      int destination,
      long bid,
      long ask,
      long bidSize,
      long askSize,
      long observed) {
    if (!running.get()) return false;
    long seq;
    try {
      seq = ring.tryNext();
    } catch (InsufficientCapacityException ex) {
      core.metrics.backpressure.increment();
      return false;
    }
    try {
      EngineEvent e = ring.get(seq).reset(EngineEvent.Kind.QUOTE);
      e.instrumentId = instrument;
      e.destinationId = destination;
      e.a = bid;
      e.b = ask;
      e.c = bidSize;
      e.d = askSize;
      e.e = observed;
      e.ingressNanos = System.nanoTime();
    } finally {
      ring.publish(seq);
    }
    core.metrics.offered.increment();
    return true;
  }

  public boolean offer(EngineEvent event) {
    if (!running.get()) return false;
    long sequence;
    try {
      sequence = ring.tryNext();
    } catch (InsufficientCapacityException ex) {
      core.metrics.backpressure.increment();
      return false;
    }
    try {
      ring.get(sequence).copyFrom(event).ingressNanos = System.nanoTime();
    } finally {
      ring.publish(sequence);
    }
    core.metrics.offered.increment();
    return true;
  }

  /**
   * Cold-path management publication; timeout never means the command was cancelled or rolled back.
   */
  public void publish(EngineEvent event, long timeoutMillis) {
    long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (!offer(event)) {
      if (!running.get()) throw new IllegalStateException("Engine stopped", failure);
      if (System.nanoTime() > until) throw new IllegalStateException("Ingress capacity exhausted");
      LockSupport.parkNanos(100_000);
    }
  }

  /** Obtain a serialized snapshot; bounded to 128 concurrent management requests. */
  public CompletableFuture<EngineCore.Snapshot> snapshot() {
    if (snapshotCount.incrementAndGet() > 128) {
      snapshotCount.decrementAndGet();
      return CompletableFuture.failedFuture(new IllegalStateException("Management capacity"));
    }
    CompletableFuture<EngineCore.Snapshot> f = new CompletableFuture<>();
    snapshots.add(f);
    try {
      publish(new EngineEvent().reset(EngineEvent.Kind.CHECKPOINT), 1000);
    } catch (RuntimeException ex) {
      if (snapshots.remove(f)) snapshotCount.decrementAndGet();
      f.completeExceptionally(ex);
    }
    return f;
  }

  private void clockLoop() {
    long base = core.metrics.logicalNanos, start = System.nanoTime();
    EngineEvent event = new EngineEvent();
    while (running.get()) {
      event.reset(EngineEvent.Kind.TIME);
      event.a = base + System.nanoTime() - start;
      event.b = System.currentTimeMillis();
      if (!offer(event)) {
        LockSupport.parkNanos(100_000);
        continue;
      }
      LockSupport.parkNanos(1_000_000);
    }
  }

  /** Wait for currently published inputs, for tests/shutdown only. */
  public void awaitIdle(long timeoutMillis) {
    long target = ring.getCursor(),
        until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (completed < target) {
      if (failure != null) throw new IllegalStateException("Core failed", failure);
      if (System.nanoTime() > until) throw new IllegalStateException("Drain timeout");
      LockSupport.parkNanos(100_000);
    }
  }

  /** Core thread identity for external allocation and CPU diagnostics. */
  public long coreThreadId() {
    return coreThreadId;
  }

  public EngineMetrics metrics() {
    return core.metrics;
  }

  public long remainingCapacity() {
    return ring.remainingCapacity();
  }

  public void close() {
    running.set(false);
    if (clock != null) {
      clock.interrupt();
      try {
        clock.join(2000);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    try {
      disruptor.shutdown(5, TimeUnit.SECONDS);
    } catch (com.lmax.disruptor.TimeoutException ex) {
      disruptor.halt();
      throw new IllegalStateException("Core drain timeout", ex);
    }
    CompletableFuture<EngineCore.Snapshot> f;
    while ((f = snapshots.poll()) != null)
      f.completeExceptionally(new IllegalStateException("Engine closed"));
  }
}
