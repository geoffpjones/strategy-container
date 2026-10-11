package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;

/**
 * Capability API restricted to its parent. Commands are asynchronous and reserve quantity before
 * routing.
 */
public interface StrategyContext {
  record Result(Outcome outcome, long commandId, long childId) {
    public boolean accepted() {
      return outcome == Outcome.ACCEPTED;
    }
  }

  /** Borrowed parent view; valid only on the core thread. */
  Views.Parent parent();

  /** Current logical monotonic time in nanoseconds, supplied by ordered clock inputs. */
  long nowNanos();

  /** Immutable instrument definition for this parent. */
  Instrument instrument();

  /** Configured immutable destination, or null for an unknown ID. */
  Destination destination(int id);

  /** Borrowed latest quote, or null if unavailable; check validity and freshness. */
  Views.Quote quote(int destinationId);

  /** Borrowed owned child; null for unknown ID, throws for another parent's child. */
  default Views.Child child(long childId) {
    throw new UnsupportedOperationException("Child views unavailable");
  }

  /** Visit owned children on the core thread; includes terminal children. */
  default void visitChildren(Views.ChildVisitor visitor) {
    throw new UnsupportedOperationException("Child views unavailable");
  }

  /** Effective instrument/venue/policy bounds; refresh on each use. */
  default Views.OrderBounds bounds(int destinationId) {
    throw new UnsupportedOperationException("Bounds unavailable");
  }

  /** Borrowed current reference; null when no composite was configured. */
  default Views.ReferenceMarket referenceMarket() {
    return null;
  }

  /** Frozen inception benchmark, or null while pending. */
  default InceptionBenchmark benchmark() {
    return null;
  }

  /** Subscribe this parent; shared connector subscriptions are reference counted. */
  void subscribe(int destinationId);

  /** Release this parent's subscription; repeated release is harmless. */
  void unsubscribe(int destinationId);

  /** Request a child using integer quantity and scaled price; acceptance reserves exposure. */
  Result submit(
      int destinationId,
      long quantity,
      long price,
      OrderType type,
      TimeInForce tif,
      boolean postOnly);

  /**
   * Atomically replace owned child leaves and price; retains maximum potentially fillable exposure.
   */
  Result replace(long childId, long remainingQuantity, long price);

  /** Request cancellation of an owned child; reservation remains until a terminal report. */
  Result cancel(long childId);

  /** Request OMS status of an owned child without releasing its reservation. */
  Result query(long childId);

  /** Register or replace a parent-local timer at a future logical deadline. */
  void schedule(long timerId, long deadlineNanos);

  /** Remove the parent-local timer if present. */
  void cancelTimer(long timerId);

  /** Pause this parent and request cancellation of its working children. */
  void suspend(String reason);

  /** Publish an operational status and update the parent diagnostic. */
  void status(String message);
}
