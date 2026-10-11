package io.strategycontainer.api;

/**
 * Thread-safe connector ingress. False means bounded backpressure: retain the input and retry;
 * never silently drop it.
 */
public interface ConnectorContext {
  /** Offer a normalized integer-price quote with a logical observation timestamp. */
  boolean quote(
      int instrumentId,
      int destinationId,
      long bid,
      long ask,
      long bidSize,
      long askSize,
      long observedLogicalNanos);

  /** Offer a normalized financial report; preserve and retry on backpressure. */
  boolean report(ExecutionReport report);

  /** Change session availability; online does not imply reconciled or execution-ready. */
  boolean session(int destinationId, boolean online);

  /**
   * Publish completion only after enumerating open orders, fills, and internal bookings from the
   * OMS.
   */
  boolean reconciled(int destinationId);

  /** Adopt an OMS order missing from the durable prefix, including its client parent terms. */
  boolean recover(RecoveredOrder order);

  /** Published logical time for ingress timestamps; never a scheduling substitute. */
  long logicalNanos();
}
