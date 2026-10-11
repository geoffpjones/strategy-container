package io.strategycontainer.api;

/** Frozen exact midpoint represented by its two integer-price legs and capture provenance. */
public record InceptionBenchmark(
    long bid,
    long ask,
    int bidDestination,
    int askDestination,
    long bidObservedNanos,
    long askObservedNanos,
    int constituents,
    long captureSequence,
    long captureNanos) {
  public InceptionBenchmark {
    if (bid <= 0
        || ask <= bid
        || bidDestination <= 0
        || askDestination <= 0
        || bidObservedNanos < 0
        || askObservedNanos < 0
        || constituents < 1
        || captureSequence < 0
        || captureNanos < bidObservedNanos
        || captureNanos < askObservedNanos)
      throw new IllegalArgumentException("Invalid inception benchmark");
  }

  /** Compare integer execution prices against the exact half-tick midpoint without overflow. */
  public boolean favourable(long price, Types.Side side) {
    long distance = ask - bid, floor = bid + distance / 2;
    return side == Types.Side.BUY ? price <= floor : price >= floor + (distance % 2);
  }

  /** Floating point midpoint for display; reporting retains exact half ticks. */
  public double mid() {
    return bid * .5 + ask * .5;
  }
}
