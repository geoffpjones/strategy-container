package io.strategycontainer.orders;

import io.strategycontainer.api.Views;

/** Core-owned mutable quote; consumers see only the borrowed read-only interface. */
public final class QuoteState implements Views.Quote {
  public int instrument, destination;
  public long bid, ask, bidSize, askSize, observed;
  public boolean valid;

  public int instrumentId() {
    return instrument;
  }

  public int destinationId() {
    return destination;
  }

  public long bid() {
    return bid;
  }

  public long ask() {
    return ask;
  }

  public long bidSize() {
    return bidSize;
  }

  public long askSize() {
    return askSize;
  }

  public long observedNanos() {
    return observed;
  }

  public boolean valid() {
    return valid;
  }
}
