package io.strategycontainer.core;

import io.strategycontainer.api.*;
import io.strategycontainer.orders.QuoteState;

/** Core-owned shared composite. Cached by logical time and normalized market revision. */
final class CompositeReference implements Views.ReferenceMarket {
  final int instrument;
  final ExecutionProfile profile;
  private final QuoteState[] quotes;
  private final Destination[] destinations;
  private final boolean[] online;
  private long time = Long.MIN_VALUE, revision = Long.MIN_VALUE;
  private long bid, ask, bidObserved, askObserved;
  private int bidDestination, askDestination, constituents;
  private boolean valid;

  CompositeReference(
      int instrument,
      ExecutionProfile profile,
      QuoteState[] quotes,
      Destination[] destinations,
      boolean[] online) {
    this.instrument = instrument;
    this.profile = profile;
    this.quotes = quotes;
    this.destinations = destinations;
    this.online = online;
  }

  void refresh(long now, long marketRevision) {
    if (time == now && revision == marketRevision) return;
    time = now;
    revision = marketRevision;
    bid = 0;
    ask = Long.MAX_VALUE;
    constituents = 0;
    bidDestination = askDestination = 0;
    bidObserved = askObserved = 0;
    for (int i = 0; i < profile.referenceDestinations().size(); i++) {
      int id = profile.referenceDestinations().get(i);
      Destination d = destinations[id];
      QuoteState q = quotes[id];
      if (d == null
          || !d.firm()
          || !online[id]
          || q == null
          || !q.valid
          || q.bid <= 0
          || q.ask <= q.bid
          || q.bidSize <= 0
          || q.askSize <= 0
          || q.observed > now
          || now - q.observed > profile.referenceAgeNanos()) continue;
      constituents++;
      if (q.bid > bid) {
        bid = q.bid;
        bidDestination = id;
        bidObserved = q.observed;
      }
      if (q.ask < ask) {
        ask = q.ask;
        askDestination = id;
        askObserved = q.observed;
      }
    }
    valid = constituents > 0 && bid < ask;
    if (constituents == 0) ask = 0;
  }

  public long bid() {
    return bid;
  }

  public long ask() {
    return ask;
  }

  public int bidDestination() {
    return bidDestination;
  }

  public int askDestination() {
    return askDestination;
  }

  public long bidObservedNanos() {
    return bidObserved;
  }

  public long askObservedNanos() {
    return askObserved;
  }

  public int constituents() {
    return constituents;
  }

  public boolean valid() {
    return valid;
  }
}
