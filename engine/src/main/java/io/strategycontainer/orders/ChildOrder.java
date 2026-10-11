package io.strategycontainer.orders;

import io.strategycontainer.api.Types.*;
import io.strategycontainer.api.Views;

/** Single-writer child state. Reservation is independent of venue acknowledgement state. */
public final class ChildOrder implements Views.Child {
  public long id() {
    return id;
  }

  public int destinationId() {
    return destination;
  }

  public long leaves() {
    return Math.max(0, quantity - filled);
  }

  public long reserved() {
    return reservation;
  }

  public long price() {
    return price;
  }

  public long pendingQuantity() {
    return Math.max(0, replaceQuantity - filled);
  }

  public long pendingPrice() {
    return replacePrice;
  }

  public long lastUpdateNanos() {
    return lastReplace;
  }

  public ChildState state() {
    return state;
  }

  public boolean postOnly() {
    return postOnly;
  }

  public ChildOrder nextOwned;

  /** Core-owned ledger chain ordered by stable child ID, including terminal history. */
  public ChildOrder nextGlobal;

  public long id,
      parentId,
      quantity,
      filled,
      price,
      reservation,
      replaceQuantity,
      replacePrice,
      lastReplace,
      commandId;
  public int destination;
  public OrderType type;
  public TimeInForce tif;
  public boolean postOnly;
  public ChildState state;

  public boolean terminal() {
    return state == ChildState.CANCELLED
        || state == ChildState.FILLED
        || state == ChildState.REJECTED;
  }
}
