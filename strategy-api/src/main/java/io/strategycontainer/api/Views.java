package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;

/**
 * Read-only borrowed views. Access only on the owning engine callback thread; never retain across
 * callbacks.
 */
public final class Views {
  private Views() {}

  public interface Quote {
    int instrumentId();

    int destinationId();

    long bid();

    long ask();

    long bidSize();

    long askSize();

    long observedNanos();

    boolean valid();
  }

  /** Borrowed parent-owned child ledger, including pending and terminal states. */
  public interface Child {
    long id();

    int destinationId();

    long leaves();

    long reserved();

    long price();

    long pendingQuantity();

    long pendingPrice();

    long lastUpdateNanos();

    Types.ChildState state();

    boolean postOnly();

    boolean terminal();
  }

  /** Allocation-free traversal; do not retain the borrowed child. */
  @FunctionalInterface
  public interface ChildVisitor {
    void accept(Child child);
  }

  /** Effective policy intersection; a snapshot of bounds, not authorization to route. */
  public interface OrderBounds {
    long minimumQuantity();

    long maximumQuantity();

    long availableQuantity();

    int availableChildSlots();

    long minimumReplaceNanos();

    boolean eligible();
  }

  /** Cached composite BBO across eligible fresh, firm destinations. */
  public interface ReferenceMarket {
    long bid();

    long ask();

    int bidDestination();

    int askDestination();

    long bidObservedNanos();

    long askObservedNanos();

    int constituents();

    boolean valid();

    default double mid() {
      return bid() * .5 + ask() * .5;
    }
  }

  public interface Parent {
    ParentSpec specification();

    ParentState state();

    long version();

    long venueFilled();

    long washFilled();

    long reserved();

    long residual();

    int workingChildren();
  }
}
