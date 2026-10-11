package io.strategycontainer.execution;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;

/** Parent-owned fixed-capacity layer reconciler. No allocations on quote/plan callbacks. */
public final class PassiveLayers {
  private final long[] ids = new long[3], targets = new long[3];
  private final double[] weights = {1, StrictMath.exp(-1), StrictMath.exp(-2)};
  private StrategyContext context;
  private int destination;
  private final Views.ChildVisitor restoreVisitor = this::restoreChild;
  private final Views.ChildVisitor cancelVisitor = this::cancelChild;

  /** Restore tracked resting children after restart; foreign-destination children are drained. */
  public void restore(StrategyContext c, int destination) {
    this.context = c;
    this.destination = destination;
    java.util.Arrays.fill(ids, 0);
    c.visitChildren(restoreVisitor);
    context = null;
  }

  private long trackedReserved(StrategyContext c) {
    long total = 0;
    for (long id : ids) {
      Views.Child child = id == 0 ? null : c.child(id);
      if (child != null && !child.terminal()) total += child.reserved();
    }
    return total;
  }

  private void restoreChild(Views.Child child) {
    if (child.terminal() || !child.postOnly()) return;
    if (child.destinationId() != destination) {
      context.cancel(child.id());
      return;
    }
    int position = 0;
    boolean buy = context.parent().specification().side() == Side.BUY;
    while (position < ids.length && ids[position] != 0) {
      Views.Child old = context.child(ids[position]);
      if (buy ? child.price() > old.price() : child.price() < old.price()) break;
      if (child.price() == old.price() && child.id() < old.id()) break;
      position++;
    }
    if (position >= ids.length) {
      context.cancel(child.id());
      return;
    }
    if (ids[ids.length - 1] != 0) context.cancel(ids[ids.length - 1]);
    for (int i = ids.length - 1; i > position; i--) ids[i] = ids[i - 1];
    ids[position] = child.id();
  }

  private void cancelChild(Views.Child child) {
    if (!child.terminal() && child.postOnly()) context.cancel(child.id());
  }

  /** Request cancellation of every parent-owned passive child; retain IDs until terminal facts. */
  public void cancelAll(StrategyContext c) {
    context = c;
    c.visitChildren(cancelVisitor);
    context = null;
  }

  public boolean working(StrategyContext c) {
    for (long id : ids) {
      Views.Child child = id == 0 ? null : c.child(id);
      if (child != null && !child.terminal()) return true;
    }
    return false;
  }

  /**
   * Reconcile total passive exposure at bounded exponential levels. Includes existing reservations.
   */
  public void plan(
      StrategyContext c,
      StrategySettings settings,
      long exposure,
      long anchor,
      int count,
      boolean urgent) {
    destination = settings.destination();
    Instrument instrument = c.instrument();
    Destination venue = c.destination(destination);
    Views.OrderBounds bounds = c.bounds(destination);
    if (!bounds.eligible()) return;
    long tracked = 0;
    for (long id : ids) {
      Views.Child old = id == 0 ? null : c.child(id);
      if (old != null && !old.terminal()) tracked += old.reserved();
    }
    exposure = Math.max(0, exposure - Math.max(0, c.parent().reserved() - tracked));
    long minimum = bounds.minimumQuantity(),
        maximum = bounds.maximumQuantity(),
        lot = instrument.lot();
    double weight = 0;
    for (int i = 0; i < count; i++) weight += weights[i];
    long assigned = 0;
    for (int i = 0; i < 3; i++) {
      Views.Child old = ids[i] == 0 ? null : c.child(ids[i]);
      if (old == null || old.terminal()) ids[i] = 0;
      targets[i] =
          i >= count
              ? 0
              : Trajectory.rounded(
                  i == count - 1 ? exposure - assigned : (long) (exposure * weights[i] / weight),
                  lot);
      assigned += targets[i];
    }
    long dust = 0;
    for (int i = 0; i < count; i++)
      if (targets[i] < minimum) {
        dust += targets[i];
        targets[i] = 0;
      }
    // Collect undersized layer allocations into the nearest level rather than repeatedly rejecting
    // them.
    targets[0] += dust;
    for (int i = 0; i < 3; i++) {
      Views.Child old = ids[i] == 0 ? null : c.child(ids[i]);
      long amount = Math.min(maximum, targets[i]);
      long offset = Math.multiplyExact(i, instrument.tick());
      long price =
          c.parent().specification().side() == Side.BUY
              ? anchor - offset
              : Math.addExact(anchor, offset);
      if (amount < minimum || price <= 0) {
        if (old != null && !old.terminal()) c.cancel(old.id());
        continue;
      }
      if (old != null && !old.terminal()) {
        if (old.state() != ChildState.WORKING) continue;
        Views.Quote quote = c.quote(destination);
        boolean buy = c.parent().specification().side() == Side.BUY;
        if (buy ? old.price() >= quote.ask() : old.price() <= quote.bid()) {
          c.cancel(old.id());
          continue;
        }
        long diff = price >= old.price() ? price - old.price() : old.price() - price;
        long wait = Math.max(settings.restingNanos(), bounds.minimumReplaceNanos());
        boolean sizeChanged = old.reserved() != amount;
        boolean priceChanged = diff / instrument.tick() >= (urgent ? 1 : 2);
        if (!sizeChanged && !priceChanged) continue;
        if (c.nowNanos() - old.lastUpdateNanos() < wait) continue;
        if (!priceChanged) price = old.price();
        if (venue.atomicReplace()) {
          long headroom =
              Math.min(
                  c.bounds(destination).availableQuantity() + old.reserved(),
                  Math.max(0, exposure - trackedReserved(c)) + old.reserved());
          amount = Math.min(amount, headroom);
          amount = Trajectory.rounded(amount, lot);
          if (amount >= minimum) c.replace(old.id(), amount, price);
        } else c.cancel(old.id());
      } else {
        bounds = c.bounds(destination);
        amount =
            Trajectory.rounded(
                Math.min(
                    amount,
                    Math.min(
                        bounds.availableQuantity(), Math.max(0, exposure - trackedReserved(c)))),
                lot);
        if (amount < minimum || bounds.availableChildSlots() <= 0) continue;
        TimeInForce tif =
            venue.supports(OrderType.LIMIT, TimeInForce.DAY) ? TimeInForce.DAY : TimeInForce.GTC;
        StrategyContext.Result result =
            c.submit(destination, amount, price, OrderType.LIMIT, tif, true);
        if (result.accepted()) ids[i] = result.childId();
        else break;
      }
    }
  }
}
