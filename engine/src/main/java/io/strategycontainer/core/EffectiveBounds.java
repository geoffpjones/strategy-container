package io.strategycontainer.core;

import io.strategycontainer.api.Views;

/** Reused policy-intersection scratch view, confined to a parent callback. */
final class EffectiveBounds implements Views.OrderBounds {
  long minimum, maximum, available, replaceNanos;
  int slots;
  boolean eligible;

  public long minimumQuantity() {
    return minimum;
  }

  public long maximumQuantity() {
    return maximum;
  }

  public long availableQuantity() {
    return available;
  }

  public int availableChildSlots() {
    return slots;
  }

  public long minimumReplaceNanos() {
    return replaceNanos;
  }

  public boolean eligible() {
    return eligible;
  }
}
