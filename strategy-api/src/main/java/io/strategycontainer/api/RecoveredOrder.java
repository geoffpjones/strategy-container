package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;

/**
 * OMS recovery snapshot. Connectors must reconcile historical execution references before declaring
 * completion.
 */
public record RecoveredOrder(
    ParentSpec parent,
    long childId,
    long commandId,
    int destinationId,
    long remainingQuantity,
    long cumulativeFilled,
    long price,
    OrderType type,
    TimeInForce tif,
    boolean postOnly,
    InceptionBenchmark benchmark) {
  /**
   * Compatibility constructor; started IS parents require separately retained benchmark metadata.
   */
  public RecoveredOrder(
      ParentSpec parent,
      long childId,
      long commandId,
      int destinationId,
      long remainingQuantity,
      long cumulativeFilled,
      long price,
      OrderType type,
      TimeInForce tif,
      boolean postOnly) {
    this(
        parent,
        childId,
        commandId,
        destinationId,
        remainingQuantity,
        cumulativeFilled,
        price,
        type,
        tif,
        postOnly,
        null);
  }

  public RecoveredOrder {
    if (parent == null
        || childId <= 0
        || commandId <= 0
        || destinationId <= 0
        || remainingQuantity < 0
        || cumulativeFilled < 0
        || price <= 0
        || type == null
        || tif == null) throw new IllegalArgumentException("Invalid recovered order");
  }
}
