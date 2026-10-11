package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;

/**
 * Owned normalized execution report. Corrections reference the original execution ID; quantity is
 * its replacement total.
 */
public record ExecutionReport(
    long parentId,
    long childId,
    long commandId,
    int destinationId,
    ReportType type,
    long quantity,
    long price,
    String executionId,
    RejectClass rejectClass,
    LiquidityRole liquidityRole) {
  /** Compatibility constructor for adapters without liquidity classification. */
  public ExecutionReport(
      long parentId,
      long childId,
      long commandId,
      int destinationId,
      ReportType type,
      long quantity,
      long price,
      String executionId,
      RejectClass rejectClass) {
    this(
        parentId,
        childId,
        commandId,
        destinationId,
        type,
        quantity,
        price,
        executionId,
        rejectClass,
        LiquidityRole.UNKNOWN);
  }

  public ExecutionReport {
    java.util.Objects.requireNonNull(liquidityRole);
  }
}
