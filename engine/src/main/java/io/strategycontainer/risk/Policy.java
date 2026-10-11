package io.strategycontainer.risk;

import io.strategycontainer.api.Types.Scope;

/** Scoped policy; key is numeric except STRATEGY, which uses strategyName. */
public record Policy(Scope scope, long key, String strategyName, Limits limits) {
  public Policy {
    if (scope == null || limits == null || strategyName == null)
      throw new IllegalArgumentException("Invalid policy");
  }

  public static Policy engine(Limits limits) {
    return new Policy(Scope.ENGINE, 0, "", limits);
  }
}
