package io.strategycontainer.api;

import java.util.List;
import java.util.Objects;

/** Immutable execution permissions and reference requirements, resolved at parent admission. */
public record ExecutionProfile(
    boolean passiveOnly,
    List<Integer> referenceDestinations,
    long referenceAgeNanos,
    long benchmarkWaitNanos) {
  public ExecutionProfile {
    Objects.requireNonNull(referenceDestinations);
    referenceDestinations = referenceDestinations.stream().distinct().sorted().toList();
    if (referenceDestinations.stream().anyMatch(i -> i <= 0)
        || referenceAgeNanos <= 0
        || benchmarkWaitNanos <= 0) throw new IllegalArgumentException("Invalid execution profile");
  }

  /** Existing strategies permit aggression and have no inception benchmark. */
  public static ExecutionProfile unrestricted() {
    return new ExecutionProfile(false, List.of(), 1_000_000_000L, 5_000_000_000L);
  }

  /** Whether this parent must capture an inception midpoint before trading. */
  public boolean requiresBenchmark() {
    return !referenceDestinations.isEmpty();
  }
}
