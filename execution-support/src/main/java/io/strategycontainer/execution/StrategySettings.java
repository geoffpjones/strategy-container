package io.strategycontainer.execution;

import io.strategycontainer.api.ParentSpec;
import java.util.Map;

/** Validated immutable common controls; time is logical nanoseconds and prices are tick offsets. */
public record StrategySettings(
    int destination,
    long aheadNanos,
    long behindNanos,
    long intervalNanos,
    long quoteAgeNanos,
    int layers,
    double workingFraction,
    int improvementTicks,
    long restingNanos) {
  public static StrategySettings from(ParentSpec p) {
    Map<String, String> m = p.parameters();
    int d = integer(m, "destination", 1),
        layers = integer(m, "layers", 3),
        improvement = integer(m, "improvementTicks", 1);
    long ahead = nanos(m, "aheadSeconds", 30, 1_000_000_000L),
        behind = nanos(m, "behindSeconds", 60, 1_000_000_000L),
        interval = nanos(m, "planningMillis", 1000, 1_000_000L),
        age = nanos(m, "quoteAgeMillis", 1000, 1_000_000L),
        rest = nanos(m, "minimumRestingMillis", 1000, 1_000_000L);
    double fraction = decimal(m, "workingFraction", .10);
    if (d <= 0
        || layers < 1
        || layers > 3
        || improvement < 0
        || improvement > 100
        || interval <= 0
        || age <= 0
        || !Double.isFinite(fraction)
        || fraction <= 0
        || fraction > 1) throw new IllegalArgumentException("Invalid execution settings");
    return new StrategySettings(
        d, ahead, behind, interval, age, layers, fraction, improvement, rest);
  }

  public static int integer(Map<String, String> m, String key, int fallback) {
    return Integer.parseInt(m.getOrDefault(key, Integer.toString(fallback)));
  }

  public static double decimal(Map<String, String> m, String key, double fallback) {
    return Double.parseDouble(m.getOrDefault(key, Double.toString(fallback)));
  }

  public static long nanos(Map<String, String> m, String key, long fallback, long multiplier) {
    long v = Long.parseLong(m.getOrDefault(key, Long.toString(fallback)));
    if (v < 0) throw new IllegalArgumentException("Negative " + key);
    return Math.multiplyExact(v, multiplier);
  }
}
