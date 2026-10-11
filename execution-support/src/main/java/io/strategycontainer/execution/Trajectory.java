package io.strategycontainer.execution;

/** Pure deterministic trajectory functions; zero urgency is exactly the linear limit. */
public final class Trajectory {
  private Trajectory() {}

  public static long quantity(long total, long elapsed, long duration, double urgency) {
    if (elapsed <= 0) return 0;
    if (elapsed >= duration) return total;
    double u = elapsed / (double) duration;
    double fraction =
        urgency < 1e-6 ? u : 1 - StrictMath.sinh(urgency * (1 - u)) / StrictMath.sinh(urgency);
    return Math.min(total, Math.max(0, (long) (total * fraction)));
  }

  /** Saturate time offsets so even extreme valid configuration cannot wrap an execution window. */
  public static long ahead(long elapsed, long offset, long duration) {
    return offset >= duration - elapsed ? duration : elapsed + offset;
  }

  public static long rounded(long value, long lot) {
    return Math.max(0, value) - Math.max(0, value) % lot;
  }
}
