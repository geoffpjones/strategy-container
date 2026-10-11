package io.strategycontainer.core;

import java.util.concurrent.atomic.LongAdder;

/** Operational counters do not participate in decisions or replay hashes. */
public final class EngineMetrics {
  public volatile long slowParent;
  public volatile boolean journalHealthy = true;
  public volatile long processed,
      sequence,
      logicalNanos,
      callbackMaxNanos,
      queueRemaining,
      journalWritten,
      journalDurable;
  private final long[] latencyMicros = new long[4096];
  private volatile long latencySamples;

  public void latency(long nanos) {
    latencyMicros[(int) Math.min(4095, Math.max(0, nanos / 1000))]++;
    latencySamples++;
  }

  public long percentileMicros(double fraction) {
    long target = (long) Math.ceil(latencySamples * fraction), count = 0;
    for (int i = 0; i < latencyMicros.length; i++) {
      count += latencyMicros[i];
      if (count >= target) return i;
    }
    return 4095;
  }

  public void resetLatency() {
    java.util.Arrays.fill(latencyMicros, 0);
    latencySamples = 0;
  }

  public final LongAdder offered = new LongAdder(),
      backpressure = new LongAdder(),
      commands = new LongAdder(),
      errors = new LongAdder();

  public String text() {
    return "journalHealthy="
        + journalHealthy
        + " processed="
        + processed
        + " sequence="
        + sequence
        + " logicalNanos="
        + logicalNanos
        + " backpressure="
        + backpressure.sum()
        + " commands="
        + commands.sum()
        + " callbackMaxNanos="
        + callbackMaxNanos
        + " journalWritten="
        + journalWritten
        + " journalDurable="
        + journalDurable;
  }
}
