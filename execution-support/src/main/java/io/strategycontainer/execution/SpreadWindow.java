package io.strategycontainer.execution;

/** Bounded rolling, equally time-sampled spread histogram, with explicit overflow bucket. */
public final class SpreadWindow {
  private final int[] histogram = new int[128], samples = new int[256];
  private int next, count;
  private long last = Long.MIN_VALUE;

  public boolean observe(long now, long interval, long ticks) {
    if (last != Long.MIN_VALUE && now - last < interval)
      return count >= 32 && ticks <= quantile(.20) && ticks < quantile(.50);
    boolean cheap = count >= 32 && ticks <= quantile(.20) && ticks < quantile(.50);
    int bucket = (int) Math.min(127, Math.max(0, ticks));
    if (count == samples.length) histogram[samples[next]]--;
    else count++;
    samples[next] = bucket;
    histogram[bucket]++;
    next = (next + 1) % samples.length;
    last = now;
    return cheap;
  }

  private int quantile(double p) {
    int target = (int) StrictMath.ceil(count * p), sum = 0;
    for (int i = 0; i < histogram.length; i++) {
      sum += histogram[i];
      if (sum >= target) return i;
    }
    return 127;
  }
}
