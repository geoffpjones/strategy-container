package io.strategycontainer.risk;

/**
 * Immutable deployment policy. Prices/notionals use instrument-defined units. All budgets are
 * mandatory.
 */
public record Limits(
    long maxParent,
    long minChild,
    long maxChild,
    long maxWorking,
    int maxChildren,
    long maxNotional,
    long quoteAgeNanos,
    long collarTicks,
    long minimumReplaceNanos,
    int messagesPerSecond,
    int burst,
    int cancelPerSecond,
    int cancelBurst,
    int consecutiveRejects,
    int minimumRejectSamples,
    int rejectPercent,
    long rejectWindowNanos) {
  public Limits {
    if (maxParent <= 0
        || minChild <= 0
        || maxChild < minChild
        || maxWorking <= 0
        || maxChildren <= 0
        || maxNotional <= 0
        || quoteAgeNanos <= 0
        || collarTicks < 0
        || minimumReplaceNanos < 0
        || messagesPerSecond <= 0
        || burst <= 0
        || cancelPerSecond <= 0
        || cancelBurst <= 0
        || consecutiveRejects <= 0
        || minimumRejectSamples <= 0
        || minimumRejectSamples > 1024
        || rejectPercent <= 0
        || rejectPercent > 100
        || rejectWindowNanos <= 0)
      throw new IllegalArgumentException("Incomplete/invalid risk profile");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private long parent = 100_000_000,
        min = 1,
        child = 10_000_000,
        working = 100_000_000,
        notional = Long.MAX_VALUE,
        age = 1_000_000_000L,
        collar = 100,
        replace;
    private int children = 64,
        rate = 1000,
        burst = 100,
        cancelRate = 1000,
        cancelBurst = 100,
        rejects = 5,
        samples = 20,
        percent = 50;
    private long window = 10_000_000_000L;

    public Builder maxParent(long v) {
      parent = v;
      return this;
    }

    public Builder childSizes(long a, long b) {
      min = a;
      child = b;
      return this;
    }

    public Builder maxWorking(long v) {
      working = v;
      return this;
    }

    public Builder maxChildren(int v) {
      children = v;
      return this;
    }

    public Builder maxNotional(long v) {
      notional = v;
      return this;
    }

    public Builder quoteAgeNanos(long v) {
      age = v;
      return this;
    }

    public Builder collarTicks(long v) {
      collar = v;
      return this;
    }

    public Builder minimumReplaceNanos(long v) {
      replace = v;
      return this;
    }

    public Builder messages(int r, int b) {
      rate = r;
      burst = b;
      return this;
    }

    public Builder cancellations(int r, int b) {
      cancelRate = r;
      cancelBurst = b;
      return this;
    }

    public Builder rejects(int consecutive, int minimum, int percent, long nanos) {
      rejects = consecutive;
      samples = minimum;
      this.percent = percent;
      window = nanos;
      return this;
    }

    public Limits build() {
      return new Limits(
          parent,
          min,
          child,
          working,
          children,
          notional,
          age,
          collar,
          replace,
          rate,
          burst,
          cancelRate,
          cancelBurst,
          rejects,
          samples,
          percent,
          window);
    }
  }
}
