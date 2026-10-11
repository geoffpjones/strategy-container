package io.strategycontainer.risk;

/** Single-writer token bucket driven exclusively by logical nanoseconds. */
public final class TokenBucket {
  private final int rate, burst;
  private double tokens;
  private long last;

  public TokenBucket(int rate, int burst, long now) {
    this.rate = rate;
    this.burst = burst;
    tokens = burst;
    last = now;
  }

  private void refill(long now) {
    if (now > last) {
      tokens = Math.min(burst, tokens + (now - last) * (rate / 1_000_000_000.0));
      last = now;
    }
  }

  public boolean available(long now, int cost) {
    refill(now);
    return tokens >= cost;
  }

  public void consume(int cost) {
    tokens -= cost;
  }

  public double remaining(long now) {
    refill(now);
    return tokens;
  }
}
