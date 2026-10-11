package io.strategycontainer.risk;

/** Core-owned scoped throughput and reject-window state. */
public final class PolicyState {
  public final Policy policy;
  public final TokenBucket messages, cancellations;
  private final long[] times = new long[1024];
  private final boolean[] rejected = new boolean[1024];
  private int next, count, consecutive;
  public boolean tripped;

  public PolicyState(Policy policy, long now) {
    this.policy = policy;
    Limits l = policy.limits();
    messages = new TokenBucket(l.messagesPerSecond(), l.burst(), now);
    cancellations = new TokenBucket(l.cancelPerSecond(), l.cancelBurst(), now);
  }

  public boolean outcome(long now, boolean reject) {
    times[next] = now;
    rejected[next] = reject;
    next = (next + 1) % times.length;
    count = Math.min(times.length, count + 1);
    consecutive = reject ? consecutive + 1 : 0;
    int samples = 0, failures = 0;
    Limits l = policy.limits();
    for (int i = 0; i < count; i++) {
      if (now - times[i] <= l.rejectWindowNanos()) {
        samples++;
        if (rejected[i]) failures++;
      }
    }
    tripped |=
        consecutive >= l.consecutiveRejects()
            || samples >= l.minimumRejectSamples()
                && (long) failures * 100 >= (long) samples * l.rejectPercent();
    return tripped;
  }

  public void reset() {
    tripped = false;
    next = count = consecutive = 0;
  }
}
