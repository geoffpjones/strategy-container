package io.strategycontainer.core;

/**
 * Bounded engine capacities. Builder defaults are development capacities, not production risk
 * profiles.
 */
public record EngineConfig(
    int ringSize,
    int maxInstruments,
    int maxDestinations,
    int maxParents,
    int maxChildren,
    int maxPolicies,
    int maxTimers,
    long slowCallbackNanos,
    boolean busySpin) {
  public EngineConfig {
    if (ringSize < 16
        || Integer.bitCount(ringSize) != 1
        || maxInstruments <= 0
        || maxDestinations <= 0
        || maxParents <= 0
        || maxChildren <= 0
        || maxPolicies <= 0
        || maxTimers <= 0
        || slowCallbackNanos <= 0) throw new IllegalArgumentException("Invalid capacities");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private int ring = 65536,
        instruments = 1024,
        destinations = 16,
        parents = 256,
        children = 16384,
        policies = 128,
        timers = 4096;
    private long slow = 10_000_000;
    private boolean spin;

    public Builder ringSize(int v) {
      ring = v;
      return this;
    }

    public Builder instruments(int v) {
      instruments = v;
      return this;
    }

    public Builder destinations(int v) {
      destinations = v;
      return this;
    }

    public Builder parents(int v) {
      parents = v;
      return this;
    }

    public Builder children(int v) {
      children = v;
      return this;
    }

    public Builder policies(int v) {
      policies = v;
      return this;
    }

    public Builder timers(int v) {
      timers = v;
      return this;
    }

    public Builder slowCallbackNanos(long v) {
      slow = v;
      return this;
    }

    public Builder busySpin(boolean v) {
      spin = v;
      return this;
    }

    public EngineConfig build() {
      return new EngineConfig(
          ring, instruments, destinations, parents, children, policies, timers, slow, spin);
    }
  }
}
