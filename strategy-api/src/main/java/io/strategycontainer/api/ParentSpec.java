package io.strategycontainer.api;

import io.strategycontainer.api.Types.Side;
import java.util.Map;
import java.util.Objects;

/** Parent and strategy configuration; parameters are defensively copied and immutable. */
public record ParentSpec(
    long id,
    int accountId,
    int instrumentId,
    Side side,
    long quantity,
    long limit,
    long startNanos,
    long endNanos,
    String strategy,
    Map<String, String> parameters) {
  public ParentSpec {
    Objects.requireNonNull(side);
    Objects.requireNonNull(strategy);
    parameters = Map.copyOf(parameters);
    if (id <= 0
        || accountId <= 0
        || instrumentId <= 0
        || quantity <= 0
        || limit <= 0
        || startNanos < 0
        || endNanos <= startNanos) throw new IllegalArgumentException("Invalid parent");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private long id, quantity, limit, start, end;
    private int account = 1, instrument;
    private Side side = Side.BUY;
    private String strategy = "twap";
    private Map<String, String> params = Map.of();

    public Builder id(long v) {
      id = v;
      return this;
    }

    public Builder accountId(int v) {
      account = v;
      return this;
    }

    public Builder instrumentId(int v) {
      instrument = v;
      return this;
    }

    public Builder side(Side v) {
      side = v;
      return this;
    }

    public Builder quantity(long v) {
      quantity = v;
      return this;
    }

    public Builder limit(long v) {
      limit = v;
      return this;
    }

    public Builder window(long a, long b) {
      start = a;
      end = b;
      return this;
    }

    public Builder strategy(String v) {
      strategy = v;
      return this;
    }

    public Builder parameters(Map<String, String> v) {
      params = v;
      return this;
    }

    public ParentSpec build() {
      return new ParentSpec(
          id, account, instrument, side, quantity, limit, start, end, strategy, params);
    }
  }
}
