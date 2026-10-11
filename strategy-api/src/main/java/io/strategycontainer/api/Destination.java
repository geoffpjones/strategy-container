package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;
import java.util.Objects;

/**
 * Immutable destination capability declaration. IDs are numeric and bounded by deployment capacity.
 */
public record Destination(
    int id,
    String name,
    int instrumentId,
    long minimumQuantity,
    long maximumQuantity,
    int combinations,
    boolean postOnly,
    boolean protectedPeg,
    boolean atomicReplace,
    boolean internalBooking,
    boolean firm,
    int replaceMessageCost) {
  public Destination {
    Objects.requireNonNull(name);
    if (id <= 0
        || instrumentId < 0
        || minimumQuantity <= 0
        || maximumQuantity < minimumQuantity
        || replaceMessageCost <= 0) throw new IllegalArgumentException("Invalid destination");
  }

  public boolean supports(OrderType type, TimeInForce tif) {
    return (combinations & (1 << (type.ordinal() * 4 + tif.ordinal()))) != 0;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private int id, instrument, combinations;
    private String name;
    private long min = 1, max = Long.MAX_VALUE;
    private boolean postOnly, peg, atomic = true, booking, firm = true;
    private int replaceCost = 1;

    public Builder id(int v) {
      id = v;
      return this;
    }

    public Builder name(String v) {
      name = v;
      return this;
    }

    public Builder instrumentId(int v) {
      instrument = v;
      return this;
    }

    public Builder sizes(long a, long b) {
      min = a;
      max = b;
      return this;
    }

    public Builder supports(OrderType t, TimeInForce f) {
      combinations |= 1 << (t.ordinal() * 4 + f.ordinal());
      return this;
    }

    public Builder postOnly(boolean v) {
      postOnly = v;
      return this;
    }

    public Builder protectedPeg(boolean v) {
      peg = v;
      return this;
    }

    public Builder atomicReplace(boolean v) {
      atomic = v;
      return this;
    }

    public Builder internalBooking(boolean v) {
      booking = v;
      return this;
    }

    public Builder firm(boolean v) {
      firm = v;
      return this;
    }

    public Builder replaceMessageCost(int v) {
      replaceCost = v;
      return this;
    }

    public Destination build() {
      return new Destination(
          id,
          name,
          instrument,
          min,
          max,
          combinations,
          postOnly,
          peg,
          atomic,
          booking,
          firm,
          replaceCost);
    }
  }
}
