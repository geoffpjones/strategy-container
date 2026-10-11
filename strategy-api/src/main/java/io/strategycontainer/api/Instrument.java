package io.strategycontainer.api;

import java.util.Objects;

/**
 * Fixed-point definition: price / priceScale is the quoted price; quantity is in declared units.
 */
public record Instrument(
    int id,
    String symbol,
    String quantityUnit,
    long priceScale,
    long tick,
    long lot,
    long minimumQuantity,
    long multiplier) {
  public Instrument {
    Objects.requireNonNull(symbol);
    Objects.requireNonNull(quantityUnit);
    if (id <= 0
        || priceScale <= 0
        || tick <= 0
        || lot <= 0
        || minimumQuantity <= 0
        || multiplier <= 0) throw new IllegalArgumentException("Invalid instrument");
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private int id;
    private String symbol, unit = "units";
    private long scale = 100_000, tick = 1, lot = 1, minimum = 1, multiplier = 1;

    public Builder id(int v) {
      id = v;
      return this;
    }

    public Builder symbol(String v) {
      symbol = v;
      return this;
    }

    public Builder quantityUnit(String v) {
      unit = v;
      return this;
    }

    public Builder priceScale(long v) {
      scale = v;
      return this;
    }

    public Builder tick(long v) {
      tick = v;
      return this;
    }

    public Builder lot(long v) {
      lot = v;
      return this;
    }

    public Builder minimumQuantity(long v) {
      minimum = v;
      return this;
    }

    public Builder multiplier(long v) {
      multiplier = v;
      return this;
    }

    public Instrument build() {
      return new Instrument(id, symbol, unit, scale, tick, lot, minimum, multiplier);
    }
  }
}
