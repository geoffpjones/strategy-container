package io.strategycontainer.orders;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;

/** Core-owned authoritative parent state. References are never exposed to connector threads. */
public final class ParentOrder implements Views.Parent {
  public ParentSpec spec;
  public ParentState state = ParentState.ACTIVE;
  public long version = 1, venue, wash, reserved;
  public int children, localFailures, venueRejects;
  public String diagnostic = "";

  public ParentSpec specification() {
    return spec;
  }

  public ParentState state() {
    return state;
  }

  public long version() {
    return version;
  }

  public long venueFilled() {
    return venue;
  }

  public long washFilled() {
    return wash;
  }

  public long reserved() {
    return reserved;
  }

  public long residual() {
    return spec.quantity() - venue - wash;
  }

  public int workingChildren() {
    return children;
  }
}
