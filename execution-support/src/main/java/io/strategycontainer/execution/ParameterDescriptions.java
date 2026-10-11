package io.strategycontainer.execution;

import java.util.Map;

/** Common immutable strategy parameter descriptions for operator discovery. */
public final class ParameterDescriptions {
  public Map<String, String> common() {
    return Map.ofEntries(
        Map.entry("destination", "Execution destination (default 1)"),
            Map.entry("planningMillis", "Planning interval (default 1000)"),
        Map.entry("aheadSeconds", "Release lead (default 30)"),
            Map.entry("behindSeconds", "Permitted lag (default 60)"),
        Map.entry("layers", "Exponential layers, 1..3 (default 3)"),
            Map.entry("workingFraction", "Maximum passive exposure fraction (default 0.10)"),
        Map.entry("minimumRestingMillis", "Minimum passive replacement interval (default 1000)"),
            Map.entry("quoteAgeMillis", "Execution quote maximum age (default 1000)"));
  }
}
