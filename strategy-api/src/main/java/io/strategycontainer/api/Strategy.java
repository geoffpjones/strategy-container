package io.strategycontainer.api;

/**
 * All callbacks execute serially on the engine core; implementations must not block or read system
 * time.
 */
public interface Strategy {
  /** Initialize subscriptions and timers on creation or explicit restart. */
  default void onStart(StrategyContext context) {}

  /** Consume a borrowed normalized quote on the core thread. */
  default void onQuote(StrategyContext context, Views.Quote quote) {}

  /** Handle a parent-local timer at the current logical time. */
  default void onTimer(StrategyContext context, long timerId) {}

  /** Observe a normalized report after the container updates its ledger. */
  default void onOrderEvent(StrategyContext context, long childId, Types.ReportType type) {}

  /** Apply an accepted amendment or rebuild plans when resumed. */
  default void onConfiguration(StrategyContext context) {}

  /** Release resources when terminal or replaced by an explicit restart; must not block. */
  default void onStop(StrategyContext context) {}
}
