package io.strategycontainer.api;

/**
 * Connector methods run outside the core. Emit all outcomes through the engine-provided sink, never
 * mutate ledger state.
 */
public interface Connector extends AutoCloseable {
  String name();

  void send(ExecutionCommand command) throws Exception;

  /** Subscribe on connector I/O thread; implementation owns the external protocol. */
  default void subscribe(int instrumentId) throws Exception {}

  /** Unsubscribe on connector I/O thread. */
  default void unsubscribe(int instrumentId) throws Exception {}

  default void close() {}
}
