package io.strategycontainer.core;

/** Thread-safe, bounded ingress publication; false means the caller must retain/retry the event. */
@FunctionalInterface
public interface InputSink {
  boolean offer(EngineEvent event);
}
