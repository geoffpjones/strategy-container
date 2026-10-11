package io.strategycontainer.core;

/** Core-thread output observer; implementations must be bounded and non-blocking. */
@FunctionalInterface
public interface OutputSink {
  void accept(Output output);
}
