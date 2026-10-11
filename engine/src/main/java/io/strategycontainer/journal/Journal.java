package io.strategycontainer.journal;

import io.strategycontainer.core.*;

/** Nonblocking core-thread journal interface. Failure must inhibit new exposure. */
public interface Journal extends AutoCloseable {
  boolean input(long sequence, EngineEvent event);

  boolean output(Output output);

  default boolean complete(long sequence) {
    return true;
  }

  boolean healthy();

  long writtenSequence();

  long durableSequence();

  default void close() {}

  static Journal none() {
    return new Journal() {
      public boolean input(long s, EngineEvent e) {
        return true;
      }

      public boolean output(Output o) {
        return true;
      }

      public boolean healthy() {
        return true;
      }

      public long writtenSequence() {
        return 0;
      }

      public long durableSequence() {
        return 0;
      }
    };
  }
}
