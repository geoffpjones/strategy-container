package io.strategycontainer.replay;

import io.strategycontainer.bootstrap.PluginRegistry;
import io.strategycontainer.core.*;
import io.strategycontainer.journal.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;

/**
 * Streaming offline verification: no real connector, HTTP listener or system clock publisher is
 * started.
 */
public final class ReplayVerifier {
  public record Result(long inputs, long outputs, String stateHash) {}

  private ReplayVerifier() {}

  public static Result verify(Path directory, EngineConfig config, PluginRegistry plugins)
      throws IOException {
    return verify(directory, config, plugins, Long.MAX_VALUE);
  }

  /** Verify only an explicitly selected complete prefix, for crash recovery. */
  public static Result verifyPrefix(
      Path directory, EngineConfig config, PluginRegistry plugins, long maximumSequence)
      throws IOException {
    return verify(directory, config, plugins, maximumSequence);
  }

  private static Result verify(
      Path directory, EngineConfig config, PluginRegistry plugins, long maximumSequence)
      throws IOException {
    ArrayDeque<Output> generated = new ArrayDeque<>();
    EngineCore core = new EngineCore(config, plugins, Journal.none(), generated::add, o -> {});
    core.replayMode();
    long[] count = {0, 0};
    JournalReader.Visitor visitor =
        (type, seq, payload) -> {
          if (type == 1) {
            if (!generated.isEmpty())
              throw new Divergence("Missing recorded outputs before " + seq);
            core.process(seq, BinaryCodec.input(payload));
            count[0]++;
          } else if (type == 2) {
            Output recorded = BinaryCodec.output(payload, seq), actual = generated.poll();
            if (!recorded.equals(actual))
              throw new Divergence(
                  "First divergence at input "
                      + seq
                      + ": recorded="
                      + recorded
                      + ", generated="
                      + actual);
            count[1]++;
          } else if (!generated.isEmpty())
            throw new Divergence("Unrecorded outputs at input " + seq + ": " + generated.peek());
        };
    String manifest = plugins.manifest() + config;
    if (maximumSequence == Long.MAX_VALUE) JournalReader.visit(directory, manifest, visitor);
    else JournalReader.visitPrefix(directory, manifest, maximumSequence, visitor);
    if (!generated.isEmpty()) throw new IOException("Missing recorded outputs");
    return new Result(count[0], count[1], core.stateHash());
  }

  /** Bypass input-error conversion so replay stops on the actual first mismatch. */
  public static final class Divergence extends Error {
    private static final long serialVersionUID = 1L;

    public Divergence(String message) {
      super(message);
    }
  }
}
