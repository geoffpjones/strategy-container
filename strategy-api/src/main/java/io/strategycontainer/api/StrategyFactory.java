package io.strategycontainer.api;

import java.util.Map;

/** Implement and declare this provider in META-INF/services to deploy a separate strategy JAR. */
public interface StrategyFactory {
  String name();

  Strategy create(ParentSpec parent);

  /** Engine-enforced permissions and inception-reference requirements for this parent. */
  default ExecutionProfile executionProfile(ParentSpec parent) {
    return ExecutionProfile.unrestricted();
  }

  default void validate(ParentSpec parent) {}

  default Map<String, String> parameterDescriptions() {
    return Map.of();
  }
}
