package io.strategycontainer.api;

import java.util.Map;

/**
 * ServiceLoader provider implemented by each asset-class connector JAR. Secrets stay in connector
 * configuration.
 */
public interface ConnectorFactory {
  String name();

  Connector create(
      Destination destination, Map<String, String> configuration, ConnectorContext context);
}
