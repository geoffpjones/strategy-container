package io.strategycontainer.bootstrap;

import io.strategycontainer.api.*;
import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/**
 * Startup-only provider discovery. All loaded artifacts are fingerprinted before journal creation.
 */
public final class PluginRegistry implements AutoCloseable {
  private final Map<String, StrategyFactory> factories = new TreeMap<>();
  private final Map<String, ConnectorFactory> connectors = new TreeMap<>();
  private final List<URLClassLoader> loaders = new ArrayList<>();

  public PluginRegistry register(StrategyFactory f) {
    if (f.name() == null || f.name().isBlank() || factories.putIfAbsent(f.name(), f) != null)
      throw new IllegalArgumentException("Duplicate/invalid strategy name");
    return this;
  }

  public PluginRegistry discover(Path directory) throws IOException {
    if (!Files.exists(directory)) return this;
    List<Path> jars;
    try (var stream = Files.list(directory)) {
      jars = stream.filter(p -> p.toString().endsWith(".jar")).sorted().toList();
    }
    for (Path jar : jars) {
      URLClassLoader loader =
          new URLClassLoader(
              new java.net.URL[] {jar.toUri().toURL()}, StrategyFactory.class.getClassLoader());
      loaders.add(loader);
      for (StrategyFactory f : ServiceLoader.load(StrategyFactory.class, loader)) register(f);
      for (ConnectorFactory f : ServiceLoader.load(ConnectorFactory.class, loader))
        if (connectors.putIfAbsent(f.name(), f) != null)
          throw new IllegalArgumentException("Duplicate connector name");
    }
    return this;
  }

  public StrategyFactory require(String name) {
    StrategyFactory f = factories.get(name);
    if (f == null) throw new IllegalArgumentException("Unknown strategy: " + name);
    return f;
  }

  public ConnectorFactory connector(String name) {
    ConnectorFactory f = connectors.get(name);
    if (f == null) throw new IllegalArgumentException("Unknown connector: " + name);
    return f;
  }

  public Set<String> names() {
    return Collections.unmodifiableSet(factories.keySet());
  }

  /**
   * Hash the complete artifact, not just its provider class; class-directory builds use sorted
   * relative paths.
   */
  private static String fingerprint(Class<?> type) {
    try {
      Path location = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
      MessageDigest d = MessageDigest.getInstance("SHA-256");
      if (Files.isDirectory(location)) {
        List<Path> files;
        try (var stream = Files.walk(location)) {
          files = stream.filter(Files::isRegularFile).sorted().toList();
        }
        for (Path file : files) {
          d.update(
              location
                  .relativize(file)
                  .toString()
                  .getBytes(java.nio.charset.StandardCharsets.UTF_8));
          try (var in = Files.newInputStream(file)) {
            byte[] bytes = new byte[8192];
            int n;
            while ((n = in.read(bytes)) != -1) d.update(bytes, 0, n);
          }
        }
      } else
        try (var in = Files.newInputStream(location)) {
          byte[] bytes = new byte[8192];
          int n;
          while ((n = in.read(bytes)) != -1) d.update(bytes, 0, n);
        }
      return HexFormat.of().formatHex(d.digest());
    } catch (Exception ex) {
      throw new IllegalStateException("Cannot fingerprint artifact", ex);
    }
  }

  public String manifest() {
    StringBuilder b = new StringBuilder("strategy-container/schema=2/java=21\n");
    b.append("engine=")
        .append(fingerprint(io.strategycontainer.core.EngineCore.class))
        .append('\n')
        .append("api=")
        .append(fingerprint(Strategy.class))
        .append('\n');
    factories.forEach(
        (n, f) ->
            b.append("strategy:")
                .append(n)
                .append('=')
                .append(fingerprint(f.getClass()))
                .append('\n'));
    connectors.forEach(
        (n, f) ->
            b.append("connector:")
                .append(n)
                .append('=')
                .append(fingerprint(f.getClass()))
                .append('\n'));
    return b.toString();
  }

  public void close() throws IOException {
    for (URLClassLoader loader : loaders) loader.close();
  }
}
