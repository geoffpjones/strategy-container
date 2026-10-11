package io.strategycontainer.bootstrap;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.risk.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Explicit deployment profile. Required production risk settings are never silently defaulted. */
public final class Deployment {
  private final Properties p;

  private Deployment(Properties p) {
    this.p = p;
  }

  public static Deployment load(Path file) throws IOException {
    Properties p = new Properties();
    try (var in = Files.newInputStream(file)) {
      p.load(in);
    }
    return new Deployment(p);
  }

  private String required(String key) {
    String v = p.getProperty(key);
    if (v == null || v.isBlank())
      throw new IllegalArgumentException("Missing deployment setting " + key);
    return v;
  }

  private long number(String key) {
    return Long.parseLong(required(key));
  }

  private int integer(String key) {
    return Math.toIntExact(number(key));
  }

  public Limits limits() {
    return Limits.builder()
        .maxParent(number("risk.maxParent"))
        .childSizes(number("risk.minChild"), number("risk.maxChild"))
        .maxWorking(number("risk.maxWorking"))
        .maxChildren(integer("risk.maxChildren"))
        .maxNotional(number("risk.maxNotional"))
        .quoteAgeNanos(number("risk.quoteAgeNanos"))
        .collarTicks(number("risk.collarTicks"))
        .minimumReplaceNanos(number("risk.minimumReplaceNanos"))
        .messages(integer("risk.rate"), integer("risk.burst"))
        .cancellations(integer("risk.cancelRate"), integer("risk.cancelBurst"))
        .rejects(
            integer("risk.consecutiveRejects"),
            integer("risk.minimumRejectSamples"),
            integer("risk.rejectPercent"),
            number("risk.rejectWindowNanos"))
        .build();
  }

  public List<Instrument> instruments() {
    List<Instrument> list = new ArrayList<>();
    for (String id : required("instruments").split(",")) {
      String prefix = "instrument." + id + ".";
      list.add(
          Instrument.builder()
              .id(Integer.parseInt(id))
              .symbol(required(prefix + "symbol"))
              .quantityUnit(required(prefix + "unit"))
              .priceScale(number(prefix + "scale"))
              .tick(number(prefix + "tick"))
              .lot(number(prefix + "lot"))
              .minimumQuantity(number(prefix + "minimum"))
              .multiplier(number(prefix + "multiplier"))
              .build());
    }
    return List.copyOf(list);
  }

  public List<Destination> destinations() {
    List<Destination> list = new ArrayList<>();
    for (String id : required("destinations").split(",")) {
      String prefix = "destination." + id + ".";
      Destination.Builder d =
          Destination.builder()
              .id(Integer.parseInt(id))
              .name(required(prefix + "name"))
              .instrumentId(integer(prefix + "instrument"))
              .sizes(number(prefix + "minimum"), number(prefix + "maximum"))
              .postOnly(Boolean.parseBoolean(required(prefix + "postOnly")))
              .protectedPeg(Boolean.parseBoolean(required(prefix + "protectedPeg")))
              .atomicReplace(Boolean.parseBoolean(required(prefix + "atomicReplace")))
              .firm(Boolean.parseBoolean(required(prefix + "firm")))
              .internalBooking(Boolean.parseBoolean(required(prefix + "internalBooking")))
              .replaceMessageCost(integer(prefix + "replaceMessageCost"));
      for (String combination : required(prefix + "combinations").split(",")) {
        String[] pair = combination.split(":");
        d.supports(OrderType.valueOf(pair[0]), TimeInForce.valueOf(pair[1]));
      }
      list.add(d.build());
    }
    return List.copyOf(list);
  }

  public String connector(int destination) {
    return required("destination." + destination + ".connector");
  }

  /** Connector secrets are not included in normalized engine journalling. */
  public Map<String, String> connectorSettings(int destination) {
    Map<String, String> settings = new TreeMap<>();
    String prefix = "connector." + destination + ".";
    p.stringPropertyNames().stream()
        .filter(k -> k.startsWith(prefix))
        .forEach(k -> settings.put(k.substring(prefix.length()), p.getProperty(k)));
    return Map.copyOf(settings);
  }
}
