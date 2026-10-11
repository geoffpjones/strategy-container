package io.strategycontainer.management;

import com.sun.net.httpserver.*;
import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.bootstrap.PluginRegistry;
import io.strategycontainer.core.*;
import io.strategycontainer.risk.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

/**
 * Loopback-first HTTP management. Mutations are form-encoded, authenticated, versioned and
 * journalled inputs.
 */
public final class ManagementServer implements AutoCloseable {
  public enum Role {
    VIEWER,
    TRADER,
    ADMIN
  }

  public record Principal(String name, Role role, Set<String> washBooks) {
    public Principal {
      washBooks = Set.copyOf(washBooks);
    }
  }

  private final HttpServer server;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final EngineRuntime runtime;
  private final PluginRegistry plugins;
  private final EventFeed feed;
  private final Map<String, Principal> tokens;
  private final Semaphore requests = new Semaphore(128);

  public ManagementServer(
      InetSocketAddress address,
      EngineRuntime runtime,
      PluginRegistry plugins,
      EventFeed feed,
      Map<String, Principal> tokens)
      throws IOException {
    if (tokens.isEmpty() || tokens.keySet().stream().anyMatch(t -> t.length() < 16))
      throw new IllegalArgumentException("Bearer tokens must be at least 16 characters");
    this.runtime = runtime;
    this.plugins = plugins;
    this.feed = feed;
    this.tokens = Map.copyOf(tokens);
    server = HttpServer.create(address, 64);
    server.setExecutor(executor);
    server.createContext("/", this::handle);
  }

  public void start() {
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private Principal authenticate(HttpExchange exchange) {
    String header = exchange.getRequestHeaders().getFirst("Authorization");
    if (header == null || !header.startsWith("Bearer ")) return null;
    byte[] actual = header.substring(7).getBytes(StandardCharsets.UTF_8);
    for (var entry : tokens.entrySet())
      if (MessageDigest.isEqual(actual, entry.getKey().getBytes(StandardCharsets.UTF_8)))
        return entry.getValue();
    return null;
  }

  private void handle(HttpExchange x) throws IOException {
    if (!requests.tryAcquire()) {
      respond(x, 503, Map.of("error", "Management capacity"));
      return;
    }
    try {
      String path = x.getRequestURI().getPath();
      if (path.equals("/") && x.getRequestMethod().equals("GET")) {
        byte[] body;
        try (var in = getClass().getResourceAsStream("/management.html")) {
          if (in == null) throw new IOException("Missing UI");
          body = in.readAllBytes();
        }
        x.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        x.getResponseHeaders()
            .set(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline';"
                    + " connect-src 'self'");
        x.sendResponseHeaders(200, body.length);
        x.getResponseBody().write(body);
        x.close();
        return;
      }
      Principal user = authenticate(x);
      if (user == null) {
        respond(x, 401, Map.of("error", "Bearer token required"));
        return;
      }
      if (x.getRequestMethod().equals("GET")) {
        switch (path) {
          case "/api/state" -> respond(x, 200, runtime.snapshot().get(2, TimeUnit.SECONDS));
          case "/api/health" ->
              respond(
                  x,
                  200,
                  Map.of(
                      "metrics",
                      runtime.metrics().text(),
                      "ingressRemaining",
                      runtime.remainingCapacity()));
          case "/api/strategies" -> respond(x, 200, plugins.names());
          case "/api/events" -> respond(x, 200, feed.recent());
          case "/api/stream" -> stream(x);
          default -> {
            if (path.startsWith("/api/strategies/")) {
              String name = path.substring("/api/strategies/".length());
              respond(
                  x,
                  200,
                  Map.of(
                      "name", name, "parameters", plugins.require(name).parameterDescriptions()));
            } else respond(x, 404, Map.of("error", "Unknown endpoint"));
          }
        }
        return;
      }
      if (!x.getRequestMethod().equals("POST")) {
        respond(x, 405, Map.of("error", "GET or POST required"));
        return;
      }
      if (user.role() == Role.VIEWER) {
        respond(x, 403, Map.of("error", "Trading role required"));
        return;
      }
      String key = x.getRequestHeaders().getFirst("Idempotency-Key");
      if (key == null || key.isBlank() || key.length() > 128)
        throw new IllegalArgumentException("Idempotency-Key required, maximum 128 characters");
      Map<String, String> form = form(x);
      EngineEvent event = command(path, form, user);
      event.actor = user.name();
      event.requestKey = user.name() + ":" + key;
      runtime.publish(event, 1000);
      respond(
          x,
          202,
          Map.of(
              "status",
              "queued",
              "requestKey",
              event.requestKey,
              "note",
              "Inspect streamed command outcomes; queued is not venue acknowledgement"));
    } catch (IllegalArgumentException ex) {
      respond(
          x, 400, Map.of("error", ex.getMessage() == null ? "Invalid request" : ex.getMessage()));
    } catch (Exception ex) {
      respond(x, 503, Map.of("error", ex.getClass().getSimpleName()));
    } finally {
      requests.release();
    }
  }

  private Map<String, String> form(HttpExchange x) throws IOException {
    String content = x.getRequestHeaders().getFirst("Content-Type");
    if (content == null || !content.startsWith("application/x-www-form-urlencoded"))
      throw new IllegalArgumentException("Form-encoded body required");
    byte[] bytes = x.getRequestBody().readNBytes(16385);
    if (bytes.length > 16384) throw new IllegalArgumentException("Body too large");
    Map<String, String> m = new HashMap<>();
    String body = new String(bytes, StandardCharsets.UTF_8);
    for (String field : body.split("&")) {
      if (field.isEmpty()) continue;
      String[] pair = field.split("=", 2);
      String k = URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
          v = pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
      if (m.putIfAbsent(k, v) != null) throw new IllegalArgumentException("Duplicate form field");
    }
    return m;
  }

  private static String required(Map<String, String> f, String key) {
    String v = f.get(key);
    if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing " + key);
    return v;
  }

  private static long number(Map<String, String> f, String key) {
    return Long.parseLong(required(f, key));
  }

  private static int integer(Map<String, String> f, String key) {
    return Math.toIntExact(number(f, key));
  }

  private EngineEvent command(String path, Map<String, String> f, Principal user) {
    EngineEvent e = new EngineEvent();
    String[] parts = path.split("/");
    if (path.equals("/api/parents")) {
      e.reset(EngineEvent.Kind.CREATE);
      e.data = parent(f);
      e.parentId = ((ParentSpec) e.data).id();
      return e;
    }
    if (parts.length == 5 && parts[2].equals("parents")) {
      e.parentId = Long.parseLong(parts[3]);
      EngineEvent.Kind kind =
          switch (parts[4]) {
            case "pause" -> EngineEvent.Kind.PAUSE;
            case "resume" -> EngineEvent.Kind.RESUME;
            case "restart" -> EngineEvent.Kind.RESTART;
            case "cancel" -> EngineEvent.Kind.CANCEL_PARENT;
            case "amend" -> EngineEvent.Kind.AMEND;
            case "preview" -> EngineEvent.Kind.PREVIEW;
            case "book" -> EngineEvent.Kind.CONFIRM_BOOK;
            case "submit" -> EngineEvent.Kind.SUBMIT;
            case "replace" -> EngineEvent.Kind.REPLACE;
            case "cancel-child" -> EngineEvent.Kind.CANCEL_CHILD;
            case "query" -> EngineEvent.Kind.QUERY;
            default -> throw new IllegalArgumentException("Unknown parent action");
          };
      long parent = e.parentId;
      e.reset(kind);
      e.parentId = parent;
      e.expectedVersion = number(f, "version");
      switch (kind) {
        case AMEND -> {
          e.a = number(f, "version");
          e.data = parent(f);
        }
        case PREVIEW -> {
          e.a = number(f, "version");
          e.destinationId = integer(f, "destination");
          e.key = required(f, "washBook");
          if (!user.washBooks().contains(e.key))
            throw new IllegalArgumentException("Unauthorized wash book");
          e.text = required(f, "reason");
        }
        case CONFIRM_BOOK -> {
          e.id = number(f, "preview");
          e.key = required(f, "bookingKey");
        }
        case SUBMIT -> {
          e.destinationId = integer(f, "destination");
          e.a = number(f, "quantity");
          e.b = number(f, "price");
          e.flags =
              OrderType.valueOf(f.getOrDefault("type", "LIMIT")).ordinal()
                  | (TimeInForce.valueOf(f.getOrDefault("tif", "IOC")).ordinal() << 8)
                  | (Boolean.parseBoolean(f.getOrDefault("postOnly", "false")) ? 1 << 16 : 0);
        }
        case REPLACE -> {
          e.childId = number(f, "child");
          e.a = number(f, "quantity");
          e.b = number(f, "price");
        }
        case CANCEL_CHILD, QUERY -> e.childId = number(f, "child");
        default -> {}
      }
      return e;
    }
    if (user.role() != Role.ADMIN)
      throw new IllegalArgumentException("Administrator role required");
    switch (path) {
      case "/api/instruments" -> {
        e.reset(EngineEvent.Kind.INSTRUMENT);
        e.data =
            Instrument.builder()
                .id(integer(f, "id"))
                .symbol(required(f, "symbol"))
                .quantityUnit(f.getOrDefault("unit", "units"))
                .priceScale(number(f, "scale"))
                .tick(number(f, "tick"))
                .lot(number(f, "lot"))
                .minimumQuantity(number(f, "minimum"))
                .build();
      }
      case "/api/destinations" -> {
        e.reset(EngineEvent.Kind.DESTINATION);
        Destination.Builder d =
            Destination.builder()
                .id(integer(f, "id"))
                .name(required(f, "name"))
                .instrumentId(integer(f, "instrument"))
                .sizes(number(f, "minimum"), number(f, "maximum"))
                .postOnly(Boolean.parseBoolean(f.getOrDefault("postOnly", "false")))
                .internalBooking(Boolean.parseBoolean(f.getOrDefault("internalBooking", "false")));
        for (String tif : required(f, "tifs").split(","))
          d.supports(OrderType.LIMIT, TimeInForce.valueOf(tif));
        e.data = d.build();
      }
      case "/api/policies" -> {
        e.reset(EngineEvent.Kind.POLICY);
        Limits l =
            Limits.builder()
                .maxParent(number(f, "maxParent"))
                .childSizes(number(f, "minChild"), number(f, "maxChild"))
                .maxWorking(number(f, "maxWorking"))
                .maxChildren(integer(f, "maxChildren"))
                .quoteAgeNanos(number(f, "quoteAgeNanos"))
                .collarTicks(number(f, "collarTicks"))
                .messages(integer(f, "rate"), integer(f, "burst"))
                .cancellations(integer(f, "cancelRate"), integer(f, "cancelBurst"))
                .rejects(
                    integer(f, "consecutiveRejects"),
                    integer(f, "minimumRejectSamples"),
                    integer(f, "rejectPercent"),
                    number(f, "rejectWindowNanos"))
                .build();
        e.data =
            new Policy(
                Scope.valueOf(required(f, "scope")),
                Long.parseLong(f.getOrDefault("key", "0")),
                f.getOrDefault("strategy", ""),
                l);
      }
      case "/api/session" -> {
        e.reset(EngineEvent.Kind.SESSION);
        e.destinationId = integer(f, "destination");
        e.a = Boolean.parseBoolean(required(f, "online")) ? 1 : 0;
      }
      case "/api/policies/reset" -> {
        e.reset(EngineEvent.Kind.RESET_POLICY);
        e.flags = Scope.valueOf(required(f, "scope")).ordinal();
        e.a = Long.parseLong(f.getOrDefault("key", "0"));
        e.text = f.getOrDefault("strategy", "");
      }
      case "/api/reconciled" -> {
        e.reset(EngineEvent.Kind.RECONCILIATION_COMPLETE);
        e.destinationId = integer(f, "destination");
      }
      case "/api/ready" -> e.reset(EngineEvent.Kind.READY);
      default -> throw new IllegalArgumentException("Unknown endpoint");
    }
    return e;
  }

  private ParentSpec parent(Map<String, String> f) {
    Map<String, String> params = new TreeMap<>();
    f.forEach(
        (k, v) -> {
          if (k.startsWith("param.")) params.put(k.substring(6), v);
        });
    return ParentSpec.builder()
        .id(number(f, "id"))
        .accountId(integer(f, "account"))
        .instrumentId(integer(f, "instrument"))
        .side(Side.valueOf(required(f, "side")))
        .quantity(number(f, "quantity"))
        .limit(number(f, "limit"))
        .window(number(f, "startNanos"), number(f, "endNanos"))
        .strategy(required(f, "strategy"))
        .parameters(params)
        .build();
  }

  private void stream(HttpExchange x) throws IOException, InterruptedException {
    try (EventFeed.Subscription subscription = feed.subscribe()) {
      x.getResponseHeaders().set("Content-Type", "text/event-stream");
      x.getResponseHeaders().set("Cache-Control", "no-store");
      x.sendResponseHeaders(200, 0);
      try (OutputStream out = x.getResponseBody()) {
        while (subscription.alive()) {
          Output o = subscription.poll();
          String line = o == null ? ": heartbeat\n\n" : "data: " + Json.write(o) + "\n\n";
          out.write(line.getBytes(StandardCharsets.UTF_8));
          out.flush();
        }
      }
    }
  }

  private static void respond(HttpExchange x, int status, Object body) throws IOException {
    byte[] bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
    x.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    x.getResponseHeaders().set("Cache-Control", "no-store");
    x.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = x.getResponseBody()) {
      out.write(bytes);
    }
  }

  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
