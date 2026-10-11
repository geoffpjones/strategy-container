package io.strategycontainer.core;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.bootstrap.PluginRegistry;
import io.strategycontainer.journal.Journal;
import io.strategycontainer.orders.*;
import io.strategycontainer.risk.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Deterministic single-writer state machine. Use directly only in offline replay; live access is
 * via EngineRuntime.
 */
public final class EngineCore {
  private final EngineConfig config;
  private final PluginRegistry plugins;
  private final Journal journal;
  private final OutputSink outputs, commands;
  public final EngineMetrics metrics = new EngineMetrics();
  private final Instrument[] instruments;
  private final Destination[] destinations;
  private final boolean[] online;
  private final boolean[] reconciled;
  private final QuoteState[][] quotes;
  private final int[][] subscriptionCounts;
  private final Context[] parents;
  private final CompositeReference[] referenceGroups;
  private int referenceCount;
  private long marketRevision;
  private final ChildOrder[] children;
  private ChildOrder firstGlobal, lastGlobal;
  private final PolicyState[] policies;
  private final PriorityQueue<Timer> timers =
      new PriorityQueue<>(
          Comparator.comparingLong(Timer::deadline).thenComparingLong(Timer::registration));
  private final Map<String, Trade> trades = new HashMap<>();
  private final Map<String, String> managementRequests = new HashMap<>();
  private final Set<String> operatorKeys = new HashSet<>();
  private final Map<Long, Preview> previews = new HashMap<>();
  private long sequence, now, utc, commandCounter, childCounter, timerCounter;
  private int parentCount, policyCount;
  private boolean ready, journalFailed;
  private boolean replay;
  private Thread owner;
  private final Map<Long, ExecutionCommand> pendingDispatch = new LinkedHashMap<>();
  private final System.Logger logger = System.getLogger(EngineCore.class.getName());

  private record Timer(long parent, long id, long deadline, long registration) {}

  private static final class Trade {
    long parent, child, quantity, price;
    boolean internal;

    Trade(long p, long c, long q, long px, boolean internal) {
      parent = p;
      child = c;
      quantity = q;
      price = px;
      this.internal = internal;
    }
  }

  public record Preview(
      long id,
      long parentId,
      long version,
      int destinationId,
      long quantity,
      long price,
      long observedNanos,
      String washBook,
      String reason) {}

  public record Snapshot(
      long sequence,
      long logicalNanos,
      boolean ready,
      List<ParentSnapshot> parents,
      List<ChildSnapshot> children,
      List<DestinationSnapshot> destinations,
      List<Policy> policies,
      String hash) {}

  public record ChildSnapshot(
      long id,
      long parentId,
      int destination,
      String state,
      long quantity,
      long filled,
      long price,
      long reserved,
      long pendingQuantity) {}

  public record DestinationSnapshot(Destination definition, boolean online) {}

  /**
   * Owned management view, including the immutable current specification for plan visualization.
   */
  public record ParentSnapshot(
      long id,
      String strategy,
      String state,
      long version,
      long quantity,
      long venueFilled,
      long washFilled,
      long reserved,
      int children,
      String diagnostic,
      String strategyState,
      InceptionBenchmark benchmark,
      ExecutionCosts executionCosts,
      ParentSpec specification) {}

  /**
   * Exact price-only diagnostics; monetary values are in instrument quote-price units times
   * quantity.
   */
  public record ExecutionCosts(
      java.math.BigDecimal averagePrice,
      java.math.BigDecimal venueShortfall,
      java.math.BigDecimal washShortfall,
      java.math.BigDecimal residualOpportunityCost,
      long residual,
      long reservedExposure,
      int referenceConstituents,
      boolean historyComplete) {}

  public EngineCore(
      EngineConfig config,
      PluginRegistry plugins,
      Journal journal,
      OutputSink outputs,
      OutputSink commands) {
    this.config = config;
    this.plugins = plugins;
    this.journal = journal;
    this.outputs = outputs;
    this.commands = commands;
    instruments = new Instrument[config.maxInstruments() + 1];
    destinations = new Destination[config.maxDestinations() + 1];
    online = new boolean[destinations.length];
    reconciled = new boolean[destinations.length];
    quotes = new QuoteState[instruments.length][destinations.length];
    subscriptionCounts = new int[instruments.length][destinations.length];
    parents = new Context[config.maxParents()];
    referenceGroups = new CompositeReference[config.maxParents()];
    children = new ChildOrder[config.maxChildren() + 1];
    policies = new PolicyState[config.maxPolicies()];
  }

  /**
   * Disable operational timing decisions in replay; deterministic strategy failures still produce
   * the same outputs.
   */
  public void replayMode() {
    replay = true;
  }

  public void liveMode() {
    replay = false;
  }

  public long sequence() {
    return sequence;
  }

  public void transferOwnership() {
    owner = null;
  }

  /** Process exactly one ordered input; callers must serialize all invocations. */
  public void process(long seq, EngineEvent e) {
    if (owner == null) owner = Thread.currentThread();
    if (owner != Thread.currentThread()) throw new IllegalStateException("Core thread ownership");
    if (seq != sequence + 1) throw new IllegalArgumentException("Non-contiguous input sequence");
    sequence = seq;
    if (!journal.input(seq, e)) {
      if (!journalFailed)
        logger.log(
            System.Logger.Level.ERROR,
            "Journal unavailable: blocking new exposure at input {0}",
            seq);
      journalFailed = true;
      ready = false;
    }
    try {
      boolean execute = true;
      if (!e.requestKey.isEmpty()) {
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(16384);
        io.strategycontainer.journal.BinaryCodec.input(buffer, 0, e);
        String signature =
            HexFormat.of()
                .formatHex(
                    MessageDigest.getInstance("SHA-256")
                        .digest(Arrays.copyOf(buffer.array(), buffer.position())));
        String old = managementRequests.get(e.requestKey);
        if (old != null) {
          emit(
              old.equals(signature) ? "DUPLICATE_REQUEST" : "IDEMPOTENCY_CONFLICT",
              e.parentId,
              e.childId,
              e.requestKey);
          execute = false;
        } else {
          if (managementRequests.size() >= config.maxChildren())
            throw new IllegalArgumentException("Idempotency capacity");
          managementRequests.put(e.requestKey, signature);
        }
      }
      if (execute && e.expectedVersion >= 0 && context(e.parentId).p.version != e.expectedVersion)
        throw new IllegalArgumentException("Stale parent version");
      if (execute && !e.requestKey.isEmpty())
        emit("OPERATOR_COMMAND", e.parentId, e.childId, e.requestKey);
      if (execute)
        switch (e.kind) {
          case TIME -> advance(e.a, e.b);
          case INSTRUMENT -> {
            Instrument i = (Instrument) e.data;
            if (i.id() >= instruments.length)
              throw new IllegalArgumentException("Instrument capacity");
            if (instruments[i.id()] != null)
              throw new IllegalArgumentException("Instrument already defined");
            instruments[i.id()] = i;
            for (Destination d : destinations)
              if (d != null && (d.instrumentId() == 0 || d.instrumentId() == i.id()))
                initializeQuote(i.id(), d.id());
            emit("INSTRUMENT", 0, 0, i.symbol());
          }
          case DESTINATION -> {
            Destination d = (Destination) e.data;
            if (d.id() >= destinations.length
                || d.instrumentId() != 0 && instrument(d.instrumentId()) == null)
              throw new IllegalArgumentException("Invalid destination");
            if (destinations[d.id()] != null)
              throw new IllegalArgumentException("Destination already defined");
            destinations[d.id()] = d;
            for (Instrument i : instruments)
              if (i != null && (d.instrumentId() == 0 || d.instrumentId() == i.id()))
                initializeQuote(i.id(), d.id());
            emit("DESTINATION", 0, 0, d.name());
          }
          case POLICY -> configurePolicy((Policy) e.data);
          case QUOTE -> price(e);
          case CREATE -> create((ParentSpec) e.data);
          case AMEND -> amend(e, (ParentSpec) e.data);
          case PAUSE -> pause(context(e.parentId), "operator pause", false);
          case CANCEL_PARENT -> pause(context(e.parentId), "operator cancel", true);
          case RESUME, RESTART -> resume(e);
          case SUBMIT -> {
            Context p = context(e.parentId);
            p.submit(
                e.destinationId,
                e.a,
                e.b,
                OrderType.values()[e.flags & 0xff],
                TimeInForce.values()[(e.flags >>> 8) & 0xff],
                (e.flags & (1 << 16)) != 0);
          }
          case REPLACE -> context(e.parentId).replace(e.childId, e.a, e.b);
          case CANCEL_CHILD -> context(e.parentId).cancel(e.childId);
          case QUERY -> context(e.parentId).query(e.childId);
          case REPORT -> report(e);
          case DISPATCH -> authorizeDispatch(e.id);
          case OVERRUN -> pause(context(e.parentId), "Callback latency limit exceeded", false);
          case RECOVER -> {
            ready = false;
            Arrays.fill(reconciled, false);
            pendingDispatch.clear();
            for (int i = 0; i < parentCount; i++)
              if (parents[i].p.state == ParentState.ACTIVE) {
                parents[i].p.state = ParentState.ERROR;
                parents[i].p.diagnostic = "Recovery requires OMS reconciliation";
              }
            for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
              if (c != null && !c.terminal()) c.state = ChildState.UNCERTAIN;
            emit("RECOVERY", 0, 0, "");
          }
          case SESSION -> session(e);
          case RECONCILIATION_COMPLETE -> {
            if (destination(e.destinationId) == null)
              throw new IllegalArgumentException("Unknown destination");
            for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
              if (c != null && c.destination == e.destinationId && c.state == ChildState.UNCERTAIN)
                throw new IllegalArgumentException("Unresolved child");
            reconciled[e.destinationId] = true;
            emit("RECONCILED_SESSION", 0, 0, Integer.toString(e.destinationId));
          }
          case RECOVER_CHILD -> recoverChild((RecoveredOrder) e.data);
          case RESET_POLICY -> resetPolicy(e);
          case READY -> {
            boolean valid = hasEnginePolicy() && journal.healthy() && !journalFailed;
            for (Destination d : destinations)
              if (d != null && online[d.id()] && !reconciled[d.id()]) valid = false;
            for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
              if (c != null
                  && (c.state == ChildState.UNCERTAIN
                      || c.state == ChildState.PENDING_NEW
                      || c.state == ChildState.PENDING_REPLACE
                      || c.state == ChildState.PENDING_CANCEL)) valid = false;
            for (int i = 0; i < parentCount; i++)
              if (parents[i].p.state == ParentState.BOOKING) valid = false;
            ready = valid;
            emit(valid ? "READY" : "UNRESOLVED", 0, 0, "");
          }
          case PREVIEW -> preview(e);
          case CONFIRM_BOOK -> book(e);
          case CHECKPOINT -> emit("CHECKPOINT", 0, 0, stateHash());
        }
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalStateException(ex);
    } catch (RuntimeException ex) {
      metrics.errors.increment();
      emit(
          "INPUT_ERROR",
          e.parentId,
          e.childId,
          ex.getClass().getSimpleName() + ":" + ex.getMessage());
    }
    if (!journal.complete(seq)) {
      journalFailed = true;
      ready = false;
    }
    metrics.processed++;
    metrics.sequence = sequence;
    metrics.logicalNanos = now;
    metrics.journalHealthy = journal.healthy() && !journalFailed;
    metrics.journalWritten = journal.writtenSequence();
    metrics.journalDurable = journal.durableSequence();
  }

  private void initializeQuote(int instrument, int destination) {
    QuoteState q = new QuoteState();
    q.instrument = instrument;
    q.destination = destination;
    quotes[instrument][destination] = q;
  }

  private boolean hasEnginePolicy() {
    for (int i = 0; i < policyCount; i++)
      if (policies[i].policy.scope() == Scope.ENGINE) return true;
    return false;
  }

  private void resetPolicy(EngineEvent e) {
    Scope scope = Scope.values()[e.flags];
    for (int i = 0; i < policyCount; i++) {
      PolicyState p = policies[i];
      if (p.policy.scope() == scope
          && p.policy.key() == e.a
          && p.policy.strategyName().equals(e.text)) {
        for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
          if (c != null
              && !c.terminal()
              && matches(p.policy, context(c.parentId).p, c.destination)
              && c.state == ChildState.UNCERTAIN)
            throw new IllegalArgumentException("Reconcile before breaker reset");
        p.reset();
        emit("BREAKER_RESET", 0, 0, scope + ":" + e.a);
        return;
      }
    }
    throw new IllegalArgumentException("Unknown policy");
  }

  private void recoverChild(RecoveredOrder r) {
    if (ready) throw new IllegalArgumentException("Recovery requires execution disabled");
    Destination d = destination(r.destinationId());
    if (d == null
        || instrument(r.parent().instrumentId()) == null
        || r.childId() >= children.length)
      throw new IllegalArgumentException("Recovery metadata/capacity");
    Context p = find(r.parent().id());
    if (p == null) {
      if (parentCount == parents.length) throw new IllegalArgumentException("Parent capacity");
      StrategyFactory factory = plugins.require(r.parent().strategy());
      factory.validate(r.parent());
      p = new Context(r.parent(), factory.create(r.parent()));
      p.recoveredHistoryIncomplete = r.cumulativeFilled() > 0;
      p.recoveryBenchmarkMissing = p.profile.requiresBenchmark() && r.benchmark() == null;
      if (r.benchmark() != null) validateRecoveredBenchmark(p, r.benchmark());
      p.benchmark = r.benchmark();
      p.p.state = ParentState.ERROR;
      p.p.diagnostic = "Recovered parent requires restart";
      parents[parentCount++] = p;
    }
    if (r.benchmark() != null) {
      validateRecoveredBenchmark(p, r.benchmark());
      if (p.benchmark != null && !p.benchmark.equals(r.benchmark()))
        throw new IllegalArgumentException("Conflicting inception benchmark");
      p.benchmark = r.benchmark();
      p.recoveryBenchmarkMissing = false;
    }
    ChildOrder c = child(r.childId());
    if (c != null && (c.parentId != p.p.spec.id() || c.destination != r.destinationId()))
      throw new IllegalArgumentException("Recovered child ownership");
    if (c == null) {
      c = new ChildOrder();
      c.id = r.childId();
      c.parentId = p.p.spec.id();
      c.destination = r.destinationId();
      children[(int) c.id] = c;
      insertGlobal(c);
      c.nextOwned = p.firstChild;
      p.firstChild = c;
      childCounter = Math.max(childCounter, c.id);
    } else {
      p.p.reserved -= c.reservation;
      if (!c.terminal()) p.p.children--;
    }
    if (r.cumulativeFilled() != c.filled) p.recoveredHistoryIncomplete = true;
    p.p.venue += r.cumulativeFilled() - c.filled;
    c.filled = r.cumulativeFilled();
    c.quantity = Math.addExact(c.filled, r.remainingQuantity());
    c.reservation = r.remainingQuantity();
    c.price = r.price();
    c.type = r.type();
    c.tif = r.tif();
    c.postOnly = r.postOnly();
    c.commandId = r.commandId();
    c.replaceQuantity = c.replacePrice = 0;
    c.state = c.reservation == 0 ? ChildState.CANCELLED : ChildState.WORKING;
    p.p.reserved += c.reservation;
    if (c.reservation > 0) p.p.children++;
    p.p.version++;
    commandCounter = Math.max(commandCounter, r.commandId());
    emit("RECOVERED_CHILD", p.p.spec.id(), c.id, Long.toString(c.reservation));
    checkBreach(p);
  }

  private void validateProfile(ParentSpec spec, ExecutionProfile profile) {
    Objects.requireNonNull(profile);
    for (int id : profile.referenceDestinations()) {
      Destination d = destination(id);
      if (d == null || d.instrumentId() != 0 && d.instrumentId() != spec.instrumentId())
        throw new IllegalArgumentException("Unknown/incompatible reference destination");
    }
    if (profile.passiveOnly()) {
      int id = Integer.parseInt(spec.parameters().getOrDefault("destination", "1"));
      Destination d = destination(id);
      if (d == null
          || !d.postOnly()
          || !d.supports(OrderType.LIMIT, TimeInForce.DAY)
              && !d.supports(OrderType.LIMIT, TimeInForce.GTC)
          || d.instrumentId() != 0 && d.instrumentId() != spec.instrumentId())
        throw new IllegalArgumentException(
            "Passive strategy requires a compatible post-only DAY/GTC destination");
    }
  }

  private CompositeReference referenceGroup(ParentSpec spec, ExecutionProfile profile) {
    if (!profile.requiresBenchmark()) return null;
    for (int i = 0; i < referenceCount; i++) {
      CompositeReference r = referenceGroups[i];
      if (r.instrument == spec.instrumentId()
          && r.profile.referenceAgeNanos() == profile.referenceAgeNanos()
          && r.profile.referenceDestinations().equals(profile.referenceDestinations())) return r;
    }
    if (referenceCount == referenceGroups.length)
      throw new IllegalArgumentException("Reference group capacity");
    CompositeReference r =
        new CompositeReference(
            spec.instrumentId(), profile, quotes[spec.instrumentId()], destinations, online);
    referenceGroups[referenceCount++] = r;
    return r;
  }

  private void captureBenchmark(Context p) {
    if (p.benchmark != null
        || p.reference == null
        || p.recoveryBenchmarkMissing
        || p.p.state != ParentState.ACTIVE
        || now < p.p.spec.startNanos()
        || now >= p.p.spec.endNanos()) return;
    p.reference.refresh(now, marketRevision);
    CompositeReference r = p.reference;
    if (!r.valid()) return;
    p.benchmark =
        new InceptionBenchmark(
            r.bid(),
            r.ask(),
            r.bidDestination(),
            r.askDestination(),
            r.bidObservedNanos(),
            r.askObservedNanos(),
            r.constituents(),
            sequence,
            now);
    emit("BENCHMARK_CAPTURED", p.p.spec.id(), 0, p.benchmark.toString());
  }

  private void validateRecoveredBenchmark(Context p, InceptionBenchmark b) {
    if (!p.profile.requiresBenchmark()
        || !p.profile.referenceDestinations().contains(b.bidDestination())
        || !p.profile.referenceDestinations().contains(b.askDestination())
        || b.constituents() > p.profile.referenceDestinations().size()
        || b.captureNanos() - b.bidObservedNanos() > p.profile.referenceAgeNanos()
        || b.captureNanos() - b.askObservedNanos() > p.profile.referenceAgeNanos())
      throw new IllegalArgumentException("Invalid original inception metadata");
  }

  private Views.OrderBounds effectiveBounds(Context p, int id) {
    EffectiveBounds b = p.effectiveBounds;
    Instrument i = instrument(p.p.spec.instrumentId());
    Destination d = destination(id);
    b.minimum =
        d == null ? i.minimumQuantity() : Math.max(i.minimumQuantity(), d.minimumQuantity());
    b.maximum = d == null ? 0 : d.maximumQuantity();
    b.available = Math.max(0, p.p.residual() - p.p.reserved);
    b.slots = (int) Math.max(0, children.length - 1 - childCounter);
    b.replaceNanos = 0;
    b.eligible =
        d != null
            && (d.instrumentId() == 0 || d.instrumentId() == i.id())
            && online[id]
            && ready
            && !journalFailed
            && p.p.state == ParentState.ACTIVE
            && now >= p.p.spec.startNanos()
            && now < p.p.spec.endNanos()
            && (!p.profile.requiresBenchmark() || p.benchmark != null);
    for (int n = 0; n < policyCount; n++) {
      PolicyState state = policies[n];
      if (!matches(state.policy, p.p, id)) continue;
      Limits l = state.policy.limits();
      b.minimum = Math.max(b.minimum, l.minChild());
      b.maximum = Math.min(b.maximum, l.maxChild());
      b.available = Math.min(b.available, Math.max(0, l.maxWorking() - working(state.policy)));
      b.slots = Math.min(b.slots, Math.max(0, l.maxChildren() - workingCount(state.policy)));
      b.replaceNanos = Math.max(b.replaceNanos, l.minimumReplaceNanos());
      b.eligible &= !state.tripped;
    }
    if (b.eligible) b.eligible = fresh(p, quotes[p.p.spec.instrumentId()][id], id);
    if (b.eligible && p.reference != null) {
      p.reference.refresh(now, marketRevision);
      b.eligible = p.reference.valid();
    }
    long remainder = b.minimum % i.lot();
    if (remainder != 0) b.minimum = Math.addExact(b.minimum, i.lot() - remainder);
    b.maximum -= b.maximum % i.lot();
    b.available -= b.available % i.lot();
    b.eligible &= b.maximum >= b.minimum;
    return b;
  }

  private String strategyState(Context p) {
    if (p.recoveryBenchmarkMissing) return "BENCHMARK_RECOVERY_BLOCKED";
    if (p.profile.requiresBenchmark() && p.benchmark == null && p.p.state == ParentState.ACTIVE)
      return "BENCHMARK_PENDING";
    if (now >= p.p.spec.endNanos() && p.p.residual() > 0) return "EXPIRED_INCOMPLETE";
    return p.p.state == ParentState.ACTIVE && !p.strategyStatus.isEmpty()
        ? p.strategyStatus
        : p.p.state.name();
  }

  private ExecutionCosts costs(Context p) {
    java.math.BigDecimal venue = java.math.BigDecimal.ZERO, wash = java.math.BigDecimal.ZERO;
    java.math.BigDecimal venueQuantity = java.math.BigDecimal.ZERO,
        washQuantity = java.math.BigDecimal.ZERO;
    for (Trade t : trades.values())
      if (t.parent == p.p.spec.id()) {
        java.math.BigDecimal q = java.math.BigDecimal.valueOf(t.quantity),
            value = q.multiply(java.math.BigDecimal.valueOf(t.price));
        if (t.internal) {
          wash = wash.add(value);
          washQuantity = washQuantity.add(q);
        } else {
          venue = venue.add(value);
          venueQuantity = venueQuantity.add(q);
        }
      }
    boolean complete = !p.recoveredHistoryIncomplete;
    java.math.BigDecimal multiplier =
        java.math.BigDecimal.valueOf(instrument(p.p.spec.instrumentId()).multiplier());
    java.math.BigDecimal scale =
        java.math.BigDecimal.valueOf(instrument(p.p.spec.instrumentId()).priceScale());
    java.math.BigDecimal quantity = venueQuantity.add(washQuantity);
    java.math.BigDecimal avg =
        complete && quantity.signum() > 0
            ? venue.add(wash).divide(quantity.multiply(scale), 12, java.math.RoundingMode.HALF_EVEN)
            : null;
    java.math.BigDecimal vs = null, ws = null, opportunity = null;
    int constituents = 0;
    if (p.reference != null) {
      p.reference.refresh(now, marketRevision);
      constituents = p.reference.constituents();
    }
    if (p.benchmark != null) {
      java.math.BigDecimal mid =
          java.math.BigDecimal.valueOf(p.benchmark.bid())
              .add(java.math.BigDecimal.valueOf(p.benchmark.ask()))
              .divide(java.math.BigDecimal.valueOf(2));
      int sign = p.p.spec.side() == Side.BUY ? 1 : -1;
      if (complete) {
        vs =
            venue
                .subtract(venueQuantity.multiply(mid))
                .multiply(java.math.BigDecimal.valueOf(sign))
                .multiply(multiplier)
                .divide(scale, 12, java.math.RoundingMode.HALF_EVEN);
        ws =
            wash.subtract(washQuantity.multiply(mid))
                .multiply(java.math.BigDecimal.valueOf(sign))
                .multiply(multiplier)
                .divide(scale, 12, java.math.RoundingMode.HALF_EVEN);
      }
      long bid =
          p.finalMark
              ? p.expiryBid
              : p.reference != null && p.reference.valid() ? p.reference.bid() : 0;
      long ask =
          p.finalMark
              ? p.expiryAsk
              : p.reference != null && p.reference.valid() ? p.reference.ask() : 0;
      if (bid > 0 && ask > bid)
        opportunity =
            java.math.BigDecimal.valueOf(bid)
                .add(java.math.BigDecimal.valueOf(ask))
                .divide(java.math.BigDecimal.valueOf(2))
                .subtract(mid)
                .multiply(java.math.BigDecimal.valueOf(sign))
                .multiply(java.math.BigDecimal.valueOf(Math.max(0, p.p.residual())))
                .multiply(multiplier)
                .divide(scale, 12, java.math.RoundingMode.HALF_EVEN);
    }
    return new ExecutionCosts(
        avg, vs, ws, opportunity, p.p.residual(), p.p.reserved, constituents, complete);
  }

  private Instrument instrument(int id) {
    return id > 0 && id < instruments.length ? instruments[id] : null;
  }

  private Destination destination(int id) {
    return id > 0 && id < destinations.length ? destinations[id] : null;
  }

  private Context context(long id) {
    for (int i = 0; i < parentCount; i++) if (parents[i].p.spec.id() == id) return parents[i];
    throw new IllegalArgumentException("Unknown parent " + id);
  }

  private Context find(long id) {
    for (int i = 0; i < parentCount; i++) if (parents[i].p.spec.id() == id) return parents[i];
    return null;
  }

  /** Maintain deterministic ID iteration without scanning unused configured capacity. */
  private void insertGlobal(ChildOrder c) {
    if (firstGlobal == null) {
      firstGlobal = lastGlobal = c;
      return;
    }
    if (c.id > lastGlobal.id) {
      lastGlobal.nextGlobal = c;
      lastGlobal = c;
      return;
    }
    if (c.id < firstGlobal.id) {
      c.nextGlobal = firstGlobal;
      firstGlobal = c;
      return;
    }
    ChildOrder previous = firstGlobal;
    while (previous.nextGlobal != null && previous.nextGlobal.id < c.id)
      previous = previous.nextGlobal;
    c.nextGlobal = previous.nextGlobal;
    previous.nextGlobal = c;
  }

  private ChildOrder child(long id) {
    return id > 0 && id < children.length ? children[(int) id] : null;
  }

  private void emit(String type, long parent, long child, String detail) {
    Output o = new Output(sequence, type, parent, child, detail, null);
    if (!journal.output(o)) {
      journalFailed = true;
      ready = false;
    }
    outputs.accept(o);
    if (!replay
        && (type.equals("BREACH")
            || type.equals("STRATEGY_ERROR")
            || type.equals("INPUT_ERROR")
            || type.equals("BOOK_UNCERTAIN")))
      logger.log(
          System.Logger.Level.WARNING,
          "event={0} sequence={1} logicalNanos={2} parent={3} child={4} detail={5}",
          type,
          sequence,
          now,
          parent,
          child,
          detail);
  }

  private void dispatch(ExecutionCommand command) {
    Output o = new Output(sequence, "COMMAND", command.parentId(), command.childId(), "", command);
    if (!journal.output(o)) {
      journalFailed = true;
      ready = false;
    }
    outputs.accept(o);
    metrics.commands.increment();
    pendingDispatch.put(command.id(), command);
    commands.accept(o);
  }

  private void authorizeDispatch(long id) {
    ExecutionCommand c = pendingDispatch.remove(id);
    if (c == null) {
      emit("ROUTE_SKIP", 0, 0, Long.toString(id));
      return;
    }
    Context p = context(c.parentId());
    boolean approve = true;
    if (c.type() == CommandType.NEW || c.type() == CommandType.REPLACE) {
      ChildOrder order = child(c.childId());
      approve =
          order != null
              && !order.terminal()
              && order.state != ChildState.PENDING_CANCEL
              && constraints(
                      p,
                      c.destinationId(),
                      c.quantity(),
                      c.price(),
                      c.orderType(),
                      c.timeInForce(),
                      c.postOnly(),
                      order,
                      true)
                  == Outcome.ACCEPTED;
      if (!approve && order != null && !order.terminal()) {
        if (c.type() == CommandType.NEW) terminal(p, order, ChildState.REJECTED);
        else {
          long leaves = Math.max(0, order.quantity - order.filled);
          p.p.reserved += leaves - order.reservation;
          order.reservation = leaves;
          order.replaceQuantity = order.replacePrice = 0;
          if (order.state != ChildState.PENDING_CANCEL) order.state = ChildState.WORKING;
        }
      }
    } else if (c.type() == CommandType.CANCEL) {
      ChildOrder order = child(c.childId());
      approve = order != null && !order.terminal();
    } else if (c.type() == CommandType.BOOK) {
      QuoteState q = quotes[p.p.spec.instrumentId()][c.destinationId()];
      approve =
          ready
              && journal.healthy()
              && fresh(p, q, c.destinationId())
              && c.price() == (p.p.spec.side() == Side.BUY ? q.ask : q.bid)
              && !badLimit(p.p.spec.side(), c.price(), p.p.spec.limit());
      if (!approve) {
        p.p.reserved = 0;
        p.p.state = ParentState.ERROR;
        p.p.diagnostic = "Internal booking failed final gate";
      }
    }
    Output out =
        new Output(
            sequence,
            approve ? "ROUTE" : "ROUTE_SKIP",
            c.parentId(),
            c.childId(),
            Long.toString(id),
            approve ? c : null);
    if (!journal.output(out)) {
      journalFailed = true;
      ready = false;
      if (c.type() == CommandType.NEW
          || c.type() == CommandType.REPLACE
          || c.type() == CommandType.BOOK) {
        approve = false;
        out =
            new Output(sequence, "ROUTE_SKIP", c.parentId(), c.childId(), Long.toString(id), null);
      } else
        logger.log(
            System.Logger.Level.WARNING,
            "Emergency risk-reducing dispatch without healthy journal: command {0}",
            id);
    }
    outputs.accept(out);
    commands.accept(out);
  }

  private boolean matches(Policy policy, ParentOrder p, int destination) {
    return switch (policy.scope()) {
      case ENGINE -> true;
      case ACCOUNT -> policy.key() == p.spec.accountId();
      case PARENT -> policy.key() == p.spec.id();
      case STRATEGY -> policy.strategyName().equals(p.spec.strategy());
      case INSTRUMENT -> policy.key() == p.spec.instrumentId();
      case DESTINATION -> policy.key() == destination;
    };
  }

  private void configurePolicy(Policy policy) {
    int index = -1;
    for (int i = 0; i < policyCount; i++) {
      Policy old = policies[i].policy;
      if (old.scope() == policy.scope()
          && old.key() == policy.key()
          && old.strategyName().equals(policy.strategyName())) index = i;
    }
    if (index < 0) {
      if (policyCount == policies.length) throw new IllegalArgumentException("Policy capacity");
      index = policyCount++;
    }
    policies[index] = new PolicyState(policy, now);
    emit("POLICY", 0, 0, policy.scope() + ":" + policy.key());
    for (int i = 0; i < parentCount; i++) {
      Context p = parents[i];
      for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
        if (c != null
            && c.parentId == p.p.spec.id()
            && !c.terminal()
            && constraints(
                    p, c.destination, c.reservation, c.price, c.type, c.tif, c.postOnly, c, false)
                != Outcome.ACCEPTED) p.cancel(c.id);
    }
  }

  private void create(ParentSpec spec) {
    if (find(spec.id()) != null) throw new IllegalArgumentException("Duplicate parent");
    Instrument i = instrument(spec.instrumentId());
    if (i == null
        || spec.quantity() % i.lot() != 0
        || spec.limit() % i.tick() != 0
        || !hasEnginePolicy())
      throw new IllegalArgumentException("Instrument/lot/limit/profile invalid");
    if (parentCount == parents.length) throw new IllegalArgumentException("Parent capacity");
    for (int n = 0; n < policyCount; n++) {
      Policy policy = policies[n].policy;
      if (policy.scope() == Scope.DESTINATION) continue;
      ParentOrder candidate = new ParentOrder();
      candidate.spec = spec;
      if (matches(policy, candidate, 0) && spec.quantity() > policy.limits().maxParent())
        throw new IllegalArgumentException("Parent size limit");
    }
    StrategyFactory factory = plugins.require(spec.strategy());
    factory.validate(spec);
    Context p = new Context(spec, factory.create(spec));
    parents[parentCount++] = p;
    captureBenchmark(p);
    emit("PARENT_CREATED", spec.id(), 0, spec.strategy());
    callback(p, 0, null, null);
  }

  private void amend(EngineEvent e, ParentSpec spec) {
    Context p = context(e.parentId);
    if (e.a != p.p.version) throw new IllegalArgumentException("Stale parent version");
    if (spec.id() != p.p.spec.id()
        || spec.instrumentId() != p.p.spec.instrumentId()
        || spec.side() != p.p.spec.side()
        || spec.accountId() != p.p.spec.accountId()
        || !spec.strategy().equals(p.p.spec.strategy()))
      throw new IllegalArgumentException("Immutable parent identity");
    if (p.p.state == ParentState.FILLED
        || p.p.state == ParentState.CANCELLED
        || p.p.state == ParentState.BOOKING)
      throw new IllegalArgumentException("Terminal/booking parent");
    if (spec.quantity() < p.p.venue + p.p.wash)
      throw new IllegalArgumentException("Quantity below fills");
    if (spec.quantity() < p.p.venue + p.p.wash + p.p.reserved) {
      pause(p, "Drain exposure before quantity reduction", false);
      emit("AMEND_DEFERRED", e.parentId, 0, "");
      return;
    }
    Instrument i = instrument(spec.instrumentId());
    if (spec.quantity() % i.lot() != 0 || spec.limit() % i.tick() != 0)
      throw new IllegalArgumentException("Invalid increments");
    for (int n = 0; n < policyCount; n++)
      if (matches(policies[n].policy, p.p, 0)
          && spec.quantity() > policies[n].policy.limits().maxParent())
        throw new IllegalArgumentException("Parent size limit");
    plugins.require(spec.strategy()).validate(spec);
    ExecutionProfile nextProfile = plugins.require(spec.strategy()).executionProfile(spec);
    validateProfile(spec, nextProfile);
    if (p.benchmark != null
        && (!p.profile.referenceDestinations().equals(nextProfile.referenceDestinations())
            || p.profile.referenceAgeNanos() != nextProfile.referenceAgeNanos()))
      throw new IllegalArgumentException("Inception reference is immutable after capture");
    if (p.profile.passiveOnly() != nextProfile.passiveOnly())
      throw new IllegalArgumentException("Execution permission is immutable");
    p.profile = nextProfile;
    p.reference = referenceGroup(spec, nextProfile);
    p.p.spec = spec;
    p.finalMark = false;
    p.expiryBid = p.expiryAsk = 0;
    p.p.version++;
    emit("PARENT_AMENDED", e.parentId, 0, "");
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null
          && c.parentId == spec.id()
          && !c.terminal()
          && badLimit(spec.side(), c.price, spec.limit())) p.cancel(c.id);
    callback(p, 3, null, null);
  }

  private void resume(EngineEvent e) {
    Context p = context(e.parentId);
    if (p.p.state != ParentState.PAUSED && p.p.state != ParentState.ERROR)
      throw new IllegalArgumentException("Parent is not paused");
    if (p.recoveryBenchmarkMissing)
      throw new IllegalArgumentException("Original inception benchmark metadata required");
    if (!ready || p.p.reserved != 0 || p.p.children != 0 || now >= p.p.spec.endNanos())
      throw new IllegalArgumentException("Reconciliation/deadline prevents resume");
    for (int i = 0; i < policyCount; i++)
      if (policies[i].policy.scope() == Scope.PARENT && matches(policies[i].policy, p.p, 0))
        policies[i].reset();
    p.p.localFailures = p.p.venueRejects = 0;
    p.p.state = ParentState.ACTIVE;
    p.p.version++;
    captureBenchmark(p);
    emit("RESUMED", e.parentId, 0, "");
    if (e.kind == EngineEvent.Kind.RESTART || p.stopped) {
      stopStrategy(p);
      p.stopped = false;
      p.strategy = plugins.require(p.p.spec.strategy()).create(p.p.spec);
      callback(p, 0, null, null);
    } else callback(p, 3, null, null);
  }

  private void pause(Context p, String reason, boolean cancel) {
    if (p.p.state == ParentState.FILLED || p.p.state == ParentState.CANCELLED) return;
    p.p.state = cancel ? ParentState.CANCELLING : ParentState.PAUSED;
    p.p.version++;
    p.p.diagnostic = reason;
    timers.removeIf(t -> t.parent() == p.p.spec.id());
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null && c.parentId == p.p.spec.id() && !c.terminal()) p.cancel(c.id);
    settle(p);
    emit("PAUSED", p.p.spec.id(), 0, reason);
  }

  private void settle(Context p) {
    if (p.p.residual() == 0 && p.p.reserved == 0 && p.p.state != ParentState.ERROR) {
      p.p.state = ParentState.FILLED;
      timers.removeIf(t -> t.parent() == p.p.spec.id());
    } else if (p.p.state == ParentState.CANCELLING && p.p.children == 0 && p.p.reserved == 0)
      p.p.state = ParentState.CANCELLED;
    if (p.p.state == ParentState.FILLED || p.p.state == ParentState.CANCELLED) stopStrategy(p);
  }

  private void stopStrategy(Context p) {
    if (p.stopped) return;
    p.stopped = true;
    callback(p, 5, null, null);
    for (int id = 1; id < p.subscriptions.length; id++) p.unsubscribe(id);
  }

  private void advance(long nanos, long utcMillis) {
    if (nanos < now) throw new IllegalArgumentException("Logical clock regression");
    now = nanos;
    utc = utcMillis;
    for (int i = 0; i < parentCount; i++) {
      Context p = parents[i];
      if (p.p.state == ParentState.ACTIVE && now >= p.p.spec.endNanos()) {
        p.finalMark = true;
        if (p.reference != null) {
          p.reference.refresh(now, marketRevision);
          if (p.reference.valid()) {
            p.expiryBid = p.reference.bid();
            p.expiryAsk = p.reference.ask();
          }
        }
        pause(p, "deadline", true);
      } else if (p.p.state == ParentState.ACTIVE) {
        captureBenchmark(p);
        if (p.profile.requiresBenchmark()
            && p.benchmark == null
            && now - Math.max(p.acceptedNanos, p.p.spec.startNanos())
                >= p.profile.benchmarkWaitNanos()) pause(p, "Missing inception benchmark", false);
      }
    }
    // Throttled or rejected cancellations retain exposure and retry as logical time replenishes
    // budget.
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal) {
      if (c == null
          || c.terminal()
          || c.state == ChildState.PENDING_CANCEL
          || c.state == ChildState.UNCERTAIN) continue;
      Context p = context(c.parentId);
      if (p.p.state == ParentState.PAUSED
          || p.p.state == ParentState.CANCELLING
          || p.p.state == ParentState.ERROR) p.cancel(c.id);
    }
    int fired = 0;
    while (!timers.isEmpty() && timers.peek().deadline() <= now) {
      if (++fired > config.maxTimers()) throw new IllegalStateException("Timer callback runaway");
      Timer t = timers.remove();
      Context p = find(t.parent());
      if (p != null && p.p.state == ParentState.ACTIVE) callback(p, 2, null, t.id());
    }
  }

  private void price(EngineEvent e) {
    if (e.instrumentId <= 0
        || e.instrumentId >= quotes.length
        || e.destinationId <= 0
        || e.destinationId >= destinations.length) throw new IllegalArgumentException("Quote IDs");
    QuoteState q = quotes[e.instrumentId][e.destinationId];
    if (q == null) throw new IllegalArgumentException("Unknown quote source");
    q.bid = e.a;
    q.ask = e.b;
    q.bidSize = e.c;
    q.askSize = e.d;
    q.observed = e.e;
    q.valid = e.a > 0 && e.b >= e.a && e.c >= 0 && e.d >= 0 && e.e >= 0 && e.e <= now;
    marketRevision++;
    for (int i = 0; i < parentCount; i++) {
      Context p = parents[i];
      if (p.p.state == ParentState.ACTIVE && p.p.spec.instrumentId() == e.instrumentId)
        captureBenchmark(p);
      if (p.p.state == ParentState.ACTIVE
          && p.p.spec.instrumentId() == e.instrumentId
          && p.subscriptions[e.destinationId]) callback(p, 1, q, null);
    }
  }

  private void session(EngineEvent e) {
    Destination d = destination(e.destinationId);
    if (d == null) throw new IllegalArgumentException("Unknown destination");
    online[d.id()] = e.a != 0;
    marketRevision++;
    reconciled[d.id()] = false;
    emit("SESSION", 0, 0, d.name() + ":" + online[d.id()]);
    if (!online[d.id()]) {
      for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
        if (c != null && c.destination == d.id() && !c.terminal()) {
          c.state = ChildState.UNCERTAIN;
          Context p = context(c.parentId);
          p.p.state = ParentState.ERROR;
          p.p.diagnostic = "Destination disconnected";
        }
    }
  }

  private boolean badLimit(Side side, long price, long limit) {
    return side == Side.BUY ? price > limit : price < limit;
  }

  private long working(Policy policy) {
    long total = 0;
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null && !c.terminal()) {
        Context p = context(c.parentId);
        if (matches(policy, p.p, c.destination)) total = Math.addExact(total, c.reservation);
      }
    return total;
  }

  private int workingCount(Policy policy) {
    int n = 0;
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null && !c.terminal() && matches(policy, context(c.parentId).p, c.destination)) n++;
    return n;
  }

  private Outcome constraints(
      Context context,
      int destinationId,
      long quantity,
      long price,
      OrderType type,
      TimeInForce tif,
      boolean postOnly,
      ChildOrder replace,
      boolean lifecycle) {
    ParentOrder p = context.p;
    Destination d = destination(destinationId);
    Instrument instrument = instrument(p.spec.instrumentId());
    if (lifecycle
        && (!ready
            || journalFailed
            || !journal.healthy()
            || p.state != ParentState.ACTIVE
            || now < p.spec.startNanos()
            || now >= p.spec.endNanos())) return Outcome.LIFECYCLE;
    if (d == null
        || d.instrumentId() != 0 && d.instrumentId() != p.spec.instrumentId()
        || !online[d.id()]) return Outcome.DESTINATION;
    if (context.profile.requiresBenchmark()) {
      if (context.benchmark == null) return Outcome.UNRESOLVED;
      context.reference.refresh(now, marketRevision);
      if (!context.reference.valid()) return Outcome.STALE;
    }
    if (context.profile.passiveOnly()
        && (!postOnly
            || type != OrderType.LIMIT
            || (tif != TimeInForce.DAY && tif != TimeInForce.GTC)
            || !d.postOnly())) return Outcome.UNSUPPORTED;
    if (!d.supports(type, tif)
        || postOnly && !d.postOnly()
        || type == OrderType.PEGGED && !d.protectedPeg()
        || replace != null && replace.state == ChildState.WORKING && !d.atomicReplace())
      return Outcome.UNSUPPORTED;
    if (quantity <= 0
        || quantity < instrument.minimumQuantity()
        || quantity < d.minimumQuantity()
        || quantity > d.maximumQuantity()
        || quantity % instrument.lot() != 0) return Outcome.SIZE;
    if (price <= 0 || price % instrument.tick() != 0) return Outcome.PRICE;
    if (badLimit(p.spec.side(), price, p.spec.limit())) return Outcome.LIMIT;
    long old = replace == null ? 0 : replace.reservation;
    long reservation =
        replace == null
            ? quantity
            : Math.max(quantity, Math.max(0, replace.quantity - replace.filled));
    long extra = reservation - old;
    if (extra > p.residual() - p.reserved) return Outcome.EXPOSURE;
    QuoteState q = quotes[p.spec.instrumentId()][d.id()];
    if (q == null || !q.valid || q.observed > now) return Outcome.STALE;
    if (context.profile.passiveOnly()
        && (p.spec.side() == Side.BUY ? price >= q.ask : price <= q.bid)) return Outcome.PRICE;
    for (int i = 0; i < policyCount; i++) {
      PolicyState state = policies[i];
      if (!matches(state.policy, p, d.id())) continue;
      Limits l = state.policy.limits();
      if (state.tripped && lifecycle) return Outcome.DESTINATION;
      if (quantity < l.minChild() || quantity > l.maxChild()) return Outcome.SIZE;
      if (working(state.policy) + extra > l.maxWorking()
          || workingCount(state.policy) + (replace == null ? 1 : 0) > l.maxChildren())
        return Outcome.EXPOSURE;
      if (now - q.observed > l.quoteAgeNanos()) return Outcome.STALE;
      long reference = p.spec.side() == Side.BUY ? q.ask : q.bid;
      long diff = price >= reference ? price - reference : reference - price;
      if (diff / instrument.tick() > l.collarTicks()) return Outcome.PRICE;
      if (l.maxNotional() != Long.MAX_VALUE) {
        try {
          long numerator =
              Math.multiplyExact(Math.multiplyExact(quantity, price), instrument.multiplier());
          long limit = Math.multiplyExact(l.maxNotional(), instrument.priceScale());
          if (numerator > limit) return Outcome.SIZE;
        } catch (ArithmeticException ex) {
          return Outcome.SIZE;
        }
      }
      if (replace != null
          && replace.state == ChildState.WORKING
          && now - replace.lastReplace < l.minimumReplaceNanos()) return Outcome.THROTTLED;
    }
    return Outcome.ACCEPTED;
  }

  private boolean budget(Context p, int dest, int cost, boolean cancel) {
    for (int i = 0; i < policyCount; i++) {
      PolicyState s = policies[i];
      if (matches(s.policy, p.p, dest)
          && !(cancel ? s.cancellations : s.messages).available(now, cost)) return false;
    }
    for (int i = 0; i < policyCount; i++) {
      PolicyState s = policies[i];
      if (matches(s.policy, p.p, dest)) (cancel ? s.cancellations : s.messages).consume(cost);
    }
    return true;
  }

  private StrategyContext.Result rejected(Context p, long child, Outcome outcome) {
    emit("CONSTRAINT", p.p.spec.id(), child, outcome.name());
    if (outcome != Outcome.THROTTLED && outcome != Outcome.LIFECYCLE) {
      p.p.localFailures++;
      int threshold = Integer.MAX_VALUE;
      for (int i = 0; i < policyCount; i++)
        if (matches(policies[i].policy, p.p, 0))
          threshold = Math.min(threshold, policies[i].policy.limits().consecutiveRejects());
      if (p.p.localFailures >= threshold) pause(p, "Local request failure breaker", false);
    }
    return new StrategyContext.Result(outcome, 0, child);
  }

  private void terminal(Context p, ChildOrder c, ChildState state) {
    p.p.reserved -= c.reservation;
    c.reservation = 0;
    if (!c.terminal()) p.p.children--;
    c.state = state;
    settle(p);
  }

  private void report(EngineEvent e) {
    ReportType type = ReportType.values()[e.flags];
    if (type == ReportType.BOOKED || type == ReportType.BOOK_REJECTED) {
      bookingReport(e, type);
      return;
    }
    if (type == ReportType.UNCERTAIN && e.childId == 0) {
      Context p = context(e.parentId);
      p.p.diagnostic = "Internal booking outcome uncertain; reconcile";
      emit("BOOK_UNCERTAIN", e.parentId, 0, e.text);
      return;
    }
    ChildOrder c = child(e.childId);
    if (c == null || c.destination != e.destinationId)
      throw new IllegalArgumentException("Report ownership");
    Context p = context(c.parentId);
    switch (type) {
      case ACK -> {
        if (c.state == ChildState.PENDING_NEW) {
          c.state = ChildState.WORKING;
          venueOutcome(p, c, false, e);
        }
      }
      case FILL -> {
        if (e.a <= 0 || e.b <= 0 || e.text.isBlank())
          throw new IllegalArgumentException("Invalid fill");
        String tradeKey = c.destination + ":" + e.text;
        if (trades.containsKey(tradeKey)) {
          emit("DUPLICATE_FILL", c.parentId, c.id, e.text);
          return;
        }
        trades.put(tradeKey, new Trade(c.parentId, c.id, e.a, e.b, false));
        applyFill(p, c, e.a, e.b);
        if (p.profile.passiveOnly() && e.liquidityRole == LiquidityRole.TAKER.ordinal()) {
          p.p.state = ParentState.ERROR;
          p.p.diagnostic = "Taker fill violated passive-only contract";
          for (ChildOrder other = p.firstChild; other != null; other = other.nextOwned)
            if (!other.terminal()) p.cancel(other.id);
          emit("BREACH", c.parentId, c.id, p.p.diagnostic);
        }
      }
      case CANCELLED -> terminal(p, c, ChildState.CANCELLED);
      case REJECTED -> {
        if (c.state != ChildState.PENDING_NEW && c.state != ChildState.UNCERTAIN)
          throw new IllegalArgumentException("Unexpected new rejection");
        terminal(p, c, ChildState.REJECTED);
        venueOutcome(p, c, true, e);
      }
      case CANCEL_REJECTED -> {
        c.state = ChildState.UNCERTAIN;
        p.p.state = ParentState.ERROR;
        p.p.diagnostic = "Cancel rejected; reconcile";
        venueOutcome(p, c, true, e);
      }
      case REPLACE_REJECTED -> {
        if (c.state != ChildState.PENDING_REPLACE && c.state != ChildState.PENDING_CANCEL)
          throw new IllegalArgumentException("No pending replace");
        long leaves = Math.max(0, c.quantity - c.filled);
        p.p.reserved += leaves - c.reservation;
        c.reservation = leaves;
        c.replaceQuantity = c.replacePrice = 0;
        if (c.state != ChildState.PENDING_CANCEL) c.state = ChildState.WORKING;
        venueOutcome(p, c, true, e);
      }
      case REPLACED -> {
        if (c.replacePrice == 0) throw new IllegalArgumentException("No pending replace");
        c.quantity = c.replaceQuantity;
        c.price = c.replacePrice;
        c.replaceQuantity = c.replacePrice = 0;
        long leaves = Math.max(0, c.quantity - c.filled);
        p.p.reserved += leaves - c.reservation;
        c.reservation = leaves;
        if (leaves == 0) terminal(p, c, ChildState.FILLED);
        else if (c.state != ChildState.PENDING_CANCEL) c.state = ChildState.WORKING;
        venueOutcome(p, c, false, e);
      }
      case UNCERTAIN -> {
        c.state = ChildState.UNCERTAIN;
        p.p.state = ParentState.ERROR;
        p.p.diagnostic = "Unknown child outcome";
      }
      case RECONCILED -> {
        if (e.a < 0) throw new IllegalArgumentException("Invalid reconciled leaves");
        if (e.a == 0) terminal(p, c, ChildState.CANCELLED);
        else {
          if (c.terminal()) p.p.children++;
          p.p.reserved += e.a - c.reservation;
          c.reservation = e.a;
          c.quantity = Math.addExact(c.filled, e.a);
          c.replaceQuantity = c.replacePrice = 0;
          c.state = ChildState.WORKING;
          if (p.p.state != ParentState.ACTIVE) p.cancel(c.id);
        }
      }
      case CORRECT, BUST -> {
        Trade trade = trades.get(c.destination + ":" + e.text);
        if (trade == null || trade.child != c.id)
          throw new IllegalArgumentException("Unknown trade correction");
        long newQuantity = type == ReportType.BUST ? 0 : e.a;
        if (newQuantity < 0 || e.b < 0) throw new IllegalArgumentException("Invalid correction");
        long delta = newQuantity - trade.quantity;
        trade.quantity = newQuantity;
        trade.price = e.b;
        applyFill(p, c, delta, e.b);
        if (p.p.state == ParentState.FILLED && p.p.residual() > 0) {
          p.p.state = ParentState.PAUSED;
          p.p.diagnostic = "Trade correction reopened residual";
        }
      }
      default -> throw new IllegalArgumentException("Unexpected report");
    }
    p.p.version++;
    emit("REPORT", c.parentId, c.id, type + ":" + e.text + ":" + e.a + ":" + e.b);
    checkBreach(p);
    if (p.p.state == ParentState.ACTIVE) callback(p, 4, null, c.id, type);
  }

  private void applyFill(Context p, ChildOrder c, long quantity, long price) {
    c.filled = Math.addExact(c.filled, quantity);
    p.p.venue = Math.addExact(p.p.venue, quantity);
    long old = c.reservation;
    if (!c.terminal()) {
      c.reservation =
          c.state == ChildState.PENDING_REPLACE || c.replacePrice != 0
              ? Math.max(0, Math.max(c.quantity, c.replaceQuantity) - c.filled)
              : Math.max(0, c.quantity - c.filled);
      p.p.reserved += c.reservation - old;
      if (c.reservation == 0) terminal(p, c, ChildState.FILLED);
    }
    if (quantity > 0 && badLimit(p.p.spec.side(), price, p.p.spec.limit())) {
      p.p.state = ParentState.ERROR;
      p.p.diagnostic = "Execution price breached parent limit";
      emit("BREACH", p.p.spec.id(), c.id, p.p.diagnostic);
      for (ChildOrder other = firstGlobal; other != null; other = other.nextGlobal)
        if (other != null && other.parentId == p.p.spec.id() && !other.terminal())
          p.cancel(other.id);
    }
    settle(p);
  }

  private void checkBreach(Context p) {
    if (p.p.residual() < 0 || p.p.reserved > p.p.residual()) {
      p.p.state = ParentState.ERROR;
      p.p.diagnostic = "Authoritative fill/reservation breach";
      emit("BREACH", p.p.spec.id(), 0, p.p.diagnostic);
      for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
        if (c != null && c.parentId == p.p.spec.id() && !c.terminal()) p.cancel(c.id);
    }
  }

  private void venueOutcome(Context p, ChildOrder c, boolean reject, EngineEvent e) {
    if (reject) {
      p.p.venueRejects++;
      if (e.c == RejectClass.LIQUIDITY.ordinal()) return;
    }
    for (int i = 0; i < policyCount; i++) {
      PolicyState s = policies[i];
      if (matches(s.policy, p.p, c.destination) && s.outcome(now, reject)) {
        if (s.policy.scope() == Scope.DESTINATION) {
          online[c.destination] = false;
          for (int j = 0; j < parentCount; j++)
            for (ChildOrder order = firstGlobal; order != null; order = order.nextGlobal)
              if (order != null
                  && order.parentId == parents[j].p.spec.id()
                  && order.destination == c.destination
                  && !order.terminal()) pause(parents[j], "Destination reject breaker", false);
        } else {
          for (int j = 0; j < parentCount; j++)
            if (matches(s.policy, parents[j].p, c.destination))
              pause(parents[j], "Venue reject breaker", false);
        }
      }
    }
  }

  private void preview(EngineEvent e) {
    Context p = context(e.parentId);
    Destination d = destination(e.destinationId);
    if (d == null
        || !d.internalBooking()
        || d.instrumentId() != 0 && d.instrumentId() != p.p.spec.instrumentId())
      throw new IllegalArgumentException("No internal booking capability");
    if (e.a != p.p.version
        || p.p.reserved != 0
        || p.p.children != 0
        || p.p.residual() <= 0
        || p.p.state == ParentState.ACTIVE
        || p.p.state == ParentState.BOOKING
        || e.text.isBlank()
        || e.key.isBlank())
      throw new IllegalArgumentException("Invalid/unresolved booking preview");
    QuoteState q = quotes[p.p.spec.instrumentId()][d.id()];
    if (!fresh(p, q, d.id())) throw new IllegalArgumentException("Stale booking quote");
    long price = p.p.spec.side() == Side.BUY ? q.ask : q.bid;
    if (badLimit(p.p.spec.side(), price, p.p.spec.limit()))
      throw new IllegalArgumentException("Booking exceeds parent limit");
    Preview v =
        new Preview(
            ++commandCounter,
            e.parentId,
            p.p.version,
            d.id(),
            p.p.residual(),
            price,
            q.observed,
            e.key,
            e.text);
    previews.values().removeIf(old -> old.parentId() == v.parentId());
    previews.put(v.id(), v);
    emit(
        "BOOK_PREVIEW",
        e.parentId,
        0,
        v.id() + ":" + v.quantity() + ":" + v.price() + ":" + v.washBook());
  }

  private boolean fresh(Context p, QuoteState q, int dest) {
    if (q == null || !q.valid || q.observed > now || !online[dest]) return false;
    for (int i = 0; i < policyCount; i++)
      if (matches(policies[i].policy, p.p, dest)
          && now - q.observed > policies[i].policy.limits().quoteAgeNanos()) return false;
    return true;
  }

  private void book(EngineEvent e) {
    Preview v = previews.get(e.id);
    Context p = context(e.parentId);
    if (e.key.isBlank() || operatorKeys.contains(e.key))
      throw new IllegalArgumentException("Duplicate/missing booking key");
    if (v == null
        || v.parentId() != e.parentId
        || v.version() != p.p.version
        || p.p.reserved != 0
        || p.p.children != 0
        || p.p.residual() != v.quantity()
        || p.p.state == ParentState.ACTIVE
        || p.p.state == ParentState.BOOKING
        || !ready
        || !journal.healthy())
      throw new IllegalArgumentException("Stale/unresolved booking preview");
    QuoteState q = quotes[p.p.spec.instrumentId()][v.destinationId()];
    if (!fresh(p, q, v.destinationId())
        || q.observed != v.observedNanos()
        || (p.p.spec.side() == Side.BUY ? q.ask : q.bid) != v.price())
      throw new IllegalArgumentException("Booking preview expired");
    if (operatorKeys.size() >= config.maxChildren())
      throw new IllegalArgumentException("Booking idempotency capacity");
    operatorKeys.add(e.key);
    p.p.state = ParentState.BOOKING;
    p.p.reserved = v.quantity();
    p.p.version++;
    long command = ++commandCounter;
    dispatch(
        new ExecutionCommand(
            command,
            e.parentId,
            0,
            v.destinationId(),
            p.p.spec.instrumentId(),
            p.p.spec.side(),
            CommandType.BOOK,
            v.quantity(),
            v.price(),
            OrderType.LIMIT,
            TimeInForce.DAY,
            false,
            v.washBook(),
            v.reason()));
    emit("BOOK_PENDING", e.parentId, 0, Long.toString(command));
    p.bookingCommand = command;
    p.bookingPrice = v.price();
    p.bookingDestination = v.destinationId();
  }

  private void bookingReport(EngineEvent e, ReportType type) {
    Context p = context(e.parentId);
    if (p.p.state != ParentState.BOOKING
        || e.id != p.bookingCommand
        || e.destinationId != p.bookingDestination)
      throw new IllegalArgumentException("Unknown booking acknowledgement");
    if (type == ReportType.BOOK_REJECTED) {
      p.p.reserved = 0;
      p.p.state = ParentState.ERROR;
      p.p.version++;
      emit("BOOK_REJECTED", e.parentId, 0, e.text);
      return;
    }
    if (e.text.isBlank() || e.a != p.p.reserved || e.b != p.bookingPrice)
      throw new IllegalArgumentException("Booking terms mismatch");
    String k = "book:" + e.destinationId + ":" + e.text;
    if (trades.containsKey(k)) return;
    trades.put(k, new Trade(e.parentId, 0, e.a, e.b, true));
    p.p.wash += e.a;
    p.p.reserved = 0;
    p.p.version++;
    settle(p);
    emit("BOOK_CONFIRMED", e.parentId, 0, e.text);
  }

  private void callback(Context p, int kind, Views.Quote quote, Long value) {
    callback(p, kind, quote, value, null);
  }

  private void callback(Context p, int kind, Views.Quote quote, Long value, ReportType report) {
    long start = System.nanoTime();
    try {
      switch (kind) {
        case 0 -> p.strategy.onStart(p);
        case 1 -> p.strategy.onQuote(p, quote);
        case 2 -> p.strategy.onTimer(p, value);
        case 3 -> p.strategy.onConfiguration(p);
        case 4 -> p.strategy.onOrderEvent(p, value, report);
        case 5 -> p.strategy.onStop(p);
        default -> throw new IllegalArgumentException();
      }
    } catch (RuntimeException ex) {
      p.p.state = ParentState.ERROR;
      p.p.diagnostic = "Strategy callback failed: " + ex.getClass().getSimpleName();
      pause(p, p.p.diagnostic, false);
      emit("STRATEGY_ERROR", p.p.spec.id(), 0, p.p.diagnostic);
    } finally {
      long duration = System.nanoTime() - start;
      metrics.callbackMaxNanos = Math.max(metrics.callbackMaxNanos, duration);
      if (!replay && duration > config.slowCallbackNanos()) {
        metrics.slowParent = p.p.spec.id();
      }
    }
  }

  /** Management snapshots allocate outside price callbacks, via an ordered management request. */
  public Snapshot snapshot() {
    List<ParentSnapshot> list = new ArrayList<>();
    for (int i = 0; i < parentCount; i++) {
      ParentOrder p = parents[i].p;
      list.add(
          new ParentSnapshot(
              p.spec.id(),
              p.spec.strategy(),
              p.state.name(),
              p.version,
              p.spec.quantity(),
              p.venue,
              p.wash,
              p.reserved,
              p.children,
              p.diagnostic,
              strategyState(parents[i]),
              parents[i].benchmark,
              costs(parents[i]),
              p.spec));
    }
    List<ChildSnapshot> orders = new ArrayList<>();
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null)
        orders.add(
            new ChildSnapshot(
                c.id,
                c.parentId,
                c.destination,
                c.state.name(),
                c.quantity,
                c.filled,
                c.price,
                c.reservation,
                c.replaceQuantity));
    List<DestinationSnapshot> venues = new ArrayList<>();
    for (Destination d : destinations)
      if (d != null) venues.add(new DestinationSnapshot(d, online[d.id()]));
    List<Policy> rules = new ArrayList<>();
    for (int i = 0; i < policyCount; i++) rules.add(policies[i].policy);
    return new Snapshot(
        sequence,
        now,
        ready,
        List.copyOf(list),
        List.copyOf(orders),
        List.copyOf(venues),
        List.copyOf(rules),
        stateHash());
  }

  public Preview preview(long id) {
    return previews.get(id);
  }

  /** Stable state digest used for verification; excludes wall-clock operational counters. */
  public String stateHash() {
    StringBuilder b =
        new StringBuilder()
            .append(sequence)
            .append(':')
            .append(now)
            .append(':')
            .append(utc)
            .append(':')
            .append(ready)
            .append(':')
            .append(commandCounter)
            .append(':')
            .append(childCounter);
    for (int i = 0; i < parentCount; i++) {
      ParentOrder p = parents[i].p;
      b.append('|')
          .append(p.spec.id())
          .append(':')
          .append(p.spec.quantity())
          .append(':')
          .append(p.spec.limit())
          .append(':')
          .append(p.spec.startNanos())
          .append(':')
          .append(p.spec.endNanos())
          .append(':')
          .append(new TreeMap<>(p.spec.parameters()))
          .append(':')
          .append(p.state)
          .append(':')
          .append(p.version)
          .append(':')
          .append(p.venue)
          .append(':')
          .append(p.wash)
          .append(':')
          .append(p.reserved)
          .append(':')
          .append(p.children)
          .append(':')
          .append(parents[i].profile)
          .append(':')
          .append(parents[i].benchmark)
          .append(':')
          .append(parents[i].recoveryBenchmarkMissing);
    }
    for (ChildOrder c = firstGlobal; c != null; c = c.nextGlobal)
      if (c != null)
        b.append('|')
            .append(c.id)
            .append(':')
            .append(c.state)
            .append(':')
            .append(c.quantity)
            .append(':')
            .append(c.filled)
            .append(':')
            .append(c.price)
            .append(':')
            .append(c.reservation)
            .append(':')
            .append(c.replaceQuantity);
    for (var entry : new TreeMap<>(trades).entrySet()) {
      Trade t = entry.getValue();
      b.append("|trade:")
          .append(entry.getKey())
          .append(':')
          .append(t.parent)
          .append(':')
          .append(t.child)
          .append(':')
          .append(t.quantity)
          .append(':')
          .append(t.price)
          .append(':')
          .append(t.internal);
    }
    for (Timer t : timers.stream().sorted(Comparator.comparingLong(Timer::registration)).toList())
      b.append('|').append(t);
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(b.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private final class Context implements StrategyContext {
    final ParentOrder p = new ParentOrder();
    Strategy strategy;
    final boolean[] subscriptions = new boolean[destinations.length];
    long bookingCommand, bookingPrice;
    int bookingDestination;
    boolean stopped;
    String strategyStatus = "";
    ExecutionProfile profile;
    CompositeReference reference;
    InceptionBenchmark benchmark;
    final long acceptedNanos = now;
    boolean recoveryBenchmarkMissing, recoveredHistoryIncomplete;
    ChildOrder firstChild;
    boolean finalMark;
    long expiryBid, expiryAsk;
    final EffectiveBounds effectiveBounds = new EffectiveBounds();

    private void assertOwner() {
      if (Thread.currentThread() != owner)
        throw new IllegalStateException("Strategy context is confined to the core thread");
    }

    Context(ParentSpec spec, Strategy strategy) {
      p.spec = spec;
      this.strategy = strategy;
      profile = plugins.require(spec.strategy()).executionProfile(spec);
      validateProfile(spec, profile);
      reference = referenceGroup(spec, profile);
    }

    public Views.Parent parent() {
      assertOwner();
      return p;
    }

    public long nowNanos() {
      assertOwner();
      return now;
    }

    public Instrument instrument() {
      assertOwner();
      return EngineCore.this.instrument(p.spec.instrumentId());
    }

    public Destination destination(int id) {
      assertOwner();
      return EngineCore.this.destination(id);
    }

    public Views.Quote quote(int id) {
      assertOwner();
      return id > 0 && id < destinations.length ? quotes[p.spec.instrumentId()][id] : null;
    }

    public Views.Child child(long id) {
      assertOwner();
      ChildOrder c = EngineCore.this.child(id);
      if (c != null && c.parentId != p.spec.id())
        throw new IllegalArgumentException("Child ownership");
      return c;
    }

    public void visitChildren(Views.ChildVisitor visitor) {
      assertOwner();
      for (ChildOrder c = firstChild; c != null; c = c.nextOwned) visitor.accept(c);
    }

    public Views.OrderBounds bounds(int id) {
      assertOwner();
      return effectiveBounds(this, id);
    }

    public Views.ReferenceMarket referenceMarket() {
      assertOwner();
      if (reference != null) reference.refresh(now, marketRevision);
      return reference;
    }

    public InceptionBenchmark benchmark() {
      assertOwner();
      return benchmark;
    }

    public void subscribe(int id) {
      assertOwner();
      Destination d = destination(id);
      if (d == null || d.instrumentId() != 0 && d.instrumentId() != p.spec.instrumentId())
        throw new IllegalArgumentException("Unsupported subscription");
      if (!subscriptions[id]) {
        subscriptions[id] = true;
        if (++subscriptionCounts[p.spec.instrumentId()][id] != 1) return;
        dispatch(
            new ExecutionCommand(
                ++commandCounter,
                p.spec.id(),
                0,
                id,
                p.spec.instrumentId(),
                p.spec.side(),
                CommandType.SUBSCRIBE,
                0,
                0,
                OrderType.LIMIT,
                TimeInForce.DAY,
                false,
                "",
                ""));
      }
    }

    public void unsubscribe(int id) {
      assertOwner();
      if (id > 0 && id < subscriptions.length && subscriptions[id]) {
        subscriptions[id] = false;
        if (--subscriptionCounts[p.spec.instrumentId()][id] != 0) return;
        dispatch(
            new ExecutionCommand(
                ++commandCounter,
                p.spec.id(),
                0,
                id,
                p.spec.instrumentId(),
                p.spec.side(),
                CommandType.UNSUBSCRIBE,
                0,
                0,
                OrderType.LIMIT,
                TimeInForce.DAY,
                false,
                "",
                ""));
      }
    }

    public Result submit(
        int dest, long quantity, long price, OrderType type, TimeInForce tif, boolean postOnly) {
      assertOwner();
      Outcome outcome = constraints(this, dest, quantity, price, type, tif, postOnly, null, true);
      if (outcome != Outcome.ACCEPTED) return rejected(this, 0, outcome);
      if (childCounter + 1 >= children.length) return rejected(this, 0, Outcome.CAPACITY);
      if (!budget(this, dest, 1, false)) return rejected(this, 0, Outcome.THROTTLED);
      ChildOrder c = new ChildOrder();
      c.id = ++childCounter;
      c.parentId = p.spec.id();
      c.destination = dest;
      c.quantity = c.reservation = quantity;
      c.price = price;
      c.type = type;
      c.tif = tif;
      c.postOnly = postOnly;
      c.state = ChildState.PENDING_NEW;
      c.lastReplace = now;
      c.commandId = ++commandCounter;
      children[(int) c.id] = c;
      insertGlobal(c);
      c.nextOwned = firstChild;
      firstChild = c;
      p.reserved += quantity;
      p.children++;
      p.localFailures = 0;
      dispatch(command(c, CommandType.NEW, c.commandId, quantity, price));
      return new Result(Outcome.ACCEPTED, c.commandId, c.id);
    }

    private ChildOrder owned(long id) {
      ChildOrder c = EngineCore.this.child(id);
      if (c == null || c.parentId != p.spec.id())
        throw new IllegalArgumentException("Child ownership");
      return c;
    }

    public Result replace(long id, long quantity, long price) {
      assertOwner();
      ChildOrder c = owned(id);
      if (c.state != ChildState.WORKING) return rejected(this, id, Outcome.UNRESOLVED);
      Outcome outcome =
          constraints(this, c.destination, quantity, price, c.type, c.tif, c.postOnly, c, true);
      if (outcome != Outcome.ACCEPTED) return rejected(this, id, outcome);
      if (!budget(this, c.destination, destination(c.destination).replaceMessageCost(), false))
        return rejected(this, id, Outcome.THROTTLED);
      long reservation = Math.max(quantity, c.quantity - c.filled);
      p.reserved += reservation - c.reservation;
      c.reservation = reservation;
      c.replaceQuantity = Math.addExact(c.filled, quantity);
      c.replacePrice = price;
      c.lastReplace = now;
      c.state = ChildState.PENDING_REPLACE;
      c.commandId = ++commandCounter;
      dispatch(command(c, CommandType.REPLACE, c.commandId, quantity, price));
      return new Result(Outcome.ACCEPTED, c.commandId, id);
    }

    public Result cancel(long id) {
      assertOwner();
      ChildOrder c = owned(id);
      if (c.terminal()) return new Result(Outcome.DUPLICATE, 0, id);
      if (c.state == ChildState.PENDING_CANCEL)
        return new Result(Outcome.DUPLICATE, c.commandId, id);
      if (!budget(this, c.destination, 1, true)) return new Result(Outcome.THROTTLED, 0, id);
      c.state = ChildState.PENDING_CANCEL;
      c.commandId = ++commandCounter;
      dispatch(command(c, CommandType.CANCEL, c.commandId, 0, c.price));
      return new Result(Outcome.ACCEPTED, c.commandId, id);
    }

    public Result query(long id) {
      assertOwner();
      ChildOrder c = owned(id);
      if (!budget(this, c.destination, 1, true)) return new Result(Outcome.THROTTLED, 0, id);
      long command = ++commandCounter;
      dispatch(command(c, CommandType.STATUS, command, 0, c.price));
      return new Result(Outcome.ACCEPTED, command, id);
    }

    private ExecutionCommand command(
        ChildOrder c, CommandType type, long id, long quantity, long price) {
      return new ExecutionCommand(
          id,
          p.spec.id(),
          c.id,
          c.destination,
          p.spec.instrumentId(),
          p.spec.side(),
          type,
          quantity,
          price,
          c.type,
          c.tif,
          c.postOnly,
          "",
          "");
    }

    public void schedule(long id, long deadline) {
      assertOwner();
      if (deadline <= now) throw new IllegalArgumentException("Timer must be in future");
      cancelTimer(id);
      if (timers.size() >= config.maxTimers()) throw new IllegalArgumentException("Timer capacity");
      timers.add(new Timer(p.spec.id(), id, deadline, ++timerCounter));
    }

    public void cancelTimer(long id) {
      assertOwner();
      timers.removeIf(t -> t.parent() == p.spec.id() && t.id() == id);
    }

    public void suspend(String reason) {
      assertOwner();
      pause(this, reason, false);
    }

    public void status(String message) {
      assertOwner();
      strategyStatus = message;
      p.diagnostic = message;
      emit("STATUS", p.spec.id(), 0, message);
    }
  }
}
