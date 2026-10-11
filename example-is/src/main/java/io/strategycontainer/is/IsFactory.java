package io.strategycontainer.is;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.execution.*;
import java.util.*;

/** Inception-mid shortfall controller with configured, rather than calibrated, urgency. */
public final class IsFactory implements StrategyFactory {
  public String name() {
    return "is";
  }

  public void validate(ParentSpec p) {
    StrategySettings.from(p);
    urgency(p);
    executionProfile(p);
  }

  private static double urgency(ParentSpec p) {
    double k = StrategySettings.decimal(p.parameters(), "urgency", 2);
    if (!Double.isFinite(k) || k < 0 || k > 8)
      throw new IllegalArgumentException("urgency must be 0..8");
    return k;
  }

  public ExecutionProfile executionProfile(ParentSpec p) {
    String value = p.parameters().get("referenceDestinations");
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("referenceDestinations required");
    List<Integer> refs =
        Arrays.stream(value.split(",", -1)).map(String::trim).map(Integer::parseInt).toList();
    return new ExecutionProfile(
        false,
        refs,
        StrategySettings.nanos(p.parameters(), "referenceAgeMillis", 1000, 1_000_000L),
        StrategySettings.nanos(p.parameters(), "benchmarkWaitMillis", 5000, 1_000_000L));
  }

  public Strategy create(ParentSpec p) {
    return new Is(p);
  }

  public Map<String, String> parameterDescriptions() {
    Map<String, String> result =
        new TreeMap<>(new io.strategycontainer.execution.ParameterDescriptions().common());
    result.put("urgency", "Front-loading coefficient, 0..8 (default 2; zero is linear)");
    result.put(
        "referenceDestinations",
        "Required comma-separated composite destinations; one fresh firm constituent suffices");
    result.put("referenceAgeMillis", "Reference freshness (default 1000)");
    result.put("benchmarkWaitMillis", "Benchmark acquisition timeout (default 5000)");
    return Map.copyOf(result);
  }

  private final class Is implements Strategy {
    private StrategySettings settings;
    private double urgency;
    private ExecutionProfile profile;
    private final PassiveLayers layers = new PassiveLayers();
    private SpreadWindow spreads = new SpreadWindow();
    private long lastPlan = Long.MIN_VALUE;
    private boolean awaitingDrain;
    private String stage = "";

    private void stage(StrategyContext c, String value) {
      if (!stage.equals(value)) {
        stage = value;
        c.status(value);
      }
    }

    Is(ParentSpec p) {
      settings = StrategySettings.from(p);
      urgency = urgency(p);
      profile = executionProfile(p);
    }

    public void onStart(StrategyContext c) {
      configure(c);
    }

    public void onConfiguration(StrategyContext c) {
      configure(c);
    }

    private void configure(StrategyContext c) {
      int old = settings.destination();
      ExecutionProfile oldProfile = profile;
      settings = StrategySettings.from(c.parent().specification());
      urgency = urgency(c.parent().specification());
      profile = executionProfile(c.parent().specification());
      if (old != settings.destination()) c.unsubscribe(old);
      for (int id : oldProfile.referenceDestinations())
        if (!profile.referenceDestinations().contains(id) && id != settings.destination())
          c.unsubscribe(id);
      c.subscribe(settings.destination());
      for (int id : profile.referenceDestinations()) c.subscribe(id);
      layers.restore(c, settings.destination());
      lastPlan = Long.MIN_VALUE;
      spreads = new SpreadWindow();
      Destination d = c.destination(settings.destination());
      if (d == null
          || (!d.supports(OrderType.LIMIT, TimeInForce.IOC)
              && !d.supports(OrderType.LIMIT, TimeInForce.FOK))) {
        c.suspend("IS requires IOC or FOK execution support");
        return;
      }
      plan(c);
      ExecutionLogic.nextTimer(c, settings);
    }

    public void onQuote(StrategyContext c, Views.Quote q) {
      plan(c);
    }

    public void onTimer(StrategyContext c, long id) {
      plan(c);
      ExecutionLogic.nextTimer(c, settings);
    }

    public void onOrderEvent(StrategyContext c, long id, ReportType type) {
      if (awaitingDrain && !layers.working(c)) {
        awaitingDrain = false;
        lastPlan = Long.MIN_VALUE;
        plan(c);
      }
    }

    private void plan(StrategyContext c) {
      long now = c.nowNanos();
      ParentSpec p = c.parent().specification();
      if (c.parent().state() != ParentState.ACTIVE || now < p.startNanos() || now >= p.endNanos())
        return;
      Views.Quote q = c.quote(settings.destination());
      Views.ReferenceMarket reference = c.referenceMarket();
      if (c.benchmark() == null) {
        stage(c, "IS_BENCHMARK_PENDING");
        return;
      }
      if (!ExecutionLogic.fresh(c, settings, q) || reference == null || !reference.valid()) {
        stage(c, "IS_WAITING_MARKET");
        layers.cancelAll(c);
        return;
      }
      if (lastPlan != Long.MIN_VALUE && now - lastPlan < settings.intervalNanos()) return;
      lastPlan = now;
      long duration = p.endNanos() - p.startNanos(), elapsed = now - p.startNanos();
      long filled = c.parent().venueFilled() + c.parent().washFilled();
      long upper =
          Trajectory.quantity(
              p.quantity(),
              Trajectory.ahead(elapsed, settings.aheadNanos(), duration),
              duration,
              urgency);
      boolean terminal = ExecutionLogic.terminal(p, now);
      long terminalWindow = Math.min(60_000_000_000L, duration / 10), lag = settings.behindNanos();
      if (terminal && terminalWindow > 0)
        lag = (long) (lag * ((p.endNanos() - now) / (double) terminalWindow));
      long lower = Trajectory.quantity(p.quantity(), Math.max(0, elapsed - lag), duration, urgency);
      boolean behind = filled < lower;
      long touch = p.side() == Side.BUY ? q.ask() : q.bid();
      boolean favourable = c.benchmark().favourable(touch, p.side());
      boolean cheap =
          spreads.observe(
              now, settings.intervalNanos(), (q.ask() - q.bid()) / c.instrument().tick());
      boolean opportunity = favourable || cheap;
      long lookahead = opportunity ? 5_000_000_000L : settings.intervalNanos();
      long target =
          Trajectory.quantity(
              p.quantity(), Trajectory.ahead(elapsed, lookahead, duration), duration, urgency);
      if (behind) target = Math.max(target, lower);
      if (terminal) target = p.quantity();
      target = Math.min(target, upper);
      Destination d = c.destination(settings.destination());
      boolean take =
          opportunity
              || behind
              || terminal
              || !d.postOnly()
              || (!d.supports(OrderType.LIMIT, TimeInForce.DAY)
                  && !d.supports(OrderType.LIMIT, TimeInForce.GTC));
      if (take) {
        layers.cancelAll(c);
        if (layers.working(c)) {
          stage(c, "IS_WAITING_PASSIVE_DRAIN");
          awaitingDrain = true;
          return;
        }
        awaitingDrain = false;
        stage(c, terminal ? "IS_TERMINAL" : behind ? "IS_CATCH_UP" : "IS_OPPORTUNITY");
        if (p.side() == Side.BUY ? touch > p.limit() : touch < p.limit()) return;
        Views.OrderBounds bounds = c.bounds(settings.destination());
        if (!bounds.eligible() || bounds.availableChildSlots() <= 0) return;
        long amount =
            Math.min(
                Math.max(0, target - filled - c.parent().reserved()),
                Math.min(bounds.maximumQuantity(), bounds.availableQuantity()));
        amount =
            Trajectory.rounded(
                Math.min(amount, p.side() == Side.BUY ? q.askSize() : q.bidSize()),
                c.instrument().lot());
        if (amount < bounds.minimumQuantity()) return;
        TimeInForce tif =
            d.supports(OrderType.LIMIT, TimeInForce.IOC) ? TimeInForce.IOC : TimeInForce.FOK;
        c.submit(settings.destination(), amount, touch, OrderType.LIMIT, tif, false);
      } else {
        stage(c, "IS_PASSIVE_PACING");
        long exposure =
            Math.min(
                Math.max(0, target - filled),
                Math.min(c.parent().residual(), ExecutionLogic.workingCap(c, settings)));
        layers.plan(
            c,
            settings,
            exposure,
            ExecutionLogic.anchor(c, settings, q, false),
            settings.layers(),
            false);
      }
    }
  }
}
