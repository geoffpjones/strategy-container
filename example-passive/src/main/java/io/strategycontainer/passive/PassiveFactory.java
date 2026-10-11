package io.strategycontainer.passive;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import io.strategycontainer.execution.*;
import java.util.Map;

/** Paced execution using exclusively venue-protected post-only liquidity provision. */
public final class PassiveFactory implements StrategyFactory {
  public String name() {
    return "passive";
  }

  public void validate(ParentSpec parent) {
    StrategySettings.from(parent);
  }

  public ExecutionProfile executionProfile(ParentSpec parent) {
    return new ExecutionProfile(true, java.util.List.of(), 1_000_000_000L, 5_000_000_000L);
  }

  public Strategy create(ParentSpec parent) {
    return new Passive(StrategySettings.from(parent));
  }

  public Map<String, String> parameterDescriptions() {
    return Map.ofEntries(
        Map.entry("destination", "Post-only DAY/GTC execution destination (default 1)"),
        Map.entry("planningMillis", "Planning interval (default 1000)"),
        Map.entry("aheadSeconds", "Release lead (default 30)"),
        Map.entry("behindSeconds", "Permitted lag (default 60)"),
        Map.entry("layers", "Exponential layers, 1..3 (default 3)"),
        Map.entry("workingFraction", "Maximum resting fraction of parent, (0,1] (default 0.10)"),
        Map.entry("improvementTicks", "Inside-spread improvement when behind (default 1)"),
        Map.entry("minimumRestingMillis", "Minimum replacement resting time (default 1000)"),
        Map.entry("quoteAgeMillis", "Execution quote maximum age (default 1000)"));
  }

  private static final class Passive implements Strategy {
    private StrategySettings settings;
    private final PassiveLayers layers = new PassiveLayers();
    private long lastPlan = Long.MIN_VALUE;
    private String stage = "";

    private void stage(StrategyContext c, String value) {
      if (!stage.equals(value)) {
        stage = value;
        c.status(value);
      }
    }

    Passive(StrategySettings settings) {
      this.settings = settings;
    }

    public void onStart(StrategyContext c) {
      configure(c);
    }

    public void onConfiguration(StrategyContext c) {
      configure(c);
    }

    private void configure(StrategyContext c) {
      int old = settings.destination();
      settings = StrategySettings.from(c.parent().specification());
      if (old != settings.destination()) c.unsubscribe(old);
      c.subscribe(settings.destination());
      layers.restore(c, settings.destination());
      lastPlan = Long.MIN_VALUE;
      plan(c);
      ExecutionLogic.nextTimer(c, settings);
    }

    public void onQuote(StrategyContext c, Views.Quote q) {
      if (q.destinationId() == settings.destination()) plan(c);
    }

    public void onTimer(StrategyContext c, long id) {
      plan(c);
      ExecutionLogic.nextTimer(c, settings);
    }

    public void onOrderEvent(StrategyContext c, long id, ReportType type) {
      /* Reconcile on the next bounded planning tick. */
    }

    private void plan(StrategyContext c) {
      long now = c.nowNanos();
      ParentSpec p = c.parent().specification();
      if (c.parent().state() != ParentState.ACTIVE || now < p.startNanos() || now >= p.endNanos())
        return;
      Views.Quote q = c.quote(settings.destination());
      if (!ExecutionLogic.fresh(c, settings, q)) {
        stage(c, "PASSIVE_WAITING_QUOTE");
        layers.cancelAll(c);
        return;
      }
      if (lastPlan != Long.MIN_VALUE && now - lastPlan < settings.intervalNanos()) return;
      lastPlan = now;
      long duration = p.endNanos() - p.startNanos(),
          elapsed = now - p.startNanos(),
          filled = c.parent().venueFilled() + c.parent().washFilled();
      long upper =
          Trajectory.quantity(
              p.quantity(),
              Trajectory.ahead(elapsed, settings.aheadNanos(), duration),
              duration,
              0);
      long lower =
          Trajectory.quantity(
              p.quantity(), Math.max(0, elapsed - settings.behindNanos()), duration, 0);
      boolean behind = filled < lower, terminal = ExecutionLogic.terminal(p, now);
      stage(c, terminal ? "PASSIVE_TERMINAL" : behind ? "PASSIVE_BEHIND" : "PASSIVE_PACING");
      long target =
          Trajectory.quantity(
              p.quantity(),
              Trajectory.ahead(elapsed, settings.intervalNanos(), duration),
              duration,
              0);
      if (terminal) target = p.quantity();
      long exposure =
          Math.min(
              Math.max(0, Math.min(upper, target) - filled),
              Math.min(c.parent().residual(), ExecutionLogic.workingCap(c, settings)));
      layers.plan(
          c,
          settings,
          exposure,
          ExecutionLogic.anchor(c, settings, q, behind || terminal),
          terminal ? 1 : settings.layers(),
          behind || terminal);
    }
  }
}
