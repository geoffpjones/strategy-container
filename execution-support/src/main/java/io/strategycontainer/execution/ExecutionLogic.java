package io.strategycontainer.execution;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;

/** Common bounded price/clock calculations. All methods consume borrowed views synchronously. */
public final class ExecutionLogic {
  private ExecutionLogic() {}

  public static boolean fresh(StrategyContext c, StrategySettings s, Views.Quote q) {
    return q != null
        && q.valid()
        && q.bid() > 0
        && q.ask() > q.bid()
        && q.bidSize() > 0
        && q.askSize() > 0
        && q.observedNanos() <= c.nowNanos()
        && c.nowNanos() - q.observedNanos() <= s.quoteAgeNanos();
  }

  public static long anchor(StrategyContext c, StrategySettings s, Views.Quote q, boolean improve) {
    long tick = c.instrument().tick();
    long ticks = (q.ask() - q.bid()) / tick;
    long improvement = improve ? Math.min(s.improvementTicks(), Math.max(0, ticks - 1)) * tick : 0;
    boolean buy = c.parent().specification().side() == Side.BUY;
    long raw = buy ? q.bid() + improvement : q.ask() - improvement;
    long price =
        buy
            ? Math.min(raw, c.parent().specification().limit())
            : Math.max(raw, c.parent().specification().limit());
    return buy
        ? price - price % tick
        : price % tick == 0 ? price : Math.addExact(price, tick - price % tick);
  }

  public static void nextTimer(StrategyContext c, StrategySettings s) {
    if (c.parent().state() != ParentState.ACTIVE) return;
    long now = c.nowNanos(),
        start = c.parent().specification().startNanos(),
        end = c.parent().specification().endNanos();
    long origin = Math.max(now, start);
    if (s.intervalNanos() < end - origin) c.schedule(1, origin + s.intervalNanos());
  }

  public static long workingCap(StrategyContext c, StrategySettings s) {
    return Trajectory.rounded(
        (long) (c.parent().specification().quantity() * s.workingFraction()), c.instrument().lot());
  }

  public static boolean terminal(ParentSpec p, long now) {
    return p.endNanos() - now <= Math.min(60_000_000_000L, (p.endNanos() - p.startNanos()) / 10);
  }
}
