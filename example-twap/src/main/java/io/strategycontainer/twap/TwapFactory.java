package io.strategycontainer.twap;

import io.strategycontainer.api.*;
import io.strategycontainer.api.Types.*;
import java.util.Map;

/**
 * Separately deployed reference TWAP; execution decisions never depend on engine implementation
 * classes.
 */
public final class TwapFactory implements StrategyFactory {
  public String name() {
    return "twap";
  }

  public Strategy create(ParentSpec parent) {
    return new Twap(parent);
  }

  public void validate(ParentSpec parent) {
    new Settings(parent);
  }

  public Map<String, String> parameterDescriptions() {
    return Map.of(
        "destination",
        "Numeric destination ID",
        "aheadSeconds",
        "Maximum execution lead (default 30)",
        "behindSeconds",
        "Permitted lag (default 60)",
        "planningMillis",
        "Planning interval (default 1000)",
        "mode",
        "balanced or aggressive",
        "layers",
        "Passive levels, 1..3");
  }

  private static final class Settings {
    final int destination, layers;
    final long ahead, behind, interval;
    final boolean aggressive;

    Settings(ParentSpec p) {
      Map<String, String> m = p.parameters();
      destination = Integer.parseInt(m.getOrDefault("destination", "1"));
      layers = Integer.parseInt(m.getOrDefault("layers", "3"));
      ahead =
          Math.multiplyExact(Long.parseLong(m.getOrDefault("aheadSeconds", "30")), 1_000_000_000L);
      behind =
          Math.multiplyExact(Long.parseLong(m.getOrDefault("behindSeconds", "60")), 1_000_000_000L);
      interval =
          Math.multiplyExact(Long.parseLong(m.getOrDefault("planningMillis", "1000")), 1_000_000L);
      String mode = m.getOrDefault("mode", "balanced");
      if (destination <= 0
          || layers < 1
          || layers > 3
          || ahead < 0
          || behind < 0
          || interval <= 0
          || !mode.equals("balanced") && !mode.equals("aggressive"))
        throw new IllegalArgumentException("Invalid TWAP settings");
      aggressive = mode.equals("aggressive");
    }
  }

  private static final class Twap implements Strategy {
    private Settings settings;
    private long lastPlan = Long.MIN_VALUE;
    private final int[] histogram = new int[128];
    private int samples;
    private final long[] passive = new long[3];
    private long lastOpportunity = Long.MIN_VALUE;

    Twap(ParentSpec p) {
      settings = new Settings(p);
    }

    public void onStart(StrategyContext c) {
      c.subscribe(settings.destination);
      c.schedule(
          1,
          Math.max(c.nowNanos() + 1, c.parent().specification().startNanos() + settings.interval));
    }

    public void onConfiguration(StrategyContext c) {
      settings = new Settings(c.parent().specification());
      c.subscribe(settings.destination);
      c.schedule(1, c.nowNanos() + settings.interval);
    }

    public void onTimer(StrategyContext c, long id) {
      plan(c, false);
      if (c.parent().state() == ParentState.ACTIVE) c.schedule(1, c.nowNanos() + settings.interval);
    }

    public void onQuote(StrategyContext c, Views.Quote q) {
      if (!q.valid() || q.destinationId() != settings.destination) return;
      long ticks = (q.ask() - q.bid()) / c.instrument().tick();
      int bucket = (int) Math.min(127, ticks);
      boolean cheap = false;
      if (samples >= 32) {
        int target = Math.max(1, samples / 5), sum = 0, quantile = 127;
        for (int i = 0; i < 128; i++) {
          sum += histogram[i];
          if (sum >= target) {
            quantile = i;
            break;
          }
        }
        cheap = bucket <= quantile && bucket < median();
      }
      if (samples == 100_000) {
        samples = 0;
        for (int i = 0; i < 128; i++) {
          histogram[i] /= 2;
          samples += histogram[i];
        }
      }
      histogram[bucket]++;
      samples++;
      if (cheap
          && (lastOpportunity == Long.MIN_VALUE
              || c.nowNanos() - lastOpportunity >= settings.interval)) {
        plan(c, true);
        lastOpportunity = c.nowNanos();
      }
    }

    private int median() {
      int sum = 0;
      for (int i = 0; i < 128; i++) {
        sum += histogram[i];
        if (sum >= samples / 2) return i;
      }
      return 127;
    }

    private static long portion(long quantity, long elapsed, long duration) {
      if (elapsed <= 0) return 0;
      if (elapsed >= duration) return quantity;
      return (long) (quantity * (elapsed / (double) duration));
    }

    private void plan(StrategyContext c, boolean opportunity) {
      Views.Parent parent = c.parent();
      ParentSpec p = parent.specification();
      long now = c.nowNanos();
      if (now < p.startNanos() || now >= p.endNanos()) return;
      if (!opportunity && lastPlan != Long.MIN_VALUE && now - lastPlan < settings.interval) return;
      lastPlan = now;
      Views.Quote q = c.quote(settings.destination);
      Destination d = c.destination(settings.destination);
      if (q == null || !q.valid() || d == null) return;
      long duration = p.endNanos() - p.startNanos(),
          elapsed = now - p.startNanos(),
          terminal = Math.min(60_000_000_000L, duration / 10);
      long upper = portion(p.quantity(), Math.min(duration, elapsed + settings.ahead), duration);
      long filled = parent.venueFilled() + parent.washFilled();
      long room =
          Math.min(parent.residual() - parent.reserved(), upper - filled - parent.reserved());
      boolean finishing = p.endNanos() - now <= terminal;
      long lag = settings.behind;
      if (finishing && terminal > 0)
        lag = (long) (lag * ((p.endNanos() - now) / (double) terminal));
      long lower = portion(p.quantity(), Math.max(0, elapsed - lag), duration);
      boolean behind = filled < lower;
      if (finishing || behind) {
        for (int i = 0; i < passive.length; i++)
          if (passive[i] != 0) {
            c.cancel(passive[i]);
            passive[i] = 0;
          }
      }
      if (room <= 0) return;
      long interval = opportunity ? 5_000_000_000L : settings.interval;
      long desired =
          portion(p.quantity(), Math.min(duration, elapsed + interval), duration)
              - filled
              - parent.reserved();
      if (finishing || behind) desired = Math.max(desired, parent.residual() - parent.reserved());
      long quantity = Math.min(room, Math.max(0, desired));
      Instrument instrument = c.instrument();
      long minimum = Math.max(instrument.minimumQuantity(), d.minimumQuantity());
      quantity -= quantity % instrument.lot();
      if (quantity < minimum) return;
      boolean take = opportunity || finishing || behind || settings.aggressive || !d.postOnly();
      if (take) {
        long price = p.side() == Side.BUY ? q.ask() : q.bid();
        if (p.side() == Side.BUY ? price > p.limit() : price < p.limit()) return;
        long available = p.side() == Side.BUY ? q.askSize() : q.bidSize();
        quantity = Math.min(quantity, Math.min(available, d.maximumQuantity()));
        quantity -= quantity % instrument.lot();
        if (quantity < minimum) return;
        TimeInForce tif =
            d.supports(OrderType.LIMIT, TimeInForce.IOC) ? TimeInForce.IOC : TimeInForce.FOK;
        if (!d.supports(OrderType.LIMIT, tif)) return;
        c.submit(d.id(), quantity, price, OrderType.LIMIT, tif, false);
        return;
      }
      if (parent.workingChildren() > 0) return;
      long passiveCap = portion(p.quantity(), Math.min(duration, 10_000_000_000L), duration);
      quantity = Math.min(quantity, passiveCap);
      double total = 0;
      for (int k = 0; k < settings.layers; k++) total += Math.exp(-k);
      long allocated = 0;
      for (int k = 0; k < settings.layers; k++) {
        long amount =
            k == settings.layers - 1
                ? quantity - allocated
                : (long) (quantity * Math.exp(-k) / total);
        amount -= amount % instrument.lot();
        if (amount < minimum) continue;
        long price =
            p.side() == Side.BUY
                ? q.bid() - k * instrument.tick()
                : q.ask() + k * instrument.tick();
        if (price <= 0 || (p.side() == Side.BUY ? price > p.limit() : price < p.limit())) continue;
        StrategyContext.Result result =
            c.submit(d.id(), amount, price, OrderType.LIMIT, TimeInForce.DAY, true);
        if (result.accepted()) {
          passive[k] = result.childId();
          allocated += amount;
        }
      }
    }
  }
}
