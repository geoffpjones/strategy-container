package io.strategycontainer.management;

import io.strategycontainer.core.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Bounded owned output feed. Slow clients disconnect rather than blocking the execution core. */
public final class EventFeed implements OutputSink {
  private final CopyOnWriteArrayList<Subscription> clients = new CopyOnWriteArrayList<>();
  private final AtomicReferenceArray<Output> history = new AtomicReferenceArray<>(1024);
  private final AtomicLong count = new AtomicLong();

  public final class Subscription implements AutoCloseable {
    private final ArrayBlockingQueue<Output> queue = new ArrayBlockingQueue<>(256);
    private volatile boolean alive = true;

    public Output poll() throws InterruptedException {
      return queue.poll(1, TimeUnit.SECONDS);
    }

    public boolean alive() {
      return alive;
    }

    public void close() {
      alive = false;
      clients.remove(this);
    }
  }

  public Subscription subscribe() {
    if (clients.size() >= 64) throw new IllegalStateException("Stream capacity");
    Subscription s = new Subscription();
    clients.add(s);
    return s;
  }

  public void accept(Output output) {
    long n = count.getAndIncrement();
    history.set((int) n & 1023, output);
    for (Subscription client : clients) if (!client.queue.offer(output)) client.close();
  }

  public java.util.List<Output> recent() {
    long end = count.get(), start = Math.max(0, end - 1024);
    java.util.List<Output> result = new java.util.ArrayList<>();
    for (long i = start; i < end; i++) {
      Output o = history.get((int) i & 1023);
      if (o != null) result.add(o);
    }
    return result;
  }
}
