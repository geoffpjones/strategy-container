package io.strategycontainer.journal;

import io.strategycontainer.core.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32C;

/**
 * Bounded SPSC journal; the core encodes into preallocated slots and a dedicated thread performs
 * all disk I/O.
 */
public final class AsyncJournal implements Journal {
  public static final int MAGIC = 0x53454E47;
  private final ByteBuffer[] slots;
  private final long[] sequences;
  private final int mask;
  private final AtomicLong published = new AtomicLong(), consumed = new AtomicLong();
  private final Path directory;
  private final Thread writer;
  private volatile boolean running = true, healthy = true;
  private volatile Throwable failure;
  private volatile long written, durable;
  private final long rotateBytes, syncNanos;
  private final String manifest;

  /** Directory must be new or empty: existing journals are never overwritten. */
  public AsyncJournal(
      Path directory, String manifest, int capacity, long rotateBytes, long syncNanos)
      throws IOException {
    if (Integer.bitCount(capacity) != 1 || capacity < 2 || rotateBytes < 1024 || syncNanos <= 0)
      throw new IllegalArgumentException("Invalid journal options");
    this.directory = directory;
    this.manifest = manifest;
    this.rotateBytes = rotateBytes;
    this.syncNanos = syncNanos;
    mask = capacity - 1;
    Files.createDirectories(directory);
    try (var paths = Files.list(directory)) {
      if (paths.findAny().isPresent()) throw new IOException("Journal directory is not empty");
    }
    slots = new ByteBuffer[capacity];
    sequences = new long[capacity];
    for (int i = 0; i < capacity; i++) slots[i] = ByteBuffer.allocate(1024);
    writer = new Thread(this::writeLoop, "engine-journal");
    writer.start();
  }

  private ByteBuffer claim() {
    long p = published.get();
    if (!healthy || p - consumed.get() >= slots.length) {
      if (healthy) failure = new IllegalStateException("Journal capacity exhausted");
      healthy = false;
      return null;
    }
    return slots[(int) p & mask].clear().position(8);
  }

  private void publish(long sequence) {
    long p = published.get();
    sequences[(int) p & mask] = sequence;
    published.lazySet(p + 1);
    LockSupport.unpark(writer);
  }

  public boolean input(long seq, EngineEvent event) {
    ByteBuffer b = claim();
    if (b == null) return false;
    try {
      try {
        BinaryCodec.input(b, seq, event);
      } catch (java.nio.BufferOverflowException large) {
        if (b.capacity() >= 16392) throw large;
        b = ByteBuffer.allocate(16392);
        slots[(int) published.get() & mask] = b;
        b.position(8);
        BinaryCodec.input(b, seq, event);
      }
      publish(seq);
      return true;
    } catch (RuntimeException ex) {
      failure = ex;
      healthy = false;
      return false;
    }
  }

  public boolean output(Output output) {
    ByteBuffer b = claim();
    if (b == null) return false;
    try {
      try {
        BinaryCodec.output(b, output);
      } catch (java.nio.BufferOverflowException large) {
        if (b.capacity() >= 16392) throw large;
        b = ByteBuffer.allocate(16392);
        slots[(int) published.get() & mask] = b;
        b.position(8);
        BinaryCodec.output(b, output);
      }
      publish(output.sequence());
      return true;
    } catch (RuntimeException ex) {
      failure = ex;
      healthy = false;
      return false;
    }
  }

  public boolean complete(long seq) {
    ByteBuffer b = claim();
    if (b == null) return false;
    b.put((byte) 3).putLong(seq);
    publish(seq);
    return true;
  }

  public boolean healthy() {
    return healthy;
  }

  public long writtenSequence() {
    return written;
  }

  public long durableSequence() {
    return durable;
  }

  public long backlog() {
    return published.get() - consumed.get();
  }

  public Throwable failure() {
    return failure;
  }

  private FileChannel open(int segment) throws IOException {
    FileChannel f =
        FileChannel.open(
            directory.resolve(String.format("%06d.sej", segment)),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
    byte[] m = manifest.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ByteBuffer h =
        ByteBuffer.allocate(12 + m.length)
            .putInt(MAGIC)
            .putInt(BinaryCodec.SCHEMA)
            .putInt(m.length)
            .put(m);
    h.flip();
    while (h.hasRemaining()) f.write(h);
    return f;
  }

  private void writeLoop() {
    FileChannel f = null;
    try {
      int segment = 0;
      f = open(segment);
      ByteBuffer diskBuffer = ByteBuffer.allocateDirect(1 << 20);
      CRC32C crc = new CRC32C();
      long lastSync = System.nanoTime();
      while (running || consumed.get() < published.get()) {
        long c = consumed.get();
        if (c == published.get()) {
          if (System.nanoTime() - lastSync >= syncNanos) {
            f.force(false);
            durable = written;
            lastSync = System.nanoTime();
          }
          LockSupport.parkNanos(100_000);
          continue;
        }
        int available = (int) Math.min(4096, published.get() - c), count = 0;
        long lastWritten = written;
        diskBuffer.clear();
        while (count < available) {
          ByteBuffer b = slots[(int) (c + count) & mask];
          int total = b.position();
          if (total > diskBuffer.remaining()) break;
          int length = total - 8;
          crc.reset();
          crc.update(b.array(), 8, length);
          b.putInt(0, length).putInt(4, (int) crc.getValue());
          diskBuffer.put(b.array(), 0, total);
          if (b.array()[8] == 3) lastWritten = sequences[(int) (c + count) & mask];
          count++;
        }
        diskBuffer.flip();
        while (diskBuffer.hasRemaining()) f.write(diskBuffer);
        written = lastWritten;
        consumed.lazySet(c + count);
        if (f.position() >= rotateBytes) {
          f.force(false);
          durable = written;
          f.close();
          f = open(++segment);
          lastSync = System.nanoTime();
        } else if (System.nanoTime() - lastSync >= syncNanos) {
          f.force(false);
          durable = written;
          lastSync = System.nanoTime();
        }
      }
      f.force(false);
      durable = written;
    } catch (Throwable ex) {
      failure = ex;
      healthy = false;
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException ex) {
          failure = ex;
          healthy = false;
        }
    }
  }

  public void close() {
    running = false;
    LockSupport.unpark(writer);
    try {
      writer.join(10_000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    }
    if (writer.isAlive()) throw new IllegalStateException("Journal writer did not stop");
    if (failure != null) throw new IllegalStateException("Journal failed", failure);
  }
}
