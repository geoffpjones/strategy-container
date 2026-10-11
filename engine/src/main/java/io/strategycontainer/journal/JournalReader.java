package io.strategycontainer.journal;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/**
 * Streaming, checksum-validating journal reader. The callback buffer is borrowed until it returns.
 */
public final class JournalReader {
  @FunctionalInterface
  public interface Visitor {
    void accept(byte type, long sequence, ByteBuffer payload) throws IOException;
  }

  private JournalReader() {}

  /** Validate and visit a complete recording without retaining its frames in memory. */
  public static long visit(Path directory, String expectedManifest, Visitor visitor)
      throws IOException {
    return scan(directory, expectedManifest, Long.MAX_VALUE, false, visitor);
  }

  /**
   * Locate the last complete transaction. Only a truncated final tail is tolerated; corruption is
   * rejected.
   */
  public static long completePrefix(Path directory, String expectedManifest) throws IOException {
    return scan(directory, expectedManifest, Long.MAX_VALUE, true, (t, s, b) -> {});
  }

  /** Replay an explicitly selected complete prefix without inspecting the truncated suffix. */
  public static long visitPrefix(
      Path directory, String expectedManifest, long maximumSequence, Visitor visitor)
      throws IOException {
    return scan(directory, expectedManifest, maximumSequence, false, visitor);
  }

  private static long scan(
      Path directory,
      String expectedManifest,
      long maximumSequence,
      boolean tolerateTail,
      Visitor visitor)
      throws IOException {
    List<Path> segments;
    try (var files = Files.list(directory)) {
      segments = files.filter(p -> p.getFileName().toString().endsWith(".sej")).sorted().toList();
    }
    if (segments.isEmpty()) throw new IOException("No journal segments");
    String manifest = null;
    long lastInput = 0, lastComplete = 0;
    boolean complete = true;
    byte[] bytes = new byte[16384];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    CRC32C crc = new CRC32C();
    for (int segment = 0; segment < segments.size(); segment++) {
      if (!segments
          .get(segment)
          .getFileName()
          .toString()
          .equals(String.format("%06d.sej", segment)))
        throw new IOException("Missing journal segment");
      boolean last = segment == segments.size() - 1;
      try (DataInputStream in =
          new DataInputStream(
              new BufferedInputStream(Files.newInputStream(segments.get(segment)), 65536))) {
        if (in.readInt() != AsyncJournal.MAGIC || in.readInt() != BinaryCodec.SCHEMA)
          throw new IOException("Unsupported journal header");
        int n = in.readInt();
        if (n < 0 || n > 1_000_000) throw new IOException("Invalid manifest");
        byte[] m = in.readNBytes(n);
        if (m.length != n) throw new EOFException("Truncated manifest");
        String value = new String(m, java.nio.charset.StandardCharsets.UTF_8);
        if (manifest == null) manifest = value;
        else if (!manifest.equals(value))
          throw new IOException("Manifest changed between segments");
        if (!value.equals(expectedManifest))
          throw new IOException("Replay artifact/configuration manifest mismatch");
        while (true) {
          int first = in.read();
          if (first < 0) break;
          int length =
              (first << 24)
                  | (in.readUnsignedByte() << 16)
                  | (in.readUnsignedByte() << 8)
                  | in.readUnsignedByte();
          int checksum = in.readInt();
          if (length < 9 || length > bytes.length) throw new IOException("Invalid frame length");
          in.readFully(bytes, 0, length);
          crc.reset();
          crc.update(bytes, 0, length);
          if ((int) crc.getValue() != checksum) throw new IOException("Journal checksum mismatch");
          buffer.clear().limit(length);
          byte type = buffer.get();
          long seq = buffer.getLong();
          if (seq > maximumSequence) return lastComplete;
          if (type == 1) {
            if (!complete || seq != lastInput + 1)
              throw new IOException("Input sequence gap at " + seq);
            lastInput = seq;
            complete = false;
          } else if (type == 2) {
            if (complete || seq != lastInput) throw new IOException("Output sequence mismatch");
          } else if (type == 3) {
            if (complete || seq != lastInput || buffer.hasRemaining())
              throw new IOException("Invalid completion marker");
            complete = true;
            lastComplete = seq;
          } else throw new IOException("Unknown frame type");
          visitor.accept(type, seq, buffer);
        }
      } catch (EOFException ex) {
        if (!tolerateTail || !last) throw ex;
        return lastComplete;
      }
    }
    if (!complete && !tolerateTail)
      throw new EOFException("Incomplete input/output transaction at journal tail");
    return lastComplete;
  }
}
