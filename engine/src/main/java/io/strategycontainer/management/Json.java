package io.strategycontainer.management;

import java.lang.reflect.RecordComponent;
import java.util.*;

/** Cold-path JSON writer for owned management records; no runtime serialization dependency. */
public final class Json {
  private Json() {}

  public static String write(Object value) {
    StringBuilder b = new StringBuilder();
    append(b, value);
    return b.toString();
  }

  private static void append(StringBuilder b, Object v) {
    if (v == null) b.append("null");
    else if (v instanceof String || v instanceof Enum<?>) quote(b, v.toString());
    else if (v instanceof Number || v instanceof Boolean) b.append(v);
    else if (v instanceof Map<?, ?> map) {
      b.append('{');
      boolean first = true;
      for (var e : map.entrySet()) {
        if (!first) b.append(',');
        first = false;
        quote(b, e.getKey().toString());
        b.append(':');
        append(b, e.getValue());
      }
      b.append('}');
    } else if (v instanceof Iterable<?> items) {
      b.append('[');
      boolean first = true;
      for (Object item : items) {
        if (!first) b.append(',');
        first = false;
        append(b, item);
      }
      b.append(']');
    } else if (v.getClass().isRecord()) {
      b.append('{');
      boolean first = true;
      for (RecordComponent c : v.getClass().getRecordComponents()) {
        if (!first) b.append(',');
        first = false;
        quote(b, c.getName());
        b.append(':');
        try {
          append(b, c.getAccessor().invoke(v));
        } catch (ReflectiveOperationException ex) {
          throw new IllegalArgumentException(ex);
        }
      }
      b.append('}');
    } else quote(b, v.toString());
  }

  private static void quote(StringBuilder b, String s) {
    b.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> b.append("\\\"");
        case '\\' -> b.append("\\\\");
        case '\n' -> b.append("\\n");
        case '\r' -> b.append("\\r");
        case '\t' -> b.append("\\t");
        default -> {
          if (c < 32) b.append(String.format("\\u%04x", (int) c));
          else b.append(c);
        }
      }
    }
    b.append('"');
  }
}
