package io.strategycontainer.api;

/** Wire-independent enums; ordinal values are versioned by the journal schema. */
public final class Types {
  private Types() {}

  public enum Side {
    BUY,
    SELL
  }

  public enum OrderType {
    LIMIT,
    PEGGED
  }

  public enum TimeInForce {
    IOC,
    FOK,
    DAY,
    GTC
  }

  public enum ParentState {
    ACTIVE,
    PAUSED,
    CANCELLING,
    CANCELLED,
    FILLED,
    ERROR,
    BOOKING
  }

  public enum ChildState {
    PENDING_NEW,
    WORKING,
    PENDING_CANCEL,
    PENDING_REPLACE,
    UNCERTAIN,
    CANCELLED,
    FILLED,
    REJECTED
  }

  public enum ReportType {
    ACK,
    FILL,
    CANCELLED,
    REJECTED,
    REPLACED,
    CANCEL_REJECTED,
    REPLACE_REJECTED,
    UNCERTAIN,
    RECONCILED,
    CORRECT,
    BUST,
    BOOKED,
    BOOK_REJECTED
  }

  public enum LiquidityRole {
    UNKNOWN,
    MAKER,
    TAKER
  }

  public enum RejectClass {
    TECHNICAL,
    VALIDATION,
    LIQUIDITY
  }

  public enum CommandType {
    NEW,
    REPLACE,
    CANCEL,
    STATUS,
    BOOK,
    SUBSCRIBE,
    UNSUBSCRIBE
  }

  public enum Outcome {
    ACCEPTED,
    CAPACITY,
    LIFECYCLE,
    SIZE,
    PRICE,
    LIMIT,
    EXPOSURE,
    THROTTLED,
    STALE,
    DESTINATION,
    UNSUPPORTED,
    VERSION,
    UNRESOLVED,
    DUPLICATE,
    INVALID
  }

  public enum Scope {
    ENGINE,
    ACCOUNT,
    STRATEGY,
    PARENT,
    INSTRUMENT,
    DESTINATION
  }
}
