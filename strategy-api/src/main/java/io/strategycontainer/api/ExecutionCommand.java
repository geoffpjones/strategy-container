package io.strategycontainer.api;

import io.strategycontainer.api.Types.*;

/**
 * Owned immutable outbound command. Quantity means leaves quantity; connectors translate their own
 * protocol semantics.
 */
public record ExecutionCommand(
    long id,
    long parentId,
    long childId,
    int destinationId,
    int instrumentId,
    Side side,
    CommandType type,
    long quantity,
    long price,
    OrderType orderType,
    TimeInForce timeInForce,
    boolean postOnly,
    String reference,
    String reason) {}
