package io.strategycontainer.core;

import io.strategycontainer.api.ExecutionCommand;

/** Deterministic output, correlated with its causative input sequence. */
public record Output(
    long sequence,
    String type,
    long parentId,
    long childId,
    String detail,
    ExecutionCommand command) {}
