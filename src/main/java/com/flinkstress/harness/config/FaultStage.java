package com.flinkstress.harness.config;

/**
 * Where a fault is injected. Two are stream boundaries served by a
 * fault-capable pass-through operator ({@link #AFTER_SOURCE}, {@link #BEFORE_SINK});
 * the other three are injected <em>inside</em> the real operators
 * ({@link #SOURCE}, {@link #AGGREGATE}, {@link #SINK}) for operator-accurate
 * targeting.
 */
public enum FaultStage {
    /** Inside the synthetic source's generation loop. */
    SOURCE,
    /** Pass-through operator right after the source. */
    AFTER_SOURCE,
    /** Inside the windowed aggregate function. */
    AGGREGATE,
    /** Pass-through operator right before the sink. */
    BEFORE_SINK,
    /** Inside the sink's record handling. */
    SINK
}
