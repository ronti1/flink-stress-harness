package com.flinkstress.harness.config;

/**
 * Where a fault is injected. The harness inserts a fault-capable pass-through
 * operator immediately after the source and immediately before the sink; a
 * fault targets one of these injection points.
 */
public enum FaultStage {
    /** Pass-through operator right after the source. */
    AFTER_SOURCE,
    /** Pass-through operator right before the sink. */
    BEFORE_SINK
}
