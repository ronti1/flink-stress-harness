package com.flinkstress.harness.config;

/** Watermark generation strategy for event-time processing. */
public enum WatermarkStrategyType {
    /** Allow out-of-orderness up to {@code watermark.boundedOutOfOrdernessMs}. */
    BOUNDED_OUT_OF_ORDERNESS,
    /** Assume timestamps are monotonically increasing (ascending). */
    MONOTONOUS,
    /** Emit no watermarks (event-time windows fire only at end-of-stream). */
    NONE
}
