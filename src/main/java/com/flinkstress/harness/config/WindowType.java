package com.flinkstress.harness.config;

/** Window assigner type. */
public enum WindowType {
    /** Fixed-size, non-overlapping windows by event time. */
    TUMBLING_TIME,
    /** Fixed-count, non-overlapping windows (count trigger, no time/watermarks). */
    TUMBLING_COUNT,
    /** Session windows with an inactivity gap by event time. */
    SESSION
}
