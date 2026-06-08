package com.flinkstress.harness.config;

/** How a fault's recurrence interval is measured. */
public enum TriggerMode {
    /** Trigger once every N records observed by the subtask. */
    EVERY_N_RECORDS,
    /** Trigger once every N milliseconds of wall-clock time. */
    EVERY_MS
}
