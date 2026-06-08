package com.flinkstress.harness.config;

/** Fault kind. */
public enum FaultType {
    /** No fault. */
    NONE,
    /** Throw a Java exception in the target operator (region failover / restart). */
    APP_EXCEPTION,
    /** Crash the TaskManager JVM hosting the target subtask (Runtime.halt). */
    SYSTEM_HALT
}
