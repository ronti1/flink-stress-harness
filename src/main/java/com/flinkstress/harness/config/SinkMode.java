package com.flinkstress.harness.config;

/** Sink selection. */
public enum SinkMode {
    /** Latency-measuring sink that registers Flink metrics (throughput, e2e latency histogram). */
    METRICS,
    /** Print aggregates to stdout (debugging / no-Grafana runs). */
    CONSOLE
}
