package com.flinkstress.harness.config;

/** Source firing strategy. */
public enum RateMode {
    /** Steady fixed records/sec (latency &amp; soak tests). */
    CONSTANT,
    /** Step up the rate every interval until a ceiling (find max throughput). */
    RAMP,
    /** Fire as fast as the pipeline will accept (unbounded). */
    MAX,
    /** Alternate between a base rate and a peak rate (spike pattern). */
    BURST
}
