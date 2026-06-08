package com.flinkstress.harness.model;

/**
 * Output of the windowed aggregation. Carries {@code maxEmitTimeMs} so the
 * sink can compute end-to-end latency for the records that contributed to the
 * window.
 */
public class AggResult implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    public String key;
    public long count;
    public long sumBytes;
    /** Max source emit time (ms) across records in this window; basis for latency at the sink. */
    public long maxEmitTimeMs;
    /** Wall-clock time (ms) when this aggregate was produced by the aggregator. */
    public long aggTimeMs;

    public AggResult() {
    }

    public AggResult(String key, long count, long sumBytes, long maxEmitTimeMs, long aggTimeMs) {
        this.key = key;
        this.count = count;
        this.sumBytes = sumBytes;
        this.maxEmitTimeMs = maxEmitTimeMs;
        this.aggTimeMs = aggTimeMs;
    }

    @Override
    public String toString() {
        return "AggResult{key=" + key
                + ", count=" + count
                + ", sumBytes=" + sumBytes
                + ", maxEmitTimeMs=" + maxEmitTimeMs
                + ", aggTimeMs=" + aggTimeMs
                + '}';
    }
}
