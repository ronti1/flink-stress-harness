package com.flinkstress.harness.sink;

import com.flinkstress.harness.model.AggResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Terminal sink that turns the pipeline output into Flink metrics (scraped by
 * Prometheus). It registers, under the operator metric group:
 *
 * <ul>
 *   <li>{@code e2eLatencyMs} &mdash; histogram of end-to-end latency
 *       ({@code now - maxEmitTimeMs}); exposes p50/p99/p999 via Prometheus.</li>
 *   <li>{@code processedRecordsPerSec} &mdash; meter of source records represented
 *       by emitted aggregates (true end-to-end throughput).</li>
 *   <li>{@code aggregatesOut} / {@code processedRecords} / {@code processedBytes}
 *       &mdash; counters.</li>
 * </ul>
 *
 * <p>Latency definition: wall-clock at the sink minus the newest source emit
 * time among the records in the window. Window size therefore contributes to
 * the measured latency; this is consistent across clusters so the A/B
 * comparison stays valid.
 */
public class LatencyMeasuringSink extends RichSinkFunction<AggResult> {

    private static final long serialVersionUID = 1L;

    private final int reservoirSize;

    private transient Histogram latency;
    private transient Meter throughput;
    private transient Counter aggregatesOut;
    private transient Counter processedRecords;
    private transient Counter processedBytes;

    public LatencyMeasuringSink() {
        this(10_000);
    }

    public LatencyMeasuringSink(int reservoirSize) {
        this.reservoirSize = reservoirSize;
    }

    @Override
    public void open(Configuration parameters) {
        var group = getRuntimeContext().getMetricGroup();
        this.latency = group.histogram("e2eLatencyMs", new SlidingHistogram(reservoirSize));
        this.throughput = group.meter("processedRecordsPerSec", new MeterView(60));
        this.aggregatesOut = group.counter("aggregatesOut");
        this.processedRecords = group.counter("processedRecords");
        this.processedBytes = group.counter("processedBytes");
    }

    @Override
    public void invoke(AggResult value, Context context) {
        if (value.maxEmitTimeMs > 0) {
            latency.update(latencyMs(System.currentTimeMillis(), value.maxEmitTimeMs));
        }
        aggregatesOut.inc();
        processedRecords.inc(value.count);
        processedBytes.inc(value.sumBytes);
        throughput.markEvent(value.count);
    }

    /** End-to-end latency in ms, clamped at 0 to absorb minor clock skew. */
    static long latencyMs(long nowMs, long maxEmitTimeMs) {
        long e2e = nowMs - maxEmitTimeMs;
        return e2e < 0 ? 0 : e2e;
    }
}
