package com.flinkstress.harness.sink;

import com.flinkstress.harness.model.AggResult;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

/**
 * Exercises the sink operators through Flink's operator test harness so the
 * metric-registration and {@code invoke} paths run (they need a RuntimeContext).
 */
class SinkHarnessTest {

    private static AggResult agg(long maxEmitTimeMs) {
        return new AggResult("k1", 5, 50, maxEmitTimeMs, System.currentTimeMillis());
    }

    @Test
    void consoleSinkProcessesRecords() throws Exception {
        try (OneInputStreamOperatorTestHarness<AggResult, Object> h =
                     new OneInputStreamOperatorTestHarness<>(new StreamSink<>(new ConsoleSink()))) {
            h.open();
            h.processElement(new StreamRecord<>(agg(System.currentTimeMillis() - 100)));
            h.processElement(new StreamRecord<>(agg(0))); // exercises the "no emit time" branch
        }
    }

    @Test
    void latencyMeasuringSinkRegistersMetricsAndProcesses() throws Exception {
        try (OneInputStreamOperatorTestHarness<AggResult, Object> h =
                     new OneInputStreamOperatorTestHarness<>(new StreamSink<>(new LatencyMeasuringSink(128)))) {
            h.open(); // registers histogram, meter, counters
            h.processElement(new StreamRecord<>(agg(System.currentTimeMillis() - 250)));
            h.processElement(new StreamRecord<>(agg(0))); // skips latency update path
        }
    }
}
