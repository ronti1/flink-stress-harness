package com.flinkstress.harness.sink;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.TriggerMode;
import com.flinkstress.harness.fault.FaultConfig;
import com.flinkstress.harness.fault.FaultInjector;
import com.flinkstress.harness.fault.HarnessInjectedException;
import com.flinkstress.harness.model.AggResult;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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

    @Test
    void latencySinkInjectsSinkStageFault() throws Exception {
        FaultConfig f = new FaultConfig(FaultType.APP_EXCEPTION, FaultStage.SINK,
                TriggerMode.EVERY_N_RECORDS, 1, 1, 1.0, 1);
        FaultInjector fi = new FaultInjector(Collections.singletonList(f), 0L);
        LatencyMeasuringSink sink = new LatencyMeasuringSink(128).withFaultInjector(fi);
        try (OneInputStreamOperatorTestHarness<AggResult, Object> h =
                     new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink))) {
            h.open();
            Throwable t = catchThrowable(() ->
                    h.processElement(new StreamRecord<>(agg(System.currentTimeMillis()))));
            assertThat(t).isNotNull();
            boolean found = false;
            for (Throwable x = t; x != null; x = x.getCause()) {
                if (x instanceof HarnessInjectedException) {
                    found = true;
                }
            }
            assertThat(found).isTrue();
        }
    }
}
