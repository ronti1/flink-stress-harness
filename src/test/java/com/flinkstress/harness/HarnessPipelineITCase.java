package com.flinkstress.harness;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.PartitionMode;
import com.flinkstress.harness.keyby.HarnessKeySelector;
import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import com.flinkstress.harness.source.SyntheticSource;
import com.flinkstress.harness.window.WindowFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end pipeline tests on a local Flink MiniCluster (run by Failsafe in the
 * {@code verify} phase). A bounded source ({@code source.maxRecords}) lets the
 * job terminate so results can be collected and asserted.
 */
class HarnessPipelineITCase {

    private static HarnessConfig config(Map<String, String> overrides) {
        Map<String, String> base = new HashMap<>(overrides);
        base.putIfAbsent("source.rateMode", "MAX");
        base.putIfAbsent("job.parallelism", "1");
        return HarnessConfig.fromMap(base);
    }

    @Test
    void countWindowOfSizeOneEmitsOneAggregatePerRecord() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put("source.maxRecords", "2000");
        o.put("source.numKeys", "50");
        o.put("window.type", "TUMBLING_COUNT");
        o.put("window.countSize", "1");
        HarnessConfig cfg = config(o);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> src = env.addSource(new SyntheticSource(cfg));
        KeyedStream<Event, String> keyed = src.keyBy(new HarnessKeySelector(PartitionMode.KEY_BY, cfg.numKeys));
        SingleOutputStreamOperator<AggResult> agg = WindowFactory.apply(keyed, cfg);

        List<AggResult> results = collect(agg);

        assertThat(results).hasSize(2000);
        assertThat(results).allSatisfy(r -> {
            assertThat(r.key).isNotNull();
            assertThat(r.count).isEqualTo(1L);
        });
        assertThat(results.stream().mapToLong(r -> r.count).sum()).isEqualTo(2000L);
    }

    @Test
    void timeWindowsFireOnSourceCompletionAndCoverAllRecords() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put("source.maxRecords", "1000");
        o.put("source.numKeys", "20");
        o.put("window.type", "TUMBLING_TIME");
        o.put("window.sizeMs", "50");
        o.put("watermark.boundedOutOfOrdernessMs", "0");
        HarnessConfig cfg = config(o);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<Event> ws = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(cfg.boundedOutOfOrdernessMs))
                .withTimestampAssigner((e, ts) -> e.eventTimeMs);

        DataStreamSource<Event> src = env.addSource(new SyntheticSource(cfg));
        KeyedStream<Event, String> keyed = src.assignTimestampsAndWatermarks(ws)
                .keyBy(new HarnessKeySelector(PartitionMode.KEY_BY, cfg.numKeys));
        SingleOutputStreamOperator<AggResult> agg = WindowFactory.apply(keyed, cfg);

        List<AggResult> results = collect(agg);

        // On source completion Flink emits MAX_WATERMARK, firing all windows, so
        // every record is accounted for exactly once across the fired windows.
        assertThat(results).isNotEmpty();
        assertThat(results.stream().mapToLong(r -> r.count).sum()).isEqualTo(1000L);
        assertThat(results).allSatisfy(r -> assertThat(r.maxEmitTimeMs).isGreaterThan(0L));
    }

    private static List<AggResult> collect(SingleOutputStreamOperator<AggResult> stream) throws Exception {
        List<AggResult> results = new ArrayList<>();
        try (CloseableIterator<AggResult> it = stream.executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        return results;
    }
}
