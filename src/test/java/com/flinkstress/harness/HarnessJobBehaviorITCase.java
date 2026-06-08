package com.flinkstress.harness;

import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.WindowType;
import com.flinkstress.harness.fault.HarnessInjectedException;
import com.flinkstress.harness.keyby.HarnessKeySelector;
import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import com.flinkstress.harness.source.SyntheticSource;
import com.flinkstress.harness.window.WindowFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Job-level behavioural verification on a real Flink MiniCluster. Each test
 * drives the actual operators (source, keying, windows, faults) with a bounded
 * workload and asserts on collected output or terminal job state, so the
 * runtime behaviour — not just the unit logic — is proven.
 */
class HarnessJobBehaviorITCase {

    private static Map<String, String> opts(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static DataStream<Event> sourced(StreamExecutionEnvironment env, HarnessConfig cfg) {
        DataStreamSource<Event> src = env.addSource(new SyntheticSource(cfg));
        if (cfg.windowType == WindowType.TUMBLING_TIME || cfg.windowType == WindowType.SESSION) {
            WatermarkStrategy<Event> ws = WatermarkStrategy
                    .<Event>forBoundedOutOfOrderness(Duration.ofMillis(cfg.boundedOutOfOrdernessMs))
                    .withTimestampAssigner((e, ts) -> e.eventTimeMs);
            return src.assignTimestampsAndWatermarks(ws);
        }
        return src;
    }

    private static List<AggResult> runAndCollect(HarnessConfig cfg) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<Event, String> keyed =
                sourced(env, cfg).keyBy(new HarnessKeySelector(cfg.partitionMode, cfg.numKeys));
        SingleOutputStreamOperator<AggResult> agg = WindowFactory.apply(keyed, cfg);
        List<AggResult> out = new ArrayList<>();
        try (CloseableIterator<AggResult> it = agg.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static long sumCounts(List<AggResult> results) {
        return results.stream().mapToLong(r -> r.count).sum();
    }

    // ---- source rate: paced (CONSTANT) branch runs end-to-end ----

    @Test
    void pacedConstantSourceEmitsExactlyMaxRecords() throws Exception {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "source.rateMode", "CONSTANT",
                "source.ratePerSec", "1000000", // high so the test is fast; still exercises the token bucket
                "source.maxRecords", "2000",
                "source.numKeys", "50",
                "window.type", "TUMBLING_COUNT",
                "window.countSize", "1"));
        List<AggResult> results = runAndCollect(cfg);
        assertThat(results).hasSize(2000);
        assertThat(sumCounts(results)).isEqualTo(2000L);
    }

    // ---- late data + watermarks: no record loss when the bound covers lateness ----

    @Test
    void lateDataFlowsThroughEventTimeWindowsWithoutLoss() throws Exception {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "source.rateMode", "CONSTANT",
                "source.ratePerSec", "1000000",
                "source.maxRecords", "3000",
                "source.numKeys", "20",
                "late.enabled", "true",
                "late.pct", "0.3",
                "late.minMs", "1000",
                "late.maxMs", "8000",
                "watermark.boundedOutOfOrdernessMs", "8000", // >= maxLateness => nothing dropped
                "window.type", "TUMBLING_TIME",
                "window.sizeMs", "1000",
                "window.allowedLatenessMs", "8000"));
        List<AggResult> results = runAndCollect(cfg);
        assertThat(sumCounts(results)).isEqualTo(3000L);
    }

    // ---- session windows: group by gap, cover all records ----

    @Test
    void sessionWindowsGroupAndCoverAllRecords() throws Exception {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "source.rateMode", "CONSTANT",
                "source.ratePerSec", "1000000",
                "source.maxRecords", "2000",
                "source.numKeys", "10",
                "watermark.boundedOutOfOrdernessMs", "0",
                "window.type", "SESSION",
                "window.sessionGapMs", "1000"));
        List<AggResult> results = runAndCollect(cfg);
        assertThat(results).isNotEmpty();
        assertThat(sumCounts(results)).isEqualTo(2000L);
    }

    // ---- skew: keyBy concentrates load on the hot keys ----

    @Test
    void keyBySkewConcentratesLoadOnHotKeys() throws Exception {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "source.rateMode", "MAX",
                "source.maxRecords", "5000",
                "source.numKeys", "1000",
                "skew.enabled", "true",
                "skew.hotKeyCount", "5",
                "skew.hotKeyTrafficPct", "0.8",
                "partition.mode", "KEY_BY",
                "window.type", "TUMBLING_COUNT",
                "window.countSize", "1"));
        List<AggResult> results = runAndCollect(cfg);

        Map<String, Long> perKey = results.stream()
                .collect(Collectors.groupingBy(r -> r.key, Collectors.summingLong(r -> r.count)));
        long total = perKey.values().stream().mapToLong(Long::longValue).sum();
        long hot = 0;
        for (int i = 0; i < 5; i++) {
            hot += perKey.getOrDefault("k" + i, 0L);
        }
        assertThat(total).isEqualTo(5000L);
        assertThat((double) hot / total).isGreaterThan(0.7); // ~0.8 hot-key share lands on 5 keys
    }

    // ---- rebalance: even spread, ignoring data skew ----

    @Test
    void rebalanceSpreadsEvenlyIgnoringDataSkew() throws Exception {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "source.rateMode", "MAX",
                "source.maxRecords", "5000",
                "source.numKeys", "100",
                "skew.enabled", "true",
                "skew.hotKeyCount", "5",
                "skew.hotKeyTrafficPct", "0.9",
                "partition.mode", "REBALANCE",
                "window.type", "TUMBLING_COUNT",
                "window.countSize", "1"));
        List<AggResult> results = runAndCollect(cfg);

        Map<String, Long> perKey = results.stream()
                .collect(Collectors.groupingBy(r -> r.key, Collectors.summingLong(r -> r.count)));
        long total = perKey.values().stream().mapToLong(Long::longValue).sum();
        long max = perKey.values().stream().mapToLong(Long::longValue).max().orElse(0);
        assertThat(total).isEqualTo(5000L);
        // 100 partition buckets, even spread => each ~1%; assert no bucket dominates
        assertThat((double) max / total).isLessThan(0.05);
    }

    // ---- fault injection: app exception actually fails a running job ----

    @Test
    void appFaultFailsRunningJobWithInjectedException() {
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "restart.strategy", "none",
                "sink.mode", "CONSOLE",
                "source.rateMode", "MAX",
                "source.maxRecords", "100000",
                "source.numKeys", "50",
                "window.type", "TUMBLING_COUNT",
                "window.countSize", "1",
                "fault.app.enabled", "true",
                "fault.app.stage", "AFTER_SOURCE",
                "fault.app.triggerMode", "EVERY_N_RECORDS",
                "fault.app.triggerN", "200",
                "fault.app.maxOccurrences", "1"));
        StreamExecutionEnvironment env = HarnessJob.buildEnvironment(cfg);
        env.setParallelism(1);
        HarnessJob.buildPipeline(env, cfg);

        Throwable thrown = catchThrowable(env::execute);
        assertThat(thrown).isNotNull();
        assertThat(hasCause(thrown, HarnessInjectedException.class)).isTrue();
    }

    // ---- recovery: the harness pipeline + restart strategy recover from a mid-pipeline failure ----

    @Test
    void jobRecoversFromMidPipelineFailureWhenRestartEnabled() throws Exception {
        FailOnce.FIRED.set(false);
        HarnessConfig cfg = HarnessConfig.fromMap(opts(
                "restart.strategy", "fixed-delay",
                "restart.attempts", "3",
                "restart.delayMs", "200",
                "source.rateMode", "MAX",
                "source.maxRecords", "2000",
                "source.numKeys", "50",
                "window.type", "TUMBLING_COUNT",
                "window.countSize", "1"));
        StreamExecutionEnvironment env = HarnessJob.buildEnvironment(cfg);
        env.setParallelism(1);

        KeyedStream<Event, String> keyed =
                sourced(env, cfg).keyBy(new HarnessKeySelector(cfg.partitionMode, cfg.numKeys));
        SingleOutputStreamOperator<AggResult> agg = WindowFactory.apply(keyed, cfg);
        agg.map(new FailOnce()).setParallelism(1).addSink(new DiscardingSink<>());

        // Should complete despite the injected one-shot failure (i.e. it recovered).
        env.execute();
        assertThat(FailOnce.FIRED.get()).isTrue();
    }

    private static boolean hasCause(Throwable t, Class<?> type) {
        while (t != null) {
            if (type.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /** Throws exactly once across the whole JVM (survives restart via a static flag). */
    static final class FailOnce implements MapFunction<AggResult, AggResult> {
        static final AtomicBoolean FIRED = new AtomicBoolean(false);
        private long count;

        @Override
        public AggResult map(AggResult value) {
            count++;
            if (count >= 50 && FIRED.compareAndSet(false, true)) {
                throw new RuntimeException("one-shot test failure to trigger recovery");
            }
            return value;
        }
    }
}
