package com.flinkstress.harness;

import com.flinkstress.harness.config.HarnessConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates job wiring without executing: {@code getExecutionPlan()} builds the
 * stream graph (and thus exercises every branch of {@link HarnessJob#buildPipeline})
 * without spinning up a cluster.
 */
class HarnessJobTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void buildEnvironmentAppliesParallelismRocksDbAndCheckpointing() {
        HarnessConfig cfg = HarnessConfig.fromMap(map(
                "job.parallelism", "3",
                "state.backend", "rocksdb",
                "checkpoint.enabled", "true",
                "checkpoint.dir", "s3://bucket/chk"));
        StreamExecutionEnvironment env = HarnessJob.buildEnvironment(cfg);
        assertThat(env.getParallelism()).isEqualTo(3);
        assertThat(env.getCheckpointConfig().isCheckpointingEnabled()).isTrue();
    }

    @Test
    void configureRestartFixedDelay() {
        Configuration conf = new Configuration();
        HarnessJob.configureRestart(conf, HarnessConfig.fromMap(map(
                "restart.strategy", "fixed-delay",
                "restart.attempts", "7",
                "restart.delayMs", "2000")));
        Map<String, String> m = conf.toMap();
        assertThat(m).containsEntry("restart-strategy.type", "fixed-delay");
        assertThat(m).containsEntry("restart-strategy.fixed-delay.attempts", "7");
    }

    @Test
    void configureRestartExponentialDelay() {
        Configuration conf = new Configuration();
        HarnessJob.configureRestart(conf, HarnessConfig.fromMap(map(
                "restart.strategy", "exponential-delay",
                "restart.delayMs", "1000")));
        Map<String, String> m = conf.toMap();
        assertThat(m).containsEntry("restart-strategy.type", "exponential-delay");
        assertThat(m).containsKey("restart-strategy.exponential-delay.initial-backoff");
    }

    @Test
    void buildPipelineMetricsSinkWithTimeWindow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HarnessConfig cfg = HarnessConfig.fromMap(map(
                "window.type", "TUMBLING_TIME",
                "sink.mode", "METRICS"));
        HarnessJob.buildPipeline(env, cfg);
        String plan = env.getExecutionPlan();
        assertThat(plan).contains("synthetic-source");
        assertThat(plan).contains("latency-sink");
    }

    @Test
    void buildPipelineConsoleSinkCountWindowWithBothFaults() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HarnessConfig cfg = HarnessConfig.fromMap(map(
                "window.type", "TUMBLING_COUNT",
                "sink.mode", "CONSOLE",
                "partition.mode", "REBALANCE",
                "fault.app.enabled", "true",
                "fault.app.stage", "AFTER_SOURCE",
                "fault.sys.enabled", "true",
                "fault.sys.stage", "BEFORE_SINK"));
        HarnessJob.buildPipeline(env, cfg);
        String plan = env.getExecutionPlan();
        assertThat(plan).contains("fault-after-source");
        assertThat(plan).contains("fault-before-sink");
        assertThat(plan).contains("console-sink");
    }

    @Test
    void buildPipelineSessionWindow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HarnessConfig cfg = HarnessConfig.fromMap(map("window.type", "SESSION"));
        HarnessJob.buildPipeline(env, cfg);
        assertThat(env.getExecutionPlan()).contains("window-aggregate");
    }

    @Test
    void buildPipelineProcessingTimeHasNoWatermarksOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HarnessConfig cfg = HarnessConfig.fromMap(map(
                "time.characteristic", "PROCESSING_TIME",
                "window.type", "TUMBLING_TIME"));
        HarnessJob.buildPipeline(env, cfg);
        String plan = env.getExecutionPlan();
        assertThat(plan).contains("window-aggregate");
        assertThat(plan).doesNotContain("watermarks");
    }

    @Test
    void buildPipelineEventTimeRendersWatermarks() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HarnessConfig cfg = HarnessConfig.fromMap(map(
                "window.type", "TUMBLING_TIME",
                "watermark.strategy", "MONOTONOUS"));
        HarnessJob.buildPipeline(env, cfg);
        assertThat(env.getExecutionPlan()).contains("watermarks");
    }

    @Test
    void watermarkStrategyBuildsForEachType() {
        assertThat(HarnessJob.watermarkStrategy(HarnessConfig.fromMap(
                map("watermark.strategy", "BOUNDED_OUT_OF_ORDERNESS")))).isNotNull();
        assertThat(HarnessJob.watermarkStrategy(HarnessConfig.fromMap(
                map("watermark.strategy", "MONOTONOUS")))).isNotNull();
        assertThat(HarnessJob.watermarkStrategy(HarnessConfig.fromMap(
                map("watermark.strategy", "NONE", "watermark.idlenessMs", "1000")))).isNotNull();
    }
}
