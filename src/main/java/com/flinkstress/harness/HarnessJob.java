package com.flinkstress.harness;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.SinkMode;
import com.flinkstress.harness.config.StateBackend;
import com.flinkstress.harness.config.TimeCharacteristic;
import com.flinkstress.harness.config.WindowType;
import com.flinkstress.harness.fault.FaultInjectionMap;
import com.flinkstress.harness.fault.FaultInjector;
import com.flinkstress.harness.keyby.HarnessKeySelector;
import com.flinkstress.harness.model.AggResult;
import com.flinkstress.harness.model.Event;
import com.flinkstress.harness.sink.ConsoleSink;
import com.flinkstress.harness.sink.LatencyMeasuringSink;
import com.flinkstress.harness.source.SyntheticSource;
import com.flinkstress.harness.window.WindowFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point. Builds and runs the configurable stress pipeline:
 *
 * <pre>
 *   SyntheticSource -&gt; [watermarks] -&gt; [fault:AFTER_SOURCE] -&gt; keyBy
 *       -&gt; window+aggregate -&gt; [fault:BEFORE_SINK] -&gt; sink
 * </pre>
 *
 * All behaviour is driven by {@link HarnessConfig} (CLI args and/or a
 * {@code --scenario} preset).
 */
public final class HarnessJob {

    private static final Logger LOG = LoggerFactory.getLogger(HarnessJob.class);

    private HarnessJob() {
    }

    public static void main(String[] args) throws Exception {
        HarnessConfig cfg = HarnessConfig.fromArgs(args);
        LOG.info("Starting {} with {}", cfg.jobName, cfg);

        StreamExecutionEnvironment env = buildEnvironment(cfg);
        buildPipeline(env, cfg);
        env.execute(cfg.jobName);
    }

    static StreamExecutionEnvironment buildEnvironment(HarnessConfig cfg) {
        Configuration conf = new Configuration();
        configureRestart(conf, cfg);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(cfg.parallelism);

        if (cfg.stateBackend == StateBackend.ROCKSDB) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(cfg.rocksdbIncremental));
        } else {
            env.setStateBackend(new HashMapStateBackend());
        }

        if (cfg.checkpointEnabled) {
            env.enableCheckpointing(cfg.checkpointIntervalMs);
            env.getCheckpointConfig().setCheckpointStorage(cfg.checkpointDir);
            env.getCheckpointConfig().setCheckpointTimeout(cfg.checkpointTimeoutMs);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(cfg.checkpointMinPauseMs);
        }
        return env;
    }

    static void configureRestart(Configuration conf, HarnessConfig cfg) {
        conf.setString("restart-strategy.type", cfg.restartStrategy);
        if ("fixed-delay".equalsIgnoreCase(cfg.restartStrategy)) {
            conf.setString("restart-strategy.fixed-delay.attempts", String.valueOf(cfg.restartAttempts));
            conf.setString("restart-strategy.fixed-delay.delay", cfg.restartDelayMs + " ms");
        } else if ("exponential-delay".equalsIgnoreCase(cfg.restartStrategy)) {
            conf.setString("restart-strategy.exponential-delay.initial-backoff", cfg.restartDelayMs + " ms");
            conf.setString("restart-strategy.exponential-delay.max-backoff", (cfg.restartDelayMs * 10) + " ms");
            conf.setString("restart-strategy.exponential-delay.backoff-multiplier", "2.0");
        }
    }

    static void buildPipeline(StreamExecutionEnvironment env, HarnessConfig cfg) {
        DataStreamSource<Event> source = env.addSource(new SyntheticSource(cfg));
        source.name("synthetic-source").uid("synthetic-source")
                .setParallelism(cfg.effectiveSourceParallelism());

        DataStream<Event> events = source;
        // Event-time windows need timestamps + watermarks; processing-time and
        // count windows do not.
        boolean eventTimeWindows = (cfg.windowType == WindowType.TUMBLING_TIME
                || cfg.windowType == WindowType.SESSION)
                && cfg.timeCharacteristic == TimeCharacteristic.EVENT_TIME;
        if (eventTimeWindows) {
            events = source.assignTimestampsAndWatermarks(watermarkStrategy(cfg))
                    .name("watermarks").uid("watermarks");
        }

        FaultInjectionMap<Event> afterSource = FaultInjectionMap.forStage(cfg, FaultStage.AFTER_SOURCE);
        if (afterSource.hasFaults()) {
            events = events.map(afterSource).name("fault-after-source").uid("fault-after-source");
        }

        KeyedStream<Event, String> keyed =
                events.keyBy(new HarnessKeySelector(cfg.partitionMode, cfg.numKeys));

        SingleOutputStreamOperator<AggResult> aggregated = WindowFactory.apply(keyed, cfg)
                .name("window-aggregate")
                .uid("window-aggregate");

        DataStream<AggResult> preSink = aggregated;
        FaultInjectionMap<AggResult> beforeSink = FaultInjectionMap.forStage(cfg, FaultStage.BEFORE_SINK);
        if (beforeSink.hasFaults()) {
            preSink = aggregated.map(beforeSink).name("fault-before-sink").uid("fault-before-sink");
        }

        FaultInjector sinkFault = FaultInjector.forStage(cfg, FaultStage.SINK);
        if (cfg.sinkMode == SinkMode.CONSOLE) {
            preSink.addSink(new ConsoleSink().withFaultInjector(sinkFault))
                    .name("console-sink").uid("console-sink");
        } else {
            preSink.addSink(new LatencyMeasuringSink().withFaultInjector(sinkFault))
                    .name("latency-sink").uid("latency-sink");
        }
    }

    /** Builds the configured watermark strategy for event-time processing. */
    static WatermarkStrategy<Event> watermarkStrategy(HarnessConfig cfg) {
        WatermarkStrategy<Event> base;
        switch (cfg.watermarkStrategyType) {
            case MONOTONOUS:
                base = WatermarkStrategy.<Event>forMonotonousTimestamps();
                break;
            case NONE:
                base = WatermarkStrategy.<Event>noWatermarks();
                break;
            case BOUNDED_OUT_OF_ORDERNESS:
            default:
                base = WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofMillis(cfg.boundedOutOfOrdernessMs));
                break;
        }
        base = base.withTimestampAssigner((e, ts) -> e.eventTimeMs);
        if (cfg.watermarkIdlenessMs > 0) {
            base = base.withIdleness(Duration.ofMillis(cfg.watermarkIdlenessMs));
        }
        return base;
    }
}
