package com.flinkstress.harness.config;

import com.flinkstress.harness.fault.FaultConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Typed, immutable view over every harness knob. Built from a
 * {@link ParameterTool} which is assembled by {@link #fromArgs(String[])} as:
 *
 * <ol>
 *   <li>optional {@code --scenario NAME} preset (loaded from classpath), then</li>
 *   <li>command-line {@code --key value} overrides on top.</li>
 * </ol>
 */
public final class HarnessConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // --- job ---
    public final String jobName;
    public final int parallelism;
    public final int sourceParallelism;

    // --- source ---
    public final RateMode rateMode;
    public final long ratePerSec;
    public final int recordSizeBytes;
    public final long numKeys;
    public final long maxRecords;
    public final long seed;

    // ramp
    public final long rampStartRatePerSec;
    public final long rampStepRatePerSec;
    public final long rampStepSeconds;
    public final long rampMaxRatePerSec;

    // burst
    public final long burstBaseRatePerSec;
    public final long burstPeakRatePerSec;
    public final long burstPeakSeconds;
    public final long burstPeriodSeconds;

    // --- skew ---
    public final boolean skewEnabled;
    public final int hotKeyCount;
    public final double hotKeyTrafficPct;

    // --- late data ---
    public final boolean lateEnabled;
    public final double latePct;
    public final long lateMinMs;
    public final long lateMaxMs;
    public final long boundedOutOfOrdernessMs;
    public final long allowedLatenessMs;
    public final TimeCharacteristic timeCharacteristic;
    public final WatermarkStrategyType watermarkStrategyType;
    public final long watermarkIdlenessMs;

    // --- partition ---
    public final PartitionMode partitionMode;

    // --- window ---
    public final WindowType windowType;
    public final long windowSizeMs;
    public final long windowCountSize;
    public final long sessionGapMs;

    // --- udf ---
    public final long cpuBurnMicros;
    public final int stateAccumulatorBytes;

    // --- sink ---
    public final SinkMode sinkMode;

    // --- state / checkpoint ---
    public final StateBackend stateBackend;
    public final boolean rocksdbIncremental;
    public final boolean checkpointEnabled;
    public final long checkpointIntervalMs;
    public final long checkpointTimeoutMs;
    public final long checkpointMinPauseMs;
    public final String checkpointDir;

    // --- restart strategy ---
    public final String restartStrategy;
    public final int restartAttempts;
    public final long restartDelayMs;

    // --- faults ---
    public final FaultConfig appFault;
    public final FaultConfig sysFault;

    HarnessConfig(ParameterTool p) {
        this.jobName = p.get("job.name", "flink-stress-harness");
        this.parallelism = p.getInt("job.parallelism", 1);
        this.sourceParallelism = p.getInt("source.parallelism", 0);

        this.rateMode = enumOf(RateMode.class, p.get("source.rateMode", "CONSTANT"));
        this.ratePerSec = p.getLong("source.ratePerSec", 10_000L);
        this.recordSizeBytes = p.getInt("source.recordSizeBytes", 256);
        this.numKeys = p.getLong("source.numKeys", 1_000L);
        this.maxRecords = p.getLong("source.maxRecords", 0L);
        this.seed = p.getLong("source.seed", 1234L);

        this.rampStartRatePerSec = p.getLong("source.ramp.startRatePerSec", 1_000L);
        this.rampStepRatePerSec = p.getLong("source.ramp.stepRatePerSec", 1_000L);
        this.rampStepSeconds = p.getLong("source.ramp.stepSeconds", 10L);
        this.rampMaxRatePerSec = p.getLong("source.ramp.maxRatePerSec", 0L);

        this.burstBaseRatePerSec = p.getLong("source.burst.baseRatePerSec", 1_000L);
        this.burstPeakRatePerSec = p.getLong("source.burst.peakRatePerSec", 20_000L);
        this.burstPeakSeconds = p.getLong("source.burst.peakSeconds", 5L);
        this.burstPeriodSeconds = p.getLong("source.burst.periodSeconds", 30L);

        this.skewEnabled = p.getBoolean("skew.enabled", false);
        this.hotKeyCount = p.getInt("skew.hotKeyCount", 10);
        this.hotKeyTrafficPct = p.getDouble("skew.hotKeyTrafficPct", 0.8);

        this.lateEnabled = p.getBoolean("late.enabled", false);
        this.latePct = p.getDouble("late.pct", 0.05);
        this.lateMinMs = p.getLong("late.minMs", 1_000L);
        this.lateMaxMs = p.getLong("late.maxMs", 10_000L);
        this.boundedOutOfOrdernessMs = p.getLong("watermark.boundedOutOfOrdernessMs", 5_000L);
        this.allowedLatenessMs = p.getLong("window.allowedLatenessMs", 0L);
        this.timeCharacteristic = enumOf(TimeCharacteristic.class, p.get("time.characteristic", "EVENT_TIME"));
        this.watermarkStrategyType = enumOf(WatermarkStrategyType.class,
                p.get("watermark.strategy", "BOUNDED_OUT_OF_ORDERNESS"));
        this.watermarkIdlenessMs = p.getLong("watermark.idlenessMs", 0L);

        this.partitionMode = enumOf(PartitionMode.class, p.get("partition.mode", "KEY_BY"));

        this.windowType = enumOf(WindowType.class, p.get("window.type", "TUMBLING_TIME"));
        this.windowSizeMs = p.getLong("window.sizeMs", 5_000L);
        this.windowCountSize = p.getLong("window.countSize", 1_000L);
        this.sessionGapMs = p.getLong("window.sessionGapMs", 5_000L);

        this.cpuBurnMicros = p.getLong("udf.cpuBurnMicros", 0L);
        this.stateAccumulatorBytes = p.getInt("udf.stateAccumulatorBytes", 0);

        this.sinkMode = enumOf(SinkMode.class, p.get("sink.mode", "METRICS"));

        this.stateBackend = enumOf(StateBackend.class, p.get("state.backend", "HASHMAP"));
        this.rocksdbIncremental = p.getBoolean("state.rocksdb.incremental", true);
        this.checkpointEnabled = p.getBoolean("checkpoint.enabled", false);
        this.checkpointIntervalMs = p.getLong("checkpoint.intervalMs", 10_000L);
        this.checkpointTimeoutMs = p.getLong("checkpoint.timeoutMs", 600_000L);
        this.checkpointMinPauseMs = p.getLong("checkpoint.minPauseMs", 0L);
        this.checkpointDir = p.get("checkpoint.dir", "");

        this.restartStrategy = p.get("restart.strategy", "fixed-delay");
        this.restartAttempts = p.getInt("restart.attempts", 10);
        this.restartDelayMs = p.getLong("restart.delayMs", 5_000L);

        this.appFault = faultFrom(p, "fault.app", FaultType.APP_EXCEPTION, FaultStage.AFTER_SOURCE,
                TriggerMode.EVERY_MS, 60_000L);
        this.sysFault = faultFrom(p, "fault.sys", FaultType.SYSTEM_HALT, FaultStage.AFTER_SOURCE,
                TriggerMode.EVERY_MS, 120_000L);

        validate();
    }

    public int effectiveSourceParallelism() {
        return sourceParallelism > 0 ? sourceParallelism : parallelism;
    }

    private void validate() {
        if (recordSizeBytes < 0) {
            throw new IllegalArgumentException("source.recordSizeBytes must be >= 0");
        }
        if (numKeys <= 0) {
            throw new IllegalArgumentException("source.numKeys must be > 0");
        }
        if (skewEnabled) {
            if (hotKeyCount <= 0 || hotKeyCount > numKeys) {
                throw new IllegalArgumentException("skew.hotKeyCount must be in (0, numKeys]");
            }
            if (hotKeyTrafficPct < 0.0 || hotKeyTrafficPct > 1.0) {
                throw new IllegalArgumentException("skew.hotKeyTrafficPct must be in [0,1]");
            }
        }
        if (lateEnabled) {
            if (latePct < 0.0 || latePct > 1.0) {
                throw new IllegalArgumentException("late.pct must be in [0,1]");
            }
            if (lateMinMs < 0 || lateMaxMs < lateMinMs) {
                throw new IllegalArgumentException("late range invalid: require 0 <= minMs <= maxMs");
            }
        }
        if (checkpointEnabled && checkpointDir.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpoint.dir is required when checkpoint.enabled=true");
        }
    }

    private static FaultConfig faultFrom(ParameterTool p, String prefix, FaultType enabledType,
                                         FaultStage defaultStage, TriggerMode defaultTrigger,
                                         long defaultTriggerN) {
        boolean enabled = p.getBoolean(prefix + ".enabled", false);
        if (!enabled) {
            return FaultConfig.disabled();
        }
        FaultStage stage = enumOf(FaultStage.class, p.get(prefix + ".stage", defaultStage.name()));
        TriggerMode mode = enumOf(TriggerMode.class, p.get(prefix + ".triggerMode", defaultTrigger.name()));
        long triggerN = p.getLong(prefix + ".triggerN", defaultTriggerN);
        int maxOcc = p.getInt(prefix + ".maxOccurrences", 1);
        double prob = p.getDouble(prefix + ".probability", 1.0);
        int haltCode = p.getInt(prefix + ".haltExitCode", 1);
        return new FaultConfig(enabledType, stage, mode, triggerN, maxOcc, prob, haltCode);
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String raw) {
        try {
            return Enum.valueOf(type, raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value '" + raw + "' for " + type.getSimpleName()
                    + "; allowed: " + java.util.Arrays.toString(type.getEnumConstants()));
        }
    }

    public static HarnessConfig fromArgs(String[] args) {
        ParameterTool cli = ParameterTool.fromArgs(args);
        Map<String, String> merged = new HashMap<>();
        if (cli.has("scenario")) {
            merged.putAll(ScenarioLoader.load(cli.get("scenario")));
        }
        merged.putAll(cli.toMap());
        return new HarnessConfig(ParameterTool.fromMap(merged));
    }

    public static HarnessConfig fromMap(Map<String, String> map) {
        return new HarnessConfig(ParameterTool.fromMap(map));
    }

    @Override
    public String toString() {
        return "HarnessConfig{"
                + "jobName=" + jobName
                + ", parallelism=" + parallelism
                + ", rateMode=" + rateMode
                + ", ratePerSec=" + ratePerSec
                + ", recordSizeBytes=" + recordSizeBytes
                + ", numKeys=" + numKeys
                + ", maxRecords=" + maxRecords
                + ", skewEnabled=" + skewEnabled
                + ", hotKeyCount=" + hotKeyCount
                + ", hotKeyTrafficPct=" + hotKeyTrafficPct
                + ", lateEnabled=" + lateEnabled
                + ", latePct=" + latePct
                + ", lateRangeMs=[" + lateMinMs + "," + lateMaxMs + "]"
                + ", partitionMode=" + partitionMode
                + ", windowType=" + windowType
                + ", timeCharacteristic=" + timeCharacteristic
                + ", watermarkStrategy=" + watermarkStrategyType
                + ", windowSizeMs=" + windowSizeMs
                + ", windowCountSize=" + windowCountSize
                + ", sessionGapMs=" + sessionGapMs
                + ", cpuBurnMicros=" + cpuBurnMicros
                + ", stateAccumulatorBytes=" + stateAccumulatorBytes
                + ", sinkMode=" + sinkMode
                + ", stateBackend=" + stateBackend
                + ", checkpointEnabled=" + checkpointEnabled
                + ", checkpointDir=" + checkpointDir
                + ", appFault=" + appFault
                + ", sysFault=" + sysFault
                + '}';
    }
}
