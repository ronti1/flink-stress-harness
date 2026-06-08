package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.TriggerMode;

import java.io.Serializable;

/**
 * Immutable description of a single injectable fault. Two of these are wired
 * by the harness (an application-exception fault and a system-halt fault),
 * each independently targetable to a {@link FaultStage} with its own cadence.
 */
public final class FaultConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final FaultType type;
    public final FaultStage stage;
    public final TriggerMode triggerMode;
    /** Records or milliseconds between triggers, per {@link #triggerMode}. */
    public final long triggerN;
    /** Max number of times the fault fires per subtask; &le; 0 means unlimited. */
    public final int maxOccurrences;
    /** Probability [0,1] that the fault fires once the trigger condition is met. */
    public final double probability;
    /** Exit code used for {@link FaultType#SYSTEM_HALT}. */
    public final int haltExitCode;

    public FaultConfig(FaultType type, FaultStage stage, TriggerMode triggerMode,
                       long triggerN, int maxOccurrences, double probability, int haltExitCode) {
        this.type = type;
        this.stage = stage;
        this.triggerMode = triggerMode;
        this.triggerN = triggerN;
        this.maxOccurrences = maxOccurrences;
        this.probability = probability;
        this.haltExitCode = haltExitCode;
    }

    public boolean isEnabled() {
        return type != FaultType.NONE;
    }

    public static FaultConfig disabled() {
        return new FaultConfig(FaultType.NONE, FaultStage.AFTER_SOURCE,
                TriggerMode.EVERY_MS, Long.MAX_VALUE, 0, 0.0, 1);
    }

    @Override
    public String toString() {
        return "FaultConfig{type=" + type
                + ", stage=" + stage
                + ", triggerMode=" + triggerMode
                + ", triggerN=" + triggerN
                + ", maxOccurrences=" + maxOccurrences
                + ", probability=" + probability
                + ", haltExitCode=" + haltExitCode
                + '}';
    }
}
