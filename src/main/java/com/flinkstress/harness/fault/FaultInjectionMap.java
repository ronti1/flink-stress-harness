package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.TriggerMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Pass-through map operator that injects faults. A single instance handles all
 * faults targeting one {@link FaultStage} (typically the app-exception fault
 * and/or the system-halt fault). Each fault has independent cadence
 * ({@link TriggerMode}), occurrence cap and probability.
 *
 * <p>The operator is generic over the record type because the fault logic only
 * counts records; it sits on the {@code Event} stream (after the source) or the
 * aggregate stream (before the sink) unchanged.
 *
 * <p>The trigger decision is isolated in {@link #evaluate} (pure, deterministic
 * given its inputs) so it can be unit tested without a running JVM-killer.
 */
public class FaultInjectionMap<T> extends RichMapFunction<T, T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FaultInjectionMap.class);

    /** Outcome of evaluating one fault for one record. */
    public enum Action {
        NONE, THROW, HALT
    }

    /** Per-subtask mutable runtime state for one fault. */
    public static final class FaultState {
        public int occurrences;
        public long lastTriggerMs;

        public FaultState(long lastTriggerMs) {
            this.lastTriggerMs = lastTriggerMs;
        }
    }

    private final List<FaultConfig> faults;
    private final long seed;
    private HaltStrategy haltStrategy = new JvmHaltStrategy();

    private transient List<FaultState> states;
    private transient Random rnd;
    private transient long recordCount;

    public FaultInjectionMap(List<FaultConfig> faults, long seed) {
        this.faults = faults;
        this.seed = seed;
    }

    /** Overrides the halt action (used by tests to avoid killing the JVM). */
    public FaultInjectionMap<T> withHaltStrategy(HaltStrategy strategy) {
        this.haltStrategy = strategy;
        return this;
    }

    public boolean hasFaults() {
        return !faults.isEmpty();
    }

    @Override
    public void open(Configuration parameters) {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        long now = System.currentTimeMillis();
        this.states = new ArrayList<>(faults.size());
        for (int i = 0; i < faults.size(); i++) {
            states.add(new FaultState(now));
        }
        this.rnd = new Random(seed + 7919L * subtaskIndex);
        this.recordCount = 0L;
    }

    @Override
    public T map(T value) {
        recordCount++;
        long now = System.currentTimeMillis();
        for (int i = 0; i < faults.size(); i++) {
            Action action = evaluate(faults.get(i), states.get(i), recordCount, now, rnd.nextDouble());
            switch (action) {
                case THROW:
                    throw new HarnessInjectedException("Injected application fault at stage "
                            + faults.get(i).stage + " after " + recordCount + " records");
                case HALT:
                    LOG.warn("Injecting SYSTEM_HALT (exit={}) at stage {} after {} records",
                            faults.get(i).haltExitCode, faults.get(i).stage, recordCount);
                    haltStrategy.halt(faults.get(i).haltExitCode);
                    break;
                case NONE:
                default:
                    break;
            }
        }
        return value;
    }

    /**
     * Decides whether {@code fault} fires for the current record, mutating
     * {@code st} (occurrence count, last-trigger time) as a side effect.
     *
     * @param roll a random draw in [0,1) compared against the fault probability
     */
    static Action evaluate(FaultConfig fault, FaultState st, long recordCount, long nowMs, double roll) {
        if (fault.type == FaultType.NONE) {
            return Action.NONE;
        }
        if (fault.maxOccurrences > 0 && st.occurrences >= fault.maxOccurrences) {
            return Action.NONE;
        }

        boolean due;
        if (fault.triggerMode == TriggerMode.EVERY_N_RECORDS) {
            due = fault.triggerN > 0 && recordCount % fault.triggerN == 0;
        } else {
            due = (nowMs - st.lastTriggerMs) >= fault.triggerN;
        }
        if (!due) {
            return Action.NONE;
        }

        st.lastTriggerMs = nowMs;
        if (roll < fault.probability) {
            st.occurrences++;
            return fault.type == FaultType.APP_EXCEPTION ? Action.THROW : Action.HALT;
        }
        return Action.NONE;
    }

    /** Collects the faults from config that target the given stage. */
    public static <T> FaultInjectionMap<T> forStage(HarnessConfig cfg, FaultStage stage) {
        List<FaultConfig> applicable = new ArrayList<>(2);
        if (cfg.appFault.isEnabled() && cfg.appFault.stage == stage) {
            applicable.add(cfg.appFault);
        }
        if (cfg.sysFault.isEnabled() && cfg.sysFault.stage == stage) {
            applicable.add(cfg.sysFault);
        }
        return new FaultInjectionMap<>(applicable, cfg.seed);
    }
}
