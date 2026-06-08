package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.HarnessConfig;
import com.flinkstress.harness.config.TriggerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Reusable, serializable fault scheduler. Holds the faults targeting one
 * {@link FaultStage}, keeps per-subtask runtime state, and on each record
 * decides (via the pure {@link #evaluate}) whether to throw an application
 * exception or halt the JVM.
 *
 * <p>This is the single home of the fault logic. It is used two ways:
 * <ul>
 *   <li>by {@link FaultInjectionMap} as a stream-boundary pass-through operator, and</li>
 *   <li>directly <em>inside</em> the real operators (source, aggregate, sink) for
 *       operator-accurate injection.</li>
 * </ul>
 */
public final class FaultInjector implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FaultInjector.class);

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
    private transient boolean opened;

    public FaultInjector(List<FaultConfig> faults, long seed) {
        this.faults = faults;
        this.seed = seed;
    }

    /** Overrides the halt action (used by tests to avoid killing the JVM). */
    public FaultInjector withHaltStrategy(HaltStrategy strategy) {
        this.haltStrategy = strategy;
        return this;
    }

    public boolean hasFaults() {
        return !faults.isEmpty();
    }

    /** Initialises per-subtask state; call from the operator's open with its subtask index. */
    public void open(int subtaskIndex) {
        long now = System.currentTimeMillis();
        this.states = new ArrayList<>(faults.size());
        for (int i = 0; i < faults.size(); i++) {
            states.add(new FaultState(now));
        }
        this.rnd = new Random(seed + 7919L * subtaskIndex);
        this.recordCount = 0L;
        this.opened = true;
    }

    private void ensureOpen() {
        if (!opened) {
            open(0); // non-Rich callers (e.g. AggregateFunction) have no subtask index
        }
    }

    /** Call once per record; throws or halts if a targeted fault fires. */
    public void onRecord() {
        ensureOpen();
        recordCount++;
        long now = System.currentTimeMillis();
        for (int i = 0; i < faults.size(); i++) {
            FaultConfig fault = faults.get(i);
            Action action = evaluate(fault, states.get(i), recordCount, now, rnd.nextDouble());
            switch (action) {
                case THROW:
                    throw new HarnessInjectedException("Injected application fault at stage "
                            + fault.stage + " after " + recordCount + " records");
                case HALT:
                    LOG.warn("Injecting SYSTEM_HALT (exit={}) at stage {} after {} records",
                            fault.haltExitCode, fault.stage, recordCount);
                    haltStrategy.halt(fault.haltExitCode);
                    break;
                case NONE:
                default:
                    break;
            }
        }
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
    public static FaultInjector forStage(HarnessConfig cfg, FaultStage stage) {
        List<FaultConfig> applicable = new ArrayList<>(2);
        if (cfg.appFault.isEnabled() && cfg.appFault.stage == stage) {
            applicable.add(cfg.appFault);
        }
        if (cfg.sysFault.isEnabled() && cfg.sysFault.stage == stage) {
            applicable.add(cfg.sysFault);
        }
        return new FaultInjector(applicable, cfg.seed);
    }
}
