package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.HarnessConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Stream-boundary fault operator: a generic pass-through map that delegates to a
 * {@link FaultInjector}. Used at the {@code AFTER_SOURCE} and {@code BEFORE_SINK}
 * boundaries. The same {@link FaultInjector} is also embedded directly inside
 * the source/aggregate/sink operators for in-operator injection.
 */
public class FaultInjectionMap<T> extends RichMapFunction<T, T> {

    private static final long serialVersionUID = 1L;

    private final FaultInjector injector;

    public FaultInjectionMap(List<FaultConfig> faults, long seed) {
        this(new FaultInjector(faults, seed));
    }

    FaultInjectionMap(FaultInjector injector) {
        this.injector = injector;
    }

    /** Overrides the halt action (used by tests to avoid killing the JVM). */
    public FaultInjectionMap<T> withHaltStrategy(HaltStrategy strategy) {
        injector.withHaltStrategy(strategy);
        return this;
    }

    public boolean hasFaults() {
        return injector.hasFaults();
    }

    @Override
    public void open(Configuration parameters) {
        injector.open(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public T map(T value) {
        injector.onRecord();
        return value;
    }

    /** Collects the faults from config that target the given (boundary) stage. */
    public static <T> FaultInjectionMap<T> forStage(HarnessConfig cfg, FaultStage stage) {
        return new FaultInjectionMap<>(FaultInjector.forStage(cfg, stage));
    }
}
