package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.TriggerMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FaultInjectionEvaluateTest {

    private static FaultConfig fault(FaultType type, TriggerMode mode, long n, int max, double prob) {
        return new FaultConfig(type, FaultStage.AFTER_SOURCE, mode, n, max, prob, 1);
    }

    @Test
    void disabledFaultNeverFires() {
        FaultConfig f = FaultConfig.disabled();
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        assertThat(FaultInjectionMap.evaluate(f, st, 10, 10_000, 0.0))
                .isEqualTo(FaultInjectionMap.Action.NONE);
    }

    @Test
    void everyNRecordsFiresOnMultiples() {
        FaultConfig f = fault(FaultType.APP_EXCEPTION, TriggerMode.EVERY_N_RECORDS, 3, 0, 1.0);
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        assertThat(FaultInjectionMap.evaluate(f, st, 1, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.NONE);
        assertThat(FaultInjectionMap.evaluate(f, st, 2, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.NONE);
        assertThat(FaultInjectionMap.evaluate(f, st, 3, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.THROW);
        assertThat(FaultInjectionMap.evaluate(f, st, 6, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.THROW);
    }

    @Test
    void systemHaltActionForSystemFault() {
        FaultConfig f = fault(FaultType.SYSTEM_HALT, TriggerMode.EVERY_N_RECORDS, 1, 0, 1.0);
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        assertThat(FaultInjectionMap.evaluate(f, st, 1, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.HALT);
    }

    @Test
    void maxOccurrencesCapsFiring() {
        FaultConfig f = fault(FaultType.APP_EXCEPTION, TriggerMode.EVERY_N_RECORDS, 1, 2, 1.0);
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        assertThat(FaultInjectionMap.evaluate(f, st, 1, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.THROW);
        assertThat(FaultInjectionMap.evaluate(f, st, 2, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.THROW);
        // cap reached
        assertThat(FaultInjectionMap.evaluate(f, st, 3, 0, 0.0)).isEqualTo(FaultInjectionMap.Action.NONE);
        assertThat(st.occurrences).isEqualTo(2);
    }

    @Test
    void probabilityGateBlocksWhenRollExceeds() {
        FaultConfig f = fault(FaultType.APP_EXCEPTION, TriggerMode.EVERY_N_RECORDS, 1, 0, 0.5);
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        // roll >= probability -> no fire, occurrence not counted
        assertThat(FaultInjectionMap.evaluate(f, st, 1, 0, 0.9)).isEqualTo(FaultInjectionMap.Action.NONE);
        assertThat(st.occurrences).isZero();
        // roll < probability -> fire
        assertThat(FaultInjectionMap.evaluate(f, st, 2, 0, 0.1)).isEqualTo(FaultInjectionMap.Action.THROW);
        assertThat(st.occurrences).isEqualTo(1);
    }

    @Test
    void everyMsFiresAfterIntervalAndUpdatesLastTrigger() {
        FaultConfig f = fault(FaultType.SYSTEM_HALT, TriggerMode.EVERY_MS, 1000, 0, 1.0);
        FaultInjectionMap.FaultState st = new FaultInjectionMap.FaultState(0);
        // 500ms elapsed -> not due
        assertThat(FaultInjectionMap.evaluate(f, st, 1, 500, 0.0)).isEqualTo(FaultInjectionMap.Action.NONE);
        // 1000ms elapsed -> due, fires, lastTrigger advances to 1000
        assertThat(FaultInjectionMap.evaluate(f, st, 2, 1000, 0.0)).isEqualTo(FaultInjectionMap.Action.HALT);
        assertThat(st.lastTriggerMs).isEqualTo(1000);
        // only 1500 now (500 since last) -> not due
        assertThat(FaultInjectionMap.evaluate(f, st, 3, 1500, 0.0)).isEqualTo(FaultInjectionMap.Action.NONE);
        // 2000 -> due again
        assertThat(FaultInjectionMap.evaluate(f, st, 4, 2000, 0.0)).isEqualTo(FaultInjectionMap.Action.HALT);
    }
}
