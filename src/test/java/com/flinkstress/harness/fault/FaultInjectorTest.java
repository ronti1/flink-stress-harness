package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.TriggerMode;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runtime behaviour of {@link FaultInjector#onRecord()} (the shared scheduler). */
class FaultInjectorTest {

    private static FaultConfig app(long n, int max) {
        return new FaultConfig(FaultType.APP_EXCEPTION, FaultStage.SOURCE,
                TriggerMode.EVERY_N_RECORDS, n, max, 1.0, 1);
    }

    private static FaultConfig sys(long n, int max, int exit) {
        return new FaultConfig(FaultType.SYSTEM_HALT, FaultStage.SINK,
                TriggerMode.EVERY_N_RECORDS, n, max, 1.0, exit);
    }

    @Test
    void noFaultsIsANoOp() {
        FaultInjector fi = new FaultInjector(Collections.emptyList(), 0L);
        assertThat(fi.hasFaults()).isFalse();
        fi.open(0);
        for (int i = 0; i < 10; i++) {
            fi.onRecord(); // must not throw
        }
    }

    @Test
    void appFaultThrowsAtInterval() {
        FaultInjector fi = new FaultInjector(Collections.singletonList(app(2, 1)), 0L);
        fi.open(0);
        fi.onRecord(); // record 1 -> ok
        assertThatThrownBy(fi::onRecord) // record 2 -> fault
                .isInstanceOf(HarnessInjectedException.class);
    }

    @Test
    void systemHaltUsesHaltStrategyAndRespectsMaxOccurrences() {
        RecordingHaltStrategy halt = new RecordingHaltStrategy();
        FaultInjector fi = new FaultInjector(Collections.singletonList(sys(2, 2, 7)), 0L)
                .withHaltStrategy(halt);
        fi.open(0);
        for (int i = 0; i < 6; i++) {
            fi.onRecord();
        }
        assertThat(halt.calls.get()).isEqualTo(2); // fires at record 2 and 4, then capped
        assertThat(halt.lastExitCode).isEqualTo(7);
    }

    @Test
    void lazyOpenWhenOnRecordCalledWithoutExplicitOpen() {
        FaultInjector fi = new FaultInjector(Collections.singletonList(app(1, 1)), 0L);
        // no open() call -> ensureOpen() kicks in
        assertThatThrownBy(fi::onRecord).isInstanceOf(HarnessInjectedException.class);
    }
}
