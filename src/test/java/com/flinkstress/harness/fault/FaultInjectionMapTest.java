package com.flinkstress.harness.fault;

import com.flinkstress.harness.config.FaultStage;
import com.flinkstress.harness.config.FaultType;
import com.flinkstress.harness.config.TriggerMode;
import com.flinkstress.harness.model.Event;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FaultInjectionMapTest {

    private static Event ev() {
        return new Event(1, "k1", System.currentTimeMillis(), System.currentTimeMillis(), new byte[8]);
    }

    private static OneInputStreamOperatorTestHarness<Event, Event> harness(FaultInjectionMap<Event> fn)
            throws Exception {
        OneInputStreamOperatorTestHarness<Event, Event> h =
                new OneInputStreamOperatorTestHarness<>(new StreamMap<>(fn));
        h.open();
        return h;
    }

    @Test
    void appExceptionThrownAtConfiguredInterval() throws Exception {
        FaultConfig app = new FaultConfig(FaultType.APP_EXCEPTION, FaultStage.AFTER_SOURCE,
                TriggerMode.EVERY_N_RECORDS, 2, 1, 1.0, 1);
        FaultInjectionMap<Event> fn = new FaultInjectionMap<>(Collections.singletonList(app), 1L);

        try (OneInputStreamOperatorTestHarness<Event, Event> h = harness(fn)) {
            h.processElement(new StreamRecord<>(ev())); // record 1 -> ok
            assertThatThrownBy(() -> h.processElement(new StreamRecord<>(ev()))) // record 2 -> fault
                    .isInstanceOf(HarnessInjectedException.class);
        }
    }

    @Test
    void systemHaltInvokedWithoutKillingJvm() throws Exception {
        RecordingHaltStrategy halt = new RecordingHaltStrategy();
        FaultConfig sys = new FaultConfig(FaultType.SYSTEM_HALT, FaultStage.AFTER_SOURCE,
                TriggerMode.EVERY_N_RECORDS, 2, 2, 1.0, 7);
        FaultInjectionMap<Event> fn =
                new FaultInjectionMap<Event>(Collections.singletonList(sys), 1L).withHaltStrategy(halt);

        try (OneInputStreamOperatorTestHarness<Event, Event> h = harness(fn)) {
            for (int i = 0; i < 4; i++) {
                h.processElement(new StreamRecord<>(ev()));
            }
        }
        assertThat(halt.calls.get()).isEqualTo(2); // records 2 and 4
        assertThat(halt.lastExitCode).isEqualTo(7);
    }

    @Test
    void passesThroughWhenNoFaultsConfigured() throws Exception {
        FaultInjectionMap<Event> fn = new FaultInjectionMap<>(Collections.emptyList(), 1L);
        assertThat(fn.hasFaults()).isFalse();

        try (OneInputStreamOperatorTestHarness<Event, Event> h = harness(fn)) {
            Event e = ev();
            h.processElement(new StreamRecord<>(e));
            assertThat(h.extractOutputValues()).containsExactly(e);
        }
    }
}
