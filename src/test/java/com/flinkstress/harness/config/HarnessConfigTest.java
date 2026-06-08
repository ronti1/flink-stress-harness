package com.flinkstress.harness.config;

import com.flinkstress.harness.fault.FaultConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HarnessConfigTest {

    @Test
    void appliesDefaultsWhenEmpty() {
        HarnessConfig cfg = HarnessConfig.fromMap(new HashMap<>());
        assertThat(cfg.rateMode).isEqualTo(RateMode.CONSTANT);
        assertThat(cfg.ratePerSec).isEqualTo(10_000L);
        assertThat(cfg.recordSizeBytes).isEqualTo(256);
        assertThat(cfg.windowType).isEqualTo(WindowType.TUMBLING_TIME);
        assertThat(cfg.stateBackend).isEqualTo(StateBackend.HASHMAP);
        assertThat(cfg.partitionMode).isEqualTo(PartitionMode.KEY_BY);
        assertThat(cfg.sinkMode).isEqualTo(SinkMode.METRICS);
        assertThat(cfg.appFault.isEnabled()).isFalse();
        assertThat(cfg.sysFault.isEnabled()).isFalse();
    }

    @Test
    void parsesEnumsCaseInsensitively() {
        Map<String, String> m = new HashMap<>();
        m.put("source.rateMode", "ramp");
        m.put("window.type", "session");
        m.put("state.backend", "RocksDB");
        HarnessConfig cfg = HarnessConfig.fromMap(m);
        assertThat(cfg.rateMode).isEqualTo(RateMode.RAMP);
        assertThat(cfg.windowType).isEqualTo(WindowType.SESSION);
        assertThat(cfg.stateBackend).isEqualTo(StateBackend.ROCKSDB);
    }

    @Test
    void rejectsInvalidEnum() {
        Map<String, String> m = new HashMap<>();
        m.put("source.rateMode", "TURBO");
        assertThatThrownBy(() -> HarnessConfig.fromMap(m))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RateMode");
    }

    @Test
    void buildsEnabledFaultsFromConfig() {
        Map<String, String> m = new HashMap<>();
        m.put("fault.app.enabled", "true");
        m.put("fault.app.stage", "BEFORE_SINK");
        m.put("fault.app.triggerMode", "EVERY_N_RECORDS");
        m.put("fault.app.triggerN", "500");
        m.put("fault.app.maxOccurrences", "3");
        m.put("fault.sys.enabled", "true");
        HarnessConfig cfg = HarnessConfig.fromMap(m);

        FaultConfig app = cfg.appFault;
        assertThat(app.isEnabled()).isTrue();
        assertThat(app.type).isEqualTo(FaultType.APP_EXCEPTION);
        assertThat(app.stage).isEqualTo(FaultStage.BEFORE_SINK);
        assertThat(app.triggerMode).isEqualTo(TriggerMode.EVERY_N_RECORDS);
        assertThat(app.triggerN).isEqualTo(500L);
        assertThat(app.maxOccurrences).isEqualTo(3);

        assertThat(cfg.sysFault.type).isEqualTo(FaultType.SYSTEM_HALT);
    }

    @Test
    void validatesSkewBounds() {
        Map<String, String> m = new HashMap<>();
        m.put("skew.enabled", "true");
        m.put("source.numKeys", "100");
        m.put("skew.hotKeyCount", "200");
        assertThatThrownBy(() -> HarnessConfig.fromMap(m))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hotKeyCount");
    }

    @Test
    void validatesLateRange() {
        Map<String, String> m = new HashMap<>();
        m.put("late.enabled", "true");
        m.put("late.minMs", "5000");
        m.put("late.maxMs", "1000");
        assertThatThrownBy(() -> HarnessConfig.fromMap(m))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("late range");
    }

    @Test
    void requiresCheckpointDirWhenEnabled() {
        Map<String, String> m = new HashMap<>();
        m.put("checkpoint.enabled", "true");
        assertThatThrownBy(() -> HarnessConfig.fromMap(m))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkpoint.dir");
    }

    @Test
    void scenarioPresetIsOverriddenByCli() {
        // ceiling-ramp.yaml sets RAMP; CLI flips rateMode and ratePerSec.
        String[] args = {"--scenario", "ceiling-ramp", "--source.rateMode", "CONSTANT",
                "--source.ratePerSec", "777"};
        HarnessConfig cfg = HarnessConfig.fromArgs(args);
        assertThat(cfg.rateMode).isEqualTo(RateMode.CONSTANT);
        assertThat(cfg.ratePerSec).isEqualTo(777L);
        // value only present in the scenario still applies
        assertThat(cfg.rampStepSeconds).isEqualTo(15L);
    }

    @Test
    void effectiveSourceParallelismFallsBackToJob() {
        Map<String, String> m = new HashMap<>();
        m.put("job.parallelism", "8");
        assertThat(HarnessConfig.fromMap(m).effectiveSourceParallelism()).isEqualTo(8);
        m.put("source.parallelism", "3");
        assertThat(HarnessConfig.fromMap(m).effectiveSourceParallelism()).isEqualTo(3);
    }
}
