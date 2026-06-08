package com.flinkstress.harness.config;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScenarioLoaderTest {

    @Test
    void flattensNestedMapToDotKeys() {
        Map<String, Object> source = new LinkedHashMap<>();
        Map<String, Object> ramp = new LinkedHashMap<>();
        ramp.put("startRatePerSec", 5000);
        source.put("rateMode", "RAMP");
        source.put("ramp", ramp);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("source", source);

        Map<String, String> flat = ScenarioLoader.flattenMap(root);
        assertThat(flat).containsEntry("source.rateMode", "RAMP");
        assertThat(flat).containsEntry("source.ramp.startRatePerSec", "5000");
    }

    @Test
    void loadsScenarioFromClasspath() {
        Map<String, String> flat = ScenarioLoader.load("kill-recovery");
        assertThat(flat).containsEntry("fault.sys.enabled", "true");
        assertThat(flat).containsEntry("checkpoint.enabled", "true");
        assertThat(flat).containsEntry("source.rateMode", "CONSTANT");
    }

    @Test
    void missingScenarioThrows() {
        assertThatThrownBy(() -> ScenarioLoader.load("does-not-exist"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Scenario not found");
    }

    @Test
    void allBundledScenariosParseIntoValidConfig() {
        String[] scenarios = {
                "ceiling-ramp", "latency-soak", "kill-recovery", "backpressure",
                "skew-hotkey", "late-data", "processing-time"
        };
        for (String name : scenarios) {
            HarnessConfig cfg = HarnessConfig.fromArgs(new String[]{"--scenario", name});
            assertThat(cfg).as("scenario %s", name).isNotNull();
        }
    }
}
