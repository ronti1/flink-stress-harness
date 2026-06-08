package com.flinkstress.harness.source;

import com.flinkstress.harness.config.HarnessConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RateControllerTest {

    private static RateController controller(Map<String, String> overrides) {
        return new RateController(HarnessConfig.fromMap(overrides));
    }

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void constantReturnsFixedRateAndIsPaced() {
        RateController c = controller(map("source.rateMode", "CONSTANT", "source.ratePerSec", "12345"));
        assertThat(c.isPaced()).isTrue();
        assertThat(c.currentRatePerSec(0)).isEqualTo(12345L);
        assertThat(c.currentRatePerSec(999_999)).isEqualTo(12345L);
    }

    @Test
    void maxIsUnpaced() {
        RateController c = controller(map("source.rateMode", "MAX"));
        assertThat(c.isPaced()).isFalse();
        assertThat(c.currentRatePerSec(0)).isEqualTo(RateController.UNPACED);
    }

    @Test
    void rampStepsUpEveryInterval() {
        RateController c = controller(map(
                "source.rateMode", "RAMP",
                "source.ramp.startRatePerSec", "1000",
                "source.ramp.stepRatePerSec", "500",
                "source.ramp.stepSeconds", "10"));
        assertThat(c.currentRatePerSec(0)).isEqualTo(1000L);
        assertThat(c.currentRatePerSec(9_999)).isEqualTo(1000L);   // still in first step
        assertThat(c.currentRatePerSec(10_000)).isEqualTo(1500L);  // second step
        assertThat(c.currentRatePerSec(25_000)).isEqualTo(2000L);  // third step
    }

    @Test
    void rampHonoursCeiling() {
        RateController c = controller(map(
                "source.rateMode", "RAMP",
                "source.ramp.startRatePerSec", "1000",
                "source.ramp.stepRatePerSec", "1000",
                "source.ramp.stepSeconds", "1",
                "source.ramp.maxRatePerSec", "3000"));
        assertThat(c.currentRatePerSec(10_000)).isEqualTo(3000L); // would be 11000 uncapped
    }

    @Test
    void burstAlternatesBetweenPeakAndBase() {
        RateController c = controller(map(
                "source.rateMode", "BURST",
                "source.burst.baseRatePerSec", "100",
                "source.burst.peakRatePerSec", "900",
                "source.burst.peakSeconds", "2",
                "source.burst.periodSeconds", "10"));
        assertThat(c.currentRatePerSec(0)).isEqualTo(900L);       // peak phase
        assertThat(c.currentRatePerSec(1_999)).isEqualTo(900L);
        assertThat(c.currentRatePerSec(2_000)).isEqualTo(100L);   // base phase
        assertThat(c.currentRatePerSec(9_999)).isEqualTo(100L);
        assertThat(c.currentRatePerSec(10_000)).isEqualTo(900L);  // next period peak
    }
}
