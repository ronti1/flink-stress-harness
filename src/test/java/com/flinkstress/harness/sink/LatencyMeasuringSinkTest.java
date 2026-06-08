package com.flinkstress.harness.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LatencyMeasuringSinkTest {

    @Test
    void latencyIsNowMinusEmit() {
        assertThat(LatencyMeasuringSink.latencyMs(1_000_500L, 1_000_000L)).isEqualTo(500L);
    }

    @Test
    void negativeLatencyClampedToZero() {
        // clock skew: emit time slightly in the future
        assertThat(LatencyMeasuringSink.latencyMs(1_000_000L, 1_000_050L)).isZero();
    }
}
