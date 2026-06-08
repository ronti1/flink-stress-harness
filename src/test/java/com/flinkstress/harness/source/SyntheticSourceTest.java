package com.flinkstress.harness.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SyntheticSourceTest {

    @Test
    void perSubtaskMaxSplitsEvenly() {
        assertThat(SyntheticSource.perSubtaskMax(100, 4, 0)).isEqualTo(25);
        assertThat(SyntheticSource.perSubtaskMax(100, 4, 3)).isEqualTo(25);
    }

    @Test
    void perSubtaskMaxDistributesRemainderToLowIndices() {
        // 10 records across 3 subtasks -> 4,3,3
        assertThat(SyntheticSource.perSubtaskMax(10, 3, 0)).isEqualTo(4);
        assertThat(SyntheticSource.perSubtaskMax(10, 3, 1)).isEqualTo(3);
        assertThat(SyntheticSource.perSubtaskMax(10, 3, 2)).isEqualTo(3);
        long sum = SyntheticSource.perSubtaskMax(10, 3, 0)
                + SyntheticSource.perSubtaskMax(10, 3, 1)
                + SyntheticSource.perSubtaskMax(10, 3, 2);
        assertThat(sum).isEqualTo(10);
    }

    @Test
    void unboundedReturnsZero() {
        assertThat(SyntheticSource.perSubtaskMax(0, 4, 1)).isEqualTo(0);
    }
}
