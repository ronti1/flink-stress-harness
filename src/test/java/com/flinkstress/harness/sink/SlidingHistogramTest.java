package com.flinkstress.harness.sink;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlidingHistogramTest {

    @Test
    void emptyHistogramReturnsZeros() {
        SlidingHistogram h = new SlidingHistogram(16);
        HistogramStatistics s = h.getStatistics();
        assertThat(s.size()).isZero();
        assertThat(s.getMin()).isZero();
        assertThat(s.getMax()).isZero();
        assertThat(s.getMean()).isZero();
        assertThat(s.getQuantile(0.99)).isZero();
    }

    @Test
    void computesMinMaxMeanAndQuantiles() {
        SlidingHistogram h = new SlidingHistogram(1000);
        for (int i = 1; i <= 100; i++) {
            h.update(i);
        }
        HistogramStatistics s = h.getStatistics();
        assertThat(h.getCount()).isEqualTo(100);
        assertThat(s.getMin()).isEqualTo(1);
        assertThat(s.getMax()).isEqualTo(100);
        assertThat(s.getMean()).isEqualTo(50.5);
        // nearest-rank: p50 -> ceil(0.5*100)=50 -> value 50
        assertThat(s.getQuantile(0.50)).isEqualTo(50);
        assertThat(s.getQuantile(0.99)).isEqualTo(99);
        assertThat(s.getQuantile(1.0)).isEqualTo(100);
    }

    @Test
    void reservoirSlidesAndKeepsRecentValues() {
        SlidingHistogram h = new SlidingHistogram(3);
        h.update(1);
        h.update(2);
        h.update(3);
        h.update(4); // evicts 1
        HistogramStatistics s = h.getStatistics();
        assertThat(s.size()).isEqualTo(3);
        assertThat(s.getMin()).isEqualTo(2);
        assertThat(s.getMax()).isEqualTo(4);
        assertThat(h.getCount()).isEqualTo(4);
    }

    @Test
    void rejectsNonPositiveReservoir() {
        assertThatThrownBy(() -> new SlidingHistogram(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
